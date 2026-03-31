#!/usr/bin/env python3
"""
Capture DroneCAN CAN frames and commit them to Nestor (Cyphal Cloud).

Two modes of operation:
  live     -- Listen on a CAN interface and commit frames in real time.
  upload   -- Commit a previously recorded .cf3d file to the server.

Requires the Nestor library (for fec_envelope) to be importable.
Install: pip install -e /path/to/nestor   OR   set NESTOR_PATH.

Usage:
    # Real-time capture and commit
    python commit_traffic.py live --iface vcan0 --device my-drone --device-uid 0x1

    # Upload a recorded file later
    python commit_traffic.py upload traffic.cf3d --device my-drone --device-uid 0x1
"""

import argparse
import logging
import os
import struct
import sys
import time
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Attempt to import fec_envelope from the local nestor checkout.
# Falls back to PYTHONPATH / installed package.
# ---------------------------------------------------------------------------
_NESTOR_DIR = os.environ.get("NESTOR_PATH", str(Path(__file__).resolve().parent.parent / "nestor"))
if _NESTOR_DIR not in sys.path:
    sys.path.insert(0, _NESTOR_DIR)

try:
    from nestor.fec_envelope import box, RECORD_BYTES, USER_DATA_BYTES
except ImportError:
    sys.exit(
        "ERROR: Cannot import nestor.fec_envelope.\n"
        "  Either install nestor (pip install -e /path/to/nestor)\n"
        "  or set NESTOR_PATH to the nestor repo root."
    )

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger(__name__)

# SocketCAN flag constants (matches Linux kernel / nestor model.py)
CAN_EFF_FLAG = 0x80000000


def pack_cf3d_record(boot_id: int, seqno: int, hw_ts_us: int,
                     can_id_with_flags: int, data: bytes) -> bytes:
    """Build one 256-byte CF3D record from a raw CAN frame."""
    if len(data) > 64:
        raise ValueError(f"CAN payload too long: {len(data)} > 64")
    buf = bytearray(USER_DATA_BYTES)  # 105 bytes, zero-filled
    buf[0] = 0  # version
    struct.pack_into("<QQQ", buf, 8, boot_id, seqno, hw_ts_us)
    struct.pack_into("<I", buf, 36, can_id_with_flags)
    buf[40] = len(data)
    buf[41:41 + len(data)] = data
    return box(bytes(buf))


def commit_batch(server: str, device: str, device_uid: str,
                 payload: bytes) -> bool:
    """POST binary CF3D payload to the Nestor /commit endpoint."""
    url = f"{server}/cf3d/api/v1/commit"
    params = {"device": device, "device_uid": device_uid}
    try:
        resp = requests.post(
            url,
            params=params,
            data=payload,
            headers={"Content-Type": "application/octet-stream"},
            timeout=30,
        )
        n_records = len(payload) // RECORD_BYTES
        if resp.status_code == 200:
            LOG.info("Committed %d records OK (ack: %s)", n_records, resp.text.strip())
            return True
        elif resp.status_code == 207:
            LOG.warning("Partial commit (%d records): %s", n_records, resp.text.strip())
            return True
        else:
            LOG.error("Commit failed %d: %s", resp.status_code, resp.text.strip())
            return False
    except requests.RequestException as exc:
        LOG.error("Commit request failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Shared CAN capture logic used by both "live" and "record" subcommands.
# ---------------------------------------------------------------------------

def _make_capture_hook(boot_id, t0, state, on_record):
    """Return a CAN driver IO hook that encodes frames and calls on_record(bytes)."""
    def frame_hook(direction, frame):
        can_id = frame.id
        data = bytes(frame.data)
        can_id_with_flags = can_id | (CAN_EFF_FLAG if frame.extended else 0)
        hw_ts_us = int((frame.ts_monotonic - t0) * 1_000_000)
        state["seqno"] += 1
        record = pack_cf3d_record(boot_id, state["seqno"], hw_ts_us,
                                  can_id_with_flags, data)
        state["captured"] += 1
        on_record(record)
    return frame_hook


# ---------------------------------------------------------------------------
# Subcommand: live
# ---------------------------------------------------------------------------

def cmd_live(args):
    import dronecan

    boot_id = args.boot_id if args.boot_id is not None else int(time.time())
    t0 = time.monotonic()
    batch = bytearray()
    last_flush = time.monotonic()
    state = {"seqno": 0, "captured": 0, "committed": 0, "failed": 0}

    node = dronecan.make_node(args.iface, node_id=args.node_id)

    def flush():
        nonlocal batch, last_flush
        if not batch:
            return
        payload = bytes(batch)
        ok = commit_batch(args.server, args.device, args.device_uid, payload)
        n = len(payload) // RECORD_BYTES
        if ok:
            state["committed"] += n
        else:
            state["failed"] += n
        batch = bytearray()
        last_flush = time.monotonic()

    def on_record(rec):
        batch.extend(rec)

    hook = _make_capture_hook(boot_id, t0, state, on_record)

    LOG.info("LIVE: %s (node %d) -> %s device=%s uid=%s",
             args.iface, args.node_id, args.server, args.device, args.device_uid)
    LOG.info("Boot ID: %d | Batch: %d frames | Flush: %.1fs",
             boot_id, args.batch_size, args.flush_interval)

    hook_handle = node.can_driver.add_io_hook(hook)
    try:
        while True:
            try:
                node.spin(timeout=0.1)
            except dronecan.transport.TransferError as exc:
                LOG.debug("Ignoring transfer error: %s", exc)
            if len(batch) >= args.batch_size * RECORD_BYTES:
                flush()
            elif batch and (time.monotonic() - last_flush) >= args.flush_interval:
                flush()
    except KeyboardInterrupt:
        hook_handle.remove()
        flush()
        LOG.info("Stats: captured=%d committed=%d failed=%d",
                 state["captured"], state["committed"], state["failed"])


# ---------------------------------------------------------------------------
# Subcommand: upload
# ---------------------------------------------------------------------------

def cmd_upload(args):
    filepath = Path(args.file)
    if not filepath.exists():
        sys.exit(f"File not found: {filepath}")

    data = filepath.read_bytes()
    if len(data) % RECORD_BYTES != 0:
        LOG.warning("File size %d is not a multiple of %d; trailing bytes will be ignored",
                    len(data), RECORD_BYTES)
        data = data[:len(data) - (len(data) % RECORD_BYTES)]

    total = len(data) // RECORD_BYTES
    if total == 0:
        sys.exit("No complete CF3D records in file")

    LOG.info("UPLOAD: %s (%d records) -> %s device=%s uid=%s",
             filepath, total, args.server, args.device, args.device_uid)

    batch_bytes = args.batch_size * RECORD_BYTES
    committed = 0
    failed = 0
    for offset in range(0, len(data), batch_bytes):
        chunk = data[offset:offset + batch_bytes]
        n = len(chunk) // RECORD_BYTES
        if commit_batch(args.server, args.device, args.device_uid, chunk):
            committed += n
        else:
            failed += n
        LOG.info("Progress: %d / %d", committed + failed, total)

    LOG.info("Done: committed=%d failed=%d total=%d", committed, failed, total)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Capture DroneCAN traffic and commit to Nestor (Cyphal Cloud)")
    sub = parser.add_subparsers(dest="command", required=True)

    # --- live ---
    p_live = sub.add_parser("live", help="Capture and commit in real time")
    p_live.add_argument("--iface", default="vcan0")
    p_live.add_argument("--node-id", type=int, default=127)
    p_live.add_argument("--device", default="local")
    p_live.add_argument("--device-uid", required=True)
    p_live.add_argument("--server", default="https://cyphalcloud.zubax.com")
    p_live.add_argument("--batch-size", type=int, default=100)
    p_live.add_argument("--flush-interval", type=float, default=5.0)
    p_live.add_argument("--boot-id", type=int, default=None)

    # --- upload ---
    p_up = sub.add_parser("upload", help="Upload a recorded .cf3d file to Nestor")
    p_up.add_argument("file", help="Path to .cf3d file")
    p_up.add_argument("--device", default="local")
    p_up.add_argument("--device-uid", required=True)
    p_up.add_argument("--server", default="https://cyphalcloud.zubax.com")
    p_up.add_argument("--batch-size", type=int, default=100)

    args = parser.parse_args()
    {"live": cmd_live, "upload": cmd_upload}[args.command](args)


if __name__ == "__main__":
    main()
