#!/usr/bin/env python3
"""
Standalone end-to-end validator for Nestor.

Why this exists:
- The nox session should stay small and orchestration-heavy logic belongs in a dedicated script.
- The test exercises the real process boundary (`nestor serve`) plus the real uploader tool
  (`tools/nestor_ingest.py`) and then verifies query endpoints.
- The validation dataset (`data/0000000.cf3d`) is treated as a golden input fixture.

Success criteria:
1. Server starts and becomes reachable.
2. Ingest tool uploads the dataset successfully.
3. Query endpoints report exactly the expected record count and latest seqno.
"""

from __future__ import annotations

import argparse
import json
import logging
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

LOGGER = logging.getLogger("nestor_e2e")
DEFAULT_DATASET_PATH = Path("data") / "0000000.cf3d"
DEFAULT_INGEST_SCRIPT_PATH = Path("tools") / "nestor_ingest.py"
DEFAULT_DEVICE_UID = "0x123"
DEFAULT_DEVICE = "e2e-dataset"
DEFAULT_READINESS_TIMEOUT_S = 30.0


def _configure_logging() -> None:
    # The E2E runner is diagnostic-first: default to DEBUG so failures in CI are actionable.
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    LOGGER.debug("Logging configured for E2E test runner at DEBUG level")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="End-to-end Nestor test: serve + ingest + retrieval")
    parser.add_argument("--dataset", default=str(DEFAULT_DATASET_PATH), help="Path to .cf3d validation dataset")
    parser.add_argument(
        "--ingest-script",
        default=str(DEFAULT_INGEST_SCRIPT_PATH),
        help="Path to ingest script (tools/nestor_ingest.py)",
    )
    parser.add_argument("--device-uid", default=DEFAULT_DEVICE_UID, help="Device UID string passed to ingest script")
    parser.add_argument("--device", default=DEFAULT_DEVICE, help="Device identifier passed to ingest script")
    parser.add_argument(
        "--readiness-timeout-s",
        type=float,
        default=DEFAULT_READINESS_TIMEOUT_S,
        help="Server readiness timeout in seconds",
    )
    return parser.parse_args(argv)


def _pick_free_port() -> int:
    # Bind to port 0 to let the OS pick an available local TCP port.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_server_ready(base_url: str, server_process: subprocess.Popen[bytes], timeout_s: float) -> None:
    # Readiness probing serves two purposes:
    # 1) Avoid racing ingest against server startup.
    # 2) Detect "server died on startup" early and report its exit code.
    deadline = time.monotonic() + timeout_s
    probe_url = f"{base_url}/cf3d/api/v1/devices"
    last_error: Exception | None = None
    LOGGER.debug("Waiting for server readiness: url=%s timeout_s=%.1f", probe_url, timeout_s)
    while time.monotonic() < deadline:
        if server_process.poll() is not None:
            raise RuntimeError(f"server process exited before readiness probe: return_code={server_process.returncode}")
        try:
            with urlopen(probe_url, timeout=2.0) as response:
                status_code = int(response.getcode())
                if status_code == 200:
                    LOGGER.info("Server readiness probe succeeded with status=%d", status_code)
                    return
                LOGGER.warning("Readiness probe returned non-200 status=%d; continuing", status_code)
        except Exception as ex:
            last_error = ex
            time.sleep(0.2)
    raise RuntimeError(f"server did not become ready within {timeout_s:.1f}s: last_error={last_error!r}")


def _http_get_json(url: str) -> dict[str, object]:
    LOGGER.debug("HTTP GET JSON: url=%s", url)
    with urlopen(url, timeout=10.0) as response:
        body = response.read().decode("utf-8")
    return json.loads(body)


def _terminate_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        LOGGER.debug("Server process already exited: return_code=%s", process.returncode)
        return
    LOGGER.info("Terminating server process pid=%d", process.pid)
    process.terminate()
    try:
        process.wait(timeout=5.0)
    except subprocess.TimeoutExpired:
        LOGGER.warning("Server did not terminate in time; killing pid=%d", process.pid)
        process.kill()
        process.wait(timeout=5.0)


def _tail_log(path: Path, max_lines: int = 120) -> str:
    if not path.exists():
        return f"<log file missing: {path}>"
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    if len(lines) <= max_lines:
        return "\n".join(lines)
    return "\n".join(lines[-max_lines:])


def _run_ingest(
    *,
    ingest_script_path: Path,
    base_url: str,
    device_uid: str,
    device: str,
    dataset_path: Path,
) -> None:
    # Use the current interpreter explicitly so the ingest tool uses the same virtualenv
    # and dependencies as this E2E runner.
    command = [
        sys.executable,
        str(ingest_script_path),
        "--server",
        base_url,
        "--device_uid",
        device_uid,
        "--device",
        device,
        str(dataset_path),
    ]
    LOGGER.info("Starting ingest command: %s", command)
    result = subprocess.run(command, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"ingest command failed with exit code {result.returncode}")
    LOGGER.info("Ingest command completed successfully")


def run(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    dataset_path = Path(args.dataset)
    ingest_script_path = Path(args.ingest_script)
    readiness_timeout_s = float(args.readiness_timeout_s)
    device_uid = str(args.device_uid)
    device = str(args.device)

    # Preflight checks keep failures deterministic and immediately understandable.
    if not dataset_path.exists():
        LOGGER.error("Validation dataset is missing: %s", dataset_path)
        return 1
    if not ingest_script_path.exists():
        LOGGER.error("Ingest script is missing: %s", ingest_script_path)
        return 1
    if dataset_path.stat().st_size % 256 != 0:
        LOGGER.error(
            "Dataset size must be divisible by 256: path=%s bytes=%d", dataset_path, dataset_path.stat().st_size
        )
        return 1

    # CF3D storage records are always 256 bytes; the dataset should therefore be exact-multiple.
    expected_records = dataset_path.stat().st_size // 256
    if expected_records <= 0:
        LOGGER.error("Validation dataset is unexpectedly empty: %s", dataset_path)
        return 1

    LOGGER.info(
        "Starting E2E test: dataset=%s expected_records=%d ingest_script=%s device_uid=%s device=%r",
        dataset_path,
        expected_records,
        ingest_script_path,
        device_uid,
        device,
    )

    with tempfile.TemporaryDirectory(prefix="nestor-e2e-") as temp_dir:
        # Isolate all runtime artifacts (DB + logs + captured stdio) in a temp directory so
        # each run is hermetic and leaves no persistent state behind.
        temp_root = Path(temp_dir)
        db_path = temp_root / "nestor-e2e.db"
        log_path = temp_root / "nestor-e2e.log"
        stdout_path = temp_root / "nestor-e2e.stdout.log"
        stderr_path = temp_root / "nestor-e2e.stderr.log"
        port = _pick_free_port()
        base_url = f"http://127.0.0.1:{port}"
        LOGGER.debug(
            "E2E runtime paths: db=%s log=%s stdout=%s stderr=%s base_url=%s",
            db_path,
            log_path,
            stdout_path,
            stderr_path,
            base_url,
        )

        with stdout_path.open("wb") as stdout_file, stderr_path.open("wb") as stderr_file:
            # Start a real server process through the CLI module path to match production startup.
            server_process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "nestor",
                    "serve",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(port),
                    "--db-path",
                    str(db_path),
                    "--log-file",
                    str(log_path),
                    "--log-level",
                    "INFO",
                ],
                stdout=stdout_file,
                stderr=stderr_file,
            )

            try:
                _wait_for_server_ready(base_url, server_process=server_process, timeout_s=readiness_timeout_s)

                # Upload the dataset exactly as a real client would.
                _run_ingest(
                    ingest_script_path=ingest_script_path,
                    base_url=base_url,
                    device_uid=device_uid,
                    device=device,
                    dataset_path=dataset_path,
                )

                # First retrieval check: the uploaded device must become query-visible.
                devices_body = _http_get_json(f"{base_url}/cf3d/api/v1/devices")
                devices = list(devices_body.get("devices", []))
                known_devices = [str(entry.get("device")) for entry in devices if isinstance(entry, dict)]
                if device not in known_devices:
                    raise AssertionError(f"uploaded device not found in /devices: devices={devices!r}")

                # Second retrieval check: at least one boot must be discoverable for this device.
                boots_body = _http_get_json(f"{base_url}/cf3d/api/v1/boots?{urlencode({'device': device})}")
                boot_entries = list(boots_body.get("boots", []))
                if not boot_entries:
                    raise AssertionError(f"no boots returned for {device!r}")
                boot_ids = sorted(int(entry["boot_id"]) for entry in boot_entries)

                # Third retrieval check: fetch records for all discovered boots and validate
                # cardinality + latest seqno against dataset-derived expectations.
                query_items: list[tuple[str, str]] = [("device", device), ("limit", "10000")]
                query_items.extend(("boot_id", str(boot_id)) for boot_id in boot_ids)
                records_body = _http_get_json(f"{base_url}/cf3d/api/v1/records?{urlencode(query_items, doseq=True)}")
                latest_seqno_seen = int(records_body.get("latest_seqno_seen", -1))
                records = list(records_body.get("records", []))

                if len(records) != expected_records:
                    raise AssertionError(
                        f"unexpected returned records count={len(records)}, expected={expected_records}, boot_ids={boot_ids}"
                    )
                if latest_seqno_seen != expected_records - 1:
                    raise AssertionError(
                        f"unexpected latest_seqno_seen={latest_seqno_seen}, expected={expected_records - 1}"
                    )

                LOGGER.info("E2E verification succeeded: latest_seqno_seen=%d boot_ids=%s", latest_seqno_seen, boot_ids)
                return 0
            except Exception:
                # Emit concise but high-value diagnostics so CI failures can be triaged quickly.
                LOGGER.critical(
                    "E2E test failed. Diagnostics:\n"
                    "Server log (%s) tail:\n%s\n"
                    "Server stdout (%s) tail:\n%s\n"
                    "Server stderr (%s) tail:\n%s",
                    log_path,
                    _tail_log(log_path),
                    stdout_path,
                    _tail_log(stdout_path),
                    stderr_path,
                    _tail_log(stderr_path),
                    exc_info=True,
                )
                return 1
            finally:
                _terminate_process(server_process)


def main() -> int:
    _configure_logging()
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
