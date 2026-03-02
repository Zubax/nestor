#!/usr/bin/env python3
"""
nestor_ingest: Upload raw CF3D logs to a Nestor server.

The uploader streams one or more .cf3d files in fixed-size chunks to:
  /cf3d/api/v1/commit

Behavior summary:
  - Files are processed in deterministic lexicographic order.
  - Each chunk is POSTed with device metadata (device_uid, device).
  - Transient failures are retried with exponential backoff.
  - A verbose final ingest report is always emitted.

Notes:
  - This tool sends opaque bytes; it does not parse CF3D payload contents.
  - The server may still reject malformed data at decode/parse time.
"""

from __future__ import annotations

import argparse
import logging
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

COMMIT_PATH = "/cf3d/api/v1/commit"
CHUNK_BYTES = 8 * 1024 * 1024
REQUEST_TIMEOUT_S = 30.0
MAX_ATTEMPTS = 5
BASE_BACKOFF_S = 1.0
HELP_HEADER = (__doc__ or "").strip()

LOGGER = logging.getLogger("nestor_ingest")


@dataclass
class UploadStats:
    total_files: int
    processed_files: int = 0
    total_bytes_read: int = 0
    total_bytes_sent: int = 0
    total_chunks: int = 0
    total_http_attempts: int = 0
    total_chunks_succeeded: int = 0
    retry_count: int = 0
    status_histogram: Counter[int] = field(default_factory=Counter)
    last_ack_seqno: int | None = None
    started_monotonic: float = field(default_factory=time.monotonic)


@dataclass(frozen=True)
class FailureContext:
    file_path: Path
    chunk_index: int
    chunk_offset: int
    chunk_size: int
    attempts: int
    message: str


@dataclass
class UploadError(Exception):
    message: str
    retryable: bool
    status_code: int | None = None
    attempts: int = 1

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True)
class CommitResponse:
    status_code: int
    ack_seqno: int
    body_text: str


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    LOGGER.debug(
        "Logging configured: level=DEBUG chunk_bytes=%d timeout_s=%.1f max_attempts=%d base_backoff_s=%.1f",
        CHUNK_BYTES,
        REQUEST_TIMEOUT_S,
        MAX_ATTEMPTS,
        BASE_BACKOFF_S,
    )


def _parse_device_uid(value: str) -> int:
    try:
        return int(value, 0)
    except ValueError as ex:
        raise argparse.ArgumentTypeError("device_uid must be an integer literal (auto-radix)") from ex


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=HELP_HEADER,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--server", required=True, help="Base server URL, e.g., http://192.168.1.123")
    parser.add_argument("--device_uid", required=True, type=_parse_device_uid, help="Integer literal (auto-radix)")
    parser.add_argument("--device", required=True, help="Opaque non-empty device identifier")
    parser.add_argument("files", nargs="+", help="One or more CF3D log files")
    args = parser.parse_args(argv)
    if not args.device:
        parser.error("device must be non-empty")
    return args


def _build_commit_url(server: str) -> str:
    base = server.strip()
    if not base:
        raise ValueError("server must be non-empty")
    parsed = urllib.parse.urlsplit(base)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("server must be a valid absolute http(s) URL")
    commit_url = base.rstrip("/") + COMMIT_PATH
    LOGGER.debug("Resolved commit URL: server=%r commit_url=%r", server, commit_url)
    return commit_url


def _resolve_files(paths: Sequence[str]) -> list[Path]:
    resolved: list[Path] = []
    for raw_path in paths:
        path = Path(raw_path)
        LOGGER.debug("Preflight file check: raw=%r resolved=%s", raw_path, path)
        if not path.exists():
            raise FileNotFoundError(f"input file does not exist: {path}")
        if not path.is_file():
            raise ValueError(f"input path is not a regular file: {path}")
        try:
            with path.open("rb"):
                pass
        except OSError as ex:
            raise OSError(f"input file is not readable: {path}: {ex}") from ex
        resolved.append(path)
    sorted_paths = sorted(resolved, key=lambda p: (p.name, str(p)))
    LOGGER.info("Input files sorted lexicographically by file name: %s", [p.name for p in sorted_paths])
    LOGGER.debug("Sorted file processing order (full paths): %s", [str(p) for p in sorted_paths])
    return sorted_paths


def _decode_body(body_bytes: bytes) -> str:
    return body_bytes.decode("utf-8", errors="replace")


def _parse_ack_seqno(body_text: str) -> int:
    lines = body_text.splitlines()
    if not lines:
        raise ValueError("commit response body is empty")
    first_line = lines[0].strip()
    if not first_line:
        raise ValueError("commit response first line is empty")
    return int(first_line, 10)


def _raise_http_error(status_code: int, body_text: str) -> None:
    body_preview = body_text.replace("\n", "\\n")
    if len(body_preview) > 300:
        body_preview = body_preview[:300] + "...(truncated)"
    message = f"http status={status_code} body={body_preview!r}"
    retryable = 500 <= status_code <= 599
    raise UploadError(message=message, retryable=retryable, status_code=status_code)


def _post_chunk(
    *,
    commit_url: str,
    device_uid: int,
    device: str,
    payload: bytes,
) -> CommitResponse:
    params = urllib.parse.urlencode({"device_uid": str(device_uid), "device": device})
    url = f"{commit_url}?{params}"
    request = urllib.request.Request(url=url, data=payload, method="POST")
    request.add_header("Content-Type", "application/octet-stream")
    request.add_header("Accept", "text/plain")

    LOGGER.debug(
        "Sending commit request: url=%s payload_bytes=%d device_uid=%d device=%r",
        url,
        len(payload),
        device_uid,
        device,
    )
    try:
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_S) as response:
            status_code = int(response.getcode())
            body_text = _decode_body(response.read())
    except urllib.error.HTTPError as ex:
        status_code = int(ex.code)
        body_text = _decode_body(ex.read())
        LOGGER.error("HTTP error response received: status_code=%d", status_code)
        _raise_http_error(status_code, body_text)
    except (urllib.error.URLError, TimeoutError, socket.timeout, OSError) as ex:
        LOGGER.error("Network exception during upload: %s", ex)
        raise UploadError(message=f"network error: {ex}", retryable=True) from ex

    if status_code not in (200, 207):
        LOGGER.error("Unexpected HTTP status: status_code=%d", status_code)
        _raise_http_error(status_code, body_text)

    try:
        ack_seqno = _parse_ack_seqno(body_text)
    except ValueError as ex:
        LOGGER.error("Failed to parse ACK seqno from response body")
        raise UploadError(message=f"malformed ACK in response: {ex}", retryable=True, status_code=status_code) from ex

    LOGGER.debug(
        "Commit response accepted: status_code=%d ack_seqno=%d body_lines=%d",
        status_code,
        ack_seqno,
        len(body_text.splitlines()),
    )
    return CommitResponse(status_code=status_code, ack_seqno=ack_seqno, body_text=body_text)


def _upload_with_retries(
    *,
    commit_url: str,
    device_uid: int,
    device: str,
    payload: bytes,
    file_path: Path,
    chunk_index: int,
    chunk_offset: int,
    stats: UploadStats,
) -> CommitResponse:
    last_error: UploadError | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        stats.total_http_attempts += 1
        stats.total_bytes_sent += len(payload)
        attempt_started = time.monotonic()
        LOGGER.debug(
            "Upload attempt started: file=%s chunk_index=%d chunk_offset=%d chunk_bytes=%d attempt=%d/%d",
            file_path,
            chunk_index,
            chunk_offset,
            len(payload),
            attempt,
            MAX_ATTEMPTS,
        )
        try:
            response = _post_chunk(
                commit_url=commit_url,
                device_uid=device_uid,
                device=device,
                payload=payload,
            )
            duration = time.monotonic() - attempt_started
            stats.status_histogram[response.status_code] += 1
            if response.status_code == 207:
                LOGGER.warning(
                    "Chunk committed with partial server-side decode/parse failures: "
                    "file=%s chunk_index=%d offset=%d attempt=%d ack_seqno=%d duration_s=%.3f",
                    file_path,
                    chunk_index,
                    chunk_offset,
                    attempt,
                    response.ack_seqno,
                    duration,
                )
            else:
                LOGGER.info(
                    "Chunk committed successfully: file=%s chunk_index=%d offset=%d attempt=%d "
                    "ack_seqno=%d duration_s=%.3f",
                    file_path,
                    chunk_index,
                    chunk_offset,
                    attempt,
                    response.ack_seqno,
                    duration,
                )
            return response
        except UploadError as ex:
            duration = time.monotonic() - attempt_started
            if ex.status_code is not None:
                stats.status_histogram[ex.status_code] += 1
            last_error = UploadError(
                message=str(ex),
                retryable=ex.retryable,
                status_code=ex.status_code,
                attempts=attempt,
            )
            LOGGER.error(
                "Upload attempt failed: file=%s chunk_index=%d offset=%d attempt=%d/%d retryable=%s "
                "status_code=%r duration_s=%.3f error=%s",
                file_path,
                chunk_index,
                chunk_offset,
                attempt,
                MAX_ATTEMPTS,
                ex.retryable,
                ex.status_code,
                duration,
                ex,
            )
            if not ex.retryable or attempt >= MAX_ATTEMPTS:
                break
            backoff_s = BASE_BACKOFF_S * (2 ** (attempt - 1))
            stats.retry_count += 1
            LOGGER.warning(
                "Scheduling retry: file=%s chunk_index=%d offset=%d next_attempt=%d sleep_s=%.1f",
                file_path,
                chunk_index,
                chunk_offset,
                attempt + 1,
                backoff_s,
            )
            time.sleep(backoff_s)

    if last_error is None:
        raise UploadError(message="upload failed for unknown reason", retryable=False, attempts=MAX_ATTEMPTS)
    raise last_error


def _emit_final_report(*, stats: UploadStats, failure: FailureContext | None) -> None:
    elapsed_s = time.monotonic() - stats.started_monotonic
    throughput_mib_s = 0.0 if elapsed_s <= 0 else (stats.total_bytes_read / (1024 * 1024)) / elapsed_s
    LOGGER.info("========== Final Ingest Report ==========")
    LOGGER.info("files_total=%d files_processed=%d", stats.total_files, stats.processed_files)
    LOGGER.info("bytes_read=%d bytes_sent=%d", stats.total_bytes_read, stats.total_bytes_sent)
    LOGGER.info(
        "chunks_total=%d chunks_succeeded=%d http_attempts=%d retries=%d",
        stats.total_chunks,
        stats.total_chunks_succeeded,
        stats.total_http_attempts,
        stats.retry_count,
    )
    LOGGER.info("last_ack_seqno=%s", "none" if stats.last_ack_seqno is None else stats.last_ack_seqno)
    status_parts = [f"{code}:{count}" for code, count in sorted(stats.status_histogram.items())]
    LOGGER.info("http_status_histogram=%s", ",".join(status_parts) if status_parts else "none")
    LOGGER.info("elapsed_s=%.3f throughput_mib_s=%.3f", elapsed_s, throughput_mib_s)
    if failure is None:
        LOGGER.info("result=SUCCESS")
    else:
        LOGGER.error(
            "result=FAILED file=%s chunk_index=%d offset=%d chunk_bytes=%d attempts=%d error=%s",
            failure.file_path,
            failure.chunk_index,
            failure.chunk_offset,
            failure.chunk_size,
            failure.attempts,
            failure.message,
        )


def run(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        commit_url = _build_commit_url(args.server)
        files = _resolve_files(args.files)
    except (ValueError, OSError) as ex:
        LOGGER.error("Preflight validation failed: %s", ex)
        return 1

    stats = UploadStats(total_files=len(files))
    failure: FailureContext | None = None
    current_path = Path("<none>")
    current_chunk_index = 0
    current_chunk_offset = 0
    current_chunk_size = 0

    LOGGER.info(
        "Starting ingest: server=%s device_uid=%d device=%r files=%d chunk_bytes=%d",
        args.server,
        args.device_uid,
        args.device,
        len(files),
        CHUNK_BYTES,
    )
    LOGGER.warning(
        "Uploader is configured for opaque fixed-size chunk streaming without 256-byte alignment handling; "
        "server-side trailing-byte truncation remains possible per request."
    )
    try:
        for file_index, path in enumerate(files, start=1):
            current_path = path
            current_chunk_index = 0
            current_chunk_offset = 0
            current_chunk_size = 0
            LOGGER.info("Processing file %d/%d: path=%s", file_index, len(files), path)
            file_bytes = 0
            chunk_index = 0
            with path.open("rb") as file_obj:
                while True:
                    chunk_offset = file_obj.tell()
                    payload = file_obj.read(CHUNK_BYTES)
                    if not payload:
                        break
                    chunk_index += 1
                    current_chunk_index = chunk_index
                    current_chunk_offset = chunk_offset
                    current_chunk_size = len(payload)
                    stats.total_chunks += 1
                    stats.total_bytes_read += len(payload)
                    file_bytes += len(payload)
                    LOGGER.debug(
                        "Read chunk: file=%s chunk_index=%d offset=%d chunk_bytes=%d",
                        path,
                        chunk_index,
                        chunk_offset,
                        len(payload),
                    )

                    response = _upload_with_retries(
                        commit_url=commit_url,
                        device_uid=args.device_uid,
                        device=args.device,
                        payload=payload,
                        file_path=path,
                        chunk_index=chunk_index,
                        chunk_offset=chunk_offset,
                        stats=stats,
                    )
                    stats.total_chunks_succeeded += 1
                    stats.last_ack_seqno = response.ack_seqno

            stats.processed_files += 1
            LOGGER.info(
                "Completed file %d/%d: path=%s file_bytes=%d chunks=%d cumulative_ack=%s",
                file_index,
                len(files),
                path,
                file_bytes,
                chunk_index,
                "none" if stats.last_ack_seqno is None else stats.last_ack_seqno,
            )
    except UploadError as ex:
        LOGGER.error("Ingest aborted due to upload failure: %s", ex)
        failure = FailureContext(
            file_path=current_path,
            chunk_index=current_chunk_index,
            chunk_offset=current_chunk_offset,
            chunk_size=current_chunk_size,
            attempts=ex.attempts,
            message=str(ex),
        )
    except Exception:
        LOGGER.critical("Unexpected ingest exception", exc_info=True)
        failure = FailureContext(
            file_path=current_path,
            chunk_index=current_chunk_index,
            chunk_offset=current_chunk_offset,
            chunk_size=current_chunk_size,
            attempts=1,
            message="unexpected exception (see logs)",
        )
    finally:
        _emit_final_report(stats=stats, failure=failure)

    return 0 if failure is None else 1


def main() -> int:
    _configure_logging()
    try:
        return run()
    except Exception:
        LOGGER.critical("Unhandled exception in main", exc_info=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
