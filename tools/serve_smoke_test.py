#!/usr/bin/env python3
"""
Smoke-test runner for `nestor serve`.

Why this exists:
- Keep nox orchestration concise by moving procedural logic into a dedicated script.
- Validate that a plain install (`pip install .`) has all runtime dependencies needed to start
  the server process.
- Capture startup failures with enough detail (exit code + stdout/stderr) for quick diagnosis.
"""

from __future__ import annotations

import http.client
import logging
import socket
import subprocess
import tempfile
import time
from pathlib import Path

LOGGER = logging.getLogger("nestor_serve_smoke")
DEFAULT_READINESS_TIMEOUT_S = 15.0


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    LOGGER.debug("Logging configured for serve smoke test runner")


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_readiness(*, port: int, process: subprocess.Popen[str], timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    LOGGER.debug("Waiting for readiness on port=%d timeout_s=%.1f", port, timeout_s)
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"server process exited before readiness: return_code={process.returncode}")
        try:
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=0.5)
            connection.request("GET", "/cf3d/api/v1/devices")
            response = connection.getresponse()
            response.read()
            connection.close()
            if response.status == 200:
                LOGGER.info("Readiness probe succeeded: status=%d", response.status)
                return
            LOGGER.warning("Readiness probe returned unexpected status=%d; retrying", response.status)
        except OSError as ex:
            last_error = ex
            time.sleep(0.1)
    raise RuntimeError(f"server did not become ready within {timeout_s:.1f}s: last_error={last_error!r}")


def _stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        LOGGER.debug("Server process already exited: return_code=%s", process.returncode)
        return
    LOGGER.info("Terminating server process pid=%d", process.pid)
    process.terminate()
    try:
        process.wait(timeout=5.0)
    except subprocess.TimeoutExpired:
        LOGGER.warning("Server did not terminate after SIGTERM; killing pid=%d", process.pid)
        process.kill()
        process.wait(timeout=5.0)


def run() -> int:
    port = _pick_free_port()
    LOGGER.debug("Selected local TCP port=%d for serve smoke test", port)
    with tempfile.TemporaryDirectory(prefix="nestor-serve-smoke-") as temp_dir:
        temp_root = Path(temp_dir)
        log_file = temp_root / "nestor.log"
        command = [
            "nestor",
            "serve",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--db-path",
            ":memory:",
            "--log-file",
            str(log_file),
        ]
        LOGGER.info("Launching server process: command=%s", command)
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout_data = ""
        stderr_data = ""
        failure: Exception | None = None
        try:
            _wait_for_readiness(port=port, process=process, timeout_s=DEFAULT_READINESS_TIMEOUT_S)
        except Exception as ex:
            failure = ex
        finally:
            _stop_process(process)
            if process.stdout is not None:
                stdout_data = process.stdout.read()
            if process.stderr is not None:
                stderr_data = process.stderr.read()
            LOGGER.debug("Captured process output: stdout_bytes=%d stderr_bytes=%d", len(stdout_data), len(stderr_data))
        if failure is not None:
            LOGGER.critical(
                "Serve smoke test failed: command=%s return_code=%s stdout=%r stderr=%r",
                command,
                process.returncode,
                stdout_data,
                stderr_data,
                exc_info=(type(failure), failure, failure.__traceback__),
            )
            return 1
        LOGGER.info("Serve smoke test passed")
        return 0


def main() -> int:
    _configure_logging()
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
