# Tools

This folder contains standalone self-contained helper scripts for data ingestion, fetching, and server validation.

## `nestor_ingest.py`

Uploads one or more raw `.cf3d` files to the server.
The script reads file bytes, sends them in chunks, retries on transient failures, and prints a final report.

```bash
python tools/nestor_ingest.py --server http://127.0.0.1:8000 --device_uid 0x123 --device "Vehicle-A" data/*.cf3d
```

## `e2e_test.py`

Runs a full local E2E check; intended for development only. Read the contents for details.

## `serve_smoke_test.py`

Runs a focused startup smoke test for `nestor serve`:
- launches a real `nestor serve` process in a temporary environment,
- waits for readiness via `GET /cf3d/api/v1/devices`,
- terminates the process and reports detailed diagnostics on failure.

This is the script used by the `nox -s serve_smoke` session.
