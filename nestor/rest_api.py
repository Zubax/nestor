from __future__ import annotations

import asyncio
import logging
import struct
import unittest
from collections.abc import Iterable, Sequence
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, ClassVar
from unittest.mock import patch

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from nestor.database import Boot, Database, DeviceInfo, SqliteDatabase
from nestor.fec_envelope import RECORD_BYTES, USER_DATA_BYTES, UnboxError, box, unbox
from nestor.model import (
    CAN_EFF_FLAG,
    CAN_ERR_FLAG,
    CAN_RTR_FLAG,
    CANFrame,
    CANFrameRecord,
    CANFrameRecordCommitted,
)

if TYPE_CHECKING:
    from fastapi.testclient import TestClient

WAIT_MAX_TIMEOUT_S = 30
WAIT_POLL_INTERVAL_S = 0.25
RECORDS_DEFAULT_LIMIT = 1000
RECORDS_MAX_LIMIT = 10000

LOGGER = logging.getLogger(__name__)
router = APIRouter()


class ErrorResponse(BaseModel):
    detail: str | list[dict[str, Any]] = Field(description="Error details")


class CANFrameDTO(BaseModel):
    can_id: int = Field(description="CAN ID without SocketCAN flags")
    extended: bool = Field(description="True if the frame uses extended (29-bit) identifier format")
    rtr: bool = Field(description="True if the frame is a remote transmission request")
    error: bool = Field(description="True if the frame is an error frame")
    data_hex: str = Field(description="CAN frame payload bytes encoded as lowercase hexadecimal")


class CANFrameRecordDTO(BaseModel):
    hw_ts_us: int = Field(
        description="Monotonic hardware timestamp in microseconds since this boot; sampled at frame capture time"
    )
    boot_id: int = Field(description="Identifier of the device boot session where this frame was captured")
    seqno: int = Field(
        description="Device-wide monotonic frame sequence number used for ordering, deduplication, and pagination"
    )
    commit_ts: int = Field(
        description="Server-side Unix timestamp (seconds) when this record was committed to the database"
    )
    frame: CANFrameDTO


class BootDTO(BaseModel):
    boot_id: int
    first_record: CANFrameRecordDTO
    last_record: CANFrameRecordDTO


class DeviceDTO(BaseModel):
    device: str
    last_heard_ts: int
    last_uid: int


class DevicesResponse(BaseModel):
    devices: list[DeviceDTO]


class BootsResponse(BaseModel):
    device: str
    boots: list[BootDTO]


class RecordsFilterEcho(BaseModel):
    boot_ids: list[int]
    seqno_min: int | None
    seqno_max: int | None
    wait_timeout_s: int
    limit: int


class RecordsResponse(BaseModel):
    device: str
    filters: RecordsFilterEcho
    latest_seqno_seen: int | None
    records: list[CANFrameRecordDTO]


def _serialize_frame(frame: CANFrame) -> CANFrameDTO:
    return CANFrameDTO(
        can_id=frame.can_id,
        extended=frame.extended,
        rtr=frame.rtr,
        error=frame.error,
        data_hex=bytes(frame.data).hex(),
    )


def _serialize_record(record: CANFrameRecordCommitted) -> CANFrameRecordDTO:
    return CANFrameRecordDTO(
        hw_ts_us=record.hw_ts_us,
        boot_id=record.boot_id,
        seqno=record.seqno,
        commit_ts=record.commit_ts,
        frame=_serialize_frame(record.frame),
    )


def _serialize_boot(boot: Boot) -> BootDTO:
    return BootDTO(
        boot_id=boot.boot_id,
        first_record=_serialize_record(boot.first_record),
        last_record=_serialize_record(boot.last_record),
    )


def _serialize_device_info(device: DeviceInfo) -> DeviceDTO:
    return DeviceDTO(device=device.device, last_heard_ts=device.last_heard_ts, last_uid=device.last_uid)


def _parse_device_uid(
    device_uid: Annotated[str, Query(min_length=1, description="Integer literal (auto-radix)")],
) -> int:
    try:
        return int(device_uid, 0)
    except ValueError as ex:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="device_uid must be an integer literal (auto-radix)",
        ) from ex


def get_database(request: Request) -> Database:
    database = getattr(request.app.state, "database", None)
    if not isinstance(database, Database):
        LOGGER.critical(
            "Application database dependency is invalid: type=%s",
            None if database is None else type(database).__name__,
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="internal server error")
    return database


@router.post(
    "/cf3d/api/v1/commit",
    response_class=PlainTextResponse,
    tags=["commit"],
    summary="Commit CAN frame records",
    description=(
        "Upload one or more binary Reed-Solomon-wrapped CF3D records. "
        "Query parameter 'device' is canonical; deprecated alias 'device_tag' is accepted on this endpoint only. "
        "Successful responses return cumulative ACK (last known seqno) as the first text line."
    ),
    responses={
        200: {
            "description": "All full records parsed/committed successfully.",
            "content": {"text/plain": {"example": "12345"}},
        },
        207: {
            "description": "Partial success (some records failed decode/parse).",
            "content": {"text/plain": {"example": "12345\naccepted=10 failed_decode=1 failed_parse=0"}},
        },
        422: {"model": ErrorResponse, "description": "Validation error"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def commit(
    request: Request,
    device_uid: Annotated[int, Depends(_parse_device_uid)],
    database: Annotated[Database, Depends(get_database)],
    device: Annotated[str | None, Query(min_length=1, description="Opaque device identifier")] = None,
    device_tag: Annotated[
        str | None,
        Query(min_length=1, description="DEPRECATED: use 'device'. Supported only on /commit."),
    ] = None,
) -> PlainTextResponse:
    """
    Commit endpoint for CF3D devices.
    """
    if device is None and device_tag is None:
        LOGGER.warning("Commit request rejected: both 'device' and deprecated 'device_tag' are missing")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="either 'device' or deprecated 'device_tag' query parameter is required",
        )

    resolved_device = device if device is not None else device_tag
    assert resolved_device is not None
    if device is None and device_tag is not None:
        LOGGER.warning(
            "Commit request is using deprecated query parameter 'device_tag': device_uid=%d device_tag=%r",
            device_uid,
            device_tag,
        )
    elif device is not None and device_tag is not None and device != device_tag:
        LOGGER.warning(
            "Commit request has conflicting device identifiers; preferring 'device' over deprecated 'device_tag': "
            "device_uid=%d device=%r device_tag=%r",
            device_uid,
            device,
            device_tag,
        )

    payload = await request.body()
    full_records, trailing_bytes = divmod(len(payload), RECORD_BYTES)
    LOGGER.debug(
        "Commit request received: device_uid=%d device=%r payload_bytes=%d full_records=%d trailing_bytes=%d",
        device_uid,
        resolved_device,
        len(payload),
        full_records,
        trailing_bytes,
    )
    if trailing_bytes:
        LOGGER.warning(
            "Commit payload has trailing bytes that will be ignored: device_uid=%d device=%r trailing_bytes=%d",
            device_uid,
            resolved_device,
            trailing_bytes,
        )

    accepted_records: list[CANFrameRecord] = []
    decode_failures = 0
    parse_failures = 0
    for index in range(full_records):
        offset = index * RECORD_BYTES
        boxed = payload[offset : offset + RECORD_BYTES]
        unboxed = unbox(boxed)
        if isinstance(unboxed, UnboxError):
            decode_failures += 1
            LOGGER.error(
                "Commit record decode failed: device_uid=%d device=%r index=%d error=%s",
                device_uid,
                resolved_device,
                index,
                unboxed.name,
            )
            continue

        record = _parse_unboxed_commit_record(unboxed)
        if record is None:
            parse_failures += 1
            LOGGER.error(
                "Commit record parse failed after decode: device_uid=%d device=%r index=%d",
                device_uid,
                resolved_device,
                index,
            )
            continue

        accepted_records.append(record)
        LOGGER.debug(
            "Commit record accepted: device_uid=%d device=%r index=%d seqno=%d boot_id=%d "
            "can_id_with_flags=%d can_id=%d extended=%s rtr=%s error=%s data_len=%d",
            device_uid,
            resolved_device,
            index,
            record.seqno,
            record.boot_id,
            record.frame.can_id_with_flags,
            record.frame.can_id,
            record.frame.extended,
            record.frame.rtr,
            record.frame.error,
            len(bytes(record.frame.data)),
        )

    try:
        last_seqno = database.commit(device_uid=device_uid, device=resolved_device, records=accepted_records)
    except Exception:
        LOGGER.critical(
            "Unexpected commit exception: device_uid=%d device=%r",
            device_uid,
            resolved_device,
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="internal server error",
        )

    has_partial_failures = decode_failures + parse_failures > 0
    response_status = status.HTTP_207_MULTI_STATUS if has_partial_failures else status.HTTP_200_OK
    body_lines = [str(last_seqno)]
    if has_partial_failures:
        body_lines.append(
            f"accepted={len(accepted_records)} failed_decode={decode_failures} failed_parse={parse_failures}"
        )

    LOGGER.info(
        "Commit request processed: device_uid=%d device=%r status_code=%d last_seqno=%d "
        "accepted=%d failed_decode=%d failed_parse=%d full_records=%d trailing_bytes=%d",
        device_uid,
        resolved_device,
        response_status,
        last_seqno,
        len(accepted_records),
        decode_failures,
        parse_failures,
        full_records,
        trailing_bytes,
    )
    return PlainTextResponse(content="\n".join(body_lines), status_code=response_status)


@router.get(
    "/cf3d/api/v1/devices",
    response_model=DevicesResponse,
    tags=["query"],
    summary="List known devices",
    description="Returns all known devices currently present in the database.",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "application/json": {
                    "example": {
                        "devices": [
                            {"device": "alpha", "last_heard_ts": 1704067200, "last_uid": 123},
                            {"device": "beta", "last_heard_ts": 1704067201, "last_uid": 456},
                        ]
                    }
                }
            },
        },
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
def get_devices(
    database: Annotated[Database, Depends(get_database)],
) -> DevicesResponse:
    try:
        devices = [_serialize_device_info(item) for item in database.get_devices()]
    except Exception:
        LOGGER.critical("Unexpected exception while listing devices", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="internal server error")

    LOGGER.info("Devices query completed: count=%d", len(devices))
    return DevicesResponse(devices=devices)


@router.get(
    "/cf3d/api/v1/boots",
    response_model=BootsResponse,
    tags=["query"],
    summary="Query boot ranges for a device",
    description=(
        "Returns boot ranges for the specified device. "
        "Commit timestamps are filtered using overlap semantics between earliest/latest bounds and boot commit span."
    ),
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "application/json": {
                    "example": {
                        "device": "alpha",
                        "boots": [
                            {
                                "boot_id": 100,
                                "first_record": {
                                    "hw_ts_us": 10,
                                    "boot_id": 100,
                                    "seqno": 1,
                                    "commit_ts": 1704067200,
                                    "frame": {
                                        "can_id": 291,
                                        "extended": False,
                                        "rtr": False,
                                        "error": False,
                                        "data_hex": "01",
                                    },
                                },
                                "last_record": {
                                    "hw_ts_us": 20,
                                    "boot_id": 100,
                                    "seqno": 2,
                                    "commit_ts": 1704067201,
                                    "frame": {
                                        "can_id": 291,
                                        "extended": False,
                                        "rtr": False,
                                        "error": False,
                                        "data_hex": "02",
                                    },
                                },
                            }
                        ],
                    }
                }
            },
        },
        422: {"model": ErrorResponse, "description": "Validation error"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
def get_boots(
    device: Annotated[str, Query(min_length=1, description="Device identifier")],
    earliest_commit: Annotated[datetime | None, Query(description="Lower commit-time bound (ISO-8601)")] = None,
    latest_commit: Annotated[datetime | None, Query(description="Upper commit-time bound (ISO-8601)")] = None,
    database: Database = Depends(get_database),
) -> BootsResponse:
    LOGGER.debug(
        "Boots query request: device=%r earliest_commit=%r latest_commit=%r",
        device,
        earliest_commit,
        latest_commit,
    )
    try:
        boots = list(database.get_boots(device, earliest_commit, latest_commit))
    except Exception:
        LOGGER.critical(
            "Unexpected exception while querying boots: device=%r earliest_commit=%r latest_commit=%r",
            device,
            earliest_commit,
            latest_commit,
            exc_info=True,
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="internal server error")

    serialized = [_serialize_boot(boot) for boot in boots]
    if not serialized:
        LOGGER.warning(
            "Boots query returned no results: device=%r earliest_commit=%r latest_commit=%r",
            device,
            earliest_commit,
            latest_commit,
        )
    else:
        LOGGER.info(
            "Boots query completed: device=%r result_count=%d earliest_commit=%r latest_commit=%r",
            device,
            len(serialized),
            earliest_commit,
            latest_commit,
        )
    return BootsResponse(device=device, boots=serialized)


def _query_records_once(
    database: Database,
    device: str,
    boot_ids: list[int],
    seqno_min: int | None,
    seqno_max: int | None,
) -> tuple[list[CANFrameRecordCommitted], int | None]:
    records = list(database.get_records(device=device, boot_ids=boot_ids, seqno_min=seqno_min, seqno_max=seqno_max))
    records.sort(key=lambda record: record.seqno)
    latest_seqno_seen = records[-1].seqno if records else None
    return records, latest_seqno_seen


@router.get(
    "/cf3d/api/v1/records",
    response_model=RecordsResponse,
    tags=["query"],
    summary="Query CAN records",
    description=(
        "Returns records for specified device and boot IDs. "
        "Optional long polling is enabled via wait_timeout_s. "
        "For lossless pagination, set seqno_min to last returned record seqno + 1. "
        "For catch-up behavior that may skip unseen records from truncated pages, "
        "set seqno_min to latest_seqno_seen + 1."
    ),
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "application/json": {
                    "example": {
                        "device": "alpha",
                        "filters": {
                            "boot_ids": [1],
                            "seqno_min": 10,
                            "seqno_max": None,
                            "wait_timeout_s": 0,
                            "limit": 1000,
                        },
                        "latest_seqno_seen": 10,
                        "records": [
                            {
                                "hw_ts_us": 100,
                                "boot_id": 1,
                                "seqno": 10,
                                "commit_ts": 1704067200,
                                "frame": {
                                    "can_id": 291,
                                    "extended": False,
                                    "rtr": False,
                                    "error": False,
                                    "data_hex": "aabb",
                                },
                            }
                        ],
                    }
                }
            },
        },
        422: {"model": ErrorResponse, "description": "Validation error"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def get_records(
    device: Annotated[str, Query(min_length=1, description="Device identifier")],
    boot_id: Annotated[list[int], Query(min_length=1, description="Repeated boot_id parameter")],
    seqno_min: Annotated[int | None, Query(description="Inclusive minimum sequence number")] = None,
    seqno_max: Annotated[int | None, Query(description="Inclusive maximum sequence number")] = None,
    wait_timeout_s: Annotated[
        int,
        Query(ge=0, le=WAIT_MAX_TIMEOUT_S, description="Optional long-poll timeout in seconds"),
    ] = 0,
    limit: Annotated[int, Query(ge=1, le=RECORDS_MAX_LIMIT, description="Page size")] = RECORDS_DEFAULT_LIMIT,
    database: Database = Depends(get_database),
) -> RecordsResponse:
    boot_ids = sorted(set(int(value) for value in boot_id))
    filters = RecordsFilterEcho(
        boot_ids=boot_ids,
        seqno_min=seqno_min,
        seqno_max=seqno_max,
        wait_timeout_s=wait_timeout_s,
        limit=limit,
    )
    LOGGER.debug(
        "Records query request: device=%r boot_ids=%s seqno_min=%r seqno_max=%r wait_timeout_s=%d limit=%d",
        device,
        boot_ids,
        seqno_min,
        seqno_max,
        wait_timeout_s,
        limit,
    )

    if seqno_max is not None and seqno_min is not None and seqno_min > seqno_max:
        LOGGER.warning(
            "Records query has invalid effective range; returning empty: device=%r seqno_min=%d seqno_max=%d",
            device,
            seqno_min,
            seqno_max,
        )
        return RecordsResponse(
            device=device,
            filters=filters,
            latest_seqno_seen=None,
            records=[],
        )

    loop = asyncio.get_running_loop()
    deadline = loop.time() + wait_timeout_s
    poll_count = 0

    try:
        while True:
            poll_count += 1
            matching_records, latest_seqno_seen = _query_records_once(
                database=database,
                device=device,
                boot_ids=boot_ids,
                seqno_min=seqno_min,
                seqno_max=seqno_max,
            )
            total_matched = len(matching_records)
            LOGGER.debug(
                "Records query poll result: device=%r poll_count=%d total_matched=%d latest_seqno_seen=%r",
                device,
                poll_count,
                total_matched,
                latest_seqno_seen,
            )

            if total_matched > 0:
                paged_records = matching_records[:limit]
                serialized = [_serialize_record(record) for record in paged_records]
                LOGGER.info(
                    "Records query completed with data: device=%r poll_count=%d total_matched=%d returned=%d",
                    device,
                    poll_count,
                    total_matched,
                    len(serialized),
                )
                return RecordsResponse(
                    device=device,
                    filters=filters,
                    latest_seqno_seen=latest_seqno_seen,
                    records=serialized,
                )

            now = loop.time()
            if wait_timeout_s <= 0 or now >= deadline:
                timed_out = wait_timeout_s > 0
                if timed_out:
                    LOGGER.warning(
                        "Records query timed out with no matching records: device=%r wait_timeout_s=%d polls=%d",
                        device,
                        wait_timeout_s,
                        poll_count,
                    )
                else:
                    LOGGER.info(
                        "Records query completed with empty snapshot: device=%r poll_count=%d",
                        device,
                        poll_count,
                    )
                return RecordsResponse(
                    device=device,
                    filters=filters,
                    latest_seqno_seen=latest_seqno_seen,
                    records=[],
                )

            sleep_duration = min(WAIT_POLL_INTERVAL_S, deadline - now)
            LOGGER.debug(
                "Records query waiting for new records: device=%r sleep_duration=%.3f poll_count=%d",
                device,
                sleep_duration,
                poll_count,
            )
            await asyncio.sleep(sleep_duration)
    except Exception:
        LOGGER.critical(
            "Unexpected exception while querying records: device=%r boot_ids=%s seqno_min=%r seqno_max=%r",
            device,
            boot_ids,
            seqno_min,
            seqno_max,
            exc_info=True,
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="internal server error")


def _parse_unboxed_commit_record(buf: bytes | bytearray | memoryview) -> CANFrameRecord | None:
    mv = memoryview(buf)
    if len(mv) < 1:
        LOGGER.warning("Unboxed record is empty")
        return None

    version = int(mv[0])
    if version != 0:
        LOGGER.warning("Unsupported unboxed record version=%d", version)
        return None
    if len(mv) < 41:
        LOGGER.warning("Unboxed record too short for v0 header: len=%d", len(mv))
        return None

    boot_id, seqno, timestamp_us = struct.unpack_from("<QQQ", mv, 8)
    (can_id_with_flags,) = struct.unpack_from("<I", mv, 36)
    data_len = int(mv[40])
    if data_len > 64:
        LOGGER.warning("Unboxed record has invalid CAN payload length: data_len=%d", data_len)
        return None

    total_len = 41 + data_len
    if len(mv) < total_len:
        LOGGER.warning("Unboxed record truncated: len=%d needed=%d", len(mv), total_len)
        return None

    data = bytes(mv[41:total_len])
    return CANFrameRecord(
        hw_ts_us=int(timestamp_us),
        boot_id=int(boot_id),
        seqno=int(seqno),
        frame=CANFrame(can_id_with_flags=int(can_id_with_flags), data=data),
    )


def _pack_unboxed_commit_record_v0(record: CANFrameRecord) -> bytes:
    data = bytes(record.frame.data)
    if len(data) > 64:
        raise ValueError(f"CAN payload too long for v0 record: {len(data)} > 64")

    out = bytearray(USER_DATA_BYTES)
    out[0] = 0
    struct.pack_into("<QQQ", out, 8, record.boot_id, record.seqno, record.hw_ts_us)
    struct.pack_into("<I", out, 36, record.frame.can_id_with_flags)
    out[40] = len(data)
    out[41 : 41 + len(data)] = data
    return bytes(out)


def _close_database(database: Database) -> None:
    close_method = getattr(database, "close", None)
    if not callable(close_method):
        LOGGER.debug("Database does not expose close(); skipping shutdown close for %s", type(database).__name__)
        return
    try:
        close_method()
        LOGGER.info("Database closed cleanly during app shutdown: type=%s", type(database).__name__)
    except Exception:
        LOGGER.error("Failed to close database during app shutdown: type=%s", type(database).__name__, exc_info=True)


def create_app(database: Database) -> FastAPI:
    @asynccontextmanager
    async def _lifespan(_app: FastAPI):
        try:
            yield
        finally:
            LOGGER.info("REST API app shutdown event received")
            _close_database(database)

    created_app = FastAPI(lifespan=_lifespan)
    created_app.state.database = database
    created_app.include_router(router)

    LOGGER.info("REST API app created with database backend=%s", type(database).__name__)
    return created_app


def _import_test_client_class() -> type["TestClient"]:
    LOGGER.debug("Importing FastAPI TestClient for REST API unit tests")
    try:
        from fastapi.testclient import TestClient as imported_test_client
    except Exception as ex:
        LOGGER.critical("Failed to import FastAPI TestClient", exc_info=True)
        raise RuntimeError("Running REST API tests requires optional dependency 'httpx'.") from ex
    return imported_test_client


class _FakeDatabase(Database):
    def __init__(self, ack_seqno: int = 0) -> None:
        self._ack_seqno = ack_seqno
        self.commits: list[tuple[int, str, list[CANFrameRecord]]] = []
        self.devices: list[DeviceInfo] = []
        self.boots_by_device: dict[str, list[Boot]] = {}
        self.records_by_device: dict[str, list[CANFrameRecordCommitted]] = {}
        self.records_script_by_device: dict[str, list[list[CANFrameRecordCommitted]]] = {}
        self.fail_methods: set[str] = set()
        self.last_get_boots_args: tuple[str, datetime | None, datetime | None] | None = None
        self.last_get_records_args: tuple[str, list[int], int | None, int | None] | None = None
        self.get_records_call_count = 0

    def commit(self, device_uid: int, device: str, records: Sequence[CANFrameRecord]) -> int:
        if "commit" in self.fail_methods:
            raise RuntimeError("forced commit failure")
        self.commits.append((device_uid, device, list(records)))
        return self._ack_seqno

    def get_devices(self) -> Iterable[DeviceInfo]:
        if "get_devices" in self.fail_methods:
            raise RuntimeError("forced devices failure")
        return list(self.devices)

    def get_boots(
        self, device: str, earliest_commit: datetime | None, latest_commit: datetime | None
    ) -> Iterable[Boot]:
        if "get_boots" in self.fail_methods:
            raise RuntimeError("forced boots failure")
        self.last_get_boots_args = (device, earliest_commit, latest_commit)
        return list(self.boots_by_device.get(device, []))

    def get_records(
        self, device: str, boot_ids: Iterable[int], seqno_min: int | None, seqno_max: int | None
    ) -> Iterable[CANFrameRecordCommitted]:
        if "get_records" in self.fail_methods:
            raise RuntimeError("forced records failure")
        boot_list = [int(boot_id) for boot_id in boot_ids]
        self.last_get_records_args = (device, boot_list, seqno_min, seqno_max)
        self.get_records_call_count += 1

        scripted = self.records_script_by_device.get(device)
        if scripted:
            source = list(scripted.pop(0))
        else:
            source = list(self.records_by_device.get(device, []))
        return self._filter_records(source, boot_list, seqno_min, seqno_max)

    @staticmethod
    def _filter_records(
        records: list[CANFrameRecordCommitted],
        boot_ids: list[int],
        seqno_min: int | None,
        seqno_max: int | None,
    ) -> list[CANFrameRecordCommitted]:
        if seqno_min is not None and seqno_max is not None and seqno_min > seqno_max:
            return []

        boot_id_set = set(int(boot_id) for boot_id in boot_ids)
        out: list[CANFrameRecordCommitted] = []
        for record in records:
            if record.boot_id not in boot_id_set:
                continue
            if seqno_min is not None and record.seqno < seqno_min:
                continue
            if seqno_max is not None and record.seqno > seqno_max:
                continue
            out.append(record)
        out.sort(key=lambda value: value.seqno)
        return out


class _RestAPITests(unittest.TestCase):
    app: ClassVar[FastAPI]
    client: ClassVar["TestClient"]

    @classmethod
    def setUpClass(cls) -> None:
        cls.app = create_app(SqliteDatabase())
        cls.client = _import_test_client_class()(cls.app)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.close()

    def setUp(self) -> None:
        self.database = _FakeDatabase(ack_seqno=321)
        self.app.dependency_overrides[get_database] = lambda: self.database

    def tearDown(self) -> None:
        self.app.dependency_overrides.clear()

    @staticmethod
    def _make_record(
        seqno: int = 1,
        *,
        boot_id: int = 1001,
        hw_ts_us: int = 5_000,
        can_id_with_flags: int = 0x123,
        data: bytes = b"\x01\x02",
    ) -> CANFrameRecord:
        return CANFrameRecord(
            hw_ts_us=hw_ts_us,
            boot_id=boot_id,
            seqno=seqno,
            frame=CANFrame(can_id_with_flags=can_id_with_flags, data=data),
        )

    @staticmethod
    def _make_committed_record(
        seqno: int = 1,
        *,
        boot_id: int = 1001,
        hw_ts_us: int = 5_000,
        can_id_with_flags: int = 0x123,
        data: bytes = b"\x01\x02",
        commit_ts: int = 1704067200,
    ) -> CANFrameRecordCommitted:
        base = _RestAPITests._make_record(
            seqno=seqno,
            boot_id=boot_id,
            hw_ts_us=hw_ts_us,
            can_id_with_flags=can_id_with_flags,
            data=data,
        )
        return CANFrameRecordCommitted(
            hw_ts_us=base.hw_ts_us,
            boot_id=base.boot_id,
            seqno=base.seqno,
            commit_ts=commit_ts,
            frame=base.frame,
        )

    def _post_commit(
        self,
        payload: bytes = b"",
        *,
        device_uid: str = "123",
        device: str | None = "abc",
        device_tag: str | None = None,
    ):
        params: dict[str, str] = {"device_uid": device_uid}
        if device is not None:
            params["device"] = device
        if device_tag is not None:
            params["device_tag"] = device_tag
        return self.client.post(
            "/cf3d/api/v1/commit",
            params=params,
            content=payload,
        )

    @staticmethod
    def _validation_dataset_path() -> Path:
        return Path(__file__).resolve().parent.parent / "data" / "0000000.cf3d"

    @classmethod
    def _load_validation_dataset_blocks(cls) -> list[bytes]:
        dataset_path = cls._validation_dataset_path()
        if not dataset_path.exists():
            raise RuntimeError(f"validation dataset is missing: {dataset_path}")
        payload = dataset_path.read_bytes()
        if len(payload) % RECORD_BYTES != 0:
            raise RuntimeError(f"validation dataset byte size must be divisible by {RECORD_BYTES}: got {len(payload)}")
        return [payload[offset : offset + RECORD_BYTES] for offset in range(0, len(payload), RECORD_BYTES)]

    @classmethod
    def _load_validation_dataset_records(cls) -> tuple[list[bytes], list[CANFrameRecord]]:
        blocks = cls._load_validation_dataset_blocks()
        records: list[CANFrameRecord] = []
        for index, block in enumerate(blocks):
            unboxed = unbox(block)
            if not isinstance(unboxed, bytes):
                raise RuntimeError(f"validation dataset block {index} decode failed: {unboxed!r}")
            parsed = _parse_unboxed_commit_record(unboxed)
            if parsed is None:
                raise RuntimeError(f"validation dataset block {index} parse failed")
            records.append(parsed)
        return blocks, records

    def test_commit_empty_payload_returns_ack(self) -> None:
        response = self._post_commit()
        self.assertEqual(200, response.status_code)
        self.assertEqual("321", response.text)
        self.assertEqual(1, len(self.database.commits))
        committed_uid, committed_device, committed_records = self.database.commits[0]
        self.assertEqual(123, committed_uid)
        self.assertEqual("abc", committed_device)
        self.assertEqual([], committed_records)

    def test_commit_valid_record_is_decoded_and_committed(self) -> None:
        expected = self._make_record(
            seqno=42,
            boot_id=7,
            hw_ts_us=123456,
            can_id_with_flags=0x1ABCDEFF,
            data=b"\xaa\xbb",
        )
        payload = box(_pack_unboxed_commit_record_v0(expected))
        response = self._post_commit(payload, device_uid="0x10", device="vehicle")
        self.assertEqual(200, response.status_code)
        self.assertEqual("321", response.text)

        self.assertEqual(1, len(self.database.commits))
        committed_uid, committed_device, committed_records = self.database.commits[0]
        self.assertEqual(16, committed_uid)
        self.assertEqual("vehicle", committed_device)
        self.assertEqual([expected], committed_records)

    def test_validation_dataset_all_blocks_decode_and_parse(self) -> None:
        blocks = self._load_validation_dataset_blocks()
        self.assertGreater(len(blocks), 0, msg="validation dataset is unexpectedly empty")
        parsed_records: list[CANFrameRecord] = []
        for index, block in enumerate(blocks):
            decoded = unbox(block)
            self.assertIsInstance(decoded, bytes, msg=f"validation dataset block {index} failed to decode: {decoded!r}")
            assert isinstance(decoded, bytes)

            parsed = _parse_unboxed_commit_record(decoded)
            self.assertIsNotNone(parsed, msg=f"validation dataset block {index} failed to parse")
            assert parsed is not None
            self.assertLessEqual(
                len(bytes(parsed.frame.data)),
                64,
                msg=f"validation dataset block {index} has invalid payload length",
            )
            parsed_records.append(parsed)

        expected_seqnos = list(range(len(parsed_records)))
        self.assertEqual(expected_seqnos, [record.seqno for record in parsed_records])
        self.assertEqual({0, 1, 2}, {record.boot_id for record in parsed_records})

    def test_validation_dataset_commit_and_retrieve_with_real_database(self) -> None:
        blocks, expected_records = self._load_validation_dataset_records()
        payload = b"".join(blocks)
        expected_records = sorted(expected_records, key=lambda record: record.seqno)
        expected_seqnos = [record.seqno for record in expected_records]
        expected_latest_seqno = expected_seqnos[-1]

        with _import_test_client_class()(create_app(SqliteDatabase())) as local_client:
            commit_response = local_client.post(
                "/cf3d/api/v1/commit",
                params={"device_uid": "0x123", "device": "validation-dataset"},
                content=payload,
            )
            self.assertEqual(200, commit_response.status_code)
            self.assertEqual(str(expected_latest_seqno), commit_response.text.splitlines()[0])

            tags_response = local_client.get("/cf3d/api/v1/devices")
            self.assertEqual(200, tags_response.status_code)
            self.assertIn("validation-dataset", [item["device"] for item in tags_response.json()["devices"]])

            boots_response = local_client.get("/cf3d/api/v1/boots", params={"device": "validation-dataset"})
            self.assertEqual(200, boots_response.status_code)
            boots_body = boots_response.json()
            boot_ids = sorted(int(item["boot_id"]) for item in boots_body["boots"])
            self.assertEqual([0, 1, 2], boot_ids)
            self.assertTrue(all(int(item["first_record"]["commit_ts"]) > 0 for item in boots_body["boots"]))
            self.assertTrue(all(int(item["last_record"]["commit_ts"]) > 0 for item in boots_body["boots"]))

            query_params: list[tuple[str, str | int | float | bool | None]] = [
                ("device", "validation-dataset"),
                ("limit", "10000"),
            ]
            query_params.extend(("boot_id", str(boot_id)) for boot_id in boot_ids)
            records_response = local_client.get("/cf3d/api/v1/records", params=query_params)
            self.assertEqual(200, records_response.status_code)
            records_body = records_response.json()

            returned_records = records_body["records"]
            self.assertEqual(len(expected_records), len(returned_records))
            self.assertEqual(expected_seqnos, [int(item["seqno"]) for item in returned_records])
            self.assertTrue(all(int(item["commit_ts"]) > 0 for item in returned_records))

            first_expected = expected_records[0]
            last_expected = expected_records[-1]
            first_frame = returned_records[0]["frame"]
            last_frame = returned_records[-1]["frame"]
            self.assertEqual(first_expected.frame.can_id, int(first_frame["can_id"]))
            self.assertEqual(first_expected.frame.extended, bool(first_frame["extended"]))
            self.assertEqual(first_expected.frame.rtr, bool(first_frame["rtr"]))
            self.assertEqual(first_expected.frame.error, bool(first_frame["error"]))
            self.assertEqual(bytes(first_expected.frame.data).hex(), first_frame["data_hex"])
            self.assertEqual(last_expected.frame.can_id, int(last_frame["can_id"]))
            self.assertEqual(last_expected.frame.extended, bool(last_frame["extended"]))
            self.assertEqual(last_expected.frame.rtr, bool(last_frame["rtr"]))
            self.assertEqual(last_expected.frame.error, bool(last_frame["error"]))
            self.assertEqual(bytes(last_expected.frame.data).hex(), last_frame["data_hex"])

    def test_commit_partial_decode_failure_returns_207_with_ack_first_line(self) -> None:
        valid = self._make_record(seqno=5)
        payload = box(_pack_unboxed_commit_record_v0(valid)) + (b"\x00" * RECORD_BYTES)
        response = self._post_commit(payload)
        self.assertEqual(207, response.status_code)
        lines = response.text.splitlines()
        self.assertGreaterEqual(len(lines), 1)
        self.assertEqual("321", lines[0])

        self.assertEqual(1, len(self.database.commits))
        _, _, committed_records = self.database.commits[0]
        self.assertEqual([valid], committed_records)

    def test_commit_partial_parse_failure_returns_207_with_ack_first_line(self) -> None:
        malformed_unboxed = bytearray(USER_DATA_BYTES)
        malformed_unboxed[0] = 99  # Unsupported version.
        payload = box(bytes(malformed_unboxed))
        response = self._post_commit(payload)
        self.assertEqual(207, response.status_code)
        lines = response.text.splitlines()
        self.assertGreaterEqual(len(lines), 1)
        self.assertEqual("321", lines[0])

        self.assertEqual(1, len(self.database.commits))
        _, _, committed_records = self.database.commits[0]
        self.assertEqual([], committed_records)

    def test_commit_truncates_trailing_bytes_without_partial_status(self) -> None:
        valid = self._make_record(seqno=8)
        payload = box(_pack_unboxed_commit_record_v0(valid)) + b"trailing"
        response = self._post_commit(payload)
        self.assertEqual(200, response.status_code)
        self.assertEqual("321", response.text)

        self.assertEqual(1, len(self.database.commits))
        _, _, committed_records = self.database.commits[0]
        self.assertEqual([valid], committed_records)

    def test_commit_all_invalid_records_returns_207_and_empty_commit(self) -> None:
        payload = (b"\x00" * RECORD_BYTES) + (b"\x00" * RECORD_BYTES)
        response = self._post_commit(payload)
        self.assertEqual(207, response.status_code)
        lines = response.text.splitlines()
        self.assertGreaterEqual(len(lines), 1)
        self.assertEqual("321", lines[0])

        self.assertEqual(1, len(self.database.commits))
        _, _, committed_records = self.database.commits[0]
        self.assertEqual([], committed_records)

    def test_parse_unboxed_commit_record_v0_success(self) -> None:
        expected = self._make_record(
            seqno=777,
            boot_id=17,
            hw_ts_us=987654321,
            can_id_with_flags=0x123,
            data=b"\x11\x22\x33",
        )
        parsed = _parse_unboxed_commit_record(_pack_unboxed_commit_record_v0(expected))
        self.assertEqual(expected, parsed)

    def test_pack_and_parse_unboxed_commit_record_preserves_flags(self) -> None:
        flagged = self._make_record(
            seqno=778,
            boot_id=18,
            hw_ts_us=123456789,
            can_id_with_flags=(CAN_EFF_FLAG | CAN_RTR_FLAG | CAN_ERR_FLAG | 0x12),
            data=b"\x44\x55",
        )
        parsed = _parse_unboxed_commit_record(_pack_unboxed_commit_record_v0(flagged))
        self.assertEqual(flagged, parsed)
        assert parsed is not None
        self.assertEqual(0x12, parsed.frame.can_id)
        self.assertTrue(parsed.frame.extended)
        self.assertTrue(parsed.frame.rtr)
        self.assertTrue(parsed.frame.error)

    def test_parse_unboxed_commit_record_rejects_unsupported_version(self) -> None:
        payload = bytearray(USER_DATA_BYTES)
        payload[0] = 1
        self.assertIsNone(_parse_unboxed_commit_record(payload))

    def test_parse_unboxed_commit_record_rejects_too_short_buffer(self) -> None:
        self.assertIsNone(_parse_unboxed_commit_record(b"\x00" * 40))

    def test_parse_unboxed_commit_record_rejects_data_length_over_64(self) -> None:
        payload = bytearray(200)
        payload[0] = 0
        payload[40] = 65
        self.assertIsNone(_parse_unboxed_commit_record(payload))

    def test_parse_unboxed_commit_record_rejects_truncated_payload(self) -> None:
        payload = bytearray(50)
        payload[0] = 0
        payload[40] = 32  # Needs at least 73 bytes.
        self.assertIsNone(_parse_unboxed_commit_record(payload))

    def test_device_uid_auto_radix_detection(self) -> None:
        committed_uids: list[int] = []
        for value in ("16", "0x10", "0o20", "0b10000"):
            with self.subTest(device_uid=value):
                response = self._post_commit(device_uid=value)
                self.assertEqual(200, response.status_code)
                self.assertEqual("321", response.text)
                committed_uids.append(self.database.commits[-1][0])
        self.assertEqual([16, 16, 16, 16], committed_uids)

    def test_missing_device_uid_is_rejected(self) -> None:
        response = self.client.post("/cf3d/api/v1/commit", params={"device": "abc"})
        self.assertEqual(422, response.status_code)

    def test_missing_device_is_rejected(self) -> None:
        response = self.client.post("/cf3d/api/v1/commit", params={"device_uid": "123"})
        self.assertEqual(422, response.status_code)

    def test_commit_accepts_deprecated_device_tag_alias(self) -> None:
        response = self._post_commit(device=None, device_tag="legacy-device")
        self.assertEqual(200, response.status_code)
        self.assertEqual("321", response.text)
        self.assertEqual(1, len(self.database.commits))
        _, committed_device, _ = self.database.commits[0]
        self.assertEqual("legacy-device", committed_device)

    def test_commit_prefers_device_when_alias_conflicts(self) -> None:
        with self.assertLogs(__name__, level="WARNING") as captured:
            response = self._post_commit(device="new-device", device_tag="old-device")
        self.assertEqual(200, response.status_code)
        self.assertEqual("321", response.text)
        self.assertTrue(any("preferring 'device'" in line for line in captured.output))
        self.assertEqual(1, len(self.database.commits))
        _, committed_device, _ = self.database.commits[0]
        self.assertEqual("new-device", committed_device)

    def test_invalid_device_uid_is_rejected(self) -> None:
        response = self.client.post(
            "/cf3d/api/v1/commit",
            params={"device_uid": "xyz", "device": "abc"},
        )
        self.assertEqual(422, response.status_code)

    def test_empty_device_is_rejected(self) -> None:
        response = self.client.post(
            "/cf3d/api/v1/commit",
            params={"device_uid": "123", "device": ""},
        )
        self.assertEqual(422, response.status_code)

    def test_get_devices_returns_json(self) -> None:
        self.database.devices = [
            DeviceInfo(device="alpha", last_heard_ts=1704067200, last_uid=123),
            DeviceInfo(device="beta", last_heard_ts=1704067201, last_uid=456),
        ]
        response = self.client.get("/cf3d/api/v1/devices")
        self.assertEqual(200, response.status_code)
        self.assertEqual(
            {
                "devices": [
                    {"device": "alpha", "last_heard_ts": 1704067200, "last_uid": 123},
                    {"device": "beta", "last_heard_ts": 1704067201, "last_uid": 456},
                ]
            },
            response.json(),
        )

    def test_get_devices_internal_error_returns_500(self) -> None:
        self.database.fail_methods.add("get_devices")
        response = self.client.get("/cf3d/api/v1/devices")
        self.assertEqual(500, response.status_code)

    def test_get_boots_returns_json(self) -> None:
        first = self._make_committed_record(
            seqno=10,
            boot_id=7,
            hw_ts_us=1,
            can_id_with_flags=0x100,
            data=b"\xaa",
            commit_ts=1704067200,
        )
        last = self._make_committed_record(
            seqno=20,
            boot_id=7,
            hw_ts_us=2,
            can_id_with_flags=0x200,
            data=b"\xbb",
            commit_ts=1704067201,
        )
        self.database.boots_by_device["alpha"] = [Boot(boot_id=7, first_record=first, last_record=last)]

        response = self.client.get("/cf3d/api/v1/boots", params={"device": "alpha"})
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual("alpha", body["device"])
        self.assertEqual(1, len(body["boots"]))
        self.assertEqual("aa", body["boots"][0]["first_record"]["frame"]["data_hex"])
        self.assertEqual("bb", body["boots"][0]["last_record"]["frame"]["data_hex"])
        self.assertEqual(1704067200, int(body["boots"][0]["first_record"]["commit_ts"]))
        self.assertEqual(1704067201, int(body["boots"][0]["last_record"]["commit_ts"]))
        self.assertFalse(body["boots"][0]["first_record"]["frame"]["extended"])
        self.assertFalse(body["boots"][0]["first_record"]["frame"]["rtr"])
        self.assertFalse(body["boots"][0]["first_record"]["frame"]["error"])
        self.assertFalse(body["boots"][0]["last_record"]["frame"]["extended"])
        self.assertFalse(body["boots"][0]["last_record"]["frame"]["rtr"])
        self.assertFalse(body["boots"][0]["last_record"]["frame"]["error"])

    def test_get_boots_unknown_tag_returns_empty_list(self) -> None:
        response = self.client.get("/cf3d/api/v1/boots", params={"device": "unknown"})
        self.assertEqual(200, response.status_code)
        self.assertEqual({"device": "unknown", "boots": []}, response.json())

    def test_get_boots_passes_parsed_datetime_filters(self) -> None:
        response = self.client.get(
            "/cf3d/api/v1/boots",
            params={
                "device": "alpha",
                "earliest_commit": "2024-01-01T00:00:00Z",
                "latest_commit": "2024-01-01T01:00:00Z",
            },
        )
        self.assertEqual(200, response.status_code)
        self.assertIsNotNone(self.database.last_get_boots_args)
        assert self.database.last_get_boots_args is not None
        _, earliest, latest = self.database.last_get_boots_args
        self.assertIsNotNone(earliest)
        self.assertIsNotNone(latest)
        assert earliest is not None and latest is not None
        self.assertEqual(int(datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc).timestamp()), int(earliest.timestamp()))
        self.assertEqual(int(datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc).timestamp()), int(latest.timestamp()))

    def test_get_boots_invalid_datetime_rejected(self) -> None:
        response = self.client.get("/cf3d/api/v1/boots", params={"device": "alpha", "earliest_commit": "bad"})
        self.assertEqual(422, response.status_code)

    def test_get_records_requires_boot_id(self) -> None:
        response = self.client.get("/cf3d/api/v1/records", params={"device": "alpha"})
        self.assertEqual(422, response.status_code)

    def test_get_records_returns_limited_first_page_results(self) -> None:
        self.database.records_by_device["alpha"] = [
            self._make_committed_record(seqno=1, boot_id=1, data=b"\x01", commit_ts=1704067200),
            self._make_committed_record(seqno=2, boot_id=1, data=b"\x02", commit_ts=1704067201),
            self._make_committed_record(seqno=3, boot_id=1, data=b"\x03", commit_ts=1704067202),
        ]

        response = self.client.get(
            "/cf3d/api/v1/records",
            params={"device": "alpha", "boot_id": 1, "limit": 2},
        )
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual(2, len(body["records"]))
        self.assertEqual([1, 2], [item["seqno"] for item in body["records"]])
        self.assertTrue(all("extended" in item["frame"] for item in body["records"]))
        self.assertTrue(all("rtr" in item["frame"] for item in body["records"]))
        self.assertTrue(all("error" in item["frame"] for item in body["records"]))
        self.assertTrue(all(item["frame"]["extended"] is False for item in body["records"]))
        self.assertTrue(all(item["frame"]["rtr"] is False for item in body["records"]))
        self.assertTrue(all(item["frame"]["error"] is False for item in body["records"]))
        self.assertEqual([1704067200, 1704067201], [int(item["commit_ts"]) for item in body["records"]])
        self.assertNotIn("offset", body["filters"])
        self.assertNotIn("total_matched", body)
        self.assertNotIn("timed_out", body)

    def test_get_records_serializes_flags_for_flagged_frame(self) -> None:
        flagged_id = CAN_EFF_FLAG | CAN_RTR_FLAG | CAN_ERR_FLAG | 0x1ABC
        self.database.records_by_device["alpha"] = [
            self._make_committed_record(
                seqno=10,
                boot_id=1,
                can_id_with_flags=flagged_id,
                data=b"\xfa\xce",
                commit_ts=1704067200,
            ),
        ]
        response = self.client.get(
            "/cf3d/api/v1/records",
            params={"device": "alpha", "boot_id": 1},
        )
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual(1, len(body["records"]))
        frame = body["records"][0]["frame"]
        self.assertEqual(0x1ABC, frame["can_id"])
        self.assertTrue(frame["extended"])
        self.assertTrue(frame["rtr"])
        self.assertTrue(frame["error"])
        self.assertEqual("face", frame["data_hex"])
        self.assertEqual(1704067200, int(body["records"][0]["commit_ts"]))

    def test_get_records_unknown_tag_returns_empty(self) -> None:
        response = self.client.get("/cf3d/api/v1/records", params={"device": "unknown", "boot_id": 1})
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual([], body["records"])
        self.assertNotIn("total_matched", body)
        self.assertNotIn("timed_out", body)

    def test_get_records_invalid_range_returns_empty_200(self) -> None:
        response = self.client.get(
            "/cf3d/api/v1/records",
            params={"device": "alpha", "boot_id": 1, "seqno_min": 10, "seqno_max": 1},
        )
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual([], body["records"])
        self.assertNotIn("total_matched", body)
        self.assertNotIn("timed_out", body)

    def test_get_records_seqno_min_filters_results(self) -> None:
        self.database.records_by_device["alpha"] = [
            self._make_committed_record(seqno=1, boot_id=1, commit_ts=1704067200),
            self._make_committed_record(seqno=2, boot_id=1, commit_ts=1704067201),
            self._make_committed_record(seqno=3, boot_id=1, commit_ts=1704067202),
        ]
        response = self.client.get(
            "/cf3d/api/v1/records",
            params={"device": "alpha", "boot_id": 1, "seqno_min": 3},
        )
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual([3], [item["seqno"] for item in body["records"]])

    def test_get_records_seqno_min_resumes_after_last_returned_record(self) -> None:
        self.database.records_by_device["alpha"] = [
            self._make_committed_record(seqno=1, boot_id=1, commit_ts=1704067200),
            self._make_committed_record(seqno=2, boot_id=1, commit_ts=1704067201),
            self._make_committed_record(seqno=3, boot_id=1, commit_ts=1704067202),
        ]

        first = self.client.get(
            "/cf3d/api/v1/records",
            params={"device": "alpha", "boot_id": 1, "limit": 2},
        )
        self.assertEqual(200, first.status_code)
        first_body = first.json()
        self.assertEqual([1, 2], [item["seqno"] for item in first_body["records"]])

        second = self.client.get(
            "/cf3d/api/v1/records",
            params={"device": "alpha", "boot_id": 1, "seqno_min": 3},
        )
        self.assertEqual(200, second.status_code)
        second_body = second.json()
        self.assertEqual([3], [item["seqno"] for item in second_body["records"]])

    def test_get_records_long_poll_wakes_when_new_data_appears(self) -> None:
        wake_record = self._make_committed_record(seqno=9, boot_id=1, commit_ts=1704067200)
        self.database.records_script_by_device["alpha"] = [[], [wake_record]]

        with patch("nestor.rest_api.WAIT_POLL_INTERVAL_S", 0.01):
            response = self.client.get(
                "/cf3d/api/v1/records",
                params={"device": "alpha", "boot_id": 1, "seqno_min": 9, "wait_timeout_s": 1},
            )
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual([9], [item["seqno"] for item in body["records"]])
        self.assertNotIn("timed_out", body)
        self.assertGreaterEqual(self.database.get_records_call_count, 2)

    def test_get_records_long_poll_timeout_returns_empty_without_timed_out_field(self) -> None:
        self.database.records_script_by_device["alpha"] = [[], [], [], []]

        with patch("nestor.rest_api.WAIT_POLL_INTERVAL_S", 0.01):
            response = self.client.get(
                "/cf3d/api/v1/records",
                params={"device": "alpha", "boot_id": 1, "seqno_min": 101, "wait_timeout_s": 1},
            )
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual([], body["records"])
        self.assertNotIn("timed_out", body)

    def test_get_records_wait_timeout_validation(self) -> None:
        response = self.client.get(
            "/cf3d/api/v1/records",
            params={"device": "alpha", "boot_id": 1, "wait_timeout_s": 31},
        )
        self.assertEqual(422, response.status_code)

    def test_get_records_internal_error_returns_500(self) -> None:
        self.database.fail_methods.add("get_records")
        response = self.client.get("/cf3d/api/v1/records", params={"device": "alpha", "boot_id": 1})
        self.assertEqual(500, response.status_code)


if __name__ == "__main__":
    unittest.main(verbosity=2)
