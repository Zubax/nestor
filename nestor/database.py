from __future__ import annotations

import logging
import sqlite3
import tempfile
import threading
import unittest
import warnings
from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from nestor.model import CANFrame, CANFrameRecord, CANFrameRecordCommitted

LOGGER = logging.getLogger(__name__)

_SQLITE_INT64_MIN = -(1 << 63)
_SQLITE_INT64_MAX = (1 << 63) - 1
_UINT64_MAX = (1 << 64) - 1


@dataclass(frozen=True)
class Boot:
    """
    Models the record range from a device within a given boot ID.
    """

    boot_id: int
    first_record: CANFrameRecordCommitted
    last_record: CANFrameRecordCommitted


@dataclass(frozen=True)
class DeviceInfo:
    device: str
    last_heard_ts: int
    """Unix timestamp when last request seen from this device."""
    last_uid: int


class Database(ABC):
    @abstractmethod
    def commit(self, device_uid: int, device: str, records: Sequence[CANFrameRecord]) -> int:
        """
        Adds the records to the database, skipping those whose seqno is already in the database.
        Returns the latest recorded seqno for this device after this transaction.
        Devices routinely attempt to upload records that are already known so seqno filtering is crucial.
        """
        raise NotImplementedError

    @abstractmethod
    def get_devices(self) -> Iterable[DeviceInfo]:
        """All devices ever seen, with per-device metadata."""
        raise NotImplementedError

    @abstractmethod
    def get_boots(
        self, device: str, earliest_commit: datetime | None, latest_commit: datetime | None
    ) -> Iterable[Boot]:
        raise NotImplementedError

    @abstractmethod
    def get_records(
        self, device: str, boot_ids: Iterable[int], seqno_min: int | None, seqno_max: int | None
    ) -> Iterable[CANFrameRecordCommitted]:
        raise NotImplementedError


@dataclass(frozen=True)
class _StoredFrameRow:
    device_uid: int
    hw_ts_us: int
    boot_id: int
    can_id_with_flags: int
    data: bytes


class SqliteDatabase(Database):
    """
    SQLite-backed implementation tuned for high-volume append+query workloads.

    Design notes:
    - Idempotency is enforced by UNIQUE(device_id, seqno); duplicate uploads are normal.
    - `devices.last_seqno` is a transactional cache used to return cumulative ACK quickly
      without aggregating over the very large `can_frames` table on each commit.
    - `devices.last_device_uid` is operational metadata, not correctness state:
      it lets us detect/log device-name->UID changes and track heartbeat-style empty commits cheaply.
      Core dedup/ACK behavior does not depend on this column.
    """

    _BUSY_TIMEOUT_MS = 5000
    # SQLite limits host parameters per statement. Keep chunks below common defaults for portability.
    _SQL_VARIABLE_CHUNK = 900

    def __init__(self, filename: str | None = None) -> None:
        self._filename = filename if filename is not None else ":memory:"
        self._lock = threading.RLock()
        LOGGER.info("Opening SQLite database at %s", self._filename)
        self._connection = sqlite3.connect(self._filename, check_same_thread=False)
        self._configure_connection()
        self._initialize_schema()

    def close(self) -> None:
        with self._lock:
            LOGGER.info("Closing SQLite database at %s", self._filename)
            self._connection.close()

    def commit(self, device_uid: int, device: str, records: Sequence[CANFrameRecord]) -> int:
        if not device:
            raise ValueError("device must be a non-empty string")

        with self._lock:
            LOGGER.debug(
                "Starting commit transaction: device_uid=%d device=%r records=%d",
                device_uid,
                device,
                len(records),
            )
            try:
                device_uid_db = _device_uid_to_sqlite_int64(device_uid)
                cursor = self._connection.cursor()
                cursor.execute("BEGIN IMMEDIATE")
                device_id, previous_last_seqno, previous_device_uid = self._ensure_device(
                    cursor=cursor,
                    device_uid=device_uid_db,
                    device=device,
                )
                if previous_device_uid != device_uid:
                    # This does not fail the commit. The device name remains the primary identity on the server side,
                    # while UID drift is surfaced as an anomaly for operators.
                    LOGGER.warning(
                        "Device %r changed UID from %d to %d",
                        device,
                        previous_device_uid,
                        device_uid,
                    )

                if not records:
                    # Empty commits are heartbeat-like uploads from devices that currently have no new frames.
                    # We still refresh metadata for observability and return cached cumulative ACK.
                    cursor.execute(
                        """
                        UPDATE devices
                        SET
                            last_device_uid=?,
                            last_heard_ts=CAST(strftime('%s', 'now') AS INTEGER)
                        WHERE device_id=?
                        """,
                        (device_uid_db, device_id),
                    )
                    cursor.execute("COMMIT")
                    LOGGER.info(
                        "Commit completed with empty payload: device=%r last_seqno=%d last_heard_ts_refreshed=true",
                        device,
                        previous_last_seqno,
                    )
                    return previous_last_seqno

                seq_to_records: dict[int, list[CANFrameRecord]] = {}
                rows_to_insert: list[tuple[int, int, int, int, int, int, bytes]] = []
                max_incoming_seqno = previous_last_seqno
                for record in records:
                    normalized_data = self._normalize_data(record.frame.data)
                    rows_to_insert.append(
                        (
                            device_uid_db,
                            device_id,
                            record.hw_ts_us,
                            record.boot_id,
                            record.seqno,
                            record.frame.can_id_with_flags,
                            normalized_data,
                        )
                    )
                    seq_to_records.setdefault(record.seqno, []).append(record)
                    if record.seqno > max_incoming_seqno:
                        max_incoming_seqno = record.seqno

                duplicate_seqnos = [seqno for seqno, by_seq in seq_to_records.items() if len(by_seq) > 1]
                if duplicate_seqnos:
                    LOGGER.warning(
                        "Commit payload for device=%r contains duplicate seqnos: %s",
                        device,
                        sorted(duplicate_seqnos),
                    )

                before_changes = self._connection.total_changes
                cursor.executemany(
                    """
                    INSERT OR IGNORE INTO can_frames
                    (
                        device_uid,
                        device_id,
                        hw_ts_us,
                        boot_id,
                        seqno,
                        can_id_with_flags,
                        data
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows_to_insert,
                )
                inserted_count = self._connection.total_changes - before_changes
                LOGGER.debug(
                    "INSERT OR IGNORE finished: device=%r attempted=%d inserted=%d deduplicated=%d",
                    device,
                    len(rows_to_insert),
                    inserted_count,
                    len(rows_to_insert) - inserted_count,
                )

                stored_rows = self._get_rows_by_seqno(
                    cursor=cursor,
                    device_id=device_id,
                    seqnos=seq_to_records.keys(),
                )
                mismatch_count = 0
                for seqno, records_for_seq in seq_to_records.items():
                    stored = stored_rows.get(seqno)
                    if stored is None:
                        LOGGER.error(
                            "Commit verification failed: missing stored row for device=%r seqno=%d",
                            device,
                            seqno,
                        )
                        raise RuntimeError(f"internal consistency error: missing stored row for seqno={seqno}")
                    for record in records_for_seq:
                        if not self._record_matches_row(device_uid=device_uid, record=record, row=stored):
                            mismatch_count += 1
                            LOGGER.warning(
                                "Duplicate seqno mismatch for device=%r seqno=%d "
                                "stored(device_uid=%d boot_id=%d hw_ts_us=%d can_id_with_flags=%d data=%s) "
                                "incoming(device_uid=%d boot_id=%d hw_ts_us=%d can_id_with_flags=%d data=%s)",
                                device,
                                seqno,
                                stored.device_uid,
                                stored.boot_id,
                                stored.hw_ts_us,
                                stored.can_id_with_flags,
                                stored.data.hex(),
                                device_uid,
                                record.boot_id,
                                record.hw_ts_us,
                                record.frame.can_id_with_flags,
                                bytes(record.frame.data).hex(),
                            )

                cursor.execute(
                    """
                    UPDATE devices
                    SET
                        last_seqno=CASE WHEN last_seqno < ? THEN ? ELSE last_seqno END,
                        last_device_uid=?,
                        last_heard_ts=CAST(strftime('%s', 'now') AS INTEGER)
                    WHERE device_id=?
                    """,
                    (max_incoming_seqno, max_incoming_seqno, device_uid_db, device_id),
                )
                # Read back cached ACK inside the same transaction to guarantee the returned value reflects
                # committed state seen by future commits.
                cursor.execute(
                    "SELECT last_seqno, last_heard_ts FROM devices WHERE device_id=?",
                    (device_id,),
                )
                row = cursor.fetchone()
                if row is None:
                    LOGGER.error(
                        "Unable to read updated last_seqno for device=%r device_id=%d",
                        device,
                        device_id,
                    )
                    raise RuntimeError("failed to fetch updated last_seqno")
                last_seqno = int(row[0])
                last_heard_ts = int(row[1])
                cursor.execute("COMMIT")

                LOGGER.info(
                    "Commit transaction done: device=%r device_uid=%d inserted=%d mismatches=%d "
                    "last_seqno=%d last_heard_ts=%d",
                    device,
                    device_uid,
                    inserted_count,
                    mismatch_count,
                    last_seqno,
                    last_heard_ts,
                )
                return last_seqno
            except sqlite3.Error:
                LOGGER.error(
                    "SQLite error while committing records: device=%r device_uid=%d",
                    device,
                    device_uid,
                    exc_info=True,
                )
                self._rollback_quietly()
                raise
            except Exception:
                LOGGER.critical(
                    "Unexpected exception while committing records: device=%r device_uid=%d",
                    device,
                    device_uid,
                    exc_info=True,
                )
                self._rollback_quietly()
                raise

    def get_devices(self) -> Iterable[DeviceInfo]:
        with self._lock:
            LOGGER.debug("Fetching all known devices")
            cursor = self._connection.cursor()
            cursor.execute(
                "SELECT device, last_heard_ts, last_device_uid FROM devices ORDER BY device COLLATE NOCASE ASC"
            )
            result = [
                DeviceInfo(
                    device=str(row[0]),
                    last_heard_ts=int(row[1]),
                    last_uid=_sqlite_int64_to_device_uid(int(row[2])),
                )
                for row in cursor.fetchall()
            ]
            LOGGER.info("Fetched %d devices", len(result))
            return result

    def get_boots(
        self, device: str, earliest_commit: datetime | None, latest_commit: datetime | None
    ) -> Iterable[Boot]:
        with self._lock:
            LOGGER.debug(
                "Fetching boots for device=%r earliest_commit=%r latest_commit=%r",
                device,
                earliest_commit,
                latest_commit,
            )
            cursor = self._connection.cursor()
            device_id = self._get_device_id(cursor=cursor, device=device)
            if device_id is None:
                LOGGER.info("No data for unknown device=%r", device)
                return []

            earliest_ts = None if earliest_commit is None else _datetime_to_epoch_seconds(earliest_commit)
            latest_ts = None if latest_commit is None else _datetime_to_epoch_seconds(latest_commit)

            cursor.execute(
                """
                SELECT
                    grouped.boot_id,
                    first_frame.hw_ts_us,
                    first_frame.boot_id,
                    first_frame.seqno,
                    first_frame.commit_ts,
                    first_frame.can_id_with_flags,
                    first_frame.data,
                    last_frame.hw_ts_us,
                    last_frame.boot_id,
                    last_frame.seqno,
                    last_frame.commit_ts,
                    last_frame.can_id_with_flags,
                    last_frame.data
                FROM
                    (
                        SELECT
                            boot_id,
                            MIN(seqno) AS min_seqno,
                            MAX(seqno) AS max_seqno
                        FROM can_frames
                        WHERE device_id=?
                        GROUP BY boot_id
                        HAVING
                            (? IS NULL OR MIN(commit_ts) <= ?)
                            AND
                            (? IS NULL OR MAX(commit_ts) >= ?)
                    ) AS grouped
                JOIN can_frames AS first_frame
                    ON first_frame.device_id=?
                    AND first_frame.boot_id=grouped.boot_id
                    AND first_frame.seqno=grouped.min_seqno
                JOIN can_frames AS last_frame
                    ON last_frame.device_id=?
                    AND last_frame.boot_id=grouped.boot_id
                    AND last_frame.seqno=grouped.max_seqno
                ORDER BY grouped.boot_id ASC
                """,
                (
                    device_id,
                    latest_ts,
                    latest_ts,
                    earliest_ts,
                    earliest_ts,
                    device_id,
                    device_id,
                ),
            )

            out: list[Boot] = []
            for row in cursor.fetchall():
                first_record = CANFrameRecordCommitted(
                    hw_ts_us=int(row[1]),
                    boot_id=int(row[2]),
                    seqno=int(row[3]),
                    commit_ts=int(row[4]),
                    frame=CANFrame(
                        can_id_with_flags=int(row[5]),
                        data=bytes(row[6]),
                    ),
                )
                last_record = CANFrameRecordCommitted(
                    hw_ts_us=int(row[7]),
                    boot_id=int(row[8]),
                    seqno=int(row[9]),
                    commit_ts=int(row[10]),
                    frame=CANFrame(
                        can_id_with_flags=int(row[11]),
                        data=bytes(row[12]),
                    ),
                )
                out.append(Boot(boot_id=int(row[0]), first_record=first_record, last_record=last_record))
            LOGGER.info(
                "Fetched %d boots for device=%r earliest_commit=%r latest_commit=%r",
                len(out),
                device,
                earliest_commit,
                latest_commit,
            )
            return out

    def get_records(
        self, device: str, boot_ids: Iterable[int], seqno_min: int | None, seqno_max: int | None
    ) -> Iterable[CANFrameRecordCommitted]:
        if seqno_min is not None and seqno_max is not None and seqno_min > seqno_max:
            LOGGER.warning(
                "Requested invalid seqno range for device=%r: seqno_min=%d > seqno_max=%d",
                device,
                seqno_min,
                seqno_max,
            )
            return []

        unique_boot_ids = sorted(set(int(boot_id) for boot_id in boot_ids))
        if not unique_boot_ids:
            LOGGER.info("No boot IDs requested for device=%r", device)
            return []

        with self._lock:
            LOGGER.debug(
                "Fetching records for device=%r boot_ids=%s seqno_min=%r seqno_max=%r",
                device,
                unique_boot_ids,
                seqno_min,
                seqno_max,
            )
            cursor = self._connection.cursor()
            device_id = self._get_device_id(cursor=cursor, device=device)
            if device_id is None:
                LOGGER.info("No data for unknown device=%r", device)
                return []

            result: list[CANFrameRecordCommitted] = []
            for boot_id_chunk in _chunked(unique_boot_ids, self._SQL_VARIABLE_CHUNK):
                placeholders = ",".join("?" for _ in boot_id_chunk)
                query = (
                    "SELECT hw_ts_us, boot_id, seqno, commit_ts, can_id_with_flags, data "
                    "FROM can_frames "
                    f"WHERE device_id=? AND boot_id IN ({placeholders})"
                )
                parameters: list[int] = [device_id, *boot_id_chunk]

                if seqno_min is not None:
                    query += " AND seqno>=?"
                    parameters.append(seqno_min)
                if seqno_max is not None:
                    query += " AND seqno<=?"
                    parameters.append(seqno_max)
                query += " ORDER BY seqno ASC"

                cursor.execute(query, parameters)
                for row in cursor.fetchall():
                    result.append(
                        CANFrameRecordCommitted(
                            hw_ts_us=int(row[0]),
                            boot_id=int(row[1]),
                            seqno=int(row[2]),
                            commit_ts=int(row[3]),
                            frame=CANFrame(
                                can_id_with_flags=int(row[4]),
                                data=bytes(row[5]),
                            ),
                        )
                    )
            result.sort(key=lambda record: record.seqno)
            LOGGER.info(
                "Fetched %d records for device=%r boots=%d seqno_min=%r seqno_max=%r",
                len(result),
                device,
                len(unique_boot_ids),
                seqno_min,
                seqno_max,
            )
            return result

    def _configure_connection(self) -> None:
        with self._lock:
            LOGGER.debug("Configuring SQLite PRAGMA settings for %s", self._filename)
            cursor = self._connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute(f"PRAGMA busy_timeout={self._BUSY_TIMEOUT_MS}")
            cursor.execute("PRAGMA temp_store=MEMORY")
            cursor.execute("PRAGMA synchronous=NORMAL")
            if self._filename != ":memory:":
                cursor.execute("PRAGMA journal_mode=WAL")
                mode_row = cursor.fetchone()
                LOGGER.info(
                    "Enabled WAL mode for %s; journal_mode=%s",
                    self._filename,
                    None if mode_row is None else mode_row[0],
                )
            self._connection.commit()

    def _initialize_schema(self) -> None:
        with self._lock:
            LOGGER.debug("Ensuring SQLite schema is initialized")
            cursor = self._connection.cursor()
            cursor.executescript("""
                -- Device dictionary for deduplication and metadata.
                -- `last_seqno` is a write-through cache to avoid expensive MAX(seqno) lookups on huge tables.
                -- `last_device_uid` is observability state:
                -- it enables cheap detection of device-name->UID drift and reflects heartbeat commits.
                CREATE TABLE IF NOT EXISTS devices
                (
                    device_id        INTEGER PRIMARY KEY,
                    device           TEXT    NOT NULL UNIQUE,
                    last_seqno       INTEGER NOT NULL DEFAULT 0,
                    last_heard_ts    INTEGER NOT NULL DEFAULT (CAST(strftime('%s', 'now') AS INTEGER)),
                    last_device_uid  INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS can_frames
                (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    -- Integer Unix time (seconds) for fast range scans on large tables.
                    -- We keep query inputs as local datetimes and convert them to epoch seconds.
                    commit_ts     INTEGER NOT NULL DEFAULT (CAST(strftime('%s', 'now') AS INTEGER)),
                    device_uid    INTEGER NOT NULL,
                    device_id     INTEGER NOT NULL,
                    hw_ts_us      INTEGER NOT NULL,
                    boot_id       INTEGER NOT NULL,
                    seqno         INTEGER NOT NULL,
                    can_id_with_flags INTEGER NOT NULL,
                    data          BLOB    NOT NULL CHECK (length(data) <= 64),
                    FOREIGN KEY (device_id) REFERENCES devices(device_id),
                    -- Server-side idempotency key for uploader retries.
                    UNIQUE (device_id, seqno)
                );

                -- Boot and seqno-oriented retrieval path.
                CREATE INDEX IF NOT EXISTS can_frames_device_boot_seq
                    ON can_frames (device_id, boot_id, seqno);

                -- Commit-time window filtering path.
                CREATE INDEX IF NOT EXISTS can_frames_device_commit
                    ON can_frames (device_id, commit_ts);

                -- Combined commit-time + boot aggregation path.
                CREATE INDEX IF NOT EXISTS can_frames_device_boot_commit
                    ON can_frames (device_id, boot_id, commit_ts);
                """)
            self._connection.commit()
            LOGGER.info("SQLite schema is ready")

    def _ensure_device(self, cursor: sqlite3.Cursor, device_uid: int, device: str) -> tuple[int, int, int]:
        LOGGER.debug("Ensuring device row exists for %r", device)
        cursor.execute(
            "INSERT OR IGNORE INTO devices (device, last_device_uid) VALUES (?, ?)",
            (device, device_uid),
        )
        cursor.execute(
            "SELECT device_id, last_seqno, last_device_uid FROM devices WHERE device=?",
            (device,),
        )
        row = cursor.fetchone()
        if row is None:
            LOGGER.error("Failed to load/create device row for %r", device)
            raise RuntimeError(f"failed to create or fetch device row for {device!r}")
        resolved_uid = _sqlite_int64_to_device_uid(int(row[2]))
        return int(row[0]), int(row[1]), resolved_uid

    def _get_device_id(self, cursor: sqlite3.Cursor, device: str) -> int | None:
        cursor.execute("SELECT device_id FROM devices WHERE device=?", (device,))
        row = cursor.fetchone()
        if row is None:
            return None
        return int(row[0])

    def _get_rows_by_seqno(
        self, cursor: sqlite3.Cursor, device_id: int, seqnos: Iterable[int]
    ) -> dict[int, _StoredFrameRow]:
        seqno_list = sorted(set(int(seqno) for seqno in seqnos))
        LOGGER.debug("Fetching %d stored rows for verification", len(seqno_list))
        result: dict[int, _StoredFrameRow] = {}
        if not seqno_list:
            return result

        for seqno_chunk in _chunked(seqno_list, self._SQL_VARIABLE_CHUNK):
            placeholders = ",".join("?" for _ in seqno_chunk)
            query = (
                "SELECT seqno, device_uid, hw_ts_us, boot_id, can_id_with_flags, data "
                "FROM can_frames "
                f"WHERE device_id=? AND seqno IN ({placeholders})"
            )
            cursor.execute(query, [device_id, *seqno_chunk])
            for row in cursor.fetchall():
                result[int(row[0])] = _StoredFrameRow(
                    device_uid=_sqlite_int64_to_device_uid(int(row[1])),
                    hw_ts_us=int(row[2]),
                    boot_id=int(row[3]),
                    can_id_with_flags=int(row[4]),
                    data=bytes(row[5]),
                )
        LOGGER.debug("Fetched %d stored rows for verification", len(result))
        return result

    @staticmethod
    def _record_matches_row(device_uid: int, record: CANFrameRecord, row: _StoredFrameRow) -> bool:
        return (
            row.device_uid == device_uid
            and row.hw_ts_us == record.hw_ts_us
            and row.boot_id == record.boot_id
            and row.can_id_with_flags == record.frame.can_id_with_flags
            and row.data == bytes(record.frame.data)
        )

    @staticmethod
    def _normalize_data(data: bytes | bytearray) -> bytes:
        out = bytes(data)
        if len(out) > 64:
            raise ValueError(f"CAN payload too long: {len(out)} > 64")
        return out

    def _rollback_quietly(self) -> None:
        try:
            self._connection.rollback()
            LOGGER.debug("Transaction rollback completed")
        except sqlite3.Error:
            LOGGER.error("Rollback failed", exc_info=True)


def _device_uid_to_sqlite_int64(device_uid: int) -> int:
    value = int(device_uid)
    if value < 0 or value > _UINT64_MAX:
        raise ValueError(f"device_uid must be in uint64 range [0, {_UINT64_MAX}], got {value}")
    if value <= _SQLITE_INT64_MAX:
        return value
    return value - (_UINT64_MAX + 1)


def _sqlite_int64_to_device_uid(value: int) -> int:
    out = int(value)
    if out < _SQLITE_INT64_MIN or out > _SQLITE_INT64_MAX:
        raise ValueError(f"SQLite INTEGER value out of int64 range: {out}")
    if out >= 0:
        return out
    return out + (_UINT64_MAX + 1)


def _chunked(values: Sequence[int], chunk_size: int) -> Iterable[list[int]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    for index in range(0, len(values), chunk_size):
        yield list(values[index : index + chunk_size])


def _datetime_to_epoch_seconds(value: datetime) -> int:
    # Treat naive datetimes as local wall time for API compatibility.
    if value.tzinfo is None:
        return int(value.timestamp())
    return int(value.astimezone().timestamp())


def _make_record(
    seqno: int,
    *,
    boot_id: int = 1,
    hw_ts_us: int | None = None,
    can_id_with_flags: int = 0x123,
    data: bytes = b"\xaa",
) -> CANFrameRecord:
    return CANFrameRecord(
        hw_ts_us=seqno if hw_ts_us is None else hw_ts_us,
        boot_id=boot_id,
        seqno=seqno,
        frame=CANFrame(can_id_with_flags=can_id_with_flags, data=data),
    )


class _DatabaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db = SqliteDatabase()

    def tearDown(self) -> None:
        self.db.close()

    def test_commit_and_get_devices(self) -> None:
        records = [
            _make_record(1, boot_id=100, data=b"\x01"),
            _make_record(2, boot_id=100, data=b"\x02"),
        ]
        latest = self.db.commit(device_uid=1234, device="alpha", records=records)
        self.assertEqual(2, latest)
        devices = list(self.db.get_devices())
        self.assertEqual(["alpha"], [item.device for item in devices])
        self.assertTrue(all(item.last_heard_ts > 0 for item in devices))
        self.assertEqual([1234], [item.last_uid for item in devices])

    def test_commit_is_idempotent_for_duplicates(self) -> None:
        records = [
            _make_record(1, boot_id=10, data=b"\x01"),
            _make_record(2, boot_id=10, data=b"\x02"),
        ]
        first = self.db.commit(device_uid=99, device="alpha", records=records)
        second = self.db.commit(device_uid=99, device="alpha", records=records)
        self.assertEqual(2, first)
        self.assertEqual(2, second)
        stored = list(self.db.get_records("alpha", [10], None, None))
        self.assertEqual([1, 2], [record.seqno for record in stored])

    def test_commit_uint64_device_uid_above_signed_range_round_trips(self) -> None:
        uid = 15077748194817838259
        latest = self.db.commit(
            device_uid=uid,
            device="alpha",
            records=[_make_record(1, boot_id=10, data=b"\x01")],
        )
        self.assertEqual(1, latest)

        devices = list(self.db.get_devices())
        self.assertEqual(["alpha"], [item.device for item in devices])
        self.assertEqual([uid], [item.last_uid for item in devices])

        cursor = self.db._connection.cursor()
        cursor.execute("SELECT last_device_uid FROM devices WHERE device='alpha'")
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(_device_uid_to_sqlite_int64(uid), int(row[0]))

        cursor.execute(
            "SELECT device_uid FROM can_frames WHERE device_id=(SELECT device_id FROM devices WHERE device='alpha')"
        )
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(_device_uid_to_sqlite_int64(uid), int(row[0]))

    def test_duplicate_commit_for_uint64_uid_keeps_mismatch_count_zero(self) -> None:
        uid = 15077748194817838259
        record = _make_record(1, boot_id=10, data=b"\x01")
        self.db.commit(device_uid=uid, device="alpha", records=[record])
        with self.assertLogs(__name__, level="INFO") as captured:
            latest = self.db.commit(device_uid=uid, device="alpha", records=[record])
        self.assertEqual(1, latest)
        self.assertTrue(any("mismatches=0" in message for message in captured.output))

    def test_duplicate_mismatch_logs_warning_and_keeps_existing(self) -> None:
        original = _make_record(1, boot_id=10, hw_ts_us=111, can_id_with_flags=0x123, data=b"\x11")
        conflicting = _make_record(1, boot_id=11, hw_ts_us=222, can_id_with_flags=0x456, data=b"\x22")
        self.db.commit(device_uid=42, device="alpha", records=[original])
        with self.assertLogs(__name__, level="WARNING") as captured:
            latest = self.db.commit(device_uid=42, device="alpha", records=[conflicting])
        self.assertEqual(1, latest)
        self.assertTrue(any("Duplicate seqno mismatch" in message for message in captured.output))

        stored = list(self.db.get_records("alpha", [10, 11], None, None))
        self.assertEqual(1, len(stored))
        self.assertEqual(10, stored[0].boot_id)
        self.assertEqual(111, stored[0].hw_ts_us)
        self.assertEqual(0x123, stored[0].frame.can_id)
        self.assertEqual(b"\x11", bytes(stored[0].frame.data))

    def test_commit_preserves_can_id_with_flags_on_round_trip(self) -> None:
        flagged_id = 0xE0000012
        expected = _make_record(1, boot_id=10, hw_ts_us=111, can_id_with_flags=flagged_id, data=b"\x11")
        self.db.commit(device_uid=42, device="alpha", records=[expected])
        stored = list(self.db.get_records("alpha", [10], None, None))
        self.assertEqual(1, len(stored))
        self.assertEqual(flagged_id, stored[0].frame.can_id_with_flags)
        self.assertEqual(0x12, stored[0].frame.can_id)
        self.assertTrue(stored[0].frame.extended)
        self.assertTrue(stored[0].frame.rtr)
        self.assertTrue(stored[0].frame.error)

    def test_empty_commit_returns_cached_last_seqno(self) -> None:
        self.assertEqual(0, self.db.commit(device_uid=555, device="beta", records=[]))
        devices = list(self.db.get_devices())
        self.assertEqual(["beta"], [item.device for item in devices])
        self.assertTrue(all(item.last_heard_ts > 0 for item in devices))
        self.assertEqual([555], [item.last_uid for item in devices])

        self.db.commit(device_uid=555, device="beta", records=[_make_record(7, boot_id=1)])
        cursor = self.db._connection.cursor()
        cursor.execute("UPDATE devices SET last_heard_ts=0 WHERE device='beta'")
        self.db._connection.commit()
        self.assertEqual(7, self.db.commit(device_uid=555, device="beta", records=[]))
        cursor.execute("SELECT last_heard_ts FROM devices WHERE device='beta'")
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        assert row is not None
        self.assertGreater(int(row[0]), 0)

    def test_non_empty_commit_refreshes_last_heard_ts(self) -> None:
        self.db.commit(device_uid=10, device="gamma", records=[_make_record(1, boot_id=1)])
        cursor = self.db._connection.cursor()
        cursor.execute("UPDATE devices SET last_heard_ts=0 WHERE device='gamma'")
        self.db._connection.commit()

        latest = self.db.commit(device_uid=10, device="gamma", records=[_make_record(2, boot_id=1)])
        self.assertEqual(2, latest)
        cursor.execute("SELECT last_heard_ts FROM devices WHERE device='gamma'")
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        assert row is not None
        self.assertGreater(int(row[0]), 0)

    def test_get_records_filters_and_orders(self) -> None:
        records = [
            _make_record(5, boot_id=2, data=b"\x05"),
            _make_record(1, boot_id=1, data=b"\x01"),
            _make_record(4, boot_id=2, data=b"\x04"),
            _make_record(2, boot_id=1, data=b"\x02"),
            _make_record(3, boot_id=2, data=b"\x03"),
        ]
        self.db.commit(device_uid=1, device="alpha", records=records)
        selected = list(self.db.get_records("alpha", [2], 3, 5))
        self.assertEqual([3, 4, 5], [record.seqno for record in selected])
        self.assertEqual([2, 2, 2], [record.boot_id for record in selected])
        self.assertTrue(all(record.commit_ts > 0 for record in selected))

    def test_get_boots_uses_overlap_time_window(self) -> None:
        records = [
            _make_record(1, boot_id=100, hw_ts_us=10, data=b"\x01"),
            _make_record(2, boot_id=100, hw_ts_us=20, data=b"\x02"),
            _make_record(10, boot_id=200, hw_ts_us=30, data=b"\x03"),
            _make_record(11, boot_id=200, hw_ts_us=40, data=b"\x04"),
        ]
        self.db.commit(device_uid=1, device="alpha", records=records)

        cursor = self.db._connection.cursor()
        cursor.execute(
            "UPDATE can_frames SET commit_ts=? WHERE device_id=(SELECT device_id FROM devices WHERE device=?) AND seqno=1",
            (_datetime_to_epoch_seconds(datetime(2024, 1, 1, 0, 0, 0)), "alpha"),
        )
        cursor.execute(
            "UPDATE can_frames SET commit_ts=? WHERE device_id=(SELECT device_id FROM devices WHERE device=?) AND seqno=2",
            (_datetime_to_epoch_seconds(datetime(2024, 1, 1, 2, 0, 0)), "alpha"),
        )
        cursor.execute(
            "UPDATE can_frames SET commit_ts=? WHERE device_id=(SELECT device_id FROM devices WHERE device=?) AND seqno=10",
            (_datetime_to_epoch_seconds(datetime(2024, 1, 2, 0, 0, 0)), "alpha"),
        )
        cursor.execute(
            "UPDATE can_frames SET commit_ts=? WHERE device_id=(SELECT device_id FROM devices WHERE device=?) AND seqno=11",
            (_datetime_to_epoch_seconds(datetime(2024, 1, 2, 1, 0, 0)), "alpha"),
        )
        self.db._connection.commit()

        boots = list(
            self.db.get_boots(
                "alpha",
                earliest_commit=datetime(2024, 1, 1, 1, 0, 0),
                latest_commit=datetime(2024, 1, 1, 1, 30, 0),
            )
        )
        self.assertEqual(1, len(boots))
        self.assertEqual(100, boots[0].boot_id)
        self.assertEqual(1, boots[0].first_record.seqno)
        self.assertEqual(2, boots[0].last_record.seqno)
        self.assertEqual(_datetime_to_epoch_seconds(datetime(2024, 1, 1, 0, 0, 0)), boots[0].first_record.commit_ts)
        self.assertEqual(_datetime_to_epoch_seconds(datetime(2024, 1, 1, 2, 0, 0)), boots[0].last_record.commit_ts)

    def test_file_backed_database_persists_data(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "frames.sqlite3"
            first = SqliteDatabase(str(db_path))
            first.commit(device_uid=101, device="persist", records=[_make_record(7, boot_id=1, data=b"\x77")])
            first.close()

            second = SqliteDatabase(str(db_path))
            try:
                self.assertEqual(7, second.commit(device_uid=101, device="persist", records=[]))
                records = list(second.get_records("persist", [1], None, None))
                self.assertEqual(1, len(records))
                self.assertEqual(7, records[0].seqno)
                self.assertEqual(b"\x77", bytes(records[0].frame.data))
            finally:
                second.close()

    def test_database_abstract_methods_raise_not_implemented(self) -> None:
        class _Probe(Database):
            def commit(self, device_uid: int, device: str, records: Sequence[CANFrameRecord]) -> int:
                return 0

            def get_devices(self) -> Iterable[DeviceInfo]:
                return []

            def get_boots(
                self, device: str, earliest_commit: datetime | None, latest_commit: datetime | None
            ) -> Iterable[Boot]:
                return []

            def get_records(
                self, device: str, boot_ids: Iterable[int], seqno_min: int | None, seqno_max: int | None
            ) -> Iterable[CANFrameRecordCommitted]:
                return []

        probe = _Probe()
        with self.assertRaises(NotImplementedError):
            Database.commit(probe, 1, "x", [])
        with self.assertRaises(NotImplementedError):
            _ = list(Database.get_devices(probe))
        with self.assertRaises(NotImplementedError):
            _ = list(Database.get_boots(probe, "x", None, None))
        with self.assertRaises(NotImplementedError):
            _ = list(Database.get_records(probe, "x", [], None, None))

        # Execute concrete probe methods as well, to keep line coverage complete.
        self.assertEqual(0, probe.commit(1, "x", []))
        self.assertEqual([], list(probe.get_devices()))
        self.assertEqual([], list(probe.get_boots("x", None, None)))
        self.assertEqual([], list(probe.get_records("x", [], None, None)))

    def test_commit_rejects_empty_device(self) -> None:
        with self.assertRaisesRegex(ValueError, "device must be a non-empty string"):
            self.db.commit(device_uid=1, device="", records=[_make_record(1)])

    def test_commit_logs_uid_change_and_duplicate_seqnos_in_same_payload(self) -> None:
        self.db.commit(device_uid=100, device="alpha", records=[_make_record(1)])

        duplicate = _make_record(2, boot_id=1, hw_ts_us=22, can_id_with_flags=0x123, data=b"\x22")
        with self.assertLogs(__name__, level="WARNING") as captured:
            latest = self.db.commit(device_uid=101, device="alpha", records=[duplicate, duplicate])
        self.assertEqual(2, latest)
        self.assertTrue(any("changed UID" in message for message in captured.output))
        self.assertTrue(any("contains duplicate seqnos" in message for message in captured.output))

    def test_commit_missing_stored_row_raises_runtime_error(self) -> None:
        with patch.object(self.db, "_get_rows_by_seqno", return_value={}):
            with self.assertLogs(__name__, level="CRITICAL") as captured:
                with self.assertRaisesRegex(RuntimeError, "missing stored row"):
                    self.db.commit(device_uid=1, device="alpha", records=[_make_record(1)])
        self.assertTrue(any("Unexpected exception while committing records" in message for message in captured.output))

    def test_commit_missing_last_seqno_row_raises_runtime_error(self) -> None:
        cursor = self.db._connection.cursor()
        cursor.execute("PRAGMA foreign_keys=OFF")
        cursor.executescript("""
            CREATE TRIGGER IF NOT EXISTS _test_delete_devices_after_update
            AFTER UPDATE ON devices
            BEGIN
                DELETE FROM devices WHERE device_id=NEW.device_id;
            END;
            """)
        self.db._connection.commit()
        with self.assertLogs(__name__, level="CRITICAL") as captured:
            with self.assertRaisesRegex(RuntimeError, "failed to fetch updated last_seqno"):
                self.db.commit(device_uid=1, device="alpha", records=[_make_record(1)])
        self.assertTrue(any("Unexpected exception while committing records" in message for message in captured.output))

    def test_commit_sqlite_error_path_runs_rollback(self) -> None:
        with patch.object(self.db, "_ensure_device", return_value=(999_999, 0, None)):
            with self.assertLogs(__name__, level="ERROR") as captured:
                with self.assertRaises(sqlite3.Error):
                    self.db.commit(device_uid=1, device="alpha", records=[_make_record(1)])
        self.assertTrue(any("SQLite error while committing records" in message for message in captured.output))

    def test_get_boots_unknown_device_returns_empty(self) -> None:
        self.assertEqual([], list(self.db.get_boots("unknown", None, None)))

    def test_get_records_invalid_range_returns_empty(self) -> None:
        self.assertEqual([], list(self.db.get_records("alpha", [1], 10, 5)))

    def test_get_records_empty_boot_ids_returns_empty(self) -> None:
        self.assertEqual([], list(self.db.get_records("alpha", [], None, None)))

    def test_get_records_unknown_device_returns_empty(self) -> None:
        self.assertEqual([], list(self.db.get_records("unknown", [1], None, None)))

    def test_ensure_device_raises_if_row_missing(self) -> None:
        class _CursorWithoutRows:
            def execute(self, *_args: object, **_kwargs: object) -> None:
                return None

            def fetchone(self) -> None:
                return None

        with self.assertRaisesRegex(RuntimeError, "failed to create or fetch device row"):
            self.db._ensure_device(_CursorWithoutRows(), 1, "alpha")  # type: ignore[arg-type]

    def test_get_device_id_returns_none_when_missing(self) -> None:
        class _CursorWithoutRows:
            def execute(self, *_args: object, **_kwargs: object) -> None:
                return None

            def fetchone(self) -> None:
                return None

        self.assertIsNone(self.db._get_device_id(_CursorWithoutRows(), "alpha"))  # type: ignore[arg-type]

    def test_get_rows_by_seqno_empty_input(self) -> None:
        cursor = self.db._connection.cursor()
        self.assertEqual({}, self.db._get_rows_by_seqno(cursor, 1, []))

    def test_normalize_data_rejects_payload_longer_than_64(self) -> None:
        with self.assertRaisesRegex(ValueError, "CAN payload too long"):
            SqliteDatabase._normalize_data(bytes(range(65)))

    def test_rollback_quietly_logs_when_rollback_itself_fails(self) -> None:
        class _BadConnection:
            def rollback(self) -> None:
                raise sqlite3.Error("forced rollback failure")

        original_connection = self.db._connection
        try:
            self.db._connection = _BadConnection()  # type: ignore[assignment]
            with self.assertLogs(__name__, level="ERROR") as captured:
                self.db._rollback_quietly()
            self.assertTrue(any("Rollback failed" in message for message in captured.output))
        finally:
            self.db._connection = original_connection

    def test_chunked_rejects_non_positive_chunk_size(self) -> None:
        with self.assertRaisesRegex(ValueError, "chunk_size must be positive"):
            _ = list(_chunked([1, 2, 3], 0))

    def test_datetime_to_epoch_seconds_handles_aware_datetime(self) -> None:
        value = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(1704067200, _datetime_to_epoch_seconds(value))

    def test_device_uid_int64_conversion_boundaries(self) -> None:
        self.assertEqual(0, _device_uid_to_sqlite_int64(0))
        self.assertEqual(_SQLITE_INT64_MAX, _device_uid_to_sqlite_int64(_SQLITE_INT64_MAX))
        self.assertEqual(_SQLITE_INT64_MIN, _device_uid_to_sqlite_int64(_SQLITE_INT64_MAX + 1))
        self.assertEqual(-1, _device_uid_to_sqlite_int64(_UINT64_MAX))

        self.assertEqual(0, _sqlite_int64_to_device_uid(0))
        self.assertEqual(_SQLITE_INT64_MAX, _sqlite_int64_to_device_uid(_SQLITE_INT64_MAX))
        self.assertEqual(_SQLITE_INT64_MAX + 1, _sqlite_int64_to_device_uid(_SQLITE_INT64_MIN))
        self.assertEqual(_UINT64_MAX, _sqlite_int64_to_device_uid(-1))

        with self.assertRaisesRegex(ValueError, "uint64 range"):
            _device_uid_to_sqlite_int64(-1)
        with self.assertRaisesRegex(ValueError, "uint64 range"):
            _device_uid_to_sqlite_int64(_UINT64_MAX + 1)
        with self.assertRaisesRegex(ValueError, "out of int64 range"):
            _sqlite_int64_to_device_uid(_SQLITE_INT64_MAX + 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
