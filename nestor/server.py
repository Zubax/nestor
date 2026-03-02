from __future__ import annotations

import argparse
import logging
import os
import sys
import tempfile
import time
import types
import unittest
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path
from unittest.mock import Mock, patch

from fastapi import FastAPI

from nestor.database import SqliteDatabase
from nestor.rest_api import create_app

LOGGER = logging.getLogger(__name__)

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000
DEFAULT_DB_PATH = "./nestor.db"
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_FILE = "./nestor.log"
DEFAULT_LOG_MAX_BYTES = 128 * 1024 * 1024
DEFAULT_LOG_BACKUP_COUNT = 20

ENV_HOST = "NESTOR_HOST"
ENV_PORT = "NESTOR_PORT"
ENV_DB_PATH = "NESTOR_DB_PATH"
ENV_UDS = "NESTOR_UDS"
ENV_ROOT_PATH = "NESTOR_ROOT_PATH"
ENV_LOG_LEVEL = "NESTOR_LOG_LEVEL"
ENV_LOG_FILE = "NESTOR_LOG_FILE"
ENV_LOG_MAX_BYTES = "NESTOR_LOG_MAX_BYTES"
ENV_LOG_BACKUP_COUNT = "NESTOR_LOG_BACKUP_COUNT"

_VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
_ANSI_RESET = "\x1b[0m"
_ANSI_LEVEL_COLORS = {
    "DEBUG": "\x1b[36m",
    "INFO": "\x1b[32m",
    "WARNING": "\x1b[33m",
    "ERROR": "\x1b[31m",
    "CRITICAL": "\x1b[1;31m",
}
_ANSI_LOGGER_COLOR = "\x1b[2;34m"


def _utc_converter(timestamp: float | None = None) -> time.struct_time:
    resolved_timestamp = time.time() if timestamp is None else timestamp
    return time.gmtime(resolved_timestamp)


@dataclass(frozen=True)
class ServeConfig:
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    db_path: str = DEFAULT_DB_PATH
    uds: str | None = None
    root_path: str = ""
    log_level: str = DEFAULT_LOG_LEVEL
    log_file: str = DEFAULT_LOG_FILE
    log_max_bytes: int = DEFAULT_LOG_MAX_BYTES
    log_backup_count: int = DEFAULT_LOG_BACKUP_COUNT


class _UTCFormatter(logging.Formatter):
    converter = staticmethod(_utc_converter)


class _StderrColorFormatter(_UTCFormatter):
    @staticmethod
    def _wrap_with_ansi(text: str, ansi_prefix: str) -> str:
        return f"{ansi_prefix}{text}{_ANSI_RESET}"

    def format(self, record: logging.LogRecord) -> str:
        rendered = super().format(record)
        first_line, separator, tail = rendered.partition("\n")
        parts = first_line.split(" | ")
        if len(parts) < 5:
            return rendered

        level_ansi = _ANSI_LEVEL_COLORS.get(record.levelname, _ANSI_LEVEL_COLORS["INFO"])
        parts[1] = self._wrap_with_ansi(parts[1], level_ansi)
        parts[3] = self._wrap_with_ansi(parts[3], _ANSI_LOGGER_COLOR)
        colorized_head = " | ".join(parts)
        if not separator:
            return colorized_head
        return f"{colorized_head}{separator}{tail}"


def _env_or_default(env: Mapping[str, str], key: str, default: str) -> str:
    value = env.get(key)
    return default if value is None else value


def _parse_port(value: str) -> int:
    try:
        out = int(value)
    except ValueError as ex:
        raise argparse.ArgumentTypeError(f"invalid port: {value!r}") from ex
    if not (1 <= out <= 65535):
        raise argparse.ArgumentTypeError(f"port out of range [1, 65535]: {out}")
    return out


def _parse_positive_int(value: str, *, field_name: str) -> int:
    try:
        out = int(value)
    except ValueError as ex:
        raise argparse.ArgumentTypeError(f"invalid {field_name}: {value!r}") from ex
    if out <= 0:
        raise argparse.ArgumentTypeError(f"{field_name} must be > 0: {out}")
    return out


def _parse_log_level(value: str) -> str:
    out = value.upper()
    if out not in _VALID_LOG_LEVELS:
        allowed = ", ".join(sorted(_VALID_LOG_LEVELS))
        raise argparse.ArgumentTypeError(f"invalid log level {value!r}; expected one of: {allowed}")
    return out


def parse_serve_config(argv: Sequence[str] | None = None, env: Mapping[str, str] | None = None) -> ServeConfig:
    env_map = os.environ if env is None else env

    host_default = _env_or_default(env_map, ENV_HOST, DEFAULT_HOST)
    port_default = _parse_port(_env_or_default(env_map, ENV_PORT, str(DEFAULT_PORT)))
    db_path_default = _env_or_default(env_map, ENV_DB_PATH, DEFAULT_DB_PATH)
    uds_default = env_map.get(ENV_UDS)
    root_path_default = _env_or_default(env_map, ENV_ROOT_PATH, "")
    log_level_default = _parse_log_level(_env_or_default(env_map, ENV_LOG_LEVEL, DEFAULT_LOG_LEVEL))
    log_file_default = _env_or_default(env_map, ENV_LOG_FILE, DEFAULT_LOG_FILE)
    log_max_bytes_default = _parse_positive_int(
        _env_or_default(env_map, ENV_LOG_MAX_BYTES, str(DEFAULT_LOG_MAX_BYTES)),
        field_name="log max bytes",
    )
    log_backup_count_default = _parse_positive_int(
        _env_or_default(env_map, ENV_LOG_BACKUP_COUNT, str(DEFAULT_LOG_BACKUP_COUNT)),
        field_name="log backup count",
    )

    parser = argparse.ArgumentParser(
        prog="nestor serve",
        description="Run Nestor CF3D REST API server",
    )
    parser.add_argument("--host", default=host_default, help=f"TCP bind host (env: {ENV_HOST})")
    parser.add_argument(
        "--port",
        default=port_default,
        type=_parse_port,
        help=f"TCP bind port in range [1, 65535] (env: {ENV_PORT})",
    )
    parser.add_argument(
        "--db-path",
        default=db_path_default,
        help=f"SQLite path or ':memory:' (env: {ENV_DB_PATH})",
    )
    parser.add_argument(
        "--uds",
        default=uds_default,
        help=f"Unix domain socket path (overrides host/port) (env: {ENV_UDS})",
    )
    parser.add_argument("--root-path", default=root_path_default, help=f"ASGI root path (env: {ENV_ROOT_PATH})")
    parser.add_argument(
        "--log-level",
        default=log_level_default,
        type=_parse_log_level,
        help=f"Log threshold (env: {ENV_LOG_LEVEL})",
    )
    parser.add_argument(
        "--log-file",
        default=log_file_default,
        help=f"Rotating log file path (env: {ENV_LOG_FILE})",
    )
    parser.add_argument(
        "--log-max-bytes",
        default=log_max_bytes_default,
        type=lambda raw: _parse_positive_int(raw, field_name="log max bytes"),
        help=f"Rotate file when size exceeds this value (env: {ENV_LOG_MAX_BYTES})",
    )
    parser.add_argument(
        "--log-backup-count",
        default=log_backup_count_default,
        type=lambda raw: _parse_positive_int(raw, field_name="log backup count"),
        help=f"Maximum number of rotated log files to keep (env: {ENV_LOG_BACKUP_COUNT})",
    )

    parsed = parser.parse_args(list(argv) if argv is not None else None)
    return ServeConfig(
        host=str(parsed.host),
        port=int(parsed.port),
        db_path=str(parsed.db_path),
        uds=None if parsed.uds in (None, "") else str(parsed.uds),
        root_path=str(parsed.root_path),
        log_level=str(parsed.log_level),
        log_file=str(parsed.log_file),
        log_max_bytes=int(parsed.log_max_bytes),
        log_backup_count=int(parsed.log_backup_count),
    )


def _is_loopback_host(host: str) -> bool:
    normalized = host.strip().lower()
    return normalized in {"127.0.0.1", "localhost", "::1"}


def _ensure_parent_directory(path: Path) -> None:
    if path.parent == Path(""):
        return
    path.parent.mkdir(parents=True, exist_ok=True)


def _should_color_stderr(stream: object) -> bool:
    isatty_method = getattr(stream, "isatty", None)
    if not callable(isatty_method):
        return False
    try:
        return bool(isatty_method())
    except Exception:
        return False


def configure_logging(config: ServeConfig) -> None:
    level = logging._nameToLevel.get(config.log_level.upper())
    if not isinstance(level, int):
        raise ValueError(f"invalid log level {config.log_level!r}")

    log_path = Path(config.log_file).expanduser()
    _ensure_parent_directory(log_path)

    plain_formatter = _UTCFormatter(
        fmt="%(asctime)s.%(msecs)03d | %(levelname)-8s | %(process)7d | %(name)-32s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root_logger = logging.getLogger()
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            LOGGER.error("Failed to close pre-existing logging handler", exc_info=True)

    stderr_handler = logging.StreamHandler()
    stderr_handler.setLevel(level)
    if _should_color_stderr(stderr_handler.stream):
        stderr_handler.setFormatter(
            _StderrColorFormatter(
                fmt="%(asctime)s.%(msecs)03d | %(levelname)-8s | %(process)7d | %(name)-32s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
    else:
        stderr_handler.setFormatter(plain_formatter)

    file_handler = RotatingFileHandler(
        filename=str(log_path),
        maxBytes=config.log_max_bytes,
        backupCount=config.log_backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(plain_formatter)

    root_logger.setLevel(level)
    root_logger.addHandler(stderr_handler)
    root_logger.addHandler(file_handler)

    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uv_logger = logging.getLogger(logger_name)
        uv_logger.handlers.clear()
        uv_logger.propagate = True
        uv_logger.setLevel(level)

    LOGGER.info(
        "Logging configured: level=%s stderr=true file=%s rotate_max_bytes=%d rotate_backups=%d",
        config.log_level,
        log_path,
        config.log_max_bytes,
        config.log_backup_count,
    )


def build_database(config: ServeConfig) -> SqliteDatabase:
    if config.db_path == ":memory:":
        if config.uds is None and not _is_loopback_host(config.host):
            LOGGER.warning(
                "Using in-memory database with externally reachable bind host=%r; all data is non-persistent",
                config.host,
            )
        LOGGER.info("Opening in-memory SQLite database")
        return SqliteDatabase()

    db_path = Path(config.db_path).expanduser()
    _ensure_parent_directory(db_path)
    LOGGER.info("Opening file-backed SQLite database at %s", db_path)
    return SqliteDatabase(str(db_path))


def _prepare_uds_path(uds: str) -> str:
    uds_path = Path(uds).expanduser()
    _ensure_parent_directory(uds_path)
    return str(uds_path)


def serve(config: ServeConfig) -> None:
    configure_logging(config)
    database = build_database(config)
    application = create_app(database)

    try:
        import uvicorn
    except Exception as ex:
        LOGGER.critical("Failed to import uvicorn", exc_info=True)
        raise RuntimeError("uvicorn is required to run the server") from ex

    if config.uds:
        uds_path = _prepare_uds_path(config.uds)
        LOGGER.info("Starting server on UDS=%s root_path=%r", uds_path, config.root_path)
        uvicorn.run(
            app=application,
            uds=uds_path,
            proxy_headers=True,
            forwarded_allow_ips="*",
            root_path=config.root_path,
            log_config=None,
        )
        return

    LOGGER.info("Starting server on host=%s port=%d root_path=%r", config.host, config.port, config.root_path)
    uvicorn.run(
        app=application,
        host=config.host,
        port=config.port,
        proxy_headers=True,
        forwarded_allow_ips="*",
        root_path=config.root_path,
        log_config=None,
    )


def create_app_from_env() -> FastAPI:
    config = parse_serve_config([], os.environ)
    configure_logging(config)
    database = build_database(config)
    LOGGER.info("Creating ASGI app from environment configuration")
    return create_app(database)


class _ServerTests(unittest.TestCase):
    def test_parse_serve_config_defaults(self) -> None:
        config = parse_serve_config([], env={})
        self.assertEqual(DEFAULT_HOST, config.host)
        self.assertEqual(DEFAULT_PORT, config.port)
        self.assertEqual(DEFAULT_DB_PATH, config.db_path)
        self.assertIsNone(config.uds)
        self.assertEqual(DEFAULT_LOG_LEVEL, config.log_level)
        self.assertEqual(DEFAULT_LOG_FILE, config.log_file)
        self.assertEqual(DEFAULT_LOG_MAX_BYTES, config.log_max_bytes)
        self.assertEqual(DEFAULT_LOG_BACKUP_COUNT, config.log_backup_count)

    def test_parse_serve_config_reads_environment(self) -> None:
        env = {
            ENV_HOST: "127.0.0.1",
            ENV_PORT: "9001",
            ENV_DB_PATH: "/tmp/env-db.sqlite3",
            ENV_UDS: "/tmp/nestor.sock",
            ENV_ROOT_PATH: "/gw",
            ENV_LOG_LEVEL: "warning",
            ENV_LOG_FILE: "/tmp/nestor.log",
            ENV_LOG_MAX_BYTES: "4096",
            ENV_LOG_BACKUP_COUNT: "7",
        }
        config = parse_serve_config([], env=env)
        self.assertEqual("127.0.0.1", config.host)
        self.assertEqual(9001, config.port)
        self.assertEqual("/tmp/env-db.sqlite3", config.db_path)
        self.assertEqual("/tmp/nestor.sock", config.uds)
        self.assertEqual("/gw", config.root_path)
        self.assertEqual("WARNING", config.log_level)
        self.assertEqual("/tmp/nestor.log", config.log_file)
        self.assertEqual(4096, config.log_max_bytes)
        self.assertEqual(7, config.log_backup_count)

    def test_parse_serve_config_cli_overrides_environment(self) -> None:
        env = {
            ENV_HOST: "127.0.0.1",
            ENV_PORT: "9001",
            ENV_DB_PATH: "/tmp/env-db.sqlite3",
            ENV_LOG_LEVEL: "warning",
        }
        config = parse_serve_config(
            [
                "--host",
                "0.0.0.0",
                "--port",
                "8000",
                "--db-path",
                "./cli.sqlite3",
                "--log-level",
                "debug",
            ],
            env=env,
        )
        self.assertEqual("0.0.0.0", config.host)
        self.assertEqual(8000, config.port)
        self.assertEqual("./cli.sqlite3", config.db_path)
        self.assertEqual("DEBUG", config.log_level)

    def test_build_database_creates_missing_parent_directories(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "nested" / "deeper" / "nestor.sqlite3"
            config = ServeConfig(db_path=str(db_path))
            database = build_database(config)
            try:
                self.assertTrue(db_path.parent.exists())
                self.assertTrue(db_path.exists())
            finally:
                database.close()

    def test_build_database_warns_for_exposed_in_memory_setup(self) -> None:
        config = ServeConfig(db_path=":memory:", host="0.0.0.0")
        with self.assertLogs(__name__, level="WARNING") as captured:
            database = build_database(config)
        database.close()
        self.assertTrue(any("non-persistent" in line for line in captured.output))

    def test_configure_logging_installs_stream_and_rotating_handlers(self) -> None:
        root_logger = logging.getLogger()
        saved_handlers = list(root_logger.handlers)
        saved_level = root_logger.level

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                log_file = Path(temp_dir) / "logs" / "nestor.log"
                config = ServeConfig(log_file=str(log_file), log_level="INFO")
                configure_logging(config)

                self.assertEqual(2, len(root_logger.handlers))
                self.assertTrue(any(isinstance(handler, RotatingFileHandler) for handler in root_logger.handlers))
                self.assertTrue(any(isinstance(handler, logging.StreamHandler) for handler in root_logger.handlers))

                LOGGER.info("server logging smoke test")
                for handler in root_logger.handlers:
                    handler.flush()

                self.assertTrue(log_file.exists())
                contents = log_file.read_text(encoding="utf-8")
                self.assertIn("server logging smoke test", contents)
                self.assertIn("| INFO", contents)
        finally:
            for handler in list(root_logger.handlers):
                root_logger.removeHandler(handler)
                handler.close()
            root_logger.setLevel(saved_level)
            for handler in saved_handlers:
                root_logger.addHandler(handler)

    def test_should_color_stderr_uses_isatty_when_available(self) -> None:
        class _FakeTTYStream:
            def __init__(self, out: bool) -> None:
                self._out = out

            def isatty(self) -> bool:
                return self._out

        self.assertTrue(_should_color_stderr(_FakeTTYStream(True)))
        self.assertFalse(_should_color_stderr(_FakeTTYStream(False)))
        self.assertFalse(_should_color_stderr(object()))

    def test_configure_logging_uses_color_formatter_for_tty_stderr(self) -> None:
        root_logger = logging.getLogger()
        saved_handlers = list(root_logger.handlers)
        saved_level = root_logger.level

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                log_file = Path(temp_dir) / "logs" / "nestor.log"
                config = ServeConfig(log_file=str(log_file), log_level="INFO")
                with patch("nestor.server._should_color_stderr", return_value=True):
                    configure_logging(config)

                stream_handlers = [
                    handler
                    for handler in root_logger.handlers
                    if isinstance(handler, logging.StreamHandler) and not isinstance(handler, RotatingFileHandler)
                ]
                self.assertEqual(1, len(stream_handlers))
                formatter = stream_handlers[0].formatter
                self.assertIsNotNone(formatter)
                assert formatter is not None
                self.assertIsInstance(formatter, _StderrColorFormatter)

                record = logging.LogRecord(
                    name="nestor.server",
                    level=logging.WARNING,
                    pathname=__file__,
                    lineno=0,
                    msg="color formatter probe",
                    args=(),
                    exc_info=None,
                )
                rendered = formatter.format(record)
                self.assertIn("\x1b[", rendered)
                self.assertIn("WARNING", rendered)
                self.assertIn("nestor.server", rendered)
        finally:
            for handler in list(root_logger.handlers):
                root_logger.removeHandler(handler)
                handler.close()
            root_logger.setLevel(saved_level)
            for handler in saved_handlers:
                root_logger.addHandler(handler)

    def test_configure_logging_stderr_and_file_remain_plain_when_color_disabled(self) -> None:
        root_logger = logging.getLogger()
        saved_handlers = list(root_logger.handlers)
        saved_level = root_logger.level

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                log_file = Path(temp_dir) / "logs" / "nestor.log"
                config = ServeConfig(log_file=str(log_file), log_level="INFO")
                with patch("nestor.server._should_color_stderr", return_value=False):
                    configure_logging(config)

                stream_handlers = [
                    handler
                    for handler in root_logger.handlers
                    if isinstance(handler, logging.StreamHandler) and not isinstance(handler, RotatingFileHandler)
                ]
                self.assertEqual(1, len(stream_handlers))
                formatter = stream_handlers[0].formatter
                self.assertIsNotNone(formatter)
                assert formatter is not None
                self.assertNotIsInstance(formatter, _StderrColorFormatter)

                LOGGER.info("plain formatter probe")
                for handler in root_logger.handlers:
                    handler.flush()

                self.assertTrue(log_file.exists())
                contents = log_file.read_text(encoding="utf-8")
                self.assertIn("plain formatter probe", contents)
                self.assertNotIn("\x1b[", contents)
        finally:
            for handler in list(root_logger.handlers):
                root_logger.removeHandler(handler)
                handler.close()
            root_logger.setLevel(saved_level)
            for handler in saved_handlers:
                root_logger.addHandler(handler)

    def test_serve_invokes_uvicorn_with_host_port(self) -> None:
        config = ServeConfig(host="127.0.0.1", port=8123, root_path="/gw")
        fake_uvicorn = types.SimpleNamespace(run=Mock())
        fake_database = Mock()
        fake_app = object()

        with (
            patch("nestor.server.configure_logging") as mocked_configure,
            patch("nestor.server.build_database", return_value=fake_database) as mocked_build_db,
            patch("nestor.server.create_app", return_value=fake_app) as mocked_create_app,
            patch.dict(sys.modules, {"uvicorn": fake_uvicorn}),
        ):
            serve(config)

        mocked_configure.assert_called_once_with(config)
        mocked_build_db.assert_called_once_with(config)
        mocked_create_app.assert_called_once_with(fake_database)
        fake_uvicorn.run.assert_called_once()
        kwargs = fake_uvicorn.run.call_args.kwargs
        self.assertEqual(fake_app, kwargs["app"])
        self.assertEqual("127.0.0.1", kwargs["host"])
        self.assertEqual(8123, kwargs["port"])
        self.assertEqual("/gw", kwargs["root_path"])
        self.assertTrue(kwargs["proxy_headers"])
        self.assertEqual("*", kwargs["forwarded_allow_ips"])
        self.assertNotIn("uds", kwargs)

    def test_serve_invokes_uvicorn_with_uds(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            uds_path = Path(temp_dir) / "run" / "nestor.sock"
            config = ServeConfig(uds=str(uds_path))
            fake_uvicorn = types.SimpleNamespace(run=Mock())
            fake_database = Mock()
            fake_app = object()

            with (
                patch("nestor.server.configure_logging"),
                patch("nestor.server.build_database", return_value=fake_database),
                patch("nestor.server.create_app", return_value=fake_app),
                patch.dict(sys.modules, {"uvicorn": fake_uvicorn}),
            ):
                serve(config)

            kwargs = fake_uvicorn.run.call_args.kwargs
            self.assertEqual(str(uds_path), kwargs["uds"])
            self.assertNotIn("host", kwargs)
            self.assertNotIn("port", kwargs)

    def test_create_app_from_env_builds_application(self) -> None:
        config = ServeConfig()
        fake_database = Mock()
        fake_app = Mock(spec=FastAPI)

        with (
            patch("nestor.server.parse_serve_config", return_value=config) as mocked_parse,
            patch("nestor.server.configure_logging") as mocked_logging,
            patch("nestor.server.build_database", return_value=fake_database) as mocked_database,
            patch("nestor.server.create_app", return_value=fake_app) as mocked_create_app,
        ):
            out = create_app_from_env()

        mocked_parse.assert_called_once()
        mocked_logging.assert_called_once_with(config)
        mocked_database.assert_called_once_with(config)
        mocked_create_app.assert_called_once_with(fake_database)
        self.assertIs(fake_app, out)


if __name__ == "__main__":
    unittest.main(verbosity=2)
