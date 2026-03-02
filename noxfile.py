from __future__ import annotations

from pathlib import Path

import nox

PACKAGE_DIR = Path("nestor")
EXCLUDED_TEST_FILE = "__main__.py"
E2E_SCRIPT_PATH = Path("tools") / "e2e_test.py"
SERVE_SMOKE_SCRIPT_PATH = Path("tools") / "serve_smoke_test.py"

nox.options.sessions = ("tests", "serve_smoke", "e2e", "black", "mypy")


def _discover_test_modules() -> list[str]:
    modules = sorted(
        f"{PACKAGE_DIR.name}.{path.stem}" for path in PACKAGE_DIR.glob("*.py") if path.name != EXCLUDED_TEST_FILE
    )
    if not modules:
        raise RuntimeError(f"No Python files found under {PACKAGE_DIR}/ excluding {EXCLUDED_TEST_FILE}")
    return modules


@nox.session
def tests(session: nox.Session) -> None:
    session.install("coverage", "httpx", ".")
    modules = _discover_test_modules()
    session.run(
        "coverage",
        "run",
        "--branch",
        "--source",
        PACKAGE_DIR.name,
        "--omit",
        "*/nestor/__main__.py",
        "-m",
        "unittest",
        *modules,
    )
    session.run(
        "coverage",
        "report",
        "-m",
        "--omit",
        "*/nestor/__main__.py",
        "--fail-under=80",
    )


@nox.session
def e2e(session: nox.Session) -> None:
    session.install("httpx", ".")
    if not E2E_SCRIPT_PATH.exists():
        raise RuntimeError(f"E2E test script is missing: {E2E_SCRIPT_PATH}")
    session.run("python", str(E2E_SCRIPT_PATH), external=True)


@nox.session
def serve_smoke(session: nox.Session) -> None:
    session.install(".")
    if not SERVE_SMOKE_SCRIPT_PATH.exists():
        raise RuntimeError(f"Serve smoke script is missing: {SERVE_SMOKE_SCRIPT_PATH}")
    session.run("python", str(SERVE_SMOKE_SCRIPT_PATH))


@nox.session
def black(session: nox.Session) -> None:
    session.install("black")
    session.run("black", "--check", ".")


@nox.session
def mypy(session: nox.Session) -> None:
    session.install("mypy", ".")
    session.run("mypy", "nestor")
