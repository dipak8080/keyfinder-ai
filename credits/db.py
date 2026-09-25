"""SQLite access for the credits package.

Its own DB file, its own connections. One short-lived connection per unit of
work; every write goes through tx(), which takes an IMMEDIATE lock so two
concurrent job submissions can't spend the same credit.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from .config import get_settings

log = logging.getLogger("credits.db")

_MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"


# --- time helpers -----------------------------------------------------------

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    """All timestamps in this DB use this exact format, so plain string
    comparison is also chronological comparison."""
    return utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def parse_ts(value) -> datetime | None:
    """Parses ISO 8601 (Z, +00:00, 0 to 9 fraction digits) or epoch seconds."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    text = str(value).strip()
    if text.replace(".", "", 1).isdigit():
        return datetime.fromtimestamp(float(text), tz=timezone.utc)
    text = text.replace(" ", "T", 1)
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    main, sep, rest = text.partition(".")
    if sep:
        n = 0
        while n < len(rest) and rest[n].isdigit():
            n += 1
        digits, tail = rest[:n], rest[n:]
        text = f"{main}.{(digits + '000000')[:6]}{tail}"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def normalize_ts(value) -> str | None:
    """Any timestamp as this DB's own format, or None if unparseable."""
    dt = parse_ts(value)
    return iso(dt) if dt else None


def add_months(dt: datetime, months: int) -> datetime:
    total = dt.month - 1 + months
    year, month = dt.year + total // 12, total % 12 + 1
    days = [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28,
            31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1]
    return dt.replace(year=year, month=month, day=min(dt.day, days))


def period_key(dt: datetime | None = None) -> str:
    """Free-tier billing period: calendar month, UTC."""
    return (dt or utcnow()).strftime("%Y-%m")


def next_period_start_iso(dt: datetime | None = None) -> str:
    d = dt or utcnow()
    year, month = (d.year + 1, 1) if d.month == 12 else (d.year, d.month + 1)
    return datetime(year, month, 1, tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --- connections ------------------------------------------------------------

@contextmanager
def connect() -> Iterator[sqlite3.Connection]:
    path = get_settings().db_path
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        yield conn
    finally:
        conn.close()


@contextmanager
def tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Serialised write transaction. Rolls back on any exception."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


@contextmanager
def write() -> Iterator[sqlite3.Connection]:
    """Shorthand for connect() + tx()."""
    with connect() as conn, tx(conn):
        yield conn


# --- migrations -------------------------------------------------------------

def run_migrations() -> None:
    """Apply every .sql file in credits/migrations/ in filename order.
    Idempotent — call it on every boot."""
    with connect() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            " filename TEXT PRIMARY KEY, applied_at TEXT NOT NULL)"
        )
        applied = {r["filename"] for r in conn.execute("SELECT filename FROM schema_migrations")}
        for path in sorted(_MIGRATIONS_DIR.glob("*.sql")):
            if path.name in applied:
                continue
            log.info("applying credits migration %s", path.name)
            conn.executescript(path.read_text(encoding="utf-8"))
            conn.execute(
                "INSERT OR REPLACE INTO schema_migrations (filename, applied_at) VALUES (?, ?)",
                (path.name, now_iso()),
            )
    log.info("credits db ready at %s", get_settings().db_path)


def healthcheck() -> dict:
    path = get_settings().db_path
    with connect() as conn:
        tables = [
            r["name"]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        ]
        ledger_rows = conn.execute("SELECT COUNT(*) AS n FROM credit_ledger").fetchone()["n"]
        metered_jobs = conn.execute("SELECT COUNT(*) AS n FROM gpu_job_metrics").fetchone()["n"]
    return {
        "ok": True,
        "db_path": path,
        "db_bytes": os.path.getsize(path) if os.path.exists(path) else 0,
        "tables": tables,
        "ledger_rows": ledger_rows,
        "metered_jobs": metered_jobs,
    }