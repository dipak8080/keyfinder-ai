"""
Persistent count of successfully processed jobs, for the public stats
endpoint. Its own SQLite file in /app/data so it survives deploys and is
independent of logs.db: pruning or deleting request logs never touches it.

Daily buckets per endpoint, so the total and a rolling 7-day figure both
come from one table. Seeded once, on first ever startup, from historical
request_logs if that file exists; after that the seed row is frozen and
the counter only moves forward via record().
"""

import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone

logger = logging.getLogger("uvicorn.error")

STATS_DB_PATH = os.environ.get("STATS_DB_PATH", "/app/data/stats.db")
_REQUEST_LOG_DB = os.environ.get("REQUEST_LOG_DB_PATH", "/app/data/logs.db")

_lock = threading.Lock()
_cache: dict = {"at": 0.0, "payload": None}
_CACHE_SECONDS = 60


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(STATS_DB_PATH, timeout=5)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init() -> None:
    """Create the table and, on the very first run, seed from history."""
    try:
        os.makedirs(os.path.dirname(STATS_DB_PATH), exist_ok=True)
        with _lock, _connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS job_counts (
                    day TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    count INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (day, endpoint)
                )
                """
            )
            row = conn.execute("SELECT COUNT(*) FROM job_counts").fetchone()
            if row and row[0] == 0:
                _seed(conn)
    except Exception as e:
        logger.warning(f"[stats] init failed (non-fatal): {e}")


def _seed(conn: sqlite3.Connection) -> None:
    """One-time baseline from request_logs: every successful POST to a tool
    route counts as one processed job. Runs only when job_counts is empty."""
    seeded = 0
    try:
        if os.path.exists(_REQUEST_LOG_DB):
            logs = sqlite3.connect(_REQUEST_LOG_DB, timeout=5)
            try:
                row = logs.execute(
                    """
                    SELECT COUNT(*) FROM request_logs
                    WHERE method = 'POST'
                      AND status_code < 400
                      AND tool IS NOT NULL
                      AND tool != '-'
                    """
                ).fetchone()
                seeded = int(row[0]) if row else 0
            finally:
                logs.close()
    except Exception as e:
        logger.warning(f"[stats] seed count from logs.db failed, seeding 0: {e}")
        seeded = 0
    conn.execute(
        "INSERT INTO job_counts (day, endpoint, count) VALUES ('seed', 'seed', ?)",
        (seeded,),
    )
    conn.commit()
    logger.info(f"[stats] seeded job counter with {seeded} historical jobs")


def record(endpoint: str) -> None:
    """One successfully processed job. Must never raise into the caller."""
    try:
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with _lock, _connect() as conn:
            conn.execute(
                """
                INSERT INTO job_counts (day, endpoint, count) VALUES (?, ?, 1)
                ON CONFLICT (day, endpoint) DO UPDATE SET count = count + 1
                """,
                (day, endpoint),
            )
            conn.commit()
    except Exception as e:
        logger.warning(f"[stats] record failed (non-fatal): {e}")


def snapshot() -> dict:
    """Totals for the public endpoint, cached in-process for a minute."""
    now = time.time()
    if _cache["payload"] is not None and now - _cache["at"] < _CACHE_SECONDS:
        return _cache["payload"]
    total = 0
    week = 0
    try:
        cutoff = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with _lock, _connect() as conn:
            row = conn.execute("SELECT COALESCE(SUM(count), 0) FROM job_counts").fetchone()
            total = int(row[0]) if row else 0
            row = conn.execute(
                """
                SELECT COALESCE(SUM(count), 0) FROM job_counts
                WHERE day != 'seed' AND day >= date(?, '-6 days')
                """,
                (cutoff,),
            ).fetchone()
            week = int(row[0]) if row else 0
    except Exception as e:
        logger.warning(f"[stats] snapshot failed (non-fatal): {e}")
    payload = {"total_jobs": total, "last_7_days": week}
    _cache["at"] = now
    _cache["payload"] = payload
    return payload