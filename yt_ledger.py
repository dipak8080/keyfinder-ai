import os
import sqlite3
import threading
import time
import logging

logger = logging.getLogger(__name__)

LEDGER_DB_PATH = os.environ.get("YT_LEDGER_DB_PATH", "/app/data/yt_ledger.db")
LEDGER_RETENTION_DAYS = int(os.environ.get("YT_LEDGER_RETENTION_DAYS", "30"))

_lock = threading.Lock()
_conn = None
_writes = 0


def _db():
    global _conn
    if _conn is None:
        os.makedirs(os.path.dirname(LEDGER_DB_PATH), exist_ok=True)
        _conn = sqlite3.connect(LEDGER_DB_PATH, timeout=5, check_same_thread=False)
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute(
            """
            CREATE TABLE IF NOT EXISTS attempts (
                ts REAL NOT NULL,
                via TEXT NOT NULL,
                account TEXT NOT NULL,
                ok INTEGER NOT NULL,
                kind TEXT,
                phase TEXT,
                detail TEXT
            )
            """
        )
        _conn.execute("CREATE INDEX IF NOT EXISTS idx_attempts_ts ON attempts(ts)")
        _conn.commit()
    return _conn


def record_attempt(via: str, account: str, ok: bool, kind: str = "", phase: str = "", detail: str = ""):
    global _writes
    try:
        with _lock:
            conn = _db()
            conn.execute(
                "INSERT INTO attempts (ts, via, account, ok, kind, phase, detail) VALUES (?,?,?,?,?,?,?)",
                (time.time(), via, account, 1 if ok else 0, kind or None, phase or None, (detail or "")[:300] or None),
            )
            _writes += 1
            if _writes % 500 == 0:
                conn.execute("DELETE FROM attempts WHERE ts < ?", (time.time() - LEDGER_RETENTION_DAYS * 86400,))
            conn.commit()
    except Exception as e:
        logger.warning(f"[LEDGER] write failed: {e}")


def summary(hours: float = 24) -> dict:
    since = time.time() - hours * 3600
    with _lock:
        conn = _db()
        paths = conn.execute(
            "SELECT via, account, COUNT(*), SUM(ok) FROM attempts WHERE ts >= ? GROUP BY via, account ORDER BY COUNT(*) DESC",
            (since,),
        ).fetchall()
        kinds = conn.execute(
            "SELECT via, account, kind, COUNT(*) FROM attempts WHERE ts >= ? AND ok = 0 GROUP BY via, account, kind ORDER BY COUNT(*) DESC",
            (since,),
        ).fetchall()
        unknown = conn.execute(
            "SELECT detail, COUNT(*) FROM attempts WHERE ts >= ? AND kind = 'other' GROUP BY detail ORDER BY COUNT(*) DESC LIMIT 10",
            (since,),
        ).fetchall()
    return {
        "hours": hours,
        "paths": [
            {"via": v, "account": a, "attempts": n, "ok": s or 0, "ok_pct": round(100 * (s or 0) / n, 1) if n else None}
            for v, a, n, s in paths
        ],
        "failures": [{"via": v, "account": a, "kind": k, "count": n} for v, a, k, n in kinds],
        "unknown_errors": [{"detail": d, "count": n} for d, n in unknown],
    }