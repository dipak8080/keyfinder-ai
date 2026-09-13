"""
idempotency.py - Replay protection for POST requests that spend money.

THE HOLE THIS CLOSES. Every metered submit route reads the upload, probes
the duration, charges, then spawns. A client whose request times out
somewhere in the first three steps has no way to know whether the charge
landed, so it retries - and the retry gets a fresh job_id, which is what
paywall.guard keys its own idempotency on. Second upload, second charge,
second GPU run, one user. separation_upgrade.py is the only route that
was safe, because it dedupes on the SOURCE job id.

HOW IT WORKS. A client sends `Idempotency-Key: <uuid>` on a POST. The
first request through reserves that key and runs normally; its JSON
response is stored. Any later request with the same key and the same
subject and path gets the stored response back without the route ever
running. A duplicate that arrives while the first is still in flight
gets a 409 rather than a second charge.

NO HEADER, NO BEHAVIOUR CHANGE. A request without the header passes
straight through, which is what every existing client does today. This
can ship before the frontend sends anything.

SEPARATE DATABASE, deliberately. The credits DB has a migration chain
and a schema that money depends on; a replay cache does not belong in
it. This file owns its own SQLite file and creates its own table on
first use, so there is nothing to migrate and nothing to roll back.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, Response

logger = logging.getLogger(__name__)

HEADER = "idempotency-key"
SUBJECT_COOKIE = "af_sid"

# How long a stored response stays replayable. Long enough to cover a
# client retrying across a network outage, short enough that the table
# stays small without a cron job.
TTL_SECONDS = int(os.environ.get("IDEMPOTENCY_TTL_SECONDS", "3600"))

# A reservation older than this with no response recorded is treated as
# abandoned - the process that made it died mid-request. Must be longer
# than the slowest synchronous submit (upload + ffprobe + charge).
STALE_INFLIGHT_SECONDS = int(os.environ.get("IDEMPOTENCY_STALE_SECONDS", "300"))

# Responses above this are not stored. Submit routes answer with a small
# JSON body; anything larger is not something worth replaying.
MAX_STORED_BYTES = 64 * 1024

_DB_PATH = os.environ.get("IDEMPOTENCY_DB_PATH", "")

_sweep_lock = asyncio.Lock()
_last_sweep = 0.0
_db_ready = False


def _db_path() -> str:
    if _DB_PATH:
        return _DB_PATH
    try:
        from credits.config import get_settings
        base = Path(get_settings().db_path).parent
    except Exception:
        base = Path("data")
    return str(base / "idempotency.db")


@contextmanager
def _connect():
    """Caller gets a closed connection on exit. `with sqlite3.connect(...)`
    is a TRANSACTION context manager, not a closing one, so the handles
    here were only ever released by refcount.

    Schema and the persistent PRAGMAs run once per process rather than on
    every call. journal_mode and synchronous are stored in the database
    file, so re-issuing them per request bought nothing; busy_timeout is
    per-connection and stays. This middleware runs on every POST, and the
    setup measured 179us against 28us without it."""
    global _db_ready
    path = _db_path()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout=30000")
        if not _db_ready:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute(
                """CREATE TABLE IF NOT EXISTS idempotency (
                       key         TEXT PRIMARY KEY,
                       created_at  REAL NOT NULL,
                       status_code INTEGER,
                       body        TEXT
                   )"""
            )
            _db_ready = True
        yield conn
    finally:
        conn.close()


def _fingerprint(subject: str, path: str, supplied: str) -> str:
    return hashlib.sha256(f"{subject}|{path}|{supplied}".encode("utf-8")).hexdigest()


def _reserve(key: str) -> tuple[str, int | None, str | None]:
    """Claim the key, or report what is already there.

    Returns one of:
        ("fresh", None, None)        nothing existed, we own it now
        ("replay", status, body)     a completed response is stored
        ("in_flight", None, None)    someone else is mid-request

    The INSERT is the lock. Two racing duplicates cannot both get
    "fresh" because the primary key rejects the second one.
    """
    now = time.time()
    with _connect() as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO idempotency (key, created_at) VALUES (?, ?)",
            (key, now),
        )
        if cur.rowcount == 1:
            return "fresh", None, None

        row = conn.execute(
            "SELECT created_at, status_code, body FROM idempotency WHERE key = ?",
            (key,),
        ).fetchone()
        if row is None:
            # Swept between the insert and the read. Treat as ours.
            conn.execute(
                "INSERT OR REPLACE INTO idempotency (key, created_at) VALUES (?, ?)",
                (key, now),
            )
            return "fresh", None, None

        if row["status_code"] is not None:
            return "replay", int(row["status_code"]), row["body"]

        if now - float(row["created_at"]) > STALE_INFLIGHT_SECONDS:
            # The request that reserved this never finished. Take it over
            # rather than blocking the caller forever.
            conn.execute(
                "UPDATE idempotency SET created_at = ?, status_code = NULL, body = NULL "
                "WHERE key = ?",
                (now, key),
            )
            return "fresh", None, None

        return "in_flight", None, None


def _store(key: str, status_code: int, body: str) -> None:
    with _connect() as conn:
        conn.execute(
            "UPDATE idempotency SET status_code = ?, body = ? WHERE key = ?",
            (status_code, body, key),
        )


def _release(key: str) -> None:
    """Drop the reservation so the caller can legitimately try again.

    Called for every non-2xx outcome. A 402, a 429 or a 500 must not be
    replayed: the user fixes the cause and presses the button again, and
    that second press has to reach the route.
    """
    with _connect() as conn:
        conn.execute("DELETE FROM idempotency WHERE key = ?", (key,))


def _sweep() -> None:
    with _connect() as conn:
        conn.execute(
            "DELETE FROM idempotency WHERE created_at < ?", (time.time() - TTL_SECONDS,)
        )


async def _maybe_sweep() -> None:
    global _last_sweep
    if time.time() - _last_sweep < 600:
        return
    async with _sweep_lock:
        if time.time() - _last_sweep < 600:
            return
        _last_sweep = time.time()
    try:
        await asyncio.to_thread(_sweep)
    except Exception:
        logger.warning("[IDEMPOTENCY] sweep failed", exc_info=True)


async def _read_body(response: Response) -> bytes:
    chunks = []
    total = 0
    async for chunk in response.body_iterator:
        if isinstance(chunk, str):
            chunk = chunk.encode("utf-8")
        total += len(chunk)
        if total > MAX_STORED_BYTES:
            chunks.append(chunk)
            async for rest in response.body_iterator:
                chunks.append(rest.encode("utf-8") if isinstance(rest, str) else rest)
            return b"".join(chunks)
        chunks.append(chunk)
    return b"".join(chunks)


class IdempotencyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        supplied = request.headers.get(HEADER)
        if request.method != "POST" or not supplied or len(supplied) > 200:
            return await call_next(request)

        await _maybe_sweep()

        subject = request.cookies.get(SUBJECT_COOKIE) or "anon"
        key = _fingerprint(subject, request.url.path, supplied)

        try:
            state, status, body = await asyncio.to_thread(_reserve, key)
        except Exception:
            # The replay cache must never be the reason a submit fails.
            logger.warning("[IDEMPOTENCY] reserve failed, passing through", exc_info=True)
            return await call_next(request)

        if state == "replay":
            logger.info(f"[IDEMPOTENCY] replaying {request.url.path} for key {supplied[:12]}")
            return Response(
                content=body or "",
                status_code=status or 200,
                media_type="application/json",
                headers={"Idempotent-Replay": "true"},
            )

        if state == "in_flight":
            logger.info(f"[IDEMPOTENCY] duplicate in flight on {request.url.path}")
            return JSONResponse(
                status_code=409,
                content={
                    "kind": "duplicate_request",
                    "message": "This request is already running. Wait for it to finish rather than starting it again.",
                },
            )

        try:
            response = await call_next(request)
        except Exception:
            try:
                await asyncio.to_thread(_release, key)
            except Exception:
                pass
            raise

        if response.status_code >= 300:
            try:
                await asyncio.to_thread(_release, key)
            except Exception:
                pass
            return response

        raw = await _read_body(response)
        headers = dict(response.headers)
        headers.pop("content-length", None)

        if len(raw) <= MAX_STORED_BYTES:
            try:
                await asyncio.to_thread(_store, key, response.status_code, raw.decode("utf-8"))
            except Exception:
                logger.warning("[IDEMPOTENCY] store failed", exc_info=True)
        else:
            try:
                await asyncio.to_thread(_release, key)
            except Exception:
                pass

        return Response(
            content=raw,
            status_code=response.status_code,
            headers=headers,
            media_type=response.media_type,
        )