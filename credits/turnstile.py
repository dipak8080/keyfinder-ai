"""Cloudflare Turnstile gate for free GPU runs.

Off unless TURNSTILE_SECRET_KEY is set. A visitor gets
TURNSTILE_FREE_RUNS_BEFORE_CHALLENGE non-credit GPU runs per day, then a
one-time challenge unlocks TURNSTILE_PASS_HOURS more. Paid runs bypass it.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from .config import get_settings
from .db import connect, now_iso, tx

log = logging.getLogger("credits.turnstile")

SITEVERIFY = "https://challenges.cloudflare.com/turnstile/v0/siteverify"


def enabled() -> bool:
    s = get_settings()
    return bool(s.turnstile_secret_key) and s.turnstile_free_runs_before_challenge > 0


def runs_today(ip_hash: str) -> int:
    with connect() as conn:
        row = conn.execute(
            """SELECT COUNT(*) AS n FROM gpu_job_metrics
               WHERE ip_hash=? AND COALESCE(charge_type, 'none') != 'credit'
                 AND created_at >= strftime('%Y-%m-%dT00:00:00', 'now')""",
            (ip_hash,),
        ).fetchone()
    return int(row["n"] or 0)


def is_passed(ip_hash: str) -> bool:
    with connect() as conn:
        row = conn.execute(
            "SELECT verified_until FROM turnstile_passes WHERE ip_hash=?", (ip_hash,)
        ).fetchone()
    return bool(row) and row["verified_until"] > now_iso()


def needs_challenge(ip_hash: str) -> bool:
    if not enabled():
        return False
    if runs_today(ip_hash) < get_settings().turnstile_free_runs_before_challenge:
        return False
    return not is_passed(ip_hash)


def mark_passed(ip_hash: str) -> None:
    s = get_settings()
    until = (datetime.now(timezone.utc) + timedelta(hours=s.turnstile_pass_hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    now = now_iso()
    with connect() as conn, tx(conn):
        conn.execute(
            """INSERT INTO turnstile_passes (ip_hash, verified_until, passes, created_at, updated_at)
               VALUES (?,?,1,?,?)
               ON CONFLICT(ip_hash) DO UPDATE SET verified_until=excluded.verified_until,
                 passes=passes+1, updated_at=excluded.updated_at""",
            (ip_hash, until, now, now),
        )


async def verify(token: str, remote_ip: str | None) -> bool:
    import httpx

    secret = get_settings().turnstile_secret_key
    if not secret or not token:
        return False
    data = {"secret": secret, "response": token}
    if remote_ip:
        data["remoteip"] = remote_ip
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            res = await client.post(SITEVERIFY, data=data)
        body = res.json()
        ok = bool(body.get("success"))
        if not ok:
            log.info("turnstile rejected: %s", body.get("error-codes"))
        return ok
    except Exception:  # noqa: BLE001
        log.exception("turnstile siteverify failed")
        return False


def stats(days: int = 7) -> dict:
    with connect() as conn:
        row = conn.execute(
            """SELECT COUNT(*) AS solved,
                      SUM(CASE WHEN verified_until > strftime('%Y-%m-%dT%H:%M:%SZ','now') THEN 1 ELSE 0 END) AS active
               FROM turnstile_passes
               WHERE updated_at >= strftime('%Y-%m-%dT%H:%M:%SZ','now',?)""",
            (f"-{days} days",),
        ).fetchone()
    return {"solved": int(row["solved"] or 0), "active": int(row["active"] or 0), "enabled": enabled()}