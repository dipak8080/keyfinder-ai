"""Pending email claims, recorded when a checkout starts.

Browser linking now goes through order_sources.subject_id (see fulfil.py),
so a claim is only consumed on payment, and used for linking only when a
payment carries no checkout reference. Best-effort: the receipt email's
magic link recovers a purchase on any device.
"""

from __future__ import annotations

import sqlite3
from datetime import timedelta

from .config import get_settings
from .db import connect, now_iso, tx, utcnow


def record_claim(email: str, subject_id: str, pack: str, ip_hash: str | None) -> None:
    s = get_settings()
    email = email.strip().lower()
    expires = (utcnow() + timedelta(minutes=s.claim_ttl_minutes)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    with connect() as conn, tx(conn):
        conn.execute(
            """INSERT INTO pending_claims (email, subject_id, pack, ip_hash, created_at, expires_at)
               VALUES (?,?,?,?,?,?)
               ON CONFLICT(email) DO UPDATE SET
                 subject_id=excluded.subject_id, pack=excluded.pack,
                 ip_hash=excluded.ip_hash, created_at=excluded.created_at,
                 expires_at=excluded.expires_at, claimed_at=NULL""",
            (email, subject_id, pack, ip_hash, now_iso(), expires),
        )


def take_claim(conn: sqlite3.Connection, email: str) -> sqlite3.Row | None:
    """Look up and consume a pending claim for this email. Returns None if
    there isn't one, it expired, or it was already claimed (replayed webhook)."""
    email = email.strip().lower()
    row = conn.execute(
        "SELECT * FROM pending_claims WHERE email=? AND claimed_at IS NULL AND expires_at > ?",
        (email, now_iso()),
    ).fetchone()
    if row is None:
        return None
    conn.execute("UPDATE pending_claims SET claimed_at=? WHERE email=?", (now_iso(), email))
    return row