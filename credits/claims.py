"""Ko-fi's webhook carries no custom data — only the buyer's email. So before
sending someone to a ko-fi.com/s/<code> link, we record which browser (subject)
is about to buy which pack, keyed by the email they're about to pay with. When
the webhook lands, we match on email and credit that same browser silently.

Best-effort by design: if the match misses (different email at checkout,
claim expired), the purchase still succeeds — the receipt email's magic link
recovers it on any device. Nothing is ever blocked by this table; it only
makes the common case (buy → see credits in the same tab) work smoothly.
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