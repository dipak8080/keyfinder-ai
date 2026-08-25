"""Who is making this request?

Anonymous users are identified by a signed cookie (af_sid) holding a random
subject id, plus a hash of their IP. Buying credits creates an account from
the checkout email; a magic link (step 3) links a browser to that account so
the balance follows them everywhere.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from fastapi import Request, Response

from .config import get_settings
from .db import connect, now_iso, tx
from .security import hash_ip, new_id, sign, unsign

SUBJECT_COOKIE = "af_sid"
SESSION_COOKIE = "af_session"
SUBJECT_PURPOSE = "subject"
SESSION_PURPOSE = "session"
SUBJECT_MAX_AGE = 60 * 60 * 24 * 730  # 2 years


@dataclass
class Identity:
    subject_id: str
    ip_hash: str
    account_id: str | None = None
    email: str | None = None
    session_id: str | None = None

    @property
    def owner(self) -> tuple[str, str]:
        return ("account", self.account_id) if self.account_id else ("subject", self.subject_id)

    @property
    def owner_key(self) -> str:
        kind, value = self.owner
        return f"{kind}:{value}"

    @property
    def is_authenticated(self) -> bool:
        return self.account_id is not None


def client_ip(request: Request) -> str:
    settings = get_settings()
    if settings.trust_cf_ip:
        cf = request.headers.get("cf-connecting-ip")
        if cf:
            return cf.strip()
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else ""


def _set_subject_cookie(response: Response, subject_id: str) -> None:
    s = get_settings()
    response.set_cookie(
        SUBJECT_COOKIE, sign(subject_id, purpose=SUBJECT_PURPOSE),
        max_age=SUBJECT_MAX_AGE, httponly=True, secure=s.cookie_secure,
        samesite=s.cookie_samesite, domain=s.cookie_domain, path="/",
    )


def set_session_cookie(response: Response, session_id: str, max_age: int) -> None:
    s = get_settings()
    response.set_cookie(
        SESSION_COOKIE, sign(session_id, purpose=SESSION_PURPOSE),
        max_age=max_age, httponly=True, secure=s.cookie_secure,
        samesite=s.cookie_samesite, domain=s.cookie_domain, path="/",
    )


def clear_session_cookie(response: Response) -> None:
    s = get_settings()
    response.delete_cookie(SESSION_COOKIE, domain=s.cookie_domain, path="/",
                           secure=s.cookie_secure, samesite=s.cookie_samesite)


def _load_session(conn: sqlite3.Connection, session_id: str) -> sqlite3.Row | None:
    return conn.execute(
        """SELECT s.id AS session_id, s.account_id, a.email, a.status
           FROM sessions s JOIN accounts a ON a.id = s.account_id
           WHERE s.id = ? AND s.revoked_at IS NULL AND s.expires_at > ?""",
        (session_id, now_iso()),
    ).fetchone()


def resolve_identity(request: Request, response: Response) -> Identity:
    """Read (and mint if needed) the caller's identity. Safe on every request."""
    ip_hash = hash_ip(client_ip(request))
    subject_id = unsign(request.cookies.get(SUBJECT_COOKIE), purpose=SUBJECT_PURPOSE)
    session_id = unsign(request.cookies.get(SESSION_COOKIE), purpose=SESSION_PURPOSE)

    minted = subject_id is None
    if minted:
        subject_id = new_id("sub_")

    account_id: str | None = None
    email: str | None = None
    live_session: str | None = None

    with connect() as conn, tx(conn):
        row = conn.execute("SELECT id, account_id FROM subjects WHERE id = ?", (subject_id,)).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO subjects (id, account_id, first_ip_hash, last_ip_hash, created_at, last_seen_at)"
                " VALUES (?, NULL, ?, ?, ?, ?)",
                (subject_id, ip_hash, ip_hash, now_iso(), now_iso()),
            )
        else:
            account_id = row["account_id"]
            conn.execute("UPDATE subjects SET last_ip_hash=?, last_seen_at=? WHERE id=?",
                        (ip_hash, now_iso(), subject_id))

        if session_id:
            srow = _load_session(conn, session_id)
            if srow and srow["status"] == "active":
                live_session = srow["session_id"]
                account_id = srow["account_id"]
                email = srow["email"]
                conn.execute(
                    "UPDATE subjects SET account_id=? WHERE id=? AND (account_id IS NULL OR account_id!=?)",
                    (account_id, subject_id, account_id),
                )
            else:
                session_id = None
        elif account_id:
            arow = conn.execute("SELECT email, status FROM accounts WHERE id=?", (account_id,)).fetchone()
            account_id = account_id if (arow and arow["status"] == "active") else None
            email = arow["email"] if arow and account_id else None

    if minted or request.cookies.get(SUBJECT_COOKIE) is None:
        _set_subject_cookie(response, subject_id)

    return Identity(subject_id=subject_id, ip_hash=ip_hash, account_id=account_id,
                    email=email, session_id=live_session)


def link_subject_to_account(conn: sqlite3.Connection, subject_id: str, account_id: str) -> None:
    conn.execute("UPDATE subjects SET account_id = ? WHERE id = ?", (account_id, subject_id))


def get_or_create_account(conn: sqlite3.Connection, email: str) -> str:
    email = email.strip().lower()
    row = conn.execute("SELECT id FROM accounts WHERE email = ?", (email,)).fetchone()
    if row:
        return row["id"]
    account_id = new_id("acc_")
    conn.execute("INSERT INTO accounts (id, email, status, created_at) VALUES (?, ?, 'active', ?)",
                (account_id, email, now_iso()))
    return account_id