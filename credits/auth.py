"""Passwordless auth. A Ko-fi purchase creates the account; this is how
someone gets back into it from a different browser than the one they bought
from."""

from __future__ import annotations

import logging
import sqlite3
from datetime import timedelta
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, EmailStr

from . import ledger as ledger_mod
from . import mailer, paywall
from .config import get_settings
from .db import connect, now_iso, tx, utcnow
from .identity import (
    Identity, SUBJECT_COOKIE, SUBJECT_PURPOSE,
    clear_session_cookie, get_or_create_account, link_subject_to_account, set_session_cookie,
)
from .security import hash_token, new_id, new_token, unsign

log = logging.getLogger("credits.auth")
router = APIRouter(prefix="/auth", tags=["auth"])


class MagicLinkRequest(BaseModel):
    email: EmailStr


def issue_magic_link(conn: sqlite3.Connection, *, email: str, subject_id: str | None,
                     ip_hash: str | None, ttl_minutes: int | None = None,
                     purpose: str = "login") -> str:
    """Create a one-time token, return the full verify URL. Caller emails it."""
    s = get_settings()
    token = new_token(32)
    ttl = ttl_minutes if ttl_minutes is not None else s.magic_link_ttl_minutes
    expires = (utcnow() + timedelta(minutes=ttl)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    conn.execute(
        """INSERT INTO magic_links (token_hash, email, subject_id, purpose, ip_hash, created_at, expires_at)
           VALUES (?,?,?,?,?,?,?)""",
        (hash_token(token), email.strip().lower(), subject_id, purpose, ip_hash, now_iso(), expires),
    )
    return f"{s.api_base_url}/auth/verify?token={quote(token)}"


@router.post("/magic-link")
async def request_magic_link(body: MagicLinkRequest, identity: Identity = Depends(paywall.get_identity)) -> dict:
    s = get_settings()
    email = body.email.strip().lower()

    with connect() as conn:
        recent = conn.execute(
            """SELECT COUNT(*) AS n FROM magic_links WHERE (email=? OR ip_hash=?)
               AND created_at > strftime('%Y-%m-%dT%H:%M:%SZ','now','-1 hour')""",
            (email, identity.ip_hash),
        ).fetchone()
        if recent["n"] >= s.magic_links_per_hour:
            raise HTTPException(status_code=429, detail={"error": "too_many_requests",
                                                         "message": "Too many sign-in emails. Try again in an hour."})
        with tx(conn):
            link = issue_magic_link(conn, email=email, subject_id=identity.subject_id, ip_hash=identity.ip_hash)

    subject, html, text = mailer.magic_link_email(link, s.magic_link_ttl_minutes)
    try:
        await mailer.send_email(email, subject, html, text)
    except Exception:  # noqa: BLE001
        log.exception("failed to send magic link to %s", email)
        raise HTTPException(status_code=502, detail={"error": "email_failed"})
    return {"ok": True, "message": "Check your email for the sign-in link."}


@router.get("/verify")
async def verify(request: Request, token: str = "") -> RedirectResponse:
    s = get_settings()

    def _fail(status: str) -> RedirectResponse:
        """Four statuses, not two.

        'invalid' reads like an accusation, and for the most common real
        cause - a link opened 40 minutes after it was emailed - it is
        simply wrong. 'expired' and 'used' are both RECOVERABLE states
        with obvious next actions ("send another", "you're already
        signed in on this device"), and telling them apart is the
        difference between a user retrying and a user emailing support.

        Enumeration is not a concern here: reaching any of these
        requires already holding a 256-bit token, so the extra detail
        reveals nothing to someone who doesn't.
        """
        return RedirectResponse(
            f"{s.frontend_url}/auth/verified?status={status}", status_code=303
        )

    if not token:
        return _fail("invalid")

    token_h = hash_token(token)
    with connect() as conn, tx(conn):
        row = conn.execute("SELECT * FROM magic_links WHERE token_hash=?", (token_h,)).fetchone()
        if row is None:
            return _fail("invalid")
        if row["used_at"] is not None:
            return _fail("used")
        if row["expires_at"] <= now_iso():
            return _fail("expired")
        conn.execute("UPDATE magic_links SET used_at=? WHERE token_hash=?", (now_iso(), token_h))

        account_id = get_or_create_account(conn, row["email"])
        conn.execute("UPDATE accounts SET last_login_at=? WHERE id=?", (now_iso(), account_id))

        subject_id = unsign(request.cookies.get(SUBJECT_COOKIE), purpose=SUBJECT_PURPOSE) or row["subject_id"]
        if subject_id:
            existing = conn.execute("SELECT id FROM subjects WHERE id=?", (subject_id,)).fetchone()
            if existing is None:
                conn.execute(
                    "INSERT INTO subjects (id, account_id, first_ip_hash, last_ip_hash, created_at, last_seen_at)"
                    " VALUES (?,?,NULL,NULL,?,?)", (subject_id, account_id, now_iso(), now_iso()),
                )
            else:
                link_subject_to_account(conn, subject_id, account_id)
            ledger_mod.merge_free_usage(conn, subject_id, account_id)

        session_id = new_id("ses_")
        expires = (utcnow() + timedelta(days=s.session_ttl_days)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        conn.execute(
            "INSERT INTO sessions (id, account_id, subject_id, created_at, expires_at) VALUES (?,?,?,?,?)",
            (session_id, account_id, subject_id, now_iso(), expires),
        )

    resp = RedirectResponse(f"{s.frontend_url}/auth/verified?status=ok", status_code=303)
    set_session_cookie(resp, session_id, max_age=s.session_ttl_days * 86400)
    return resp


@router.post("/logout")
async def logout(response: Response, identity: Identity = Depends(paywall.get_identity)) -> dict:
    """Detach this browser: revoke the session and unlink the subject. Nothing
    is deleted — signing in again from any device restores the same credits."""
    with connect() as conn, tx(conn):
        if identity.session_id:
            conn.execute("UPDATE sessions SET revoked_at=? WHERE id=?", (now_iso(), identity.session_id))
        conn.execute("UPDATE subjects SET account_id=NULL WHERE id=?", (identity.subject_id,))
    clear_session_cookie(response)
    return {"ok": True}


@router.post("/device-link")
async def device_link(identity: Identity = Depends(paywall.get_identity)) -> dict:
    """A sign-in link for the caller's OWN account, returned in the body
    instead of emailed - so the frontend can render it as a QR code.

    THE PROBLEM THIS SOLVES
    -----------------------
    Credits live on an account, and a browser reaches that account
    through its af_sid cookie. That makes the buying device work with
    nothing to type - genuinely less friction than any competitor, who
    all demand a signup before you can even preview.

    The cost lands entirely on the SECOND device. Someone who buys on a
    laptop and later opens their phone sees a zero balance, because the
    phone is a different browser with a different cookie. The receipt
    email's magic link fixes it, but that is an app switch, an inbox, a
    search, and a tap - at the exact moment they are trying to use the
    thing they just paid for.

    A QR code collapses that to roughly four seconds: point the phone
    camera at the laptop screen, tap the notification, done. No email,
    no typing, no app switch.

    WHY THIS IS NOT A NEW SECURITY SURFACE
    --------------------------------------
    It mints nothing the caller could not already get. To reach this
    route you must already hold a cookie linked to the account - the
    same cookie that already displays the balance and can already spend
    every credit on it. Handing that caller a link to their own account
    grants zero additional authority.

    Contrast with /auth/magic-link, which anyone may call for any
    address: that one is emailed precisely because the caller has not
    proven anything, and delivery to the inbox IS the proof. Here the
    cookie is the proof, so the body is the right channel.

    Three constraints that do matter:

      1. SHORT TTL (5 min, vs 30 for email). This link is displayed on a
         screen. A screenshot, a screen share, or someone behind you
         should not carry a working credential for half an hour.
      2. SINGLE USE - inherited from the magic_links.used_at check in
         verify(). Scanning it consumes it.
      3. RATE LIMITED per account, bounding a compromised session
         minting links in bulk.

    Returns 401 rather than minting anything for an anonymous caller.
    That is not a real user path - the frontend only shows this button
    when a balance is present - but the check has to exist, because
    without it this route would email-lessly hand a session to whoever
    asked.
    """
    s = get_settings()

    if not identity.account_id or not identity.email:
        # No account linked to this browser. Nothing to share, and
        # nothing we could safely invent.
        raise HTTPException(status_code=401, detail={
            "kind": "not_linked",
            "message": "This browser isn't linked to an account yet.",
        })

    with connect() as conn:
        recent = conn.execute(
            """SELECT COUNT(*) AS n FROM magic_links
               WHERE email=? AND purpose='device_link'
                 AND created_at > strftime('%Y-%m-%dT%H:%M:%SZ','now','-1 hour')""",
            (identity.email,),
        ).fetchone()
        if recent["n"] >= s.device_links_per_hour:
            raise HTTPException(status_code=429, detail={
                "kind": "rate_limited",
                "message": "Too many device links. Try again in an hour.",
            })

        with tx(conn):
            link = issue_magic_link(
                conn,
                email=identity.email,
                subject_id=None,          # the SCANNING device supplies its own
                ip_hash=identity.ip_hash,
                ttl_minutes=s.device_link_ttl_minutes,
                purpose="device_link",
            )

    log.info("issued device link for %s (expires in %dm)", identity.email, s.device_link_ttl_minutes)
    return {
        "url": link,
        "expires_in_seconds": s.device_link_ttl_minutes * 60,
        "email": identity.email,
    }