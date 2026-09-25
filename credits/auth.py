"""Passwordless auth. A purchase creates the account; this is how
someone gets back into it from a different browser than the one they bought
from."""

from __future__ import annotations

import base64
import json
import logging
import sqlite3
from datetime import timedelta
from urllib.parse import quote, urlencode

import httpx

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, EmailStr

from . import ledger as ledger_mod
from . import mailer, paywall
from .config import get_settings
from .db import connect, now_iso, tx, utcnow
from .identity import (
    Identity, SUBJECT_COOKIE, SUBJECT_PURPOSE,
    clear_session_cookie, client_ip, get_or_create_account, link_subject_to_account, set_session_cookie,
)
from .security import hash_ip, hash_token, new_id, new_token, sign, unsign

log = logging.getLogger("credits.auth")
router = APIRouter(prefix="/auth", tags=["auth"])


class MagicLinkRequest(BaseModel):
    email: EmailStr
    updates: bool = False


def issue_magic_link(conn: sqlite3.Connection, *, email: str, subject_id: str | None,
                     ip_hash: str | None, ttl_minutes: int | None = None,
                     purpose: str = "login", updates: bool = False) -> str:
    """Create a one-time token, return the full verify URL. Caller emails it."""
    s = get_settings()
    token = new_token(32)
    ttl = ttl_minutes if ttl_minutes is not None else s.magic_link_ttl_minutes
    expires = (utcnow() + timedelta(minutes=ttl)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    conn.execute(
        """INSERT INTO magic_links (token_hash, email, subject_id, purpose, ip_hash, created_at, expires_at,
               email_updates) VALUES (?,?,?,?,?,?,?,?)""",
        (hash_token(token), email.strip().lower(), subject_id, purpose, ip_hash, now_iso(), expires,
         1 if updates else 0),
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
            link = issue_magic_link(conn, email=email, subject_id=identity.subject_id, ip_hash=identity.ip_hash,
                                    updates=body.updates)

    subject, html, text = mailer.magic_link_email(link, s.magic_link_ttl_minutes)
    try:
        await mailer.send_email(email, subject, html, text)
    except Exception:  # noqa: BLE001
        log.exception("failed to send magic link to %s", email)
        raise HTTPException(status_code=502, detail={"error": "email_failed"})
    return {"ok": True, "message": "Check your email for the sign-in link."}


def _grant_signup_bonus(conn: sqlite3.Connection, account_id: str, ip_hash: str | None,
                        method: str) -> int:
    """Welcome credits for a brand-new account, capped per network."""
    s = get_settings()
    credits = s.signup_bonus_credits
    if credits <= 0:
        return 0
    if ip_hash and ip_hash != "unknown":
        recent = conn.execute(
            """SELECT COUNT(*) AS n FROM signup_bonuses WHERE ip_hash=?
               AND granted_at > strftime('%Y-%m-%dT%H:%M:%SZ','now','-30 days')""",
            (ip_hash,),
        ).fetchone()
        if recent["n"] >= s.signup_bonus_per_ip_30d:
            log.info("signup bonus skipped for %s: network cap reached", account_id)
            return 0
    inserted = conn.execute(
        "INSERT OR IGNORE INTO signup_bonuses (account_id, ip_hash, credits, method, granted_at)"
        " VALUES (?,?,?,?,?)", (account_id, ip_hash, credits, method, now_iso()),
    ).rowcount
    if not inserted:
        return 0
    ledger_mod.grant(conn, owner_type="account", owner_id=account_id, amount=credits, kind="bonus",
                     idempotency_key=f"signup_bonus:{account_id}", note=f"welcome bonus ({method})")
    return credits


def _complete_sign_in(conn: sqlite3.Connection, request: Request, *, email: str,
                      fallback_subject_id: str | None, ip_hash: str | None,
                      method: str, updates: bool = False) -> tuple[str, int]:
    """Shared by every sign-in method. Returns (session_id, bonus_credits)."""
    s = get_settings()
    is_new = conn.execute("SELECT 1 FROM accounts WHERE email=?", (email,)).fetchone() is None
    account_id = get_or_create_account(conn, email)
    conn.execute("UPDATE accounts SET last_login_at=? WHERE id=?", (now_iso(), account_id))
    if updates:
        conn.execute("UPDATE accounts SET email_updates=1 WHERE id=?", (account_id,))

    subject_id = unsign(request.cookies.get(SUBJECT_COOKIE), purpose=SUBJECT_PURPOSE) or fallback_subject_id
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
        from . import referrals
        referrals.attach(conn, account_id)

    bonus = _grant_signup_bonus(conn, account_id, ip_hash, method) if is_new else 0

    session_id = new_id("ses_")
    expires = (utcnow() + timedelta(days=s.session_ttl_days)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    conn.execute(
        "INSERT INTO sessions (id, account_id, subject_id, created_at, expires_at) VALUES (?,?,?,?,?)",
        (session_id, account_id, subject_id, now_iso(), expires),
    )
    return session_id, bonus


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
        session_id, bonus = _complete_sign_in(
            conn, request, email=row["email"], fallback_subject_id=row["subject_id"],
            ip_hash=row["ip_hash"], method="email", updates=bool(row["email_updates"]),
        )

    query = "status=ok" + (f"&bonus={bonus}" if bonus else "")
    resp = RedirectResponse(f"{s.frontend_url}/auth/verified?{query}", status_code=303)
    set_session_cookie(resp, session_id, max_age=s.session_ttl_days * 86400)
    return resp


GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://openidconnect.googleapis.com/v1/userinfo"
GOOGLE_STATE_COOKIE = "af_google_state"
GOOGLE_STATE_PURPOSE = "google_oauth_state"
GOOGLE_STATE_MAX_AGE = 600


def _google_redirect_uri() -> str:
    return f"{get_settings().api_base_url}/auth/google/callback"


def _safe_next(path: str | None) -> str:
    """A same-site path only. Control characters, backslashes and
    protocol-relative forms are refused, raw or percent-encoded."""
    from urllib.parse import unquote
    if not path:
        return "/"
    for candidate in (path, unquote(path), unquote(unquote(path))):
        if (not candidate.startswith("/") or candidate.startswith("//") or "\\" in candidate
                or any(ord(ch) < 32 or ord(ch) == 127 for ch in candidate)):
            return "/"
    return path[:300]


def _encode_state(data: dict) -> str:
    raw = base64.urlsafe_b64encode(json.dumps(data, separators=(",", ":")).encode()).decode().rstrip("=")
    return sign(raw, purpose=GOOGLE_STATE_PURPOSE)


def _decode_state(cookie: str | None) -> dict | None:
    raw = unsign(cookie, purpose=GOOGLE_STATE_PURPOSE)
    if not raw:
        return None
    try:
        return json.loads(base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4)))
    except ValueError:
        return None


@router.get("/google/start")
async def google_start(next: str = "/", updates: bool = False,
                       identity: Identity = Depends(paywall.get_identity)) -> RedirectResponse:
    s = get_settings()
    if not s.google_client_id or not s.google_client_secret:
        raise HTTPException(status_code=503, detail={"error": "google_signin_unavailable"})
    nonce = new_token(24)
    params = {
        "client_id": s.google_client_id,
        "redirect_uri": _google_redirect_uri(),
        "response_type": "code",
        "scope": "openid email profile",
        "state": nonce,
        "prompt": "select_account",
    }
    resp = RedirectResponse(f"{GOOGLE_AUTH_URL}?{urlencode(params)}", status_code=303)
    resp.set_cookie(
        GOOGLE_STATE_COOKIE,
        _encode_state({"n": nonce, "next": _safe_next(next), "sub": identity.subject_id, "u": 1 if updates else 0}),
        max_age=GOOGLE_STATE_MAX_AGE, httponly=True, secure=s.cookie_secure,
        samesite="lax", path="/auth/google",
    )
    return resp


@router.get("/google/callback")
async def google_callback(request: Request, code: str = "", state: str = "", error: str = "") -> RedirectResponse:
    s = get_settings()
    saved = _decode_state(request.cookies.get(GOOGLE_STATE_COOKIE))

    def _done(query: str) -> RedirectResponse:
        resp = RedirectResponse(f"{s.frontend_url}/auth/verified?{query}", status_code=303)
        resp.delete_cookie(GOOGLE_STATE_COOKIE, path="/auth/google", secure=s.cookie_secure, samesite="lax")
        return resp

    next_path = quote(saved["next"] if saved else "/", safe="")
    if error:
        return _done(f"status=cancelled&method=google&next={next_path}")
    if not saved or not code or not state or state != saved.get("n"):
        return _done("status=invalid&method=google")

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            token_res = await client.post(GOOGLE_TOKEN_URL, data={
                "code": code,
                "client_id": s.google_client_id,
                "client_secret": s.google_client_secret,
                "redirect_uri": _google_redirect_uri(),
                "grant_type": "authorization_code",
            })
            token_res.raise_for_status()
            access_token = token_res.json().get("access_token")
            if not access_token:
                raise ValueError("no access_token in Google response")
            info_res = await client.get(GOOGLE_USERINFO_URL, headers={"Authorization": f"Bearer {access_token}"})
            info_res.raise_for_status()
            info = info_res.json()
    except (httpx.HTTPError, ValueError):
        log.exception("Google sign-in exchange failed")
        return _done("status=error&method=google")

    email = (info.get("email") or "").strip().lower()
    if not email or info.get("email_verified") is not True:
        return _done("status=unverified&method=google")

    ip_hash = hash_ip(client_ip(request))
    with connect() as conn, tx(conn):
        session_id, bonus = _complete_sign_in(
            conn, request, email=email, fallback_subject_id=saved.get("sub"),
            ip_hash=ip_hash, method="google", updates=bool(saved.get("u")),
        )

    query = f"status=ok&method=google&next={next_path}" + (f"&bonus={bonus}" if bonus else "")
    resp = _done(query)
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