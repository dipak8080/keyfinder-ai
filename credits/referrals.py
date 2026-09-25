"""Give N, get N: a friend's first purchase rewards both accounts.

Rewards are paid on a real purchase, never on sign-up, so throwaway
accounts earn nothing. First code wins, nobody can refer themselves or
their own network, and a refunded purchase takes both rewards back."""

from __future__ import annotations

import logging
import secrets
import sqlite3

from . import ledger as ledger_mod
from .config import get_settings
from .db import connect, now_iso, tx
from .identity import Identity

log = logging.getLogger("credits.referrals")

_ALPHABET = "abcdefghjkmnpqrstuvwxyz23456789"


def _new_code() -> str:
    return "".join(secrets.choice(_ALPHABET) for _ in range(8))


def code_for(conn: sqlite3.Connection, account_id: str) -> str:
    row = conn.execute("SELECT code FROM referral_codes WHERE account_id=?", (account_id,)).fetchone()
    if row:
        return row["code"]
    for _ in range(5):
        code = _new_code()
        if conn.execute("INSERT OR IGNORE INTO referral_codes (account_id, code, created_at) VALUES (?,?,?)",
                        (account_id, code, now_iso())).rowcount:
            return code
    raise RuntimeError("could not allocate a referral code")


def claim(identity: Identity, code: str) -> str:
    """Remembers the first referral code this browser arrived with.
    Returns 'ok' or the reason it was ignored."""
    s = get_settings()
    if not s.referral_enabled:
        return "disabled"
    code = (code or "").strip().lower()
    with connect() as conn, tx(conn):
        row = conn.execute("SELECT account_id FROM referral_codes WHERE code=?", (code,)).fetchone()
        if row is None:
            return "unknown_code"
        referrer = row["account_id"]
        if identity.account_id == referrer:
            return "own_code"
        same_network = conn.execute(
            "SELECT 1 FROM subjects WHERE account_id=? AND (last_ip_hash=? OR first_ip_hash=?) LIMIT 1",
            (referrer, identity.ip_hash, identity.ip_hash)).fetchone()
        if same_network:
            return "same_network"
        inserted = conn.execute(
            """INSERT OR IGNORE INTO referral_claims (subject_id, code, referrer_account_id, ip_hash, created_at)
               VALUES (?,?,?,?,?)""", (identity.subject_id, code, referrer, identity.ip_hash, now_iso())).rowcount
        if identity.account_id:
            attach(conn, identity.account_id)
    return "ok" if inserted else "already_claimed"


def attach(conn: sqlite3.Connection, account_id: str) -> None:
    """Links a browser's claim to a new customer account. Existing
    customers (any earlier paid order) are never attributed."""
    if conn.execute("SELECT 1 FROM referrals WHERE referee_account_id=?", (account_id,)).fetchone():
        return
    if conn.execute("SELECT 1 FROM orders WHERE account_id=? AND status='paid' LIMIT 1", (account_id,)).fetchone():
        return
    claim_row = conn.execute(
        """SELECT c.code, c.referrer_account_id FROM referral_claims c
           JOIN subjects s ON s.id=c.subject_id WHERE s.account_id=? ORDER BY c.created_at LIMIT 1""",
        (account_id,)).fetchone()
    if claim_row is None or claim_row["referrer_account_id"] == account_id:
        return
    conn.execute(
        """INSERT OR IGNORE INTO referrals (referee_account_id, referrer_account_id, code, status, created_at)
           VALUES (?,?,?, 'pending', ?)""", (account_id, claim_row["referrer_account_id"], claim_row["code"], now_iso()))


def on_paid_order(conn: sqlite3.Connection, account_id: str, payment_id: str) -> str | None:
    """Pays both sides when this is the referee's first paid order.
    Returns the referrer account id when the referrer was rewarded."""
    s = get_settings()
    if not s.referral_enabled:
        return None
    attach(conn, account_id)
    ref = conn.execute("SELECT * FROM referrals WHERE referee_account_id=? AND status='pending'",
                       (account_id,)).fetchone()
    if ref is None:
        return None
    paid = conn.execute("SELECT COUNT(*) AS n FROM orders WHERE account_id=? AND status='paid'",
                        (account_id,)).fetchone()["n"]
    if paid != 1:
        conn.execute("UPDATE referrals SET status='not_first_order' WHERE referee_account_id=?", (account_id,))
        return None
    reward = s.referral_reward_credits
    rewarded_this_month = conn.execute(
        """SELECT COUNT(*) AS n FROM referrals WHERE referrer_account_id=? AND status='rewarded'
           AND rewarded_at >= strftime('%Y-%m-01T00:00:00','now')""", (ref["referrer_account_id"],)).fetchone()["n"]
    referrer_paid = rewarded_this_month < s.referral_monthly_cap
    ledger_mod.grant(conn, owner_type="account", owner_id=account_id, amount=reward, kind="bonus",
                     idempotency_key=f"referral_referee:{account_id}", order_id=payment_id,
                     note="referral: welcome credits")
    if referrer_paid:
        ledger_mod.grant(conn, owner_type="account", owner_id=ref["referrer_account_id"], amount=reward,
                         kind="bonus", idempotency_key=f"referral_referrer:{account_id}", order_id=payment_id,
                         note="referral: friend's first purchase")
    conn.execute("UPDATE referrals SET status=?, order_id=?, rewarded_at=? WHERE referee_account_id=?",
                 ("rewarded" if referrer_paid else "capped", payment_id, now_iso(), account_id))
    log.info("referral %s -> %s rewarded (%s credits each, referrer paid=%s)",
             ref["referrer_account_id"], account_id, reward, referrer_paid)
    return ref["referrer_account_id"] if referrer_paid else None


def reverse_for_payment(conn: sqlite3.Connection, payment_id: str) -> int:
    """Takes back referral rewards paid for a purchase that was refunded
    or lost in a dispute. Only unspent credits; returns credits taken."""
    ref = conn.execute("SELECT * FROM referrals WHERE order_id=? AND status IN ('rewarded','capped')",
                       (payment_id,)).fetchone()
    if ref is None:
        return 0
    taken = 0
    for owner, key in ((ref["referee_account_id"], f"referral_referee:{ref['referee_account_id']}"),
                       (ref["referrer_account_id"], f"referral_referrer:{ref['referee_account_id']}")):
        granted = conn.execute("SELECT delta FROM credit_ledger WHERE idempotency_key=?", (key,)).fetchone()
        if granted is None:
            continue
        balance = conn.execute("SELECT COALESCE(SUM(delta),0) AS b FROM credit_ledger WHERE owner_type='account'"
                               " AND owner_id=?", (owner,)).fetchone()["b"]
        amount = max(0, min(int(granted["delta"]), int(balance)))
        ledger_mod.grant(conn, owner_type="account", owner_id=owner, amount=-amount, kind="chargeback",
                         idempotency_key=f"{key}:reversed", note="referral reversed after refund")
        taken += amount
    conn.execute("UPDATE referrals SET status='reversed' WHERE referee_account_id=?", (ref["referee_account_id"],))
    return taken


def summary(account_id: str) -> dict:
    s = get_settings()
    with connect() as conn, tx(conn):
        code = code_for(conn, account_id)
        counts = {r["status"]: r["n"] for r in conn.execute(
            "SELECT status, COUNT(*) AS n FROM referrals WHERE referrer_account_id=? GROUP BY status",
            (account_id,)).fetchall()}
    return {
        "enabled": s.referral_enabled,
        "code": code,
        "link": f"{s.frontend_url}/?ref={code}",
        "reward_credits": s.referral_reward_credits,
        "friends_rewarded": counts.get("rewarded", 0),
        "friends_pending": counts.get("pending", 0),
        "credits_earned": counts.get("rewarded", 0) * s.referral_reward_credits,
    }