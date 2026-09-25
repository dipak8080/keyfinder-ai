"""Studio Pass credit lots.

Every paid Pass cycle is one lot. Credits from a lot are spent before pack
credits, soonest expiry first, and whatever is left when the lot expires is
removed from the balance. Pack credits never expire and are never touched
by this module."""

from __future__ import annotations

import logging
import sqlite3

from .config import get_settings
from .db import add_months, connect, iso, now_iso, parse_ts, tx, utcnow

log = logging.getLogger("credits.passlots")


def expiry_for(granted_at=None) -> str:
    start = parse_ts(granted_at) or utcnow()
    return iso(add_months(start, 1 + get_settings().studio_pass_rollover_months))


def add_lot(conn: sqlite3.Connection, *, account_id: str, payment_id: str, credits: int,
            subscription_id: str | None = None) -> bool:
    if credits <= 0:
        return False
    stamp = now_iso()
    return conn.execute(
        """INSERT OR IGNORE INTO pass_credit_lots (account_id, payment_id, subscription_id, credits,
               remaining, expires_at, status, created_at, updated_at)
           VALUES (?,?,?,?,?,?, 'active', ?, ?)""",
        (account_id, payment_id, subscription_id or None, credits, credits,
         expiry_for(stamp), stamp, stamp),
    ).rowcount > 0


def allocate(conn: sqlite3.Connection, *, owner_type: str, owner_id: str, job_id: str, credits: int) -> int:
    """Draws a job's credits from the owner's live lots. Returns how many came from lots."""
    if owner_type != "account" or credits <= 0:
        return 0
    lots = conn.execute(
        """SELECT id, remaining FROM pass_credit_lots
           WHERE account_id=? AND status='active' AND remaining>0 AND expires_at>?
           ORDER BY expires_at, id""",
        (owner_id, now_iso()),
    ).fetchall()
    need, taken = credits, 0
    for lot in lots:
        if need <= 0:
            break
        use = min(need, int(lot["remaining"]))
        conn.execute("UPDATE pass_credit_lots SET remaining=remaining-?, updated_at=? WHERE id=?",
                     (use, now_iso(), lot["id"]))
        conn.execute(
            """INSERT INTO pass_lot_usage (job_id, lot_id, credits) VALUES (?,?,?)
               ON CONFLICT(job_id, lot_id) DO UPDATE SET credits=credits+excluded.credits""",
            (job_id, lot["id"], use))
        need -= use
        taken += use
    return taken


def restore(conn: sqlite3.Connection, job_id: str) -> int:
    """Gives a refunded job's credits back to the lots they came from. A lot
    that already expired is reopened, so the next sweep expires them again."""
    rows = conn.execute("SELECT lot_id, credits FROM pass_lot_usage WHERE job_id=?", (job_id,)).fetchall()
    total = 0
    for r in rows:
        conn.execute(
            """UPDATE pass_credit_lots SET remaining=remaining+?, updated_at=?,
                   status=CASE WHEN status='expired' THEN 'active' ELSE status END
               WHERE id=? AND status<>'reversed'""",
            (r["credits"], now_iso(), r["lot_id"]))
        total += int(r["credits"])
    conn.execute("DELETE FROM pass_lot_usage WHERE job_id=?", (job_id,))
    return total


def reduce_for_payment(conn: sqlite3.Connection, payment_id: str, credits_taken: int,
                       fully_reversed: bool) -> None:
    """A refund or lost dispute took credits back; the lot shrinks to match."""
    lot = conn.execute("SELECT id, remaining FROM pass_credit_lots WHERE payment_id=?", (payment_id,)).fetchone()
    if lot is None:
        return
    remaining = max(0, int(lot["remaining"]) - max(0, credits_taken))
    status = "reversed" if fully_reversed else None
    conn.execute(
        "UPDATE pass_credit_lots SET remaining=?, updated_at=?, status=COALESCE(?, status) WHERE id=?",
        (0 if fully_reversed else remaining, now_iso(), status, lot["id"]))


def _account_balance(conn: sqlite3.Connection, account_id: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(SUM(delta),0) AS b FROM credit_ledger WHERE owner_type='account' AND owner_id=?",
        (account_id,)).fetchone()
    return int(row["b"] or 0)


def expire_due(limit: int = 500) -> dict:
    """Removes the unused part of every lot past its expiry. Idempotent."""
    from .ledger import grant

    report = {"lots": 0, "credits": 0}
    with connect() as conn:
        due = conn.execute(
            """SELECT id FROM pass_credit_lots WHERE status='active' AND expires_at<=?
               ORDER BY expires_at LIMIT ?""", (now_iso(), limit)).fetchall()
    for d in due:
        with connect() as conn, tx(conn):
            lot = conn.execute("SELECT * FROM pass_credit_lots WHERE id=? AND status='active'",
                               (d["id"],)).fetchone()
            if lot is None:
                continue
            amount = max(0, min(int(lot["remaining"]), _account_balance(conn, lot["account_id"])))
            if amount:
                grant(conn, owner_type="account", owner_id=lot["account_id"], amount=-amount,
                      kind="admin_adjust",
                      idempotency_key=f"pass_expiry:{lot['id']}:{lot['expired_credits']}",
                      order_id=lot["payment_id"],
                      note=f"Studio Pass credits expired ({amount} unused)")
            conn.execute(
                """UPDATE pass_credit_lots SET remaining=0, expired_credits=expired_credits+?,
                       status='expired', updated_at=? WHERE id=?""",
                (amount, now_iso(), lot["id"]))
        report["lots"] += 1
        report["credits"] += amount
    if report["lots"]:
        log.info("pass credit expiry: %s", report)
    return report


def summary(account_id: str | None) -> dict:
    if not account_id:
        return {"credits": 0, "next_expiry": None}
    with connect() as conn:
        live = conn.execute(
            """SELECT remaining, expires_at FROM pass_credit_lots
               WHERE account_id=? AND status='active' AND remaining>0 AND expires_at>?
               ORDER BY expires_at""", (account_id, now_iso())).fetchall()
        balance = _account_balance(conn, account_id)
    total = min(sum(int(r["remaining"]) for r in live), max(0, balance))
    nxt = None
    if live and total > 0:
        nxt = {"credits": min(int(live[0]["remaining"]), total), "at": live[0]["expires_at"]}
    return {"credits": total, "next_expiry": nxt}