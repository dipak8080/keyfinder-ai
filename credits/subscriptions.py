"""Studio Pass state: which accounts hold an active subscription, the
emails sent when that changes, and a periodic sync with Dodo in case a
webhook is ever missed."""

from __future__ import annotations

import asyncio
import logging
import sqlite3

from .config import get_settings
from .db import connect, now_iso, tx
from .identity import Identity, get_or_create_account

log = logging.getLogger("credits.subscriptions")

ACTIVE_STATUSES = ("active",)
ENDED_STATUSES = ("cancelled", "expired", "failed")
MANAGE_PATH = "/account"


def _transition(prev_status: str | None, prev_cancel: bool | None, status: str, cancel: bool) -> str | None:
    if status == "active" and prev_status != "active":
        return "reactivated" if prev_status == "on_hold" else "started"
    if status == "on_hold" and prev_status != "on_hold":
        return "on_hold"
    if status in ENDED_STATUSES and prev_status not in ENDED_STATUSES and prev_status is not None:
        return "ended"
    if status == "active" and prev_status == "active" and prev_cancel is not None and cancel != prev_cancel:
        return "cancel_scheduled" if cancel else "cancel_undone"
    return None


def _alert(message: str) -> None:
    try:
        from monitoring import alert_now
        alert_now(message)
    except Exception:  # noqa: BLE001
        log.critical(message)


def record_dodo_event(event_type: str, data: dict, event_at: str | None,
                      force: bool = False) -> tuple[str, str | None, dict]:
    """Upserts one subscription from a Dodo subscription payload. Returns
    (status, transition, row) where transition names the email to send."""
    sub_id = str(data.get("subscription_id") or "")
    if not sub_id:
        raise ValueError("subscription event without subscription_id")
    status = str(data.get("status") or "").lower() or event_type.split(".", 1)[-1]
    cancel = bool(data.get("cancel_at_next_billing_date"))
    customer = data.get("customer") or {}
    email = str(customer.get("email") or "").strip().lower()
    stamp = event_at or now_iso()

    with connect() as conn, tx(conn):
        row = conn.execute("SELECT * FROM subscriptions WHERE subscription_id=?", (sub_id,)).fetchone()
        if not force and row and row["last_event_at"] and row["last_event_at"] > stamp:
            log.info("subscription %s: ignoring older %s", sub_id, event_type)
            return row["status"], None, dict(row)
        account_id = row["account_id"] if row and row["account_id"] else (
            get_or_create_account(conn, email) if email else None)
        transition = _transition(row["status"] if row else None,
                                 bool(row["cancel_at_next_billing_date"]) if row else None,
                                 status, cancel)
        conn.execute(
            """INSERT INTO subscriptions (subscription_id, provider, account_id, email, customer_id,
                   product_id, status, cancel_at_next_billing_date, next_billing_date,
                   last_event, last_event_at, created_at, updated_at)
               VALUES (?, 'dodo', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(subscription_id) DO UPDATE SET
                   account_id=COALESCE(subscriptions.account_id, excluded.account_id),
                   email=COALESCE(excluded.email, subscriptions.email),
                   customer_id=COALESCE(excluded.customer_id, subscriptions.customer_id),
                   product_id=COALESCE(excluded.product_id, subscriptions.product_id),
                   status=excluded.status,
                   cancel_at_next_billing_date=excluded.cancel_at_next_billing_date,
                   next_billing_date=COALESCE(excluded.next_billing_date, subscriptions.next_billing_date),
                   last_event=excluded.last_event,
                   last_event_at=MAX(COALESCE(subscriptions.last_event_at, ''), excluded.last_event_at),
                   updated_at=excluded.updated_at""",
            (sub_id, account_id, email or None, str(customer.get("customer_id") or "") or None,
             str(data.get("product_id") or "") or None, status, 1 if cancel else 0,
             str(data.get("next_billing_date") or "") or None,
             event_type, stamp, now_iso(), now_iso()),
        )
        saved = dict(conn.execute("SELECT * FROM subscriptions WHERE subscription_id=?", (sub_id,)).fetchone())
        duplicate = None
        if status == "active" and account_id:
            duplicate = conn.execute(
                "SELECT subscription_id FROM subscriptions WHERE account_id=? AND status='active'"
                " AND subscription_id<>?", (account_id, sub_id)).fetchone()

    if duplicate:
        _alert(f"Studio Pass: account {account_id} ({email}) has two active subscriptions "
               f"({duplicate['subscription_id']} and {sub_id}). Cancel one and refund it in Dodo.")
    log.info("subscription %s -> %s (%s)%s", sub_id, status, event_type,
             f" transition={transition}" if transition else "")
    return status, transition, saved


def set_cancel_flag(subscription_id: str, cancel: bool) -> str | None:
    """Local mirror of a cancel/resume we just sent to Dodo. Returns the
    transition so the caller can send the matching email once."""
    with connect() as conn, tx(conn):
        row = conn.execute("SELECT status, cancel_at_next_billing_date FROM subscriptions WHERE subscription_id=?",
                           (subscription_id,)).fetchone()
        conn.execute("UPDATE subscriptions SET cancel_at_next_billing_date=?, updated_at=? WHERE subscription_id=?",
                     (1 if cancel else 0, now_iso(), subscription_id))
    if row is None or row["status"] != "active" or bool(row["cancel_at_next_billing_date"]) == cancel:
        return None
    return "cancel_scheduled" if cancel else "cancel_undone"


def _current(conn: sqlite3.Connection, account_id: str):
    return conn.execute(
        """SELECT * FROM subscriptions WHERE account_id=?
           ORDER BY CASE status WHEN 'active' THEN 0 WHEN 'on_hold' THEN 1 ELSE 2 END, updated_at DESC
           LIMIT 1""",
        (account_id,),
    ).fetchone()


def current_for(identity: Identity):
    if not identity or not identity.account_id:
        return None
    with connect() as conn:
        return _current(conn, identity.account_id)


def has_active_pass(identity: Identity) -> bool:
    row = current_for(identity)
    return bool(row and row["status"] in ACTIVE_STATUSES)


def summary(identity: Identity) -> dict:
    s = get_settings()
    row = current_for(identity)
    return {
        "available": s.studio_pass_enabled,
        "price_usd": s.studio_pass_price_usd,
        "credits_per_month": s.studio_pass_credits,
        "options_included": s.studio_pass_options_included,
        "active": bool(row and row["status"] in ACTIVE_STATUSES),
        "status": row["status"] if row else None,
        "renews_at": row["next_billing_date"] if row else None,
        "cancel_at_period_end": bool(row["cancel_at_next_billing_date"]) if row else False,
        "can_manage": bool(row and row["customer_id"]),
    }


async def notify(transition: str | None, row: dict) -> None:
    """Sends the lifecycle email for a transition. Never raises."""
    if not transition or not row or not row.get("email"):
        return
    from . import mailer
    s = get_settings()
    try:
        subject, html, text = mailer.pass_email(
            transition, credits=s.studio_pass_credits, price_usd=s.studio_pass_price_usd,
            date=row.get("next_billing_date"), manage_url=f"{s.frontend_url}{MANAGE_PATH}",
        )
        await mailer.send_email(row["email"], subject, html, text)
        log.info("studio pass email %s sent for %s", transition, row.get("subscription_id"))
    except Exception:  # noqa: BLE001
        log.exception("studio pass email %s failed for %s", transition, row.get("subscription_id"))


def sync_with_dodo(days: int = 45) -> dict:
    """Re-reads every recent subscription from Dodo, then grants any paid
    cycle whose payment.succeeded webhook never arrived. Idempotent."""
    from . import fulfil
    from .providers import dodo as dd
    from .providers import WebhookUnprocessable

    report = {"checked": 0, "updated": 0, "payments_applied": 0, "errors": 0}
    with connect() as conn:
        rows = conn.execute(
            """SELECT subscription_id FROM subscriptions
               WHERE status IN ('active','on_hold','pending')
                  OR updated_at > strftime('%Y-%m-%dT%H:%M:%SZ','now',?)""",
            (f"-{int(days)} days",),
        ).fetchall()

    for r in rows:
        sub_id = r["subscription_id"]
        report["checked"] += 1
        try:
            data = dd.get_subscription(sub_id)
            status, transition, saved = record_dodo_event("sync", data, None, force=True)
            if transition:
                report["updated"] += 1
                asyncio.run(notify(transition, saved))
            for item in dd.list_subscription_payments(sub_id):
                payment_id = str(item.get("payment_id") or "")
                if not payment_id:
                    continue
                with connect() as conn:
                    seen = conn.execute("SELECT 1 FROM orders WHERE provider='dodo' AND provider_order_id=?",
                                        (payment_id,)).fetchone()
                if seen:
                    continue
                try:
                    event = dd.event_from_payment(dd.get_payment(payment_id))
                except WebhookUnprocessable as exc:
                    log.error("sync: payment %s unprocessable: %s", payment_id, exc)
                    continue
                if event is None:
                    continue
                granted, balance = fulfil.apply_payment(event)
                if granted:
                    report["payments_applied"] += 1
                    log.warning("sync: applied missed Studio Pass payment %s (%s credits)",
                                payment_id, event.credits)
                    asyncio.run(fulfil.send_receipt(event, balance))
        except Exception:  # noqa: BLE001
            report["errors"] += 1
            log.exception("sync failed for subscription %s", sub_id)
    if report["checked"]:
        log.info("studio pass sync: %s", report)
    return report


def admin_overview() -> dict:
    s = get_settings()
    with connect() as conn:
        rows = [dict(r) for r in conn.execute(
            """SELECT subscription_id, email, status, cancel_at_next_billing_date AS cancel_at_period_end,
                      next_billing_date, created_at, updated_at
               FROM subscriptions ORDER BY updated_at DESC LIMIT 500""").fetchall()]
        churned_30d = conn.execute(
            """SELECT COUNT(*) AS n FROM subscriptions WHERE status IN ('cancelled','expired')
               AND updated_at > strftime('%Y-%m-%dT%H:%M:%SZ','now','-30 days')""").fetchone()["n"]
        started_30d = conn.execute(
            """SELECT COUNT(*) AS n FROM subscriptions
               WHERE created_at > strftime('%Y-%m-%dT%H:%M:%SZ','now','-30 days')""").fetchone()["n"]
    counts: dict[str, int] = {}
    for r in rows:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    active = counts.get("active", 0)
    return {
        "counts": counts,
        "active": active,
        "cancelling_at_period_end": sum(1 for r in rows if r["status"] == "active" and r["cancel_at_period_end"]),
        "mrr_usd": round(active * s.studio_pass_price_usd, 2),
        "started_30d": started_30d,
        "churned_30d": churned_30d,
        "subscriptions": rows,
    }