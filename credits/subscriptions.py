"""Studio Pass state: which accounts hold an active subscription, the
emails sent when that changes, and a periodic sync with Dodo in case a
webhook is ever missed."""

from __future__ import annotations

import asyncio
import logging
import sqlite3

from .config import get_settings
from .db import connect, normalize_ts, now_iso, tx
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


_TRACKED = ("account_id", "email", "customer_id", "product_id", "status",
            "cancel_at_next_billing_date", "next_billing_date")


def _resolve_account(conn: sqlite3.Connection, row, data: dict, email: str):
    """Account id this server wrote into the checkout, then the stored row,
    then the first Pass order, and only then the buyer's email."""
    metadata = data.get("metadata") or {}
    candidates = [str(metadata.get("af_account") or ""), row["account_id"] if row else None]
    order = conn.execute(
        "SELECT account_id FROM orders WHERE subscription_id=? AND account_id IS NOT NULL ORDER BY created_at LIMIT 1",
        (str(data.get("subscription_id") or ""),)).fetchone()
    candidates.append(order["account_id"] if order else None)
    for candidate in candidates:
        if candidate:
            acc = conn.execute("SELECT id, email FROM accounts WHERE id=?", (candidate,)).fetchone()
            if acc:
                return acc["id"], acc["email"]
    if email:
        account_id = get_or_create_account(conn, email)
        return account_id, email
    return None, None


def record_dodo_event(event_type: str, data: dict, event_at: str | None,
                      force: bool = False) -> tuple[str, str | None, dict]:
    """Upserts one subscription from a Dodo subscription payload. Returns
    (status, transition, row) where transition names the email to send.
    updated_at only moves when a tracked field actually changes."""
    sub_id = str(data.get("subscription_id") or "")
    if not sub_id:
        raise ValueError("subscription event without subscription_id")
    status = str(data.get("status") or "").lower() or event_type.split(".", 1)[-1]
    cancel = 1 if data.get("cancel_at_next_billing_date") else 0
    customer = data.get("customer") or {}
    buyer_email = str(customer.get("email") or "").strip().lower()
    stamp = normalize_ts(event_at) or now_iso()
    now = now_iso()

    with connect() as conn, tx(conn):
        row = conn.execute("SELECT * FROM subscriptions WHERE subscription_id=?", (sub_id,)).fetchone()
        if not force and row and row["last_event_at"]:
            last = normalize_ts(row["last_event_at"]) or row["last_event_at"]
            if last > stamp:
                log.info("subscription %s: ignoring older %s", sub_id, event_type)
                return row["status"], None, dict(row)
        account_id, account_email = _resolve_account(conn, row, data, buyer_email)
        new = {
            "account_id": account_id,
            "email": account_email or buyer_email or (row["email"] if row else None),
            "customer_id": str(customer.get("customer_id") or "") or (row["customer_id"] if row else None),
            "product_id": str(data.get("product_id") or "") or (row["product_id"] if row else None),
            "status": status,
            "cancel_at_next_billing_date": cancel,
            "next_billing_date": (normalize_ts(data.get("next_billing_date"))
                                  or (row["next_billing_date"] if row else None)),
        }
        transition = _transition(row["status"] if row else None,
                                 bool(row["cancel_at_next_billing_date"]) if row else None,
                                 status, bool(cancel))
        last_event_at = max(stamp, normalize_ts(row["last_event_at"]) or "") if row else stamp
        if row is None:
            conn.execute(
                """INSERT INTO subscriptions (subscription_id, provider, account_id, email, customer_id,
                       product_id, status, cancel_at_next_billing_date, next_billing_date,
                       last_event, last_event_at, created_at, updated_at, last_synced_at, ended_at)
                   VALUES (?, 'dodo', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (sub_id, new["account_id"], new["email"], new["customer_id"], new["product_id"],
                 status, cancel, new["next_billing_date"], event_type, last_event_at, now, now,
                 now if force else None, now if status in ENDED_STATUSES else None),
            )
        else:
            changed = [k for k in _TRACKED if new[k] != row[k]]
            ended_at = row["ended_at"]
            if status in ENDED_STATUSES and not ended_at:
                ended_at = now
            elif status not in ENDED_STATUSES:
                ended_at = None
            conn.execute(
                f"""UPDATE subscriptions SET {", ".join(f"{k}=?" for k in _TRACKED)},
                       last_event=?, last_event_at=?, ended_at=?,
                       updated_at=?, last_synced_at=COALESCE(?, last_synced_at)
                   WHERE subscription_id=?""",
                (*[new[k] for k in _TRACKED], event_type, last_event_at, ended_at,
                 now if changed else row["updated_at"], now if force else None, sub_id),
            )
        saved = dict(conn.execute("SELECT * FROM subscriptions WHERE subscription_id=?", (sub_id,)).fetchone())
        duplicates = []
        if status == "active" and account_id:
            duplicates = [dict(r) for r in conn.execute(
                "SELECT subscription_id, created_at FROM subscriptions WHERE account_id=? AND status='active'"
                " AND subscription_id<>?", (account_id, sub_id)).fetchall()]

    if duplicates:
        _handle_duplicates(account_id, saved, duplicates)
    log.info("subscription %s -> %s (%s)%s", sub_id, status, event_type,
             f" transition={transition}" if transition else "")
    return status, transition, saved


def _handle_duplicates(account_id: str, current: dict, others: list[dict]) -> None:
    """A second active Pass on one account: the newer one is set to stop
    renewing, and the owner is told to refund it."""
    rows = sorted([current, *others], key=lambda r: (r["created_at"], r["subscription_id"]))
    keep, extras = rows[0], rows[1:]
    from .providers import dodo as dd
    for extra in extras:
        try:
            dd.set_cancel_at_period_end(extra["subscription_id"], True)
            set_cancel_flag(extra["subscription_id"], True)
            outcome = "set to stop renewing"
        except Exception:  # noqa: BLE001
            log.exception("could not stop duplicate subscription %s", extra["subscription_id"])
            outcome = "COULD NOT be stopped, cancel it by hand"
        _alert(f"Studio Pass: account {account_id} had a second active subscription "
               f"{extra['subscription_id']} (keeping {keep['subscription_id']}). It was {outcome}. "
               f"Refund it in Dodo.")


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


def has_blocking_pass(identity: Identity):
    """An active or on-hold Pass that should stop a second checkout."""
    row = current_for(identity)
    return row if row and row["status"] in ("active", "on_hold") else None


def summary(identity: Identity) -> dict:
    from .passlots import summary as lot_summary
    s = get_settings()
    row = current_for(identity)
    lots = lot_summary(identity.account_id if identity else None)
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
        "rollover_months": s.studio_pass_rollover_months,
        "pass_credits": lots["credits"],
        "next_expiry": lots["next_expiry"],
    }


def notify(transition: str | None, row: dict | None, dedupe: str | None = None) -> bool:
    """Queues the lifecycle email for a transition. Never raises."""
    from .notifications import queue_pass_email
    return queue_pass_email(transition, row, dedupe)


def sync_with_dodo(days: int = 45) -> dict:
    """Re-reads live and recently ended subscriptions from Dodo, then grants
    any paid cycle whose payment.succeeded webhook never arrived. Idempotent."""
    from . import fulfil
    from .providers import dodo as dd
    from .providers import WebhookUnprocessable

    report = {"checked": 0, "updated": 0, "payments_applied": 0, "errors": 0}
    with connect() as conn:
        rows = conn.execute(
            """SELECT subscription_id FROM subscriptions
               WHERE status IN ('active','on_hold','pending')
                  OR ended_at IS NULL
                  OR ended_at > strftime('%Y-%m-%dT%H:%M:%SZ','now',?)""",
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
                notify(transition, saved, dedupe=f"sync:{saved.get('updated_at')}")
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
    if report["updated"]:
        from .notifications import send_transactional_blocking
        send_transactional_blocking()
    return report


def admin_overview() -> dict:
    s = get_settings()
    with connect() as conn:
        counts = {r["status"]: r["n"] for r in conn.execute(
            "SELECT status, COUNT(*) AS n FROM subscriptions GROUP BY status").fetchall()}
        agg = conn.execute(
            """SELECT
                 SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) AS active,
                 SUM(CASE WHEN status='active' AND cancel_at_next_billing_date=1 THEN 1 ELSE 0 END) AS cancelling,
                 SUM(CASE WHEN created_at > strftime('%Y-%m-%dT%H:%M:%SZ','now','-30 days') THEN 1 ELSE 0 END)
                     AS started_30d,
                 SUM(CASE WHEN ended_at > strftime('%Y-%m-%dT%H:%M:%SZ','now','-30 days') THEN 1 ELSE 0 END)
                     AS churned_30d
               FROM subscriptions""").fetchone()
        rows = [dict(r) for r in conn.execute(
            """SELECT subscription_id, account_id, email, status,
                      cancel_at_next_billing_date AS cancel_at_period_end,
                      next_billing_date, created_at, updated_at, last_synced_at, ended_at
               FROM subscriptions ORDER BY updated_at DESC LIMIT 200""").fetchall()]
        lots = conn.execute(
            """SELECT COALESCE(SUM(CASE WHEN status='active' THEN remaining ELSE 0 END),0) AS outstanding,
                      COALESCE(SUM(expired_credits),0) AS expired
               FROM pass_credit_lots""").fetchone()
    active = int(agg["active"] or 0)
    cancelling = int(agg["cancelling"] or 0)
    return {
        "counts": counts,
        "active": active,
        "cancelling_at_period_end": cancelling,
        "mrr_usd": round((active - cancelling) * s.studio_pass_price_usd, 2),
        "started_30d": int(agg["started_30d"] or 0),
        "churned_30d": int(agg["churned_30d"] or 0),
        "pass_credits_outstanding": int(lots["outstanding"]),
        "pass_credits_expired": int(lots["expired"]),
        "subscriptions": rows,
    }