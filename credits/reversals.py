"""Takes back credits when Dodo refunds a payment or loses a dispute, and
ends the Studio Pass a refunded or disputed payment belonged to.

Only credits still unspent can be taken back; the balance never goes
below zero. Each refund and each lost dispute is applied once, and the
total taken per order never exceeds what the order granted."""

from __future__ import annotations

import logging
import math

from . import ledger as ledger_mod
from .db import connect, now_iso, tx

log = logging.getLogger("credits.reversals")


def _alert(message: str) -> None:
    try:
        from monitoring import alert_now
        alert_now(message)
    except Exception:  # noqa: BLE001
        log.critical(message)


def _is_pass(pack: str | None) -> bool:
    return "pass" in [p.strip() for p in (pack or "").split(",")]


def _reverse(payment_id: str, fraction: float, *, key: str, reason: str, dispute: bool = False) -> dict:
    """Takes back up to fraction of an order's credits. The running total per
    order means two refunds, or a refund and a lost dispute, never take
    more than the order granted."""
    with connect() as conn, tx(conn):
        order = conn.execute(
            """SELECT id, account_id, credits, email, pack, subscription_id, credits_reversed
               FROM orders WHERE provider='dodo' AND provider_order_id=?""",
            (payment_id,),
        ).fetchone()
        if order is None or not order["account_id"]:
            log.warning("%s for %s: no matching order, nothing to take back", reason, payment_id)
            return {"applied": False, "reason": "no_order"}
        if conn.execute("SELECT 1 FROM credit_ledger WHERE idempotency_key=?", (key,)).fetchone():
            return {"applied": False, "reason": "already_applied", "pass": _is_pass(order["pack"]),
                    "order": dict(order)}

        credits = int(order["credits"])
        already = int(order["credits_reversed"] or 0)
        wanted = max(0, math.ceil(credits * max(0.0, min(1.0, fraction))))
        target = max(0, min(wanted, credits - already))
        balance = conn.execute(
            "SELECT COALESCE(SUM(delta),0) AS b FROM credit_ledger WHERE owner_type='account' AND owner_id=?",
            (order["account_id"],),
        ).fetchone()["b"]
        taken = max(0, min(target, int(balance)))
        ledger_mod.grant(conn, owner_type="account", owner_id=order["account_id"], amount=-taken,
                         kind="chargeback", idempotency_key=key, order_id=order["id"],
                         note=f"{reason}: {taken} of {target} credits taken back")
        total = already + target
        if dispute:
            status = "disputed"
        else:
            status = "refunded" if total >= credits else "partially_refunded"
        conn.execute("UPDATE orders SET status=?, credits_reversed=? WHERE id=?", (status, total, order["id"]))
        if _is_pass(order["pack"]):
            from .passlots import reduce_for_payment
            reduce_for_payment(conn, payment_id, taken, fully_reversed=total >= credits)
        from . import referrals
        referral_taken = referrals.reverse_for_payment(conn, payment_id)
        if referral_taken:
            log.warning("%s for %s: took back %s referral credits", reason, payment_id, referral_taken)

    short = target - taken
    log.warning("%s for %s (%s): took back %s of %s credits", reason, payment_id, order["email"], taken, target)
    if short:
        _alert(f"{reason} on {payment_id} ({order['email']}): {short} credits were already spent "
               f"and could not be taken back.")
    return {"applied": True, "taken": taken, "target": target, "pass": _is_pass(order["pack"]),
            "order": dict(order)}


def cancel_pass_for_payment(payment_id: str, order: dict | None, reason: str) -> dict:
    """Ends the Studio Pass a refunded or disputed payment belonged to, in
    Dodo and locally. Only that payment's own subscription is touched."""
    from . import subscriptions
    from .providers import dodo as dd

    sub_id = (order or {}).get("subscription_id")
    if not sub_id:
        try:
            sub_id = str(dd.get_payment(payment_id).get("subscription_id") or "")
        except Exception:  # noqa: BLE001
            log.exception("could not look up the subscription for %s", payment_id)
    if not sub_id:
        _alert(f"{reason} on Studio Pass payment {payment_id}: its subscription could not be found. "
               f"Cancel it in Dodo by hand.")
        return {"cancelled": False, "reason": "no_subscription"}

    with connect() as conn:
        row = conn.execute("SELECT * FROM subscriptions WHERE subscription_id=?", (sub_id,)).fetchone()
    if row is not None and row["status"] in subscriptions.ENDED_STATUSES:
        return {"cancelled": False, "reason": "already_ended", "subscription_id": sub_id}

    try:
        dd.cancel_subscription(sub_id)
    except Exception:  # noqa: BLE001
        log.exception("could not cancel subscription %s", sub_id)
        _alert(f"{reason} on Studio Pass payment {payment_id}: subscription {sub_id} could NOT be "
               f"cancelled in Dodo. Cancel it by hand.")
        return {"cancelled": False, "reason": "dodo_error", "subscription_id": sub_id}

    stamp = now_iso()
    with connect() as conn, tx(conn):
        conn.execute(
            """UPDATE subscriptions SET status='cancelled', ended_at=COALESCE(ended_at, ?), updated_at=?,
                   last_event='auto_cancel', last_event_at=?
               WHERE subscription_id=?""", (stamp, stamp, stamp, sub_id))
        fresh = conn.execute("SELECT * FROM subscriptions WHERE subscription_id=?", (sub_id,)).fetchone()
    if fresh is not None and row is not None:
        subscriptions.notify("ended", dict(fresh), dedupe=f"auto_cancel:{payment_id}")
    _alert(f"Studio Pass {sub_id} was cancelled automatically after {reason} on payment {payment_id}.")
    log.warning("studio pass %s cancelled after %s on %s", sub_id, reason, payment_id)
    return {"cancelled": True, "subscription_id": sub_id}


def _finish(result: dict, payment_id: str, reason: str) -> dict:
    order = result.pop("order", None)
    if result.pop("pass", False):
        result["pass_cancel"] = cancel_pass_for_payment(payment_id, order, reason)
    return result


def apply_refund(data: dict) -> dict:
    from .providers import dodo as dd

    payment_id = str(data.get("payment_id") or "")
    refund_id = str(data.get("refund_id") or "")
    if not payment_id or not refund_id:
        raise ValueError("refund without payment_id or refund_id")
    fraction = 1.0
    if data.get("is_partial"):
        try:
            payment = dd.get_payment(payment_id)
            total = float(payment.get("total_amount") or 0)
            fraction = float(data.get("amount") or 0) / total if total else 1.0
        except Exception:  # noqa: BLE001
            log.exception("could not size partial refund %s; taking back all credits", refund_id)
    result = _reverse(payment_id, fraction, key=f"dodo_refund:{refund_id}", reason="refund")
    return _finish(result, payment_id, "a refund")


def apply_dispute(event_type: str, data: dict) -> dict:
    payment_id = str(data.get("payment_id") or "")
    if not payment_id:
        raise ValueError("dispute without payment_id")
    if event_type == "dispute.opened":
        _alert(f"Dispute opened on {payment_id} ({data.get('amount')} {data.get('currency')}). "
               f"Respond in the Dodo dashboard within 4 days.")
        with connect() as conn:
            order = conn.execute(
                "SELECT pack, subscription_id FROM orders WHERE provider='dodo' AND provider_order_id=?",
                (payment_id,)).fetchone()
        result = {"applied": False, "reason": "opened"}
        if order is not None and _is_pass(order["pack"]):
            result["pass_cancel"] = cancel_pass_for_payment(payment_id, dict(order), "a dispute")
        return result
    if event_type == "dispute.lost":
        result = _reverse(payment_id, 1.0, key=f"dodo_dispute:{payment_id}", reason="lost dispute",
                          dispute=True)
        return _finish(result, payment_id, "a lost dispute")
    log.info("dodo %s for %s", event_type, payment_id)
    return {"applied": False, "reason": "noted"}