"""Takes back credits when Dodo refunds a payment or loses a dispute.

Only credits still unspent can be taken back; the balance never goes
below zero. Each refund and each lost dispute is applied once."""

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


def _reverse(payment_id: str, fraction: float, *, key: str, status: str, reason: str) -> dict:
    with connect() as conn, tx(conn):
        order = conn.execute(
            "SELECT id, account_id, credits, email FROM orders WHERE provider='dodo' AND provider_order_id=?",
            (payment_id,),
        ).fetchone()
        if order is None or not order["account_id"]:
            log.warning("%s for %s: no matching order, nothing to take back", reason, payment_id)
            return {"applied": False, "reason": "no_order"}
        if conn.execute("SELECT 1 FROM credit_ledger WHERE idempotency_key=?", (key,)).fetchone():
            return {"applied": False, "reason": "already_applied"}

        target = min(order["credits"], max(0, math.ceil(order["credits"] * max(0.0, min(1.0, fraction)))))
        balance = conn.execute(
            "SELECT COALESCE(SUM(delta),0) AS b FROM credit_ledger WHERE owner_type='account' AND owner_id=?",
            (order["account_id"],),
        ).fetchone()["b"]
        taken = max(0, min(target, int(balance)))
        ledger_mod.grant(conn, owner_type="account", owner_id=order["account_id"], amount=-taken,
                         kind="chargeback", idempotency_key=key, order_id=order["id"],
                         note=f"{reason}: {taken} of {target} credits taken back")
        conn.execute("UPDATE orders SET status=? WHERE id=?", (status, order["id"]))

    short = target - taken
    log.warning("%s for %s (%s): took back %s of %s credits", reason, payment_id, order["email"], taken, target)
    if short:
        _alert(f"{reason} on {payment_id} ({order['email']}): {short} credits were already spent "
               f"and could not be taken back.")
    return {"applied": True, "taken": taken, "target": target}


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
    status = "refunded" if fraction >= 0.999 else "partially_refunded"
    return _reverse(payment_id, fraction, key=f"dodo_refund:{refund_id}", status=status, reason="refund")


def apply_dispute(event_type: str, data: dict) -> dict:
    payment_id = str(data.get("payment_id") or "")
    if not payment_id:
        raise ValueError("dispute without payment_id")
    if event_type == "dispute.opened":
        _alert(f"Dispute opened on {payment_id} ({data.get('amount')} {data.get('currency')}). "
               f"Respond in the Dodo dashboard within 4 days.")
        return {"applied": False, "reason": "opened"}
    if event_type == "dispute.lost":
        return _reverse(payment_id, 1.0, key=f"dodo_dispute:{payment_id}", status="disputed", reason="lost dispute")
    log.info("dodo %s for %s", event_type, payment_id)
    return {"applied": False, "reason": "noted"}