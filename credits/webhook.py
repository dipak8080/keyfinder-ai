"""
credits/webhook.py - The one webhook route, for every provider.

    POST /credits/webhook/{provider}

Everything provider-specific happened in credits/providers/<name>.py
before this file sees anything. From PaymentEvent onward the flow is
identical regardless of who took the money:

    verify -> replay guard -> account -> claim match -> order -> ledger -> receipt

STATUS CODES ARE PART OF THE CONTRACT
-------------------------------------
Ko-fi retries until it gets a 200, so what this returns decides whether
a payment is redelivered:

    200  processed, or deliberately ignored (a tip), or a duplicate
    401  bad secret            - do NOT retry, it'll still be bad later
    400  unprocessable payload - do NOT retry, it'll still be malformed
    500  something broke here  - DO retry, this is our fault

Returning 500 for a malformed body would have Ko-fi redelivering it for
hours. Returning 200 for our own failure would silently lose a payment
someone actually made. Both mistakes are easy and neither is visible
until it costs a real customer, which is why the mapping is spelled out
rather than left to whatever HTTPException happens to be raised.
"""

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, HTTPException, Path, Request

from . import claims, mailer
from .config import get_settings
from .db import connect, now_iso, tx
from .identity import get_or_create_account, link_subject_to_account
from .ledger import grant
from .providers import (
    PaymentEvent,
    WebhookRejected,
    WebhookUnprocessable,
    get_adapter,
)
from .security import new_id

log = logging.getLogger("credits.webhook")
router = APIRouter(prefix="/credits", tags=["credits"])


@router.post("/webhook/{provider}")
async def payment_webhook(request: Request, provider: str = Path(...)) -> dict:
    settings = get_settings()

    # The URL names the provider, but only the CONFIGURED one is
    # accepted. Otherwise a stale webhook still registered at an old
    # provider could keep granting credits after a migration.
    if provider != settings.payments_provider:
        log.warning("webhook for %r but PAYMENTS_PROVIDER is %r", provider, settings.payments_provider)
        raise HTTPException(status_code=404, detail={"error": "unknown_provider"})

    adapter = get_adapter(provider)
    raw_body = await request.body()

    try:
        payload = await adapter.read_payload(request)
    except Exception as exc:  # noqa: BLE001
        log.warning("could not decode %s webhook body: %s", provider, exc)
        raise HTTPException(status_code=400, detail={"error": "bad_payload"})

    if not adapter.verify(payload, raw_body=raw_body, headers=request.headers):
        log.warning("rejected %s webhook: verification failed", provider)
        raise HTTPException(status_code=401, detail={"error": "bad_signature"})

    try:
        event = adapter.to_event(payload)
    except WebhookUnprocessable as exc:
        log.error("%s webhook unprocessable: %s", provider, exc)
        raise HTTPException(status_code=400, detail={"error": "unprocessable", "message": str(exc)})
    except WebhookRejected:
        raise HTTPException(status_code=401, detail={"error": "bad_signature"})

    if event is None:
        # A tip or a membership payment. Real, authenticated, and not
        # ours to act on - 200 so it isn't redelivered forever.
        return {"ok": True, "ignored": True}

    delivery_key = f"{provider}:{event.delivery_id}"
    with connect() as conn, tx(conn):
        seen = conn.execute(
            "SELECT processed_at FROM webhook_events WHERE event_id=?", (delivery_key,)
        ).fetchone()
        if seen and seen["processed_at"]:
            return {"ok": True, "duplicate": True}
        conn.execute(
            "INSERT OR REPLACE INTO webhook_events (event_id, provider, event_name, received_at, payload)"
            " VALUES (?,?,?,?,?)",
            (delivery_key, provider, "payment", now_iso(), json.dumps(event.raw)[:20000]),
        )

    try:
        granted, balance = _apply_payment(event)
    except Exception as exc:  # noqa: BLE001
        log.exception("failed to apply %s payment %s", provider, event.provider_txid)
        with connect() as conn, tx(conn):
            conn.execute("UPDATE webhook_events SET error=? WHERE event_id=?",
                        (str(exc)[:500], delivery_key))
        # 500 on purpose: our fault, so let the provider redeliver.
        raise HTTPException(status_code=500, detail={"error": "processing_failed"})

    with connect() as conn, tx(conn):
        conn.execute("UPDATE webhook_events SET processed_at=? WHERE event_id=?",
                    (now_iso(), delivery_key))

    # Receipt is sent only on a first application, never on a replay,
    # and outside the transaction so a mail outage can't roll back
    # credits that were legitimately granted.
    if granted:
        await _send_receipt(event, balance)

    return {"ok": True}


def _apply_payment(event: PaymentEvent) -> tuple[bool, int]:
    """Account, claim, order and ledger in one transaction.

    Returns (was_newly_granted, balance). was_newly_granted is False
    when the ledger's idempotency key already existed - i.e. the same
    payment arriving under a different delivery id.
    """
    with connect() as conn, tx(conn):
        account_id = get_or_create_account(conn, event.email)

        # Match this payment back to the browser that started checkout,
        # so credits appear in the tab they bought from. Best-effort:
        # a miss just means they use the magic link in the receipt.
        claim = claims.take_claim(conn, event.email)
        subject_id = claim["subject_id"] if claim else None
        if subject_id:
            exists = conn.execute("SELECT id FROM subjects WHERE id=?", (subject_id,)).fetchone()
            if exists is None:
                conn.execute(
                    "INSERT INTO subjects (id, account_id, first_ip_hash, last_ip_hash,"
                    " created_at, last_seen_at) VALUES (?,?,NULL,NULL,?,?)",
                    (subject_id, account_id, now_iso(), now_iso()),
                )
            else:
                link_subject_to_account(conn, subject_id, account_id)

        conn.execute(
            """INSERT OR IGNORE INTO orders (id, provider, provider_order_id, provider_ref,
               account_id, subject_id, email, pack, credits, amount_cents, currency,
               status, test_mode, created_at, raw)
               VALUES (?,?,?,?,?,?,?,?,?,?,?, 'paid', 0, ?, ?)""",
            (new_id("ord_"), event.provider, event.provider_txid,
             str(event.raw.get("url") or ""), account_id, subject_id, event.email,
             ",".join(event.pack_keys), event.credits, round(event.amount_usd * 100),
             event.currency, now_iso(), json.dumps(event.raw)[:20000]),
        )

        granted = grant(
            conn, owner_type="account", owner_id=account_id, amount=event.credits,
            kind="purchase",
            # The payment id, not the delivery id - this is what makes a
            # redelivered payment credit exactly once.
            idempotency_key=f"{event.provider}:{event.provider_txid}",
            order_id=event.provider_txid,
            note=",".join(event.pack_keys) or f"{event.provider} order",
        )

        balance = conn.execute(
            """SELECT COALESCE(SUM(delta),0) AS b FROM credit_ledger
               WHERE (owner_type='account' AND owner_id=?)
                  OR (owner_type='subject' AND owner_id IN
                      (SELECT id FROM subjects WHERE account_id=?))""",
            (account_id, account_id),
        ).fetchone()["b"]

    log.info("%s payment %s: %+d credits to %s (packs=%s, new=%s)",
             event.provider, event.provider_txid, event.credits, event.email,
             event.pack_keys, granted)
    return granted, int(balance)


async def _send_receipt(event: PaymentEvent, balance: int) -> None:
    from .auth import issue_magic_link

    with connect() as conn, tx(conn):
        link = issue_magic_link(conn, email=event.email, subject_id=None, ip_hash=None)

    subject, html, text = mailer.receipt_email(event.credits, balance, link)
    try:
        await mailer.send_email(event.email, subject, html, text)
    except Exception:  # noqa: BLE001
        # Never re-raise: the credits are already granted and the
        # payment is complete. A failed receipt is a support ticket,
        # not a reason to make the provider redeliver a paid order.
        log.exception("receipt email failed for %s - credits WERE granted", event.email)