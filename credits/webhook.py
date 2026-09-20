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
import os

from fastapi import APIRouter, HTTPException, Path, Request

from . import fulfil
from .config import get_settings
from .db import connect, now_iso, tx
from .providers import (
    WebhookRejected,
    WebhookUnprocessable,
    get_adapter,
)

log = logging.getLogger("credits.webhook")
router = APIRouter(prefix="/credits", tags=["credits"])


def _enabled_providers(settings) -> set[str]:
    """PAYMENTS_ENABLED_PROVIDERS, comma separated. Defaults to the single
    configured provider, so existing deployments are unchanged."""
    raw = os.getenv("PAYMENTS_ENABLED_PROVIDERS", "").strip()
    if not raw:
        return {settings.payments_provider}
    return {name.strip().lower() for name in raw.split(",") if name.strip()}


@router.post("/webhook/{provider}")
async def payment_webhook(request: Request, provider: str = Path(...)) -> dict:
    settings = get_settings()

    # Only an ENABLED provider is accepted, so a stale webhook still
    # registered at an old provider cannot keep granting credits after a
    # migration. More than one may be live at a time: Ko-fi kept for
    # donations while PayPal takes credit sales.
    if provider not in _enabled_providers(settings):
        log.warning("webhook for %r but enabled providers are %s",
                    provider, sorted(_enabled_providers(settings)))
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
        granted, balance = fulfil.apply_payment(event)
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
        await fulfil.send_receipt(event, balance)

    return {"ok": True}