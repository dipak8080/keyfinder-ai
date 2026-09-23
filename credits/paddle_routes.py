"""
credits/paddle_routes.py - Endpoints the Paddle overlay checkout calls.

    GET  /credits/paddle/config        client token + environment for Paddle.js
    POST /credits/paddle/transaction   create a transaction for one pack
    POST /credits/paddle/confirm       grant credits once Paddle reports it paid

The transaction.completed webhook is the backstop. Both paths key the
ledger on the transaction id.
"""

from __future__ import annotations

import asyncio
import logging
import re

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, EmailStr, Field

from rate_limit import check_rate_limit

from . import claims, fulfil, paywall
from .config import get_settings
from .db import connect, now_iso, tx
from .identity import Identity
from .providers import WebhookUnprocessable
from .providers import paddle as pd
from .webhook import _enabled_providers

log = logging.getLogger("credits.paddle")
router = APIRouter(prefix="/credits/paddle", tags=["credits"])


def _rate_limited(max_requests: int, window_seconds: int):
    def dep(request: Request) -> None:
        check_rate_limit(request, max_requests=max_requests, window_seconds=window_seconds)
    return dep


class TransactionRequest(BaseModel):
    pack: str = Field(..., max_length=32)
    email: EmailStr
    source: str | None = Field(default=None, max_length=64)
    tool: str | None = Field(default=None, max_length=64)
    page: str | None = Field(default=None, max_length=128)


class ConfirmRequest(BaseModel):
    transaction_id: str = Field(..., max_length=64, pattern=r"^txn_[a-z0-9]+$")


_TAG_RE = re.compile(r"[^a-z0-9_\-/:.]")


def _tag(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = _TAG_RE.sub("", value.strip().lower())[:64]
    return cleaned or None


def _enabled() -> bool:
    return pd.NAME in _enabled_providers(get_settings()) and pd.configured()


def _require_enabled() -> None:
    if not _enabled():
        raise HTTPException(status_code=503, detail={"error": "paddle_not_configured"})


def _record_source(txn_id: str, body: TransactionRequest, identity: Identity) -> None:
    try:
        with connect() as conn, tx(conn):
            conn.execute(
                """INSERT OR REPLACE INTO order_sources
                   (provider, provider_order_id, source, tool, page, subject_id, created_at)
                   VALUES ('paddle', ?, ?, ?, ?, ?, ?)""",
                (txn_id, _tag(body.source), _tag(body.tool),
                 (body.page or "").strip()[:128] or None, identity.subject_id, now_iso()),
            )
    except Exception:  # noqa: BLE001
        log.exception("could not record order source for %s", txn_id)


@router.get("/config")
def paddle_config() -> dict:
    enabled = _enabled()
    return {
        "enabled": enabled,
        "client_token": pd.client_token() if enabled else "",
        "environment": "sandbox" if pd.is_sandbox() else "production",
    }


@router.post(
    "/transaction",
    dependencies=[Depends(_rate_limited(max_requests=15, window_seconds=3600))],
)
def create_transaction(
    body: TransactionRequest,
    identity: Identity = Depends(paywall.get_identity),
) -> dict:
    _require_enabled()

    pack = get_settings().pack(body.pack)
    if pack is None:
        raise HTTPException(status_code=400, detail={"error": "unknown_pack"})

    email = str(body.email).strip().lower()

    try:
        claims.record_claim(
            email=email, subject_id=identity.subject_id,
            pack=pack.key, ip_hash=identity.ip_hash,
        )
    except Exception:  # noqa: BLE001
        log.exception("could not record claim for %s", email)

    try:
        txn = pd.create_transaction(pack, email)
    except pd.PaddleError as exc:
        log.error("paddle transaction creation failed for %s: %s", pack.key, exc)
        raise HTTPException(status_code=502, detail={"error": "paddle_unavailable"})

    txn_id = str(txn.get("id") or "")
    if not txn_id:
        raise HTTPException(status_code=502, detail={"error": "paddle_no_transaction_id"})

    _record_source(txn_id, body, identity)

    log.info("paddle txn %s created: pack=%s source=%s tool=%s",
             txn_id, pack.key, _tag(body.source), _tag(body.tool))
    return {"transaction_id": txn_id}


@router.post(
    "/confirm",
    dependencies=[Depends(_rate_limited(max_requests=60, window_seconds=3600))],
)
def confirm_transaction(body: ConfirmRequest) -> dict:
    _require_enabled()

    try:
        txn = pd.get_transaction(body.transaction_id)
    except pd.PaddleError:
        raise HTTPException(status_code=502, detail={"error": "paddle_unavailable"})

    try:
        event = pd.event_from_transaction(txn)
    except WebhookUnprocessable as exc:
        log.error("paddle txn %s unprocessable: %s", body.transaction_id, exc)
        raise HTTPException(status_code=400, detail={"error": "unprocessable", "message": str(exc)})

    if event is None:
        return {"ok": False, "pending": True, "status": str(txn.get("status") or "unknown")}

    try:
        granted, balance = fulfil.apply_payment(event)
    except Exception:  # noqa: BLE001
        log.exception("paddle txn %s: paid but fulfilment failed", event.provider_txid)
        raise HTTPException(status_code=500, detail={"error": "fulfilment_failed"})

    if granted:
        try:
            asyncio.run(fulfil.send_receipt(event, balance))
        except Exception:  # noqa: BLE001
            log.exception("receipt failed for %s - credits WERE granted", event.email)

    return {
        "ok": True,
        "credits": event.credits,
        "balance": balance,
        "already_applied": not granted,
    }