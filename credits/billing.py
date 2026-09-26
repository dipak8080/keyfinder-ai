"""
credits/billing.py - Billing history and invoices for /account.

    GET /credits/history                     the caller's ledger, newest first, paginated
    GET /credits/orders                      the caller's paid orders
    GET /credits/orders/{order_id}/invoice   invoice PDF from Dodo for one order

Read-only. Every query is scoped to the caller's own identity, and every
response is no-store (per-user data behind Cloudflare).

order_id in all three responses is our own orders.id. The ledger stores
the Dodo payment id for purchases, orders.id for reversals, and another
account's payment id for referral rewards, so history resolves it to the
caller's own orders.id and returns null for anything that isn't theirs.
"""

from __future__ import annotations

import asyncio
import logging
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from fastapi.responses import StreamingResponse

from rate_limit import check_rate_limit

from . import paywall
from .db import connect
from .identity import Identity
from .ledger import _owner_clause
from .providers import dodo as _dodo

log = logging.getLogger("credits.billing")
router = APIRouter(prefix="/credits", tags=["credits"])

NO_STORE = "no-store, no-cache, must-revalidate, private"
ORDERS_MAX = 500
_INVOICE_TIMEOUT = httpx.Timeout(30.0, connect=10.0)


def _no_store(response: Response) -> None:
    response.headers["Cache-Control"] = NO_STORE
    response.headers["Pragma"] = "no-cache"


def _invoice_available(row) -> bool:
    return (
        row["provider"] == _dodo.NAME
        and bool(row["provider_order_id"])
        and int(row["test_mode"] or 0) == (1 if _dodo.is_test() else 0)
    )


def _own_order_ids(conn, account_id: str, refs: set[str]) -> dict[str, str]:
    if not refs:
        return {}
    refs_list = list(refs)
    marks = ",".join("?" * len(refs_list))
    rows = conn.execute(
        f"""SELECT id, provider_order_id FROM orders
            WHERE account_id=? AND (provider_order_id IN ({marks}) OR id IN ({marks}))""",
        (account_id, *refs_list, *refs_list),
    ).fetchall()
    out: dict[str, str] = {}
    for r in rows:
        out[r["id"]] = r["id"]
        if r["provider_order_id"]:
            out[r["provider_order_id"]] = r["id"]
    return out


@router.get("/history")
def history(
    response: Response,
    limit: int = Query(50, ge=1, le=100),
    cursor: str = Query("", max_length=20),
    identity: Identity = Depends(paywall.get_identity),
) -> dict:
    _no_store(response)
    before: int | None = None
    if cursor:
        if not cursor.isdigit():
            raise HTTPException(status_code=400, detail={"error": "bad_cursor",
                                                         "message": "That page link is not valid."})
        before = int(cursor)

    clause, params = _owner_clause(identity)
    sql = f"SELECT id, delta, kind, created_at, note, order_id FROM credit_ledger WHERE {clause} AND delta != 0"
    args: list = list(params)
    if before is not None:
        sql += " AND id < ?"
        args.append(before)
    sql += " ORDER BY id DESC LIMIT ?"
    args.append(limit + 1)

    with connect() as conn:
        rows = conn.execute(sql, args).fetchall()
        page = rows[:limit]
        refs = {r["order_id"] for r in page if r["order_id"]}
        mapping = _own_order_ids(conn, identity.account_id, refs) if identity.account_id else {}

    items = [
        {
            "id": int(r["id"]),
            "delta": int(r["delta"]),
            "kind": r["kind"],
            "created_at": r["created_at"],
            "note": r["note"],
            "order_id": mapping.get(r["order_id"]) if r["order_id"] else None,
        }
        for r in page
    ]
    next_cursor = str(page[-1]["id"]) if len(rows) > limit and page else None
    return {"items": items, "next_cursor": next_cursor}


@router.get("/orders")
def orders(
    response: Response,
    identity: Identity = Depends(paywall.get_identity),
) -> dict:
    _no_store(response)
    if not identity.account_id:
        return {"orders": []}
    with connect() as conn:
        rows = conn.execute(
            """SELECT id, provider, provider_order_id, pack, credits, amount_cents, currency,
                      status, test_mode, created_at
               FROM orders WHERE account_id=?
               ORDER BY created_at DESC, id DESC LIMIT ?""",
            (identity.account_id, ORDERS_MAX),
        ).fetchall()
    return {
        "orders": [
            {
                "order_id": r["id"],
                "pack": r["pack"],
                "credits": int(r["credits"] or 0),
                "amount_usd": round((r["amount_cents"] or 0) / 100, 2),
                "currency": r["currency"] or "USD",
                "status": r["status"],
                "created_at": r["created_at"],
                "invoice_available": _invoice_available(r),
            }
            for r in rows
        ]
    }


def _invoice_limit(request: Request) -> None:
    check_rate_limit(request, max_requests=20, window_seconds=3600, bucket_key="/credits/orders/invoice")


def _find_order(order_id: str, account_id: str | None):
    if not account_id:
        return None
    with connect() as conn:
        return conn.execute(
            "SELECT id, provider, provider_order_id, test_mode FROM orders WHERE id=? AND account_id=?",
            (order_id, account_id),
        ).fetchone()


def _not_found() -> HTTPException:
    return HTTPException(status_code=404, detail={"error": "order_not_found",
                                                  "message": "We couldn't find that order on your account."})


def _bad_gateway() -> HTTPException:
    return HTTPException(status_code=502, detail={"error": "invoice_unavailable",
                                                  "message": "The invoice couldn't be fetched right now. "
                                                             "Try again in a minute."})


@router.get("/orders/{order_id}/invoice", dependencies=[Depends(_invoice_limit)])
async def invoice(
    order_id: str = Path(..., min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_-]+$"),
    identity: Identity = Depends(paywall.get_identity),
) -> StreamingResponse:
    row = await asyncio.to_thread(_find_order, order_id, identity.account_id)
    if row is None or not _invoice_available(row):
        raise _not_found()

    api_key = _dodo._api_key()
    if not api_key:
        log.error("invoice %s: DODO_API_KEY is not set", order_id)
        raise _bad_gateway()

    payment_id = row["provider_order_id"]
    url = f"{_dodo.api_base()}/invoices/payments/{quote(payment_id, safe='')}"
    client = httpx.AsyncClient(timeout=_INVOICE_TIMEOUT)
    try:
        upstream = await client.send(
            client.build_request("GET", url, headers={"Authorization": f"Bearer {api_key}",
                                                      "Accept": "application/pdf"}),
            stream=True,
        )
    except httpx.HTTPError as exc:
        await client.aclose()
        log.error("invoice %s: Dodo request failed: %s", order_id, exc)
        raise _bad_gateway()

    if upstream.status_code != 200:
        body = (await upstream.aread())[:500]
        await upstream.aclose()
        await client.aclose()
        log.error("invoice %s: Dodo returned %s: %r", order_id, upstream.status_code, body)
        raise _bad_gateway()

    async def stream():
        try:
            async for chunk in upstream.aiter_bytes():
                yield chunk
        finally:
            await upstream.aclose()
            await client.aclose()

    return StreamingResponse(
        stream(),
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="AudioForges-invoice-{row["id"]}.pdf"',
            "Cache-Control": NO_STORE,
            "Pragma": "no-cache",
            "X-Content-Type-Options": "nosniff",
        },
    )