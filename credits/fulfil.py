"""
credits/fulfil.py - Everything that happens once a payment is real.

Lifted out of credits/webhook.py unchanged. It lived there while the
webhook was the only way money arrived; PayPal grants credits from the
capture response instead, so both paths now call the same code rather
than one reaching into the other's privates.

apply_payment() is idempotent on event.provider_txid. A payment applied
by the capture route and then redelivered by the webhook credits once.
"""

from __future__ import annotations

import json
import logging

from . import claims, mailer
from .config import get_settings
from .db import connect, now_iso, tx
from .identity import get_or_create_account, link_subject_to_account
from .ledger import grant
from .providers import PaymentEvent
from .security import new_id

log = logging.getLogger("credits.fulfil")


def apply_payment(event: PaymentEvent) -> tuple[bool, int]:
    """Account, claim, order and ledger in one transaction.

    Returns (was_newly_granted, balance). was_newly_granted is False
    when the ledger's idempotency key already existed.
    """
    with connect() as conn, tx(conn):
        account_id = get_or_create_account(conn, event.email)

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
               VALUES (?,?,?,?,?,?,?,?,?,?,?, 'paid', ?, ?, ?)""",
            (new_id("ord_"), event.provider, event.provider_txid,
             str(event.raw.get("url") or ""), account_id, subject_id, event.email,
             ",".join(event.pack_keys), event.credits, round(event.amount_usd * 100),
             event.currency, 1 if get_settings().provider_test_mode else 0,
             now_iso(), json.dumps(event.raw)[:20000]),
        )

        granted = grant(
            conn, owner_type="account", owner_id=account_id, amount=event.credits,
            kind="purchase",
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


async def send_receipt(event: PaymentEvent, balance: int) -> None:
    from .auth import issue_magic_link

    with connect() as conn, tx(conn):
        link = issue_magic_link(conn, email=event.email, subject_id=None, ip_hash=None)

    subject, html, text = mailer.receipt_email(event.credits, balance, link)
    error: str | None = None
    try:
        await mailer.send_email(event.email, subject, html, text)
    except Exception as exc:  # noqa: BLE001
        error = str(exc)[:500]
        log.exception("receipt email failed for %s - credits WERE granted", event.email)

    try:
        with connect() as conn, tx(conn):
            conn.execute(
                """UPDATE orders SET receipt_sent_at=?, receipt_error=?
                   WHERE provider=? AND provider_order_id=?""",
                (None if error else now_iso(), error, event.provider, event.provider_txid),
            )
    except Exception:  # noqa: BLE001
        log.exception("could not record receipt status for %s", event.provider_txid)