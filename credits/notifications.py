"""Lifecycle email: low balance, the monthly free song, and product updates.

Receipts, sign-in links and Studio Pass emails still send immediately.
Everything here goes through the outbox, one row per recipient and
reason, so a daily cap keeps the shared Resend quota free for sign-in
links, and no one gets the same email twice."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from . import mailer
from .config import get_settings
from .db import connect, iso, next_period_start_iso, now_iso, tx, utcnow
from .identity import Identity
from .security import sign, unsign

log = logging.getLogger("credits.notifications")

UNSUB_PURPOSE = "email_unsubscribe"
PRIORITY = {"pass": 0, "low_balance": 1, "referral": 3, "free_song": 5, "update": 9}
TRANSACTIONAL = ("pass",)
MAX_ATTEMPTS = 5
SENDING_STALE_MINUTES = 15


def unsubscribe_url(account_id: str, scope: str) -> str:
    s = get_settings()
    token = sign(f"{account_id}:{scope}", purpose=UNSUB_PURPOSE)
    return f"{s.api_base_url}/credits/email/unsubscribe?t={token}"


def read_unsubscribe_token(token: str) -> tuple[str, str] | None:
    value = unsign(token, purpose=UNSUB_PURPOSE)
    if not value or ":" not in value:
        return None
    account_id, scope = value.rsplit(":", 1)
    return (account_id, scope) if scope in ("updates", "notices", "all") else None


def apply_unsubscribe(account_id: str, scope: str) -> None:
    fields = {"updates": "email_updates=0", "notices": "email_notices=0",
              "all": "email_updates=0, email_notices=0"}[scope]
    with connect() as conn, tx(conn):
        conn.execute(f"UPDATE accounts SET {fields} WHERE id=?", (account_id,))
        conn.execute("UPDATE email_outbox SET status='skipped' WHERE account_id=? AND status='queued'"
                     + ("" if scope == "all" else
                        " AND kind IN ('free_song','update')" if scope == "updates" else " AND kind IN ('low_balance','referral')"),
                     (account_id,))
    log.info("account %s unsubscribed from %s", account_id, scope)


def preferences(account_id: str) -> dict:
    with connect() as conn:
        row = conn.execute("SELECT email, email_updates, email_notices FROM accounts WHERE id=?",
                           (account_id,)).fetchone()
    if row is None:
        return {}
    return {"email": row["email"], "updates": bool(row["email_updates"]),
            "account_notices": bool(row["email_notices"])}


def set_preferences(account_id: str, *, updates: bool | None = None, notices: bool | None = None) -> dict:
    with connect() as conn, tx(conn):
        if updates is not None:
            conn.execute("UPDATE accounts SET email_updates=? WHERE id=?", (1 if updates else 0, account_id))
        if notices is not None:
            conn.execute("UPDATE accounts SET email_notices=? WHERE id=?", (1 if notices else 0, account_id))
    return preferences(account_id)


def _enqueue(conn, *, account_id: str | None, email: str, kind: str, dedupe_key: str,
             subject: str, html: str, text: str, unsub: str | None, not_after: str | None = None) -> bool:
    return conn.execute(
        """INSERT OR IGNORE INTO email_outbox (account_id, email, kind, dedupe_key, priority,
               subject, html, text, unsubscribe_url, status, created_at, not_after)
           VALUES (?,?,?,?,?,?,?,?,?, 'queued', ?, ?)""",
        (account_id, email, kind, dedupe_key, PRIORITY.get(kind, 5), subject, html, text, unsub,
         now_iso(), not_after),
    ).rowcount > 0


def queue_pass_email(transition: str | None, row: dict | None, dedupe: str | None = None) -> bool:
    """Queues the Studio Pass lifecycle email for a transition. Never raises."""
    if not transition or not row or not row.get("email"):
        return False
    try:
        from .subscriptions import MANAGE_PATH
        s = get_settings()
        subject, html, text = mailer.pass_email(
            transition, credits=s.studio_pass_credits, price_usd=s.studio_pass_price_usd,
            date=row.get("next_billing_date"), manage_url=f"{s.frontend_url}{MANAGE_PATH}",
            rollover_months=s.studio_pass_rollover_months,
        )
        key = f"pass:{row.get('subscription_id')}:{transition}:{dedupe or now_iso()}"
        with connect() as conn, tx(conn):
            return _enqueue(conn, account_id=row.get("account_id"), email=row["email"], kind="pass",
                            dedupe_key=key, subject=subject, html=html, text=text, unsub=None)
    except Exception:  # noqa: BLE001
        log.exception("studio pass email %s not queued for %s", transition, row.get("subscription_id"))
        return False


_kick_tasks: set = set()


def kick() -> None:
    """Sends queued transactional email now, from inside a running event loop."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    task = loop.create_task(send_queued(kinds=TRANSACTIONAL))
    _kick_tasks.add(task)
    task.add_done_callback(_kick_tasks.discard)


def send_transactional_blocking() -> None:
    """Same as kick() for sync code running in a worker thread. Never raises."""
    try:
        asyncio.run(send_queued(kinds=TRANSACTIONAL))
    except Exception:  # noqa: BLE001
        log.warning("transactional send failed; the outbox retries it", exc_info=True)


def maybe_low_balance(identity: Identity, balance_after: int | None) -> bool:
    """Queues one low-balance email per purchase cycle. Never raises."""
    try:
        s = get_settings()
        if (not s.email_low_balance_enabled or balance_after is None or not identity.account_id
                or balance_after > s.low_balance_threshold):
            return False
        from .subscriptions import has_active_pass
        if has_active_pass(identity):
            return False
        with connect() as conn, tx(conn):
            acc = conn.execute("SELECT email, email_notices, status FROM accounts WHERE id=?",
                               (identity.account_id,)).fetchone()
            if acc is None or not acc["email_notices"] or acc["status"] != "active":
                return False
            last = conn.execute(
                """SELECT MAX(id) AS id FROM credit_ledger WHERE owner_type='account' AND owner_id=?
                   AND kind IN ('purchase','bonus','admin_adjust') AND delta>0""",
                (identity.account_id,)).fetchone()["id"]
            unsub = unsubscribe_url(identity.account_id, "notices")
            subject, html, text = mailer.low_balance_email(
                int(balance_after), f"{s.frontend_url}/pricing", unsub)
            return _enqueue(conn, account_id=identity.account_id, email=acc["email"], kind="low_balance",
                            dedupe_key=f"low_balance:{identity.account_id}:{last or 0}",
                            subject=subject, html=html, text=text, unsub=unsub)
    except Exception:  # noqa: BLE001
        log.warning("low balance email not queued", exc_info=True)
        return False


def queue_referral_reward(conn, account_id: str, payment_id: str) -> None:
    s = get_settings()
    acc = conn.execute("SELECT email, email_notices FROM accounts WHERE id=?", (account_id,)).fetchone()
    if acc is None or not acc["email_notices"]:
        return
    unsub = unsubscribe_url(account_id, "notices")
    subject, html, text = mailer.referral_reward_email(s.referral_reward_credits, f"{s.frontend_url}/account", unsub)
    _enqueue(conn, account_id=account_id, email=acc["email"], kind="referral",
             dedupe_key=f"referral:{account_id}:{payment_id}", subject=subject, html=html, text=text, unsub=unsub)


def queue_monthly_free_song(now: datetime | None = None) -> int:
    """Queues 'your free song is ready' once per month for accounts that
    opted in to updates. Runs whenever this month's batch is still missing.
    Each email is dropped if it has not gone out by the end of the month."""
    s = get_settings()
    now = now or datetime.now(timezone.utc)
    if not s.email_monthly_free_song_enabled or s.free_monthly_ops < 1:
        return 0
    period = now.strftime("%Y-%m")
    month = now.strftime("%B")
    not_after = next_period_start_iso(now)
    queued = 0
    with connect() as conn, tx(conn):
        if conn.execute("SELECT 1 FROM email_outbox WHERE dedupe_key=?", (f"free_song_batch:{period}",)).fetchone():
            return 0
        rows = conn.execute(
            """SELECT a.id, a.email FROM accounts a
               WHERE a.status='active' AND a.email_updates=1
                 AND NOT EXISTS (SELECT 1 FROM subscriptions p WHERE p.account_id=a.id AND p.status='active')"""
        ).fetchall()
        for row in rows:
            unsub = unsubscribe_url(row["id"], "updates")
            subject, html, text = mailer.free_song_email(month, f"{s.frontend_url}/vocal-remover", unsub)
            queued += _enqueue(conn, account_id=row["id"], email=row["email"], kind="free_song",
                               dedupe_key=f"free_song:{row['id']}:{period}",
                               subject=subject, html=html, text=text, unsub=unsub, not_after=not_after)
        conn.execute(
            """INSERT OR IGNORE INTO email_outbox (email, kind, dedupe_key, status, created_at)
               VALUES ('-', 'marker', ?, 'marker', ?)""", (f"free_song_batch:{period}", now_iso()))
    log.info("queued %s monthly free song emails for %s", queued, period)
    return queued


def queue_update(subject: str, message: str, campaign: str) -> int:
    s = get_settings()
    queued = 0
    with connect() as conn, tx(conn):
        rows = conn.execute("SELECT id, email FROM accounts WHERE status='active' AND email_updates=1").fetchall()
        for row in rows:
            unsub = unsubscribe_url(row["id"], "updates")
            subj, html, text = mailer.update_email(subject, message, s.frontend_url, unsub)
            queued += _enqueue(conn, account_id=row["id"], email=row["email"], kind="update",
                               dedupe_key=f"update:{campaign}:{row['id']}",
                               subject=subj, html=html, text=text, unsub=unsub)
    log.info("queued update %r to %s accounts", campaign, queued)
    return queued


def sent_today() -> int:
    with connect() as conn:
        return conn.execute(
            "SELECT COUNT(*) AS n FROM email_outbox WHERE status='sent' AND sent_at >= strftime('%Y-%m-%dT00:00:00','now')"
        ).fetchone()["n"]


def _housekeep(conn) -> None:
    stamp = now_iso()
    conn.execute("UPDATE email_outbox SET status='expired' WHERE status='queued' AND not_after IS NOT NULL"
                 " AND not_after<=?", (stamp,))
    stale = iso(utcnow() - timedelta(minutes=SENDING_STALE_MINUTES))
    conn.execute("UPDATE email_outbox SET status='queued' WHERE status='sending' AND next_attempt_at<=?",
                 (stale,))


def _claim(row_id: int) -> bool:
    with connect() as conn, tx(conn):
        return conn.execute(
            "UPDATE email_outbox SET status='sending', next_attempt_at=? WHERE id=? AND status='queued'",
            (now_iso(), row_id)).rowcount > 0


async def send_queued(max_batch: int = 20, kinds: tuple | None = None) -> dict:
    """Sends queued emails, most urgent first. Transactional kinds ignore the
    daily cap; the rest share what is left of it. Failed sends retry with
    backoff up to MAX_ATTEMPTS."""
    s = get_settings()
    room = max(0, min(max_batch, s.email_daily_cap - sent_today()))
    report = {"sent": 0, "failed": 0, "retrying": 0, "room": room}
    with connect() as conn, tx(conn):
        _housekeep(conn)
    kind_filter = ""
    params: list = [now_iso()]
    if kinds:
        kind_filter = f" AND o.kind IN ({','.join('?' for _ in kinds)})"
        params += list(kinds)
    with connect() as conn:
        rows = conn.execute(
            f"""SELECT o.id, o.email, o.subject, o.html, o.text, o.unsubscribe_url, o.kind, o.attempts,
                       a.email_updates, a.email_notices
                FROM email_outbox o LEFT JOIN accounts a ON a.id=o.account_id
                WHERE o.status='queued' AND (o.next_attempt_at IS NULL OR o.next_attempt_at<=?){kind_filter}
                ORDER BY o.priority, o.id LIMIT ?""", (*params, max_batch + room)).fetchall()
    for r in rows:
        transactional = r["kind"] in TRANSACTIONAL
        if not transactional:
            if room <= 0:
                continue
            room -= 1
        if transactional:
            allowed = True
        elif r["kind"] in ("low_balance", "referral"):
            allowed = r["email_notices"]
        else:
            allowed = r["email_updates"]
        if not _claim(r["id"]):
            continue
        attempts = int(r["attempts"] or 0)
        next_at = None
        if not allowed:
            status, error, sent_at = "skipped", None, None
        else:
            try:
                await mailer.send_email(r["email"], r["subject"], r["html"], r["text"],
                                        mailer.unsubscribe_headers(r["unsubscribe_url"]))
                status, error, sent_at = "sent", None, now_iso()
                report["sent"] += 1
            except Exception as exc:  # noqa: BLE001
                attempts += 1
                error, sent_at = str(exc)[:300], None
                if attempts >= MAX_ATTEMPTS:
                    status = "failed"
                    report["failed"] += 1
                else:
                    status = "queued"
                    next_at = iso(utcnow() + timedelta(minutes=5 * 2 ** (attempts - 1)))
                    report["retrying"] += 1
                log.warning("outbox email %s attempt %s failed: %s", r["id"], attempts, exc)
        with connect() as conn, tx(conn):
            conn.execute("UPDATE email_outbox SET status=?, error=?, sent_at=?, attempts=?, next_attempt_at=?"
                         " WHERE id=?", (status, error, sent_at, attempts, next_at, r["id"]))
    return report


def outbox_stats() -> dict:
    with connect() as conn:
        rows = conn.execute(
            "SELECT kind, status, COUNT(*) AS n FROM email_outbox WHERE kind<>'marker' GROUP BY kind, status"
        ).fetchall()
        opted_in = conn.execute("SELECT COUNT(*) AS n FROM accounts WHERE email_updates=1").fetchone()["n"]
    stats: dict = {}
    for r in rows:
        stats.setdefault(r["kind"], {})[r["status"]] = r["n"]
    return {"by_kind": stats, "sent_today": sent_today(), "daily_cap": get_settings().email_daily_cap,
            "accounts_opted_in": opted_in}