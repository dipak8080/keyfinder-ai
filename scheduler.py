"""One scheduler for the periodic jobs that must survive redeploys.

Each task's last start time lives in credits.db (scheduler_runs), so a
deploy never resets a schedule: a few minutes after boot every overdue task
runs, then each runs again whenever its interval has passed. Only the live
slot runs anything."""

import asyncio
import json
from dataclasses import dataclass
from typing import Callable

from config import logger

STARTUP_DELAY_SECONDS = 180
TICK_SECONDS = 60


@dataclass(frozen=True)
class Task:
    name: str
    interval_seconds: int
    run: Callable
    is_async: bool = False


def _pass_sync():
    from credits.db import connect
    from credits.providers import dodo as dd
    from credits.subscriptions import sync_with_dodo

    if not dd.configured():
        return {"skipped": "dodo_not_configured"}
    with connect() as conn:
        if not conn.execute("SELECT 1 FROM subscriptions LIMIT 1").fetchone():
            return {"skipped": "no_subscriptions"}
    return sync_with_dodo()


def _pass_expiry():
    from credits.passlots import expire_due
    return expire_due()


def _library_sweep():
    import library
    return {"removed": library.sweep_expired()}


def _free_song():
    from credits.notifications import queue_monthly_free_song
    return {"queued": queue_monthly_free_song()}


async def _email_outbox():
    from credits.notifications import send_queued
    return await send_queued()


TASKS = (
    Task("pass_sync", 6 * 3600, _pass_sync),
    Task("pass_expiry", 3600, _pass_expiry),
    Task("library_sweep", 6 * 3600, _library_sweep),
    Task("free_song_email", 3600, _free_song),
    Task("email_outbox", 300, _email_outbox, is_async=True),
)


def _is_live() -> bool:
    from credits.admin import slot_info
    return slot_info().get("is_active") is not False


def _due(task: Task) -> bool:
    from credits.db import connect, parse_ts, utcnow
    with connect() as conn:
        row = conn.execute("SELECT last_started_at FROM scheduler_runs WHERE name=?", (task.name,)).fetchone()
    last = parse_ts(row["last_started_at"]) if row else None
    return last is None or (utcnow() - last).total_seconds() >= task.interval_seconds


def _mark_started(name: str) -> None:
    from credits.db import connect, now_iso, tx
    with connect() as conn, tx(conn):
        conn.execute(
            """INSERT INTO scheduler_runs (name, last_started_at) VALUES (?, ?)
               ON CONFLICT(name) DO UPDATE SET last_started_at=excluded.last_started_at""",
            (name, now_iso()))


def _mark_finished(name: str, ok: bool, error: str | None, result) -> None:
    from credits.db import connect, now_iso, tx
    try:
        text = json.dumps(result, default=str)[:2000] if result is not None else None
    except (TypeError, ValueError):
        text = str(result)[:2000]
    with connect() as conn, tx(conn):
        conn.execute(
            "UPDATE scheduler_runs SET last_finished_at=?, last_ok=?, last_error=?, last_result=? WHERE name=?",
            (now_iso(), 1 if ok else 0, (error or "")[:500] or None, text, name))


_running: set = set()
_tasks_in_flight: set = set()


async def _run(task: Task) -> None:
    _running.add(task.name)
    try:
        await asyncio.to_thread(_mark_started, task.name)
        if task.is_async:
            result = await task.run()
        else:
            result = await asyncio.to_thread(task.run)
        await asyncio.to_thread(_mark_finished, task.name, True, None, result)
        if isinstance(result, dict) and (result.get("errors") or result.get("failed")
                                         or result.get("payments_applied") or result.get("credits")):
            logger.warning(f"[SCHEDULER] {task.name}: {result}")
    except asyncio.CancelledError:
        raise
    except Exception as e:  # noqa: BLE001
        logger.error(f"[SCHEDULER] {task.name} failed: {e}", exc_info=True)
        try:
            await asyncio.to_thread(_mark_finished, task.name, False, str(e), None)
        except Exception:  # noqa: BLE001
            pass
    finally:
        _running.discard(task.name)


async def tick() -> list[str]:
    """Starts every task that is due and not already running."""
    if not await asyncio.to_thread(_is_live):
        return []
    started = []
    for task in TASKS:
        if task.name in _running:
            continue
        try:
            due = await asyncio.to_thread(_due, task)
        except Exception as e:  # noqa: BLE001
            logger.error(f"[SCHEDULER] could not read schedule for {task.name}: {e}")
            continue
        if due:
            job = asyncio.create_task(_run(task))
            _tasks_in_flight.add(job)
            job.add_done_callback(_tasks_in_flight.discard)
            started.append(task.name)
    return started


async def scheduler_loop() -> None:
    await asyncio.sleep(STARTUP_DELAY_SECONDS)
    while True:
        try:
            await tick()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            logger.error(f"[SCHEDULER] tick failed: {e}", exc_info=True)
        await asyncio.sleep(TICK_SECONDS)


async def shutdown() -> None:
    for job in list(_tasks_in_flight):
        job.cancel()
    for job in list(_tasks_in_flight):
        try:
            await job
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass


def status() -> dict:
    from credits.db import connect, iso, parse_ts
    from datetime import timedelta
    with connect() as conn:
        rows = {r["name"]: dict(r) for r in conn.execute("SELECT * FROM scheduler_runs").fetchall()}
    out = []
    for task in TASKS:
        row = rows.get(task.name, {})
        last = parse_ts(row.get("last_started_at"))
        out.append({
            "name": task.name,
            "interval_seconds": task.interval_seconds,
            "running": task.name in _running,
            "last_started_at": row.get("last_started_at"),
            "last_finished_at": row.get("last_finished_at"),
            "last_ok": None if row.get("last_ok") is None else bool(row["last_ok"]),
            "last_error": row.get("last_error"),
            "last_result": row.get("last_result"),
            "next_due_at": iso(last + timedelta(seconds=task.interval_seconds)) if last else "on next tick",
        })
    return {"tasks": out, "startup_delay_seconds": STARTUP_DELAY_SECONDS, "tick_seconds": TICK_SECONDS}