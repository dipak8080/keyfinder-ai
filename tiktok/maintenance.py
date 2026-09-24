"""
tiktok/maintenance.py - The "TikTok is down, we know" switch.

Flipped by hand from the admin dashboard when the canary or the logs show
TikTok extraction is broken sitewide (an extractor change, a new page
shape, a TikTok-side outage). While it is on, /tiktok-to-mp3 answers
every fresh request with a clear message instead of a generic "something
went wrong" after a failed extraction, and the tool page shows the same
notice before the user pastes anything.

Cache hits still serve while the switch is on: they never touch TikTok.

Lives in Redis so the API process and the admin process see the same
value and it survives a container restart. If Redis is unreachable the
switch reads as OFF - a broken Redis must not take the tool down on its
own. Nothing here is a circuit breaker; it changes only when a person
changes it.
"""
import json
import time
from typing import Optional

from config import logger
from redis_store import client as _redis

_KEY = "tiktok:maintenance"

DEFAULT_MESSAGE = (
    "TikTok downloads are temporarily unavailable while we fix a change "
    "on TikTok's side. Please check back in a little while."
)


def get_state() -> dict:
    """{"on": bool, "message": str, "since": float|None}. Never raises."""
    try:
        raw = _redis.get(_KEY)
    except Exception as e:
        logger.warning(f"[TIKTOK] maintenance flag read failed, treating as off: {e}")
        return {"on": False, "message": DEFAULT_MESSAGE, "since": None}
    if not raw:
        return {"on": False, "message": DEFAULT_MESSAGE, "since": None}
    try:
        data = json.loads(raw)
    except Exception:
        return {"on": False, "message": DEFAULT_MESSAGE, "since": None}
    return {
        "on": bool(data.get("on")),
        "message": (data.get("message") or DEFAULT_MESSAGE).strip(),
        "since": data.get("since"),
    }


def is_on() -> bool:
    return get_state()["on"]


def set_state(on: bool, message: Optional[str] = None) -> dict:
    """Writes the flag. Keeps the existing `since` while it stays on so the
    dashboard can show how long the tool has been paused."""
    current = get_state()
    msg = (message or "").strip() or current["message"] or DEFAULT_MESSAGE
    since = current["since"] if (on and current["on"]) else (time.time() if on else None)
    data = {"on": bool(on), "message": msg[:300], "since": since}
    _redis.set(_KEY, json.dumps(data))
    logger.warning(f"[TIKTOK] maintenance {'ON' if on else 'OFF'}: {msg[:120]}")
    return data