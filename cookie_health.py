"""
cookie_health.py - persists what the RUNTIME learns about a cookie file.

ADDED 2026-08-16.

--------------------------------------------------------------------------
WHY THIS EXISTS

_parse_cookie_expiry() in cookie_upload.py reads the expiry timestamp
written into cookies.txt at export time. That answers exactly one
question: has the clock run out? It cannot answer the question that
actually took slot 3 down - did Google revoke this session server-side?

A revoked session keeps its future expiry date forever. The file sits on
disk at full size with a date a year out, and /admin/cookies/status
reports "13mo left" indefinitely. Every field the status endpoint has to
work with agrees the slot is healthy. It isn't, and Discord already said
so hours earlier.

The only component that ever LEARNS about a revocation is
_maybe_alert_cookie_expiry() in youtube.py, which sees yt-dlp's "cookies
are no longer valid" warning during a real download. Today that knowledge
goes to Discord and dies with the process. Nothing writes it down, so the
status endpoint has no way to ever find out - which means the dashboard
stays confidently wrong about a slot you have already been paged about.

This module is the missing write-down step: a small JSON sidecar next to
the cookie files, written when a revocation is confirmed and read back by
the status endpoint.

--------------------------------------------------------------------------
WHERE record_revoked() IS CALLED FROM, AND WHERE IT MUST NOT BE

CALL IT: at the alert site inside _maybe_alert_cookie_expiry(), after
COOKIE_EXPIRY_ALERT_THRESHOLD and the per-account cooldown have both been
satisfied. That threshold is the entire reason a one-off flaky check
doesn't page you; reusing it means the panel and Discord always agree.

DO NOT call it from _disable_cookie_account(). That looks like the
natural home and is a trap. It fires on _was_cookie_flagged(), which is
the same heuristic that disabled all three accounts inside ~40 seconds
during the 2026-08-08 CDN-timeout cascade - yt-dlp emits its cookie
warning on unrelated network failures. Wiring this there would paint
every slot dead in the admin panel from a network blip, and the flag
would then be worth less than nothing.

DO NOT call it from the worker process. It won't get the chance:
_maybe_alert_cookie_expiry() returns early when _record_events_enabled is
set, handing the occurrence to the parent via _record_event(). The alert
site therefore only executes in the long-lived parent, after
apply_events() replays it. That is why a plain threading.Lock is enough
here and no file locking is needed - there is exactly one writer process.
Atomic replace (below) still covers any reader that races it.

--------------------------------------------------------------------------
WHAT THIS DOES NOT FIX

This sits downstream of the reactive alert, so it inherits its one
structural limitation: a standby slot that is never USED is never learned
about. Slots 2 and 3 are failover, not rotation, so a dead one can still
sit unflagged until the day it's needed.

What changes is that once the runtime does find out, the finding
survives, instead of scrolling out of a Discord channel while the
dashboard keeps insisting the slot has a year left.

So: absence of a revoked flag is NOT proof of health. Presence of one IS
proof of death. That asymmetry is why the UI still refuses to mark any
slot "valid", and it is the honest reading of this file.
--------------------------------------------------------------------------
"""

import json
import os
import tempfile
import threading
import time
from typing import Optional

from config import logger, YT_COOKIES_PATH_DEFAULT


def _default_health_path() -> str:
    """
    Derived from the PRIMARY cookie path's directory rather than
    hardcoded, for the same reason cookie_upload.py imports its slot
    paths from config instead of reconstructing them: if the cookie
    volume ever moves, the health file has to move with it or it silently
    becomes a file nobody reads.

    Falls back to a hardcoded path only if the derivation produces
    nothing usable, which would mean YT_COOKIES_PATH is set to a bare
    filename - broken for the cookies themselves long before it matters
    here.
    """
    primary = os.environ.get("YT_COOKIES_PATH", YT_COOKIES_PATH_DEFAULT)
    directory = os.path.dirname(primary or "")
    return os.path.join(directory or "/app/data", "cookie_health.json")


HEALTH_PATH = os.environ.get("COOKIE_HEALTH_PATH") or _default_health_path()

# Hard bound on the file. Nothing should ever write more than three keys,
# but a caller bug that passes a per-request value as `path` would grow
# this without limit and eventually make the status endpoint slow for a
# reason nobody would think to look for. Oldest-by-last_seen_at are
# dropped past this.
MAX_RECORDS = 16

# yt-dlp error text can be long and echoes server-controlled strings. It
# is stored, returned by an admin endpoint, and rendered in the
# dashboard, so it gets truncated on the way in.
MAX_REASON_LEN = 300

_lock = threading.Lock()


# ---------- low-level I/O ----------
# Both halves are total functions: they degrade to a safe default rather
# than raising. A health sidecar is an ANNOTATION on the status endpoint.
# It must never be able to take down the thing it annotates, and it must
# never be able to fail a download.


def _read_raw() -> dict:
    try:
        with open(HEALTH_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as e:
        # Corrupt or unreadable behaves exactly like "nothing recorded".
        # Logged at warning because a corrupt file means the flag is gone
        # and the panel is back to trusting the export date - worth
        # knowing, not worth failing over.
        logger.warning(f"[COOKIE-HEALTH] Could not read {HEALTH_PATH}: {e}")
        return {}
    return data if isinstance(data, dict) else {}


def _write_raw(data: dict) -> bool:
    """
    Atomic replace. A crash or a full disk mid-write would otherwise leave
    a truncated file, which _read_raw() reads as "nothing recorded" - the
    flag would vanish silently and the panel would go back to claiming the
    slot is fine. Write to a temp file in the SAME directory (os.replace
    is only atomic within a filesystem) and swap it in.
    """
    tmp_path = None
    try:
        directory = os.path.dirname(HEALTH_PATH) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".cookie_health.", suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, HEALTH_PATH)
        tmp_path = None
        return True
    except Exception as e:
        logger.warning(f"[COOKIE-HEALTH] Could not persist {HEALTH_PATH}: {e}")
        return False
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


# ---------- validation ----------


def _valid_path(path) -> bool:
    """
    Guards the one caller mistake this module is actually exposed to.

    download_with_fallback is ANON-FIRST as of 2026-08-12, so the first
    attempt of every download carries no cookiefile at all and
    _get_active_account() returns None. _maybe_alert_cookie_expiry()
    turns that into the literal label "unknown/cookie-less attempt" for
    its Discord message. That label must never reach this module as a
    key - it isn't a file, it can't be mtime-checked, it can't be
    cleared by an upload, and it would sit in the sidecar forever.

    The caller guards on `account_path` being truthy; this is the second
    line of defence, and it also rejects the relative-path and
    empty-string cases.
    """
    return isinstance(path, str) and bool(path.strip()) and os.path.isabs(path)


def _coerce_entry(entry) -> Optional[dict]:
    """
    Normalizes one record read off disk, or returns None if it is
    unusable. Hand-edited JSON, a partial write from an older version, or
    a type that changed shape all land here - and all of them must
    degrade to "no record" rather than propagating a bad type into the
    status response.
    """
    if not isinstance(entry, dict):
        return None
    try:
        revoked_at = float(entry.get("revoked_at"))
    except (TypeError, ValueError):
        # A record with no usable timestamp can't be staleness-checked
        # against the file's mtime, which is the safety property that
        # keeps a flag from sticking forever. Without it, drop the record.
        return None
    if revoked_at <= 0:
        return None

    try:
        last_seen_at = float(entry.get("last_seen_at") or revoked_at)
    except (TypeError, ValueError):
        last_seen_at = revoked_at

    try:
        hits = max(0, int(entry.get("hits") or 0))
    except (TypeError, ValueError):
        hits = 0

    reason = entry.get("reason")
    if not isinstance(reason, str):
        reason = ""

    return {
        "revoked_at": revoked_at,
        "last_seen_at": last_seen_at,
        "hits": hits,
        "reason": reason[:MAX_REASON_LEN],
    }


def _is_stale(path: str, entry: dict) -> bool:
    """
    A record is stale when it predates the file's own mtime: the file was
    replaced after the verdict was written, so the verdict describes bytes
    that no longer exist.

    THIS IS THE LOAD-BEARING SAFETY PROPERTY OF THE MODULE. Without it a
    single revocation sticks to a slot forever, survives every re-export,
    and trains you to ignore the flag - which is strictly worse than never
    having built this, because the panel would then be confidently wrong
    in the other direction and you'd have no reason to doubt it.

    clear() on upload covers the normal path. This covers every other way
    a file can change: scp, a volume restore, an edit on the box, a
    container rebuild that re-copies the file. All of those bump mtime and
    drop the flag. Erring toward dropping is correct - a false "healthy"
    gets re-detected the next time the account is used, while a false
    "revoked" is permanent and unfalsifiable.

    A missing file is also stale: the slot already reports "missing" from
    os.path.exists() in cookies_status() and the record is orphaned.
    """
    try:
        return entry["revoked_at"] < os.path.getmtime(path)
    except OSError:
        return True


# ---------- public API ----------


def snapshot() -> dict:
    """
    Returns every LIVE record, keyed by cookie path, pruning stale and
    orphaned ones as a side effect.

    One read per call by design - cookies_status() iterates three slots,
    and calling get() per slot would read and parse the same file three
    times. There is deliberately no in-memory cache: the file is under a
    kilobyte and read only by an authenticated admin endpoint, so a cache
    would buy nothing and introduce a staleness bug where the panel keeps
    showing a flag you just cleared by uploading.
    """
    try:
        with _lock:
            raw = _read_raw()
            live = {}
            dropped = []

            for path, entry in raw.items():
                coerced = _coerce_entry(entry)
                if coerced is None or not _valid_path(path) or _is_stale(path, coerced):
                    dropped.append(path)
                    continue
                live[path] = coerced

            # Bound the file. Keep the most recently active records.
            if len(live) > MAX_RECORDS:
                ordered = sorted(live.items(), key=lambda kv: kv[1]["last_seen_at"], reverse=True)
                for path, _ in ordered[MAX_RECORDS:]:
                    dropped.append(path)
                    live.pop(path, None)

            if dropped:
                _write_raw(live)
                logger.info(
                    f"[COOKIE-HEALTH] Pruned {len(dropped)} stale/invalid "
                    f"record(s): {', '.join(dropped[:5])}"
                )

            return live
    except Exception as e:
        logger.warning(f"[COOKIE-HEALTH] snapshot() failed, treating as empty: {e}")
        return {}


def get(path: str) -> Optional[dict]:
    """Single-path convenience wrapper around snapshot()."""
    if not _valid_path(path):
        return None
    return snapshot().get(path)


def record_revoked(path: str, reason: str = "", hits: Optional[int] = None) -> bool:
    """
    Marks a cookie file as revoked by the runtime. Returns True if the
    record was written.

    Call ONLY from the alert site in _maybe_alert_cookie_expiry(), after
    the threshold and cooldown checks - see this module's docstring for
    why _disable_cookie_account() is the wrong place.

    revoked_at deliberately keeps the FIRST timestamp across repeat calls,
    so "Revoked 7h ago" in the panel lines up with the Discord message you
    actually received rather than resetting on every later download that
    hits the same dead account. last_seen_at tracks the most recent
    occurrence separately, since that is the useful field for pruning.
    """
    if not _valid_path(path):
        # The anon/cookie-less case. Not an error: it means the warning
        # arrived on an attempt with no cookiefile attached, which says
        # nothing about any specific account. Nothing to record.
        logger.info(
            f"[COOKIE-HEALTH] Ignoring revocation with no usable account "
            f"path (got {path!r}) - likely a cookie-less attempt."
        )
        return False

    now = time.time()
    try:
        with _lock:
            raw = _read_raw()
            existing = _coerce_entry(raw.get(path)) or {}

            if existing and _is_stale(path, existing):
                # File was replaced since the old verdict. Start clean
                # rather than inheriting a revoked_at that predates the
                # current bytes - that value would immediately read as
                # stale on the next snapshot() and silently discard the
                # flag being written right now.
                existing = {}

            if hits is None:
                resolved_hits = existing.get("hits", 0) + 1
            else:
                try:
                    resolved_hits = max(0, int(hits))
                except (TypeError, ValueError):
                    resolved_hits = existing.get("hits", 0) + 1

            raw[path] = {
                "revoked_at": existing.get("revoked_at") or now,
                "last_seen_at": now,
                "hits": resolved_hits,
                "reason": (reason or existing.get("reason", ""))[:MAX_REASON_LEN],
            }
            written = _write_raw(raw)
    except Exception as e:
        logger.warning(f"[COOKIE-HEALTH] record_revoked({path}) failed: {e}")
        return False

    if written:
        logger.warning(
            f"[COOKIE-HEALTH] '{path}' marked revoked - this will now show in "
            f"/admin/cookies/status until the slot is re-uploaded."
        )
    return written


def clear(path: str) -> bool:
    """
    Drops any recorded verdict for `path`. Returns True if something was
    actually removed.

    Called on upload: a fresh export replaces the bytes the verdict was
    about, so carrying it forward would flag a brand-new working file as
    dead. Also the manual override if you decide the runtime got it wrong.
    """
    if not _valid_path(path):
        return False
    try:
        with _lock:
            raw = _read_raw()
            if raw.pop(path, None) is None:
                return False
            _write_raw(raw)
    except Exception as e:
        logger.warning(f"[COOKIE-HEALTH] clear({path}) failed: {e}")
        return False

    logger.info(f"[COOKIE-HEALTH] Cleared revoked flag for {path}.")
    return True


def record_success(path: str) -> bool:
    """
    Optional self-heal: clears the flag when this cookie file completes a
    real download.

    A success is strictly stronger evidence than the revocation that
    preceded it - yt-dlp cannot finish an authenticated extraction with a
    session YouTube refuses, and Google does occasionally restore one.
    Without this the panel would keep showing a working account as dead
    until you noticed and re-exported for no reason.

    ONLY call this when a cookiefile was actually attached to the
    successful attempt. Since download_with_fallback is anon-first, the
    common success path has account_path=None - an anonymous success says
    nothing whatsoever about any account's health, and clearing on one
    would erase a true verdict. record_account_result() already carries
    the correct `path` value for this.
    """
    return clear(path)


def apply_to(path: str, parsed: dict, snap: Optional[dict] = None) -> dict:
    """
    Merges the runtime verdict into a _parse_cookie_expiry() result.

    Runtime evidence outranks the date. The date only claims the clock
    hasn't run out; the runtime watched YouTube refuse the session.
    expires_at / expires_in_days are left intact so the panel can still
    show the (now meaningless) date next to the real reason - seeing
    "Revoked 7h ago" above "Expires Sep 16, 2027" is the clearest possible
    statement of what actually happened here.

    "expired" and "no_auth_cookies" are NOT overridden. Both are already
    terminal, and both say something more specific about the file itself
    than "revoked" does: a logged-out export is a different mistake with a
    different fix than a killed session, and collapsing them loses that.

    Pass `snap` when annotating several slots in one request so the
    sidecar is read once instead of once per slot.

    Returns `parsed` unchanged on any failure. The status endpoint must
    not 500 because an annotation layer had a bad day.
    """
    try:
        if not isinstance(parsed, dict):
            return parsed
        records = snapshot() if snap is None else snap
        record = records.get(path)
        if not record:
            return parsed

        merged = dict(parsed)
        merged["revoked_at"] = record["revoked_at"]
        merged["revoked_reason"] = record.get("reason") or None
        merged["revoked_hits"] = record.get("hits") or None
        if merged.get("expiry_status") not in ("expired", "no_auth_cookies"):
            merged["expiry_status"] = "revoked"
        return merged
    except Exception as e:
        logger.warning(f"[COOKIE-HEALTH] apply_to({path}) failed, returning unannotated: {e}")
        return parsed


def health_status() -> dict:
    """
    Snapshot for /admin/status, so a revoked account is visible in the
    same place as the breakers rather than only on the cookies page.
    """
    try:
        records = snapshot()
        now = time.time()
        return {
            "path": HEALTH_PATH,
            "revoked_accounts": [
                {
                    "cookie_path": path,
                    "revoked_at": entry["revoked_at"],
                    "revoked_hours_ago": round((now - entry["revoked_at"]) / 3600, 1),
                    "hits": entry["hits"],
                    "reason": entry["reason"] or None,
                }
                for path, entry in sorted(records.items())
            ],
        }
    except Exception as e:
        logger.warning(f"[COOKIE-HEALTH] health_status() failed: {e}")
        return {"path": HEALTH_PATH, "revoked_accounts": []}