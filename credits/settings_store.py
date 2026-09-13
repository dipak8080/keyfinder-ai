"""
credits/settings_store.py - runtime-editable config, resolved DB -> env -> default.

WHY THIS EXISTS
---------------
Every number in credits/config.py came from an env var, and a Docker
container holds the environment it booted with. So changing a rate limit
or a free allowance meant editing .env and restarting - which is exactly
what /admin/credits/reload-config's own docstring admits it cannot fix.

This puts an optional override row in SQLite in front of each env var.
The env var stays the fallback, so nothing changes shape and an empty
settings table behaves byte-for-byte like today.

THREE RULES THAT MATTER
-----------------------
1. NO IMPORT OF credits.config. db.py already imports config for db_path,
   so a reverse import would be a cycle. The DB path is read from env
   directly here, which is correct anyway: where the database lives cannot
   itself be a row inside that database.

2. NEVER RAISE. This runs inside settings construction, which runs during
   boot before migrations have necessarily applied. A missing file, a
   missing table, a locked DB - all resolve to "no overrides" and the
   process boots on env exactly as it did before.

3. SECRETS ARE LOCKED. Keys in LOCKED_KEYS are never readable or writable
   through here, so the admin API cannot leak the signing key or set the
   DB path out from under a running process.

VALIDATION IS A TRIAL BOOT
--------------------------
A bad value must not be discoverable at the next restart. stage() lets
the admin route rebuild the whole Settings object with a candidate
override applied; if config.py's boot invariants reject it, the write
never happens. The invariants are already written and already correct -
this reuses them rather than reimplementing them one field at a time.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

log = logging.getLogger("credits.settings_store")

# Readable or writable by nobody through the admin surface.
LOCKED_KEYS = frozenset({
    "CREDITS_DB_PATH",
    "CREDITS_SECRET_KEY",
    "CREDITS_ADMIN_TOKEN",
    "IP_HASH_SALT",
    "PAYMENTS_WEBHOOK_SECRET",
    "PAYMENTS_API_KEY",
    "RESEND_API_KEY",
    "SMTP_USER",
    "SMTP_PASSWORD",
})

# Keys the admin UI lists, with the type it should render. Anything not
# here is still settable if it passes the trial boot, so adding a knob to
# config.py does not require editing this list first - it only affects
# what the panel shows by default.
KNOWN_KEYS: dict[str, dict] = {
    "PAYWALL_ENABLED": {"type": "bool", "group": "paywall"},
    "FREE_MONTHLY_OPS": {"type": "int", "group": "free tier"},
    "FREE_MONTHLY_OPS_PER_IP": {"type": "int", "group": "free tier"},
    "CREDIT_HOLD_TIMEOUT_MINUTES": {"type": "int", "group": "paywall"},
    "RUNPOD_USD_PER_GPU_SECOND": {"type": "float", "group": "metering"},
    "MAGIC_LINK_TTL_MINUTES": {"type": "int", "group": "auth"},
    "DEVICE_LINK_TTL_MINUTES": {"type": "int", "group": "auth"},
    "SESSION_TTL_DAYS": {"type": "int", "group": "auth"},
    "MAGIC_LINKS_PER_HOUR": {"type": "int", "group": "auth"},
    "DEVICE_LINKS_PER_HOUR": {"type": "int", "group": "auth"},
    "CLAIM_TTL_MINUTES": {"type": "int", "group": "payments"},
    "PAYMENTS_TEST_MODE": {"type": "bool", "group": "payments"},
    "MAIL_PROVIDER": {"type": "str", "group": "mail"},
    "SEPARATION_SHARED_RATE_LIMIT_MAX_REQUESTS": {"type": "int", "group": "separation limits"},
    "SEPARATION_SHARED_RATE_LIMIT_WINDOW_SECONDS": {"type": "int", "group": "separation limits"},
    "SEPARATION_SHARED_DAILY_MAX_REQUESTS": {"type": "int", "group": "separation limits"},
    "SEPARATION_SHARED_DAILY_WINDOW_SECONDS": {"type": "int", "group": "separation limits"},
}

_TOOLS = (
    "separate-hq", "stems-hq", "youtube/separate-hq", "youtube/stems-hq",
    "transcribe", "audio-to-midi-hq", "audio-to-midi-hq-mix", "audio-to-sheet",
)
_TOOL_FIELDS = (
    ("ENABLED", "bool"), ("CREDITS", "int"), ("FREE_UNDER_SECONDS", "float"),
    ("PAID_RATE_LIMIT", "int"), ("PAID_RATE_WINDOW", "int"),
    ("FREE_RATE_LIMIT", "int"), ("FREE_RATE_WINDOW", "int"),
)
for _tool in _TOOLS:
    _slug = _tool.upper().replace("-", "_").replace("/", "_")
    for _field, _type in _TOOL_FIELDS:
        KNOWN_KEYS[f"PAYWALL_TOOL_{_slug}_{_field}"] = {
            "type": _type, "group": f"tool: {_tool}",
        }

# LOCKED_KEYS is a blocklist, and a blocklist cannot defend an open key
# space: set_many() accepts any key, and describe() used to report
# os.getenv() for whatever it found in the settings table. Writing a junk
# row for RUNPOD_API_KEY was therefore enough to read the real one back.
#
# Two guards now, because either alone leaves a gap. This one stops such a
# row being created for anything that merely LOOKS like a credential;
# describe() separately refuses to report env_value for any key outside
# KNOWN_KEYS, which is an allowlist and closes the case this misses.
_SECRETISH_FRAGMENTS = (
    "SECRET", "TOKEN", "PASSWORD", "PASSWD", "API_KEY", "APIKEY",
    "CREDENTIAL", "PRIVATE", "SALT", "_KEY", "COOKIE", "WEBHOOK",
)


def _is_secretish(key: str) -> bool:
    if key in KNOWN_KEYS:
        return False
    upper = key.upper()
    return any(fragment in upper for fragment in _SECRETISH_FRAGMENTS)


_staged: threading.local = threading.local()


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _db_path() -> str:
    return os.getenv("CREDITS_DB_PATH", "data/credits.db")


@contextmanager
def _conn() -> Iterator[sqlite3.Connection]:
    path = _db_path()
    conn = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout=30000")
        yield conn
    finally:
        conn.close()


def load_overrides() -> dict[str, str]:
    """Every override currently in force. {} on any failure, always."""
    staged = getattr(_staged, "overrides", None)
    if staged is not None:
        return staged

    if not Path(_db_path()).exists():
        return {}
    try:
        with _conn() as conn:
            rows = conn.execute("SELECT key, value FROM settings").fetchall()
        return {r["key"]: r["value"] for r in rows if r["key"] not in LOCKED_KEYS}
    except sqlite3.Error:
        return {}
    except Exception:  # noqa: BLE001
        log.exception("settings overrides unreadable, falling back to env")
        return {}


def resolve(name: str) -> str | None:
    """The override for `name`, or None to fall through to env."""
    if name in LOCKED_KEYS:
        return None
    return load_overrides().get(name)


@contextmanager
def stage(overrides: dict[str, str]) -> Iterator[None]:
    """Run a block as if `overrides` were the whole settings table.

    Thread-local, so a trial build on one request cannot alter what any
    concurrent request resolves.
    """
    previous = getattr(_staged, "overrides", None)
    _staged.overrides = dict(overrides)
    try:
        yield
    finally:
        if previous is None:
            _staged.overrides = None
        else:
            _staged.overrides = previous


def validate(candidate: dict[str, str]) -> None:
    """Raise ValueError if `candidate` would not boot.

    Builds the real Settings object under the candidate overrides, so
    every invariant config.py already enforces at startup is enforced
    here instead, before the row is written.
    """
    from .config import build_settings

    merged = dict(load_overrides())
    for key, value in candidate.items():
        if value is None:
            merged.pop(key, None)
        else:
            merged[key] = value

    with stage(merged):
        try:
            build_settings()
        except Exception as exc:  # noqa: BLE001
            raise ValueError(str(exc)) from exc


def set_many(values: dict[str, str], *, actor: str = "admin", note: str = "") -> dict[str, str]:
    """Validate then write. All or nothing."""
    for key in values:
        if key in LOCKED_KEYS:
            raise ValueError(f"{key} cannot be set at runtime")
        if _is_secretish(key):
            raise ValueError(f"{key} looks like a credential and cannot be set at runtime")
        if not key or len(key) > 128:
            raise ValueError("setting key must be 1-128 chars")

    normalised = {k: (None if v is None else str(v)) for k, v in values.items()}
    validate(normalised)

    now = _now_iso()
    with _conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            for key, value in normalised.items():
                row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
                old = row["value"] if row else None
                if value is None:
                    conn.execute("DELETE FROM settings WHERE key=?", (key,))
                    action = "clear"
                else:
                    conn.execute(
                        "INSERT INTO settings (key, value, updated_at, updated_by)"
                        " VALUES (?,?,?,?) ON CONFLICT(key) DO UPDATE SET"
                        " value=excluded.value, updated_at=excluded.updated_at,"
                        " updated_by=excluded.updated_by",
                        (key, value, now, actor),
                    )
                    action = "set"
                conn.execute(
                    "INSERT INTO settings_audit"
                    " (key, old_value, new_value, action, actor, note, created_at)"
                    " VALUES (?,?,?,?,?,?,?)",
                    (key, old, value, action, actor, note, now),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    log.info("settings updated by %s: %s", actor, ", ".join(sorted(normalised)))
    return load_overrides()


def clear(key: str, *, actor: str = "admin", note: str = "") -> dict[str, str]:
    return set_many({key: None}, actor=actor, note=note)


def audit(limit: int = 100, key: str | None = None) -> list[dict]:
    try:
        with _conn() as conn:
            if key:
                rows = conn.execute(
                    "SELECT * FROM settings_audit WHERE key=?"
                    " ORDER BY id DESC LIMIT ?", (key, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM settings_audit ORDER BY id DESC LIMIT ?", (limit,),
                ).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.Error:
        return []


def describe() -> list[dict]:
    """Every known key with its effective value and where that value came from.

    env_value is reported ONLY for keys in KNOWN_KEYS. Anything else gets
    None, whether or not it happens to name a real environment variable -
    that allowlist is what stops this endpoint being a read primitive for
    the container's whole environment.
    """
    overrides = load_overrides()
    out = []
    for key, meta in sorted(KNOWN_KEYS.items(), key=lambda kv: (kv[1]["group"], kv[0])):
        env_value = os.getenv(key)
        if key in overrides:
            source, value = "db", overrides[key]
        elif env_value is not None:
            source, value = "env", env_value
        else:
            source, value = "default", None
        out.append({
            "key": key,
            "group": meta["group"],
            "type": meta["type"],
            "value": value,
            "source": source,
            "env_value": env_value,
            "overridden": key in overrides,
        })
    for key, value in sorted(overrides.items()):
        if key not in KNOWN_KEYS:
            out.append({
                "key": key, "group": "other", "type": "str", "value": value,
                "source": "db", "env_value": None, "overridden": True,
            })
    return out