"""Settings for the credits package.

Deliberately does NOT import the backend's existing config.py — this package
reads its own env vars so it can be added without touching anything else.

Defaults are the "off" state: PAYWALL_ENABLED unset means nothing is charged
and nothing is blocked, on every tool.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

# The set of providers with an adapter in credits/providers/. Imported
# from there rather than restated here, so adding a provider is one file
# and cannot leave config validating against a stale list.
def _supported_providers() -> tuple[str, ...]:
    from .providers import SUPPORTED_PROVIDERS as _sp
    return _sp


# --- env helpers ------------------------------------------------------------

def _bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    try:
        return int(raw) if raw not in (None, "") else default
    except ValueError:
        return default


def _float(name: str, default: float) -> float:
    raw = os.getenv(name)
    try:
        return float(raw) if raw not in (None, "") else default
    except ValueError:
        return default


def _json(name: str, default: Any) -> Any:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{name} is not valid JSON: {exc}") from exc


def _csv(name: str, default: tuple[str, ...] = ()) -> tuple[str, ...]:
    raw = os.getenv(name)
    if not raw:
        return default
    return tuple(part.strip() for part in raw.split(",") if part.strip())


# --- defaults ---------------------------------------------------------------

# Keyed by ROUTE, not by tool family - deliberately the same strings the
# frontend's lib/data/rate-limits.ts uses ("separate-hq", "youtube/stems-hq")
# so the paywall config, the rate limits and the UI copy can be diffed by eye
# instead of translated between three naming schemes.
#
# Per-route rather than one "stem-separation" rule because the four HQ routes
# are genuinely independent products: /separate-hq is a 2-stem vocal remover,
# /stems-hq is a 4-stem split, and the youtube/* pair chain a download in
# front of each. Charging for the YouTube pair while leaving file upload free
# (or vice versa) is a pricing decision worth being able to make with one env
# var, not a code change. They all draw from ONE credit balance regardless.
#
# free_under_seconds is 0 for all four: unlike transcription, HQ separation is
# expensive at every length - MAX_SEPARATION_DURATION_SECONDS_HQ already caps
# input at 6 minutes, so there is no "short enough to be free" band to carve
# out. Left in the schema because transcription will need it if it's ever
# metered.
#
# paid_rate_limit / paid_rate_window apply ONLY to callers who hold credits;
# see credits/limits.py for why loosening it for them is safe and why the free
# numbers stay in the host config.py. 12/hour is deliberately not "unlimited":
# it bounds a compromised-cookie or shared-account worst case to something
# recoverable, while being far above any real session (nobody separates twelve
# tracks an hour by hand).
DEFAULT_TOOL_RULES: dict[str, dict[str, Any]] = {
    "separate-hq": {
        "enabled": False, "free_under_seconds": 0, "credits": 1,
        "paid_rate_limit": 12, "paid_rate_window": 3600,
    },
    "stems-hq": {
        "enabled": False, "free_under_seconds": 0, "credits": 1,
        "paid_rate_limit": 12, "paid_rate_window": 3600,
    },
    "youtube/separate-hq": {
        "enabled": False, "free_under_seconds": 0, "credits": 1,
        "paid_rate_limit": 12, "paid_rate_window": 3600,
    },
    "youtube/stems-hq": {
        "enabled": False, "free_under_seconds": 0, "credits": 1,
        "paid_rate_limit": 12, "paid_rate_window": 3600,
    },
    # Not metered at launch. Present so the machinery exists the day it is,
    # and so /credits/me reports it as free rather than as unknown.
    "transcribe": {
        "enabled": False, "free_under_seconds": 600, "credits": 1,
        "paid_rate_limit": 6, "paid_rate_window": 3600,
    },
}

DEFAULT_PACKS: dict[str, dict[str, Any]] = {
    "starter": {"credits": 10, "price_usd": 3, "label": "10 credits"},
    "regular": {"credits": 30, "price_usd": 8, "label": "30 credits"},
    "bulk": {"credits": 100, "price_usd": 20, "label": "100 credits"},
}


@dataclass(frozen=True)
class ToolRule:
    tool: str                    # route key: "separate-hq", "youtube/stems-hq", ...
    enabled: bool                # metered? False = behaves exactly as it does today
    free_under_seconds: float    # inputs shorter than this stay free (0 = never)
    credits: int                 # cost per job
    paid_rate_limit: int         # requests/window for callers WITH credits
    paid_rate_window: int        # window in seconds for the above


@dataclass(frozen=True)
class Pack:
    key: str
    credits: int
    price_usd: float
    label: str
    # Ko-fi: the shop item's direct_link_code (the bit after ko-fi.com/s/).
    price_ref: str = ""
    # Ko-fi: https://ko-fi.com/s/<direct_link_code>. Required, since Ko-fi has
    # no checkout API to create sessions against - the buy link IS the checkout.
    buy_url: str = ""

    def resolved_buy_url(self, provider: str, store_slug: str) -> str:
        if self.buy_url:
            return self.buy_url
        if provider == "kofi" and self.price_ref:
            return f"https://ko-fi.com/s/{self.price_ref}"
        return ""


@dataclass(frozen=True)
class Settings:
    # core
    db_path: str
    secret_key: str
    ip_hash_salt: str
    frontend_url: str
    api_base_url: str
    allowed_origins: tuple[str, ...]
    cookie_domain: str | None
    cookie_secure: bool
    cookie_samesite: str
    trust_cf_ip: bool

    # paywall
    paywall_enabled: bool
    tool_rules: dict[str, ToolRule]
    free_monthly_ops: int
    free_monthly_ops_per_ip: int
    hold_timeout_minutes: int

    # auth
    magic_link_ttl_minutes: int
    session_ttl_days: int
    magic_links_per_hour: int

    # payments
    payments_provider: str
    webhook_secret: str          # Ko-fi: the verification token
    provider_api_key: str        # unused for Ko-fi
    provider_store_id: str       # unused for Ko-fi
    provider_store_slug: str     # Ko-fi: your page slug, e.g. 'audioforges'
    provider_test_mode: bool
    claim_ttl_minutes: int       # how long a pre-checkout email claim stays matchable
    packs: dict[str, Pack]

    # mail
    mail_provider: str           # resend | smtp | console
    mail_from: str
    mail_from_name: str
    resend_api_key: str
    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_password: str
    smtp_starttls: bool

    # metering
    runpod_usd_per_gpu_second: float
    admin_token: str

    def rule_for(self, tool: str) -> ToolRule | None:
        return self.tool_rules.get(tool)

    def pack(self, key: str) -> Pack | None:
        return self.packs.get(key)

    def packs_sorted(self) -> list[Pack]:
        return sorted(self.packs.values(), key=lambda p: p.credits)

    def pack_by_price_ref(self, ref: str) -> Pack | None:
        """Webhook lookup: which pack did they buy?"""
        if not ref:
            return None
        target = str(ref).strip().lower()
        for pack in self.packs.values():
            if pack.price_ref and pack.price_ref.strip().lower() == target:
                return pack
        return None

    def pack_by_amount(self, amount_usd: float, tolerance: float = 0.01) -> Pack | None:
        """Ko-fi fallback: plain donations carry no item code, so match on the
        amount paid. Used only when price_ref lookup fails."""
        for pack in self.packs_sorted():
            if abs(pack.price_usd - amount_usd) <= tolerance:
                return pack
        return None


def _load_tool_rules() -> dict[str, ToolRule]:
    raw = {tool: dict(cfg) for tool, cfg in DEFAULT_TOOL_RULES.items()}

    override = _json("PAYWALL_TOOL_RULES", None)
    if isinstance(override, dict):
        for tool, cfg in override.items():
            merged = dict(raw.get(tool, {"enabled": False, "free_under_seconds": 0, "credits": 1}))
            merged.update(cfg or {})
            raw[tool] = merged

    rules: dict[str, ToolRule] = {}
    for tool, cfg in raw.items():
        # "youtube/separate-hq" -> PAYWALL_TOOL_YOUTUBE_SEPARATE_HQ_ENABLED
        slug = tool.upper().replace("-", "_").replace("/", "_")
        rules[tool] = ToolRule(
            tool=tool,
            enabled=_bool(f"PAYWALL_TOOL_{slug}_ENABLED", bool(cfg.get("enabled", False))),
            free_under_seconds=_float(
                f"PAYWALL_TOOL_{slug}_FREE_UNDER_SECONDS",
                float(cfg.get("free_under_seconds", 0) or 0),
            ),
            credits=_int(f"PAYWALL_TOOL_{slug}_CREDITS", int(cfg.get("credits", 1) or 1)),
            paid_rate_limit=_int(f"PAYWALL_TOOL_{slug}_PAID_RATE_LIMIT",
                                 int(cfg.get("paid_rate_limit", 12) or 12)),
            paid_rate_window=_int(f"PAYWALL_TOOL_{slug}_PAID_RATE_WINDOW",
                                  int(cfg.get("paid_rate_window", 3600) or 3600)),
        )
    return rules


def _load_packs() -> dict[str, Pack]:
    raw = {key: dict(cfg) for key, cfg in DEFAULT_PACKS.items()}

    override = _json("CREDIT_PACKS", None)
    if isinstance(override, dict):
        for key, cfg in override.items():
            merged = dict(raw.get(key, {}))
            merged.update(cfg or {})
            raw[key] = merged

    packs: dict[str, Pack] = {}
    for key, cfg in raw.items():
        slug = key.upper()
        credits = _int(f"PACK_{slug}_CREDITS", int(cfg.get("credits", 0) or 0))
        packs[key] = Pack(
            key=key,
            credits=credits,
            price_usd=_float(f"PACK_{slug}_PRICE_USD", float(cfg.get("price_usd", 0) or 0)),
            label=str(cfg.get("label") or f"{credits} credits"),
            price_ref=str(os.getenv(f"PACK_{slug}_PRICE_REF") or cfg.get("price_ref", "") or ""),
            buy_url=str(os.getenv(f"PACK_{slug}_BUY_URL") or cfg.get("buy_url", "") or ""),
        )
    return packs


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    secret = os.getenv("CREDITS_SECRET_KEY", "")
    if len(secret) < 32:
        raise RuntimeError(
            "CREDITS_SECRET_KEY must be at least 32 chars. Generate one with:\n"
            '  python -c "import secrets; print(secrets.token_urlsafe(48))"'
        )

    provider = os.getenv("PAYMENTS_PROVIDER", "kofi").lower().strip()
    supported = _supported_providers()
    if provider not in supported:
        raise RuntimeError(f"PAYMENTS_PROVIDER={provider!r} is not one of {supported}")

    settings = Settings(
        db_path=os.getenv("CREDITS_DB_PATH", "data/credits.db"),
        secret_key=secret,
        ip_hash_salt=os.getenv("IP_HASH_SALT") or secret,
        frontend_url=(os.getenv("FRONTEND_URL") or "https://audioforges.com").rstrip("/"),
        api_base_url=(os.getenv("CREDITS_API_BASE_URL") or "https://api.audioforges.com").rstrip("/"),
        allowed_origins=_csv(
            "CREDITS_ALLOWED_ORIGINS",
            ("https://audioforges.com", "https://www.audioforges.com"),
        ),
        cookie_domain=os.getenv("COOKIE_DOMAIN") or None,
        cookie_secure=_bool("COOKIE_SECURE", True),
        cookie_samesite=os.getenv("COOKIE_SAMESITE", "lax").lower(),
        trust_cf_ip=_bool("TRUST_CF_CONNECTING_IP", True),

        paywall_enabled=_bool("PAYWALL_ENABLED", False),
        tool_rules=_load_tool_rules(),
        free_monthly_ops=_int("FREE_MONTHLY_OPS", 2),
        free_monthly_ops_per_ip=_int("FREE_MONTHLY_OPS_PER_IP", 4),
        hold_timeout_minutes=_int("CREDIT_HOLD_TIMEOUT_MINUTES", 90),

        magic_link_ttl_minutes=_int("MAGIC_LINK_TTL_MINUTES", 30),
        session_ttl_days=_int("SESSION_TTL_DAYS", 365),
        magic_links_per_hour=_int("MAGIC_LINKS_PER_HOUR", 5),

        payments_provider=provider,
        webhook_secret=os.getenv("PAYMENTS_WEBHOOK_SECRET", ""),
        provider_api_key=os.getenv("PAYMENTS_API_KEY", ""),
        provider_store_id=os.getenv("PAYMENTS_STORE_ID", ""),
        provider_store_slug=os.getenv("PAYMENTS_STORE_SLUG", "audioforges"),
        provider_test_mode=_bool("PAYMENTS_TEST_MODE", False),
        claim_ttl_minutes=_int("CLAIM_TTL_MINUTES", 120),
        packs=_load_packs(),

        mail_provider=os.getenv("MAIL_PROVIDER", "console").lower(),
        mail_from=os.getenv("MAIL_FROM", "noreply@audioforges.com"),
        mail_from_name=os.getenv("MAIL_FROM_NAME", "AudioForges"),
        resend_api_key=os.getenv("RESEND_API_KEY", ""),
        smtp_host=os.getenv("SMTP_HOST", ""),
        smtp_port=_int("SMTP_PORT", 587),
        smtp_user=os.getenv("SMTP_USER", ""),
        smtp_password=os.getenv("SMTP_PASSWORD", ""),
        smtp_starttls=_bool("SMTP_STARTTLS", True),

        runpod_usd_per_gpu_second=_float("RUNPOD_USD_PER_GPU_SECOND", 0.00019),
        admin_token=os.getenv("CREDITS_ADMIN_TOKEN", ""),
    )

    # Fail at boot, not at the first checkout.
    if settings.paywall_enabled:
        if not settings.webhook_secret:
            raise RuntimeError(
                "PAYWALL_ENABLED=true requires PAYMENTS_WEBHOOK_SECRET "
                "(Ko-fi: Settings -> API -> Verification token)"
            )
        for pack in settings.packs_sorted():
            if not pack.resolved_buy_url(provider, settings.provider_store_slug):
                raise RuntimeError(
                    f"PAYWALL_ENABLED=true but pack '{pack.key}' has no checkout link. "
                    f"Set PACK_{pack.key.upper()}_PRICE_REF (Ko-fi shop item code) "
                    f"or PACK_{pack.key.upper()}_BUY_URL"
                )
        if provider == "kofi":
            refs = [p.price_ref.strip().lower() for p in settings.packs.values() if p.price_ref]
            if len(refs) != len(set(refs)):
                raise RuntimeError("Two packs share the same PACK_*_PRICE_REF — credits would be ambiguous")
            prices = [p.price_usd for p in settings.packs.values()]
            if len(prices) != len(set(prices)):
                raise RuntimeError(
                    "Two packs share the same price — the Ko-fi amount fallback couldn't tell them apart"
                )
    return settings


def reload_settings() -> Settings:
    """Re-read env without restarting the process (admin action, step 7)."""
    get_settings.cache_clear()
    return get_settings()