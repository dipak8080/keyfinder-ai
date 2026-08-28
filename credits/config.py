"""Settings for the credits package.

Deliberately does NOT import the backend's existing config.py — this package
reads its own env vars so it can be added without touching anything else.

Defaults are the "off" state: PAYWALL_ENABLED unset means nothing is charged
and nothing is blocked, on every tool.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, replace
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
# TRANSCRIPTION IS THE EXCEPTION, and it is a deliberate one. All three
# transcription endpoints (/speech-to-text, /youtube/transcribe,
# /video-to-text) share the single key "transcribe" rather than getting one
# key each. The separation routes are separate products with separate costs;
# the transcription routes are three front doors onto ONE RunPod endpoint and
# ONE MAX_CONCURRENT_TRANSCRIPTIONS pool. Giving them a key each would hand a
# single caller three independent budgets for one resource - which is exactly
# the drift the host config.py already complains about in its note on the
# per-path YouTube limits. One resource, one bucket.
#
# AUDIO-TO-MIDI-HQ is the second one-key-per-product entry, added
# 2026-08-28, and it is the opposite shape to transcription: one route, one
# key, but a genuinely different PRODUCT from the free /audio-to-midi rather
# than the same product run harder. See its own note below - the short
# version is that basic-pitch and YourMT3 do not share an argument list, so
# they could not have shared a route even if the pricing suited it.
#
# free_under_seconds is 0 on every rule in this dict. For HQ separation the
# reason is that it is expensive at every length - MAX_SEPARATION_DURATION_SECONDS_HQ
# already caps input, so there is no "short enough to be free" band to carve
# out. For transcription and HQ MIDI the reason is different and simpler: the
# free allowance IS the free tier, and a duration exemption stacked on top of
# it is one more rule a user has to hold in their head for no gain.
#
# ---------------------------------------------------------------------------
# RATE LIMITS: THREE GUARDS, THREE JOBS. Do not conflate them.
#
# This block exists because the first version DID conflate them, and shipped a
# free hourly limit of 1/hour alongside a free monthly allowance of 2. That
# combination is not merely tight, it is CONTRADICTORY: the second free run
# was unreachable in a single sitting, so half the allowance we advertised
# could not be spent by a normal person. Nobody comes back an hour later to
# use a free trial.
#
#   1. GPU SPEND is guarded by FREE_MONTHLY_OPS (2/user, 4/IP) and by the
#      credit balance. This is the guard that actually bounds the bill, and
#      it is exact: 2 free runs x ~$0.018-0.024 = under $0.05 per user per
#      month, across every metered tool combined.
#
#   2. UPLOAD / BANDWIDTH ABUSE cannot be guarded here at all, which is worth
#      stating plainly because it is easy to assume otherwise. Rate limiting
#      runs as a FastAPI dependency, and the multipart body has already been
#      read by then - a 429 arrives AFTER the bytes. The only layer that can
#      reject before bytes reach the VPS is Cloudflare. See the deployment
#      notes; that is a WAF rate-limiting rule, not application code.
#
#   3. QUEUE FAIRNESS is MAX_CONCURRENT_SEPARATIONS (2, matched to the RunPod
#      worker count), MAX_QUEUED_SEPARATIONS (6 in flight before a 503), and
#      their transcription equivalents. Those are global. A per-subject
#      concurrency cap is the precise tool if one buyer ever starves the
#      queue; an hourly rate limit is the blunt one, and it hurts the paying
#      customer batching an album far more than it hurts anyone abusive.
#
# What is left for the hourly limit, once those three are placed correctly, is
# a SAFETY NET - not a primary control. So it should be set for humans.
#
# FREE: derived from FREE_MONTHLY_OPS by default rather than hand-set, so the
# contradiction above cannot be re-introduced by editing one number and
# forgetting the other. get_settings() enforces free_rate_limit >=
# free_monthly_ops at boot and refuses to start otherwise - the same fail-at-
# boot treatment the pack configuration already gets, for the same reason: a
# limit that silently makes your own free tier unusable is worse than a
# container that won't start.
#
# PAID: 30/hour. For a credit holder the BALANCE is the rate limit - every run
# is already paid for, so an hourly cap protects nothing financial. Its only
# job is stopping one buyer from monopolising the queue, and 30/hour is far
# above any human workflow while still bounding a compromised-cookie worst
# case to something recoverable. Someone who buys 100 credits to batch an
# album and gets throttled at 12 has been punished for paying, which is the
# worst possible moment to add friction.
DEFAULT_TOOL_RULES: dict[str, dict[str, Any]] = {
    "separate-hq": {
        "enabled": False, "free_under_seconds": 0, "credits": 1,
        "paid_rate_limit": 30, "paid_rate_window": 3600,
        "free_rate_limit": 0,  # 0 = derive from FREE_MONTHLY_OPS
    },
    "stems-hq": {
        "enabled": False, "free_under_seconds": 0, "credits": 1,
        "paid_rate_limit": 30, "paid_rate_window": 3600,
        "free_rate_limit": 0,
    },
    "youtube/separate-hq": {
        "enabled": False, "free_under_seconds": 0, "credits": 1,
        "paid_rate_limit": 30, "paid_rate_window": 3600,
        "free_rate_limit": 0,
    },
    "youtube/stems-hq": {
        "enabled": False, "free_under_seconds": 0, "credits": 1,
        "paid_rate_limit": 30, "paid_rate_window": 3600,
        "free_rate_limit": 0,
    },
    # ---- TRANSCRIPTION ----------------------------------------------------
    # ONE key for three routes - /speech-to-text, /youtube/transcribe and
    # /video-to-text. See the "TRANSCRIPTION IS THE EXCEPTION" note above.
    #
    # PRICED FROM MEASUREMENT, NOT FROM A GUESS. RunPod's own dashboard for
    # the Whisper endpoint, over a 9-day window: 57 requests, 7,200 GPU
    # seconds, so ~126 GPU-seconds and ~$0.024 per transcription. That is
    # MORE per job than an HQ separation (~$0.018), which is what settles two
    # questions that were otherwise open:
    #
    #   1 credit, not 2. $0.024 against $0.20-0.30 of revenue is still a wide
    #   margin, and pricing this above separation would need a reason a user
    #   can see - "the transcription costs double" is not one when both take
    #   about the same wall-clock time from their side. It is also what keeps
    #   the refund path safe: refund_job() currently hardcodes a -1 free-op
    #   adjustment while charge_for_job() bumps by credits_needed, so any rule
    #   with credits > 1 would silently under-refund the free tier on a failed
    #   job. Fix that before raising this number.
    #
    #   free_under_seconds: 0, changed from 600. The original 600 assumed
    #   transcription was cheap at short lengths and worth carving out a free
    #   band for. The measurement says the cost is real at ordinary lengths,
    #   and - more decisively - transcription is not what brings people to the
    #   site. The eighteen ffmpeg tools and the standard separation routes are
    #   the draw, and they stay free forever. A free band here would have cost
    #   money to defend a funnel that runs through different tools entirely.
    #
    #   Worth knowing what 0 does NOT mean: it is not "nothing is free".
    #   FREE_MONTHLY_OPS still applies, so every visitor gets 2 free metered
    #   runs a month before they ever see a paywall. 0 only removes the
    #   duration exemption on top of that.
    #
    # THE COST NUMBER TO WATCH IS NOT THE ONE ABOVE. 95 cold starts against 57
    # requests in that same window means workers are dying between jobs and
    # every spin-up is billed. That is a bigger lever on the bill than any
    # rule in this file, and no paywall setting touches it - it is the
    # endpoint's idle timeout.
    "transcribe": {
        "enabled": False, "free_under_seconds": 0, "credits": 1,
        "paid_rate_limit": 30, "paid_rate_window": 3600,
        "free_rate_limit": 0,
    },
    # ---- AUDIO TO MIDI, HIGH QUALITY -------------------------------------
    # A SEPARATE PRODUCT from /audio-to-midi, not a quality flag on it -
    # which is why it gets its own key rather than a "quality" field on a
    # shared one. The distinction matters more here than anywhere else in
    # this dict, because everywhere else "-hq" means the same product run
    # harder:
    #
    #   /separate vs /separate-hq   same Demucs, bigger model, identical
    #                               parameters and output shape.
    #
    #   /audio-to-midi              basic-pitch. ANY instrument. Six
    #   vs /audio-to-midi-hq        tunable parameters. One MIDI track.
    #                               YourMT3. MULTI-instrument, one track
    #                               per instrument with a General MIDI
    #                               program assigned. ZERO tunable
    #                               parameters.
    #
    # The free tool stays free forever. This is an upgrade path, not a
    # paywall dropped in front of something people already use - the same
    # arrangement standard vs HQ separation has, and the reason /audio-to-
    # midi's traffic is safe to keep unmetered.
    #
    # PRICED FROM MEASUREMENT. Measured on this VPS's CPU at ~2x realtime
    # (13.7s of audio in 25s, a 4.5-minute track in ~9 minutes); on a
    # RunPod GPU that lands around 10-20 GPU-seconds for a 4-minute track,
    # roughly $0.002-0.004 a job. CHEAPER than an HQ separation (~$0.018)
    # and far cheaper than a transcription per minute of audio.
    #
    #   1 credit, and the margin is not the reason. At ~$0.003 against
    #   $0.20-0.30 of revenue this could carry 2 credits comfortably on
    #   cost alone. It does not, because refund_job() still hardcodes a -1
    #   free-op adjustment while charge_for_job() bumps by credits_needed:
    #   any rule with credits > 1 silently under-refunds the free tier on
    #   a failed job, permanently eating an op the user never spent.
    #
    #   That fix is a free_ops column on job_charges, written at charge
    #   time. Until it lands, 1 is not a pricing decision - it is the only
    #   safe value, for this rule and every other one in this dict.
    #
    #   free_under_seconds: 0, matching every other rule here. There is no
    #   "short enough to be free" band worth carving out when the whole
    #   job costs a third of a cent; the free allowance is the free tier,
    #   and a duration exemption on top would only complicate what a user
    #   has to understand.
    #
    # SHARES THE FREE ALLOWANCE with every other metered tool -
    # free_usage has no tool in its key, so FREE_MONTHLY_OPS is a pool of
    # 2 across all of them, not 2 each. Worth watching once this ships:
    # /audio-to-midi is a high-traffic entry point, so someone arriving
    # for MIDI and trying the HQ tier twice has no free HQ separations
    # left that month - on a tool they never touched. If that shows up in
    # support, FREE_MONTHLY_OPS=3 costs about 1.5 cents per user per month
    # and is one env var, where per-tool allowances would mean reopening
    # ledger.py.
    "audio-to-midi-hq": {
        "enabled": False, "free_under_seconds": 0, "credits": 1,
        "paid_rate_limit": 30, "paid_rate_window": 3600,
        "free_rate_limit": 0,
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
    free_rate_limit: int = 0     # 0 = derive from FREE_MONTHLY_OPS, see below
    free_rate_window: int = 3600


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
    device_link_ttl_minutes: int
    session_ttl_days: int
    magic_links_per_hour: int
    device_links_per_hour: int

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
                                 int(cfg.get("paid_rate_limit", 30) or 30)),
            paid_rate_window=_int(f"PAYWALL_TOOL_{slug}_PAID_RATE_WINDOW",
                                  int(cfg.get("paid_rate_window", 3600) or 3600)),
            # 0 means "derive from FREE_MONTHLY_OPS" - resolved in
            # get_settings() once free_monthly_ops is known. Set the env var
            # only to deliberately exceed the monthly allowance; setting it
            # BELOW is rejected at boot.
            free_rate_limit=_int(f"PAYWALL_TOOL_{slug}_FREE_RATE_LIMIT",
                                 int(cfg.get("free_rate_limit", 0) or 0)),
            free_rate_window=_int(f"PAYWALL_TOOL_{slug}_FREE_RATE_WINDOW",
                                  int(cfg.get("free_rate_window", 3600) or 3600)),
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
        # Deliberately MUCH shorter than the emailed link. A device link
        # is rendered as a QR code on a screen the user is looking at
        # right now - it is scanned within seconds or not at all. The
        # 30-minute window that makes sense for an email round-trip is
        # pure extra exposure here: a screenshot, a shoulder-surfer, or a
        # shared screen recording would otherwise carry a working
        # credential for half an hour.
        device_link_ttl_minutes=_int("DEVICE_LINK_TTL_MINUTES", 5),
        session_ttl_days=_int("SESSION_TTL_DAYS", 365),
        magic_links_per_hour=_int("MAGIC_LINKS_PER_HOUR", 5),
        # Higher than the email limit because there is no email to spam
        # and no enumeration surface - the caller must ALREADY hold a
        # linked account to get one at all. This bounds a compromised
        # session minting links in bulk, nothing more.
        device_links_per_hour=_int("DEVICE_LINKS_PER_HOUR", 20),

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

    # ---- Resolve derived free rate limits -------------------------------
    # A rule with free_rate_limit=0 means "match the monthly allowance".
    # Done HERE rather than in _load_tool_rules() because it needs
    # free_monthly_ops, which is a top-level setting - the dependency runs
    # the wrong way round to resolve it earlier.
    resolved_rules = {}
    for key, rule in settings.tool_rules.items():
        free_limit = rule.free_rate_limit or settings.free_monthly_ops
        resolved_rules[key] = ToolRule(
            tool=rule.tool, enabled=rule.enabled,
            free_under_seconds=rule.free_under_seconds, credits=rule.credits,
            paid_rate_limit=rule.paid_rate_limit, paid_rate_window=rule.paid_rate_window,
            free_rate_limit=free_limit, free_rate_window=rule.free_rate_window,
        )
    settings = replace(settings, tool_rules=resolved_rules)

    # ---- THE INVARIANT --------------------------------------------------
    # An hourly free limit BELOW the monthly free allowance makes part of
    # that allowance unreachable: we would be advertising 2 free runs while
    # making the second one require an hour's wait. That is not a tight
    # limit, it is a broken promise, and it is invisible until a user hits
    # it - which is exactly the class of bug worth refusing to boot over.
    #
    # Deliberately checked for EVERY rule including disabled ones. A tool
    # that is off today gets enabled by flipping one env var, and that flip
    # should not be the moment a latent contradiction goes live.
    #
    # Worth knowing for the transcription rule specifically: the host
    # config.py sets AUDIO_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS = 2/hour,
    # which already matches FREE_MONTHLY_OPS. The derived value here is
    # what the limiter actually enforces on a metered route, so the two
    # agree without anyone having to keep them in sync by hand.
    for rule in settings.tool_rules.values():
        if rule.free_rate_limit < settings.free_monthly_ops:
            raise RuntimeError(
                f"Rate limit contradiction on '{rule.tool}': free tier allows "
                f"{settings.free_monthly_ops} ops/month but only "
                f"{rule.free_rate_limit} per {rule.free_rate_window}s. The monthly "
                f"allowance would be unspendable in a single session. Either raise "
                f"PAYWALL_TOOL_{rule.tool.upper().replace('-','_').replace('/','_')}"
                f"_FREE_RATE_LIMIT to >= {settings.free_monthly_ops}, or lower "
                f"FREE_MONTHLY_OPS."
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