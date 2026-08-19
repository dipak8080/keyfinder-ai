"""
config.py - Central place for every constant and env-driven setting.
Nothing in here does work; it's just values other modules import.
"""
import os
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------- NOISE PATTERNS (automated scanner traffic) ----------
# THE single canonical list. Previously this existed as three separately
# maintained copies - log_stream.py's Python _NOISE_PATTERNS (used to
# exclude noise from the Client Errors SQL count), log_stream.py's
# embedded JS copy (the fallback HTML dashboard's own "Hide noise"
# checkbox), and page.tsx's hardcoded TS array (the real Next.js
# dashboard) - and they had already drifted: the SQL version had 17
# entries, the dashboard only had 8. That meant the "Client Errors" stat
# and the "Hide noise" checkbox were silently answering "is this noise?"
# differently, which defeats the entire point of having a shared
# definition. Fixing three files that were each right by construction
# once is how this kind of drift creeps back in; one list every
# consumer reads from is how it stays fixed.
#
# Every entry here is automated internet-wide vulnerability scanning -
# bots sweeping IP ranges for exposed control panels, PHP/Joomla/Laravel/
# Drupal/WHM exploits, leaked .env files, exposed Docker sockets,
# MCP/JSON-RPC probes, and known RCE payloads (e.g. the PHPUnit
# eval-stdin exploit). This traffic hits every public server on the
# internet, this one included, and none of it reflects a real visitor or
# a real problem with this app.
#
# Consumers, all reading this exact tuple:
#   - log_stream.py's _noise_exclusion_sql() -> excludes noise from the
#     Client Errors count
#   - log_stream.py's logs_dashboard() -> injects it into the fallback
#     HTML dashboard's embedded JS (no separate copy there any more)
#   - routes.py's admin_endpoints() -> serves it to the Next.js
#     dashboard as "noise_patterns" in the same response that already
#     carries the tool list, so the browser never hardcodes its own copy
NOISE_PATH_MARKERS = (
    "/robots.txt", "/favicon.ico", "/.env", "/wp-", "/.git",
    "/SDK/", "/phpmyadmin", "/.well-known", "/xmlrpc.php",
    "/mcp", "/jsonrpc", "/sse", "/containers/json",
    "eval-stdin.php", "/_ignition/", "/actuator/",
    "/+CSCOE+/", "/+webvpn+/", "phpunit",
    # Joomla/WHM scanner probes.
    "/administrator", "/language/en-", "/media/system/",
    "validate-sso",
    # Verified against a real day of traffic (2026-08-08): the earlier
    # "whm-login" guess never matched anything, because the real path
    # uses underscores with no hyphen at all -
    # "/___proxy_subdomain_whm/login/". Fixed to match what's actually
    # being sent instead of a plausible-looking guess.
    "proxy_subdomain_whm",
    # Broad, high-value markers added after that same traffic sample
    # showed ~300 distinct scanner paths in a single day - far more than
    # is worth enumerating individually here, and a hand-maintained list
    # can never keep pace with new campaigns anyway. Each of these
    # collapses a whole FAMILY of variants into one match: "credentials"
    # alone covers aws/gcp/firebase/service-account probes in every path
    # shape scanners send them in, "/@fs/" covers every Vite dev-server
    # path-traversal attempt regardless of what file it's grabbing for.
    # This list only feeds the Client Errors SQL exclusion now, not the
    # endpoint picker - see toolFamily()'s "known families only, else
    # bucket into Other" logic in page.tsx, which no longer depends on
    # this list being complete for the UI to stay clean.
    "/@fs/", "credentials", "service-account", "serviceaccount",
    "/.aws/", "/.ssh/", "id_rsa", "id_ed25519", "id_ecdsa", "id_dsa",
    "terraform.tf", "wp-login", "wp-config", "wp-json",
    "firebase", "gcp-", "google-service",
)

# ---------- PATHS ----------
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

FFMPEG_PATH = "/usr/bin/ffmpeg"

# ---------- UPLOAD LIMITS ----------
MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_BYTES", str(80 * 1024 * 1024)))  # 80 MB

# ---------- YT-DLP RETRY CONFIG ----------
YT_DLP_MAX_ATTEMPTS = 3
YT_DLP_BASE_BACKOFF_SECONDS = 1.5  # 1.5s, 3s, 6s (exponential)

# ADDED 2026-08-12: "requested format is not available" used to live in
# this tuple (and therefore in IP_BLOCK_MARKERS, since that tuple is
# built from this one). Confirmed in production the same day this was
# split out: the SAME cookie account produced the IDENTICAL
# "Requested format is not available" error on the direct attempt AND
# through the proxy, seconds apart (see FORMAT_UNAVAILABLE_MARKERS
# below). A different exit IP producing an identical failure is proof
# this is not an IP-reputation/bot-check problem - it was misclassified,
# which meant every occurrence (100+ in one evening) paid for a proxy
# round-trip that could never have fixed it, then tripped the proxy
# bot-check breaker as a side effect, alerting on a non-incident.
YT_BOT_CHECK_MARKERS = (
    "sign in to confirm you're not a bot",
    "sign in to confirm you are not a bot",
)

# ---------- FORMAT-UNAVAILABLE (client/cookie mismatch, NOT IP-reputation) ----------
# yt-dlp successfully reached YouTube and got a real response, but none
# of the formats in the manifest matched the player_client list that was
# active for THIS attempt (most commonly: cookies were attached, which
# drops android/android_vr from the client list per _apply_player_clients
# in youtube.py, leaving only web-family clients - and some videos simply
# don't expose a usable audio format to those clients).
#
# Deliberately its OWN marker tuple, separate from IP_BLOCK_MARKERS /
# YT_BOT_CHECK_MARKERS - see is_format_unavailable_error() in youtube.py
# for how it's used: fail-fast on the same client/cookie combo, allow
# account rotation (a different account may carry different client
# eligibility), but SKIP the proxy tier entirely, since a different exit
# IP has no effect on which formats a manifest contains.
FORMAT_UNAVAILABLE_MARKERS = (
    "requested format is not available",
)

# ---------- IP-REPUTATION FAILURES (proxy-worthy, but not the bot-check UI text) ----------
IP_BLOCK_MARKERS = YT_BOT_CHECK_MARKERS + (
    "unable to download video data",
    "http error 403",
)

# ---------- YOUTUBE DOWNLOAD DURATION CAP ----------
MAX_VIDEO_DURATION_SECONDS = int(os.environ.get("MAX_VIDEO_DURATION_SECONDS", "2400"))  # 40 min

# ---------- DOWNLOAD WALL-CLOCK CAP (frees a stuck semaphore slot) ----------
DOWNLOAD_WALL_CLOCK_TIMEOUT_SECONDS = int(os.environ.get("DOWNLOAD_WALL_CLOCK_TIMEOUT_SECONDS", "180"))

# ---------- DOWNLOAD RATE LIMIT ----------
# Dedicated limit for /download, replacing the generic shared
# RATE_LIMIT_MAX_REQUESTS default it used to sit under. One knob here
# controls the whole endpoint (both mp3 and wav) - tighten
# DOWNLOAD_RATE_LIMIT_MAX_REQUESTS alone to cut proxy spend without
# touching every other endpoint that still shares the generic default.
DOWNLOAD_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("DOWNLOAD_RATE_LIMIT_MAX_REQUESTS", "15"))
DOWNLOAD_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("DOWNLOAD_RATE_LIMIT_WINDOW_SECONDS", "3600"))  # 1 hour



# ---------- ANALYSIS TUNING ----------
ANALYSIS_MAX_SECONDS: Optional[int] = 180

TYPICAL_BPM_MIN = 70
TYPICAL_BPM_MAX = 180

KEY_DISAGREEMENT_CONFIDENCE_PENALTY = 0.75
BPM_DISAGREEMENT_CONFIDENCE_PENALTY = 0.80

# ---------- CONCURRENCY / LOAD-SHEDDING CONFIG ----------
THREAD_POOL_WORKERS = int(os.environ.get("THREAD_POOL_WORKERS", "8"))

MAX_CONCURRENT_ANALYSIS = int(os.environ.get("MAX_CONCURRENT_ANALYSIS", "4"))
MAX_CONCURRENT_DOWNLOADS = int(os.environ.get("MAX_CONCURRENT_DOWNLOADS", "6"))

QUEUE_WAIT_TIMEOUT_SECONDS = int(os.environ.get("QUEUE_WAIT_TIMEOUT_SECONDS", "60"))

# ---------- COOKIES ----------
# Can be overridden via YT_COOKIES_PATH env var - e.g. pointed at a
# persistent volume path like /app/data/cookies.txt on a VPS, so uploaded
# cookies (via /admin/upload-cookies) survive container rebuilds/redeploys.
YT_COOKIES_PATH_DEFAULT = os.environ.get("YT_COOKIES_PATH", "/app/cookies.txt")

# ---------- COOKIE EXPIRY ALERTING ----------
COOKIE_EXPIRY_MARKERS = (
    "cookies are no longer valid",
    "cookies have expired",
    "cookies have been rotated",
)

COOKIE_EXPIRY_ALERT_THRESHOLD = int(os.environ.get("COOKIE_EXPIRY_ALERT_THRESHOLD", "3"))
COOKIE_EXPIRY_ALERT_WINDOW_SECONDS = int(os.environ.get("COOKIE_EXPIRY_ALERT_WINDOW_SECONDS", str(10 * 60)))  # 10 min
COOKIE_ALERT_COOLDOWN_SECONDS = int(os.environ.get("COOKIE_ALERT_COOLDOWN_SECONDS", str(60 * 60)))  # 1 hour

# ---------- MULTI-ACCOUNT COOKIE ROTATION ----------
# Both paths can now be overridden via env vars - e.g. pointed at a
# persistent volume path like /app/data/cookies_2.txt on a VPS, so cookies
# uploaded via /admin/upload-cookies survive container rebuilds/redeploys
# instead of living only inside the ephemeral container filesystem.
COOKIE_ACCOUNT_2_B64_ENV = "YT_COOKIES_B64_2"
COOKIE_ACCOUNT_3_B64_ENV = "YT_COOKIES_B64_3"
COOKIE_ACCOUNT_2_PATH = os.environ.get("COOKIE_ACCOUNT_2_PATH", "/app/cookies_2.txt")
COOKIE_ACCOUNT_3_PATH = os.environ.get("COOKIE_ACCOUNT_3_PATH", "/app/cookies_3.txt")

COOKIE_ACCOUNT_COOLDOWN_SECONDS = int(os.environ.get("COOKIE_ACCOUNT_COOLDOWN_SECONDS", str(15 * 60)))  # 15 min

# ---------- PROXY FALLBACK / CIRCUIT BREAKER ----------
PROXY_CIRCUIT_BREAKER_COOLDOWN_SECONDS = int(
    os.environ.get("PROXY_CIRCUIT_BREAKER_COOLDOWN_SECONDS", str(30 * 60))  # 30 min
)

# ---------- DIRECT-PATH DEGRADATION BREAKER (CDN dead-edge protection) ----------
# Mirror image of the proxy circuit breaker above: that one trips when the
# PROXY is unusable and falls back to direct. This one trips when the
# DIRECT path is unusable and falls forward to the proxy.
#
# Confirmed failure, verified from the host with curl (not inferred):
#   $ curl -v --connect-timeout 10 https://rr8---sn-ojq4f5-51.googlevideo.com
#     Trying [2607:f8b0:4000:6a::8]:443...  -> timeout
#     Trying 172.217.153.232:443...         -> timeout
# YouTube keeps assigning this VPS edges in the sn-ojq4f5-51 cluster that
# are unreachable from it on BOTH address families. Retrying is pointless
# (different rrN front-ends resolve into the same dead cluster). A
# different exit IP is the only thing that reaches a live edge - the proxy
# provider's usage log confirms it: 190/190 googlevideo fetches through
# the proxy succeeded over a 7-day window.
#
# So instead of paying the doomed ~10s socket_timeout on every request
# during an episode, THRESHOLD timeouts inside WINDOW seconds mark direct
# degraded for COOLDOWN seconds and downloads skip straight to proxy.
# Deliberately NOT "always use proxy": direct is free and works most of
# the time, so proxy is engaged only while direct is provably broken.
CDN_DEGRADED_THRESHOLD = int(os.environ.get("CDN_DEGRADED_THRESHOLD", "3"))
CDN_DEGRADED_WINDOW_SECONDS = int(os.environ.get("CDN_DEGRADED_WINDOW_SECONDS", "300"))    # 5 min
CDN_DEGRADED_COOLDOWN_SECONDS = int(os.environ.get("CDN_DEGRADED_COOLDOWN_SECONDS", "600"))  # 10 min

# ---------- PROXY BOT-CHECK BREAKER (cost control) ----------
# Third breaker, and the one that protects the bill rather than latency.
#
# The proxy exists to fix IP reputation. When the proxy ITSELF starts
# returning "Sign in to confirm you're not a bot", that has stopped being
# true: the exit IP currently in rotation is challenged, and every
# further escalation is a paid request with a known outcome. Observed
# 2026-08-08: direct hit a dead CDN edge, escalated to proxy, proxy
# bot-checked in 3 seconds - repeatedly, across different videos.
#
# Deliberately separate from the proxy QUOTA breaker
# (PROXY_CIRCUIT_BREAKER_COOLDOWN_SECONDS above): that one means "the
# proxy account is out of money", this one means "the proxy works fine
# but YouTube is challenging its current exits". Different causes,
# different recovery, so a quota trip shouldn't be masked by bot-check
# noise or vice versa.
#
# Threshold is higher than the CDN breaker's because a single bot-check
# is genuinely common and self-resolving (rotating residential exits
# mean the next request may land on a clean IP). It's a sustained run
# that indicates the whole exit pool is currently challenged.
PROXY_BOTCHECK_THRESHOLD = int(os.environ.get("PROXY_BOTCHECK_THRESHOLD", "5"))
PROXY_BOTCHECK_WINDOW_SECONDS = int(os.environ.get("PROXY_BOTCHECK_WINDOW_SECONDS", "600"))    # 10 min
PROXY_BOTCHECK_COOLDOWN_SECONDS = int(os.environ.get("PROXY_BOTCHECK_COOLDOWN_SECONDS", "900"))  # 15 min

# ---------- CORS ----------
_allowed_origins_raw = os.environ.get(
    "ALLOWED_ORIGINS", "https://www.audioforges.com,https://audioforges.com"
)
if _allowed_origins_raw.strip() == "*":
    ALLOWED_ORIGINS = ["*"]
else:
    ALLOWED_ORIGINS = [o.strip() for o in _allowed_origins_raw.split(",") if o.strip()]

ALLOW_LOVABLE_PREVIEW_ORIGINS = os.environ.get("ALLOW_LOVABLE_PREVIEW_ORIGINS", "false").lower() == "true"

# ---------- RATE LIMITING ----------
RATE_LIMIT_ENABLED = os.environ.get("RATE_LIMIT_ENABLED", "true").lower() == "true"
RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("RATE_LIMIT_MAX_REQUESTS", "20"))
RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("RATE_LIMIT_WINDOW_SECONDS", "60"))

# ---------- MONITORING / ALERTING ----------
ALERT_WEBHOOK_URL = os.environ.get("ALERT_WEBHOOK_URL", "")

FAILURE_ALERT_THRESHOLD = int(os.environ.get("FAILURE_ALERT_THRESHOLD", "5"))
FAILURE_ALERT_WINDOW_SECONDS = int(os.environ.get("FAILURE_ALERT_WINDOW_SECONDS", "300"))
ALERT_COOLDOWN_SECONDS = int(os.environ.get("ALERT_COOLDOWN_SECONDS", "900"))

ADMIN_STATUS_KEY = os.environ.get("ADMIN_STATUS_KEY", "change-me")

# ---------- ADMIN ROUTE PROTECTION ----------
# Deliberately much stricter than the public tool rate limits above.
# Only the site owner should ever call /admin/* - genuine admin traffic
# is naturally low-frequency (checking status, clearing cache, uploading
# cookies occasionally), so any real volume against these routes is
# almost certainly automated abuse, not a real person being throttled
# mid-workflow. A Strix pentest run demonstrated exactly this gap: an
# automated agent fired thousands of key guesses against
# /admin/upload-cookies with nothing slowing it down, and the volume
# alone made a (local dev) target unresponsive.
#
# Two independent knobs, see admin_auth.py for the full reasoning:
#   RATE_LIMIT   - caps total requests/IP to any admin route, regardless
#                  of whether the key is right or wrong.
#   LOCKOUT       - caps WRONG-KEY attempts specifically. This is what
#                  actually stops a brute-force from ever completing,
#                  not just slows it down - a patient attacker staying
#                  under the rate limit would otherwise still
#                  eventually work through a wordlist.
ADMIN_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("ADMIN_RATE_LIMIT_MAX_REQUESTS", "300"))
ADMIN_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("ADMIN_RATE_LIMIT_WINDOW_SECONDS", "60"))

ADMIN_LOCKOUT_THRESHOLD = int(os.environ.get("ADMIN_LOCKOUT_THRESHOLD", "5"))
ADMIN_LOCKOUT_WINDOW_SECONDS = int(os.environ.get("ADMIN_LOCKOUT_WINDOW_SECONDS", str(5 * 60)))    # 5 min
ADMIN_LOCKOUT_DURATION_SECONDS = int(os.environ.get("ADMIN_LOCKOUT_DURATION_SECONDS", str(15 * 60)))  # 15 min

# ---------- CACHING ----------
CACHE_MAX_AGE_SECONDS = int(os.environ.get("CACHE_MAX_AGE_SECONDS", str(30 * 24 * 60 * 60)))  # 30 days

# ---------- SEPARATION (Demucs vocal remover + full stem splitter) ----------
# Local VPS disk, not R2 - stems are large (up to ~4x original file size
# across vocals+instrumental, and roughly double that again for a full
# 4-stem split), rarely re-requested (unlike the download cache), and
# only need to live for a couple hours (preview + download window), so a
# TTL-cleaned local directory is simpler and cheaper than routing through
# R2 for something this ephemeral.
SEPARATION_DIR = "separated"
os.makedirs(SEPARATION_DIR, exist_ok=True)

# Every Demucs model this app is allowed to invoke. Nothing outside this
# tuple ever reaches the `demucs -n <model>` CLI arg. Two reasons this is
# a whitelist rather than a free-form string:
#   1. A model name is passed straight through to a subprocess arg list -
#      a value beginning with "-" would be parsed by Demucs as a FLAG,
#      not a model name. (No shell injection risk since we never use
#      shell=True, but arg-position confusion is still real.)
#   2. A typo'd-but-harmless name means Demucs tries to fetch weights
#      that don't exist, and the job fails minutes later with an opaque
#      error instead of being rejected instantly at config load.
# NOTE: only models whose weights are already on disk (see TORCH_HOME on
# the persistent volume) should be listed here. A model listed here but
# NOT cached will make its first request spend several minutes
# downloading ~1GB of weights and almost certainly blow the timeout.
ALLOWED_SEPARATION_MODELS = ("htdemucs", "htdemucs_ft", "htdemucs_6s")

# Which stems each model actually produces, in the order Demucs names
# them. Used by separation.py to know which output files to expect from a
# full (non-two-stems) run, and by the /stems routes to validate a
# requested stem name. Keeping this as data rather than hardcoding
# ("vocals", "drums", "bass", "other") in separation.py means wiring up
# htdemucs_6s later needs no code change at all.
MODEL_STEM_NAMES = {
    "htdemucs": ("vocals", "drums", "bass", "other"),
    "htdemucs_ft": ("vocals", "drums", "bass", "other"),
    "htdemucs_6s": ("vocals", "drums", "bass", "other", "guitar", "piano"),
}

# ----- STANDARD (fast) separation path -----
# Demucs model name - htdemucs is the standard pretrained model, good
# quality/speed balance.
#
# Worth knowing: --two-stems=vocals does NOT make the vocal remover
# cheaper than a full stem split. Demucs separates all sources
# internally either way and simply sums the non-vocal ones for us. That's
# why /stems costs the same CPU as /separate and shares every tunable
# below.
SEPARATION_MODEL = os.environ.get("SEPARATION_MODEL", "htdemucs")

# Demucs' own default overlap between the chunks it splits a long track
# into before processing. Made explicit here (rather than relying on the
# CLI default) so both paths pass the flag and the standard-vs-HQ
# difference is visible in one place.
SEPARATION_OVERLAP = float(os.environ.get("SEPARATION_OVERLAP", "0.25"))

# Hard ceiling on how long the Demucs subprocess is allowed to run before
# it's killed - protects against a hung/stuck process eating a worker
# slot forever. 600s = 10 min, generous for CPU separation of a normal
# song length.
DEMUCS_TIMEOUT_SECONDS = int(os.environ.get("DEMUCS_TIMEOUT_SECONDS", "600"))

# Tracks longer than this are rejected before Demucs even starts (checked
# via ffprobe) - separation time scales with track length, and CPU
# separation of very long files could eat the whole DEMUCS_TIMEOUT_SECONDS
# budget on its own. 600s = 10 min track cap.
MAX_SEPARATION_DURATION_SECONDS = int(os.environ.get("MAX_SEPARATION_DURATION_SECONDS", "600"))

# ----- HIGH QUALITY (slow) separation path -----
# htdemucs_ft is a "bag of 4" - four separate model instances, each
# fine-tuned toward one stem, ensembled. It is the highest-quality model
# Demucs ships (better SDR across every stem than plain htdemucs), and it
# costs roughly 4x the CPU time because it's effectively 4 forward passes
# instead of 1.
#
# NOT htdemucs_6s: that model adds guitar/piano stems but scores WORSE on
# the four core stems (vocals/drums/bass/other) because its capacity is
# split six ways. It's a different feature, not a quality upgrade.
SEPARATION_MODEL_HQ = os.environ.get("SEPARATION_MODEL_HQ", "htdemucs_ft")

# Raising overlap from Demucs' 0.25 default reduces chunk-boundary
# artifacts on longer tracks. Cheapest quality knob available (~1.3x
# time) - far better value per CPU-second than --shifts, which is why
# --shifts is deliberately NOT wired up: at ~2x on top of htdemucs_ft's
# 4x it pushes a normal track past any tolerable wait.
SEPARATION_OVERLAP_HQ = float(os.environ.get("SEPARATION_OVERLAP_HQ", "0.5"))

# ~5x the standard path's cost (4x model + 1.3x overlap), so the timeout
# has to grow with it or every HQ job dies on a technicality.
DEMUCS_TIMEOUT_SECONDS_HQ = int(os.environ.get("DEMUCS_TIMEOUT_SECONDS_HQ", "1800"))  # 30 min

# Tighter than the standard path's cap, NOT looser - counterintuitive but
# correct: at ~5x the per-minute-of-audio cost, a 10 min track would eat
# the entire 30 min timeout budget and gamble on finishing. 6 min keeps
# worst case comfortably inside the timeout.
MAX_SEPARATION_DURATION_SECONDS_HQ = int(os.environ.get("MAX_SEPARATION_DURATION_SECONDS_HQ", "360"))  # 6 min

# Kill switch covering BOTH high-quality routes (/separate-hq and
# /stems-hq). Flip to false to stop accepting 15-20 min jobs that
# monopolise the single separation slot - submissions get a clean
# rejection pointing at the standard path instead of silently queueing
# behind each other.
#
# Env-driven, so changing it needs a redeploy or container restart. The
# public root response exposes it so the frontend can hide the HQ toggle
# rather than letting users submit into a guaranteed error.
SEPARATION_HQ_ENABLED = os.environ.get("SEPARATION_HQ_ENABLED", "true").lower() == "true"

# ----- Model whitelist enforcement -----
# Applied AFTER both model settings are read so one loop covers both. A
# bad value falls back to the known-good default rather than raising: a
# typo'd env var shouldn't take the whole API down on boot, it should
# just mean separation runs the model we know is cached.
for _var_name, _fallback in (("SEPARATION_MODEL", "htdemucs"), ("SEPARATION_MODEL_HQ", "htdemucs_ft")):
    _value = globals()[_var_name]
    if _value not in ALLOWED_SEPARATION_MODELS:
        logger.error(
            f"[SEPARATION] {_var_name}='{_value}' is not an allowed model "
            f"{ALLOWED_SEPARATION_MODELS} - falling back to '{_fallback}'"
        )
        globals()[_var_name] = _fallback

# ----- Job lifetime -----
# How long a completed/failed job (and its stem files, if complete) stays
# around before cleanup_expired_jobs() deletes it. 2 hours is generous
# for a preview-then-download flow without accumulating stale files.
# Shared by separation AND stems jobs (see jobs.py's create_job) - stems
# takes just as long to produce, so it needs just as long a window.
SEPARATION_JOB_TTL_SECONDS = int(os.environ.get("SEPARATION_JOB_TTL_SECONDS", str(2 * 60 * 60)))

# ----- Rate limits -----
# Separation is by far the most expensive endpoint (CPU + RAM heavy,
# minutes not seconds) so it gets its own, stricter rate limit than the
# shared /download and /analyze rule.
#
# NOTE on why this is per-HOUR while the cheap tools are per-minute: the
# limit here is not really about request rate, it's about how much of a
# single-slot resource one person may claim. See MAX_QUEUED_SEPARATIONS
# below - that, not this, is what actually protects the server. This
# number only decides how often one IP may join the queue.
SEPARATION_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("SEPARATION_RATE_LIMIT_MAX_REQUESTS", "3"))
SEPARATION_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("SEPARATION_RATE_LIMIT_WINDOW_SECONDS", "3600"))  # 1 hour

# Stricter still for HQ: one job can hold the single separation slot for
# 15-20 minutes, so a looser limit would let a single IP occupy most of
# an hour and starve everyone else's standard-path jobs behind it.
SEPARATION_HQ_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("SEPARATION_HQ_RATE_LIMIT_MAX_REQUESTS", "1"))
SEPARATION_HQ_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("SEPARATION_HQ_RATE_LIMIT_WINDOW_SECONDS", "3600"))  # 1 hour

# /stems costs the same CPU as /separate (same model, same run - only the
# output files differ) so it gets the same limits. Note these are
# SEPARATE per-IP buckets from /separate's, since the rate limiter keys
# on path: one IP can spend its /separate budget AND its /stems budget in
# the same hour. All of it queues behind MAX_CONCURRENT_SEPARATIONS
# regardless, so the practical cap is wait time, not throughput.
STEMS_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("STEMS_RATE_LIMIT_MAX_REQUESTS", "3"))
STEMS_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("STEMS_RATE_LIMIT_WINDOW_SECONDS", "3600"))  # 1 hour

STEMS_HQ_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("STEMS_HQ_RATE_LIMIT_MAX_REQUESTS", "1"))
STEMS_HQ_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("STEMS_HQ_RATE_LIMIT_WINDOW_SECONDS", "3600"))  # 1 hour

# Caps how many Demucs subprocesses can run at once across ALL users and
# ALL FOUR separation routes (/separate, /separate-hq, /stems,
# /stems-hq) - separate from MAX_CONCURRENT_ANALYSIS/DOWNLOADS since
# Demucs is far more RAM-hungry per job than either of those.
#
# Doubly non-negotiable now: htdemucs_ft holds this slot for 15-20 min at
# a time, and all four routes share it. Raising this doesn't add
# throughput on 4 cores, it just makes every concurrent job slower.
MAX_CONCURRENT_SEPARATIONS = int(os.environ.get("MAX_CONCURRENT_SEPARATIONS", "1"))

# How many separation jobs may be in flight (running + waiting) before
# new submissions are rejected with a 503.
#
# MAX_CONCURRENT_SEPARATIONS above caps how many RUN at once, but the
# semaphore that enforces it is acquired INSIDE the background task -
# so extra submissions were never rejected, they queued in memory with
# no limit. Each queued job holds its uploaded file on disk and its
# entry in the job table until it eventually runs, and the person
# watching the spinner has no way to know they're twelfth in line.
#
# At ~3-5 min per standard job on one slot, 3 in flight is already a
# ~15 minute wait for the last one. Past that, an immediate "the queue
# is full, try again shortly" is more honest - and far cheaper - than
# an open-ended wait that looks identical to the app being broken.
MAX_QUEUED_SEPARATIONS = int(os.environ.get("MAX_QUEUED_SEPARATIONS", "3"))


# ---------- AUDIO TOOLS (convert / trim / pitch / tempo / volume / reverse) ----------
# Shared local-disk output directory for all six audio-tool endpoints -
# same reasoning as SEPARATION_DIR: outputs are one-shot user downloads,
# not worth routing through R2, and only need to live for a short
# preview+download window.
AUDIO_TOOLS_DIR = "audio_tools_output"
os.makedirs(AUDIO_TOOLS_DIR, exist_ok=True)

FFPROBE_PATH = "/usr/bin/ffprobe"

RUBBERBAND_PATH = os.environ.get("RUBBERBAND_PATH", "/usr/bin/rubberband")

# How long a completed/failed audio-tool job (and its output file) stays
# around before cleanup sweeps it. Shorter than separation's 2h since
# these are much faster to regenerate on request than a Demucs run.
AUDIO_TOOL_JOB_TTL_SECONDS = int(os.environ.get("AUDIO_TOOL_JOB_TTL_SECONDS", str(60 * 60)))  # 1 hour

# Caps how many audio-tool subprocesses (ffmpeg/rubberband) can run at
# once across ALL users and ALL six endpoints combined - separate from
# MAX_CONCURRENT_SEPARATIONS since these are far lighter than Demucs, but
# still real CPU work that shouldn't be allowed unbounded.
MAX_CONCURRENT_AUDIO_TOOLS = int(os.environ.get("MAX_CONCURRENT_AUDIO_TOOLS", "4"))

# Hard ceiling on how long any single ffmpeg/rubberband subprocess spawned
# by the audio-tools modules is allowed to run before being killed -
# protects a worker slot from being eaten forever by a hung process, same
# reasoning as DEMUCS_TIMEOUT_SECONDS above but scaled down since these
# are much lighter operations than Demucs separation.
AUDIO_TOOL_SUBPROCESS_TIMEOUT_SECONDS = int(os.environ.get("AUDIO_TOOL_SUBPROCESS_TIMEOUT_SECONDS", "120"))

# Tracks longer than this are rejected before any processing starts
# (checked via ffprobe) - applies to trim/pitch/tempo/volume/reverse,
# where processing time scales with input duration. Convert is exempt
# since format conversion is comparatively cheap regardless of length,
# but still gets a size cap via MAX_UPLOAD_BYTES.
MAX_AUDIO_TOOL_DURATION_SECONDS = int(os.environ.get("MAX_AUDIO_TOOL_DURATION_SECONDS", "1200"))  # 20 min

# ---------- AUDIO TOOLS: FORMAT VALIDATION ----------
# Full any-to-any conversion matrix - every supported format can convert
# to every other supported format. Generated programmatically (not
# hand-listed like the original restricted-pairs version) so adding a
# new format to _SUPPORTED_AUDIO_FORMATS below automatically wires it
# into every existing format's target list too, with no risk of an
# entry being forgotten on one side of a pair.
_SUPPORTED_AUDIO_FORMATS = ("mp3", "wav", "flac", "m4a", "aac", "ogg", "aiff")

AUDIO_CONVERSION_MATRIX = {
    fmt: set(_SUPPORTED_AUDIO_FORMATS) - {fmt}
    for fmt in _SUPPORTED_AUDIO_FORMATS
}

# Every extension valid as an INPUT to any audio-tool endpoint (convert,
# trim, pitch, tempo, volume, reverse, noise-remove, voice-clean,
# echo-remove, silence-remove, speech-to-text) - used for upload
# validation before the file is even opened.
ALLOWED_AUDIO_INPUT_FORMATS = frozenset(AUDIO_CONVERSION_MATRIX.keys()) | {
    fmt for targets in AUDIO_CONVERSION_MATRIX.values() for fmt in targets
}

# ---------- AUDIO TOOLS: PARAMETER LIMITS ----------
# Pitch shift range, in semitones. +/-12 = one full octave either way -
# generous for musical use without inviting absurd processing on
# extreme, likely-abusive values.
PITCH_SHIFT_MIN_SEMITONES = float(os.environ.get("PITCH_SHIFT_MIN_SEMITONES", "-12"))
PITCH_SHIFT_MAX_SEMITONES = float(os.environ.get("PITCH_SHIFT_MAX_SEMITONES", "12"))

# Tempo change range, as a speed multiplier (1.0 = unchanged).
TEMPO_MIN_FACTOR = float(os.environ.get("TEMPO_MIN_FACTOR", "0.5"))
TEMPO_MAX_FACTOR = float(os.environ.get("TEMPO_MAX_FACTOR", "2.0"))

# Volume gain range, in dB. Negative = quieter, positive = louder.
VOLUME_GAIN_MIN_DB = float(os.environ.get("VOLUME_GAIN_MIN_DB", "-30"))
VOLUME_GAIN_MAX_DB = float(os.environ.get("VOLUME_GAIN_MAX_DB", "30"))

# ---------- AUDIO TOOLS: RATE LIMITS ----------
# Each tool gets its own limit, scaled to CPU cost - cheap tools
# (convert/trim/volume/reverse: mostly stream-copy or a single fast
# ffmpeg filter) get a generous shared-style limit; pitch/tempo
# (rubberband, slower and more CPU-bound) get a stricter one, same
# reasoning as /separate's stricter limit vs /download and /analyze.
AUDIO_CONVERT_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("AUDIO_CONVERT_RATE_LIMIT_MAX_REQUESTS", "5"))
AUDIO_CONVERT_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("AUDIO_CONVERT_RATE_LIMIT_WINDOW_SECONDS", "60"))

AUDIO_TRIM_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("AUDIO_TRIM_RATE_LIMIT_MAX_REQUESTS", "5"))
AUDIO_TRIM_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("AUDIO_TRIM_RATE_LIMIT_WINDOW_SECONDS", "60"))

AUDIO_VOLUME_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("AUDIO_VOLUME_RATE_LIMIT_MAX_REQUESTS", "5"))
AUDIO_VOLUME_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("AUDIO_VOLUME_RATE_LIMIT_WINDOW_SECONDS", "60"))

AUDIO_REVERSE_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("AUDIO_REVERSE_RATE_LIMIT_MAX_REQUESTS", "5"))
AUDIO_REVERSE_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("AUDIO_REVERSE_RATE_LIMIT_WINDOW_SECONDS", "60"))

AUDIO_PITCH_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("AUDIO_PITCH_RATE_LIMIT_MAX_REQUESTS", "3"))
AUDIO_PITCH_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("AUDIO_PITCH_RATE_LIMIT_WINDOW_SECONDS", "300"))  # 5 min

AUDIO_TEMPO_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("AUDIO_TEMPO_RATE_LIMIT_MAX_REQUESTS", "3"))
AUDIO_TEMPO_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("AUDIO_TEMPO_RATE_LIMIT_WINDOW_SECONDS", "300"))  # 5 min

# Noise reduction strength range, passed to ffmpeg's afftdn filter's nr
# param (higher = more aggressive noise reduction, at the cost of more
# audible artifacts on the wanted signal at high values).
NOISE_REDUCTION_MIN_STRENGTH = float(os.environ.get("NOISE_REDUCTION_MIN_STRENGTH", "0.01"))
NOISE_REDUCTION_MAX_STRENGTH = float(os.environ.get("NOISE_REDUCTION_MAX_STRENGTH", "97"))

AUDIO_NOISE_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("AUDIO_NOISE_RATE_LIMIT_MAX_REQUESTS", "5"))
AUDIO_NOISE_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("AUDIO_NOISE_RATE_LIMIT_WINDOW_SECONDS", "60"))


AUDIO_VOICE_CLEAN_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("AUDIO_VOICE_CLEAN_RATE_LIMIT_MAX_REQUESTS", "5"))
AUDIO_VOICE_CLEAN_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("AUDIO_VOICE_CLEAN_RATE_LIMIT_WINDOW_SECONDS", "60"))


AUDIO_ECHO_REMOVE_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("AUDIO_ECHO_REMOVE_RATE_LIMIT_MAX_REQUESTS", "5"))
AUDIO_ECHO_REMOVE_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("AUDIO_ECHO_REMOVE_RATE_LIMIT_WINDOW_SECONDS", "60"))



SILENCE_THRESHOLD_MIN_DB = float(os.environ.get("SILENCE_THRESHOLD_MIN_DB", "-90"))
SILENCE_THRESHOLD_MAX_DB = float(os.environ.get("SILENCE_THRESHOLD_MAX_DB", "-10"))

SILENCE_MIN_DURATION_SECONDS = float(os.environ.get("SILENCE_MIN_DURATION_MIN_SECONDS", "0.1"))
SILENCE_MAX_DURATION_SECONDS = float(os.environ.get("SILENCE_MAX_DURATION_MAX_SECONDS", "10"))

AUDIO_SILENCE_REMOVE_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("AUDIO_SILENCE_REMOVE_RATE_LIMIT_MAX_REQUESTS", "5"))
AUDIO_SILENCE_REMOVE_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("AUDIO_SILENCE_REMOVE_RATE_LIMIT_WINDOW_SECONDS", "60"))



# ---------- SPEECH TO TEXT (faster-whisper) ----------
# Unlike every other audio-tool module, this one loads a model into
# the app's own process memory ONCE at startup (see speech_to_text.py)
# rather than spawning a stateless subprocess per request - see the
# module docstring there for why this fundamentally changes the
# resource profile of this one endpoint.
WHISPER_MODEL_SIZE = os.environ.get("WHISPER_MODEL_SIZE", "small")

# int8 quantization: ~3-4x faster CPU inference and lower RAM than
# float32, with only marginal accuracy loss - the standard trade for
# CPU-only Whisper deployments. Override to "float32" if quality on a
# specific file matters more than speed for a one-off, or
# "int8_float16"/"float16" if this ever moves to a GPU instance.
WHISPER_COMPUTE_TYPE = os.environ.get("WHISPER_COMPUTE_TYPE", "int8")

WHISPER_DEVICE = os.environ.get("WHISPER_DEVICE", "cpu")

# Deliberately NOT auto-detected via CPU count like THREAD_POOL_WORKERS -
# this is capped at 1 by default and should almost certainly stay there
# on a single CPU VPS. Unlike ffmpeg-based tools (stateless, cheap,
# scale fine with a small pool), Whisper inference is a heavy, sustained
# CPU+RAM operation - running two transcriptions at once on one CPU
# doesn't parallelize meaningfully, it just makes both slower. If usage
# grows enough to need more than 1, the right fix is a GPU instance or
# a hosted transcription API, not raising this number.
MAX_CONCURRENT_TRANSCRIPTIONS = int(os.environ.get("MAX_CONCURRENT_TRANSCRIPTIONS", "1"))

# Bounded queue for BOTH transcription endpoints (/speech-to-text and
# /youtube/transcribe), mirroring MAX_QUEUED_SEPARATIONS.
#
# MAX_CONCURRENT_TRANSCRIPTIONS caps how many run at once, but that
# semaphore is acquired INSIDE the background task - so before this
# existed, submissions were never refused, they queued in memory with no
# ceiling. Each waiting job holds an uploaded file on disk and a job-table
# row, and the person watching the spinner has no way to know they are
# tenth in line.
#
# Deliberately tighter than MAX_QUEUED_SEPARATIONS: CPU transcription runs
# at roughly 1x realtime, so four queued 20-minute jobs is already over an
# hour of invisible waiting for whoever is last. The rate limiter does NOT
# cover this - it is per-IP, so N different visitors each submitting their
# allowance still stacks without bound.
MAX_QUEUED_TRANSCRIPTIONS = int(os.environ.get("MAX_QUEUED_TRANSCRIPTIONS", "4"))

# ---------- TRANSCRIPTION OPTIONS ----------
# Whisper's `task` argument. "translate" makes the model emit English
# regardless of the spoken language - it is a free parameter on the
# same forward pass, NOT a second model or a second pass, so it costs
# nothing extra beyond normal inference.
ALLOWED_TRANSCRIPTION_TASKS = ("transcribe", "translate")

# Speed tiers are implemented via beam_size, deliberately NOT via
# loading multiple model sizes. speech_to_text.py holds its model as a
# module-level singleton, so a second resident model would cost real
# RAM for the entire process lifetime on a 6GB box that also runs
# torch/demucs/essentia. beam_size is a per-CALL argument: same one
# resident model, different quality/latency trade per request.
TRANSCRIPTION_MODE_BEAM_SIZES = {
    "fast": 1,
    "balanced": 5,
}
DEFAULT_TRANSCRIPTION_MODE = os.environ.get("DEFAULT_TRANSCRIPTION_MODE", "balanced")

# Voice-activity detection: drops silent regions before they reach the
# model. On real-world speech (interviews, lectures, voice memos) this
# is a meaningful speedup for free, and it also reduces Whisper's
# tendency to hallucinate text during long silences. Kept as a flag
# because it can clip very quiet speech on badly-recorded input.
WHISPER_VAD_FILTER = os.environ.get("WHISPER_VAD_FILTER", "false").lower() == "true"

# Transcription jobs don't produce a file (output is inline text, not
# audio) - result_data lives directly in the job dict (see jobs.py) and
# just needs its own TTL, same reasoning as AUDIO_TOOL_JOB_TTL_SECONDS.
TRANSCRIPTION_JOB_TTL_SECONDS = int(os.environ.get("TRANSCRIPTION_JOB_TTL_SECONDS", str(60 * 60)))  # 1 hour

# Transcription time scales with audio length and, even with int8 on
# CPU, can be slow for long files - cap input duration more
# conservatively than the other audio tools (20 min) to keep worst-case
# wait times bounded. Raise only after confirming your VPS's actual
# throughput.
MAX_TRANSCRIPTION_DURATION_SECONDS = int(os.environ.get("MAX_TRANSCRIPTION_DURATION_SECONDS", "1200"))  # 20 min

AUDIO_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("AUDIO_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS", "2"))
AUDIO_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("AUDIO_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS", "300"))  # 5 min

# ---------- VIDEO TO TEXT ----------
# Own byte cap, well above MAX_UPLOAD_BYTES' 80MB (video is ~10x audio
# for the same running time) but well below MAX_VIDEO_UPLOAD_BYTES' 200MB.
#
# 200MB would be actively misleading here: a video that large is almost
# certainly longer than MAX_TRANSCRIPTION_DURATION_SECONDS, so accepting
# the upload only to reject it on duration wastes the user's entire
# transfer. 100MB is roughly where a 20-minute video lands at ordinary
# bitrates, so the two limits agree instead of contradicting each other.
MAX_VIDEO_TRANSCRIBE_BYTES = int(os.environ.get("MAX_VIDEO_TRANSCRIBE_BYTES", str(100 * 1024 * 1024)))  # 100 MB

# Matched to the other two transcription endpoints rather than to
# /video-to-audio's looser 5-per-5-min: the binding cost here is the
# single transcription slot, not the ffmpeg extraction, so this belongs
# with its siblings.
VIDEO_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("VIDEO_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS", "2"))
VIDEO_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("VIDEO_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS", "300"))

# ---------- YOUTUBE TO TEXT ----------
# Stricter than the other /youtube/* chained tools: this one chains a
# download onto a near-realtime CPU transcription, so a single accepted
# job can occupy the one transcription slot for the length of the video.
# The limiter is what stops a queue forming that nobody can see.
YOUTUBE_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("YOUTUBE_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS", "2"))
YOUTUBE_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("YOUTUBE_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS", "300"))

# ---------- TRANSCRIPTION BACKEND ----------
# "local" = faster-whisper in this process (CPU). "gpu" = RunPod
# serverless worker. See transcription.py for the dispatch, and note the
# local model is NOT loaded at all when this is "gpu".
#
# Env-driven so reverting is one line and a restart. That matters: if the
# GPU path misbehaves at 2am, "set this back to local" is a decision you
# can make tired.
TRANSCRIPTION_BACKEND = os.environ.get("TRANSCRIPTION_BACKEND", "local").strip().lower()
if TRANSCRIPTION_BACKEND not in ("local", "gpu"):
    logger.error(
        f"[TRANSCRIPTION] TRANSCRIPTION_BACKEND='{TRANSCRIPTION_BACKEND}' is not "
        f"'local' or 'gpu' - falling back to 'local'."
    )
    TRANSCRIPTION_BACKEND = "local"

RUNPOD_WHISPER_ENDPOINT_ID = os.environ.get("RUNPOD_WHISPER_ENDPOINT_ID", "")

# Both sides of the deadline - see run_worker_job(). Generous enough for
# a cold start (15-30s) plus a large file transfer plus inference on a
# 20-minute file, and no more: an over-long timeout is billed GPU time
# for a result nobody is waiting for.
GPU_TRANSCRIBE_TIMEOUT_SECONDS = int(os.environ.get("GPU_TRANSCRIBE_TIMEOUT_SECONDS", "900"))  # 15 min



# ---------- VIDEO TO AUDIO ----------
# Input-only formats: valid as an upload to /video-to-audio and nowhere
# else. Deliberately kept OUT of ALLOWED_AUDIO_INPUT_FORMATS and the
# conversion matrix - no other endpoint should accept a video, and no
# endpoint should ever output one.
ALLOWED_VIDEO_INPUT_FORMATS = frozenset({
    "mp4", "mov", "mkv", "avi", "webm", "flv", "wmv", "m4v", "3gp", "mpeg", "mpg",
})

# Video files are an order of magnitude larger than the audio this app
# normally handles - a few minutes of phone video routinely exceeds
# MAX_UPLOAD_BYTES. This endpoint therefore gets its own, much higher
# cap.
#
# Safe to raise this only because uploads are streamed to disk in chunks
# rather than read whole into memory (see upload.py). Reading 200MB via
# `await file.read()` on a 6GB box with no swap would be reckless;
# writing it in 1MB chunks costs almost nothing.
MAX_VIDEO_UPLOAD_BYTES = int(os.environ.get("MAX_VIDEO_UPLOAD_BYTES", str(200 * 1024 * 1024)))  # 200 MB

# Separate from MAX_VIDEO_DURATION_SECONDS (which caps YouTube
# DOWNLOADS) - same units, completely different purpose. Audio-only
# extraction is cheap even for long videos, especially on the
# stream-copy path, so this can be generous.
VIDEO_EXTRACT_MAX_DURATION_SECONDS = int(os.environ.get("VIDEO_EXTRACT_MAX_DURATION_SECONDS", "3600"))  # 60 min

# Longer than AUDIO_TOOL_SUBPROCESS_TIMEOUT_SECONDS' 120s: re-encoding
# an hour of audio out of a large container takes real time, and the
# 120s figure was scaled for short audio clips.
VIDEO_TO_AUDIO_TIMEOUT_SECONDS = int(os.environ.get("VIDEO_TO_AUDIO_TIMEOUT_SECONDS", "300"))  # 5 min

# Stricter than /convert's 5-per-60s despite similar CPU cost - the
# limiting factor here is upload bandwidth and disk, not compute.
VIDEO_TO_AUDIO_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("VIDEO_TO_AUDIO_RATE_LIMIT_MAX_REQUESTS", "5"))
VIDEO_TO_AUDIO_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("VIDEO_TO_AUDIO_RATE_LIMIT_WINDOW_SECONDS", "300"))  # 5 min



# ---------- JOIN / MERGE ----------
# Cap on files per request. The filter graph string grows one clause per
# input, and the frontend's reorder list stops being usable much past
# this anyway.
JOIN_MAX_FILES = int(os.environ.get("JOIN_MAX_FILES", "10"))

# Total across ALL files in one request, not per file - ten files at
# 45MB each would otherwise sail past a per-file check and land 450MB on
# disk. Same streaming-upload reasoning as MAX_VIDEO_UPLOAD_BYTES.
#
# NOTE: this is a TOTAL, and the per-file MAX_UPLOAD_BYTES still applies
# to each individual file in the batch. Both limits are real, and the
# frontend needs to state both - a single file over MAX_UPLOAD_BYTES is
# rejected even when the combined total is well under this number.
JOIN_MAX_TOTAL_BYTES = int(os.environ.get("JOIN_MAX_TOTAL_BYTES", str(150 * 1024 * 1024)))  # 150 MB

# Also a total, for the same reason: ten four-minute files is a
# forty-minute re-encode however modest each one looks alone.
JOIN_MAX_TOTAL_DURATION_SECONDS = int(os.environ.get("JOIN_MAX_TOTAL_DURATION_SECONDS", "1800"))  # 30 min

# Every input is resampled to this before concatenation. 44100 is the
# right default for music; the value matters less than the fact that all
# inputs share it, which is what makes mismatched files join cleanly
# instead of playing at the wrong speed.
JOIN_OUTPUT_SAMPLE_RATE = int(os.environ.get("JOIN_OUTPUT_SAMPLE_RATE", "44100"))

# Longer than AUDIO_TOOL_SUBPROCESS_TIMEOUT_SECONDS' 120s, which was
# scaled for a single short clip - this re-encodes up to 30 minutes of
# audio through a multi-input filter graph.
JOIN_TIMEOUT_SECONDS = int(os.environ.get("JOIN_TIMEOUT_SECONDS", "300"))  # 5 min

JOIN_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("JOIN_RATE_LIMIT_MAX_REQUESTS", "5"))
JOIN_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("JOIN_RATE_LIMIT_WINDOW_SECONDS", "300"))  # 5 min




# ---------- LOUDNESS NORMALIZATION (LUFS) ----------
# Named presets covering what producers actually target in practice,
# resolved to a raw LUFS number before audio_loudnorm.py ever sees them.
# Streaming platforms (Spotify, YouTube, Apple Music) generally target
# around -14 LUFS; club/DJ material is mastered louder; broadcast follows
# the EBU R128 / ATSC A/85 standard of -23 LUFS.
LOUDNORM_PRESETS = {
    "streaming": -14.0,
    "club": -9.0,
    "broadcast": -23.0,
}

# Bounds for the custom_lufs override. -70 to 5 covers every legitimate
# use case (even -5 is absurdly loud) while rejecting typos like "-1400".
LOUDNORM_MIN_LUFS = float(os.environ.get("LOUDNORM_MIN_LUFS", "-70"))
LOUDNORM_MAX_LUFS = float(os.environ.get("LOUDNORM_MAX_LUFS", "5"))

# True peak ceiling and loudness range target, held fixed rather than
# exposed as user params - these are secondary to the integrated loudness
# target for what this tool is for, and exposing every loudnorm knob
# would turn a two-choice tool into a five-field form.
LOUDNORM_TRUE_PEAK = float(os.environ.get("LOUDNORM_TRUE_PEAK", "-1.5"))
LOUDNORM_LRA = float(os.environ.get("LOUDNORM_LRA", "11"))

# Two separate timeouts since the two ffmpeg passes have different
# costs: the analysis pass just measures (cheap), the apply pass
# actually re-encodes the whole file (proportional to length).
LOUDNORM_ANALYSIS_TIMEOUT_SECONDS = int(os.environ.get("LOUDNORM_ANALYSIS_TIMEOUT_SECONDS", "120"))
LOUDNORM_APPLY_TIMEOUT_SECONDS = int(os.environ.get("LOUDNORM_APPLY_TIMEOUT_SECONDS", "120"))

LOUDNORM_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("LOUDNORM_RATE_LIMIT_MAX_REQUESTS", "5"))
LOUDNORM_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("LOUDNORM_RATE_LIMIT_WINDOW_SECONDS", "60"))



# ---------- YOUTUBE CHAINED TOOLS (paste a URL, skip the manual re-upload) ----------
# These stack TWO of the app's heaviest subsystems in one request - a
# YouTube download (yt-dlp + proxy/cookie logic) followed by either
# Essentia analysis or Demucs separation. The rate limit here is
# deliberately the strictest in the app: a single request can occupy
# BOTH the download semaphore and the separation semaphore in sequence,
# so this is the one tool whose abuse potential touches almost every
# other subsystem at once.
YOUTUBE_CHAIN_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("YOUTUBE_CHAIN_RATE_LIMIT_MAX_REQUESTS", "3"))
YOUTUBE_CHAIN_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("YOUTUBE_CHAIN_RATE_LIMIT_WINDOW_SECONDS", "3600"))  # 1 hour

# TTL for /youtube/analyze jobs specifically - result is inline JSON, not
# a file, so this can be short. Mirrors TRANSCRIPTION_JOB_TTL_SECONDS'
# reasoning.
YOUTUBE_ANALYZE_JOB_TTL_SECONDS = int(os.environ.get("YOUTUBE_ANALYZE_JOB_TTL_SECONDS", str(60 * 60)))  # 1 hour




RUNPOD_API_KEY = os.environ.get("RUNPOD_API_KEY", "")
RUNPOD_DEMUCS_ENDPOINT_ID = os.environ.get("RUNPOD_DEMUCS_ENDPOINT_ID", "")

# ---------- GPU WORKER INTERNAL AUTH ----------
# Shared secret both sides check for the direct HTTP file transfer
# between this VPS and the RunPod GPU worker - audio bytes never travel
# through RunPod's own job payload (10MB limit), they move directly
# between VPS and worker instead. Must match GPU_SHARED_SECRET set on
# the RunPod endpoint's own environment variables.
GPU_WORKER_SHARED_SECRET = os.environ.get("GPU_WORKER_SHARED_SECRET", "")

# This VPS's own public HTTPS base URL - used to build the URLs the GPU
# worker fetches input from / uploads results to.
VPS_PUBLIC_BASE_URL = os.environ.get("VPS_PUBLIC_BASE_URL", "")

# High-quality YouTube chain routes (/youtube/separate-hq,
# /youtube/stems-hq). Much stricter than the standard chain limit above
# for the same reason SEPARATION_HQ_RATE_LIMIT is stricter than
# SEPARATION_RATE_LIMIT: the standard chain holds the single separation
# slot for roughly 5 minutes, HQ holds it for 15-20. At the standard
# chain's 2-per-10-min allowance, one IP could keep that slot occupied
# for most of an hour and starve every other user's job behind it.
#
# Matched to SEPARATION_HQ_RATE_LIMIT_MAX_REQUESTS (1/hour) because the
# cost profile is nearly identical - it's the same Demucs work with a
# download bolted on the front.
#
# NOTE these are SEPARATE per-IP buckets from /separate-hq's and
# /stems-hq's, since the rate limiter keys on path. One IP can spend its
# budget on each. All four still queue behind the same single
# MAX_CONCURRENT_SEPARATIONS slot and the same MAX_QUEUED_SEPARATIONS
# depth check, so the practical ceiling remains wait time, not
# throughput.
YOUTUBE_CHAIN_HQ_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("YOUTUBE_CHAIN_HQ_RATE_LIMIT_MAX_REQUESTS", "1"))
YOUTUBE_CHAIN_HQ_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("YOUTUBE_CHAIN_HQ_RATE_LIMIT_WINDOW_SECONDS", "3600"))  # 1 hour


# ---------- AUDIO EFFECTS: FADE / CHANNELS / RESAMPLE / RINGTONE ----------
FADE_MAX_SECONDS = float(os.environ.get("FADE_MAX_SECONDS", "30"))

RESAMPLE_ALLOWED_RATES = (22050, 44100, 48000, 96000)
RESAMPLE_ALLOWED_BIT_DEPTHS = (16, 24, 32)

# iPhone's own ringtone length limit - not a server-load guard, a real
# constraint of the format. A "ringtone" longer than this isn't one.
RINGTONE_MAX_DURATION_SECONDS = float(os.environ.get("RINGTONE_MAX_DURATION_SECONDS", "40"))

FADE_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("FADE_RATE_LIMIT_MAX_REQUESTS", "5"))
FADE_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("FADE_RATE_LIMIT_WINDOW_SECONDS", "60"))

CHANNELS_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("CHANNELS_RATE_LIMIT_MAX_REQUESTS", "5"))
CHANNELS_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("CHANNELS_RATE_LIMIT_WINDOW_SECONDS", "60"))

RESAMPLE_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("RESAMPLE_RATE_LIMIT_MAX_REQUESTS", "5"))
RESAMPLE_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("RESAMPLE_RATE_LIMIT_WINDOW_SECONDS", "60"))

RINGTONE_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("RINGTONE_RATE_LIMIT_MAX_REQUESTS", "5"))
RINGTONE_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("RINGTONE_RATE_LIMIT_WINDOW_SECONDS", "60"))


# ---------- SILENCE SPLIT ----------
SILENCE_SPLIT_MAX_SEGMENTS = int(os.environ.get("SILENCE_SPLIT_MAX_SEGMENTS", "50"))
SILENCE_SPLIT_MIN_SEGMENT_SECONDS = float(os.environ.get("SILENCE_SPLIT_MIN_SEGMENT_SECONDS", "1.0"))
SILENCE_SPLIT_DETECT_TIMEOUT_SECONDS = int(os.environ.get("SILENCE_SPLIT_DETECT_TIMEOUT_SECONDS", "120"))
SILENCE_SPLIT_CUT_TIMEOUT_SECONDS = int(os.environ.get("SILENCE_SPLIT_CUT_TIMEOUT_SECONDS", "60"))
SILENCE_SPLIT_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("SILENCE_SPLIT_RATE_LIMIT_MAX_REQUESTS", "3"))
SILENCE_SPLIT_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("SILENCE_SPLIT_RATE_LIMIT_WINDOW_SECONDS", "300"))


# ---------- AUDIO TO MIDI (isolated midi-worker sidecar) ----------
# Runs in its OWN container, not this process - basic-pitch requires
# tensorflow<2.15.1 which hard-pins numpy<2.0.0, incompatible with the
# numpy==2.3.5 that essentia/librosa/demucs/torch here depend on. See
# audio_to_midi.py and midi-worker/main.py for the full reasoning.
MIDI_WORKER_URL = os.environ.get("MIDI_WORKER_URL", "http://midi-worker:8001")
MIDI_WORKER_SHARED_SECRET = os.environ.get("MIDI_WORKER_SHARED_SECRET", "")

# Generous: transcription of a 10 min track on CPU can take minutes.
# NOTE this is requests' timeout, which is per-socket-operation, not
# total wall clock - a pathologically slow trickle could still exceed
# it. The main app's own job semaphore is what actually bounds
# throughput; this just stops a dead worker from hanging a slot forever.
MIDI_WORKER_TIMEOUT_SECONDS = int(os.environ.get("MIDI_WORKER_TIMEOUT_SECONDS", "300"))  # 5 min

# Enforced on THIS side (via ffprobe at submit time) before a byte is
# ever sent to the worker - same "reject early, don't waste the round
# trip" reasoning as every other tool's duration check.
MAX_MIDI_DURATION_SECONDS = int(os.environ.get("MAX_MIDI_DURATION_SECONDS", "600"))  # 10 min

# Below this, there isn't enough signal for the model to find anything
# and the result is a guaranteed empty MIDI - reject with a clear
# message instead of burning a worker round-trip on it.
MIN_MIDI_DURATION_SECONDS = float(os.environ.get("MIN_MIDI_DURATION_SECONDS", "1.0"))

# How many transcriptions may be in flight to the worker at once from
# this side. The worker enforces its OWN limit independently
# (MIDI_WORKER_CONCURRENCY) - this one exists so a queue forms here,
# where the job system can report it, rather than as unbounded blocked
# HTTP connections.
MAX_CONCURRENT_MIDI = int(os.environ.get("MAX_CONCURRENT_MIDI", "2"))

MIDI_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("MIDI_RATE_LIMIT_MAX_REQUESTS", "5"))
MIDI_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("MIDI_RATE_LIMIT_WINDOW_SECONDS", "300"))  # 5 min

# Input formats accepted by /audio-to-midi SPECIFICALLY - a superset of
# ALLOWED_AUDIO_INPUT_FORMATS, adding opus and webm.
#
# Deliberately its own set rather than adding opus/webm to
# _SUPPORTED_AUDIO_FORMATS above: that tuple generates
# AUDIO_CONVERSION_MATRIX, so adding them there would silently make
# every existing tool offer opus/webm as an OUTPUT format too - a much
# larger, untested behaviour change than this feature needs.
#
# Safe here because basic-pitch decodes via librosa.load(), which falls
# back to audioread/ffmpeg for containers soundfile can't read - and
# midi-worker's Dockerfile installs ffmpeg for exactly this reason.
MIDI_INPUT_FORMATS = frozenset(ALLOWED_AUDIO_INPUT_FORMATS) | {"opus", "webm"}





# ---------- TIKTOK (/tiktok-to-mp3) ----------
# Its OWN duration cap, not MAX_VIDEO_DURATION_SECONDS. TikTok's own
# ceiling is 10 min, so this rejects nothing TikTok allows - and
# coupling it to YouTube's 20 min would mean a future YouTube tuning
# silently changes TikTok behaviour.
MAX_TIKTOK_DURATION_SECONDS = int(os.environ.get("MAX_TIKTOK_DURATION_SECONDS", "600"))

# Source audio measured ~64 kbps AAC across sampled posts (64208 and
# 64544 bps, 2026-08-18). Encoding to 320 gives a 5x larger file with
# bit-for-bit identical audible quality - the source caps it and no
# encoder adds information back. The frontend must therefore not
# advertise a bitrate number; "high quality MP3" is honest.
TIKTOK_MP3_BITRATE = os.environ.get("TIKTOK_MP3_BITRATE", "128k")

TIKTOK_MAX_ATTEMPTS = int(os.environ.get("TIKTOK_MAX_ATTEMPTS", "3"))
TIKTOK_BASE_BACKOFF_SECONDS = float(os.environ.get("TIKTOK_BASE_BACKOFF_SECONDS", "1.5"))

# Far more generous than /download's 10/hour, deliberately. That limit
# exists because a YouTube download can cost paid proxy bandwidth.
# TikTok has no proxy tier at all (see tiktok/core.py's docstring) and
# files are ~400 KB, so the only real cost is a semaphore slot.
TIKTOK_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("TIKTOK_RATE_LIMIT_MAX_REQUESTS", "30"))
TIKTOK_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("TIKTOK_RATE_LIMIT_WINDOW_SECONDS", "3600"))