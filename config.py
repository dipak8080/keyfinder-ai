"""
config.py - Central place for every constant and env-driven setting.
Nothing in here does work; it's just values other modules import.
"""
import os
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------- PATHS ----------
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

FFMPEG_PATH = "/usr/bin/ffmpeg"

# ---------- UPLOAD LIMITS ----------
MAX_UPLOAD_BYTES = 50 * 1024 * 1024  # 50 MB

# ---------- YT-DLP RETRY CONFIG ----------
YT_DLP_MAX_ATTEMPTS = 3
YT_DLP_BASE_BACKOFF_SECONDS = 1.5  # 1.5s, 3s, 6s (exponential)

YT_BOT_CHECK_MARKERS = (
    "sign in to confirm you're not a bot",
    "sign in to confirm you are not a bot",
    "requested format is not available",
)

# ---------- IP-REPUTATION FAILURES (proxy-worthy, but not the bot-check UI text) ----------
# YT_BOT_CHECK_MARKERS above only covers the webpage/API extraction step
# ("sign in to confirm..."). It does NOT cover a 403 at the actual media
# download step (googlevideo.com CDN rejecting the connecting IP after
# extraction, PO tokens, and JS-challenge-solving already succeeded) -
# that's a different failure point with different error text, but the
# SAME root cause (IP reputation) and the SAME fix (retry through the
# proxy). This list is checked separately by is_ip_block_error() in
# youtube.py, which is what actually gates the proxy retry in
# download_with_fallback(). YT_BOT_CHECK_MARKERS stays as-is and keeps
# driving the user-facing 503 message text in routes.py.
IP_BLOCK_MARKERS = YT_BOT_CHECK_MARKERS + (
    "unable to download video data",
    "http error 403",
)

# ---------- YOUTUBE DOWNLOAD DURATION CAP ----------
# Long videos (podcasts, DJ sets, movies) can take many minutes to download
# and convert - often longer than the frontend's fetch timeout, so the
# browser gives up and closes the connection (visible as a 499 in HTTP
# logs) while the backend keeps working in the background: burning proxy
# bandwidth (billed per GB) and Railway compute for a result nobody will
# ever receive. This cap rejects videos longer than the limit BEFORE
# starting the real download, with a clear message, instead of wasting
# resources on something that was never going to finish in time for the
# user. Set to None to disable the check entirely (no duration limit).
MAX_VIDEO_DURATION_SECONDS = int(os.environ.get("MAX_VIDEO_DURATION_SECONDS", "1200"))  # 20 min

# ---------- ANALYSIS TUNING ----------
# Bumped from 120s -> 180s. Key/BPM detection doesn't need the whole track,
# but 120s was occasionally landing entirely inside an ambient/percussion-only
# intro on some tracks, which starves both detectors of tonal information.
# 180s is still a hard memory cap, just a slightly safer one. Set to None to
# disable trimming and always analyze the full file (best accuracy, highest
# memory use).
ANALYSIS_MAX_SECONDS: Optional[int] = 180

# Most tracks in club/EDM/house/pop contexts land in this BPM range. Used
# only to correct octave errors (half/double-tempo mistakes) - if a detected
# BPM falls outside this window but 2x or 0.5x of it falls inside, we prefer
# the in-range candidate. This is a heuristic, not a genre classifier: it
# will not "fix" a legitimately slow ballad or a legitimately fast DnB track,
# it only nudges values that are suspiciously outside the common range AND
# have an in-range octave-multiple.
TYPICAL_BPM_MIN = 70
TYPICAL_BPM_MAX = 180

# If Essentia and the Librosa cross-check disagree on key, how much to
# discount the reported confidence by (multiplicative).
KEY_DISAGREEMENT_CONFIDENCE_PENALTY = 0.75
BPM_DISAGREEMENT_CONFIDENCE_PENALTY = 0.80

# ---------- CONCURRENCY / LOAD-SHEDDING CONFIG ----------
# Size this to roughly your CPU core count. Too high just means more
# threads fighting over the same CPU with no real throughput gain - it does
# NOT increase how much work the machine can actually do at once.
THREAD_POOL_WORKERS = int(os.environ.get("THREAD_POOL_WORKERS", "4"))

# Hard caps on how many /analyze and /download jobs run AT THE SAME TIME.
# This is the actual thing standing between you and an OOM crash when a lot
# of people hit the API at once - it's independent of THREAD_POOL_WORKERS
# above (that's about not freezing the event loop; this is about not
# loading many audio files into RAM simultaneously).
#
# Tune these to your instance's RAM. Essentia/Librosa audio buffers for a
# ~3 min trimmed track are roughly tens of MB each, so on a small Railway
# instance (512MB-1GB), keep these low (2-3) rather than generous.
MAX_CONCURRENT_ANALYSIS = int(os.environ.get("MAX_CONCURRENT_ANALYSIS", "3"))
MAX_CONCURRENT_DOWNLOADS = int(os.environ.get("MAX_CONCURRENT_DOWNLOADS", "3"))

# How long an incoming request is willing to sit in the queue waiting for a
# free analysis/download slot before we give up and return 503 instead of
# holding the connection open forever.
QUEUE_WAIT_TIMEOUT_SECONDS = int(os.environ.get("QUEUE_WAIT_TIMEOUT_SECONDS", "30"))

# ---------- COOKIES ----------
# cookies.txt is intentionally excluded from git (it contains a real
# YouTube login session). Railway builds pull source from GitHub, not your
# local disk - so a gitignored file never makes it into the built image, no
# matter what the Dockerfile's COPY step does. Instead, the file's content
# is stored as a base64-encoded Railway env var, and reconstructed at
# startup, once, before any requests are served. See utils.ensure_cookies_file().
YT_COOKIES_PATH_DEFAULT = "/app/cookies.txt"

# ---------- COOKIE EXPIRY ALERTING ----------
# yt-dlp logs "The provided YouTube account cookies are no longer valid"
# as a WARNING, not an exception - downloads keep succeeding anyway via
# the direct/proxy fallback, so this currently passes by completely
# silently: nothing in the existing alert system (which only fires on
# request FAILURES) ever sees it. This lets you find out cookies died
# from a Discord ping instead of stumbling on it in Railway logs.
COOKIE_EXPIRY_MARKERS = (
    "cookies are no longer valid",
    "cookies have expired",
    "cookies have been rotated",
)

# yt-dlp's cookie-validity check is a HEURISTIC, not a hard fact - it can
# flag cookies as "no longer valid" for one specific video/client combo
# even when the cookies are genuinely fine (observed in practice: the
# warning fires once, then several OTHER downloads in the same minute
# authenticate successfully with the exact same cookies.txt, no issue).
# Alerting on a single occurrence produces noisy false alarms that don't
# reflect real, persistent expiry.
#
# Instead, require COOKIE_EXPIRY_ALERT_THRESHOLD occurrences within
# COOKIE_EXPIRY_ALERT_WINDOW_SECONDS before alerting - one flaky check on
# one odd video won't cross that bar, but genuinely dead/rotated cookies
# will, since EVERY subsequent authenticated request will keep hitting the
# same warning in quick succession. COOKIE_ALERT_COOLDOWN_SECONDS still
# applies after an alert fires, to avoid repeat pings once you're already
# aware.
COOKIE_EXPIRY_ALERT_THRESHOLD = int(os.environ.get("COOKIE_EXPIRY_ALERT_THRESHOLD", "3"))
COOKIE_EXPIRY_ALERT_WINDOW_SECONDS = int(os.environ.get("COOKIE_EXPIRY_ALERT_WINDOW_SECONDS", str(10 * 60)))  # 10 min
COOKIE_ALERT_COOLDOWN_SECONDS = int(os.environ.get("COOKIE_ALERT_COOLDOWN_SECONDS", str(60 * 60)))  # 1 hour

# ---------- MULTI-ACCOUNT COOKIE ROTATION ----------
# Account 1 keeps using the EXISTING YT_COOKIES_B64 / YT_COOKIES_PATH
# mechanism (reconstructed at startup by utils.ensure_cookies_file() -
# completely unchanged). Accounts 2 and 3 are OPTIONAL additional cookie
# sessions from separate logged-in YouTube browser sessions, so a single
# dead/rotated cookie session doesn't take down cookie-based access
# entirely - if account 1 gets flagged LOGIN_REQUIRED, we rotate to
# account 2, then 3, before falling back to the (cookie-less) proxy tier.
# If YT_COOKIES_B64_2/_3 aren't set in Railway, those slots simply don't
# exist - rotation degrades gracefully to however many accounts ARE
# configured (1, 2, or 3), no breakage either way. See
# youtube.get_cookie_accounts() / _materialize_extra_cookie_accounts().
COOKIE_ACCOUNT_2_B64_ENV = "YT_COOKIES_B64_2"
COOKIE_ACCOUNT_3_B64_ENV = "YT_COOKIES_B64_3"
COOKIE_ACCOUNT_2_PATH = "/app/cookies_2.txt"
COOKIE_ACCOUNT_3_PATH = "/app/cookies_3.txt"

# How long to skip a cookie account after it's flagged LOGIN_REQUIRED /
# "no longer valid" for a real download - same circuit-breaker idea as
# the proxy one, just per-account, so we don't keep re-trying a
# known-dead account on every request during the cooldown.
COOKIE_ACCOUNT_COOLDOWN_SECONDS = int(os.environ.get("COOKIE_ACCOUNT_COOLDOWN_SECONDS", str(15 * 60)))  # 15 min

# ---------- PROXY FALLBACK / CIRCUIT BREAKER ----------
# Strategy: try every download WITHOUT the proxy first (free - cookies
# alone clear most bot-checks for normal, spread-out traffic). Only retry
# through the proxy if that direct attempt specifically hit an IP-block
# error (webpage bot-check OR media-fetch 403 - see IP_BLOCK_MARKERS above)
# - unrelated errors (video unavailable, etc.) never touch the proxy, since
# it wouldn't help and would just burn paid bandwidth. See
# youtube.download_with_fallback().
#
# If the proxy itself then fails with what looks like a billing/quota
# error (out of credit), we trip a circuit breaker instead of letting
# every subsequent request rediscover that the hard way: proxy is treated
# as unavailable for this many seconds, and requests fall back to
# direct-only during that window. An immediate webhook alert is fired the
# moment this trips (separate from monitoring.py's failure-threshold
# alert) so you find out before it becomes a full outage.
PROXY_CIRCUIT_BREAKER_COOLDOWN_SECONDS = int(
    os.environ.get("PROXY_CIRCUIT_BREAKER_COOLDOWN_SECONDS", str(30 * 60))  # 30 min
)

# ---------- CORS ----------
# Comma-separated list of EXACT allowed origins, e.g.
# "https://audioforges.lovable.app,https://audioforges.com"
# Defaults to "*" (allow everything) if not set, so this NEVER breaks
# anything until you explicitly lock it down. Once your domain is final,
# set ALLOWED_ORIGINS in Railway to your real production domain(s) only -
# "*" means literally any website can call your API from a user's browser,
# not just yours.
_allowed_origins_raw = os.environ.get("ALLOWED_ORIGINS", "*")
if _allowed_origins_raw.strip() == "*":
    ALLOWED_ORIGINS = ["*"]
else:
    ALLOWED_ORIGINS = [o.strip() for o in _allowed_origins_raw.split(",") if o.strip()]

# CORSMiddleware's allow_origins only does EXACT string matches - it can't
# do wildcard subdomains like "*.lovable.app". Lovable's in-editor preview
# sandbox serves your app from a different, randomly-generated subdomain
# each time (e.g. https://id-preview--xxxx.lovable.app,
# https://xxxx.lovableproject.com) - none of which match your fixed
# production domain in ALLOWED_ORIGINS above, so testing inside the editor
# gets CORS-blocked even though your real published site works fine. This
# regex additionally allows any Lovable preview/editor subdomain, while
# ALLOWED_ORIGINS above still locks down real production traffic to your
# actual domain(s) only.
# Set ALLOW_LOVABLE_PREVIEW_ORIGINS=false in Railway to disable this once
# you no longer need editor-preview access (e.g. fully launched product).
ALLOW_LOVABLE_PREVIEW_ORIGINS = os.environ.get("ALLOW_LOVABLE_PREVIEW_ORIGINS", "true").lower() == "true"
LOVABLE_PREVIEW_ORIGIN_REGEX = r"https://.*\.lovable\.app|https://.*\.lovableproject\.com"

# ---------- RATE LIMITING ----------
# Simple per-IP request cap on the heavy endpoints (/download, /analyze).
# In-memory only - resets on restart, and is PER INSTANCE (not shared
# across multiple Railway replicas if you ever scale horizontally). Good
# enough to stop a single abusive IP/script from hammering the API and
# running up your bill; not a replacement for real infra-level protection
# at large scale.
RATE_LIMIT_ENABLED = os.environ.get("RATE_LIMIT_ENABLED", "true").lower() == "true"
RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("RATE_LIMIT_MAX_REQUESTS", "10"))
RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("RATE_LIMIT_WINDOW_SECONDS", "60"))

# ---------- MONITORING / ALERTING ----------
# Optional webhook URL (Discord/Slack-compatible) that gets a message
# posted to it when failures spike. Leave unset to disable external
# alerting - nothing breaks, failures still get logged as CRITICAL in
# Railway's Deploy Logs, you just won't get pinged outside of checking
# logs yourself. Discord webhooks are free: Server Settings -> Integrations
# -> Webhooks -> New Webhook -> Copy Webhook URL.
ALERT_WEBHOOK_URL = os.environ.get("ALERT_WEBHOOK_URL", "")

# If this many (or more) requests to the SAME endpoint fail within
# FAILURE_ALERT_WINDOW_SECONDS, send one alert. ALERT_COOLDOWN_SECONDS then
# blocks repeat alerts for that endpoint so a sustained outage doesn't spam
# you every few seconds.
FAILURE_ALERT_THRESHOLD = int(os.environ.get("FAILURE_ALERT_THRESHOLD", "5"))
FAILURE_ALERT_WINDOW_SECONDS = int(os.environ.get("FAILURE_ALERT_WINDOW_SECONDS", "300"))
ALERT_COOLDOWN_SECONDS = int(os.environ.get("ALERT_COOLDOWN_SECONDS", "900"))

# Secret required (as ?key=... query param) to view /admin/status.
# CHANGE THIS in Railway to a long random string - if left as the
# default below, the endpoint still works but with a guessable key.
ADMIN_STATUS_KEY = os.environ.get("ADMIN_STATUS_KEY", "change-me")