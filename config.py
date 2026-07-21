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
IP_BLOCK_MARKERS = YT_BOT_CHECK_MARKERS + (
    "unable to download video data",
    "http error 403",
)

# ---------- YOUTUBE DOWNLOAD DURATION CAP ----------
MAX_VIDEO_DURATION_SECONDS = int(os.environ.get("MAX_VIDEO_DURATION_SECONDS", "900"))  # 15 min

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
RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("RATE_LIMIT_MAX_REQUESTS", "3"))
RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("RATE_LIMIT_WINDOW_SECONDS", "60"))

# ---------- MONITORING / ALERTING ----------
ALERT_WEBHOOK_URL = os.environ.get("ALERT_WEBHOOK_URL", "")

FAILURE_ALERT_THRESHOLD = int(os.environ.get("FAILURE_ALERT_THRESHOLD", "5"))
FAILURE_ALERT_WINDOW_SECONDS = int(os.environ.get("FAILURE_ALERT_WINDOW_SECONDS", "300"))
ALERT_COOLDOWN_SECONDS = int(os.environ.get("ALERT_COOLDOWN_SECONDS", "900"))

ADMIN_STATUS_KEY = os.environ.get("ADMIN_STATUS_KEY", "change-me")

# ---------- CACHING (Cloudflare R2) ----------
CACHE_ENABLED = os.environ.get("CACHE_ENABLED", "true").lower() == "true"
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME", "")
R2_ENDPOINT_URL = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com" if R2_ACCOUNT_ID else ""

CACHE_MAX_AGE_SECONDS = int(os.environ.get("CACHE_MAX_AGE_SECONDS", str(30 * 24 * 60 * 60)))  # 30 days

# ---------- SEPARATION (Demucs vocal/instrumental remover) ----------
# Local VPS disk, not R2 - stems are large (up to ~4x original file size
# across vocals+instrumental), rarely re-requested (unlike the download
# cache), and only need to live for a couple hours (preview + download
# window), so a TTL-cleaned local directory is simpler and cheaper than
# routing through R2 for something this ephemeral.
SEPARATION_DIR = "separated"
os.makedirs(SEPARATION_DIR, exist_ok=True)

# Demucs model name - htdemucs is the standard pretrained model, good
# quality/speed balance for two-stem (vocals/instrumental) separation.
SEPARATION_MODEL = os.environ.get("SEPARATION_MODEL", "htdemucs")

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

# How long a completed/failed job (and its stem files, if complete) stays
# around before cleanup_expired_jobs() deletes it. 2 hours is generous
# for a preview-then-download flow without accumulating stale files.
SEPARATION_JOB_TTL_SECONDS = int(os.environ.get("SEPARATION_JOB_TTL_SECONDS", str(2 * 60 * 60)))

# Separation is by far the most expensive endpoint (CPU + RAM heavy,
# minutes not seconds) so it gets its own, much stricter rate limit than
# /download and /analyze's shared 3-per-60s rule.
SEPARATION_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("SEPARATION_RATE_LIMIT_MAX_REQUESTS", "1"))
SEPARATION_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("SEPARATION_RATE_LIMIT_WINDOW_SECONDS", "3600"))  # 1/hour

# Caps how many Demucs subprocesses can run at once across ALL users -
# separate from MAX_CONCURRENT_ANALYSIS/DOWNLOADS since Demucs is far more
# RAM-hungry per job than either of those. Keep at 1 unless you've
# confirmed your VPS has RAM to spare for more.
MAX_CONCURRENT_SEPARATIONS = int(os.environ.get("MAX_CONCURRENT_SEPARATIONS", "1"))