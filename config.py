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

# ---------- CORS ----------
# Comma-separated list of allowed origins, e.g.
# "https://audioforges.lovable.app,https://audioforges.com"
# Defaults to "*" (allow everything) if not set, so this NEVER breaks
# anything until you explicitly lock it down. Once your domain is final,
# set ALLOWED_ORIGINS in Railway to your real domain(s) only - "*" means
# literally any website can call your API from a user's browser, not just
# yours.
_allowed_origins_raw = os.environ.get("ALLOWED_ORIGINS", "*")
if _allowed_origins_raw.strip() == "*":
    ALLOWED_ORIGINS = ["*"]
else:
    ALLOWED_ORIGINS = [o.strip() for o in _allowed_origins_raw.split(",") if o.strip()]

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