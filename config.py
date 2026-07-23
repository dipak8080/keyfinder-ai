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
MAX_VIDEO_DURATION_SECONDS = int(os.environ.get("MAX_VIDEO_DURATION_SECONDS", "1800"))  # 15 min

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
SEPARATION_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("SEPARATION_RATE_LIMIT_MAX_REQUESTS", "2"))
SEPARATION_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("SEPARATION_RATE_LIMIT_WINDOW_SECONDS", "3600"))  # 1/hour

# Caps how many Demucs subprocesses can run at once across ALL users -
# separate from MAX_CONCURRENT_ANALYSIS/DOWNLOADS since Demucs is far more
# RAM-hungry per job than either of those. Keep at 1 unless you've
# confirmed your VPS has RAM to spare for more.
MAX_CONCURRENT_SEPARATIONS = int(os.environ.get("MAX_CONCURRENT_SEPARATIONS", "1"))





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

# Transcription jobs don't produce a file (output is inline text, not
# audio) - result_data lives directly in the job dict (see jobs.py) and
# just needs its own TTL, same reasoning as AUDIO_TOOL_JOB_TTL_SECONDS.
TRANSCRIPTION_JOB_TTL_SECONDS = int(os.environ.get("TRANSCRIPTION_JOB_TTL_SECONDS", str(60 * 60)))  # 1 hour

# Transcription time scales with audio length and, even with int8 on
# CPU, can be slow for long files - cap input duration more
# conservatively than the other audio tools (20 min) to keep worst-case
# wait times bounded. Raise only after confirming your VPS's actual
# throughput.
MAX_TRANSCRIPTION_DURATION_SECONDS = int(os.environ.get("MAX_TRANSCRIPTION_DURATION_SECONDS", "900"))  # 15 min

AUDIO_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("AUDIO_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS", "2"))
AUDIO_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("AUDIO_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS", "300"))  # 5 min