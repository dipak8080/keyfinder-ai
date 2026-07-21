# ---------- VOCAL/INSTRUMENTAL SEPARATION (Demucs) ----------
# Uses Demucs' built-in --two-stems=vocals mode, which outputs exactly
# TWO files (vocals.wav + no_vocals.wav) instead of the full 4-stem split
# (vocals/drums/bass/other). This is the right choice here specifically
# because it's meaningfully faster (~2x less compute than full 4-stem
# separation) AND matches exactly what the product needs - vocals vs.
# instrumental, nothing else.



import os


SEPARATION_MODEL = os.environ.get("SEPARATION_MODEL", "htdemucs")

# Demucs on CPU is genuinely slow (roughly 1-3 min per song on a 4-vCPU
# box) and memory-hungry once the model is loaded. Running more than one
# job at a time risks starving RAM shared with /download and /analyze
# traffic on the same 6GB box - keep this at 1 unless you've specifically
# verified the VPS handles more under real concurrent load.
MAX_CONCURRENT_SEPARATIONS = int(os.environ.get("MAX_CONCURRENT_SEPARATIONS", "1"))

# Hard ceiling on how long a single Demucs subprocess is allowed to run
# before we kill it and report a failure - protects against a pathological
# input (corrupt file, extremely long track) hanging a worker thread and
# the concurrency slot it holds forever.
DEMUCS_TIMEOUT_SECONDS = int(os.environ.get("DEMUCS_TIMEOUT_SECONDS", "900"))  # 15 min

# Uploaded file duration cap for separation specifically - independent
# from ANALYSIS_MAX_SECONDS (key/BPM only needs 180s of audio; separation
# has to process the WHOLE track to produce a usable download) and from
# MAX_VIDEO_DURATION_SECONDS (that's for YouTube downloads, not uploads).
# Longer input = proportionally longer Demucs runtime, so this keeps
# processing time bounded on a CPU-only box.
MAX_SEPARATION_DURATION_SECONDS = int(os.environ.get("MAX_SEPARATION_DURATION_SECONDS", "600"))  # 10 min

# Where finished stems + job status live on local disk (NOT R2 - see
# separation.py module docstring for why: separation results are
# short-lived and rate-limited to begin with, so a persistent shared
# cache buys little, and local disk is simpler/faster for this access
# pattern). Auto-deleted after SEPARATION_JOB_TTL_SECONDS by the cleanup
# sweep in jobs.py, same way UPLOAD_DIR's temp files are already cleaned
# up elsewhere in this app.
SEPARATION_DIR = os.environ.get("SEPARATION_DIR", "separated")
os.makedirs(SEPARATION_DIR, exist_ok=True)

# How long a completed job's files + status stay available for
# preview/download before being swept. 2 hours is generous headroom for
# someone to preview both stems and download the one(s) they want,
# without stems piling up indefinitely on a 30GB disk.
SEPARATION_JOB_TTL_SECONDS = int(os.environ.get("SEPARATION_JOB_TTL_SECONDS", str(2 * 60 * 60)))

# Per-route override of the global rate limiter (see rate_limit.py) -
# deliberately much stricter than RATE_LIMIT_MAX_REQUESTS/WINDOW above,
# since separation is by far the most CPU/RAM-expensive endpoint in this
# app and MAX_CONCURRENT_SEPARATIONS=1 means a burst of requests would
# otherwise just queue up and time out anyway.
SEPARATION_RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("SEPARATION_RATE_LIMIT_MAX_REQUESTS", "1"))
SEPARATION_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("SEPARATION_RATE_LIMIT_WINDOW_SECONDS", str(60 * 60)))  # 1 hour