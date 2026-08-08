"""
utils.py - Shared low-level helpers used across the app:
- cookies.txt bootstrap from base64 env var
- memory cleanup / temp file cleanup
- thread pool + run_blocking() for offloading blocking calls
- safe upload path construction (byte-bounded, no user-controlled bytes)
- concurrency semaphores + acquire_slot_or_503()
- Camelot wheel / key math
"""
import os
import gc
import ctypes
import base64
import gzip
import asyncio
import functools
import contextvars
from concurrent.futures import ThreadPoolExecutor

from fastapi import HTTPException

from config import (
    logger,
    THREAD_POOL_WORKERS,
    MAX_CONCURRENT_ANALYSIS,
    MAX_CONCURRENT_DOWNLOADS,
    QUEUE_WAIT_TIMEOUT_SECONDS,
    YT_COOKIES_PATH_DEFAULT,
)

# ========== COOKIES BOOTSTRAP (base64 env var -> real file) ==========


def ensure_cookies_file():
    cookies_path = os.environ.get('YT_COOKIES_PATH', YT_COOKIES_PATH_DEFAULT)

    # If cookies.txt already exists on disk (e.g. uploaded directly via
    # /admin/upload-cookies onto the persistent volume), don't touch it.
    # Without this check, every container restart/redeploy would silently
    # overwrite a freshly-uploaded cookie file with whatever stale
    # YT_COOKIES_B64 / YT_COOKIES_GZ_B64 value is still sitting in .env -
    # which defeats the entire point of being able to upload cookies
    # directly instead of re-encoding+redeploying every time they expire.
    if os.path.exists(cookies_path):
        logger.info(
            f"[COOKIES] {cookies_path} already exists on disk (likely uploaded "
            f"via /admin/upload-cookies) - skipping base64 reconstruction so it "
            f"isn't overwritten by a stale env var value."
        )
        return

    cookies_gz_b64 = os.environ.get('YT_COOKIES_GZ_B64')
    cookies_b64 = os.environ.get('YT_COOKIES_B64')

    if cookies_gz_b64:
        # Gzip-compressed variant - use this if the plain base64 value
        # would exceed Railway's 32768-character variable limit (a
        # cookies.txt exported for ALL sites rather than just youtube.com
        # can easily be large enough to hit that).
        try:
            compressed = base64.b64decode(cookies_gz_b64)
            cookies_bytes = gzip.decompress(compressed)
            with open(cookies_path, 'wb') as f:
                f.write(cookies_bytes)
            logger.info(
                f"[COOKIES] Reconstructed cookies file at {cookies_path} from "
                f"YT_COOKIES_GZ_B64 ({len(cookies_bytes)} bytes decompressed)"
            )
            return
        except Exception as e:
            logger.error(f"[COOKIES] Failed to decompress/decode YT_COOKIES_GZ_B64: {e}")
            return

    if not cookies_b64:
        logger.warning(
            "[COOKIES] No cookies.txt exists on disk yet, and neither "
            "YT_COOKIES_B64 nor YT_COOKIES_GZ_B64 is set - cookies.txt will "
            "NOT be created. Downloads requiring authentication will fail "
            "until you upload cookies via /admin/upload-cookies or set an "
            "env var."
        )
        return

    try:
        cookies_bytes = base64.b64decode(cookies_b64)
        with open(cookies_path, 'wb') as f:
            f.write(cookies_bytes)
        logger.info(
            f"[COOKIES] Reconstructed cookies file at {cookies_path} from "
            f"YT_COOKIES_B64 ({len(cookies_bytes)} bytes)"
        )
    except Exception as e:
        logger.error(f"[COOKIES] Failed to reconstruct cookies file from YT_COOKIES_B64: {e}")


# ========== MEMORY / FILE CLEANUP ==========

try:
    _libc = ctypes.CDLL("libc.so.6")
except OSError:
    _libc = None


def release_memory_to_os():
    gc.collect()
    if _libc is not None:
        try:
            _libc.malloc_trim(0)
        except Exception as e:
            logger.warning(f"malloc_trim failed (non-fatal): {e}")


def cleanup_file(filepath):
    try:
        if filepath and os.path.exists(filepath):
            os.remove(filepath)
            logger.info(f"Cleaned up temp file: {filepath}")
    except Exception as e:
        logger.warning(f"Failed to clean up {filepath}: {e}")


# ========== THREAD POOL / BLOCKING CALL OFFLOAD ==========
# FastAPI's event loop is single-threaded for async code. yt_dlp, ffmpeg
# (via subprocess.run), and Essentia/Librosa are all blocking, CPU-bound
# calls - running them directly inside `async def` freezes the WHOLE server
# (including unrelated requests like /health) until that one call finishes.
# Every blocking call is routed through run_blocking() so the event loop
# stays free to accept and queue other requests while heavy work happens
# in a worker thread.
_executor = ThreadPoolExecutor(max_workers=THREAD_POOL_WORKERS)


async def run_blocking(func, *args, **kwargs):
    """Runs a blocking/synchronous function in the thread pool instead of
    on the event loop, so it doesn't freeze the whole server while it runs.

    CONTEXTVARS: the current context is explicitly copied into the worker
    thread. This is NOT automatic - asyncio.create_task() propagates
    contextvars, but loop.run_in_executor() does not, and that gap had a
    real, visible consequence: log_stream.py tags every log line with the
    request id from a contextvar, so EVERY line emitted from inside a
    blocking call (all of yt-dlp's output, download progress, ffmpeg
    errors, Demucs failures) was silently recorded with request_id="-"
    instead of the request that caused it.

    The symptom was the admin dashboard's click-through correlation
    showing only two lines for a request that had produced thirty: the
    two logged on the event loop in routes.py survived, everything from
    the worker thread was orphaned. Copying the context fixes the
    correlation for every blocking call at once, in one place, rather
    than threading a request id through dozens of function signatures.
    """
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(_executor, call)


# ========== SAFE UPLOAD PATHS ==========
# Linux caps a single filename at 255 BYTES - not characters. That
# distinction is the whole bug this exists to prevent: a Hebrew or emoji
# filename is 2-4 bytes per character in UTF-8, so a perfectly ordinary
# ~120-character name blows the limit once a 32-char job-id prefix is
# added, and open() fails with [Errno 36] File name too long. Seen in
# production 2026-08-08: a real user hit it three times in a row on
# /separate and /separate-hq and got a 500 every time.
#
# The fix is not "truncate more carefully" - it's that the user's
# filename has no business being in a temp path at all. The job id
# already guarantees uniqueness, and the original name is captured
# separately (routes.py passes original_filename into mark_*_complete
# for display). Keeping it in the path bought nothing and cost a whole
# class of failure: byte-length limits, path separators, null bytes,
# leading dashes, reserved names.
#
# Only a sanitized extension survives, because ffmpeg/Demucs genuinely
# do use it to infer container format.

MAX_EXTENSION_LENGTH = 10


def safe_extension(filename: str, fallback: str = "bin") -> str:
    """
    Extracts a conservative, filesystem-safe extension from a user
    filename. ASCII alphanumerics only - anything else (path separators,
    unicode, spaces, extra dots) is dropped rather than escaped, since no
    legitimate audio/video extension needs them and every one of them is
    a way to break out of an expected path shape.
    """
    if not filename:
        return fallback
    ext = os.path.splitext(filename)[1].lstrip(".")
    cleaned = "".join(c for c in ext if c.isascii() and c.isalnum()).lower()
    if not cleaned or len(cleaned) > MAX_EXTENSION_LENGTH:
        return fallback
    return cleaned


def build_safe_upload_path(directory: str, job_id: str, filename: str, suffix: str = "") -> str:
    """
    Builds "<directory>/<job_id><suffix>.<ext>" - bounded length by
    construction, with no user-controlled bytes outside a validated
    extension.

    `suffix` exists for the one caller that needs several files under a
    single job (/join uploads N files at once), so they don't collide
    with each other.
    """
    ext = safe_extension(filename)
    return os.path.join(directory, f"{job_id}{suffix}.{ext}")


# ========== CONCURRENCY SEMAPHORES ==========
# This is the actual thing standing between you and an OOM crash when a lot
# of people hit the API at once - it's independent of THREAD_POOL_WORKERS
# above (that's about not freezing the event loop; this is about not
# loading many audio files into RAM simultaneously).
_analysis_semaphore = asyncio.Semaphore(MAX_CONCURRENT_ANALYSIS)
_download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)


async def acquire_slot_or_503(semaphore: asyncio.Semaphore, what: str):
    """
    Waits up to QUEUE_WAIT_TIMEOUT_SECONDS for a free slot on the given
    semaphore. If one frees up in time, the caller proceeds (this IS the
    queueing behavior - excess requests wait here instead of all running
    at once). If the timeout is hit, raises a clean 503 instead of letting
    the request pile on top of an already-overloaded server.
    """
    try:
        await asyncio.wait_for(semaphore.acquire(), timeout=QUEUE_WAIT_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        logger.warning(f"Server busy: no {what} slot freed up within {QUEUE_WAIT_TIMEOUT_SECONDS}s")
        raise HTTPException(
            503,
            f"Server is at capacity ({what} slots full). Please try again shortly."
        )


# ========== CAMELOT WHEEL / KEY MATH ==========

CAMELOT = {
    'C': '8B', 'Db': '3B', 'C#': '3B', 'D': '10B', 'Eb': '5B', 'D#': '5B',
    'E': '12B', 'F': '7B', 'F#': '2B', 'Gb': '2B', 'G': '9B',
    'Ab': '4B', 'G#': '4B', 'A': '11B', 'Bb': '6B', 'A#': '6B', 'B': '1B',
    'Cm': '5A', 'C#m': '12A', 'Dbm': '12A', 'Dm': '7A', 'D#m': '2A', 'Ebm': '2A',
    'Em': '9A', 'Fm': '4A', 'F#m': '11A', 'Gbm': '11A', 'Gm': '6A',
    'G#m': '1A', 'Abm': '1A', 'Am': '8A', 'A#m': '3A', 'Bbm': '3A', 'Bm': '10A'
}

ENHARMONIC = {'C#': 'Db', 'D#': 'Eb', 'F#': 'Gb', 'G#': 'Ab', 'A#': 'Bb'}

# Fixed pitch-class ordering used for all relative-key / bass-chroma math.
# Index arithmetic below relies on this exact order.
PITCH_CLASSES = ['C', 'Db', 'D', 'Eb', 'E', 'F', 'Gb', 'G', 'Ab', 'A', 'Bb', 'B']


def normalize_key(key: str) -> str:
    return ENHARMONIC.get(key, key)


def get_camelot(key: str, scale: str) -> str:
    root = key + ('m' if scale == 'minor' else '')
    return CAMELOT.get(root, "Unknown")


def relative_minor_of_major(major_key: str) -> str:
    """C major's relative minor is A minor, etc. (minor tonic = major tonic - 3 semitones)."""
    idx = PITCH_CLASSES.index(major_key)
    return PITCH_CLASSES[(idx - 3) % 12]


def relative_major_of_minor(minor_key: str) -> str:
    """A minor's relative major is C major, etc. (major tonic = minor tonic + 3 semitones)."""
    idx = PITCH_CLASSES.index(minor_key)
    return PITCH_CLASSES[(idx + 3) % 12]