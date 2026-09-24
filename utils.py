"""
utils.py - Shared low-level helpers used across the app:
- memory cleanup / temp file cleanup
- thread pool + run_blocking() for offloading blocking calls
- safe upload path construction (byte-bounded, no user-controlled bytes)
- concurrency semaphores (all seven, app-wide) + acquire_slot_or_503()
- Camelot wheel / key math

--------------------------------------------------------------------------
WHAT CHANGED (2026-08-14): SEMAPHORE CONSOLIDATION

The four route-level semaphores (_separation_semaphore, _audio_tools_
semaphore, _transcription_semaphore, _midi_semaphore) used to live as
module-level globals inside routes.py. During the routes/ package
restructure they moved here, next to the two that already lived here
(_analysis_semaphore, _download_semaphore) - so there is exactly ONE
place in the codebase where "how many things can run at once" is
declared, instead of two. Nothing about how any of the six is used
changed: same asyncio.Semaphore objects, same import-time construction,
same acquire/release pattern via `async with` or acquire_slot_or_503().
routes/_shared.py and every routes/*.py module now import whichever of
the six they need from here instead of from routes.py.
--------------------------------------------------------------------------
"""
import os
import gc
import ctypes
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
    MAX_CONCURRENT_SEPARATIONS,
    MAX_CONCURRENT_AUDIO_TOOLS,
    MAX_CONCURRENT_MIDI,
    MAX_CONCURRENT_MIDI_HQ,
    QUEUE_WAIT_TIMEOUT_SECONDS,
)

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
#
# All six of the app's concurrency pools are declared here, together:
#   _analysis_semaphore       - /analyze, and the analyze half of
#                                /youtube/analyze
#   _download_semaphore       - /download, and the download half of every
#                                /youtube/* chained route
#   _separation_semaphore     - Demucs: /separate(-hq), /stems(-hq), and
#                                their /youtube/* equivalents (moved here
#                                from routes.py during the routes/ package
#                                restructure - see this file's own
#                                "WHAT CHANGED" note above)
#   _audio_tools_semaphore    - every ffmpeg/rubberband tool (convert,
#                                trim, volume, pitch, tempo, reverse,
#                                noise-remove, voice-clean, echo-remove,
#                                silence-remove, loudnorm, fade, channels,
#                                resample, ringtone, video-to-audio, join,
#                                silence-split) (moved here, same as above)
#   _midi_semaphore           - /audio-to-midi's HTTP call to the
#                                midi-worker sidecar, on its own pool so a
#                                slow job can't starve the ffmpeg tools
#                                (moved here, same as above)
#   _midi_hq_semaphore        - /audio-to-midi-hq's call to the RunPod
#                                MT3 worker (added 2026-08-28)
#
# THE LAST TWO ARE DELIBERATELY SEPARATE POOLS, and it is worth saying
# why, because "both are MIDI" makes sharing look obvious.
#
# They do not contend for the same resource. _midi_semaphore bounds
# concurrent HTTP calls into a CPU sidecar running on THIS box, where the
# limit exists to stop basic-pitch starving the ffmpeg tools of cores.
# _midi_hq_semaphore bounds concurrent jobs in flight to PAID GPU
# capacity, where the limit exists to match the RunPod worker count so a
# second worker is not left idle by construction (the same reasoning
# MAX_CONCURRENT_SEPARATIONS documents at length after that exact bug).
#
# Sharing one semaphore would mean a busy midi-worker could block a paid
# HQ job that never touches it, and vice versa - a free user's job
# delaying someone who paid, for no resource reason at all. Two pools,
# two constants, two numbers that move independently.
_analysis_semaphore = asyncio.Semaphore(MAX_CONCURRENT_ANALYSIS)
_download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
_separation_semaphore = asyncio.Semaphore(MAX_CONCURRENT_SEPARATIONS)
_audio_tools_semaphore = asyncio.Semaphore(MAX_CONCURRENT_AUDIO_TOOLS)
_midi_semaphore = asyncio.Semaphore(MAX_CONCURRENT_MIDI)
_midi_hq_semaphore = asyncio.Semaphore(MAX_CONCURRENT_MIDI_HQ)


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