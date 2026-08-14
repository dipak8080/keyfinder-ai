"""
utils.py - Shared low-level helpers used across the app:
- cookies.txt bootstrap from base64 env var
- memory cleanup / temp file cleanup
- thread pool + run_blocking() for offloading blocking calls
- safe upload path construction (byte-bounded, no user-controlled bytes)
- concurrency semaphores (all six, app-wide) + acquire_slot_or_503()
- Camelot wheel / key math
- killable subprocess for YouTube downloads

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
import sys
import json
import signal
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
    MAX_CONCURRENT_SEPARATIONS,
    MAX_CONCURRENT_AUDIO_TOOLS,
    MAX_CONCURRENT_TRANSCRIPTIONS,
    MAX_CONCURRENT_MIDI,
    QUEUE_WAIT_TIMEOUT_SECONDS,
    YT_COOKIES_PATH_DEFAULT,
    UPLOAD_DIR,
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
#   _transcription_semaphore  - Whisper /speech-to-text, on its own pool
#                                so a slow transcription can't starve
#                                cheap ffmpeg tools of their slots (moved
#                                here, same as above)
#   _midi_semaphore           - /audio-to-midi's HTTP call to the
#                                midi-worker sidecar, on its own pool for
#                                the same reason as transcription (moved
#                                here, same as above)
_analysis_semaphore = asyncio.Semaphore(MAX_CONCURRENT_ANALYSIS)
_download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
_separation_semaphore = asyncio.Semaphore(MAX_CONCURRENT_SEPARATIONS)
_audio_tools_semaphore = asyncio.Semaphore(MAX_CONCURRENT_AUDIO_TOOLS)
_transcription_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TRANSCRIPTIONS)
_midi_semaphore = asyncio.Semaphore(MAX_CONCURRENT_MIDI)


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


# ========== KILLABLE SUBPROCESS FOR YOUTUBE DOWNLOADS ==========
# WHY THIS EXISTS: run_blocking() above offloads to a ThreadPoolExecutor,
# and asyncio.wait_for() timing out on a thread-pool future only abandons
# the AWAIT - the thread itself keeps running to completion, since Python
# has no way to forcibly kill a thread. Confirmed in production 2026-08-14:
# a /download request logged its 180s wall-clock timeout at 4:51:04, then
# the SAME extract_info attempt (started at 4:48:08) logged its own
# failure at 4:55:26 - over 4 minutes AFTER the app had already released
# the semaphore slot and returned a 503 to the user. That gap is yt-dlp's
# PO-token (Node) / JS-challenge (Deno) subprocesses still running,
# unsupervised, still consuming a proxy connection, still holding a
# thread-pool worker - all invisible to the rest of the app, which had
# already moved on.
#
# This runs the ENTIRE download_with_fallback() call in its own OS
# process instead, so a timeout can SIGKILL the whole process GROUP
# (worker + every child it spawned) rather than politely giving up on
# waiting for a thread. Deliberately isolates the WHOLE call rather than
# hunting for yt-dlp's specific PO-token/JS-challenge child PIDs - that
# would be fragile against yt-dlp internals changing across versions;
# killing the process group is correct regardless of what yt-dlp does
# internally.

def _worker_script_path() -> str:
    """download_worker.py lives next to this file - resolved relative to
    utils.py's own location rather than assuming a fixed working
    directory, so this doesn't break if the process is ever launched from
    a different cwd."""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "download_worker.py")


async def run_in_killable_subprocess(
    ydl_opts_serializable: dict,
    url: str,
    proxy_url: str,
    timeout_seconds: int,
    job_id: str,
    progress_label: str = None,
    request_id: str = "-",
) -> dict:
    """
    Spawns download_worker.py in its own process group, writes ydl_opts+url
    to a temp input file on UPLOAD_DIR (same disk every other temp file in
    this app already uses - not /tmp, which may be a different mount and
    isn't cleaned up by any of this app's existing TTL/cleanup logic),
    waits up to timeout_seconds.

    Returns a plain dict, always - never raises. Callers (routes.py,
    youtube_chain.py) branch on result["ok"]:
      {"ok": True, "title": "..."}
      {"ok": False, "kind": "timeout" | "too_long" | "crashed" | "error",
       "error": "..."}
    "error" text is what the existing is_permanent_error() /
    is_bot_check_error() / etc. classification chain in youtube.py should
    be run against downstream - unchanged from what str(e) used to
    provide, so nothing in that classification logic needs to change.

    ydl_opts_serializable must NOT contain the 'logger' object or
    'progress_hooks' closures - those don't survive a JSON boundary. The
    caller strips them before calling this; download_worker.py
    reconstructs its own ytdlp_alert_logger internally.
    """
    input_path = os.path.join(UPLOAD_DIR, f"{job_id}_worker_in.json")
    output_path = os.path.join(UPLOAD_DIR, f"{job_id}_worker_out.json")

    # Imported lazily: youtube.py does not import utils, so there is no
    # cycle today, but a module-level import here would create one the
    # moment it ever does.
    from youtube import export_breaker_state, apply_events

    try:
        with open(input_path, "w") as f:
            json.dump(
                {
                    "ydl_opts": ydl_opts_serializable,
                    "url": url,
                    "proxy_url": proxy_url,
                    # The worker imports youtube.py fresh, so its breakers
                    # start empty. Without this it would retry a proxy the
                    # parent already circuit-broke and cookie accounts the
                    # parent already disabled.
                    "breaker_state": export_breaker_state(),
                    "progress_label": progress_label,
                    "request_id": request_id,
                },
                f,
            )
    except Exception as e:
        logger.error(f"[DOWNLOAD_WORKER] job={job_id} failed to write input file: {e}")
        return {"ok": False, "kind": "error", "error": f"Failed to prepare download: {e}"}

    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, _worker_script_path(), input_path, output_path,
            start_new_session=True,  # own process group - required for killpg below
            # NO stdout/stderr PIPE - inherits parent's fds so yt-dlp's
            # verbose output, [COOKIES]/[PROXY]/[CDN] log lines, etc. still
            # reach the container's log stream. Piping them silently
            # discarded every log line from inside a download.
        )

        try:
            await asyncio.wait_for(proc.wait(), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            # Kill the WHOLE process group, not just proc.pid - the worker's
            # Node/Deno/ffmpeg children are what actually keep burning
            # proxy bandwidth and CPU after a timeout, and they don't die
            # just because their parent does. start_new_session=True above
            # is what makes killpg() valid here.
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                logger.warning(
                    f"[DOWNLOAD_WORKER] job={job_id} exceeded {timeout_seconds}s - "
                    f"killed process group {proc.pid} (worker + any Node/Deno/ffmpeg children)"
                )
            except ProcessLookupError:
                # Process already exited between the timeout firing and us
                # trying to kill it - harmless race, nothing left to kill.
                pass
            return {"ok": False, "kind": "timeout", "error": "Download timed out."}

        if proc.returncode != 0:
            logger.error(
                f"[DOWNLOAD_WORKER] job={job_id} exited with code {proc.returncode} "
                f"(see worker's own log lines above for detail)"
            )
            return {
                "ok": False,
                "kind": "crashed",
                "error": f"Downloader process crashed unexpectedly (exit {proc.returncode}).",
            }

        try:
            with open(output_path) as f:
                result = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(
                f"[DOWNLOAD_WORKER] job={job_id} produced no valid output file: {e} "
                f"(see worker's own log lines above for detail)"
            )
            return {
                "ok": False,
                "kind": "crashed",
                "error": "Downloader process did not return a result.",
            }

        # Replay whatever tripped inside the worker into THIS process's
        # long-lived state - the worker's own copy dies with it. Wrapped
        # so a bad event can never fail a download that succeeded.
        try:
            apply_events(result.pop("events", []))
        except Exception as e:
            logger.warning(f"[DOWNLOAD_WORKER] job={job_id} failed to apply breaker events: {e}")

        return result

    finally:
        cleanup_file(input_path)
        cleanup_file(output_path)