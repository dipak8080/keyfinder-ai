"""
routes/tiktok.py - POST /tiktok-to-mp3 plus its download endpoint.

WHY A STANDALONE ROUTE RATHER THAN A `source` FIELD ON /download:
rate-limit dependencies are evaluated BEFORE the request body is read,
so a Depends() cannot see a Form value - per-source limits therefore
need per-source routes. The same constraint already forced
/youtube/separate-hq to be its own route rather than a `quality` field.
A standalone route also maps 1:1 onto the SEO landing page, which is
the whole point of the tool.

SHAPE: synchronous, like /download. TikToks are short (10 min ceiling,
typically under 60s) and conversion is a few seconds, so there is no
reason to pay the complexity of a job/polling flow. If a future source
is slow enough to need one, copy the /youtube/* chained pattern instead
of bolting async onto this.

STATUS CODES ARE NOT DECORATION. A 404 tells the frontend "this is gone,
do not retry"; a 503 means "try again shortly". Collapsing them into 500
is what turns a clear problem into an unexplained one - the same
reasoning routes/youtube.py's error chain documents. Every `kind`
core.py can raise is mapped below; there is no fall-through that leaks
raw yt-dlp text.
"""
import os
import time
import uuid
import base64
from functools import partial

from fastapi import APIRouter, HTTPException, Depends, Form
from fastapi.responses import JSONResponse

from config import (
    logger,
    UPLOAD_DIR,
    DOWNLOAD_WALL_CLOCK_TIMEOUT_SECONDS,
    TIKTOK_RATE_LIMIT_MAX_REQUESTS,
    TIKTOK_RATE_LIMIT_WINDOW_SECONDS,
)
from utils import (
    cleanup_file,
    release_memory_to_os,
    run_blocking,
    acquire_slot_or_503,
    _download_semaphore,
)
from rate_limit import check_rate_limit
from cache import get_cached_audio, put_cached_audio
from monitoring import record_result
from log_stream import set_job_context, get_current_request_id

from tiktok.core import (
    is_valid_tiktok_url,
    is_photo_url,
    extract_tiktok_id,
)
from tiktok.runner import run_tiktok_in_subprocess

from ._shared import _mb

router = APIRouter()


# Maps core.py's `kind` onto an HTTP status. Kept as an explicit dict
# rather than an if-chain so that adding a kind to core.py without
# adding it here is a visible KeyError in review, not a silent 500.
_STATUS_BY_KIND = {
    "photo_post": 400,   # user error, actionable, will never work
    "too_long": 400,
    "age_gated": 403,    # needs a login we deliberately do not have
    "blocked": 451,      # legally/regionally restricted by TikTok
    "unavailable": 404,  # gone - do not retry
    "no_audio": 422,     # valid post, nothing to convert
    "no_output": 500,
    "crashed": 500,
    "unknown": 503,      # unclassified: assume transient, invite a retry
}

# Cache namespace. MUST differ from the YouTube path's key space -
# TikTok post IDs are 19-digit numbers and YouTube IDs are 11-char
# strings so a collision is unlikely today, but relying on "unlikely"
# for a cache that serves audio to the wrong user is not a trade worth
# making.
_CACHE_FORMAT = "tiktok_mp3"


@router.post(
    "/tiktok-to-mp3",
    # Its OWN limit, not /download's. That one is 10/hour because a
    # YouTube download can cost paid proxy bandwidth; TikTok has no
    # proxy tier at all and files are ~400 KB, so the only real cost
    # here is a semaphore slot. See config.py for the full reasoning.
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=TIKTOK_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=TIKTOK_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def tiktok_to_mp3(url: str = Form(...)):
    set_job_context(tool="TIKTOK", tier="standard")

    url = (url or "").strip()

    if not is_valid_tiktok_url(url):
        logger.warning(f"[TIKTOK] Rejected - not a recognizable TikTok URL: {url[:120]}")
        raise HTTPException(400, "Please provide a valid TikTok video URL.")

    # Rejected here rather than in the worker: this costs 1ms and saves
    # a semaphore slot plus a ~3s failed extraction. Short links that
    # RESOLVE to a photo post cannot be caught until yt-dlp follows
    # them, and core.py handles those on the error text instead.
    if is_photo_url(url):
        logger.info(f"[TIKTOK] Rejected photo post: {url[:120]}")
        raise HTTPException(
            400,
            "This is a TikTok photo/slideshow post. Only videos with audio "
            "can be converted - try a video post instead."
        )

    started = time.monotonic()

    # Logged BEFORE any work, carrying the URL. Without this a failure
    # further down tells you a TikTok failed but not WHICH one - and the
    # url is the single most useful thing to have when reproducing a
    # report by hand. /download logs the same line for the same reason.
    logger.info(f"[TIKTOK] Request for {url}")

    # Only direct URLs carry an id. Short links return None here and are
    # cached AFTER conversion using the resolved id the worker reports -
    # so a vt. link still populates the cache for the next request, it
    # just cannot read from it on this one.
    post_id = extract_tiktok_id(url)

    if post_id:
        try:
            cached_audio, cached_title = await run_blocking(
                get_cached_audio, post_id, _CACHE_FORMAT
            )
        except Exception as cache_err:
            logger.warning(f"[CACHE] TikTok lookup failed (non-fatal): {cache_err}")
            cached_audio, cached_title = None, None

        if cached_audio:
            logger.info(
                f"[CACHE] HIT tiktok:{post_id} {_mb(len(cached_audio))} "
                f"in {time.monotonic() - started:.2f}s"
            )
            record_result("/tiktok-to-mp3", True)
            # SAME KEY SET as the fresh-conversion response below. A
            # cache hit returning a different shape is the kind of bug
            # that only appears on the second request for a given video,
            # which is exactly when nobody is looking. duration is None
            # here because the cache stores audio and title only.
            return JSONResponse({
                "title": cached_title or "TikTok audio",
                "audio": base64.b64encode(cached_audio).decode("utf-8"),
                "format": "mp3",
                "duration": None,
                "id": post_id,
            })

    job_id = str(uuid.uuid4())
    mp3_path = os.path.join(UPLOAD_DIR, f"{job_id}_tiktok.mp3")

    await acquire_slot_or_503(_download_semaphore, "tiktok")

    audio_b64 = None
    succeeded = False
    try:
        result = await run_tiktok_in_subprocess(
            url=url,
            out_dir=UPLOAD_DIR,
            job_id=job_id,
            timeout_seconds=DOWNLOAD_WALL_CLOCK_TIMEOUT_SECONDS,
            request_id=get_current_request_id(),
        )

        if not result.get("ok"):
            kind = result.get("kind", "unknown")
            # `error` is already user-facing - core.py guarantees raw
            # yt-dlp text never reaches it.
            message = result.get("error") or (
                "Something went wrong while converting this TikTok."
            )
            status = _STATUS_BY_KIND.get(kind, 503)
            logger.warning(f"[TIKTOK] job={job_id} failed kind={kind}: {message}")
            raise HTTPException(status, message)

        if not os.path.exists(mp3_path):
            logger.error(f"[TIKTOK] job={job_id} expected output missing: {mp3_path}")
            raise HTTPException(
                500,
                "The audio file was not produced by the converter. Please try again."
            )

        audio_bytes = await run_blocking(_read_file_bytes, mp3_path)
        audio_b64 = base64.b64encode(audio_bytes).decode("utf-8")
        title = result.get("title") or "TikTok audio"

        # Cache under the RESOLVED id, so short links populate the cache
        # for subsequent requests even though they could not read it.
        #
        # VALIDATED, NOT TRUSTED. cache.py's _cache_file_path() builds a
        # filename directly from this value, and its comment justifies
        # doing so on the basis that a YouTube id is regex-constrained
        # to [a-zA-Z0-9_-]{11} by extract_video_id(). This id comes from
        # yt-dlp's info dict instead and carries no such guarantee - a
        # value containing a path separator would write outside
        # CACHE_DIR. TikTok post ids are numeric, so anything else is
        # either a yt-dlp change worth noticing or an attempt; either
        # way, skip the cache rather than write an unvalidated path.
        raw_id = str(result.get("id") or post_id or "")
        cache_id = raw_id if raw_id.isdigit() else None
        if raw_id and not cache_id:
            logger.warning(
                f"[TIKTOK] Unexpected post id shape, not caching: {raw_id[:40]!r}"
            )

        if cache_id:
            try:
                await run_blocking(
                    put_cached_audio, cache_id, _CACHE_FORMAT, audio_bytes, title
                )
            except Exception as cache_err:
                logger.warning(f"[CACHE] TikTok save failed (non-fatal): {cache_err}")

        raw_size = len(audio_bytes)
        del audio_bytes

        logger.info(
            f"[TIKTOK] COMPLETE '{title[:50]}' {_mb(raw_size)} "
            f"in {time.monotonic() - started:.1f}s"
        )
        succeeded = True
        # duration and id are additive - a client that ignores them is
        # unaffected. They exist so the frontend can show a length
        # before playback and build a stable download filename without
        # having to parse the (emoji-laden, arbitrarily long) title.
        return JSONResponse({
            "title": title,
            "audio": audio_b64,
            "format": "mp3",
            "duration": result.get("duration"),
            "id": cache_id,
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[TIKTOK] job={job_id} unexpected error: {e}", exc_info=True)
        raise HTTPException(
            500,
            "Something went wrong while converting this TikTok. Please try again."
        )
    finally:
        # Runs on every path including the HTTPException ones above, so
        # a failed conversion never leaves a partial file behind. This
        # is the half that the /youtube/* chained tools got wrong before
        # their fail_if_unfinished() fix.
        cleanup_file(mp3_path)
        if audio_b64 is not None:
            del audio_b64
        release_memory_to_os()
        _download_semaphore.release()
        record_result("/tiktok-to-mp3", succeeded)


def _read_file_bytes(path: str) -> bytes:
    """Blocking read, dispatched via run_blocking.

    Reading a finished conversion off disk inline in the async handler
    would block every other connection for its duration, which on a
    single worker is the whole server."""
    with open(path, "rb") as f:
        return f.read()