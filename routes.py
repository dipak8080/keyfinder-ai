"""
routes.py - The two APIs (download, analyze) plus root/health/admin.
All business logic lives in youtube.py / audio_analysis.py / utils.py -
this file just wires HTTP in and out.
"""
import os
import uuid
import base64

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends, Query
from fastapi.responses import JSONResponse

from config import logger, UPLOAD_DIR, MAX_UPLOAD_BYTES, ANALYSIS_MAX_SECONDS, ADMIN_STATUS_KEY, CACHE_ENABLED, R2_BUCKET_NAME
from utils import (
    cleanup_file,
    release_memory_to_os,
    run_blocking,
    acquire_slot_or_503,
    get_camelot,
    _analysis_semaphore,
    _download_semaphore,
)
from youtube import (
    download_with_fallback,
    is_bot_check_error,
    is_geo_restricted_error,
    is_permanent_error,
    is_valid_youtube_url,
    extract_video_id,
    VideoTooLongError,
    proxy_available,
    reset_proxy_circuit_breaker,
    get_cookie_accounts,
    ytdlp_alert_logger,
)
from audio_analysis import detect_key_bpm_essentia, cross_check_with_librosa, trim_audio_for_analysis
from rate_limit import check_rate_limit
from cache import get_cached_audio, put_cached_audio
from monitoring import record_result, get_status_snapshot

router = APIRouter()


@router.post("/download", dependencies=[Depends(check_rate_limit)])
async def download_audio(url: str = Form(...), format: str = Form("mp3")):
    if format not in ["mp3", "wav"]:
        raise HTTPException(400, "Format must be 'mp3' or 'wav'")

    # Cheap, instant check BEFORE touching yt-dlp, the download semaphore,
    # or the proxy - a garbage/non-YouTube URL was never going to succeed,
    # so there's no reason to spend 3 retries-with-backoff (~10s) and a
    # concurrency slot on it. Real videos that are private/deleted/etc.
    # still pass this shape check and get caught downstream by
    # is_permanent_error() during the real yt-dlp call, same as before.
    if not is_valid_youtube_url(url):
        logger.warning(f"Rejected download - not a recognizable YouTube URL: {url}")
        raise HTTPException(400, "Please provide a valid YouTube video URL.")

    # Cache check happens BEFORE the download semaphore is touched - a
    # cache hit is a fast R2 read (roughly 1-3s), not the heavy CPU/network
    # work the semaphore is meant to bound, so cache hits deliberately
    # don't compete with real yt-dlp downloads for a concurrency slot.
    # video_id may be None for a URL shape extract_video_id() doesn't
    # recognize (rare, given is_valid_youtube_url already passed) - in
    # that case caching is just skipped, not an error.
    video_id = extract_video_id(url)
    if video_id:
        try:
            cached_audio, cached_title = await run_blocking(get_cached_audio, video_id, format)
        except Exception as cache_err:
            logger.warning(f"[CACHE] Lookup failed (non-fatal, proceeding with fresh download): {cache_err}")
            cached_audio, cached_title = None, None

        if cached_audio:
            cached_b64 = base64.b64encode(cached_audio).decode('utf-8')
            logger.info(f"[CACHE] Serving '{cached_title}' from cache instead of downloading ({len(cached_b64)} base64 chars)")
            record_result("/download", True)
            return JSONResponse({"title": cached_title or "Unknown", "audio": cached_b64, "format": format})

    temp_id = str(uuid.uuid4())
    temp_path = os.path.join(UPLOAD_DIR, f"{temp_id}.%(ext)s")
    output_file = os.path.join(UPLOAD_DIR, f"{temp_id}.{format}")

    # Base options WITHOUT a proxy - this is Tier 1 (direct + cookies),
    # tried first because it's free and clears most bot-checks on its own.
    # youtube.download_with_fallback() only adds the proxy in as Tier 2 if
    # this specifically fails with a bot-check/format-restriction error.
    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': temp_path,
        'quiet': False,
        'verbose': True,
        'noplaylist': True,
        'ffmpeg_location': '/usr/bin/ffmpeg',
        'extractor_args': {
            'youtube': {
                'player_client': ['ios', 'android', 'mweb', 'web']
            }
        },
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': format,
            'preferredquality': '192',
        }],
        # Deno being installed is not enough on its own - yt-dlp additionally
        # requires explicit opt-in before it will download the actual EJS
        # challenge-solver script Deno needs to run (a security/privacy
        # default). Without this, signature/n-parameter solving silently
        # fails even with Deno present, and YouTube's SABR-only streaming
        # then leaves no usable audio formats at all ("Requested format is
        # not available"). 'ejs:github' fetches it directly from the
        # official yt-dlp-ejs GitHub repo - small, one-time per version.
        'remote_components': {'ejs:github'},
        # Routes every log line yt-dlp produces (including the cookie-
        # expiry warning) through our logger so a Discord alert can fire
        # on it - see youtube._YtdlpAlertLogger. Every line still prints
        # exactly as before; this only adds a side-channel check on top,
        # it doesn't suppress or change any existing verbose log output.
        # Applies to BOTH tiers automatically, since download_with_fallback
        # builds the proxy attempt via {**base_ydl_opts, 'proxy': ...} -
        # the logger key carries over unchanged either way.
        'logger': ytdlp_alert_logger,
    }

    # Cookie account selection now happens INSIDE download_with_fallback
    # (see youtube.get_cookie_accounts / download_with_fallback) - it
    # rotates through up to 3 accounts on LOGIN_REQUIRED failures, so we
    # deliberately do NOT preset 'cookiefile' here anymore; whichever
    # account is tried first is chosen dynamically per-attempt.
    proxy_url = os.environ.get('YT_PROXY_URL')
    available_accounts = get_cookie_accounts()
    logger.info(
        f"[COOKIES] accounts_available={len(available_accounts)} "
        f"[PROXY] configured={bool(proxy_url)} circuit_breaker={'OPEN' if not proxy_available() else 'CLOSED'} "
        f"url={url}"
    )

    # Wait (up to QUEUE_WAIT_TIMEOUT_SECONDS) for a free download slot -
    # this is what keeps N simultaneous downloads bounded instead of
    # unbounded, and returns a clean 503 instead of crashing if the server
    # stays saturated past the wait window.
    await acquire_slot_or_503(_download_semaphore, "download")

    audio_data = None
    succeeded = False
    try:
        try:
            # Offloaded to the thread pool - yt_dlp + ffmpeg postprocessing
            # are fully blocking and would otherwise freeze the event loop.
            # download_with_fallback now does duration-checking AND the
            # real download in a single extraction pass per attempt (see
            # youtube.extract_info_with_retry) - no more separate
            # duration-only pre-check burning a second full YouTube
            # handshake. It rotates cookie accounts first, then proxy
            # (only on non-permanent, non-duration errors), and trips the
            # proxy circuit breaker on billing/quota-style proxy failures.
            info = await run_blocking(download_with_fallback, ydl_opts, url, proxy_url)
            title = info.get('title', 'Unknown')
        except VideoTooLongError as e:
            logger.warning(f"Rejected download - video too long: {e}")
            raise HTTPException(400, str(e))
        except Exception as e:
            error_text = str(e)

            if is_permanent_error(error_text):
                # The video itself is the problem - deleted, private,
                # copyright-blocked, or otherwise permanently unavailable.
                # No amount of retrying, cookie rotation, or proxy
                # switching would ever fix this (see youtube.py's
                # PERMANENT_ERROR_MARKERS). Distinct from a 500: this
                # isn't a server bug, it's a resource that genuinely
                # doesn't exist - 404 is the semantically correct status,
                # and unlike a 500 (which the frontend deliberately masks
                # with a generic message) the user gets an accurate,
                # actionable reason instead of "something went wrong."
                logger.warning(f"Permanent error for URL {url}: {error_text}")
                raise HTTPException(
                    404,
                    "This video is unavailable - it may have been deleted, made private, "
                    "or removed for copyright reasons. Please try a different video."
                )

            if is_geo_restricted_error(error_text):
                # Distinct from the generic bot-check message below - this
                # is a licensing/rights restriction, not an anti-bot
                # measure, and no amount of "try again later" will ever
                # fix it for THIS video from a server exit IP outside the
                # allowed region(s). Give the user an accurate, actionable
                # message instead of a raw 500/traceback.
                logger.warning(f"Geo-restricted video blocked download for URL: {url}")
                raise HTTPException(
                    451,  # "Unavailable For Legal Reasons" - the semantically correct status for this
                    "This video is restricted by the uploader to specific countries and "
                    "isn't available from our server's location. This isn't something we "
                    "can fix on our end for this particular video - try a different one."
                )

            if is_bot_check_error(error_text):
                logger.error(f"YouTube bot verification / format restriction blocked download for URL: {url}")
                raise HTTPException(
                    503,
                    "This video is temporarily unavailable for download because YouTube is "
                    "requiring bot verification or is restricting available formats for this client. "
                    "Please try again in a few minutes."
                )

            logger.error(f"Download failed after attempts: {error_text}")
            raise HTTPException(500, f"Failed: {error_text}")

        if not os.path.exists(output_file):
            logger.error(f"Expected output file not found after download: {output_file}")
            raise HTTPException(500, "Failed: audio file was not produced by the downloader")

        with open(output_file, "rb") as f:
            audio_bytes = f.read()
        audio_data = base64.b64encode(audio_bytes).decode('utf-8')

        if video_id:
            try:
                await run_blocking(put_cached_audio, video_id, format, audio_bytes, title)
            except Exception as cache_err:
                # A cache-save failure must never fail a download that
                # already succeeded - log and move on, user still gets
                # their file this request, it just won't be cached for
                # NEXT time.
                logger.warning(f"[CACHE] Failed to save to cache (non-fatal): {cache_err}")

        del audio_bytes

        logger.info(f"Download complete: '{title}' ({format}) → {len(audio_data)} base64 chars")

        succeeded = True
        return JSONResponse({"title": title, "audio": audio_data, "format": format})

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in /download: {e}", exc_info=True)
        raise HTTPException(500, f"Failed: {str(e)}")
    finally:
        cleanup_file(output_file)
        if audio_data is not None:
            del audio_data
        release_memory_to_os()
        _download_semaphore.release()
        # Recorded regardless of outcome - this is what /admin/status and
        # the failure-spike alert in monitoring.py are built on.
        record_result("/download", succeeded)


@router.post("/analyze", dependencies=[Depends(check_rate_limit)])
async def analyze_audio(file: UploadFile = File(...)):
    logger.info(f"Analyzing: {file.filename}")

    file_id = str(uuid.uuid4())
    file_path = os.path.join(UPLOAD_DIR, f"{file_id}_{file.filename}")
    analysis_path = file_path

    # Wait (up to QUEUE_WAIT_TIMEOUT_SECONDS) for a free analysis slot -
    # this bounds how many Essentia/Librosa audio buffers can be in memory
    # at the same time, which is the actual thing that OOM-kills the
    # container under load. Requests beyond the cap wait here (this is your
    # queue) rather than all running - and racing for RAM - at once.
    await acquire_slot_or_503(_analysis_semaphore, "analysis")

    content = None
    succeeded = False
    try:
        content = await file.read()

        if len(content) == 0:
            raise HTTPException(400, "Empty file")

        if len(content) > MAX_UPLOAD_BYTES:
            size_mb = len(content) / (1024 * 1024)
            logger.warning(f"Rejected upload '{file.filename}': {size_mb:.1f} MB exceeds 50 MB limit")
            raise HTTPException(
                400,
                f"File too large ({size_mb:.1f} MB). Maximum allowed size is 50 MB."
            )

        with open(file_path, "wb") as f:
            f.write(content)

        del content
        content = None
        release_memory_to_os()

        if ANALYSIS_MAX_SECONDS is not None:
            # ffmpeg subprocess call - blocking, offload to thread pool.
            analysis_path = await run_blocking(trim_audio_for_analysis, file_path, ANALYSIS_MAX_SECONDS)

        # Primary: Essentia (pro-level accuracy), with relative-key and
        # BPM-octave corrections already applied inside this call.
        # Offloaded - this is the single most CPU-heavy step in the request.
        key, scale, key_conf, bpm, bpm_conf = await run_blocking(detect_key_bpm_essentia, analysis_path)

        # Cross-check against Librosa as an independent second opinion.
        # This never changes the reported key/BPM - only the confidence,
        # plus an "agreement" flag the frontend can use to show a
        # "low confidence" badge if the two disagree. Also offloaded.
        key, scale, key_conf, bpm, bpm_conf, agreement = await run_blocking(
            cross_check_with_librosa, analysis_path, key, scale, key_conf, bpm, bpm_conf
        )

        camelot = get_camelot(key, scale)
        key_name = f"{key} {scale}"

        result = {
            "key": key_name,
            "camelot": camelot,
            "bpm": bpm,
            "confidence": int(min(0.99, key_conf) * 100),
            "bpm_confidence": min(99, bpm_conf),
            "cross_check": agreement,
        }

        logger.info(f"RESULT: {result}")

        succeeded = True
        return JSONResponse(result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Analysis error: {e}", exc_info=True)
        raise HTTPException(500, f"Failed: {str(e)}")
    finally:
        cleanup_file(file_path)
        if analysis_path != file_path:
            cleanup_file(analysis_path)
        if content is not None:
            del content
        release_memory_to_os()
        _analysis_semaphore.release()
        record_result("/analyze", succeeded)


@router.get("/")
async def root():
    return {
        "status": "Audio Analysis API v13.3 - ESSENTIA FIXED + KEY/BPM CORRECTIONS + MONITORING + RATE LIMITING + DURATION CAP + PROXY FALLBACK + COOKIE ALERTS + GEO-RESTRICTION HANDLING + MULTI-ACCOUNT COOKIE ROTATION + SINGLE-PASS EXTRACTION + R2 CACHING + PERMANENT-ERROR 404",
        "accuracy": "Essentia research-grade + relative major/minor correction + BPM octave correction + Librosa cross-check",
        "engine": "Essentia KeyExtractor + RhythmExtractor2013",
        "fixes": [
            "Removed invalid BPMHistogramDescriptors",
            "Proper BPM via RhythmExtractor2013 (confidence included)",
            "Robust fallback with enhanced Librosa",
            "Retry with exponential backoff on yt_dlp failures",
            "Permanent-error detection (video unavailable/private/removed) skips retries to save proxy bandwidth and fail fast",
            "Video duration cap now checked as part of a SINGLE extraction pass (extract once, check duration, reuse that same result to download via process_ie_result) instead of a separate duration-only pre-check followed by a second full independent extraction - removes an entire duplicate webpage/player-API/PO-token/JS-challenge round trip from every request",
            "Clean 503 on YouTube bot verification / format restriction instead of raw error",
            "Clean 451 on geo-restricted videos, with same-IP fail-fast (no wasted retries) but still escalates to proxy since a different exit region CAN fix it",
            "Guaranteed temp file cleanup via finally blocks",
            "Explicit memory freeing + gc.collect() + malloc_trim() after each request",
            "Audio trimmed to first 180s before analysis to cap peak memory",
            "50MB upload size limit",
            "Relative major/minor correction using bass-register chroma energy",
            "BPM half/double (octave) error correction against a typical tempo range",
            "Librosa cross-check adjusts confidence (and flags disagreement) without overriding Essentia's answer",
            "All blocking work (yt_dlp, ffmpeg, Essentia, Librosa) offloaded to a thread pool so it never freezes the event loop",
            "Concurrency capped via semaphores (MAX_CONCURRENT_ANALYSIS / MAX_CONCURRENT_DOWNLOADS) to bound peak memory",
            "Requests queue for a free slot up to QUEUE_WAIT_TIMEOUT_SECONDS, then return a clean 503 instead of crashing",
            "Broadened yt_dlp player_client list (ios, android, mweb, web) to reduce 'Requested format is not available' failures",
            "cookies.txt reconstructed at startup from base64 Railway env var (YT_COOKIES_B64), since it's gitignored and Railway builds from GitHub, not local disk",
            "Per-request [COOKIES]/[PROXY] status log line for instant visibility in Railway Deploy Logs",
            "Tiered download strategy: direct+cookies first (free), proxy retry only on bot-check errors (paid fallback, not default)",
            "Proxy circuit breaker: billing/quota-style proxy failures disable the proxy for a cooldown window instead of retrying a dead proxy on every request, with an immediate webhook alert",
            "Cookie-expiry Discord alert: fires a throttled webhook alert only after sustained (not one-off) dead-cookie warnings, instead of alerting on a single flaky yt-dlp heuristic check",
            "Multi-account cookie rotation: up to 3 cookie sessions, auto-rotates to the next account on a confirmed LOGIN_REQUIRED failure (per-account cooldown), before falling back to the proxy tier",
            "CORS locked to ALLOWED_ORIGINS (defaults to '*' until explicitly configured)",
            "Per-IP rate limiting on /download and /analyze",
            "Failure-spike monitoring with optional webhook alerting (Discord/Slack compatible)",
            "R2 caching: repeat requests for the same video+format are served straight from cache (~1-3s) instead of re-running the full yt-dlp pipeline (~20-50s) - fails safe to a normal fresh download if R2 is unreachable or not configured",
            "Clean 404 on permanently unavailable videos (deleted/private/copyright) instead of falling through to a generic masked 500 - user gets an accurate, actionable reason",
        ]
    }


@router.get("/health")
async def health():
    return {"status": "healthy"}


@router.get("/admin/status")
async def admin_status(key: str = Query(...)):
    """
    Simple operational dashboard, protected by a shared secret query param
    (?key=...). Set ADMIN_STATUS_KEY in Railway to something random/long -
    this is NOT the same thing as your site's /admin contact-messages page
    (that's a separate Lovable/frontend feature); this is just this
    backend's own health/failure snapshot, useful to hit manually or wire
    into a frontend dashboard later if you want.
    """
    if key != ADMIN_STATUS_KEY:
        raise HTTPException(403, "Invalid admin key")
    snapshot = get_status_snapshot()
    snapshot["proxy"] = {
        "circuit_breaker": "OPEN (proxy disabled)" if not proxy_available() else "CLOSED (proxy available)",
    }
    snapshot["cookies"] = {
        "accounts_available": len(get_cookie_accounts()),
    }
    snapshot["cache"] = {
        "enabled": CACHE_ENABLED,
        "configured": bool(R2_BUCKET_NAME),
    }
    return snapshot


@router.post("/admin/reset-proxy")
async def admin_reset_proxy(key: str = Query(...)):
    """
    Manually re-enables the proxy immediately (rather than waiting out
    PROXY_CIRCUIT_BREAKER_COOLDOWN_SECONDS) - use this right after topping
    up the proxy provider balance so downloads can use it again without
    delay.
    """
    if key != ADMIN_STATUS_KEY:
        raise HTTPException(403, "Invalid admin key")
    reset_proxy_circuit_breaker()
    return {"status": "proxy circuit breaker reset"}