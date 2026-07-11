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

from config import logger, UPLOAD_DIR, MAX_UPLOAD_BYTES, ANALYSIS_MAX_SECONDS, YT_COOKIES_PATH_DEFAULT, ADMIN_STATUS_KEY
from utils import (
    cleanup_file,
    release_memory_to_os,
    run_blocking,
    acquire_slot_or_503,
    get_camelot,
    _analysis_semaphore,
    _download_semaphore,
)
from youtube import extract_info_with_retry, is_bot_check_error
from audio_analysis import detect_key_bpm_essentia, cross_check_with_librosa, trim_audio_for_analysis
from rate_limit import check_rate_limit
from monitoring import record_result, get_status_snapshot

router = APIRouter()


@router.post("/download", dependencies=[Depends(check_rate_limit)])
async def download_audio(url: str = Form(...), format: str = Form("mp3")):
    if format not in ["mp3", "wav"]:
        raise HTTPException(400, "Format must be 'mp3' or 'wav'")

    temp_id = str(uuid.uuid4())
    temp_path = os.path.join(UPLOAD_DIR, f"{temp_id}.%(ext)s")
    output_file = os.path.join(UPLOAD_DIR, f"{temp_id}.{format}")

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
    }

    # Cookies status is logged on EVERY request as one unambiguous,
    # greppable line - search "[COOKIES]" in Railway's Deploy Logs to
    # instantly see whether this specific download actually had cookies
    # available, without needing shell access into the container. The
    # actual file is reconstructed once at startup from YT_COOKIES_B64 (see
    # utils.ensure_cookies_file()) - this block just checks it's really
    # there and wires it into yt-dlp's options for this request.
    cookies_path = os.environ.get('YT_COOKIES_PATH', YT_COOKIES_PATH_DEFAULT)
    cookies_active = bool(cookies_path and os.path.exists(cookies_path))
    if cookies_active:
        ydl_opts['cookiefile'] = cookies_path

    logger.info(
        f"[COOKIES] status={'ACTIVE' if cookies_active else 'MISSING'} path={cookies_path} url={url}"
    )

    proxy_url = os.environ.get('YT_PROXY_URL')
    if proxy_url:
        ydl_opts['proxy'] = proxy_url
        logger.info("Using configured proxy for yt-dlp")

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
            info = await run_blocking(extract_info_with_retry, ydl_opts, url)
            title = info.get('title', 'Unknown')
        except Exception as e:
            error_text = str(e)

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
        "status": "Audio Analysis API v12.5 - ESSENTIA FIXED + KEY/BPM CORRECTIONS + MONITORING + RATE LIMITING",
        "accuracy": "Essentia research-grade + relative major/minor correction + BPM octave correction + Librosa cross-check",
        "engine": "Essentia KeyExtractor + RhythmExtractor2013",
        "fixes": [
            "Removed invalid BPMHistogramDescriptors",
            "Proper BPM via RhythmExtractor2013 (confidence included)",
            "Robust fallback with enhanced Librosa",
            "Retry with exponential backoff on yt_dlp failures",
            "Clean 503 on YouTube bot verification / format restriction instead of raw error",
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
            "Per-request [COOKIES] status log line (ACTIVE/MISSING) for instant visibility in Railway Deploy Logs",
            "CORS locked to ALLOWED_ORIGINS (defaults to '*' until explicitly configured)",
            "Per-IP rate limiting on /download and /analyze",
            "Failure-spike monitoring with optional webhook alerting (Discord/Slack compatible)",
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
    return get_status_snapshot()