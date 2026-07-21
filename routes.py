"""
routes.py - The two APIs (download, analyze) plus root/health/admin,
plus the new /separate (vocal remover) endpoints.
All business logic lives in youtube.py / audio_analysis.py / utils.py /
separation.py - this file just wires HTTP in and out.
"""
import os
import uuid
import base64
import asyncio
import shutil
from functools import partial

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends, Query
from fastapi.responses import JSONResponse, FileResponse

from config import (
    logger,
    UPLOAD_DIR,
    MAX_UPLOAD_BYTES,
    ANALYSIS_MAX_SECONDS,
    ADMIN_STATUS_KEY,
    CACHE_ENABLED,
    R2_BUCKET_NAME,
    SEPARATION_RATE_LIMIT_MAX_REQUESTS,
    SEPARATION_RATE_LIMIT_WINDOW_SECONDS,
    MAX_CONCURRENT_SEPARATIONS,
)
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
from download_progress import make_progress_hook
from jobs import create_job, mark_complete, mark_failed, get_job, cleanup_expired_jobs
from separation import run_separation, SeparationError

router = APIRouter()

# One dedicated semaphore for separation, same pattern as
# _analysis_semaphore / _download_semaphore in utils.py - caps how many
# Demucs subprocesses can run at once (default 1, since it's the most
# RAM-hungry endpoint in this app).
_separation_semaphore = asyncio.Semaphore(MAX_CONCURRENT_SEPARATIONS)


@router.post("/download", dependencies=[Depends(check_rate_limit)])
async def download_audio(url: str = Form(...), format: str = Form("mp3")):
    if format not in ["mp3", "wav"]:
        raise HTTPException(400, "Format must be 'mp3' or 'wav'")

    if not is_valid_youtube_url(url):
        logger.warning(f"Rejected download - not a recognizable YouTube URL: {url}")
        raise HTTPException(400, "Please provide a valid YouTube video URL.")

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
        'remote_components': {'ejs:github'},
        'logger': ytdlp_alert_logger,
        'progress_hooks': [make_progress_hook(video_id or url)],
    }

    proxy_url = os.environ.get('YT_PROXY_URL')
    available_accounts = get_cookie_accounts()
    logger.info(
        f"[COOKIES] accounts_available={len(available_accounts)} "
        f"[PROXY] configured={bool(proxy_url)} circuit_breaker={'OPEN' if not proxy_available() else 'CLOSED'} "
        f"url={url}"
    )

    await acquire_slot_or_503(_download_semaphore, "download")

    audio_data = None
    succeeded = False
    try:
        try:
            info = await run_blocking(download_with_fallback, ydl_opts, url, proxy_url)
            title = info.get('title', 'Unknown')
        except VideoTooLongError as e:
            logger.warning(f"Rejected download - video too long: {e}")
            raise HTTPException(400, str(e))
        except Exception as e:
            error_text = str(e)

            if is_permanent_error(error_text):
                logger.warning(f"Permanent error for URL {url}: {error_text}")
                raise HTTPException(
                    404,
                    "This video is unavailable - it may have been deleted, made private, "
                    "or removed for copyright reasons. Please try a different video."
                )

            if is_geo_restricted_error(error_text):
                logger.warning(f"Geo-restricted video blocked download for URL: {url}")
                raise HTTPException(
                    451,
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
        record_result("/download", succeeded)


@router.post("/analyze", dependencies=[Depends(check_rate_limit)])
async def analyze_audio(file: UploadFile = File(...)):
    logger.info(f"Analyzing: {file.filename}")

    file_id = str(uuid.uuid4())
    file_path = os.path.join(UPLOAD_DIR, f"{file_id}_{file.filename}")
    analysis_path = file_path

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
            analysis_path = await run_blocking(trim_audio_for_analysis, file_path, ANALYSIS_MAX_SECONDS)

        key, scale, key_conf, bpm, bpm_conf = await run_blocking(detect_key_bpm_essentia, analysis_path)

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


# ============================================================
# /separate - Demucs vocal/instrumental separation (async job flow)
# ============================================================

async def _run_separation_background(job_id: str, file_path: str, original_filename: str):
    """
    Runs after POST /separate has already returned a response to the
    caller - this is what makes the endpoint non-blocking. Acquires the
    separation semaphore itself (rather than the route holding it before
    returning), since the whole point is the HTTP response doesn't wait
    for this to finish.
    """
    async with _separation_semaphore:
        try:
            vocals_path, instrumental_path = await run_blocking(run_separation, file_path, job_id)
            mark_complete(job_id, original_filename, vocals_path, instrumental_path)
            logger.info(f"[SEPARATION] Job {job_id} finished successfully")
        except SeparationError as e:
            mark_failed(job_id, str(e))
            logger.warning(f"[SEPARATION] Job {job_id} failed: {e}")
        except Exception as e:
            mark_failed(job_id, "Separation failed unexpectedly.")
            logger.error(f"[SEPARATION] Job {job_id} failed unexpectedly: {e}", exc_info=True)
        finally:
            cleanup_file(file_path)
            release_memory_to_os()


@router.post(
    "/separate",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=SEPARATION_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=SEPARATION_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def separate_audio(file: UploadFile = File(...)):
    """
    Accepts an audio file, immediately returns a job_id, and runs the
    actual Demucs separation in the background - separation takes
    1-5+ minutes on CPU, far too long for a normal synchronous request.
    Poll GET /separate/status/{job_id} to track progress.
    """
    cleanup_expired_jobs()  # opportunistic sweep of old jobs/files

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(400, "Empty file")
    if len(content) > MAX_UPLOAD_BYTES:
        size_mb = len(content) / (1024 * 1024)
        raise HTTPException(400, f"File too large ({size_mb:.1f} MB). Maximum allowed size is {MAX_UPLOAD_BYTES // (1024*1024)} MB.")

    job_id = create_job()
    file_path = os.path.join(UPLOAD_DIR, f"{job_id}_{file.filename}")
    with open(file_path, "wb") as f:
        f.write(content)
    del content

    asyncio.create_task(_run_separation_background(job_id, file_path, file.filename))

    logger.info(f"[SEPARATION] Job {job_id} queued for '{file.filename}'")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/separate/status/{job_id}")
async def separation_status(job_id: str):
    job = get_job(job_id)
    if job is None:
        raise HTTPException(404, "Job not found (it may have expired).")
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
    }


def _resolve_stem_path(job_id: str, stem: str) -> str:
    if stem not in ("vocals", "instrumental"):
        raise HTTPException(400, "stem must be 'vocals' or 'instrumental'")
    job = get_job(job_id)
    if job is None:
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    path = job["vocals_path"] if stem == "vocals" else job["instrumental_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Stem file not found (it may have expired).")
    return path


@router.get("/separate/preview/{job_id}")
async def separation_preview(job_id: str, stem: str = Query(...)):
    """Streams the audio inline for in-browser <audio> playback (no
    Content-Disposition: attachment header, unlike /download below)."""
    path = _resolve_stem_path(job_id, stem)
    return FileResponse(path, media_type="audio/wav")


@router.get("/separate/download/{job_id}")
async def separation_download(job_id: str, stem: str = Query(...)):
    """Same file as /preview, served as a downloadable attachment
    instead of inline playback."""
    path = _resolve_stem_path(job_id, stem)
    filename = f"{stem}.wav"
    return FileResponse(path, media_type="audio/wav", filename=filename)


@router.get("/")
async def root():
    return {
        "status": "Audio Analysis API v13.4 - ESSENTIA FIXED + KEY/BPM CORRECTIONS + MONITORING + RATE LIMITING + DURATION CAP + PROXY FALLBACK + COOKIE ALERTS + GEO-RESTRICTION HANDLING + MULTI-ACCOUNT COOKIE ROTATION + SINGLE-PASS EXTRACTION + R2 CACHING + PERMANENT-ERROR 404 + VOCAL SEPARATION",
        "accuracy": "Essentia research-grade + relative major/minor correction + BPM octave correction + Librosa cross-check",
        "engine": "Essentia KeyExtractor + RhythmExtractor2013 + Demucs (separation)",
        "fixes": [
            "Removed invalid BPMHistogramDescriptors",
            "Proper BPM via RhythmExtractor2013 (confidence included)",
            "Robust fallback with enhanced Librosa",
            "Retry with exponential backoff on yt_dlp failures",
            "Permanent-error detection (video unavailable/private/removed) skips retries to save proxy bandwidth and fail fast",
            "Video duration cap now checked as part of a SINGLE extraction pass",
            "Clean 503 on YouTube bot verification / format restriction instead of raw error",
            "Clean 451 on geo-restricted videos",
            "Guaranteed temp file cleanup via finally blocks",
            "Explicit memory freeing + gc.collect() + malloc_trim() after each request",
            "Audio trimmed to first 180s before analysis to cap peak memory",
            "50MB upload size limit",
            "Relative major/minor correction using bass-register chroma energy",
            "BPM half/double (octave) error correction against a typical tempo range",
            "Librosa cross-check adjusts confidence without overriding Essentia's answer",
            "All blocking work offloaded to a thread pool so it never freezes the event loop",
            "Concurrency capped via semaphores to bound peak memory",
            "Requests queue for a free slot up to QUEUE_WAIT_TIMEOUT_SECONDS, then return a clean 503",
            "Broadened yt_dlp player_client list to reduce format failures",
            "cookies.txt reconstructed at startup from base64 Railway env var",
            "Per-request [COOKIES]/[PROXY] status log line",
            "Tiered download strategy: direct+cookies first, proxy retry only on bot-check errors",
            "Proxy circuit breaker with immediate webhook alert",
            "Cookie-expiry Discord alert (throttled)",
            "Multi-account cookie rotation (up to 3 sessions)",
            "CORS locked to ALLOWED_ORIGINS",
            "Per-IP rate limiting on /download, /analyze, and /separate",
            "Failure-spike monitoring with optional webhook alerting",
            "R2 caching for repeat download requests",
            "Clean 404 on permanently unavailable videos",
            "Async Demucs vocal/instrumental separation with job polling, local-disk stem storage, and TTL cleanup",
        ]
    }


@router.get("/health")
async def health():
    return {"status": "healthy"}


@router.get("/admin/status")
async def admin_status(key: str = Query(...)):
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
    if key != ADMIN_STATUS_KEY:
        raise HTTPException(403, "Invalid admin key")
    reset_proxy_circuit_breaker()
    return {"status": "proxy circuit breaker reset"}