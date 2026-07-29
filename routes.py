"""
routes.py - The two APIs (download, analyze) plus root/health/admin,
plus the /separate (vocal remover) endpoints, the audio-tools group:
/convert (format conversion), /trim (cut), /volume (gain boost/
reduction), /pitch (pitch shift), /tempo (tempo/speed change), /reverse
(reverse playback), /noise-remove (background noise reduction),
/voice-clean (speech-optimized cleanup preset), /echo-remove
(echo/reverb tail suppression), and /silence-remove (strip silent gaps)
- and /speech-to-text (Whisper transcription), which sits apart from the
audio-tools group on its own semaphore and returns transcript JSON
rather than an audio file.
Each audio-tool exposes matching POST (submit), GET .../status, GET
.../preview (inline playback), and GET .../download routes;
/speech-to-text instead exposes POST, GET .../status, and
GET .../result.
All business logic lives in youtube.py / audio_analysis.py / utils.py /
separation.py / audio_converter.py / audio_cutter.py / volume_booster.py /
pitch_changer.py / tempo_changer.py / reverse_audio.py / noise_remover.py /
voice_cleaner.py / echo_remover.py / silence_remover.py / speech_to_text.py
- this file just wires HTTP in and out.
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
    SEPARATION_RATE_LIMIT_MAX_REQUESTS,
    SEPARATION_RATE_LIMIT_WINDOW_SECONDS,
    MAX_CONCURRENT_SEPARATIONS,
    MAX_CONCURRENT_AUDIO_TOOLS,
    AUDIO_CONVERT_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_CONVERT_RATE_LIMIT_WINDOW_SECONDS,
    AUDIO_CONVERSION_MATRIX,
    AUDIO_TRIM_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_TRIM_RATE_LIMIT_WINDOW_SECONDS,
    AUDIO_VOLUME_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_VOLUME_RATE_LIMIT_WINDOW_SECONDS,
    VOLUME_GAIN_MIN_DB,
    VOLUME_GAIN_MAX_DB,
    AUDIO_PITCH_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_PITCH_RATE_LIMIT_WINDOW_SECONDS,
    PITCH_SHIFT_MIN_SEMITONES,
    PITCH_SHIFT_MAX_SEMITONES,
    AUDIO_TEMPO_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_TEMPO_RATE_LIMIT_WINDOW_SECONDS,
    TEMPO_MIN_FACTOR,
    TEMPO_MAX_FACTOR,
    AUDIO_REVERSE_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_REVERSE_RATE_LIMIT_WINDOW_SECONDS,
    AUDIO_NOISE_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_NOISE_RATE_LIMIT_WINDOW_SECONDS,
    NOISE_REDUCTION_MIN_STRENGTH,
    NOISE_REDUCTION_MAX_STRENGTH,
    AUDIO_VOICE_CLEAN_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_VOICE_CLEAN_RATE_LIMIT_WINDOW_SECONDS,
    AUDIO_ECHO_REMOVE_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_ECHO_REMOVE_RATE_LIMIT_WINDOW_SECONDS,
    AUDIO_SILENCE_REMOVE_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_SILENCE_REMOVE_RATE_LIMIT_WINDOW_SECONDS,
    SILENCE_THRESHOLD_MIN_DB,
    SILENCE_THRESHOLD_MAX_DB,
    SILENCE_MIN_DURATION_SECONDS,
    SILENCE_MAX_DURATION_SECONDS,
    MAX_CONCURRENT_TRANSCRIPTIONS,
    TRANSCRIPTION_JOB_TTL_SECONDS,
    MAX_TRANSCRIPTION_DURATION_SECONDS,
    AUDIO_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS,
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
from cache import get_cached_audio, put_cached_audio, get_cache_stats, clear_cache, set_cache_max_gb
from monitoring import record_result, get_status_snapshot
from download_progress import make_progress_hook
from jobs import (
    create_job,
    mark_complete,
    mark_tool_complete,
    mark_transcription_complete,
    mark_failed,
    get_job,
    cleanup_expired_jobs,
)
from separation import run_separation, SeparationError
from audio_common import (
    validate_input_format,
    validate_conversion_pair,
    build_temp_input_path,
    build_output_path,
    AudioToolError,
    validate_duration,
    get_audio_mime_type,
)
from audio_converter import convert_audio
from audio_cutter import trim_audio
from volume_booster import apply_volume_gain
from pitch_changer import shift_pitch
from tempo_changer import change_tempo
from reverse_audio import reverse_audio
from noise_remover import remove_noise
from voice_cleaner import clean_voice
from echo_remover import remove_echo
from silence_remover import remove_silence
from speech_to_text import transcribe

router = APIRouter()

# One dedicated semaphore for separation, same pattern as
# _analysis_semaphore / _download_semaphore in utils.py - caps how many
# Demucs subprocesses can run at once (default 1, since it's the most
# RAM-hungry endpoint in this app).
_separation_semaphore = asyncio.Semaphore(MAX_CONCURRENT_SEPARATIONS)

# Dedicated semaphore for the /convert (ffmpeg) audio-tools job flow.
_audio_tools_semaphore = asyncio.Semaphore(MAX_CONCURRENT_AUDIO_TOOLS)


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

        audio_array = None
        try:
            key, scale, key_conf, bpm, bpm_conf, audio_array, essentia_sr = await run_blocking(
                detect_key_bpm_essentia, analysis_path
            )

            key, scale, key_conf, bpm, bpm_conf, agreement = await run_blocking(
                cross_check_with_librosa, audio_array, essentia_sr, key, scale, key_conf, bpm, bpm_conf
            )
        finally:
            if audio_array is not None:
                del audio_array
            release_memory_to_os()

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
    succeeded = False
    async with _separation_semaphore:
        try:
            vocals_path, instrumental_path = await run_blocking(run_separation, file_path, job_id)
            mark_complete(job_id, original_filename, vocals_path, instrumental_path)
            logger.info(f"[SEPARATION] Job {job_id} finished successfully")
            succeeded = True
        except SeparationError as e:
            mark_failed(job_id, str(e))
            logger.warning(f"[SEPARATION] Job {job_id} failed: {e}")
        except Exception as e:
            mark_failed(job_id, "Separation failed unexpectedly.")
            logger.error(f"[SEPARATION] Job {job_id} failed unexpectedly: {e}", exc_info=True)
        finally:
            cleanup_file(file_path)
            release_memory_to_os()
            record_result("/separate", succeeded)


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


# ============================================================
# Shared helper for every audio-tool's preview/download routes below
# (convert, trim, volume, pitch, tempo, reverse, noise-remove). Each
# tool's preview route sits inline with its own status/download routes
# rather than being grouped in one block at the end of the file.
# ============================================================

def _resolve_tool_output_path(job_id: str, expected_type: str) -> tuple[str, str]:
    """Shared lookup for every tool's preview/download routes. Returns
    (path, output_format). Raises HTTPException on any failure state,
    same error semantics already used by each tool's individual
    download route."""
    job = get_job(job_id)
    if job is None or job["job_type"] != expected_type:
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    path = job["output_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Output file not found (it may have expired).")
    return path, (job.get("output_format") or "bin")


# ============================================================
# /convert - Audio format conversion (async job flow)
# ============================================================

async def _run_convert_background(job_id: str, input_path: str, output_path: str,
                                    source_format: str, target_format: str, original_filename: str):
    succeeded = False
    async with _audio_tools_semaphore:
        try:
            await run_blocking(convert_audio, input_path, output_path, source_format, target_format)
            mark_tool_complete(job_id, original_filename, output_path, target_format)
            logger.info(f"[CONVERT] Job {job_id} finished successfully")
            succeeded = True
        except AudioToolError as e:
            mark_failed(job_id, str(e))
            logger.warning(f"[CONVERT] Job {job_id} failed: {e}")
        except Exception as e:
            mark_failed(job_id, "Conversion failed unexpectedly.")
            logger.error(f"[CONVERT] Job {job_id} failed unexpectedly: {e}", exc_info=True)
        finally:
            cleanup_file(input_path)
            release_memory_to_os()
            record_result("/convert", succeeded)


@router.post(
    "/convert",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_CONVERT_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_CONVERT_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def convert_audio_route(file: UploadFile = File(...), target_format: str = Form(...)):
    """
    Accepts an audio file + target_format, returns a job_id immediately,
    runs the actual ffmpeg conversion in the background. Poll
    GET /convert/status/{job_id} to track progress.
    """
    cleanup_expired_jobs()

    target_format = target_format.strip().lower()
    source_format = validate_input_format(file.filename)
    validate_conversion_pair(source_format, target_format, AUDIO_CONVERSION_MATRIX)

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(400, "Empty file")
    if len(content) > MAX_UPLOAD_BYTES:
        size_mb = len(content) / (1024 * 1024)
        raise HTTPException(400, f"File too large ({size_mb:.1f} MB). Maximum allowed size is {MAX_UPLOAD_BYTES // (1024*1024)} MB.")

    job_id = create_job(job_type="convert")
    input_path = build_temp_input_path(job_id, file.filename)
    output_path = build_output_path(job_id, target_format)
    with open(input_path, "wb") as f:
        f.write(content)
    del content

    asyncio.create_task(_run_convert_background(job_id, input_path, output_path, source_format, target_format, file.filename))

    logger.info(f"[CONVERT] Job {job_id} queued: '{file.filename}' ({source_format} -> {target_format})")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/convert/status/{job_id}")
async def convert_status(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "convert":
        raise HTTPException(404, "Job not found (it may have expired).")
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
    }


@router.get("/convert/preview/{job_id}")
async def convert_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "convert")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/convert/download/{job_id}")
async def convert_download(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "convert":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    path = job["output_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Output file not found (it may have expired).")
    ext = job.get("output_format") or "bin"
    return FileResponse(path, media_type="application/octet-stream", filename=f"converted.{ext}")


# ============================================================
# /trim - Audio cut/trim to a start-end range (async job flow)
# ============================================================

async def _run_trim_background(job_id: str, input_path: str, output_path: str,
                                  start_seconds: float, end_seconds: float, duration: float,
                                  original_filename: str, source_format: str):
    succeeded = False
    async with _audio_tools_semaphore:
        try:
            await run_blocking(trim_audio, input_path, output_path, start_seconds, end_seconds, duration)
            mark_tool_complete(job_id, original_filename, output_path, source_format)
            logger.info(f"[TRIM] Job {job_id} finished successfully")
            succeeded = True
        except AudioToolError as e:
            mark_failed(job_id, str(e))
            logger.warning(f"[TRIM] Job {job_id} failed: {e}")
        except Exception as e:
            mark_failed(job_id, "Trim failed unexpectedly.")
            logger.error(f"[TRIM] Job {job_id} failed unexpectedly: {e}", exc_info=True)
        finally:
            cleanup_file(input_path)
            release_memory_to_os()
            record_result("/trim", succeeded)


@router.post(
    "/trim",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_TRIM_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_TRIM_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def trim_audio_route(
    file: UploadFile = File(...),
    start_seconds: float = Form(...),
    end_seconds: float = Form(...),
):
    """
    Accepts an audio file + start/end range, returns a job_id
    immediately, runs the ffmpeg trim in the background. Poll
    GET /trim/status/{job_id} to track progress.
    """
    cleanup_expired_jobs()

    source_format = validate_input_format(file.filename)

    if start_seconds < 0 or end_seconds <= start_seconds:
        raise HTTPException(400, "Invalid range: end_seconds must be greater than start_seconds, and both must be non-negative.")

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(400, "Empty file")
    if len(content) > MAX_UPLOAD_BYTES:
        size_mb = len(content) / (1024 * 1024)
        raise HTTPException(400, f"File too large ({size_mb:.1f} MB). Maximum allowed size is {MAX_UPLOAD_BYTES // (1024*1024)} MB.")

    job_id = create_job(job_type="trim")
    input_path = build_temp_input_path(job_id, file.filename)
    output_path = build_output_path(job_id, source_format)
    with open(input_path, "wb") as f:
        f.write(content)
    del content

    # Duration validated here (post-write, since ffprobe needs the file
    # on disk) rather than in the background task - lets us reject an
    # out-of-range end_seconds with a synchronous 400 instead of a job
    # that immediately fails, giving the frontend a faster/cleaner error.
    try:
        duration = validate_duration(input_path)
    except AudioToolError as e:
        cleanup_file(input_path)
        raise HTTPException(400, str(e))

    if end_seconds > duration:
        cleanup_file(input_path)
        raise HTTPException(400, f"end_seconds ({end_seconds}s) exceeds the audio's actual duration ({duration:.1f}s).")

    asyncio.create_task(_run_trim_background(job_id, input_path, output_path, start_seconds, end_seconds, duration, file.filename, source_format))

    logger.info(f"[TRIM] Job {job_id} queued: '{file.filename}' [{start_seconds}s -> {end_seconds}s]")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/trim/status/{job_id}")
async def trim_status(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "trim":
        raise HTTPException(404, "Job not found (it may have expired).")
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
    }


@router.get("/trim/preview/{job_id}")
async def trim_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "trim")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/trim/download/{job_id}")
async def trim_download(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "trim":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    path = job["output_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Output file not found (it may have expired).")
    ext = job.get("output_format") or "bin"
    return FileResponse(path, media_type="application/octet-stream", filename=f"trimmed.{ext}")


# ============================================================
# /volume - Audio gain boost/reduction (async job flow)
# ============================================================

async def _run_volume_background(job_id: str, input_path: str, output_path: str,
                                    gain_db: float, original_filename: str, source_format: str):
    succeeded = False
    async with _audio_tools_semaphore:
        try:
            await run_blocking(apply_volume_gain, input_path, output_path, gain_db)
            mark_tool_complete(job_id, original_filename, output_path, source_format)
            logger.info(f"[VOLUME] Job {job_id} finished successfully")
            succeeded = True
        except AudioToolError as e:
            mark_failed(job_id, str(e))
            logger.warning(f"[VOLUME] Job {job_id} failed: {e}")
        except Exception as e:
            mark_failed(job_id, "Volume adjustment failed unexpectedly.")
            logger.error(f"[VOLUME] Job {job_id} failed unexpectedly: {e}", exc_info=True)
        finally:
            cleanup_file(input_path)
            release_memory_to_os()
            record_result("/volume", succeeded)


@router.post(
    "/volume",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_VOLUME_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_VOLUME_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def volume_route(file: UploadFile = File(...), gain_db: float = Form(...)):
    """
    Accepts an audio file + gain_db, returns a job_id immediately, runs
    the ffmpeg volume adjustment in the background. Poll
    GET /volume/status/{job_id} to track progress.
    """
    cleanup_expired_jobs()

    source_format = validate_input_format(file.filename)

    if gain_db < VOLUME_GAIN_MIN_DB or gain_db > VOLUME_GAIN_MAX_DB:
        raise HTTPException(400, f"gain_db must be between {VOLUME_GAIN_MIN_DB} and {VOLUME_GAIN_MAX_DB}.")

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(400, "Empty file")
    if len(content) > MAX_UPLOAD_BYTES:
        size_mb = len(content) / (1024 * 1024)
        raise HTTPException(400, f"File too large ({size_mb:.1f} MB). Maximum allowed size is {MAX_UPLOAD_BYTES // (1024*1024)} MB.")

    job_id = create_job(job_type="volume")
    input_path = build_temp_input_path(job_id, file.filename)
    output_path = build_output_path(job_id, source_format)
    with open(input_path, "wb") as f:
        f.write(content)
    del content

    try:
        validate_duration(input_path)
    except AudioToolError as e:
        cleanup_file(input_path)
        raise HTTPException(400, str(e))

    asyncio.create_task(_run_volume_background(job_id, input_path, output_path, gain_db, file.filename, source_format))

    logger.info(f"[VOLUME] Job {job_id} queued: '{file.filename}' ({gain_db:+.1f}dB)")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/volume/status/{job_id}")
async def volume_status(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "volume":
        raise HTTPException(404, "Job not found (it may have expired).")
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
    }


@router.get("/volume/preview/{job_id}")
async def volume_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "volume")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/volume/download/{job_id}")
async def volume_download(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "volume":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    path = job["output_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Output file not found (it may have expired).")
    ext = job.get("output_format") or "bin"
    return FileResponse(path, media_type="application/octet-stream", filename=f"volume_adjusted.{ext}")


# ============================================================
# /pitch - Pitch shift, independent of tempo (async job flow)
# ============================================================

async def _run_pitch_background(job_id: str, input_path: str, output_path: str,
                                    semitones: float, original_filename: str, source_format: str):
    succeeded = False
    async with _audio_tools_semaphore:
        try:
            await run_blocking(shift_pitch, input_path, output_path, semitones)
            mark_tool_complete(job_id, original_filename, output_path, source_format)
            logger.info(f"[PITCH] Job {job_id} finished successfully")
            succeeded = True
        except AudioToolError as e:
            mark_failed(job_id, str(e))
            logger.warning(f"[PITCH] Job {job_id} failed: {e}")
        except Exception as e:
            mark_failed(job_id, "Pitch shift failed unexpectedly.")
            logger.error(f"[PITCH] Job {job_id} failed unexpectedly: {e}", exc_info=True)
        finally:
            cleanup_file(input_path)
            release_memory_to_os()
            record_result("/pitch", succeeded)


@router.post(
    "/pitch",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_PITCH_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_PITCH_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def pitch_route(file: UploadFile = File(...), semitones: float = Form(...)):
    """
    Accepts an audio file + semitones, returns a job_id immediately,
    runs the rubberband pitch shift in the background. Poll
    GET /pitch/status/{job_id} to track progress.
    """
    cleanup_expired_jobs()

    source_format = validate_input_format(file.filename)

    if semitones < PITCH_SHIFT_MIN_SEMITONES or semitones > PITCH_SHIFT_MAX_SEMITONES:
        raise HTTPException(400, f"semitones must be between {PITCH_SHIFT_MIN_SEMITONES} and {PITCH_SHIFT_MAX_SEMITONES}.")

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(400, "Empty file")
    if len(content) > MAX_UPLOAD_BYTES:
        size_mb = len(content) / (1024 * 1024)
        raise HTTPException(400, f"File too large ({size_mb:.1f} MB). Maximum allowed size is {MAX_UPLOAD_BYTES // (1024*1024)} MB.")

    job_id = create_job(job_type="pitch")
    input_path = build_temp_input_path(job_id, file.filename)
    output_path = build_output_path(job_id, source_format)
    with open(input_path, "wb") as f:
        f.write(content)
    del content

    try:
        validate_duration(input_path)
    except AudioToolError as e:
        cleanup_file(input_path)
        raise HTTPException(400, str(e))

    asyncio.create_task(_run_pitch_background(job_id, input_path, output_path, semitones, file.filename, source_format))

    logger.info(f"[PITCH] Job {job_id} queued: '{file.filename}' ({semitones:+.1f} semitones)")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/pitch/status/{job_id}")
async def pitch_status(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "pitch":
        raise HTTPException(404, "Job not found (it may have expired).")
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
    }


@router.get("/pitch/preview/{job_id}")
async def pitch_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "pitch")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/pitch/download/{job_id}")
async def pitch_download(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "pitch":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    path = job["output_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Output file not found (it may have expired).")
    ext = job.get("output_format") or "bin"
    return FileResponse(path, media_type="application/octet-stream", filename=f"pitch_shifted.{ext}")


# ============================================================
# /tempo - Tempo/speed change, independent of pitch (async job flow)
# ============================================================

async def _run_tempo_background(job_id: str, input_path: str, output_path: str,
                                    tempo_factor: float, original_filename: str, source_format: str):
    succeeded = False
    async with _audio_tools_semaphore:
        try:
            await run_blocking(change_tempo, input_path, output_path, tempo_factor)
            mark_tool_complete(job_id, original_filename, output_path, source_format)
            logger.info(f"[TEMPO] Job {job_id} finished successfully")
            succeeded = True
        except AudioToolError as e:
            mark_failed(job_id, str(e))
            logger.warning(f"[TEMPO] Job {job_id} failed: {e}")
        except Exception as e:
            mark_failed(job_id, "Tempo change failed unexpectedly.")
            logger.error(f"[TEMPO] Job {job_id} failed unexpectedly: {e}", exc_info=True)
        finally:
            cleanup_file(input_path)
            release_memory_to_os()
            record_result("/tempo", succeeded)


@router.post(
    "/tempo",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_TEMPO_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_TEMPO_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def tempo_route(file: UploadFile = File(...), tempo_factor: float = Form(...)):
    """
    Accepts an audio file + tempo_factor, returns a job_id immediately,
    runs the rubberband tempo change in the background. Poll
    GET /tempo/status/{job_id} to track progress.
    """
    cleanup_expired_jobs()

    source_format = validate_input_format(file.filename)

    if tempo_factor < TEMPO_MIN_FACTOR or tempo_factor > TEMPO_MAX_FACTOR:
        raise HTTPException(400, f"tempo_factor must be between {TEMPO_MIN_FACTOR} and {TEMPO_MAX_FACTOR}.")

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(400, "Empty file")
    if len(content) > MAX_UPLOAD_BYTES:
        size_mb = len(content) / (1024 * 1024)
        raise HTTPException(400, f"File too large ({size_mb:.1f} MB). Maximum allowed size is {MAX_UPLOAD_BYTES // (1024*1024)} MB.")

    job_id = create_job(job_type="tempo")
    input_path = build_temp_input_path(job_id, file.filename)
    output_path = build_output_path(job_id, source_format)
    with open(input_path, "wb") as f:
        f.write(content)
    del content

    try:
        validate_duration(input_path)
    except AudioToolError as e:
        cleanup_file(input_path)
        raise HTTPException(400, str(e))

    asyncio.create_task(_run_tempo_background(job_id, input_path, output_path, tempo_factor, file.filename, source_format))

    logger.info(f"[TEMPO] Job {job_id} queued: '{file.filename}' (x{tempo_factor:.2f})")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/tempo/status/{job_id}")
async def tempo_status(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "tempo":
        raise HTTPException(404, "Job not found (it may have expired).")
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
    }


@router.get("/tempo/preview/{job_id}")
async def tempo_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "tempo")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/tempo/download/{job_id}")
async def tempo_download(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "tempo":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    path = job["output_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Output file not found (it may have expired).")
    ext = job.get("output_format") or "bin"
    return FileResponse(path, media_type="application/octet-stream", filename=f"tempo_changed.{ext}")


# ============================================================
# /reverse - Reverse audio playback (async job flow)
#
# This is the last of the audio-tools group sharing
# _audio_tools_semaphore (convert, trim, volume, pitch, tempo, reverse).
# ============================================================

async def _run_reverse_background(job_id: str, input_path: str, output_path: str,
                                     original_filename: str, source_format: str):
    succeeded = False
    async with _audio_tools_semaphore:
        try:
            await run_blocking(reverse_audio, input_path, output_path)
            mark_tool_complete(job_id, original_filename, output_path, source_format)
            logger.info(f"[REVERSE] Job {job_id} finished successfully")
            succeeded = True
        except AudioToolError as e:
            mark_failed(job_id, str(e))
            logger.warning(f"[REVERSE] Job {job_id} failed: {e}")
        except Exception as e:
            mark_failed(job_id, "Reverse failed unexpectedly.")
            logger.error(f"[REVERSE] Job {job_id} failed unexpectedly: {e}", exc_info=True)
        finally:
            cleanup_file(input_path)
            release_memory_to_os()
            record_result("/reverse", succeeded)


@router.post(
    "/reverse",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_REVERSE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_REVERSE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def reverse_route(file: UploadFile = File(...)):
    """
    Accepts an audio file, returns a job_id immediately, runs the
    ffmpeg reverse in the background. Poll GET /reverse/status/{job_id}
    to track progress.
    """
    cleanup_expired_jobs()

    source_format = validate_input_format(file.filename)

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(400, "Empty file")
    if len(content) > MAX_UPLOAD_BYTES:
        size_mb = len(content) / (1024 * 1024)
        raise HTTPException(400, f"File too large ({size_mb:.1f} MB). Maximum allowed size is {MAX_UPLOAD_BYTES // (1024*1024)} MB.")

    job_id = create_job(job_type="reverse")
    input_path = build_temp_input_path(job_id, file.filename)
    output_path = build_output_path(job_id, source_format)
    with open(input_path, "wb") as f:
        f.write(content)
    del content

    try:
        validate_duration(input_path)
    except AudioToolError as e:
        cleanup_file(input_path)
        raise HTTPException(400, str(e))

    asyncio.create_task(_run_reverse_background(job_id, input_path, output_path, file.filename, source_format))

    logger.info(f"[REVERSE] Job {job_id} queued: '{file.filename}'")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/reverse/status/{job_id}")
async def reverse_status(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "reverse":
        raise HTTPException(404, "Job not found (it may have expired).")
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
    }


@router.get("/reverse/preview/{job_id}")
async def reverse_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "reverse")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/reverse/download/{job_id}")
async def reverse_download(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "reverse":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    path = job["output_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Output file not found (it may have expired).")
    ext = job.get("output_format") or "bin"
    return FileResponse(path, media_type="application/octet-stream", filename=f"reversed.{ext}")


# ============================================================
# /noise-remove - Background noise reduction (async job flow)
# ============================================================

async def _run_noise_background(job_id: str, input_path: str, output_path: str,
                                    strength: float, original_filename: str, source_format: str):
    succeeded = False
    async with _audio_tools_semaphore:
        try:
            await run_blocking(remove_noise, input_path, output_path, strength)
            mark_tool_complete(job_id, original_filename, output_path, source_format)
            logger.info(f"[NOISE] Job {job_id} finished successfully")
            succeeded = True
        except AudioToolError as e:
            mark_failed(job_id, str(e))
            logger.warning(f"[NOISE] Job {job_id} failed: {e}")
        except Exception as e:
            mark_failed(job_id, "Noise removal failed unexpectedly.")
            logger.error(f"[NOISE] Job {job_id} failed unexpectedly: {e}", exc_info=True)
        finally:
            cleanup_file(input_path)
            release_memory_to_os()
            record_result("/noise-remove", succeeded)


@router.post(
    "/noise-remove",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_NOISE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_NOISE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def noise_remove_route(file: UploadFile = File(...), strength: float = Form(12.0)):
    """
    Accepts an audio file + optional strength, returns a job_id
    immediately, runs the ffmpeg denoiser in the background. Poll
    GET /noise-remove/status/{job_id} to track progress.
    """
    cleanup_expired_jobs()

    source_format = validate_input_format(file.filename)

    if strength < NOISE_REDUCTION_MIN_STRENGTH or strength > NOISE_REDUCTION_MAX_STRENGTH:
        raise HTTPException(400, f"strength must be between {NOISE_REDUCTION_MIN_STRENGTH} and {NOISE_REDUCTION_MAX_STRENGTH}.")

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(400, "Empty file")
    if len(content) > MAX_UPLOAD_BYTES:
        size_mb = len(content) / (1024 * 1024)
        raise HTTPException(400, f"File too large ({size_mb:.1f} MB). Maximum allowed size is {MAX_UPLOAD_BYTES // (1024*1024)} MB.")

    job_id = create_job(job_type="noise_remove")
    input_path = build_temp_input_path(job_id, file.filename)
    output_path = build_output_path(job_id, source_format)
    with open(input_path, "wb") as f:
        f.write(content)
    del content

    try:
        validate_duration(input_path)
    except AudioToolError as e:
        cleanup_file(input_path)
        raise HTTPException(400, str(e))

    asyncio.create_task(_run_noise_background(job_id, input_path, output_path, strength, file.filename, source_format))

    logger.info(f"[NOISE] Job {job_id} queued: '{file.filename}' (strength={strength})")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/noise-remove/status/{job_id}")
async def noise_remove_status(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "noise_remove":
        raise HTTPException(404, "Job not found (it may have expired).")
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
    }


@router.get("/noise-remove/preview/{job_id}")
async def noise_remove_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "noise_remove")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/noise-remove/download/{job_id}")
async def noise_remove_download(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "noise_remove":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    path = job["output_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Output file not found (it may have expired).")
    ext = job.get("output_format") or "bin"
    return FileResponse(path, media_type="application/octet-stream", filename=f"denoised.{ext}")


# ============================================================
# /voice-clean - Speech-optimized cleanup preset (async job flow)
# ============================================================

async def _run_voice_clean_background(job_id: str, input_path: str, output_path: str,
                                         original_filename: str, source_format: str):
    succeeded = False
    async with _audio_tools_semaphore:
        try:
            await run_blocking(clean_voice, input_path, output_path)
            mark_tool_complete(job_id, original_filename, output_path, source_format)
            logger.info(f"[VOICE_CLEAN] Job {job_id} finished successfully")
            succeeded = True
        except AudioToolError as e:
            mark_failed(job_id, str(e))
            logger.warning(f"[VOICE_CLEAN] Job {job_id} failed: {e}")
        except Exception as e:
            mark_failed(job_id, "Voice cleanup failed unexpectedly.")
            logger.error(f"[VOICE_CLEAN] Job {job_id} failed unexpectedly: {e}", exc_info=True)
        finally:
            cleanup_file(input_path)
            release_memory_to_os()
            record_result("/voice-clean", succeeded)


@router.post(
    "/voice-clean",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_VOICE_CLEAN_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_VOICE_CLEAN_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def voice_clean_route(file: UploadFile = File(...)):
    """
    Accepts an audio file, returns a job_id immediately, runs the
    speech-cleanup filter chain in the background. Poll
    GET /voice-clean/status/{job_id} to track progress.
    """
    cleanup_expired_jobs()

    source_format = validate_input_format(file.filename)

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(400, "Empty file")
    if len(content) > MAX_UPLOAD_BYTES:
        size_mb = len(content) / (1024 * 1024)
        raise HTTPException(400, f"File too large ({size_mb:.1f} MB). Maximum allowed size is {MAX_UPLOAD_BYTES // (1024*1024)} MB.")

    job_id = create_job(job_type="voice_clean")
    input_path = build_temp_input_path(job_id, file.filename)
    output_path = build_output_path(job_id, source_format)
    with open(input_path, "wb") as f:
        f.write(content)
    del content

    try:
        validate_duration(input_path)
    except AudioToolError as e:
        cleanup_file(input_path)
        raise HTTPException(400, str(e))

    asyncio.create_task(_run_voice_clean_background(job_id, input_path, output_path, file.filename, source_format))

    logger.info(f"[VOICE_CLEAN] Job {job_id} queued: '{file.filename}'")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/voice-clean/status/{job_id}")
async def voice_clean_status(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "voice_clean":
        raise HTTPException(404, "Job not found (it may have expired).")
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
    }


@router.get("/voice-clean/preview/{job_id}")
async def voice_clean_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "voice_clean")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/voice-clean/download/{job_id}")
async def voice_clean_download(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "voice_clean":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    path = job["output_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Output file not found (it may have expired).")
    ext = job.get("output_format") or "bin"
    return FileResponse(path, media_type="application/octet-stream", filename=f"voice_cleaned.{ext}")


# ============================================================
# /echo-remove - Echo/reverb tail suppression (async job flow)
# ============================================================

async def _run_echo_remove_background(job_id: str, input_path: str, output_path: str,
                                         original_filename: str, source_format: str):
    succeeded = False
    async with _audio_tools_semaphore:
        try:
            await run_blocking(remove_echo, input_path, output_path)
            mark_tool_complete(job_id, original_filename, output_path, source_format)
            logger.info(f"[ECHO_REMOVE] Job {job_id} finished successfully")
            succeeded = True
        except AudioToolError as e:
            mark_failed(job_id, str(e))
            logger.warning(f"[ECHO_REMOVE] Job {job_id} failed: {e}")
        except Exception as e:
            mark_failed(job_id, "Echo removal failed unexpectedly.")
            logger.error(f"[ECHO_REMOVE] Job {job_id} failed unexpectedly: {e}", exc_info=True)
        finally:
            cleanup_file(input_path)
            release_memory_to_os()
            record_result("/echo-remove", succeeded)


@router.post(
    "/echo-remove",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_ECHO_REMOVE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_ECHO_REMOVE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def echo_remove_route(file: UploadFile = File(...)):
    """
    Accepts an audio file, returns a job_id immediately, runs the
    echo-suppression filter chain in the background. Poll
    GET /echo-remove/status/{job_id} to track progress.
    """
    cleanup_expired_jobs()

    source_format = validate_input_format(file.filename)

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(400, "Empty file")
    if len(content) > MAX_UPLOAD_BYTES:
        size_mb = len(content) / (1024 * 1024)
        raise HTTPException(400, f"File too large ({size_mb:.1f} MB). Maximum allowed size is {MAX_UPLOAD_BYTES // (1024*1024)} MB.")

    job_id = create_job(job_type="echo_remove")
    input_path = build_temp_input_path(job_id, file.filename)
    output_path = build_output_path(job_id, source_format)
    with open(input_path, "wb") as f:
        f.write(content)
    del content

    try:
        validate_duration(input_path)
    except AudioToolError as e:
        cleanup_file(input_path)
        raise HTTPException(400, str(e))

    asyncio.create_task(_run_echo_remove_background(job_id, input_path, output_path, file.filename, source_format))

    logger.info(f"[ECHO_REMOVE] Job {job_id} queued: '{file.filename}'")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/echo-remove/status/{job_id}")
async def echo_remove_status(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "echo_remove":
        raise HTTPException(404, "Job not found (it may have expired).")
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
    }


@router.get("/echo-remove/preview/{job_id}")
async def echo_remove_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "echo_remove")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/echo-remove/download/{job_id}")
async def echo_remove_download(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "echo_remove":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    path = job["output_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Output file not found (it may have expired).")
    ext = job.get("output_format") or "bin"
    return FileResponse(path, media_type="application/octet-stream", filename=f"echo_removed.{ext}")


# ============================================================
# /silence-remove - Strip silent gaps throughout audio (async job flow)
# ============================================================

async def _run_silence_remove_background(job_id: str, input_path: str, output_path: str,
                                            threshold_db: float, min_duration_seconds: float,
                                            original_filename: str, source_format: str):
    succeeded = False
    async with _audio_tools_semaphore:
        try:
            await run_blocking(remove_silence, input_path, output_path, threshold_db, min_duration_seconds)
            mark_tool_complete(job_id, original_filename, output_path, source_format)
            logger.info(f"[SILENCE_REMOVE] Job {job_id} finished successfully")
            succeeded = True
        except AudioToolError as e:
            mark_failed(job_id, str(e))
            logger.warning(f"[SILENCE_REMOVE] Job {job_id} failed: {e}")
        except Exception as e:
            mark_failed(job_id, "Silence removal failed unexpectedly.")
            logger.error(f"[SILENCE_REMOVE] Job {job_id} failed unexpectedly: {e}", exc_info=True)
        finally:
            cleanup_file(input_path)
            release_memory_to_os()
            record_result("/silence-remove", succeeded)


@router.post(
    "/silence-remove",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_SILENCE_REMOVE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_SILENCE_REMOVE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def silence_remove_route(
    file: UploadFile = File(...),
    threshold_db: float = Form(-30.0),
    min_duration_seconds: float = Form(0.5),
):
    """
    Accepts an audio file + optional threshold/min-duration, returns a
    job_id immediately, runs the ffmpeg silence-strip in the
    background. Poll GET /silence-remove/status/{job_id} to track
    progress.
    """
    cleanup_expired_jobs()

    source_format = validate_input_format(file.filename)

    if threshold_db < SILENCE_THRESHOLD_MIN_DB or threshold_db > SILENCE_THRESHOLD_MAX_DB:
        raise HTTPException(400, f"threshold_db must be between {SILENCE_THRESHOLD_MIN_DB} and {SILENCE_THRESHOLD_MAX_DB}.")
    if min_duration_seconds < SILENCE_MIN_DURATION_SECONDS or min_duration_seconds > SILENCE_MAX_DURATION_SECONDS:
        raise HTTPException(400, f"min_duration_seconds must be between {SILENCE_MIN_DURATION_SECONDS} and {SILENCE_MAX_DURATION_SECONDS}.")

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(400, "Empty file")
    if len(content) > MAX_UPLOAD_BYTES:
        size_mb = len(content) / (1024 * 1024)
        raise HTTPException(400, f"File too large ({size_mb:.1f} MB). Maximum allowed size is {MAX_UPLOAD_BYTES // (1024*1024)} MB.")

    job_id = create_job(job_type="silence_remove")
    input_path = build_temp_input_path(job_id, file.filename)
    output_path = build_output_path(job_id, source_format)
    with open(input_path, "wb") as f:
        f.write(content)
    del content

    try:
        validate_duration(input_path)
    except AudioToolError as e:
        cleanup_file(input_path)
        raise HTTPException(400, str(e))

    asyncio.create_task(_run_silence_remove_background(
        job_id, input_path, output_path, threshold_db, min_duration_seconds, file.filename, source_format
    ))

    logger.info(f"[SILENCE_REMOVE] Job {job_id} queued: '{file.filename}' (threshold={threshold_db}dB, min_dur={min_duration_seconds}s)")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/silence-remove/status/{job_id}")
async def silence_remove_status(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "silence_remove":
        raise HTTPException(404, "Job not found (it may have expired).")
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
    }


@router.get("/silence-remove/preview/{job_id}")
async def silence_remove_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "silence_remove")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/silence-remove/download/{job_id}")
async def silence_remove_download(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "silence_remove":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    path = job["output_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Output file not found (it may have expired).")
    ext = job.get("output_format") or "bin"
    return FileResponse(path, media_type="application/octet-stream", filename=f"silence_removed.{ext}")


# ============================================================
# /speech-to-text - Audio transcription via faster-whisper (async job flow)
#
# Deliberately gated by its OWN semaphore, not _audio_tools_semaphore -
# Whisper inference is a heavy, sustained CPU+RAM operation fundamentally
# unlike a stateless ffmpeg subprocess (see speech_to_text.py's module
# docstring). Sharing the ffmpeg pool would let a slow transcription job
# starve fast, cheap operations like /volume or /trim of their slots.
#
# Also structurally different from every other tool above: no /preview
# route (there's no audio output to play back) and its GET result route
# is named /result, not /download, since it returns transcript JSON
# directly rather than an audio file.
# ============================================================

_transcription_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TRANSCRIPTIONS)


async def _run_transcription_background(job_id: str, input_path: str, original_filename: str):
    succeeded = False
    async with _transcription_semaphore:
        try:
            result = await run_blocking(transcribe, input_path)
            mark_transcription_complete(job_id, original_filename, result)
            logger.info(f"[SPEECH_TO_TEXT] Job {job_id} finished successfully")
            succeeded = True
        except AudioToolError as e:
            mark_failed(job_id, str(e))
            logger.warning(f"[SPEECH_TO_TEXT] Job {job_id} failed: {e}")
        except Exception as e:
            mark_failed(job_id, "Transcription failed unexpectedly.")
            logger.error(f"[SPEECH_TO_TEXT] Job {job_id} failed unexpectedly: {e}", exc_info=True)
        finally:
            cleanup_file(input_path)
            release_memory_to_os()
            record_result("/speech-to-text", succeeded)


@router.post(
    "/speech-to-text",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def speech_to_text_route(file: UploadFile = File(...)):
    """
    Accepts an audio file, returns a job_id immediately, runs Whisper
    transcription in the background. Poll
    GET /speech-to-text/status/{job_id}, then
    GET /speech-to-text/result/{job_id} once complete.
    """
    cleanup_expired_jobs()

    source_format = validate_input_format(file.filename)

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(400, "Empty file")
    if len(content) > MAX_UPLOAD_BYTES:
        size_mb = len(content) / (1024 * 1024)
        raise HTTPException(400, f"File too large ({size_mb:.1f} MB). Maximum allowed size is {MAX_UPLOAD_BYTES // (1024*1024)} MB.")

    job_id = create_job(job_type="transcribe", ttl_seconds=TRANSCRIPTION_JOB_TTL_SECONDS)
    input_path = build_temp_input_path(job_id, file.filename)
    with open(input_path, "wb") as f:
        f.write(content)
    del content

    try:
        validate_duration(input_path, max_seconds=MAX_TRANSCRIPTION_DURATION_SECONDS)
    except AudioToolError as e:
        cleanup_file(input_path)
        raise HTTPException(400, str(e))

    asyncio.create_task(_run_transcription_background(job_id, input_path, file.filename))

    logger.info(f"[SPEECH_TO_TEXT] Job {job_id} queued: '{file.filename}'")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/speech-to-text/status/{job_id}")
async def speech_to_text_status(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "transcribe":
        raise HTTPException(404, "Job not found (it may have expired).")
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
    }


@router.get("/speech-to-text/result/{job_id}")
async def speech_to_text_result(job_id: str):
    """
    Returns the transcript JSON directly - no file involved, unlike
    every other tool's /download route. This is the one endpoint in
    the whole audio-tools family that returns structured data instead
    of an audio blob.
    """
    job = get_job(job_id)
    if job is None or job["job_type"] != "transcribe":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    result = job.get("result_data")
    if not result:
        raise HTTPException(404, "Transcript not found (it may have expired).")
    return JSONResponse(result)

@router.post("/admin/clear-cache")
async def admin_clear_cache(key: str = Query(...)):
    if key != ADMIN_STATUS_KEY:
        raise HTTPException(403, "Invalid admin key")
    result = clear_cache()
    logger.info(f"[CACHE] Admin manually cleared cache: {result}")
    return {"status": "cache cleared", **result}

@router.post("/admin/cache/limit")
async def admin_set_cache_limit(key: str = Query(...), gb: float = Query(..., gt=0, le=1000)):
    if key != ADMIN_STATUS_KEY:
        raise HTTPException(403, "Invalid admin key")
    try:
        stats = set_cache_max_gb(gb)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"status": "updated", **stats}

@router.get("/")
async def root():
    return {
        "status": "Audio Analysis API v13.4 - ESSENTIA FIXED + KEY/BPM CORRECTIONS + MONITORING + RATE LIMITING + DURATION CAP + PROXY FALLBACK + COOKIE ALERTS + GEO-RESTRICTION HANDLING + MULTI-ACCOUNT COOKIE ROTATION + SINGLE-PASS EXTRACTION + LOCAL-DISK CACHING + PERMANENT-ERROR 404 + VOCAL SEPARATION + AUDIO CONVERSION + AUDIO TRIM + VOLUME ADJUSTMENT + PITCH SHIFT + TEMPO CHANGE + AUDIO REVERSE + NOISE REDUCTION + VOICE CLEANUP + ECHO REMOVAL + SILENCE REMOVAL + SPEECH-TO-TEXT TRANSCRIPTION",
        "accuracy": "Essentia research-grade + relative major/minor correction + BPM octave correction + Librosa cross-check",
        "engine": "Essentia KeyExtractor + RhythmExtractor2013 + Demucs (separation) + ffmpeg (conversion, trim, volume, reverse, noise reduction, voice cleanup, echo removal, silence removal) + rubberband (pitch, tempo) + faster-whisper (transcription)",
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
            "Per-IP rate limiting on /download, /analyze, /separate, /convert, /trim, /volume, /pitch, /tempo, /reverse, /noise-remove, /voice-clean, /echo-remove, /silence-remove, and /speech-to-text",
            "Failure-spike monitoring with optional webhook alerting",
            "Local-disk caching for repeat download requests",
            "Clean 404 on permanently unavailable videos",
            "Async Demucs vocal/instrumental separation with job polling, local-disk stem storage, and TTL cleanup",
            "Async ffmpeg audio format conversion with job polling and TTL cleanup",
            "Async ffmpeg audio trim/cut with pre-flight duration validation and TTL cleanup",
            "Async ffmpeg volume gain boost/reduction with bounds-checked gain_db and TTL cleanup",
            "Async rubberband pitch shift (independent of tempo) with bounds-checked semitones and TTL cleanup",
            "Async rubberband tempo/speed change (independent of pitch) with bounds-checked tempo_factor and TTL cleanup",
            "Async ffmpeg audio reverse with pre-flight duration validation and TTL cleanup",
            "Async ffmpeg background noise reduction with bounds-checked strength and TTL cleanup",
            "Async speech-optimized cleanup preset (voice-clean) with pre-flight duration validation and TTL cleanup",
            "Async echo/reverb tail suppression (echo-remove) with pre-flight duration validation and TTL cleanup",
            "Async ffmpeg silence-gap stripping with bounds-checked threshold_db/min_duration_seconds and TTL cleanup",
            "Inline <audio> preview endpoints (correct MIME per format) alongside every audio-tool's own status/download routes, matching the /separate/preview pattern",
            "Async faster-whisper speech-to-text transcription on its own dedicated semaphore (isolated from the ffmpeg/rubberband pool) with a longer job TTL and JSON result endpoint",
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
        "enabled": True,
        "backend": "local-disk",
        **get_cache_stats(),
    }
    return snapshot


@router.post("/admin/reset-proxy")
async def admin_reset_proxy(key: str = Query(...)):
    if key != ADMIN_STATUS_KEY:
        raise HTTPException(403, "Invalid admin key")
    reset_proxy_circuit_breaker()
    return {"status": "proxy circuit breaker reset"}