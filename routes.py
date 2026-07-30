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
from typing import List
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
    SEPARATION_MODEL,
    SEPARATION_OVERLAP,
    DEMUCS_TIMEOUT_SECONDS,
    MAX_SEPARATION_DURATION_SECONDS,
    SEPARATION_MODEL_HQ,
    SEPARATION_OVERLAP_HQ,
    DEMUCS_TIMEOUT_SECONDS_HQ,
    MAX_SEPARATION_DURATION_SECONDS_HQ,
    SEPARATION_HQ_ENABLED,
    SEPARATION_HQ_RATE_LIMIT_MAX_REQUESTS,
    SEPARATION_HQ_RATE_LIMIT_WINDOW_SECONDS,
    STEMS_RATE_LIMIT_MAX_REQUESTS,
    STEMS_RATE_LIMIT_WINDOW_SECONDS,
    STEMS_HQ_RATE_LIMIT_MAX_REQUESTS,
    STEMS_HQ_RATE_LIMIT_WINDOW_SECONDS,
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
    ALLOWED_AUDIO_INPUT_FORMATS,
    MAX_VIDEO_UPLOAD_BYTES,
    VIDEO_TO_AUDIO_RATE_LIMIT_MAX_REQUESTS,
    VIDEO_TO_AUDIO_RATE_LIMIT_WINDOW_SECONDS,
    JOIN_MAX_FILES,
    JOIN_MAX_TOTAL_BYTES,
    JOIN_RATE_LIMIT_MAX_REQUESTS,
    JOIN_RATE_LIMIT_WINDOW_SECONDS,
    LOUDNORM_RATE_LIMIT_MAX_REQUESTS,
    LOUDNORM_RATE_LIMIT_WINDOW_SECONDS,
    SILENCE_SPLIT_RATE_LIMIT_MAX_REQUESTS,
    SILENCE_SPLIT_RATE_LIMIT_WINDOW_SECONDS,
    YOUTUBE_CHAIN_RATE_LIMIT_MAX_REQUESTS,
    YOUTUBE_CHAIN_RATE_LIMIT_WINDOW_SECONDS,
    YOUTUBE_ANALYZE_JOB_TTL_SECONDS,
    FADE_MAX_SECONDS,
    FADE_RATE_LIMIT_MAX_REQUESTS,
    FADE_RATE_LIMIT_WINDOW_SECONDS,
    CHANNELS_RATE_LIMIT_MAX_REQUESTS,
    CHANNELS_RATE_LIMIT_WINDOW_SECONDS,
    RESAMPLE_ALLOWED_RATES,
    RESAMPLE_ALLOWED_BIT_DEPTHS,
    RESAMPLE_RATE_LIMIT_MAX_REQUESTS,
    RESAMPLE_RATE_LIMIT_WINDOW_SECONDS,
    RINGTONE_MAX_DURATION_SECONDS,
    RINGTONE_RATE_LIMIT_MAX_REQUESTS,
    RINGTONE_RATE_LIMIT_WINDOW_SECONDS,
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
    is_age_restricted_error,
    is_members_only_error,
    is_not_yet_live_error,
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
    mark_stems_complete,
    mark_tool_complete,
    mark_transcription_complete,
    mark_data_complete,
    mark_failed,
    get_job,
    cleanup_expired_jobs,
)
from separation import run_separation, run_stem_separation, SeparationError
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
from video_to_audio import extract_audio, validate_video_input_format
from audio_joiner import join_audio
from audio_loudnorm import normalize_loudness, resolve_target_lufs
from silence_splitter import split_on_silence
from youtube_chain import download_audio_to_file, ChainDownloadError
from audio_effects import apply_fade, convert_channels, resample_audio, make_ringtone

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

            if is_age_restricted_error(error_text):
                logger.warning(f"Age-restricted video blocked download for URL: {url}")
                raise HTTPException(
                    403,
                    "This video is age-restricted by YouTube and requires a verified "
                    "account to view. We're not able to download age-restricted content "
                    "at this time - try a different video."
                )

            if is_members_only_error(error_text):
                logger.warning(f"Members-only video blocked download for URL: {url}")
                raise HTTPException(
                    403,
                    "This video is exclusive to that channel's paid members and isn't "
                    "publicly downloadable - try a different video."
                )

            if is_not_yet_live_error(error_text):
                logger.warning(f"Not-yet-live video blocked download for URL: {url}")
                raise HTTPException(
                    409,
                    "This video is a scheduled premiere or live stream that hasn't "
                    "started yet - try again once it's live, or try a different video."
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

async def _run_separation_background(
    job_id: str,
    file_path: str,
    original_filename: str,
    model: str,
    overlap: float,
    timeout_seconds: int,
    max_duration_seconds: int,
    metric_label: str,
):
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
            vocals_path, instrumental_path = await run_blocking(
                run_separation, file_path, job_id,
                model, overlap, timeout_seconds, max_duration_seconds,
            )
            mark_complete(job_id, original_filename, vocals_path, instrumental_path)
            logger.info(f"[SEPARATION] Job {job_id} finished successfully ({model})")
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
            record_result(metric_label, succeeded)


async def _queue_separation(
    file: UploadFile,
    model: str,
    overlap: float,
    timeout_seconds: int,
    max_duration_seconds: int,
    metric_label: str,
) -> JSONResponse:
    """Shared submit path for /separate and /separate-hq - the two routes
    differ only in run knobs and rate limit, so the read/size-check/write/
    queue sequence lives here once. Knobs are resolved by the CALLER at
    submission time, so a config change can't alter a job already queued."""
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

    asyncio.create_task(_run_separation_background(
        job_id, file_path, file.filename,
        model, overlap, timeout_seconds, max_duration_seconds, metric_label,
    ))

    logger.info(f"[SEPARATION] Job {job_id} queued for '{file.filename}' (model={model})")
    return JSONResponse({"job_id": job_id, "status": "processing"})

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
    return await _queue_separation(
        file, SEPARATION_MODEL, SEPARATION_OVERLAP,
        DEMUCS_TIMEOUT_SECONDS, MAX_SEPARATION_DURATION_SECONDS, "/separate",
    )

@router.post(
    "/separate-hq",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=SEPARATION_HQ_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=SEPARATION_HQ_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def separate_audio_hq(file: UploadFile = File(...)):
    """
    High-quality separation: htdemucs_ft (4-model ensemble) at raised
    overlap. Roughly 5x the CPU time of /separate for a real quality
    gain, so it gets a longer timeout, a TIGHTER input duration cap, and
    a stricter rate limit.

    A separate route rather than a `quality` form field on /separate
    because rate-limit dependencies are evaluated before the request
    body is read - a Depends() can't see a Form value, so per-tier
    limits need per-tier routes.

    Shares the same job store and the same /separate/status,
    /separate/preview and /separate/download routes - the frontend only
    changes which URL it POSTs to.
    """
    if not SEPARATION_HQ_ENABLED:
        raise HTTPException(
            503,
            "High quality separation is temporarily unavailable due to server load. "
            "Please use standard separation."
        )

    return await _queue_separation(
        file, SEPARATION_MODEL_HQ, SEPARATION_OVERLAP_HQ,
        DEMUCS_TIMEOUT_SECONDS_HQ, MAX_SEPARATION_DURATION_SECONDS_HQ, "/separate-hq",
    )

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
# /stems - Demucs full multi-stem separation (async job flow)
#
# Same model, same semaphore, same CPU cost as /separate - the only
# difference is that the four internally-separated sources are kept as
# individual files instead of three being summed into no_vocals.wav.
#
# Its own status/preview/download routes rather than reusing /separate's,
# because the output shape differs: a stems job stores a {stem: path}
# dict, so the stem name is validated against that dict's keys rather
# than a fixed vocals/instrumental pair.
# ============================================================

async def _run_stems_background(
    job_id: str,
    file_path: str,
    original_filename: str,
    model: str,
    overlap: float,
    timeout_seconds: int,
    max_duration_seconds: int,
    metric_label: str,
):
    succeeded = False
    async with _separation_semaphore:
        try:
            stems = await run_blocking(
                run_stem_separation, file_path, job_id,
                model, overlap, timeout_seconds, max_duration_seconds,
            )
            mark_stems_complete(job_id, original_filename, stems)
            logger.info(f"[STEMS] Job {job_id} finished successfully ({model}, {len(stems)} stems)")
            succeeded = True
        except SeparationError as e:
            mark_failed(job_id, str(e))
            logger.warning(f"[STEMS] Job {job_id} failed: {e}")
        except Exception as e:
            mark_failed(job_id, "Stem separation failed unexpectedly.")
            logger.error(f"[STEMS] Job {job_id} failed unexpectedly: {e}", exc_info=True)
        finally:
            cleanup_file(file_path)
            release_memory_to_os()
            record_result(metric_label, succeeded)


async def _queue_stems(
    file: UploadFile,
    model: str,
    overlap: float,
    timeout_seconds: int,
    max_duration_seconds: int,
    metric_label: str,
) -> JSONResponse:
    """Shared submit path for /stems and /stems-hq. Mirrors
    _queue_separation above, but creates a "stems"-typed job so the
    status/preview/download routes below can reject a job_id that
    belongs to a different tool."""
    cleanup_expired_jobs()

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(400, "Empty file")
    if len(content) > MAX_UPLOAD_BYTES:
        size_mb = len(content) / (1024 * 1024)
        raise HTTPException(400, f"File too large ({size_mb:.1f} MB). Maximum allowed size is {MAX_UPLOAD_BYTES // (1024*1024)} MB.")

    job_id = create_job(job_type="stems")
    file_path = os.path.join(UPLOAD_DIR, f"{job_id}_{file.filename}")
    with open(file_path, "wb") as f:
        f.write(content)
    del content

    asyncio.create_task(_run_stems_background(
        job_id, file_path, file.filename,
        model, overlap, timeout_seconds, max_duration_seconds, metric_label,
    ))

    logger.info(f"[STEMS] Job {job_id} queued for '{file.filename}' (model={model})")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.post(
    "/stems",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=STEMS_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=STEMS_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def stems_route(file: UploadFile = File(...)):
    """
    Accepts an audio file, immediately returns a job_id, and runs full
    4-stem Demucs separation (vocals/drums/bass/other) in the
    background. Poll GET /stems/status/{job_id} to track progress - the
    status response lists the available stem names once complete.
    """
    return await _queue_stems(
        file, SEPARATION_MODEL, SEPARATION_OVERLAP,
        DEMUCS_TIMEOUT_SECONDS, MAX_SEPARATION_DURATION_SECONDS, "/stems",
    )


@router.post(
    "/stems-hq",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=STEMS_HQ_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=STEMS_HQ_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def stems_route_hq(file: UploadFile = File(...)):
    """
    High-quality full stem separation: htdemucs_ft at raised overlap.
    Same longer timeout, tighter duration cap and stricter rate limit as
    /separate-hq, and gated by the same SEPARATION_HQ_ENABLED switch.
    """
    if not SEPARATION_HQ_ENABLED:
        raise HTTPException(
            503,
            "High quality separation is temporarily unavailable due to server load. "
            "Please use standard stem separation."
        )

    return await _queue_stems(
        file, SEPARATION_MODEL_HQ, SEPARATION_OVERLAP_HQ,
        DEMUCS_TIMEOUT_SECONDS_HQ, MAX_SEPARATION_DURATION_SECONDS_HQ, "/stems-hq",
    )


@router.get("/stems/status/{job_id}")
async def stems_status(job_id: str):
    """Returns the usual status fields plus the list of stem names that
    are actually available - so the frontend can render download buttons
    from the response instead of hardcoding stem names and breaking if a
    different model is ever configured."""
    job = get_job(job_id)
    if job is None or job["job_type"] != "stems":
        raise HTTPException(404, "Job not found (it may have expired).")
    stems = job.get("stems") or {}
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
        "stems": sorted(stems.keys()),
    }


def _resolve_stems_file(job_id: str, stem: str) -> str:
    """Stems equivalent of _resolve_stem_path above. Validates the
    requested stem against the job's OWN stem dict rather than a
    hardcoded tuple, so the valid set follows whatever model produced
    the job."""
    job = get_job(job_id)
    if job is None or job["job_type"] != "stems":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    stems = job.get("stems") or {}
    if stem not in stems:
        raise HTTPException(400, f"stem must be one of: {', '.join(sorted(stems.keys()))}")
    path = stems[stem]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Stem file not found (it may have expired).")
    return path


@router.get("/stems/preview/{job_id}")
async def stems_preview(job_id: str, stem: str = Query(...)):
    """Streams one stem inline for in-browser <audio> playback."""
    path = _resolve_stems_file(job_id, stem)
    return FileResponse(path, media_type="audio/wav")


@router.get("/stems/download/{job_id}")
async def stems_download(job_id: str, stem: str = Query(...)):
    """Same file as /preview, served as a downloadable attachment."""
    path = _resolve_stems_file(job_id, stem)
    return FileResponse(path, media_type="audio/wav", filename=f"{stem}.wav")

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


# ============================================================
# /video-to-audio - Extract the audio track from a video file
#
# The one endpoint that does NOT read its upload into memory whole.
# Every other route does `content = await file.read()`, which is fine at
# MAX_UPLOAD_BYTES' 50MB but would mean holding up to 200MB of video in
# RAM here. This one streams the body to disk in 1MB chunks and enforces
# its size cap as it goes, so an oversized upload is rejected partway
# through rather than after being fully buffered.
# ============================================================

async def _run_video_to_audio_background(job_id: str, input_path: str, output_path: str,
                                            target_format: str, original_filename: str):
    succeeded = False
    async with _audio_tools_semaphore:
        try:
            was_copied = await run_blocking(extract_audio, input_path, output_path, target_format)
            mark_tool_complete(job_id, original_filename, output_path, target_format)
            logger.info(
                f"[VIDEO_TO_AUDIO] Job {job_id} finished successfully "
                f"({'stream copy' if was_copied else 're-encoded'})"
            )
            succeeded = True
        except AudioToolError as e:
            mark_failed(job_id, str(e))
            logger.warning(f"[VIDEO_TO_AUDIO] Job {job_id} failed: {e}")
        except Exception as e:
            mark_failed(job_id, "Audio extraction failed unexpectedly.")
            logger.error(f"[VIDEO_TO_AUDIO] Job {job_id} failed unexpectedly: {e}", exc_info=True)
        finally:
            cleanup_file(input_path)
            release_memory_to_os()
            record_result("/video-to-audio", succeeded)


@router.post(
    "/video-to-audio",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=VIDEO_TO_AUDIO_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=VIDEO_TO_AUDIO_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def video_to_audio_route(file: UploadFile = File(...), target_format: str = Form("mp3")):
    """
    Accepts a video file + target audio format, returns a job_id
    immediately, runs the ffmpeg extraction in the background. Poll
    GET /video-to-audio/status/{job_id} to track progress.
    """
    cleanup_expired_jobs()

    target_format = target_format.strip().lower()
    if target_format not in ALLOWED_AUDIO_INPUT_FORMATS:
        raise HTTPException(400, f"target_format must be one of: {', '.join(sorted(ALLOWED_AUDIO_INPUT_FORMATS))}")

    try:
        source_format = validate_video_input_format(file.filename)
    except AudioToolError as e:
        raise HTTPException(400, str(e))

    job_id = create_job(job_type="video_to_audio")
    input_path = os.path.join(UPLOAD_DIR, f"{job_id}_{file.filename}")
    output_path = build_output_path(job_id, target_format)

    # Streamed write with the size cap enforced per chunk. On overflow the
    # partial file is deleted immediately - without that, a rejected
    # 200MB upload would still leave 200MB sitting on a 30GB disk until
    # the TTL sweep noticed.
    total = 0
    try:
        with open(input_path, "wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_VIDEO_UPLOAD_BYTES:
                    raise HTTPException(
                        400,
                        f"File too large. Maximum allowed size is "
                        f"{MAX_VIDEO_UPLOAD_BYTES // (1024*1024)} MB."
                    )
                f.write(chunk)
    except HTTPException:
        cleanup_file(input_path)
        mark_failed(job_id, "Upload rejected.")
        raise
    except Exception as e:
        cleanup_file(input_path)
        mark_failed(job_id, "Upload failed.")
        logger.error(f"[VIDEO_TO_AUDIO] Upload write failed for job {job_id}: {e}", exc_info=True)
        raise HTTPException(500, "Failed to receive the uploaded file.")

    if total == 0:
        cleanup_file(input_path)
        mark_failed(job_id, "Empty file.")
        raise HTTPException(400, "Empty file")

    asyncio.create_task(_run_video_to_audio_background(
        job_id, input_path, output_path, target_format, file.filename
    ))

    logger.info(
        f"[VIDEO_TO_AUDIO] Job {job_id} queued: '{file.filename}' "
        f"({source_format} -> {target_format}, {total / (1024*1024):.1f} MB)"
    )
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/video-to-audio/status/{job_id}")
async def video_to_audio_status(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "video_to_audio":
        raise HTTPException(404, "Job not found (it may have expired).")
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
    }


@router.get("/video-to-audio/preview/{job_id}")
async def video_to_audio_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "video_to_audio")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/video-to-audio/download/{job_id}")
async def video_to_audio_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "video_to_audio")
    return FileResponse(path, media_type="application/octet-stream", filename=f"audio.{fmt}")



# ============================================================
# /join - Concatenate several audio files into one (async job flow)
#
# The only endpoint taking MULTIPLE uploads. Two consequences worth
# knowing: the size cap is enforced across the whole batch rather than
# per file, and the ORDER of the uploaded files is the order of the
# output - FastAPI preserves List[UploadFile] ordering, so the frontend
# controls sequencing purely by the order it appends to the form.
# ============================================================

async def _run_join_background(job_id: str, input_paths: List[str], output_path: str,
                                  target_format: str, original_filename: str):
    succeeded = False
    async with _audio_tools_semaphore:
        try:
            total_duration = await run_blocking(join_audio, input_paths, output_path, target_format)
            mark_tool_complete(job_id, original_filename, output_path, target_format)
            logger.info(f"[JOIN] Job {job_id} finished successfully ({total_duration:.1f}s total)")
            succeeded = True
        except AudioToolError as e:
            mark_failed(job_id, str(e))
            logger.warning(f"[JOIN] Job {job_id} failed: {e}")
        except Exception as e:
            mark_failed(job_id, "Joining failed unexpectedly.")
            logger.error(f"[JOIN] Job {job_id} failed unexpectedly: {e}", exc_info=True)
        finally:
            for path in input_paths:
                cleanup_file(path)
            release_memory_to_os()
            record_result("/join", succeeded)


@router.post(
    "/join",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=JOIN_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=JOIN_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def join_route(files: List[UploadFile] = File(...), target_format: str = Form("mp3")):
    """
    Accepts two or more audio files plus a target format, returns a
    job_id immediately, runs the ffmpeg concat in the background. Poll
    GET /join/status/{job_id} to track progress.

    Output order matches upload order.
    """
    cleanup_expired_jobs()

    target_format = target_format.strip().lower()
    if target_format not in ALLOWED_AUDIO_INPUT_FORMATS:
        raise HTTPException(400, f"target_format must be one of: {', '.join(sorted(ALLOWED_AUDIO_INPUT_FORMATS))}")

    if len(files) < 2:
        raise HTTPException(400, "Joining needs at least two files.")
    if len(files) > JOIN_MAX_FILES:
        raise HTTPException(400, f"You can join up to {JOIN_MAX_FILES} files at a time.")

    for f in files:
        try:
            validate_input_format(f.filename)
        except AudioToolError as e:
            raise HTTPException(400, str(e))

    job_id = create_job(job_type="join")
    input_paths: List[str] = []
    total = 0

    # Streamed to disk with the cap tracked ACROSS files, not per file -
    # and every already-written file is cleaned up on any failure, since
    # a rejected batch would otherwise leave up to 150MB behind until the
    # TTL sweep caught it.
    try:
        for index, f in enumerate(files):
            path = os.path.join(UPLOAD_DIR, f"{job_id}_{index}_{f.filename}")
            input_paths.append(path)

            with open(path, "wb") as out:
                while True:
                    chunk = await f.read(1024 * 1024)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > JOIN_MAX_TOTAL_BYTES:
                        raise HTTPException(
                            400,
                            f"Combined file size too large. Maximum total is "
                            f"{JOIN_MAX_TOTAL_BYTES // (1024*1024)} MB."
                        )
                    out.write(chunk)

            if os.path.getsize(path) == 0:
                raise HTTPException(400, f"'{f.filename}' is empty.")

    except HTTPException:
        for path in input_paths:
            cleanup_file(path)
        mark_failed(job_id, "Upload rejected.")
        raise
    except Exception as e:
        for path in input_paths:
            cleanup_file(path)
        mark_failed(job_id, "Upload failed.")
        logger.error(f"[JOIN] Upload write failed for job {job_id}: {e}", exc_info=True)
        raise HTTPException(500, "Failed to receive the uploaded files.")

    output_path = build_output_path(job_id, target_format)

    asyncio.create_task(_run_join_background(
        job_id, input_paths, output_path, target_format, files[0].filename
    ))

    logger.info(
        f"[JOIN] Job {job_id} queued: {len(files)} files -> {target_format} "
        f"({total / (1024*1024):.1f} MB)"
    )
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/join/status/{job_id}")
async def join_status(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "join":
        raise HTTPException(404, "Job not found (it may have expired).")
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
    }


@router.get("/join/preview/{job_id}")
async def join_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "join")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/join/download/{job_id}")
async def join_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "join")
    return FileResponse(path, media_type="application/octet-stream", filename=f"joined.{fmt}")

# ============================================================
# /loudnorm - Two-pass LUFS loudness normalization (async job flow)
# ============================================================

async def _run_loudnorm_background(job_id: str, input_path: str, output_path: str,
                                      target_lufs: float, original_filename: str, source_format: str):
    succeeded = False
    async with _audio_tools_semaphore:
        try:
            measured_lufs, applied_target = await run_blocking(
                normalize_loudness, input_path, output_path, target_lufs
            )
            mark_tool_complete(job_id, original_filename, output_path, source_format)
            logger.info(
                f"[LOUDNORM] Job {job_id} finished successfully "
                f"(measured {measured_lufs} LUFS -> {applied_target} LUFS)"
            )
            succeeded = True
        except AudioToolError as e:
            mark_failed(job_id, str(e))
            logger.warning(f"[LOUDNORM] Job {job_id} failed: {e}")
        except Exception as e:
            mark_failed(job_id, "Loudness normalization failed unexpectedly.")
            logger.error(f"[LOUDNORM] Job {job_id} failed unexpectedly: {e}", exc_info=True)
        finally:
            cleanup_file(input_path)
            release_memory_to_os()
            record_result("/loudnorm", succeeded)


@router.post(
    "/loudnorm",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=LOUDNORM_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=LOUDNORM_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def loudnorm_route(
    file: UploadFile = File(...),
    preset: str = Form("streaming"),
    custom_lufs: float = Form(None),
):
    """
    Accepts an audio file plus either a named preset (streaming/club/
    broadcast) or an explicit custom_lufs override, returns a job_id
    immediately, runs the two-pass ffmpeg loudnorm in the background.
    Poll GET /loudnorm/status/{job_id} to track progress.
    """
    cleanup_expired_jobs()

    source_format = validate_input_format(file.filename)

    try:
        target_lufs = resolve_target_lufs(preset, custom_lufs)
    except AudioToolError as e:
        raise HTTPException(400, str(e))

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(400, "Empty file")
    if len(content) > MAX_UPLOAD_BYTES:
        size_mb = len(content) / (1024 * 1024)
        raise HTTPException(400, f"File too large ({size_mb:.1f} MB). Maximum allowed size is {MAX_UPLOAD_BYTES // (1024*1024)} MB.")

    job_id = create_job(job_type="loudnorm")
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

    asyncio.create_task(_run_loudnorm_background(
        job_id, input_path, output_path, target_lufs, file.filename, source_format
    ))

    logger.info(f"[LOUDNORM] Job {job_id} queued: '{file.filename}' -> {target_lufs} LUFS")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/loudnorm/status/{job_id}")
async def loudnorm_status(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "loudnorm":
        raise HTTPException(404, "Job not found (it may have expired).")
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
    }


@router.get("/loudnorm/preview/{job_id}")
async def loudnorm_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "loudnorm")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/loudnorm/download/{job_id}")
async def loudnorm_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "loudnorm")
    return FileResponse(path, media_type="application/octet-stream", filename=f"normalized.{fmt}")


# ============================================================
# /silence-split - Cut a file into segments at silent gaps
#
# Reuses the "stems" job shape from /stems: a {name: path} dict, the
# same mark_stems_complete(), and the same status-lists-available-names
# pattern - the only actual difference from /stems is what produced the
# dict and how many entries it has.
# ============================================================

async def _run_silence_split_background(job_id: str, input_path: str, target_format: str,
                                            threshold_db: float, min_duration_seconds: float,
                                            original_filename: str):
    succeeded = False
    async with _audio_tools_semaphore:
        try:
            segments = await run_blocking(
                split_on_silence, input_path, job_id, target_format, threshold_db, min_duration_seconds
            )
            mark_stems_complete(job_id, original_filename, segments)
            logger.info(f"[SILENCE_SPLIT] Job {job_id} finished successfully ({len(segments)} segments)")
            succeeded = True
        except AudioToolError as e:
            mark_failed(job_id, str(e))
            logger.warning(f"[SILENCE_SPLIT] Job {job_id} failed: {e}")
        except Exception as e:
            mark_failed(job_id, "Splitting failed unexpectedly.")
            logger.error(f"[SILENCE_SPLIT] Job {job_id} failed unexpectedly: {e}", exc_info=True)
        finally:
            cleanup_file(input_path)
            release_memory_to_os()
            record_result("/silence-split", succeeded)


@router.post(
    "/silence-split",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=SILENCE_SPLIT_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=SILENCE_SPLIT_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def silence_split_route(
    file: UploadFile = File(...),
    target_format: str = Form("mp3"),
    threshold_db: float = Form(-30.0),
    min_duration_seconds: float = Form(0.5),
):
    """
    Accepts an audio file, returns a job_id immediately, detects silent
    gaps and cuts the file into one segment per non-silent span in the
    background. Poll GET /silence-split/status/{job_id} - the response
    lists the available segment names once complete, same pattern as
    /stems/status.
    """
    cleanup_expired_jobs()

    source_format = validate_input_format(file.filename)

    if target_format.strip().lower() not in ALLOWED_AUDIO_INPUT_FORMATS:
        raise HTTPException(400, f"target_format must be one of: {', '.join(sorted(ALLOWED_AUDIO_INPUT_FORMATS))}")
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

    job_id = create_job(job_type="silence_split")
    input_path = os.path.join(UPLOAD_DIR, f"{job_id}_{file.filename}")
    with open(input_path, "wb") as f:
        f.write(content)
    del content

    target_format = target_format.strip().lower()

    asyncio.create_task(_run_silence_split_background(
        job_id, input_path, target_format, threshold_db, min_duration_seconds, file.filename
    ))

    logger.info(f"[SILENCE_SPLIT] Job {job_id} queued: '{file.filename}' (threshold={threshold_db}dB)")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/silence-split/status/{job_id}")
async def silence_split_status(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "silence_split":
        raise HTTPException(404, "Job not found (it may have expired).")
    segments = job.get("stems") or {}
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
        "segments": sorted(segments.keys()),
    }


def _resolve_silence_split_file(job_id: str, segment: str) -> str:
    job = get_job(job_id)
    if job is None or job["job_type"] != "silence_split":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    segments = job.get("stems") or {}
    if segment not in segments:
        raise HTTPException(400, f"segment must be one of: {', '.join(sorted(segments.keys()))}")
    path = segments[segment]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Segment file not found (it may have expired).")
    return path


@router.get("/silence-split/preview/{job_id}")
async def silence_split_preview(job_id: str, segment: str = Query(...)):
    path = _resolve_silence_split_file(job_id, segment)
    return FileResponse(path, media_type=get_audio_mime_type(path.rsplit(".", 1)[-1]))


@router.get("/silence-split/download/{job_id}")
async def silence_split_download(job_id: str, segment: str = Query(...)):
    path = _resolve_silence_split_file(job_id, segment)
    ext = path.rsplit(".", 1)[-1]
    return FileResponse(path, media_type="application/octet-stream", filename=f"{segment}.{ext}")




# ============================================================
# /youtube/* - Paste a URL, get the processed result directly, skipping
# the manual download-then-reupload step.
#
# Each of these is TWO of the app's heaviest operations chained in one
# background job: a YouTube download, then either analysis or Demucs
# separation. The download step acquires _download_semaphore (via the
# same acquire_slot_or_503 queue-then-503 pattern /download itself uses)
# and RELEASES IT before the processing step acquires its own semaphore -
# the two are never held at once, so a slow separation doesn't also
# tie up a download slot the whole time, and vice versa.
# ============================================================

async def _run_youtube_analyze_background(job_id: str, url: str):
    succeeded = False
    file_path = None

    await acquire_slot_or_503(_download_semaphore, "youtube-analyze-download")
    try:
        file_path, title = await run_blocking(download_audio_to_file, url, job_id)
    except ChainDownloadError as e:
        mark_failed(job_id, str(e))
        logger.warning(f"[YOUTUBE_ANALYZE] Job {job_id} download failed: {e}")
        _download_semaphore.release()
        record_result("/youtube/analyze", False)
        return
    except Exception as e:
        mark_failed(job_id, "Download failed unexpectedly.")
        logger.error(f"[YOUTUBE_ANALYZE] Job {job_id} download failed unexpectedly: {e}", exc_info=True)
        _download_semaphore.release()
        record_result("/youtube/analyze", False)
        return
    else:
        _download_semaphore.release()

    analysis_path = file_path
    await acquire_slot_or_503(_analysis_semaphore, "youtube-analyze")
    try:
        if ANALYSIS_MAX_SECONDS is not None:
            analysis_path = await run_blocking(trim_audio_for_analysis, file_path, ANALYSIS_MAX_SECONDS)

        key, scale, key_conf, bpm, bpm_conf, audio_array, essentia_sr = await run_blocking(
            detect_key_bpm_essentia, analysis_path
        )
        key, scale, key_conf, bpm, bpm_conf, agreement = await run_blocking(
            cross_check_with_librosa, audio_array, essentia_sr, key, scale, key_conf, bpm, bpm_conf
        )

        camelot = get_camelot(key, scale)
        result = {
            "key": f"{key} {scale}",
            "camelot": camelot,
            "bpm": bpm,
            "confidence": int(min(0.99, key_conf) * 100),
            "bpm_confidence": min(99, bpm_conf),
            "cross_check": agreement,
        }
        mark_data_complete(job_id, title, result)
        logger.info(f"[YOUTUBE_ANALYZE] Job {job_id} finished successfully: {result}")
        succeeded = True
    except Exception as e:
        mark_failed(job_id, "Analysis failed unexpectedly.")
        logger.error(f"[YOUTUBE_ANALYZE] Job {job_id} analysis failed: {e}", exc_info=True)
    finally:
        cleanup_file(file_path)
        if analysis_path != file_path:
            cleanup_file(analysis_path)
        _analysis_semaphore.release()
        release_memory_to_os()
        record_result("/youtube/analyze", succeeded)


@router.post(
    "/youtube/analyze",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=YOUTUBE_CHAIN_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=YOUTUBE_CHAIN_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def youtube_analyze_route(url: str = Form(...)):
    """
    Accepts a YouTube URL, returns a job_id immediately, downloads the
    audio and runs key/BPM analysis in the background. Poll
    GET /youtube/analyze/status/{job_id}, then
    GET /youtube/analyze/result/{job_id} once complete.
    """
    if not is_valid_youtube_url(url):
        raise HTTPException(400, "Please provide a valid YouTube video URL.")

    job_id = create_job(job_type="youtube_analyze", ttl_seconds=YOUTUBE_ANALYZE_JOB_TTL_SECONDS)
    asyncio.create_task(_run_youtube_analyze_background(job_id, url))

    logger.info(f"[YOUTUBE_ANALYZE] Job {job_id} queued for {url}")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/youtube/analyze/status/{job_id}")
async def youtube_analyze_status(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "youtube_analyze":
        raise HTTPException(404, "Job not found (it may have expired).")
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
    }


@router.get("/youtube/analyze/result/{job_id}")
async def youtube_analyze_result(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "youtube_analyze":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    result = job.get("result_data")
    if not result:
        raise HTTPException(404, "Result not found (it may have expired).")
    return JSONResponse(result)


# ------------------------------------------------------------
# /youtube/separate - download then vocal/instrumental separation
# ------------------------------------------------------------

async def _run_youtube_separate_background(job_id: str, url: str):
    succeeded = False
    file_path = None

    await acquire_slot_or_503(_download_semaphore, "youtube-separate-download")
    try:
        file_path, title = await run_blocking(download_audio_to_file, url, job_id)
    except ChainDownloadError as e:
        mark_failed(job_id, str(e))
        logger.warning(f"[YOUTUBE_SEPARATE] Job {job_id} download failed: {e}")
        _download_semaphore.release()
        record_result("/youtube/separate", False)
        return
    except Exception as e:
        mark_failed(job_id, "Download failed unexpectedly.")
        logger.error(f"[YOUTUBE_SEPARATE] Job {job_id} download failed unexpectedly: {e}", exc_info=True)
        _download_semaphore.release()
        record_result("/youtube/separate", False)
        return
    else:
        _download_semaphore.release()

    async with _separation_semaphore:
        try:
            vocals_path, instrumental_path = await run_blocking(
                run_separation, file_path, job_id,
                SEPARATION_MODEL, SEPARATION_OVERLAP,
                DEMUCS_TIMEOUT_SECONDS, MAX_SEPARATION_DURATION_SECONDS,
            )
            mark_complete(job_id, title, vocals_path, instrumental_path)
            logger.info(f"[YOUTUBE_SEPARATE] Job {job_id} finished successfully")
            succeeded = True
        except SeparationError as e:
            mark_failed(job_id, str(e))
            logger.warning(f"[YOUTUBE_SEPARATE] Job {job_id} failed: {e}")
        except Exception as e:
            mark_failed(job_id, "Separation failed unexpectedly.")
            logger.error(f"[YOUTUBE_SEPARATE] Job {job_id} failed unexpectedly: {e}", exc_info=True)
        finally:
            cleanup_file(file_path)
            release_memory_to_os()
            record_result("/youtube/separate", succeeded)


@router.post(
    "/youtube/separate",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=YOUTUBE_CHAIN_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=YOUTUBE_CHAIN_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def youtube_separate_route(url: str = Form(...)):
    """
    Accepts a YouTube URL, returns a job_id immediately, downloads the
    audio and runs standard-tier Demucs vocal/instrumental separation in
    the background. Poll GET /youtube/separate/status/{job_id}. Once
    complete, stem paths are stored the same way /separate stores them -
    reuse /separate/preview and /separate/download with this job_id.
    """
    if not is_valid_youtube_url(url):
        raise HTTPException(400, "Please provide a valid YouTube video URL.")

    job_id = create_job(job_type="youtube_separate")
    asyncio.create_task(_run_youtube_separate_background(job_id, url))

    logger.info(f"[YOUTUBE_SEPARATE] Job {job_id} queued for {url}")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/youtube/separate/status/{job_id}")
async def youtube_separate_status(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "youtube_separate":
        raise HTTPException(404, "Job not found (it may have expired).")
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
    }


@router.get("/youtube/separate/preview/{job_id}")
async def youtube_separate_preview(job_id: str, stem: str = Query(...)):
    """Reuses the same stem-path resolution as /separate/preview - stem
    is 'vocals' or 'instrumental', looked up on this job_id instead."""
    job = get_job(job_id)
    if job is None or job["job_type"] != "youtube_separate":
        raise HTTPException(404, "Job not found (it may have expired).")
    if stem not in ("vocals", "instrumental"):
        raise HTTPException(400, "stem must be 'vocals' or 'instrumental'")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    path = job["vocals_path"] if stem == "vocals" else job["instrumental_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Stem file not found (it may have expired).")
    return FileResponse(path, media_type="audio/wav")


@router.get("/youtube/separate/download/{job_id}")
async def youtube_separate_download(job_id: str, stem: str = Query(...)):
    job = get_job(job_id)
    if job is None or job["job_type"] != "youtube_separate":
        raise HTTPException(404, "Job not found (it may have expired).")
    if stem not in ("vocals", "instrumental"):
        raise HTTPException(400, "stem must be 'vocals' or 'instrumental'")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    path = job["vocals_path"] if stem == "vocals" else job["instrumental_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Stem file not found (it may have expired).")
    return FileResponse(path, media_type="audio/wav", filename=f"{stem}.wav")


# ------------------------------------------------------------
# /youtube/stems - download then full 4-stem separation
# ------------------------------------------------------------

async def _run_youtube_stems_background(job_id: str, url: str):
    succeeded = False
    file_path = None

    await acquire_slot_or_503(_download_semaphore, "youtube-stems-download")
    try:
        file_path, title = await run_blocking(download_audio_to_file, url, job_id)
    except ChainDownloadError as e:
        mark_failed(job_id, str(e))
        logger.warning(f"[YOUTUBE_STEMS] Job {job_id} download failed: {e}")
        _download_semaphore.release()
        record_result("/youtube/stems", False)
        return
    except Exception as e:
        mark_failed(job_id, "Download failed unexpectedly.")
        logger.error(f"[YOUTUBE_STEMS] Job {job_id} download failed unexpectedly: {e}", exc_info=True)
        _download_semaphore.release()
        record_result("/youtube/stems", False)
        return
    else:
        _download_semaphore.release()

    async with _separation_semaphore:
        try:
            stems = await run_blocking(
                run_stem_separation, file_path, job_id,
                SEPARATION_MODEL, SEPARATION_OVERLAP,
                DEMUCS_TIMEOUT_SECONDS, MAX_SEPARATION_DURATION_SECONDS,
            )
            mark_stems_complete(job_id, title, stems)
            logger.info(f"[YOUTUBE_STEMS] Job {job_id} finished successfully ({len(stems)} stems)")
            succeeded = True
        except SeparationError as e:
            mark_failed(job_id, str(e))
            logger.warning(f"[YOUTUBE_STEMS] Job {job_id} failed: {e}")
        except Exception as e:
            mark_failed(job_id, "Stem separation failed unexpectedly.")
            logger.error(f"[YOUTUBE_STEMS] Job {job_id} failed unexpectedly: {e}", exc_info=True)
        finally:
            cleanup_file(file_path)
            release_memory_to_os()
            record_result("/youtube/stems", succeeded)


@router.post(
    "/youtube/stems",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=YOUTUBE_CHAIN_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=YOUTUBE_CHAIN_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def youtube_stems_route(url: str = Form(...)):
    """
    Accepts a YouTube URL, returns a job_id immediately, downloads the
    audio and runs standard-tier full 4-stem Demucs separation in the
    background. Poll GET /youtube/stems/status/{job_id} - lists
    available stem names once complete, same as /stems/status.
    """
    if not is_valid_youtube_url(url):
        raise HTTPException(400, "Please provide a valid YouTube video URL.")

    job_id = create_job(job_type="youtube_stems")
    asyncio.create_task(_run_youtube_stems_background(job_id, url))

    logger.info(f"[YOUTUBE_STEMS] Job {job_id} queued for {url}")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/youtube/stems/status/{job_id}")
async def youtube_stems_status(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "youtube_stems":
        raise HTTPException(404, "Job not found (it may have expired).")
    stems = job.get("stems") or {}
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
        "stems": sorted(stems.keys()),
    }


def _resolve_youtube_stems_file(job_id: str, stem: str) -> str:
    job = get_job(job_id)
    if job is None or job["job_type"] != "youtube_stems":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    stems = job.get("stems") or {}
    if stem not in stems:
        raise HTTPException(400, f"stem must be one of: {', '.join(sorted(stems.keys()))}")
    path = stems[stem]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Stem file not found (it may have expired).")
    return path


@router.get("/youtube/stems/preview/{job_id}")
async def youtube_stems_preview(job_id: str, stem: str = Query(...)):
    path = _resolve_youtube_stems_file(job_id, stem)
    return FileResponse(path, media_type="audio/wav")


@router.get("/youtube/stems/download/{job_id}")
async def youtube_stems_download(job_id: str, stem: str = Query(...)):
    path = _resolve_youtube_stems_file(job_id, stem)
    return FileResponse(path, media_type="audio/wav", filename=f"{stem}.wav")


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