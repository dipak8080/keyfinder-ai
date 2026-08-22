"""
routes/media.py - tools with a shape different from the plain
audio-in/audio-out ffmpeg family in audio_tools.py: /analyze
(synchronous, no job, no file output), /video-to-audio (video input,
its own size cap), /join (the only multi-upload route), /silence-split
(multi-output, reuses the "stems" storage shape).

Split out of the old monolithic routes.py (2026-08-14 restructure). Pure
move: every docstring, comment, and line of logic here is unchanged from
its original location. Nothing in this file changes behaviour.

--------------------------------------------------------------------------
WHAT CHANGED (2026-08-22): /analyze GOT ITS OWN RATE LIMIT

/analyze was the only route in the routes/ package still carrying a bare
`Depends(check_rate_limit)` with no arguments. That is not "no limit" -
it silently inherited check_rate_limit's default arguments, which fall
back to the generic RATE_LIMIT_MAX_REQUESTS / RATE_LIMIT_WINDOW_SECONDS
pair in config.py: 20 requests per 60 seconds, or 1200 an hour.

Two problems with that, and the second is the one that actually matters.

The number was wrong. /analyze streams a full upload to disk and then
holds one of only MAX_CONCURRENT_ANALYSIS (4) slots for an Essentia run
plus a librosa cross-check. Comparable tools on this server sit at 3-5
per minute. 1200/hour let a single IP keep most of the analysis pool
busy indefinitely, and /youtube/analyze - the same Essentia work with a
download in front - is capped at 15/hour.

The number was also invisible, which is worse. A bare
Depends(check_rate_limit) names nothing, so neither this file nor
config.py told you what /analyze allowed; the answer lived in a default
argument in rate_limit.py. Every other route passes its limits
explicitly through partial(). This one now does too, which is the real
point - a limit nobody can find is a limit nobody maintains.

Nothing else in this file changed.
--------------------------------------------------------------------------
"""
import os
import time
import uuid
import asyncio
from typing import List
from functools import partial

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends, Query
from fastapi.responses import JSONResponse, FileResponse

from config import (
    logger,
    UPLOAD_DIR,
    MAX_UPLOAD_BYTES,
    ANALYSIS_MAX_SECONDS,
    ANALYZE_RATE_LIMIT_MAX_REQUESTS,
    ANALYZE_RATE_LIMIT_WINDOW_SECONDS,
    ALLOWED_AUDIO_INPUT_FORMATS,
    MAX_VIDEO_UPLOAD_BYTES,
    VIDEO_TO_AUDIO_RATE_LIMIT_MAX_REQUESTS,
    VIDEO_TO_AUDIO_RATE_LIMIT_WINDOW_SECONDS,
    JOIN_MAX_FILES,
    JOIN_MAX_TOTAL_BYTES,
    JOIN_RATE_LIMIT_MAX_REQUESTS,
    JOIN_RATE_LIMIT_WINDOW_SECONDS,
    SILENCE_SPLIT_RATE_LIMIT_MAX_REQUESTS,
    SILENCE_SPLIT_RATE_LIMIT_WINDOW_SECONDS,
    SILENCE_THRESHOLD_MIN_DB,
    SILENCE_THRESHOLD_MAX_DB,
    SILENCE_MIN_DURATION_SECONDS,
    SILENCE_MAX_DURATION_SECONDS,
)
from upload import save_upload, save_uploads
from utils import (
    build_safe_upload_path,
    run_blocking,
    cleanup_file,
    release_memory_to_os,
    acquire_slot_or_503,
    get_camelot,
    _analysis_semaphore,
    _audio_tools_semaphore,
)
from audio_analysis import detect_key_bpm_essentia, cross_check_with_librosa, trim_audio_for_analysis
from rate_limit import check_rate_limit
from monitoring import record_result
from jobs import create_job, mark_failed, mark_tool_complete, mark_stems_complete, get_job
from audio_common import AudioToolError, build_output_path, get_audio_mime_type
from video_to_audio import extract_audio, validate_video_input_format
from audio_joiner import join_audio
from silence_splitter import split_on_silence
from log_stream import set_job_context, remember_job_tags, tag_from_job

from ._shared import (
    spawn_background_task,
    _mb,
    _validated_input_format,
    _accept_upload,
    _validate_duration_or_reject,
    _log_queued,
    _run_tool_job,
    _tool_status,
    _resolve_tool_output_path,
    _reject_if_audio_tools_queue_full,
)

router = APIRouter()


# ============================================================
# /analyze - Key + BPM detection (synchronous)
#
# Synchronous rather than job-based because analysis only ever looks at
# the first ANALYSIS_MAX_SECONDS of audio, so it finishes inside a normal
# request window even for a long track.
#
# Limits passed explicitly (2026-08-22) rather than relying on
# check_rate_limit's defaults - see this file's WHAT CHANGED note for
# what the bare version was actually allowing.
# ============================================================

@router.post(
    "/analyze",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=ANALYZE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=ANALYZE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def analyze_audio(file: UploadFile = File(...)):
    # Same reasoning as /download above: no job, tagged anyway so this
    # row reports "ANALYZE" consistently.
    set_job_context(tool="ANALYZE", tier="standard")

    started = time.monotonic()

    file_id = str(uuid.uuid4())
    file_path = build_safe_upload_path(UPLOAD_DIR, file_id, file.filename)
    analysis_path = file_path

    # The upload is streamed to disk BEFORE the semaphore is taken. The
    # transfer is I/O-bound and holds no CPU, so making it wait for an
    # analysis slot would occupy a slot doing nothing while the bytes
    # arrive - and on a slow connection that is most of the request.
    size = await save_upload(file, file_path, MAX_UPLOAD_BYTES, label="analyze")

    await acquire_slot_or_503(_analysis_semaphore, "analysis")

    succeeded = False
    try:
        logger.info(f"[ANALYZE] Started '{file.filename}' {_mb(size)}")

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
            # Freed here rather than at the end of the request: the array
            # is the largest thing in memory during analysis, and holding
            # it while the response is serialized doubles peak usage for
            # no reason on a box with no swap.
            if audio_array is not None:
                del audio_array
            release_memory_to_os()

        camelot = get_camelot(key, scale)

        result = {
            "key": f"{key} {scale}",
            "camelot": camelot,
            "bpm": bpm,
            "confidence": int(min(0.99, key_conf) * 100),
            "bpm_confidence": min(99, bpm_conf),
            "cross_check": agreement,
        }

        logger.info(
            f"[ANALYZE] COMPLETE '{file.filename}' in {time.monotonic() - started:.1f}s: "
            f"{result['key']} / {result['camelot']} / {result['bpm']} BPM"
        )

        succeeded = True
        return JSONResponse(result)

    except HTTPException:
        raise
    except AudioToolError as e:
        logger.warning(f"[ANALYZE] FAILED '{file.filename}': {e}")
        raise HTTPException(400, str(e))
    except Exception as e:
        logger.error(f"[ANALYZE] FAILED '{file.filename}' (unexpected): {e}", exc_info=True)
        raise HTTPException(500, "Could not analyze this file. It may be corrupt or in an unsupported format.")
    finally:
        cleanup_file(file_path)
        if analysis_path != file_path:
            cleanup_file(analysis_path)
        release_memory_to_os()
        _analysis_semaphore.release()
        record_result("/analyze", succeeded)


# ============================================================
# /video-to-audio - Extract the audio track from a video file
#
# Its own, much higher size cap: a few minutes of phone video routinely
# exceeds what an audio upload ever would.
# ============================================================

@router.post(
    "/video-to-audio",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=VIDEO_TO_AUDIO_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=VIDEO_TO_AUDIO_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def video_to_audio_route(file: UploadFile = File(...), target_format: str = Form("mp3")):
    """Poll GET /video-to-audio/status/{job_id}."""
    set_job_context(tool="VIDEO_TO_AUDIO", tier="standard")

    target_format = target_format.strip().lower()
    if target_format not in ALLOWED_AUDIO_INPUT_FORMATS:
        raise HTTPException(
            400, f"target_format must be one of: {', '.join(sorted(ALLOWED_AUDIO_INPUT_FORMATS))}"
        )

    try:
        source_format = validate_video_input_format(file.filename)
    except AudioToolError as e:
        raise HTTPException(400, str(e))

    original_filename = file.filename

    # /video-to-audio builds its own submit path (its own size cap, its
    # own format validator), so it doesn't inherit _submit_audio_tool's
    # capacity gate - it runs on the shared _audio_tools_semaphore all
    # the same. Placed after the caller's own input has been validated
    # and before create_job, matching the shared helper exactly.
    #
    # It matters more here than on most routes: this endpoint accepts up
    # to MAX_VIDEO_UPLOAD_BYTES (200MB), so a submission refused at this
    # line saves an upload an order of magnitude larger than any other
    # tool's.
    _reject_if_audio_tools_queue_full()

    job_id = create_job(job_type="video_to_audio")
    remember_job_tags(job_id)

    # build_safe_upload_path keeps the real container extension
    # (.mp4/.mov/...), which ffmpeg genuinely needs to demux a video
    # correctly, while dropping the rest of the user-supplied filename -
    # see its docstring in utils.py for why any of that name in a path is
    # a liability (255-BYTE filename limits, separators, null bytes).
    input_path = build_safe_upload_path(UPLOAD_DIR, job_id, file.filename)
    output_path = build_output_path(job_id, target_format)

    try:
        size = await save_upload(file, input_path, MAX_VIDEO_UPLOAD_BYTES, label="video_to_audio")
    except HTTPException as e:
        mark_failed(job_id, e.detail if isinstance(e.detail, str) else "Upload rejected.")
        raise

    spawn_background_task(_run_tool_job(
        tool="VIDEO_TO_AUDIO",
        metric="/video-to-audio",
        job_id=job_id,
        semaphore=_audio_tools_semaphore,
        work=lambda: run_blocking(extract_audio, input_path, output_path, target_format),
        on_success=lambda _: mark_tool_complete(job_id, original_filename, output_path, target_format),
        generic_error="Audio extraction failed unexpectedly.",
        cleanup_paths=[input_path],
        success_detail=lambda copied: "stream copy" if copied else "re-encoded",
    ))

    _log_queued("VIDEO_TO_AUDIO", job_id, original_filename, size, f"{source_format} -> {target_format}")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/video-to-audio/status/{job_id}")
async def video_to_audio_status(job_id: str):
    return _tool_status(job_id, "video_to_audio")


@router.get("/video-to-audio/preview/{job_id}")
async def video_to_audio_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "video_to_audio")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/video-to-audio/download/{job_id}")
async def video_to_audio_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "video_to_audio")
    return FileResponse(path, media_type="application/octet-stream", filename=f"audio.{fmt}")


# ============================================================
# /join - Concatenate several audio files into one
#
# The only endpoint taking MULTIPLE uploads. Two consequences: the size
# cap is enforced across the whole batch as well as per file, and the
# ORDER of the uploaded files is the order of the output (FastAPI
# preserves List[UploadFile] ordering, so the frontend controls
# sequencing purely by the order it appends to the form).
# ============================================================

@router.post(
    "/join",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=JOIN_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=JOIN_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def join_route(files: List[UploadFile] = File(...), target_format: str = Form("mp3")):
    """Poll GET /join/status/{job_id}. Output order matches upload order."""
    set_job_context(tool="JOIN", tier="standard")

    target_format = target_format.strip().lower()
    if target_format not in ALLOWED_AUDIO_INPUT_FORMATS:
        raise HTTPException(
            400, f"target_format must be one of: {', '.join(sorted(ALLOWED_AUDIO_INPUT_FORMATS))}"
        )

    if len(files) < 2:
        raise HTTPException(400, "Joining needs at least two files.")
    if len(files) > JOIN_MAX_FILES:
        raise HTTPException(400, f"You can join up to {JOIN_MAX_FILES} files at a time.")

    # Every filename is checked before ANY byte is transferred - one bad
    # extension in a ten-file batch should not cost the user a 150MB
    # upload first.
    for f in files:
        _validated_input_format(f.filename)

    first_filename = files[0].filename

    # Same shared-pool capacity gate as every other audio tool - /join
    # has its own submit path (multi-upload, batch size cap) so it
    # doesn't inherit _submit_audio_tool's.
    #
    # The most valuable placement of the four: refusing here saves a
    # JOIN_MAX_TOTAL_BYTES batch (150MB across up to ten files) that
    # would otherwise all land on disk before anything noticed the pool
    # was full.
    _reject_if_audio_tools_queue_full()

    job_id = create_job(job_type="join")

    remember_job_tags(job_id)

    dest_paths = [
        build_safe_upload_path(UPLOAD_DIR, job_id, f.filename, suffix=f"_{index}")
        for index, f in enumerate(files)
    ]

    try:
        input_paths, total = await save_uploads(
            files, dest_paths, JOIN_MAX_TOTAL_BYTES, label="join"
        )
    except HTTPException as e:
        mark_failed(job_id, e.detail if isinstance(e.detail, str) else "Upload rejected.")
        raise

    output_path = build_output_path(job_id, target_format)

    spawn_background_task(_run_tool_job(
        tool="JOIN",
        metric="/join",
        job_id=job_id,
        semaphore=_audio_tools_semaphore,
        work=lambda: run_blocking(join_audio, input_paths, output_path, target_format),
        on_success=lambda _: mark_tool_complete(job_id, first_filename, output_path, target_format),
        generic_error="Joining failed unexpectedly.",
        cleanup_paths=input_paths,
        success_detail=lambda duration: f"{duration:.1f}s total",
    ))

    logger.info(
        f"[JOIN] job={job_id} queued {len(files)} files -> {target_format} "
        f"({_mb(total)} combined)"
    )
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/join/status/{job_id}")
async def join_status(job_id: str):
    return _tool_status(job_id, "join")


@router.get("/join/preview/{job_id}")
async def join_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "join")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/join/download/{job_id}")
async def join_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "join")
    return FileResponse(path, media_type="application/octet-stream", filename=f"joined.{fmt}")


# ============================================================
# /silence-split - Cut a file into segments at silent gaps
#
# Reuses the "stems" storage shape: a {name: path} dict, the same
# mark_stems_complete(), and the same status-lists-available-names
# pattern. The only real difference from /stems is what produced the
# dict and how many entries it has.
# ============================================================

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
    """Poll GET /silence-split/status/{job_id} - the response lists the
    available segment names once complete."""
    set_job_context(tool="SILENCE_SPLIT", tier="standard")

    _validated_input_format(file.filename)

    target_format = target_format.strip().lower()
    if target_format not in ALLOWED_AUDIO_INPUT_FORMATS:
        raise HTTPException(
            400, f"target_format must be one of: {', '.join(sorted(ALLOWED_AUDIO_INPUT_FORMATS))}"
        )
    if threshold_db < SILENCE_THRESHOLD_MIN_DB or threshold_db > SILENCE_THRESHOLD_MAX_DB:
        raise HTTPException(
            400,
            f"threshold_db must be between {SILENCE_THRESHOLD_MIN_DB} and {SILENCE_THRESHOLD_MAX_DB}."
        )
    if min_duration_seconds < SILENCE_MIN_DURATION_SECONDS or min_duration_seconds > SILENCE_MAX_DURATION_SECONDS:
        raise HTTPException(
            400,
            f"min_duration_seconds must be between {SILENCE_MIN_DURATION_SECONDS} "
            f"and {SILENCE_MAX_DURATION_SECONDS}."
        )

    original_filename = file.filename

    # Shared-pool capacity gate; /silence-split has its own submit path
    # because it produces MANY outputs rather than one, so it doesn't
    # inherit _submit_audio_tool's. Same position as the others: after
    # the caller's input has been validated, before create_job.
    _reject_if_audio_tools_queue_full()

    job_id = create_job(job_type="silence_split")

    remember_job_tags(job_id)
    input_path, size = await _accept_upload(file, job_id, label="silence_split")
    await _validate_duration_or_reject(job_id, input_path)

    spawn_background_task(_run_tool_job(
        tool="SILENCE_SPLIT",
        metric="/silence-split",
        job_id=job_id,
        semaphore=_audio_tools_semaphore,
        work=lambda: run_blocking(
            split_on_silence, input_path, job_id, target_format, threshold_db, min_duration_seconds
        ),
        on_success=lambda segments: mark_stems_complete(job_id, original_filename, segments),
        generic_error="Splitting failed unexpectedly.",
        cleanup_paths=[input_path],
        success_detail=lambda segments: f"{len(segments)} segments",
    ))

    _log_queued("SILENCE_SPLIT", job_id, original_filename, size, f"threshold={threshold_db}dB")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/silence-split/status/{job_id}")
async def silence_split_status(job_id: str):
    tag_from_job(job_id)
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
    tag_from_job(job_id)
    job = get_job(job_id)
    if job is None or job["job_type"] != "silence_split":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] == "failed":
        raise HTTPException(409, job.get("error") or "This job failed.")
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