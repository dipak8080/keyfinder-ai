"""
routes/audio_tools.py - the ffmpeg/rubberband family: convert, trim,
volume, pitch, tempo, reverse, noise-remove, voice-clean, echo-remove,
silence-remove, loudnorm, fade, channels, resample, ringtone.

Split out of the old monolithic routes.py (2026-08-14 restructure). Pure
move: every docstring, comment, and line of logic here is unchanged from
its original location. Nothing in this file changes behaviour.

FILE SCOPE NOTE: the original restructure plan labelled this file "the
13 ffmpeg/rubberband tools" (matching _submit_audio_tool's own docstring
count in _shared.py, which itself predates /loudnorm and /audio-to-midi
being wired through the same shared function - see _shared.py's
docstring, carried over unchanged, for that count). /trim and /loudnorm
were not explicitly assigned to a file in the original plan. Both are
single-file-in/single-file-out ffmpeg tools structurally identical to
the rest of this family (trim just has its own custom submit flow since
range validation needs the real duration, not the shared shortcut), so
both live here rather than in media.py - keeping every simple audio
in/audio out ffmpeg route in one place. media.py is reserved for tools
with a genuinely different shape: multi-file (/join), video input
(/video-to-audio), non-file (/analyze), multi-output (/silence-split).

Twelve of these fifteen (all but /trim) share one submit path
(_submit_audio_tool, in _shared.py), one background runner
(_run_tool_job, also _shared.py) and one semaphore
(_audio_tools_semaphore, in utils.py). Each POST differs only in what it
validates and which worker it calls.
"""
import os
import asyncio
from functools import partial

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends
from fastapi.responses import JSONResponse, FileResponse

from config import (
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
    LOUDNORM_RATE_LIMIT_MAX_REQUESTS,
    LOUDNORM_RATE_LIMIT_WINDOW_SECONDS,
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
from utils import run_blocking, cleanup_file, _audio_tools_semaphore
from rate_limit import check_rate_limit
from jobs import create_job, mark_failed, mark_tool_complete, get_job
from audio_common import (
    AudioToolError,
    validate_conversion_pair,
    build_output_path,
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
from audio_loudnorm import normalize_loudness, resolve_target_lufs
from audio_effects import apply_fade, convert_channels, resample_audio, make_ringtone
from log_stream import set_job_context, remember_job_tags

from ._shared import (
    spawn_background_task,
    _validated_input_format,
    _submit_audio_tool,
    _tool_status,
    _resolve_tool_output_path,
    _accept_upload,
    _validate_duration_or_reject,
    _log_queued,
    _run_tool_job,
)

router = APIRouter()


# ---------- /convert ----------

@router.post(
    "/convert",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_CONVERT_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_CONVERT_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def convert_audio_route(file: UploadFile = File(...), target_format: str = Form(...)):
    """Format conversion. Poll GET /convert/status/{job_id}."""
    target_format = target_format.strip().lower()
    source_format = _validated_input_format(file.filename)
    try:
        validate_conversion_pair(source_format, target_format, AUDIO_CONVERSION_MATRIX)
    except AudioToolError as e:
        raise HTTPException(400, str(e))

    return await _submit_audio_tool(
        file,
        job_type="convert",
        tool="CONVERT",
        metric="/convert",
        output_format=target_format,
        # Conversion cost barely scales with length (it's a re-encode, not
        # an analysis), so it's the one tool exempt from the duration cap
        # - the size cap alone is enough.
        check_duration=False,
        build_work=lambda inp, out: (
            lambda: run_blocking(convert_audio, inp, out, source_format, target_format)
        ),
        log_detail=f"{source_format} -> {target_format}",
        generic_error="Conversion failed unexpectedly.",
    )


@router.get("/convert/status/{job_id}")
async def convert_status(job_id: str):
    return _tool_status(job_id, "convert")


@router.get("/convert/preview/{job_id}")
async def convert_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "convert")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/convert/download/{job_id}")
async def convert_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "convert")
    return FileResponse(path, media_type="application/octet-stream", filename=f"converted.{fmt}")


# ---------- /trim ----------

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
    """Cut to a start-end range. Poll GET /trim/status/{job_id}."""
    # /trim doesn't go through _submit_audio_tool (it needs the real
    # duration for its own range validation, not just the shared
    # cap-and-reject check), so it gets its own tag here.
    set_job_context(tool="TRIM", tier="standard")

    if start_seconds < 0 or end_seconds <= start_seconds:
        raise HTTPException(
            400,
            "Invalid range: end_seconds must be greater than start_seconds, "
            "and both must be non-negative."
        )

    source_format = _validated_input_format(file.filename)
    original_filename = file.filename

    job_id = create_job(job_type="trim")

    remember_job_tags(job_id)
    input_path, size = await _accept_upload(file, job_id, label="trim")
    output_path = build_output_path(job_id, source_format)

    # Trim needs the real duration for two separate reasons, so it can't
    # use the shared helper's fire-and-forget check: the cap has to be
    # enforced AND the value is passed to trim_audio() itself, and
    # end_seconds has to be range-checked against it.
    duration = await _validate_duration_or_reject(job_id, input_path)

    if end_seconds > duration:
        cleanup_file(input_path)
        mark_failed(job_id, "Requested range is past the end of the audio.")
        raise HTTPException(
            400,
            f"end_seconds ({end_seconds}s) exceeds the audio's actual duration ({duration:.1f}s)."
        )

    spawn_background_task(_run_tool_job(
        tool="TRIM",
        metric="/trim",
        job_id=job_id,
        semaphore=_audio_tools_semaphore,
        work=lambda: run_blocking(trim_audio, input_path, output_path, start_seconds, end_seconds, duration),
        on_success=lambda _: mark_tool_complete(job_id, original_filename, output_path, source_format),
        generic_error="Trim failed unexpectedly.",
        cleanup_paths=[input_path],
    ))

    _log_queued("TRIM", job_id, original_filename, size, f"[{start_seconds}s -> {end_seconds}s of {duration:.1f}s]")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/trim/status/{job_id}")
async def trim_status(job_id: str):
    return _tool_status(job_id, "trim")


@router.get("/trim/preview/{job_id}")
async def trim_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "trim")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/trim/download/{job_id}")
async def trim_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "trim")
    return FileResponse(path, media_type="application/octet-stream", filename=f"trimmed.{fmt}")


# ---------- /volume ----------

@router.post(
    "/volume",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_VOLUME_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_VOLUME_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def volume_route(file: UploadFile = File(...), gain_db: float = Form(...)):
    """Gain boost or reduction. Poll GET /volume/status/{job_id}."""
    if gain_db < VOLUME_GAIN_MIN_DB or gain_db > VOLUME_GAIN_MAX_DB:
        raise HTTPException(400, f"gain_db must be between {VOLUME_GAIN_MIN_DB} and {VOLUME_GAIN_MAX_DB}.")

    return await _submit_audio_tool(
        file,
        job_type="volume",
        tool="VOLUME",
        metric="/volume",
        build_work=lambda inp, out: (lambda: run_blocking(apply_volume_gain, inp, out, gain_db)),
        log_detail=f"{gain_db:+.1f}dB",
        generic_error="Volume adjustment failed unexpectedly.",
    )


@router.get("/volume/status/{job_id}")
async def volume_status(job_id: str):
    return _tool_status(job_id, "volume")


@router.get("/volume/preview/{job_id}")
async def volume_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "volume")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/volume/download/{job_id}")
async def volume_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "volume")
    return FileResponse(path, media_type="application/octet-stream", filename=f"volume_adjusted.{fmt}")


# ---------- /pitch ----------

@router.post(
    "/pitch",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_PITCH_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_PITCH_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def pitch_route(file: UploadFile = File(...), semitones: float = Form(...)):
    """Pitch shift, independent of tempo (rubberband)."""
    if semitones < PITCH_SHIFT_MIN_SEMITONES or semitones > PITCH_SHIFT_MAX_SEMITONES:
        raise HTTPException(
            400,
            f"semitones must be between {PITCH_SHIFT_MIN_SEMITONES} and {PITCH_SHIFT_MAX_SEMITONES}."
        )

    return await _submit_audio_tool(
        file,
        job_type="pitch",
        tool="PITCH",
        metric="/pitch",
        build_work=lambda inp, out: (lambda: run_blocking(shift_pitch, inp, out, semitones)),
        log_detail=f"{semitones:+.1f} semitones",
        generic_error="Pitch shift failed unexpectedly.",
    )


@router.get("/pitch/status/{job_id}")
async def pitch_status(job_id: str):
    return _tool_status(job_id, "pitch")


@router.get("/pitch/preview/{job_id}")
async def pitch_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "pitch")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/pitch/download/{job_id}")
async def pitch_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "pitch")
    return FileResponse(path, media_type="application/octet-stream", filename=f"pitch_shifted.{fmt}")


# ---------- /tempo ----------

@router.post(
    "/tempo",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_TEMPO_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_TEMPO_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def tempo_route(file: UploadFile = File(...), tempo_factor: float = Form(...)):
    """Tempo/speed change, independent of pitch (rubberband)."""
    if tempo_factor < TEMPO_MIN_FACTOR or tempo_factor > TEMPO_MAX_FACTOR:
        raise HTTPException(400, f"tempo_factor must be between {TEMPO_MIN_FACTOR} and {TEMPO_MAX_FACTOR}.")

    return await _submit_audio_tool(
        file,
        job_type="tempo",
        tool="TEMPO",
        metric="/tempo",
        build_work=lambda inp, out: (lambda: run_blocking(change_tempo, inp, out, tempo_factor)),
        log_detail=f"x{tempo_factor:.2f}",
        generic_error="Tempo change failed unexpectedly.",
    )


@router.get("/tempo/status/{job_id}")
async def tempo_status(job_id: str):
    return _tool_status(job_id, "tempo")


@router.get("/tempo/preview/{job_id}")
async def tempo_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "tempo")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/tempo/download/{job_id}")
async def tempo_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "tempo")
    return FileResponse(path, media_type="application/octet-stream", filename=f"tempo_changed.{fmt}")


# ---------- /reverse ----------

@router.post(
    "/reverse",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_REVERSE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_REVERSE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def reverse_route(file: UploadFile = File(...)):
    """Reverse playback."""
    return await _submit_audio_tool(
        file,
        job_type="reverse",
        tool="REVERSE",
        metric="/reverse",
        build_work=lambda inp, out: (lambda: run_blocking(reverse_audio, inp, out)),
        generic_error="Reverse failed unexpectedly.",
    )


@router.get("/reverse/status/{job_id}")
async def reverse_status(job_id: str):
    return _tool_status(job_id, "reverse")


@router.get("/reverse/preview/{job_id}")
async def reverse_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "reverse")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/reverse/download/{job_id}")
async def reverse_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "reverse")
    return FileResponse(path, media_type="application/octet-stream", filename=f"reversed.{fmt}")


# ---------- /noise-remove ----------

@router.post(
    "/noise-remove",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_NOISE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_NOISE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def noise_remove_route(file: UploadFile = File(...), strength: float = Form(12.0)):
    """Background noise reduction (ffmpeg afftdn)."""
    if strength < NOISE_REDUCTION_MIN_STRENGTH or strength > NOISE_REDUCTION_MAX_STRENGTH:
        raise HTTPException(
            400,
            f"strength must be between {NOISE_REDUCTION_MIN_STRENGTH} and {NOISE_REDUCTION_MAX_STRENGTH}."
        )

    return await _submit_audio_tool(
        file,
        job_type="noise_remove",
        tool="NOISE",
        metric="/noise-remove",
        build_work=lambda inp, out: (lambda: run_blocking(remove_noise, inp, out, strength)),
        log_detail=f"strength={strength}",
        generic_error="Noise removal failed unexpectedly.",
    )


@router.get("/noise-remove/status/{job_id}")
async def noise_remove_status(job_id: str):
    return _tool_status(job_id, "noise_remove")


@router.get("/noise-remove/preview/{job_id}")
async def noise_remove_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "noise_remove")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/noise-remove/download/{job_id}")
async def noise_remove_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "noise_remove")
    return FileResponse(path, media_type="application/octet-stream", filename=f"denoised.{fmt}")


# ---------- /voice-clean ----------

@router.post(
    "/voice-clean",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_VOICE_CLEAN_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_VOICE_CLEAN_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def voice_clean_route(file: UploadFile = File(...)):
    """Speech-optimized cleanup preset."""
    return await _submit_audio_tool(
        file,
        job_type="voice_clean",
        tool="VOICE_CLEAN",
        metric="/voice-clean",
        build_work=lambda inp, out: (lambda: run_blocking(clean_voice, inp, out)),
        generic_error="Voice cleanup failed unexpectedly.",
    )


@router.get("/voice-clean/status/{job_id}")
async def voice_clean_status(job_id: str):
    return _tool_status(job_id, "voice_clean")


@router.get("/voice-clean/preview/{job_id}")
async def voice_clean_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "voice_clean")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/voice-clean/download/{job_id}")
async def voice_clean_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "voice_clean")
    return FileResponse(path, media_type="application/octet-stream", filename=f"voice_cleaned.{fmt}")


# ---------- /echo-remove ----------

@router.post(
    "/echo-remove",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_ECHO_REMOVE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_ECHO_REMOVE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def echo_remove_route(file: UploadFile = File(...)):
    """Echo / reverb tail suppression."""
    return await _submit_audio_tool(
        file,
        job_type="echo_remove",
        tool="ECHO_REMOVE",
        metric="/echo-remove",
        build_work=lambda inp, out: (lambda: run_blocking(remove_echo, inp, out)),
        generic_error="Echo removal failed unexpectedly.",
    )


@router.get("/echo-remove/status/{job_id}")
async def echo_remove_status(job_id: str):
    return _tool_status(job_id, "echo_remove")


@router.get("/echo-remove/preview/{job_id}")
async def echo_remove_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "echo_remove")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/echo-remove/download/{job_id}")
async def echo_remove_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "echo_remove")
    return FileResponse(path, media_type="application/octet-stream", filename=f"echo_removed.{fmt}")


# ---------- /silence-remove ----------

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
    """Strips silent gaps throughout the recording."""
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

    return await _submit_audio_tool(
        file,
        job_type="silence_remove",
        tool="SILENCE_REMOVE",
        metric="/silence-remove",
        build_work=lambda inp, out: (
            lambda: run_blocking(remove_silence, inp, out, threshold_db, min_duration_seconds)
        ),
        log_detail=f"threshold={threshold_db}dB min_dur={min_duration_seconds}s",
        generic_error="Silence removal failed unexpectedly.",
    )


@router.get("/silence-remove/status/{job_id}")
async def silence_remove_status(job_id: str):
    return _tool_status(job_id, "silence_remove")


@router.get("/silence-remove/preview/{job_id}")
async def silence_remove_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "silence_remove")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/silence-remove/download/{job_id}")
async def silence_remove_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "silence_remove")
    return FileResponse(path, media_type="application/octet-stream", filename=f"silence_removed.{fmt}")


# ---------- /loudnorm ----------

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
    """Two-pass LUFS loudness normalization."""
    try:
        target_lufs = resolve_target_lufs(preset, custom_lufs)
    except AudioToolError as e:
        raise HTTPException(400, str(e))

    return await _submit_audio_tool(
        file,
        job_type="loudnorm",
        tool="LOUDNORM",
        metric="/loudnorm",
        build_work=lambda inp, out: (lambda: run_blocking(normalize_loudness, inp, out, target_lufs)),
        log_detail=f"-> {target_lufs} LUFS",
        generic_error="Loudness normalization failed unexpectedly.",
    )


@router.get("/loudnorm/status/{job_id}")
async def loudnorm_status(job_id: str):
    return _tool_status(job_id, "loudnorm")


@router.get("/loudnorm/preview/{job_id}")
async def loudnorm_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "loudnorm")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/loudnorm/download/{job_id}")
async def loudnorm_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "loudnorm")
    return FileResponse(path, media_type="application/octet-stream", filename=f"normalized.{fmt}")


# ---------- /fade ----------

@router.post(
    "/fade",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=FADE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=FADE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def fade_route(
    file: UploadFile = File(...),
    fade_in_seconds: float = Form(0.0),
    fade_out_seconds: float = Form(0.0),
):
    """Fade in and/or out."""
    if fade_in_seconds <= 0 and fade_out_seconds <= 0:
        raise HTTPException(400, "At least one of fade_in_seconds or fade_out_seconds must be greater than 0.")
    if fade_in_seconds < 0 or fade_in_seconds > FADE_MAX_SECONDS:
        raise HTTPException(400, f"fade_in_seconds must be between 0 and {FADE_MAX_SECONDS}.")
    if fade_out_seconds < 0 or fade_out_seconds > FADE_MAX_SECONDS:
        raise HTTPException(400, f"fade_out_seconds must be between 0 and {FADE_MAX_SECONDS}.")

    source_format = _validated_input_format(file.filename)

    return await _submit_audio_tool(
        file,
        job_type="fade",
        tool="FADE",
        metric="/fade",
        build_work=lambda inp, out: (
            lambda: run_blocking(apply_fade, inp, out, source_format, fade_in_seconds, fade_out_seconds)
        ),
        log_detail=f"in={fade_in_seconds}s out={fade_out_seconds}s",
        generic_error="Fade failed unexpectedly.",
    )


@router.get("/fade/status/{job_id}")
async def fade_status(job_id: str):
    return _tool_status(job_id, "fade")


@router.get("/fade/preview/{job_id}")
async def fade_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "fade")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/fade/download/{job_id}")
async def fade_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "fade")
    return FileResponse(path, media_type="application/octet-stream", filename=f"faded.{fmt}")


# ---------- /channels ----------

@router.post(
    "/channels",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=CHANNELS_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=CHANNELS_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def channels_route(file: UploadFile = File(...), target: str = Form(...)):
    """Mono <-> stereo conversion."""
    target = target.strip().lower()
    if target not in ("mono", "stereo"):
        raise HTTPException(400, "target must be 'mono' or 'stereo'.")

    source_format = _validated_input_format(file.filename)

    return await _submit_audio_tool(
        file,
        job_type="channels",
        tool="CHANNELS",
        metric="/channels",
        build_work=lambda inp, out: (
            lambda: run_blocking(convert_channels, inp, out, source_format, target)
        ),
        log_detail=f"-> {target}",
        generic_error="Channel conversion failed unexpectedly.",
    )


@router.get("/channels/status/{job_id}")
async def channels_status(job_id: str):
    return _tool_status(job_id, "channels")


@router.get("/channels/preview/{job_id}")
async def channels_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "channels")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/channels/download/{job_id}")
async def channels_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "channels")
    return FileResponse(path, media_type="application/octet-stream", filename=f"converted.{fmt}")


# ---------- /resample ----------

@router.post(
    "/resample",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=RESAMPLE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=RESAMPLE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def resample_route(
    file: UploadFile = File(...),
    sample_rate: int = Form(...),
    bit_depth: int = Form(None),
):
    """Sample rate / bit depth conversion."""
    if sample_rate not in RESAMPLE_ALLOWED_RATES:
        raise HTTPException(
            400, f"sample_rate must be one of: {', '.join(str(r) for r in RESAMPLE_ALLOWED_RATES)}"
        )
    if bit_depth is not None and bit_depth not in RESAMPLE_ALLOWED_BIT_DEPTHS:
        raise HTTPException(
            400, f"bit_depth must be one of: {', '.join(str(b) for b in RESAMPLE_ALLOWED_BIT_DEPTHS)}"
        )

    source_format = _validated_input_format(file.filename)

    return await _submit_audio_tool(
        file,
        job_type="resample",
        tool="RESAMPLE",
        metric="/resample",
        build_work=lambda inp, out: (
            lambda: run_blocking(resample_audio, inp, out, source_format, sample_rate, bit_depth)
        ),
        log_detail=f"-> {sample_rate}Hz" + (f"/{bit_depth}bit" if bit_depth else ""),
        generic_error="Resampling failed unexpectedly.",
    )


@router.get("/resample/status/{job_id}")
async def resample_status(job_id: str):
    return _tool_status(job_id, "resample")


@router.get("/resample/preview/{job_id}")
async def resample_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "resample")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/resample/download/{job_id}")
async def resample_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "resample")
    return FileResponse(path, media_type="application/octet-stream", filename=f"resampled.{fmt}")


# ---------- /ringtone ----------
#
# .m4r is not a distinct codec - it's an M4A (AAC) file that iOS
# recognizes by extension. make_ringtone() writes standard .m4a bytes;
# only the download route's filename carries .m4r.

@router.post(
    "/ringtone",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=RINGTONE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=RINGTONE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def ringtone_route(
    file: UploadFile = File(...),
    start_seconds: float = Form(0.0),
    duration_seconds: float = Form(30.0),
):
    """Trim + M4A-as-M4R ringtone maker."""
    if duration_seconds <= 0 or duration_seconds > RINGTONE_MAX_DURATION_SECONDS:
        raise HTTPException(400, f"duration_seconds must be between 0 and {RINGTONE_MAX_DURATION_SECONDS}.")
    if start_seconds < 0:
        raise HTTPException(400, "start_seconds must be non-negative.")

    return await _submit_audio_tool(
        file,
        job_type="ringtone",
        tool="RINGTONE",
        metric="/ringtone",
        output_format="m4a",
        build_work=lambda inp, out: (
            lambda: run_blocking(make_ringtone, inp, out, start_seconds, duration_seconds)
        ),
        log_detail=f"[{start_seconds}s +{duration_seconds}s]",
        generic_error="Ringtone creation failed unexpectedly.",
    )


@router.get("/ringtone/status/{job_id}")
async def ringtone_status(job_id: str):
    return _tool_status(job_id, "ringtone")


@router.get("/ringtone/preview/{job_id}")
async def ringtone_preview(job_id: str):
    path, _ = _resolve_tool_output_path(job_id, "ringtone")
    return FileResponse(path, media_type="audio/mp4")


@router.get("/ringtone/download/{job_id}")
async def ringtone_download(job_id: str):
    path, _ = _resolve_tool_output_path(job_id, "ringtone")
    return FileResponse(path, media_type="audio/mp4", filename="ringtone.m4r")