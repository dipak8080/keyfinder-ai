"""
routes/midi.py - /audio-to-midi: polyphonic transcription via the
isolated midi-worker sidecar (basic-pitch + TensorFlow, separate
container).

Split out of the old monolithic routes.py (2026-08-14 restructure). Pure
move: every docstring, comment, and line of logic here is unchanged from
its original location. Nothing in this file changes behaviour.

See audio_to_midi.py's module docstring for why this calls out to a
sidecar over HTTP instead of running basic-pitch in this process -
short version: basic-pitch's tensorflow<2.15.1 dependency hard-pins
numpy<2.0.0, which conflicts with this app's numpy==2.3.5 used by
essentia/librosa/demucs/torch. Full process isolation was the only
option that added zero risk to the existing product.
"""
import os
from functools import partial

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends
from fastapi.responses import FileResponse

from config import (
    MIDI_RATE_LIMIT_MAX_REQUESTS,
    MIDI_RATE_LIMIT_WINDOW_SECONDS,
    MAX_MIDI_DURATION_SECONDS,
    MIN_MIDI_DURATION_SECONDS,
    MIDI_INPUT_FORMATS,
)
from utils import run_blocking, _midi_semaphore
from rate_limit import check_rate_limit
from jobs import get_job
from audio_common import get_audio_mime_type
from audio_to_midi import convert_to_midi

from ._shared import _submit_audio_tool, _tool_status, _resolve_tool_output_path

router = APIRouter()


@router.post(
    "/audio-to-midi",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=MIDI_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=MIDI_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def audio_to_midi_route(
    file: UploadFile = File(...),
    onset_threshold: float = Form(0.5),
    frame_threshold: float = Form(0.3),
    minimum_note_length: float = Form(127.70),
    minimum_frequency: float = Form(None),
    maximum_frequency: float = Form(None),
):
    """Transcribe audio to MIDI via the isolated midi-worker sidecar.

    Accuracy note: basic-pitch is designed for ONE instrument at a time.
    A full mix transcribes noticeably worse than a single separated stem,
    so the frontend should point users at /stems first for best results.
    """
    if not 0.05 <= onset_threshold <= 0.95:
        raise HTTPException(400, "onset_threshold must be between 0.05 and 0.95.")
    if not 0.05 <= frame_threshold <= 0.95:
        raise HTTPException(400, "frame_threshold must be between 0.05 and 0.95.")
    if not 10 <= minimum_note_length <= 2000:
        raise HTTPException(400, "minimum_note_length must be between 10 and 2000 ms.")
    if minimum_frequency is not None and not 20 <= minimum_frequency <= 5000:
        raise HTTPException(400, "minimum_frequency must be between 20 and 5000 Hz.")
    if maximum_frequency is not None and not 20 <= maximum_frequency <= 20000:
        raise HTTPException(400, "maximum_frequency must be between 20 and 20000 Hz.")
    if (minimum_frequency is not None and maximum_frequency is not None
            and minimum_frequency >= maximum_frequency):
        raise HTTPException(400, "minimum_frequency must be less than maximum_frequency.")

    return await _submit_audio_tool(
        file,
        job_type="audio_to_midi",
        tool="AUDIO_TO_MIDI",
        metric="/audio-to-midi",
        output_format="mid",
        max_duration_seconds=MAX_MIDI_DURATION_SECONDS,
        min_duration_seconds=MIN_MIDI_DURATION_SECONDS,
        allowed_input_formats=MIDI_INPUT_FORMATS,
        build_work=lambda inp, out: (lambda: run_blocking(
            convert_to_midi, inp, out,
            onset_threshold, frame_threshold, minimum_note_length,
            minimum_frequency, maximum_frequency,
        )),
        log_detail=f"onset={onset_threshold} frame={frame_threshold} min_len={minimum_note_length}ms",
        generic_error="MIDI conversion failed unexpectedly.",
        semaphore=_midi_semaphore,
    )


@router.get("/audio-to-midi/status/{job_id}")
async def audio_to_midi_status(job_id: str):
    return _tool_status(job_id, "audio_to_midi")


@router.get("/audio-to-midi/preview/{job_id}")
async def audio_to_midi_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "audio_to_midi")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/audio-to-midi/download/{job_id}")
async def audio_to_midi_download(job_id: str):
    """
    Downloads named after the ORIGINAL uploaded file, not a generic
    "transcribed.mid" - deliberately different from every other tool's
    download route (which all use a fixed generic name like
    "converted.mp3"). A MIDI file loaded straight into a DAW is far more
    useful named after the source track ("song.mid") than a batch of
    "transcribed.mid" files from different sessions colliding on disk or
    being impossible to tell apart once downloaded.

    Falls back to "transcribed" only if the job has no recorded title at
    all (shouldn't normally happen, but the download must never 500 over
    a missing/empty original filename).
    """
    path, fmt = _resolve_tool_output_path(job_id, "audio_to_midi")
    job = get_job(job_id)
    original_title = (job.get("title") if job else None) or "transcribed"

    # Strips the ORIGINAL extension (.mp3, .wav, etc.) so it isn't
    # doubled up with the new .mid - "song.mp3" -> "song", not
    # "song.mp3.mid". safe_extension-style sanitization isn't needed
    # here since this only sets a Content-Disposition header value, not
    # a filesystem path - unlike build_temp_input_path's use case, no
    # path traversal or filesystem-length concern applies.
    base_name = os.path.splitext(original_title)[0].strip() or "transcribed"

    return FileResponse(
        path,
        media_type="application/octet-stream",
        filename=f"{base_name}.{fmt}",
    )