"""
routes/sheet_music.py - /audio-to-sheet: audio -> engraved notation
(PDF + SVG + MusicXML + MIDI), metered per the "audio-to-sheet" credit rule.

WHERE THIS FITS: the sheet/ package is the engine (transcribe -> notation ->
engrave); this route is the HTTP front door and job glue. It follows
midi_hq.py's shape almost exactly - same order of checks, same paywall guard,
same _run_tool_job hand-off - with two deliberate differences:

  1. FOUR output formats, not one. run_sheet_job writes PDF (primary), SVG,
     MusicXML and MIDI; the primary goes through mark_tool_complete and the
     full path set is carried in result_data for the format-aware download
     route below. MusicXML is the load-bearing one for the product ("fix it
     in MuseScore"); MIDI matches what Klangio/Songscription export.

  2. A REAL preview. Unlike MIDI, sheet music renders in a browser, so
     /preview serves the SVG - the "prove the quality before you pay" surface
     the whole free tier depends on.

Engine choice (Transkun for piano, YourMT3 otherwise) lives in the sheet
runner, not here - this route is engine-agnostic.
"""
import asyncio
import os
import re

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends, Query
from fastapi.responses import FileResponse, JSONResponse

import config
from config import logger, AUDIO_TOOL_JOB_TTL_SECONDS
from jobs import (
    create_job,
    mark_tool_complete,
    mark_data_complete,
    mark_failed,
    get_job,
    count_processing,
)
from audio_common import build_output_path
from utils import cleanup_file
from log_stream import set_job_context, remember_job_tags, tag_from_job

from sheet import run_sheet_job, SheetParams

from credits import paywall, metering
from credits.identity import Identity
from credits.limits import tiered_rate_limit

from ._shared import (
    spawn_background_task,
    _accept_upload,
    _validate_duration_or_reject,
    _log_queued,
    _run_tool_job,
)

router = APIRouter()

TOOL = "AUDIO_TO_SHEET"
METRIC = "/audio-to-sheet"
JOB_TYPE = "audio_to_sheet"
TOOL_KEY = "audio-to-sheet"   # credits rule key, see credits/config.py


# --- config (getattr so this imports cleanly before Step 7 adds them) -------
SHEET_ENABLED = bool(getattr(config, "SHEET_MUSIC_ENABLED", False))
SHEET_RATE_MAX = int(getattr(config, "SHEET_MUSIC_RATE_LIMIT_MAX_REQUESTS", 30))
SHEET_RATE_WINDOW = int(getattr(config, "SHEET_MUSIC_RATE_LIMIT_WINDOW_SECONDS", 3600))
MAX_SHEET_DURATION = int(getattr(config, "MAX_SHEET_MUSIC_DURATION_SECONDS", 900))
MIN_SHEET_DURATION = float(getattr(config, "MIN_SHEET_MUSIC_DURATION_SECONDS", 2))
SHEET_INPUT_FORMATS = set(getattr(
    config, "SHEET_MUSIC_INPUT_FORMATS",
    {"mp3", "wav", "flac", "m4a", "aac", "ogg", "opus", "webm"},
))
MAX_CONCURRENT_SHEET = int(getattr(config, "MAX_CONCURRENT_SHEET_MUSIC", 2))
MAX_QUEUED_SHEET = int(getattr(config, "MAX_QUEUED_SHEET_MUSIC", 6))

# Its own pool, like _midi_hq_semaphore: a sheet job spends GPU (transcription)
# and CPU (engrave) that no other tool's pool bounds. Kept local to the route
# rather than in utils.py only to keep this shippable without an extra edit;
# move it there if you prefer all semaphores in one place.
_sheet_semaphore = asyncio.Semaphore(MAX_CONCURRENT_SHEET)

_ALLOWED_INSTRUMENTS = ("auto", "piano", "guitar", "mix")
_ALLOWED_GRID = (1, 2, 3, 4, 6, 8)
_TS_RE = re.compile(r"^\s*\d+\s*/\s*\d+\s*$")

# result_data["_paths"] keys -> file extension + media type for downloads.
_FORMAT_EXT = {"pdf": "pdf", "svg": "svg", "musicxml": "musicxml", "midi": "mid"}
_MEDIA_TYPES = {
    "pdf": "application/pdf",
    "svg": "image/svg+xml",
    "musicxml": "application/vnd.recordare.musicxml+xml",
    "mid": "audio/midi",
}
_DOWNLOAD_FORMATS = frozenset(_FORMAT_EXT.keys())


def _require_available() -> None:
    if not SHEET_ENABLED:
        logger.info(f"[{TOOL}] Request rejected - SHEET_MUSIC_ENABLED is false")
        raise HTTPException(
            503,
            "Audio-to-sheet-music is currently turned off. Please try again later.",
        )


def _reject_if_sheet_queue_full() -> None:
    """Whole-server capacity bound for /audio-to-sheet, mirroring
    _reject_if_midi_hq_queue_full. The rate limiter is per-IP; this refuses
    unbounded in-memory queueing when many callers submit at once, each
    holding an upload on disk and a job row. 503, not 429: the caller did
    nothing wrong, the server is full.
    """
    depth = count_processing((JOB_TYPE,))
    if depth >= MAX_QUEUED_SHEET:
        logger.warning(f"[{TOOL}] Rejected submission - queue full ({depth}/{MAX_QUEUED_SHEET})")
        raise HTTPException(
            503,
            "The sheet-music queue is full right now. These jobs take about a "
            "minute each - please try again shortly.",
        )


def _validated_input_format(filename: str) -> str:
    if not filename:
        raise HTTPException(400, "No file was uploaded. Please choose a file and try again.")
    ext = (os.path.splitext(filename)[1] or "").lstrip(".").lower()
    if ext not in SHEET_INPUT_FORMATS:
        raise HTTPException(
            400,
            f"Unsupported file type '.{ext}'. Supported formats: "
            f"{', '.join(sorted(SHEET_INPUT_FORMATS))}.",
        )
    return ext


def _validated_options(
    instrument: str,
    grid: int,
    time_signature: str,
    isolate: bool,
    tempo_bpm: float | None,
) -> str:
    """Cheap, already-parsed checks - run before any disk or GPU work so a
    typo is an instant 400 rather than a charged failure. Returns the
    normalised instrument.
    """
    instrument = (instrument or "auto").lower()
    if instrument not in _ALLOWED_INSTRUMENTS:
        raise HTTPException(400, f"instrument must be one of: {', '.join(_ALLOWED_INSTRUMENTS)}.")
    if grid not in _ALLOWED_GRID:
        raise HTTPException(400, f"grid must be one of: {', '.join(str(g) for g in _ALLOWED_GRID)}.")
    if not _TS_RE.match(time_signature or ""):
        raise HTTPException(400, "time_signature must look like '4/4'.")
    if isolate and instrument == "piano":
        # Piano isolation IS supported (htdemucs_6s piano stem), but isolating
        # then transcribing a stem that is already solo piano just wastes a
        # separation pass - guide the caller rather than silently paying for it.
        pass
    if tempo_bpm is not None and not (20 <= tempo_bpm <= 400):
        raise HTTPException(400, "tempo_bpm must be between 20 and 400 if provided.")
    return instrument


def _record_gpu_cost(job_id: str, result: dict) -> None:
    """The transcription is the GPU stage; its timing sits in the worker's
    stats under _tx_stats._gpu (piano_gpu / midi_hq_gpu both report it in the
    shape credits/metering.py expects). Same swallow-everything discipline as
    midi_hq._record_gpu_cost: a metering failure must never fail a job whose
    GPU time was already spent.
    """
    try:
        gpu = ((result or {}).get("_tx_stats") or {}).get("_gpu") or {}
        gpu_seconds = float(gpu.get("fetch_seconds") or 0) + float(gpu.get("infer_seconds") or 0)
        if gpu_seconds <= 0:
            return
        metering.record_job_finished(job_id, status="completed", gpu_seconds=gpu_seconds)
    except Exception:  # noqa: BLE001
        logger.exception("[%s] could not record GPU cost for job %s", TOOL, job_id)


@router.post(
    "/audio-to-sheet",
    dependencies=[Depends(tiered_rate_limit(
        TOOL_KEY,
        free_max=SHEET_RATE_MAX,
        free_window=SHEET_RATE_WINDOW,
    ))],
)
async def audio_to_sheet_route(
    file: UploadFile = File(...),
    instrument: str = Form("auto"),
    isolate: bool = Form(False),
    hand_split: bool = Form(True),
    grid: int = Form(4),
    time_signature: str = Form("4/4"),
    tempo_bpm: float = Form(None),
    key_name: str = Form(None),
    identity: Identity = paywall.IdentityDep,
):
    """Transcribe audio to engraved sheet music. Metered per the
    "audio-to-sheet" rule (free_under_seconds gives the short-clip free tier).

    Poll GET /audio-to-sheet/status/{job_id}, then GET .../result for the
    stats and available formats, GET .../preview for the SVG, and
    GET .../download/{job_id}?format=pdf|svg|musicxml|midi for the files.

    Form fields (all optional except the file):
        instrument     auto | piano | guitar | mix. piano routes to Transkun.
        isolate        run stem isolation first (full-mix inputs).
        hand_split     split piano into two staves (grand staff). Default on.
        grid           quantization subdivisions per quarter (1,2,3,4,6,8).
        time_signature e.g. "4/4".
        tempo_bpm      override the detected tempo (20-400).
        key_name       override the detected key, e.g. "C major".
    """
    set_job_context(tool=TOOL, tier="sheet")

    # --- 1-4: free checks, before any disk or job is touched ---
    _require_available()
    instrument = _validated_options(instrument, grid, time_signature, isolate, tempo_bpm)
    _validated_input_format(file.filename)
    original_filename = file.filename

    # --- 5: capacity gate, last of the free checks ---
    _reject_if_sheet_queue_full()

    # --- 6: from here on we own resources that need cleaning up ---
    job_id = create_job(job_type=JOB_TYPE, ttl_seconds=AUDIO_TOOL_JOB_TTL_SECONDS)
    remember_job_tags(job_id)

    input_path, size = await _accept_upload(file, job_id, label=JOB_TYPE)
    pdf_path = build_output_path(job_id, "pdf")
    svg_path = build_output_path(job_id, "svg")
    xml_path = build_output_path(job_id, "musicxml")
    midi_path = build_output_path(job_id, "mid")

    # --- 7: duration, both ends ---
    duration = await _validate_duration_or_reject(job_id, input_path, MAX_SHEET_DURATION)
    if duration < MIN_SHEET_DURATION:
        cleanup_file(input_path)
        message = f"Audio is too short ({duration:.1f}s). Minimum is {MIN_SHEET_DURATION:.0f}s."
        mark_failed(job_id, message)
        raise HTTPException(400, message)

    params = SheetParams(
        instrument=instrument,
        isolate=bool(isolate),
        hand_split=bool(hand_split),
        grid=int(grid),
        time_signature=time_signature,
        tempo_bpm=tempo_bpm,
        key_name=key_name,
        want_pdf=True,
        want_svg=True,
        title=(os.path.splitext(original_filename)[0].strip() or "Transcription"),
    )

    # --- 8: charge, then enqueue ---
    try:
        async with paywall.guard(
            identity, job_id=job_id, tool=TOOL_KEY, input_seconds=duration
        ) as charge:
            metering.record_job_created(
                job_id=job_id,
                tool=TOOL_KEY,
                subject_id=identity.subject_id,
                account_id=identity.account_id,
                ip_hash=identity.ip_hash,
                input_seconds=duration,
                input_bytes=size,
                charge_type=charge.charge_type,
            )

            spawn_background_task(_run_tool_job(
                tool=TOOL,
                metric=METRIC,
                job_id=job_id,
                semaphore=_sheet_semaphore,
                work=lambda: run_sheet_job(
                    job_id=job_id,
                    input_path=input_path,
                    pdf_path=pdf_path,
                    svg_path=svg_path,
                    xml_path=xml_path,
                    midi_path=midi_path,
                    params=params,
                ),
                on_success=lambda result: (
                    mark_tool_complete(
                        job_id, original_filename,
                        (result.get("_paths") or {}).get("pdf") or pdf_path, "pdf",
                    ),
                    mark_data_complete(job_id, original_filename, result),
                    _record_gpu_cost(job_id, result),
                ),
                generic_error="Sheet music transcription failed unexpectedly.",
                cleanup_paths=[input_path],
                metered_tool=TOOL_KEY,
                success_detail=lambda r: (
                    f"{r.get('n_notes')} notes, {r.get('n_measures')} measures, "
                    f"{r.get('n_pages')} page(s), engine={r.get('engine')}"
                ),
            ))
    except HTTPException:
        cleanup_file(input_path)
        mark_failed(job_id, "Out of credits.")
        raise

    _log_queued(
        TOOL, job_id, original_filename, size,
        f"{duration:.1f}s instrument={instrument} isolate={isolate} "
        f"hand_split={hand_split} grid={grid} ts={time_signature} "
        f"charge={charge.charge_type}",
    )

    return JSONResponse({
        "job_id": job_id,
        "status": "processing",
        "options": {
            "instrument": instrument,
            "isolate": bool(isolate),
            "hand_split": bool(hand_split),
            "grid": int(grid),
            "time_signature": time_signature,
        },
        "billing": {
            "charged": charge.charge_type,
            "balance": charge.balance_after,
            "free_remaining": charge.free_remaining_after,
        },
    })


@router.get("/audio-to-sheet/status/{job_id}")
async def audio_to_sheet_status(job_id: str):
    from ._shared import _tool_status
    return _tool_status(job_id, JOB_TYPE)


def _complete_job_or_reject(job_id: str) -> dict:
    """Validate the job exists, belongs to this tool, and is complete.
    Shared by result / preview / download.
    """
    tag_from_job(job_id)
    job = get_job(job_id)
    if job is None or job["job_type"] != JOB_TYPE:
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] == "failed":
        raise HTTPException(409, job.get("error") or "This job failed.")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    return job


@router.get("/audio-to-sheet/result/{job_id}")
async def audio_to_sheet_result(job_id: str):
    """The transcription summary as JSON: note/measure/page counts, engine,
    detected tempo/key, and which formats are available. This is the proof
    the tool worked and what the frontend reads to enable the right download
    buttons. Internal filesystem paths (_paths) are stripped.
    """
    job = _complete_job_or_reject(job_id)
    result = job.get("result_data")
    if not result:
        raise HTTPException(404, "Result not found (it may have expired).")
    clean = {k: v for k, v in result.items() if k != "_paths"}
    return JSONResponse(clean)


@router.get("/audio-to-sheet/preview/{job_id}")
async def audio_to_sheet_preview(job_id: str):
    """The engraved score as SVG, rendered inline in the browser - the
    quality-proof surface the free tier depends on. First page only; the full
    paged document is the PDF download.
    """
    job = _complete_job_or_reject(job_id)
    paths = (job.get("result_data") or {}).get("_paths") or {}
    path = paths.get("svg")
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Preview not found (it may have expired).")
    return FileResponse(path, media_type="image/svg+xml")


@router.get("/audio-to-sheet/download/{job_id}")
async def audio_to_sheet_download(job_id: str, format: str = Query("pdf")):
    """Format-aware download. ?format=pdf|svg|musicxml|midi, default pdf.

    Named after the ORIGINAL uploaded file, matching the MIDI tools: a score
    or MusicXML loaded into MuseScore is far more useful named after the
    source track than a batch of "transcription.pdf" colliding on disk.
    """
    fmt = (format or "pdf").lower()
    if fmt not in _DOWNLOAD_FORMATS:
        raise HTTPException(
            400,
            f"format must be one of: {', '.join(sorted(_DOWNLOAD_FORMATS))}.",
        )
    job = _complete_job_or_reject(job_id)
    paths = (job.get("result_data") or {}).get("_paths") or {}
    path = paths.get(fmt)
    if not path or not os.path.exists(path):
        raise HTTPException(404, f"The {fmt} file is not available for this job.")

    base_name = os.path.splitext(job.get("title") or "transcription")[0].strip() or "transcription"
    ext = _FORMAT_EXT[fmt]
    return FileResponse(
        path,
        media_type=_MEDIA_TYPES.get(ext, "application/octet-stream"),
        filename=f"{base_name}.{ext}",
    )