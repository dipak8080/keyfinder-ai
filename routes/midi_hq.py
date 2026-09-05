"""
routes/midi_hq.py - /audio-to-midi-hq: multi-instrument transcription on
a RunPod GPU worker, metered at 1 credit.

WHERE THIS FITS: midi_hq_gpu.py is the client, gpu-worker-mt3/handler.py
is the worker, runpod_client.py carries the job between them.

--------------------------------------------------------------------------
WHY THIS IS ITS OWN ROUTE AND NOT A FLAG ON /audio-to-midi

Two reasons, and the second one is the load-bearing one.

FIRST, the obvious one this codebase already documents twice: rate-limit
dependencies are evaluated BEFORE the request body is read, so a
Depends() cannot see a Form value. Per-quality limits therefore need
per-quality routes. That is the same reason /separate-hq is not
`/separate?quality=hq` and /tiktok-to-mp3 is not `/download?source=tiktok`
(see routes/__init__.py's 2026-08-18 note).

SECOND, and specific to MIDI: these two tools do not share an argument
list, and could not, even if the rate limiter were not in the way.

    /audio-to-midi      basic-pitch. ANY instrument. onset_threshold,
                        frame_threshold, minimum_note_length,
                        minimum_frequency, maximum_frequency. One track.

    /audio-to-midi-hq   YourMT3. MULTI-instrument, one track per
                        instrument with a General MIDI program assigned.
                        ZERO tunable parameters - it is a transformer
                        that emits note events and there is nothing to
                        turn.

A shared route would have to silently ignore onset_threshold and
frame_threshold on the HQ path. Silently, because there is no sensible
error for "this parameter is meaningless at this quality" that does not
read as a bug. That is the kind of thing found by a confused user rather
than by a test.

--------------------------------------------------------------------------
WHAT SURVIVES OF THE FREE TOOL'S CONTROLS

Pitch range and minimum note length DO carry over, applied by the worker
to the OUTPUT MIDI rather than to detection. That is not an
approximation of what basic-pitch does with those parameters - it is
strictly more predictable, because it operates on decided notes. "Nothing
below C2" means exactly that here; in basic-pitch it means "bias the
detector away from low frequencies".

Sensitivity cannot carry over at all. By the time we have MIDI, the
detection decision is made. The frontend must not offer it on this tool,
and this route does not accept it - an unknown form field is ignored by
FastAPI rather than rejected, so a frontend that sends onset_threshold
here gets no error and no effect. Worth knowing when debugging "why did
my preset do nothing".

FREQUENCY IN HZ, PITCH IN MIDI NOTE NUMBERS. The free tool takes
minimum_frequency/maximum_frequency in Hz because basic-pitch's API does.
This one takes min_pitch/max_pitch as MIDI note numbers because that is
what filtering decided notes actually operates on, and because the
frontend already thinks in note numbers - AudioToMidiForm converts to Hz
only at submit time. Passing Hz here would mean converting twice, in
opposite directions, to reach the same place.

--------------------------------------------------------------------------
ORDER OF OPERATIONS, and why each step sits where it does

    1. kill switch          503 - feature off, nothing to spend
    2. backend available    503 - GPU config missing
    3. parameter validation 400 - free, no I/O
    4. filename format      400 - free, no bytes transferred
    5. queue depth          503 - server full, not caller's fault
    6. create job + upload  writes to disk
    7. duration check       spawns ffprobe
    8. paywall guard        402 - charge, then enqueue

Steps 1-5 are pure CPU on already-parsed values, so rejecting there
costs nothing. Doing them after step 6 would burn a job id, a disk
write and a cleanup cycle to tell someone their pitch range was
backwards.

Capacity (5) comes AFTER the input checks (3-4) deliberately: everything
above is the CALLER being wrong (400), that one is the SERVER being full
(503). Someone with an invalid range should be told about the range
rather than turned away for capacity, fix nothing, and hit the 400 on
retry.

The charge is LAST because it is the only step that takes something from
the user. Every cheaper rejection happens first, so a 402 means "you
genuinely cannot pay for a job we would otherwise have run" and nothing
else.
--------------------------------------------------------------------------
"""
import os

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends
from fastapi.responses import FileResponse, JSONResponse

from config import (
    logger,
    MIDI_HQ_ENABLED,
    MIDI_HQ_RATE_LIMIT_MAX_REQUESTS,
    MIDI_HQ_RATE_LIMIT_WINDOW_SECONDS,
    MAX_MIDI_HQ_DURATION_SECONDS,
    MIN_MIDI_HQ_DURATION_SECONDS,
    MIDI_HQ_INPUT_FORMATS,
    MIDI_HQ_PITCH_MIN,
    MIDI_HQ_PITCH_MAX,
    MIDI_HQ_MIN_NOTE_MS_MIN,
    MIDI_HQ_MIN_NOTE_MS_MAX,
    AUDIO_TOOL_JOB_TTL_SECONDS,
)
from utils import _midi_hq_semaphore, cleanup_file
from jobs import create_job, mark_tool_complete, mark_data_complete, mark_failed, get_job
from audio_common import get_audio_mime_type, build_output_path
from log_stream import set_job_context, remember_job_tags, tag_from_job

from midi_hq_gpu import transcribe_to_midi, is_available as midi_hq_available, INSTRUMENTS

from credits import paywall, metering
from credits.identity import Identity
from credits.limits import tiered_rate_limit

from ._shared import (
    spawn_background_task,
    _accept_upload,
    _validate_duration_or_reject,
    _log_queued,
    _run_tool_job,
    _tool_status,
    _resolve_tool_output_path,
    _reject_if_midi_hq_queue_full,
)

router = APIRouter()

TOOL = "AUDIO_TO_MIDI_HQ"
METRIC = "/audio-to-midi-hq"
JOB_TYPE = "audio_to_midi_hq"
TOOL_KEY = "audio-to-midi-hq"   # credits rule key, see credits/config.py
MIX_TOOL_KEY = "audio-to-midi-hq-mix"   # 3-credit rule for instrument=auto|mix


def _require_available() -> None:
    """Two separate 503s, deliberately not collapsed into one.

    MIDI_HQ_ENABLED is a decision - somebody turned this off, on purpose,
    and the honest thing to tell a user is that the feature is off and
    the free tool still works. midi_hq_available() is a fault - the
    RunPod config is incomplete and nobody meant that.

    Same wording to the caller either way; different log levels, because
    one of them needs an operator and the other does not.
    """
    if not MIDI_HQ_ENABLED:
        logger.info(f"[{TOOL}] Request rejected - MIDI_HQ_ENABLED is false")
        raise HTTPException(
            503,
            "High-quality MIDI transcription is currently turned off. "
            "The standard MIDI converter is still available.",
        )
    if not midi_hq_available():
        # midi_hq_gpu logs WHICH env var is missing at error level - not
        # repeated here, and deliberately not surfaced to the caller.
        logger.error(f"[{TOOL}] Request rejected - GPU backend not configured")
        raise HTTPException(
            503,
            "High-quality MIDI transcription is temporarily unavailable. "
            "Please try again later.",
        )


def _validated_input_format(filename: str) -> str:
    """MIDI_HQ_INPUT_FORMATS, not ALLOWED_AUDIO_INPUT_FORMATS.

    Checked here rather than via _shared's _validated_input_format
    because that one validates against the narrower audio-tools set,
    which excludes opus and webm. This tool accepts the same superset the
    free /audio-to-midi does - see MIDI_INPUT_FORMATS in config.py for
    why those two live outside the conversion matrix.

    Two tools that accept different file types under the same name would
    be a genuinely confusing thing to discover by being rejected on one
    and not the other, which is why MIDI_HQ_INPUT_FORMATS is an alias
    rather than its own list.
    """
    if not filename:
        raise HTTPException(400, "No file was uploaded. Please choose a file and try again.")
    ext = (os.path.splitext(filename)[1] or "").lstrip(".").lower()
    if ext not in MIDI_HQ_INPUT_FORMATS:
        raise HTTPException(
            400,
            f"Unsupported file type '.{ext}'. Supported formats: "
            f"{', '.join(sorted(MIDI_HQ_INPUT_FORMATS))}.",
        )
    return ext


def _validated_filters(
    min_pitch: int | None,
    max_pitch: int | None,
    min_note_ms: float | None,
) -> tuple[int | None, int | None, float | None]:
    """Validate the three post-processing controls.

    Every check here is free - these are already-parsed numbers, no I/O -
    which is why this runs before the job exists and before a byte is
    uploaded.

    THE ORDERING CHECK IS THE ONE THAT MATTERS. min >= max is not merely
    invalid, it is the one combination that produces an empty MIDI file
    rather than an error: the worker would filter out every note, return
    NO_NOTES_AFTER_FILTER, and the user would have paid a GPU round trip
    to learn their two sliders were the wrong way round. Rejecting it
    here costs nothing and is the difference between an instant, obvious
    400 and a confusing one-credit failure.

    Bounds are the FULL MIDI range (0-127), not a piano keyboard. See
    MIDI_HQ_PITCH_MIN in config.py: YourMT3 is multi-instrument and can
    legitimately emit notes outside a piano's range, so clamping to
    21-108 at the API would silently discard bass and percussion. A
    frontend is free to present a narrower picker.
    """
    if min_pitch is not None and not (MIDI_HQ_PITCH_MIN <= min_pitch <= MIDI_HQ_PITCH_MAX):
        raise HTTPException(
            400,
            f"min_pitch must be a MIDI note number between "
            f"{MIDI_HQ_PITCH_MIN} and {MIDI_HQ_PITCH_MAX}.",
        )
    if max_pitch is not None and not (MIDI_HQ_PITCH_MIN <= max_pitch <= MIDI_HQ_PITCH_MAX):
        raise HTTPException(
            400,
            f"max_pitch must be a MIDI note number between "
            f"{MIDI_HQ_PITCH_MIN} and {MIDI_HQ_PITCH_MAX}.",
        )
    if min_pitch is not None and max_pitch is not None and min_pitch >= max_pitch:
        raise HTTPException(400, "min_pitch must be lower than max_pitch.")

    if min_note_ms is not None and not (
        MIDI_HQ_MIN_NOTE_MS_MIN <= min_note_ms <= MIDI_HQ_MIN_NOTE_MS_MAX
    ):
        raise HTTPException(
            400,
            f"min_note_ms must be between {MIDI_HQ_MIN_NOTE_MS_MIN:.0f} and "
            f"{MIDI_HQ_MIN_NOTE_MS_MAX:.0f} ms.",
        )

    # Normalised to None so midi_hq_gpu omits the key entirely rather
    # than sending an explicit zero the worker would read as a filter.
    return (
        min_pitch,
        max_pitch,
        min_note_ms if min_note_ms else None,
    )


def _record_gpu_cost(job_id: str, result: dict) -> None:
    """Write the worker's reported timing against this job's metrics row.

    WHY THIS IS NOT IN _run_tool_job. That runner's metered_tool= hook
    records only the terminal STATUS, deliberately: the honest
    gpu_seconds figure comes from what the worker measured for its own
    run, and the runner has no access to it - `result` is opaque to it
    by design, since eighteen other tools share the same function.
    Wall clock measured there would also span RunPod queue wait and cold
    start, and recording that as GPU-seconds would inflate every cost
    estimate in a direction that looks entirely plausible.

    So the number is recorded HERE, at the one call site that sees both
    the job id and the worker's payload. transcription.py does the same
    thing one layer down for the same reason.

    ORDER IS SAFE EITHER WAY. This runs from on_success; _run_tool_job's
    `finally` then writes the status. record_job_finished COALESCEs every
    optional column, so whichever lands second cannot blank what the
    first recorded.

    Swallows everything, like every function in credits/metering.py: a
    metering failure must never turn a working transcription into a
    failed one. A missing row costs one data point; an exception here
    would cost the user their MIDI after the GPU time was already paid
    for.
    """
    try:
        gpu = (result or {}).get("_gpu") or {}
        gpu_seconds = float(gpu.get("fetch_seconds") or 0) + float(gpu.get("infer_seconds") or 0)
        if gpu_seconds <= 0:
            # Older worker image, or a malformed payload. Recording zero
            # would be worse than recording nothing: it drags the average
            # cost-per-job down with a job that certainly cost something.
            return
        metering.record_job_finished(job_id, status="completed", gpu_seconds=gpu_seconds)
    except Exception:  # noqa: BLE001
        logger.exception("[%s] could not record GPU cost for job %s", TOOL, job_id)


@router.post(
    "/audio-to-midi-hq",
    dependencies=[Depends(tiered_rate_limit(
        TOOL_KEY,
        free_max=MIDI_HQ_RATE_LIMIT_MAX_REQUESTS,
        free_window=MIDI_HQ_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def audio_to_midi_hq_route(
    file: UploadFile = File(...),
    min_pitch: int = Form(None),
    max_pitch: int = Form(None),
    min_note_ms: float = Form(None),
    instrument: str = Form("auto"),
    isolate: bool = Form(False),
    identity: Identity = paywall.IdentityDep,
):
    """Multi-instrument transcription on the GPU. 1 credit.

    Poll GET /audio-to-midi-hq/status/{job_id}, then
    GET /audio-to-midi-hq/download/{job_id} once complete.

    Form fields (all optional except the file):
        min_pitch    MIDI note number, inclusive. Notes below are dropped.
        max_pitch    MIDI note number, inclusive. Notes above are dropped.
        min_note_ms  Notes shorter than this are dropped, in milliseconds.
        instrument   auto | piano | mix | guitar. Picks the engine - see
                     midi_hq_gpu.py. Default auto = YourMT3.
        isolate      guitar only. Run htdemucs_6s first and transcribe
                     the guitar stem. Use for guitar inside a full mix.

    NOT accepted, and deliberately: onset_threshold, frame_threshold.
    Those control basic-pitch's DETECTION and have no counterpart in this
    model - see the module docstring. FastAPI ignores unknown form
    fields, so sending them is harmless and has no effect.

    tiered_rate_limit brings the affordability pre-check with it: someone
    out of credits gets a 402 with the pack list here, BEFORE the upload
    is read, rather than a 429 telling them to come back in an hour. That
    ordering is not cosmetic - see credits/limits.py for the production
    incident that made it the rule.
    """
    set_job_context(tool=TOOL, tier="hq")

    # --- 1-4: free checks, before any disk or job is touched ---
    _require_available()

    min_pitch, max_pitch, min_note_ms = _validated_filters(min_pitch, max_pitch, min_note_ms)

    instrument = (instrument or "auto").lower()
    if instrument not in INSTRUMENTS:
        raise HTTPException(400, f"instrument must be one of: {', '.join(INSTRUMENTS)}.")
    if isolate and instrument not in ("guitar", "piano"):
        raise HTTPException(400, "isolate is only supported with instrument=guitar or piano.")
    tool_key = MIX_TOOL_KEY if instrument in ("auto", "mix") else TOOL_KEY

    _validated_input_format(file.filename)
    original_filename = file.filename

    # --- 5: capacity gate, last of the free checks ---
    # Before create_job, so a refused submission leaves no job row, no
    # bytes on disk and nothing to clean up.
    _reject_if_midi_hq_queue_full()

    # --- 6: from here on we own resources that need cleaning up ---
    job_id = create_job(job_type=JOB_TYPE, ttl_seconds=AUDIO_TOOL_JOB_TTL_SECONDS)
    remember_job_tags(job_id)

    input_path, size = await _accept_upload(file, job_id, label=JOB_TYPE)
    output_path = build_output_path(job_id, "mid")

    # --- 7: duration, both ends ---
    # The upper bound MUST match MT3_MAX_SECONDS on the worker: if they
    # drift, the user waits for a job that was always going to be
    # rejected on the far side.
    duration = await _validate_duration_or_reject(
        job_id, input_path, MAX_MIDI_HQ_DURATION_SECONDS
    )

    # Lower bound. Below ~1s there is not enough signal for any
    # transcription model to find anything, and the result is a
    # guaranteed empty MIDI - an immediate, explainable 400 beats a
    # wasted GPU round trip that also costs a credit.
    if duration < MIN_MIDI_HQ_DURATION_SECONDS:
        cleanup_file(input_path)
        message = (
            f"Audio is too short ({duration:.1f}s). "
            f"Minimum is {MIN_MIDI_HQ_DURATION_SECONDS}s."
        )
        mark_failed(job_id, message)
        raise HTTPException(400, message)

    # --- 8: charge, then enqueue ---
    #
    # The affordability pre-check in the rate-limit dependency has
    # already turned away anyone who plainly cannot pay, so reaching a
    # 402 here means a genuine race - two tabs spending the last credit
    # at once. Rare, but it must not leave an uploaded file and a job row
    # behind.
    #
    # guard() charges inside BEGIN IMMEDIATE and refunds automatically if
    # the enqueue raises. The charge is idempotent per job_id.
    try:
        async with paywall.guard(
            identity, job_id=job_id, tool=tool_key, input_seconds=duration
        ) as charge:
            # Opened INSIDE the guard so charge_type is the real outcome
            # rather than a guess made before the decision - that is what
            # lets a later query separate jobs that were billable from
            # jobs that never could be.
            metering.record_job_created(
                job_id=job_id,
                tool=tool_key,
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
                semaphore=_midi_hq_semaphore,
                # Its OWN semaphore, not _midi_semaphore. Those bound
                # different resources - a CPU sidecar on this box versus
                # paid GPU capacity - and sharing would let a busy
                # midi-worker block a paid job. See utils.py.
                work=lambda: transcribe_to_midi(
                    input_path, output_path,
                    min_pitch=min_pitch,
                    max_pitch=max_pitch,
                    min_note_ms=min_note_ms,
                    instrument=instrument,
                    isolate=isolate,
                    job_id=job_id,
                ),
                # BOTH marks, deliberately. mark_tool_complete stores the
                # file (output_path/output_format) so preview and download
                # work; mark_data_complete stores the worker's stats in
                # result_data so GET .../result can serve them.
                #
                # They write to different fields on the same job dict and
                # set status/title identically, so calling both is safe
                # and order does not matter.
                #
                # WHY THE STATS NEED SERVING AT ALL. Every other job tool
                # returns audio a user can hear, so "did it work?" answers
                # itself. MIDI cannot be previewed as audio in a browser,
                # so without this the entire result of a PAID job is a
                # download link and nothing else - no way to tell a
                # 2,000-note multi-track transcription from an empty file
                # until it is open in a DAW. The track and note counts are
                # the only verifiable evidence the paid tier did anything,
                # which makes them part of the product rather than
                # diagnostics.
                on_success=lambda result: (
                    mark_tool_complete(job_id, original_filename, output_path, "mid"),
                    mark_data_complete(job_id, original_filename, result),
                    _record_gpu_cost(job_id, result),
                ),
                generic_error="High-quality MIDI transcription failed unexpectedly.",
                cleanup_paths=[input_path],
                # Closes the gpu_job_metrics row opened above. Opt-in per
                # tool - the eighteen unmetered tools sharing this runner
                # pass nothing. gpu_seconds itself is recorded by
                # midi_hq_gpu from the worker's own report; this only
                # writes the terminal status, and record_job_finished
                # COALESCEs so the two cannot blank each other.
                metered_tool=tool_key,
                success_detail=lambda r: (
                    f"{r.get('note_count')} notes, "
                    f"{r.get('track_count')} track(s), "
                    f"{r.get('notes_dropped_by_filter', 0)} filtered out"
                ),
            ))
    except HTTPException:
        # 402 from the guard. Clean up what we already own.
        cleanup_file(input_path)
        mark_failed(job_id, "Out of credits.")
        raise

    _log_queued(
        TOOL, job_id, original_filename, size,
        f"{duration:.1f}s pitch={min_pitch if min_pitch is not None else '-'}.."
        f"{max_pitch if max_pitch is not None else '-'} "
        f"min_note={min_note_ms or '-'}ms instrument={instrument} "
        f"isolate={isolate} charge={charge.charge_type}",
    )

    # The `billing` block means the frontend never needs a follow-up
    # GET /credits/me after a metered submit - the new balance is already
    # here, in the response to the action that changed it.
    return JSONResponse({
        "job_id": job_id,
        "status": "processing",
        "options": {
            "min_pitch": min_pitch,
            "max_pitch": max_pitch,
            "min_note_ms": min_note_ms,
            "instrument": instrument,
            "isolate": isolate,
        },
        "billing": {
            "charged": charge.charge_type,
            "credits": charge.credits,
            "balance": charge.balance_after,
            "free_remaining": charge.free_remaining_after,
        },
    })


@router.get("/audio-to-midi-hq/status/{job_id}")
async def audio_to_midi_hq_status(job_id: str):
    return _tool_status(job_id, JOB_TYPE)


@router.get("/audio-to-midi-hq/result/{job_id}")
async def audio_to_midi_hq_result(job_id: str):
    """What the transcription produced, as JSON. The MIDI itself is at
    /download.

    Mirrors /speech-to-text/result's contract exactly - same status
    codes, same 404-on-expired - so a frontend that already polls one
    can reuse the same handling.

    Returns the worker's own summary:

        {
          "duration_seconds": 271.4,
          "track_count": 3,
          "note_count": 1842,
          "input_seconds": 270.9,
          "notes_dropped_by_filter": 37,
          "tracks": [
            {"program": 0, "is_drum": false, "name": "Acoustic Grand Piano",
             "notes": 1204, "low": 28, "high": 96},
            ...
          ],
          "_gpu": {"fetch_seconds": 1.2, "infer_seconds": 14.8, "rtf": 18.3}
        }

    THIS IS THE ONLY PROOF THE TOOL WORKED. Every other job tool returns
    audio, so a user can just listen. MIDI cannot be previewed as audio
    in a browser, so a paid job whose entire visible output is a download
    link gives someone no way to distinguish a good transcription from an
    empty file without opening a DAW. "1,842 notes across 3 tracks" is
    the difference.

    `notes_dropped_by_filter` is worth showing when non-zero: it is the
    honest answer to "why does this look sparse?" and points at the
    user's own pitch-range or minimum-note settings rather than leaving
    them to blame the model.

    "_gpu" is operator diagnostics and is NOT part of the contract - the
    frontend should ignore it. It is left in rather than stripped because
    this route is admin-adjacent in practice (it is what you read when
    someone reports a bad result), and removing it would mean a second
    lookup to answer "how long did the GPU actually take".
    """
    tag_from_job(job_id)
    job = get_job(job_id)
    if job is None or job["job_type"] != JOB_TYPE:
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] == "failed":
        raise HTTPException(409, job.get("error") or "This job failed.")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    result = job.get("result_data")
    if not result:
        raise HTTPException(404, "Result not found (it may have expired).")
    return JSONResponse(result)


@router.get("/audio-to-midi-hq/preview/{job_id}")
async def audio_to_midi_hq_preview(job_id: str):
    """Kept for shape parity with every other job tool, though a MIDI
    file is not previewable as audio in a browser - the frontend uses
    this to render a piano roll rather than to play anything, exactly as
    /audio-to-midi/preview does."""
    path, fmt = _resolve_tool_output_path(job_id, JOB_TYPE)
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/audio-to-midi-hq/download/{job_id}")
async def audio_to_midi_hq_download(job_id: str):
    """
    Named after the ORIGINAL uploaded file, matching /audio-to-midi's
    download route rather than the generic "converted.mp3" every other
    tool uses.

    Same reasoning as there: a MIDI file loaded straight into a DAW is
    far more useful named after the source track than a batch of
    "transcribed.mid" files colliding on disk. Worth more here than on
    the free tool, since a paid transcription is likelier to be one of
    several the user is comparing.

    Falls back to "transcribed" if the job has no recorded title -
    a download must never 500 over a missing filename.
    """
    path, fmt = _resolve_tool_output_path(job_id, JOB_TYPE)
    job = get_job(job_id)
    original_title = (job.get("title") if job else None) or "transcribed"

    # Strips the ORIGINAL extension so it isn't doubled up with the new
    # .mid - "song.mp3" -> "song", not "song.mp3.mid". No path
    # sanitisation needed: this only sets a Content-Disposition header
    # value, not a filesystem path.
    base_name = os.path.splitext(original_title)[0].strip() or "transcribed"

    return FileResponse(
        path,
        media_type="application/octet-stream",
        filename=f"{base_name}.{fmt}",
    )