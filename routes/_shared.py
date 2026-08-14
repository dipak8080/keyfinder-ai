"""
routes/_shared.py - helpers shared by 2+ route modules.

Split out of the old monolithic routes.py (2026-08-14 restructure). Pure
move: every docstring, comment, and line of logic here is unchanged from
its original location. Nothing in this file changes behaviour.

What lives here, and why: these are the pieces that more than one route
module needs (separation.py + youtube.py both need the queue-depth
guard; audio_tools.py + midi.py both need the shared submit path; every
job-based tool needs the shared background runner). Anything used by
only ONE route module stays in that module instead of being pulled in
here.

The six concurrency semaphores (two original + four moved from the old
routes.py module level) now live in utils.py instead, alongside
acquire_slot_or_503() and run_blocking() - see utils.py's own docstring.
This module imports the ones it needs from there rather than defining
them, so there is exactly one place all six concurrency limits are
declared.
"""
import os
import time
import asyncio
from typing import Callable, Optional, Sequence

from fastapi import HTTPException, UploadFile
from fastapi.responses import JSONResponse

from config import logger, MAX_UPLOAD_BYTES, MAX_QUEUED_SEPARATIONS
from upload import save_upload
from utils import (
    cleanup_file,
    release_memory_to_os,
    run_blocking,
    _audio_tools_semaphore,
)
from jobs import (
    create_job,
    mark_failed,
    mark_tool_complete,
    fail_if_unfinished,
    get_job,
    count_processing,
    SEPARATION_JOB_TYPES,
)
from separation import SeparationError
from audio_common import (
    AudioToolError,
    validate_input_format,
    validate_duration,
    build_temp_input_path,
    build_output_path,
    assert_distinct_paths,
)
from monitoring import record_result
from log_stream import set_job_context, remember_job_tags, tag_from_job


def _mb(num_bytes: int) -> str:
    """Consistent size rendering for logs. One place so a grep for 'MB'
    across the log stream always matches the same format."""
    return f"{num_bytes / (1024 * 1024):.1f}MB"


def _log_queued(tool: str, job_id: str, filename: str, size_bytes: int, detail: str = ""):
    """
    The START line for a job. Deliberately carries the input size: a
    failure minutes later is much easier to reason about when the log
    already says whether the input was 2MB or 79MB, and there is no other
    record of it once the temp file is cleaned up.
    """
    suffix = f" {detail}" if detail else ""
    logger.info(
        f"[{tool}] job={job_id} queued '{filename}' {_mb(size_bytes)}{suffix}"
    )


async def _run_tool_job(
    *,
    tool: str,
    metric: str,
    job_id: str,
    semaphore: asyncio.Semaphore,
    work: Callable,
    on_success: Callable,
    generic_error: str,
    cleanup_paths: Sequence[str] = (),
    success_detail: Optional[Callable] = None,
    gpu_billed: bool = False,
):
    """
    The single background runner shared by every job-based audio tool.

    This replaces fifteen hand-written _run_*_background() functions that
    were identical apart from which worker to call and which
    mark_*_complete() to use. The duplication was not harmless: error
    handling, cleanup, metrics and logging all had to be repeated
    verbatim in each copy, and any improvement to one of them silently
    failed to reach the other fourteen.

    Arguments:
      tool           - log prefix, e.g. "CONVERT"
      metric         - record_result() label, e.g. "/convert"
      job_id         - the job to update
      semaphore      - which concurrency pool this work belongs to
      work           - zero-arg callable returning an awaitable (normally
                       a run_blocking(...) call)
      on_success     - callable(result) that marks the job complete
      generic_error  - user-facing message for an unexpected failure;
                       deliberately vague, since the detail belongs in
                       the logs, not in a response to an anonymous caller
      cleanup_paths  - input files to delete once the work is done, win
                       or lose
      success_detail - optional callable(result) -> str, appended to the
                       COMPLETE log line (e.g. "4 stems", "182.3s total")
      gpu_billed     - Records this task's own wall-clock time against
                       the GPU spend budget.

                       NOW FALSE FOR SEPARATION, and that is deliberate,
                       not an oversight. Since the GPU migration,
                       separation.py records the REAL billed number - the
                       one the RunPod worker itself reports for just the
                       Demucs run - and does it from inside
                       _run_demucs_on_gpu(). This timer, by contrast,
                       also spans RunPod queue wait, cold start, and two
                       file transfers: real latency the user experiences,
                       but not GPU-seconds anyone is billed for. Leaving
                       both enabled would DOUBLE-COUNT every separation
                       job and trip the HQ cutoff at roughly half the
                       real spend, disabling a working feature over a
                       bill that was never incurred.

                       Kept as a parameter rather than deleted because a
                       FUTURE GPU-backed tool whose worker does not
                       report its own timing would legitimately want this
                       fallback - it is the right mechanism, just no
                       longer the right source for this particular tool.

    NOTE ON tool/tier: this function does NOT call set_job_context()
    itself, deliberately. By the time this runs (inside a task spawned
    via asyncio.create_task()), the calling route handler has already
    set tool/tier on the contextvar - and since create_task() copies the
    context at the moment it's called, this task already inherited it.
    Setting it again here would be redundant at best; the single call
    site per route is what keeps "what tagged this job" traceable to one
    place instead of two.

    The `finally` block runs in a fixed order that matters:
      fail_if_unfinished() FIRST, so a job is guaranteed terminal even if
      an exception escaped every except clause above (the acquire_slot_
      or_503-inside-a-background-task case, which no `except AudioTool
      Error` or `except Exception` here would catch if it were raised
      before the try). Then file cleanup, then memory release, then the
      metric, then GPU billing - each independent of the others.
    """
    started = time.monotonic()
    succeeded = False

    async with semaphore:
        waited = time.monotonic() - started
        if waited > 1.0:
            # Only logged when it actually happened. A long wait here is
            # the difference between "the tool is slow" and "the tool was
            # queued behind someone else's job", which is otherwise
            # invisible and looks identical to the user.
            logger.info(f"[{tool}] job={job_id} waited {waited:.1f}s for a free slot")

        run_started = time.monotonic()
        try:
            result = await work()
            on_success(result)
            succeeded = True
            detail = ""
            if success_detail is not None:
                try:
                    detail = f" ({success_detail(result)})"
                except Exception:
                    # A broken log-detail callable must never turn a
                    # successful job into a failed one.
                    detail = ""
            logger.info(
                f"[{tool}] job={job_id} COMPLETE in {time.monotonic() - run_started:.1f}s{detail}"
            )

        except AudioToolError as e:
            # Expected, user-actionable failure - the message is written
            # for the person who uploaded the file, so it passes through
            # to them unchanged.
            mark_failed(job_id, str(e))
            logger.warning(
                f"[{tool}] job={job_id} FAILED in {time.monotonic() - run_started:.1f}s: {e}"
            )

        except SeparationError as e:
            mark_failed(job_id, str(e))
            logger.warning(
                f"[{tool}] job={job_id} FAILED in {time.monotonic() - run_started:.1f}s: {e}"
            )

        except asyncio.CancelledError:
            # Shutdown. Mark it so a client polling across a redeploy
            # gets a real answer instead of an eternal "processing", then
            # re-raise so the task actually stops.
            mark_failed(job_id, "The server restarted while this job was running.")
            logger.warning(f"[{tool}] job={job_id} CANCELLED (shutdown)")
            raise

        except Exception as e:
            mark_failed(job_id, generic_error)
            logger.error(
                f"[{tool}] job={job_id} FAILED in {time.monotonic() - run_started:.1f}s "
                f"(unexpected): {e}",
                exc_info=True,
            )

        finally:
            fail_if_unfinished(job_id, generic_error)
            for path in cleanup_paths:
                cleanup_file(path)
            release_memory_to_os()
            record_result(metric, succeeded)
            if gpu_billed:
                # RESERVED, currently unused by any caller (both
                # separation call sites pass gpu_billed=False - the
                # GPU spend ceiling is now RunPod's own account balance,
                # not a self-tracked counter; see the removal note at
                # the top of gpu-worker/handler.py's docstring for the
                # full reasoning). Kept as a real, working parameter
                # rather than deleted, in case a future GPU-backed tool
                # without its own worker-reported timing wants a local
                # wall-clock fallback for logging/observability - it
                # just doesn't feed a budget breaker any more.
                pass


async def _accept_upload(
    file: UploadFile,
    job_id: str,
    label: str,
    max_bytes: int = MAX_UPLOAD_BYTES,
) -> tuple:
    """
    Streams one upload to disk for a job that already exists, returning
    (input_path, size_bytes).

    The job is created BEFORE the upload rather than after, so that a
    rejected or failed transfer can be recorded against a real job id
    instead of vanishing. That is why the HTTPException is caught and
    re-raised here: without mark_failed(), a client that had already been
    handed a job id (or that retries) would find nothing explaining what
    happened.
    """
    input_path = build_temp_input_path(job_id, file.filename)
    try:
        size = await save_upload(file, input_path, max_bytes, label=label)
    except HTTPException as e:
        mark_failed(job_id, e.detail if isinstance(e.detail, str) else "Upload rejected.")
        raise
    return input_path, size


async def _validate_duration_or_reject(
    job_id: str,
    input_path: str,
    max_seconds: Optional[int] = None,
) -> float:
    """
    Runs the ffprobe duration check and turns a failure into a synchronous
    400, cleaning up as it goes.

    Two things worth noting:

    - It is dispatched through run_blocking(). validate_duration() spawns
      ffprobe, and calling it directly from an async handler (as this
      file previously did in eleven places) blocks the event loop for the
      whole probe. On a single worker that stalls every other connection,
      including the status polls the frontend depends on.

    - It runs at SUBMIT time, not inside the background task, so an
      out-of-range file gets an immediate 400 the frontend can show
      against the upload form - rather than a job that is accepted, then
      fails a second later for a reason the user could have been told
      instantly.
    """
    try:
        if max_seconds is None:
            return await run_blocking(validate_duration, input_path)
        return await run_blocking(validate_duration, input_path, max_seconds)
    except AudioToolError as e:
        cleanup_file(input_path)
        mark_failed(job_id, str(e))
        raise HTTPException(400, str(e))


def _reject_if_separation_queue_full():
    """
    The bounded queue for every Demucs-backed route.

    MAX_CONCURRENT_SEPARATIONS bounds how many separations RUN at once,
    but the semaphore enforcing it is acquired inside the background
    task - so before this check existed, submissions were never refused,
    they simply queued in memory with no ceiling. Each waiting job held
    an uploaded file on disk and a job-table entry, and the person
    watching the spinner had no way to know they were twelfth in line
    behind ~50 minutes of work.

    Rejecting at submit time is strictly kinder: the file is never
    uploaded, the disk is never touched, and the caller gets a specific
    reason with a suggestion instead of an open-ended wait that looks
    exactly like the site being broken.

    503 rather than 429 is deliberate - this is not the caller's rate
    being too high, it is the server being at capacity, and the two mean
    different things to a client deciding whether to retry.
    """
    depth = count_processing(SEPARATION_JOB_TYPES)
    if depth >= MAX_QUEUED_SEPARATIONS:
        logger.warning(
            f"[SEPARATION] Rejected submission - queue full "
            f"({depth}/{MAX_QUEUED_SEPARATIONS} jobs in flight)"
        )
        raise HTTPException(
            503,
            "The separation queue is full right now - each job takes several "
            "minutes and only one runs at a time. Please try again in a few minutes.",
        )


def _resolve_tool_output_path(job_id: str, expected_type: str) -> tuple:
    """
    Shared lookup behind every audio tool's preview and download route.
    Returns (path, output_format).

    Re-applies this job's tool/tier tag to the current request (see
    tag_from_job in log_stream.py). Every preview/download route funnels
    through here, so this one line is what stops them logging as
    untagged rows despite plainly belonging to a tagged job.

    Checking job_type is what stops a job id from one tool being used to
    read another tool's output - the id alone is not a capability, the
    pairing of id and tool is.
    """
    job = get_job(job_id)
    if job is None or job["job_type"] != expected_type:
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] == "failed":
        raise HTTPException(409, job.get("error") or "This job failed.")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    path = job["output_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Output file not found (it may have expired).")
    return path, (job.get("output_format") or "bin")


def _tool_status(job_id: str, expected_type: str) -> dict:
    """
    Shared status response for every single-output tool.

    Re-applies this job's tool/tier tag - see _resolve_tool_output_path
    above. Status polls are the highest-volume rows a job produces and
    were the most conspicuously untagged before this.

    job_type is validated here too, so polling with an id that belongs to
    a different tool returns 404 rather than a confusing "complete" for
    something the caller never submitted.
    """
    tag_from_job(job_id)
    job = get_job(job_id)
    if job is None or job["job_type"] != expected_type:
        raise HTTPException(404, "Job not found (it may have expired).")
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
        "elapsed_seconds": round(time.time() - job["created_at"], 1),
    }


def _validated_input_format(filename: str) -> str:
    """
    validate_input_format() raises AudioToolError, which is not an
    HTTPException - so calling it bare (as every route here previously
    did) turned "you uploaded a .txt" into a 500 Internal Server Error.
    A wrong file extension is the caller's mistake, not the server's, and
    400 is what lets the frontend say something useful about it.
    """
    try:
        return validate_input_format(filename)
    except AudioToolError as e:
        raise HTTPException(400, str(e))


async def _submit_audio_tool(
    file: UploadFile,
    *,
    job_type: str,
    tool: str,
    metric: str,
    build_work: Callable,
    output_format: Optional[str] = None,
    check_duration: bool = True,
    max_duration_seconds: Optional[int] = None,
    log_detail: str = "",
    generic_error: str = "Processing failed unexpectedly.",
    semaphore: Optional[asyncio.Semaphore] = None,
    allowed_input_formats: Optional[frozenset] = None,
    min_duration_seconds: Optional[float] = None,
) -> JSONResponse:
    """
    Shared submit path for every single-input, single-output audio tool.

    Order of operations is deliberate:
      1. Validate the FILENAME's format first - free, and rejects an
         obviously wrong file before a byte is transferred.
      2. Create the job, so an upload that fails partway has somewhere to
         record why.
      3. Stream the upload to disk with the size cap enforced per chunk.
      4. Probe duration (off the event loop) and reject synchronously if
         it's too long - the caller learns immediately rather than being
         handed a job id that fails a second later.
      5. Only then queue the background work.

    build_work(input_path, output_path) returns a zero-arg callable that
    the runner awaits. Passing a builder rather than the paths themselves
    keeps every tool's actual worker call visible at its own route, which
    is the part worth reading.

    Tagged with set_job_context(tool, "standard") right at the top - this
    one call covers all thirteen tools that funnel through this shared
    function (convert, volume, pitch, tempo, reverse, noise-remove,
    voice-clean, echo-remove, silence-remove, fade, channels, resample,
    ringtone), since none of them have a tier. It runs before the job is
    even created, so it's set on the contextvar before
    asyncio.create_task() below copies the context into the background
    task - and before the HTTP response is returned, so the request's own
    row in request_logs picks it up too.
    """
    set_job_context(tool=tool, tier="standard")

    # allowed_input_formats lets ONE tool (currently only /audio-to-midi)
    # widen the accepted set without touching it for the other twelve
    # tools that share this function - basic-pitch decodes opus/webm via
    # ffmpeg that no other tool here accepts. Defaults to None, which
    # preserves the original ALLOWED_AUDIO_INPUT_FORMATS-only behaviour
    # every existing caller already relies on.
    if allowed_input_formats is not None:
        ext = (os.path.splitext(file.filename or "")[1] or "").lstrip(".").lower()
        if ext not in allowed_input_formats:
            raise HTTPException(
                400,
                f"Unsupported file type '.{ext}'. Supported formats: "
                f"{', '.join(sorted(allowed_input_formats))}.",
            )
        source_format = ext
    else:
        source_format = _validated_input_format(file.filename)
    out_fmt = output_format or source_format

    # Captured NOW, not read inside the background lambda below. The
    # UploadFile is closed once the response is sent, and while .filename
    # happens to be a plain str that survives that, depending on it would
    # be relying on an implementation detail of Starlette.
    original_filename = file.filename

    job_id = create_job(job_type=job_type)

    remember_job_tags(job_id)
    input_path, size = await _accept_upload(file, job_id, label=job_type)
    output_path = build_output_path(job_id, out_fmt)

    # Fails loudly HERE if path construction ever produces the same file
    # for input and output, instead of ten seconds later inside ffmpeg
    # with an opaque exit code - and before the cleanup step can delete
    # the output thinking it's the input. Every audio tool routes through
    # this function, so this one line covers all of them, including any
    # added later. See assert_distinct_paths() for the incident that
    # motivated it.
    try:
        assert_distinct_paths(input_path, output_path)
    except AudioToolError as e:
        cleanup_file(input_path)
        mark_failed(job_id, str(e))
        raise HTTPException(500, str(e))

    if check_duration:
        duration = await _validate_duration_or_reject(job_id, input_path, max_duration_seconds)

        # Lower bound, opt-in per tool. Below ~1s there isn't enough
        # signal for MIDI transcription to find anything - this turns a
        # guaranteed-empty result into an immediate, explainable 400
        # instead of a wasted round trip to the worker.
        if min_duration_seconds is not None and duration < min_duration_seconds:
            cleanup_file(input_path)
            mark_failed(job_id, f"Audio is too short (minimum {min_duration_seconds}s).")
            raise HTTPException(
                400,
                f"Audio is too short ({duration:.1f}s). Minimum is {min_duration_seconds}s.",
            )

    asyncio.create_task(_run_tool_job(
        tool=tool,
        metric=metric,
        job_id=job_id,
        semaphore=semaphore or _audio_tools_semaphore,
        work=build_work(input_path, output_path),
        on_success=lambda _: mark_tool_complete(job_id, original_filename, output_path, out_fmt),
        generic_error=generic_error,
        cleanup_paths=[input_path],
    ))

    _log_queued(tool, job_id, original_filename, size, log_detail)
    return JSONResponse({"job_id": job_id, "status": "processing"})