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

--------------------------------------------------------------------------
ADDED 2026-08-16: spawn_background_task() - see its own docstring for the
garbage-collection hazard it closes. Every asyncio.create_task() call in
this package should go through it.
--------------------------------------------------------------------------

--------------------------------------------------------------------------
ADDED 2026-08-19: _reject_if_transcription_queue_full() - the second
bounded-queue guard in this file. It sits directly below the separation
one, and the two must BOTH stay: separation.py and youtube.py import the
separation guard, transcribe.py and youtube_transcribe.py import the
transcription one. Removing either breaks module import at startup, which
surfaces as a failed health check and an automatic rollback rather than
anything that names the missing function.
--------------------------------------------------------------------------

--------------------------------------------------------------------------
ADDED 2026-08-22: _reject_if_audio_tools_queue_full() - the THIRD and
final bounded-queue guard, closing the last unbounded pool in the app.

The audio tools were the pool that never got this treatment, almost
certainly because each individual job is fast. That is true, and it is
not the same thing as safe: eighteen endpoints feed one 4-slot
semaphore, and _run_tool_job acquires that semaphore INSIDE the
background task, so every submission past the fourth queued in memory
without limit. Same mechanism as the two bugs already fixed above, same
symptom - a spinner that looks identical to the site being broken.

Rate limits do not cover this and never could: they are per-IP. Fifty
different visitors each making ONE legitimate /convert request is fifty
queued jobs and zero rate-limit violations. Good traffic, in other
words, is exactly the case that produced it.

CALLED FROM TWO PLACES, deliberately:
  - inside _submit_audio_tool(), covering the fourteen tools that share
    that path - but ONLY when the caller passed no semaphore of its own,
    which is what keeps /audio-to-midi (its own semaphore, its own
    sidecar) out of this pool's accounting.
  - directly from the four routes with their own submit paths: /trim
    (routes/audio_tools.py) and /join, /video-to-audio, /silence-split
    (routes/media.py).
--------------------------------------------------------------------------
"""
import os
import time
import asyncio
from typing import Callable, Optional, Sequence

from fastapi import HTTPException, UploadFile
from fastapi.responses import JSONResponse

from config import (
    logger,
    MAX_UPLOAD_BYTES,
    MAX_QUEUED_SEPARATIONS,
    MAX_QUEUED_TRANSCRIPTIONS,
    MAX_QUEUED_AUDIO_TOOLS,
    AUDIO_TOOL_JOB_TYPES,
)
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
    TRANSCRIPTION_JOB_TYPES,
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


# ============================================================
# BACKGROUND TASK REGISTRY (added 2026-08-16)
#
# WHY THIS EXISTS: asyncio's event loop holds only a WEAK reference to
# the tasks it is running. This is documented behaviour, not a quirk -
# see the warning in the asyncio.create_task() docs. A task with no
# strong reference held anywhere can therefore be garbage-collected
# mid-execution: it simply stops, partway through, with no exception, no
# traceback, and no log line.
#
# Every job-based tool in this package spawned its runner as
# `asyncio.create_task(...)` used as a bare statement - the returned Task
# discarded immediately, so nothing anywhere held a reference to it.
#
# The failure mode if it ever fires is the worst possible shape: the job
# row stays at "processing" forever. mark_failed() is never called, the
# `finally` block in _run_tool_job (including fail_if_unfinished) never
# runs, because the coroutine was COLLECTED rather than raising anything
# that a finally could catch. From the outside it is identical to the
# "it just spun forever" class of report that _chain_download's own
# docstring describes fixing - same symptom, entirely different
# mechanism, and this one was still open.
#
# It is rare, because it needs a GC cycle to land at exactly the wrong
# moment. It is also much less rare on a 6GB box that runs Demucs and
# TensorFlow, which is precisely the environment where GC pressure is
# real rather than theoretical.
#
# The fix is the one the asyncio docs prescribe: keep a strong reference
# until the task finishes, then drop it. add_done_callback fires whether
# the task completed, failed, or was cancelled, so the set cannot leak.
# ============================================================
_background_tasks: set = set()


def spawn_background_task(coro) -> asyncio.Task:
    """
    asyncio.create_task() plus a strong reference held until completion.

    Use this instead of a bare asyncio.create_task() for any fire-and-
    forget task - i.e. anywhere the returned Task isn't already being
    stored in a variable that outlives the call (main.py's cleanup_task
    is stored and awaited at shutdown, so it is correctly exempt).

    Returns the Task, so a caller that DOES want to hold it still can.

    No behaviour change beyond preventing the collection hazard above:
    the coroutine runs identically, exceptions surface identically, and
    the done-callback only removes the set entry.
    """
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


def background_task_count() -> int:
    """How many spawned tasks are currently in flight. Useful from
    /admin/status if a "why is nothing finishing" question ever comes
    up - a count that only grows is the signature of tasks starting and
    never completing."""
    return len(_background_tasks)


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
    via spawn_background_task()), the calling route handler has already
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

      Worth knowing what this does NOT cover: if the task itself is
      garbage-collected mid-run, none of this executes at all, because
      collection is not an exception and a `finally` has nothing to
      unwind. That hazard is closed at the spawn site instead - see
      spawn_background_task() above.
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

    IMPORTED BY routes/separation.py AND routes/youtube.py. Deleting it
    breaks both at import time, which surfaces as a failed startup health
    check and an automatic rollback - not as anything that names this
    function.

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


def _reject_if_transcription_queue_full():
    """
    The bounded queue for both transcription routes.

    IMPORTED BY routes/transcribe.py AND routes/youtube_transcribe.py.
    Same deletion hazard as the separation guard above.

    Same mechanism and same reasoning as
    _reject_if_separation_queue_full() above - see that docstring for the
    full argument. Two things differ and both matter:

    COUNTS BOTH ENDPOINTS TOGETHER. /speech-to-text and
    /youtube/transcribe share a single semaphore, so counting one in
    isolation would let the other fill the queue unnoticed. A user
    uploading a file genuinely is behind everyone who pasted a YouTube
    link, and the guard has to reflect that.

    THE RATE LIMITER DOES NOT ALREADY COVER THIS. That limit is per-IP;
    this is a whole-server capacity bound. Ten visitors each submitting
    their permitted two requests is twenty queued jobs and zero rate-limit
    violations - which is exactly the case that makes the site look broken
    while every individual limit is being respected.

    503, not 429, for the same reason as separation: this is the server
    being at capacity, not the caller misbehaving, and the two mean
    different things to a client deciding whether to retry.
    """
    depth = count_processing(TRANSCRIPTION_JOB_TYPES)
    if depth >= MAX_QUEUED_TRANSCRIPTIONS:
        logger.warning(
            f"[TRANSCRIPTION] Rejected submission - queue full "
            f"({depth}/{MAX_QUEUED_TRANSCRIPTIONS} jobs in flight)"
        )
        raise HTTPException(
            503,
            "The transcription queue is full right now - only one file is "
            "processed at a time and each takes several minutes. Please try "
            "again shortly.",
        )


def _reject_if_audio_tools_queue_full():
    """
    The bounded queue for the shared ffmpeg/rubberband pool.

    ADDED 2026-08-22, and the last of the three. Called from
    _submit_audio_tool() below (covering fourteen tools) and directly
    from the four routes that build their own submit path: /trim in
    routes/audio_tools.py, and /join, /video-to-audio, /silence-split in
    routes/media.py.

    Identical mechanism to the two guards above - see
    _reject_if_separation_queue_full() for the full argument - with one
    difference worth stating plainly, because it is why this pool was
    overlooked for so long:

    THESE JOBS ARE FAST, AND THAT IS NOT THE SAME AS SAFE. A /volume runs
    in seconds, so an unbounded queue behind a 4-slot semaphore looks
    harmless right up until eighteen endpoints all feed it at once. And
    the per-IP rate limits offer nothing here by construction: fifty
    different visitors each making ONE permitted /convert request is
    fifty queued jobs and zero violations. The good-traffic case IS the
    failure case.

    COUNTS ALL EIGHTEEN TOOLS TOGETHER, for the same reason the
    transcription guard counts both its endpoints: one semaphore, one
    queue. Someone submitting /convert genuinely is behind everyone who
    submitted /join, and a per-tool count would let any one tool fill the
    pool while every individual count looked fine.

    EXCLUDES /audio-to-midi by construction. It shares
    _submit_audio_tool() but passes its own semaphore, and the call site
    below only invokes this guard when no semaphore was passed - so a
    busy MIDI sidecar can never reject a /convert. See
    AUDIO_TOOL_JOB_TYPES in config.py.

    503 rather than 429, same as its two siblings: the server is at
    capacity, the caller did nothing wrong, and a client deciding whether
    to retry needs to be able to tell those apart.
    """
    depth = count_processing(AUDIO_TOOL_JOB_TYPES)
    if depth >= MAX_QUEUED_AUDIO_TOOLS:
        logger.warning(
            f"[AUDIO_TOOLS] Rejected submission - queue full "
            f"({depth}/{MAX_QUEUED_AUDIO_TOOLS} jobs in flight)"
        )
        raise HTTPException(
            503,
            "The server is busy processing other files right now. These jobs "
            "are usually quick - please try again in a moment.",
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
      2. Check the queue depth - also free, and the last check before
         anything is spent. See the call site below for why it sits
         here rather than first.
      3. Create the job, so an upload that fails partway has somewhere to
         record why.
      4. Stream the upload to disk with the size cap enforced per chunk.
      5. Probe duration (off the event loop) and reject synchronously if
         it's too long - the caller learns immediately rather than being
         handed a job id that fails a second later.
      6. Only then queue the background work.

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
    spawn_background_task() below copies the context into the background
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

    # Capacity gate. Two things about its placement are deliberate.
    #
    # AFTER the format check, matching the ordering argument in
    # routes/transcribe.py's docstring: everything above is the CALLER's
    # input being wrong (400), this is the SERVER being full (503).
    # Someone who uploaded a .txt should be told about the .txt, not
    # turned away for capacity, fix nothing, and hit the 400 on retry.
    #
    # BEFORE create_job and _accept_upload, so a refused submission
    # leaves no job row, no bytes on disk, and nothing to clean up.
    #
    # `semaphore is None` is what scopes this to the SHARED audio-tools
    # pool. A caller that brought its own semaphore (only
    # /audio-to-midi today, bounded by MAX_CONCURRENT_MIDI and the
    # sidecar's own limit) is not queueing for a slot this guard
    # counts, so applying it there would reject /convert submissions
    # because an unrelated MIDI worker was busy.
    if semaphore is None:
        _reject_if_audio_tools_queue_full()

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

    # spawn_background_task, not a bare asyncio.create_task - see that
    # function's docstring for the garbage-collection hazard. A collected
    # task leaves the job stuck at "processing" forever with no log line,
    # which is the single hardest failure shape to diagnose after the
    # fact.
    spawn_background_task(_run_tool_job(
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