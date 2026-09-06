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

--------------------------------------------------------------------------
ADDED 2026-08-25: settle_or_refund() IN _run_tool_job's `finally`

One line, and it closes the worst hole the credits system had.

Before it, a credit was returned in exactly two situations: if
paywall.guard()'s enqueue raised (spawning an asyncio task is not a
failure-prone operation, so essentially never), or when
sweep_stale_holds() ran 90 minutes later. Neither covers the case that
actually happens - the job is accepted, runs, and FAILS on the GPU
worker. _run_tool_job caught that, called mark_failed(), and left the
credit held.

So a paying user watched "Separation failed" with their credit gone, for
up to an hour and a half. Technically recoverable, experientially
indistinguishable from being robbed, and landing at the single worst
moment in the product: right after someone paid and did not get the
thing they paid for.

It sits in the `finally` rather than the except-chain on purpose. The
`finally` runs on the success path, on every exception path, AND on the
asyncio.CancelledError a redeploy fires - which is the most likely way a
paid job really dies. Enumerating except-clauses instead would mean
missing whichever one gets added next year.

It is UNCONDITIONAL and a silent no-op for the eighteen unmetered tools
that share this runner, because they have no charge row. That is what
keeps the call site from growing an "is this tool metered?" branch which
would go stale the moment a tool is added - the exact drift this
codebase fights everywhere else.

What it still cannot cover, and why the sweeper stays: a task
garbage-collected mid-run, or the container killed outright. No Python
executes in either case.
--------------------------------------------------------------------------

--------------------------------------------------------------------------
ADDED 2026-08-28: _reject_if_midi_hq_queue_full() - the FOURTH bounded
queue guard, for /audio-to-midi-hq.

First one written BEFORE the bug rather than after it. The other three
each followed a production incident; this one exists because the
mechanism is now well enough understood to see it coming. Same shape as
its siblings, counting MIDI_HQ_JOB_TYPES against MAX_QUEUED_MIDI_HQ.

Deliberately does NOT count the free /audio-to-midi. Those two tools
share a name and nothing else: one is a CPU sidecar on this box bounded
by MAX_CONCURRENT_MIDI, the other is paid GPU capacity bounded by the
RunPod worker count. See utils.py's semaphore block for the full
argument against merging them.
--------------------------------------------------------------------------

--------------------------------------------------------------------------
ADDED 2026-08-27: metered_tool= on _run_tool_job, closing the
gpu_job_metrics row.

Every separation route already opens a metrics row at submit and closes
it from separation.py, which is where the RunPod-reported gpu_seconds
actually arrive. /speech-to-text has no equivalent - it goes through
this shared runner and transcription.py, so nothing here knew when its
row should be closed.

The row was therefore opened and left at status='created' forever, which
is worse than not opening it: totals() counts it as a job, so the
cost-per-job figure would be diluted by rows that never recorded an
outcome. The number that decides the price would have been wrong in the
optimistic direction.

DELIBERATELY OPT-IN rather than unconditional, which is the opposite of
the settle_or_refund() decision directly above it - so the difference is
worth stating. settle_or_refund() is safe to call blind because it looks
up a charge row and returns silently when there is none. This one issues
an UPDATE against gpu_job_metrics for every job, including the eighteen
ffmpeg tools that will never have a row there. That UPDATE would match
zero rows and be harmless, but it is a connection open, a transaction
and a write attempt on the credits DB for every /convert and /trim on
the site - and credits.db is the file that holds the money, deliberately
kept away from load (see the three-writers note in credits' PART 7).
Paying that on the hottest path in the app to save one keyword argument
on one call site is the wrong trade.

Passed today by routes/transcribe.py only. The other two transcription
routes have their own runners and call metering directly.
--------------------------------------------------------------------------

--------------------------------------------------------------------------
ADDED 2026-08-30: job_type= on _validate_duration_or_reject(), which is
what finally makes AUDIO_TOOL_MAX_DURATION_SECONDS do anything at all.

That map has sat in config.py since the per-tool timeouts landed,
carrying a docstring that describes precisely the bug it prevents - and
nothing ever imported it. A `grep -rn` for its name across every .py
file in the container returned exactly two hits, both inside config.py
itself: the definition, and a comment pointing at the definition. Zero
readers.

So every audio tool took the `max_seconds is None` branch and got
validate_duration()'s own default, which is
MAX_AUDIO_TOOL_DURATION_SECONDS (3600). pitch and tempo's 900s entries
were decoration.

Which means the failure the map was written to stop was live the entire
time, exactly as its config comment predicted: a 50-minute file passes
the 1-hour check, is accepted, takes one of only four slots in
MAX_CONCURRENT_AUDIO_TOOLS, and rubberband grinds until pitch's 600s
entry in AUDIO_TOOL_TIMEOUT_SECONDS kills it. The user waits the full
ten minutes to be told it failed, and a quarter of the shared pool is
unavailable for all of it.

WHY THIS KEPT HAPPENING is worth naming, because it is the same shape as
two other bugs this file and config.py already document: the
SILENCE_MIN_DURATION_SECONDS env lookup that read a name nothing ever
set, and the /limits values the frontend had drifted from. A constant
that exists and is never read fails identically to an env var that never
matches - silently, and always in the looser-than-documented direction.
Neither raises. Neither logs. The only symptom is that a limit you
believe you have is not there.

The frontend was advertising 1 hour on the pitch and tempo pages, which
was CORRECT against the running code and becomes wrong the moment this
deploys. Those two pages drop to 15 minutes in the same release.
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
    MAX_QUEUED_MIDI_HQ,
    AUDIO_TOOL_JOB_TYPES,
    # The per-tool duration caps. MAX_AUDIO_TOOL_DURATION_SECONDS is
    # deliberately NOT imported alongside it: validate_duration()'s
    # signature default already IS that constant, so a tool with no entry
    # in this map falls through to it without the number being restated
    # here. One fallback, one place.
    AUDIO_TOOL_MAX_DURATION_SECONDS,
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
    MIDI_HQ_JOB_TYPES,
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

# Credits. Self-contained and inert while PAYWALL_ENABLED is unset -
# settle_or_refund() is a no-op for any job with no charge row, which is
# every job on every unmetered tool.
from credits.ledger import settle_or_refund


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
    metered_tool: Optional[str] = None,
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
      metered_tool   - route key ("transcribe", ...) when this job has a
                       gpu_job_metrics row that needs closing. None for
                       every unmetered tool, which is the default and the
                       overwhelmingly common case.

                       OPT-IN ON PURPOSE - see the 2026-08-27 note in the
                       module docstring. Making it unconditional would
                       put a credits-DB write on the hot path of eighteen
                       ffmpeg tools that will never have a row to update,
                       on the one database file deliberately kept away
                       from load because it holds the money.

                       NOT the same thing as the paywall. A tool can be
                       metered here (cost recorded) while charging
                       nobody: that is exactly the state every route ran
                       in for weeks before PAYWALL_ENABLED was flipped,
                       and it is how the price was set from real numbers
                       instead of a guess.
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
      settle_or_refund() FIRST as of 2026-08-25 - see below for why the
      money moves before anything else. Then the metrics close, then
      fail_if_unfinished(), so a job is guaranteed terminal even if an
      exception escaped every except clause above (the
      acquire_slot_or_503-inside-a-background-task case, which no
      `except AudioToolError` or `except Exception` here would catch if
      it were raised before the try). Then file cleanup, then memory
      release, then the metric, then GPU billing - each independent of
      the others.

      Worth knowing what this does NOT cover: if the task itself is
      garbage-collected mid-run, none of this executes at all, because
      collection is not an exception and a `finally` has nothing to
      unwind. That hazard is closed at the spawn site instead - see
      spawn_background_task() above, and credits' sweep_stale_holds()
      for the money side of the same gap.
    """
    started = time.monotonic()
    succeeded = False
    failure: Optional[str] = None

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
            failure = str(e)
            mark_failed(job_id, str(e))
            logger.warning(
                f"[{tool}] job={job_id} FAILED in {time.monotonic() - run_started:.1f}s: {e}"
            )

        except SeparationError as e:
            failure = str(e)
            mark_failed(job_id, str(e))
            logger.warning(
                f"[{tool}] job={job_id} FAILED in {time.monotonic() - run_started:.1f}s: {e}"
            )

        except asyncio.CancelledError:
            # Shutdown. Mark it so a client polling across a redeploy
            # gets a real answer instead of an eternal "processing", then
            # re-raise so the task actually stops.
            failure = "cancelled: server restarted while this job was running"
            mark_failed(job_id, "The server restarted while this job was running.")
            logger.warning(f"[{tool}] job={job_id} CANCELLED (shutdown)")
            raise

        except Exception as e:
            failure = f"{type(e).__name__}: {e}"[:500]
            mark_failed(job_id, generic_error)
            logger.error(
                f"[{tool}] job={job_id} FAILED in {time.monotonic() - run_started:.1f}s "
                f"(unexpected): {e}",
                exc_info=True,
            )

        finally:
            # MONEY FIRST, before anything that could itself raise.
            #
            # A failed paid job must return its credit even if cleanup,
            # memory release or the metrics call then goes wrong - the
            # credit is the part the user notices, and an exception in
            # any later step must not be able to strand it.
            #
            # Unconditional, and a silent no-op for the eighteen
            # unmetered tools sharing this runner: they have no charge
            # row, so settle_or_refund() returns immediately. That is
            # what keeps this line free of an "is this tool metered?"
            # branch which would go stale the next time a tool is added.
            #
            # `succeeded` is the same flag the COMPLETE/FAILED log line
            # and record_result() already use, so the refund can never
            # disagree with what the logs say happened.
            #
            # Note this also runs on the CancelledError path above,
            # because that clause re-raises INTO this finally - so a
            # redeploy that kills an in-flight paid job returns the
            # credit in that same instant rather than 90 minutes later
            # via the sweeper. That is the most likely way a paid job
            # actually dies, which makes it the case most worth getting
            # right.
            settle_or_refund(job_id, succeeded, reason=f"{tool.lower()}_failed")

            # Close the cost row, for the routes that opened one. Second
            # in the order rather than first: money before measurement,
            # always - a metering failure must never be able to strand a
            # refund. metering swallows its own exceptions internally
            # (see credits/metering.py) so this cannot raise into the
            # steps below either.
            #
            # gpu_seconds is deliberately NOT passed. The honest number
            # comes from what the worker reports for its own run, which
            # only the backend module sees; wall clock measured here also
            # spans queue wait and cold start, and recording that as
            # GPU-seconds would inflate every cost estimate in a
            # direction that looks entirely plausible.
            if metered_tool:
                from credits import metering
                metering.record_job_finished(
                    job_id,
                    status="completed" if succeeded else "failed",
                    error=None if succeeded else (failure or generic_error),
                )

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
    job_type: Optional[str] = None,
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

    RETURNS THE DURATION, which matters more since the paywall landed:
    routes/transcribe.py feeds this value straight into paywall.guard()
    as input_seconds, so the probe that enforces the cap is also the
    probe that prices the job. One ffprobe, two decisions - and they can
    never disagree about how long the file is.

    ------------------------------------------------------------------
    job_type (ADDED 2026-08-30) - the per-tool cap, finally connected.

    See the module docstring for how long AUDIO_TOOL_MAX_DURATION_SECONDS
    sat in config.py with zero readers. Short version: pitch and tempo's
    900s entries did nothing, every tool got the 3600s fallback, and a
    50-minute file was accepted into a slot it could never finish in.

    PRECEDENCE, highest first:
      1. an explicit max_seconds from the caller. routes/transcribe.py
         and routes/midi.py pass their own caps and must keep winning -
         transcribe's value also feeds paywall.guard(), so overriding it
         here would change what a job costs.
      2. this tool's entry in AUDIO_TOOL_MAX_DURATION_SECONDS, keyed by
         the same job_type string passed to create_job().
      3. validate_duration()'s own signature default, which IS
         MAX_AUDIO_TOOL_DURATION_SECONDS.

    .get() with NO default is the point: a tool absent from the map
    leaves max_seconds None and falls through to (3) untouched. Writing
    `.get(job_type, MAX_AUDIO_TOOL_DURATION_SECONDS)` instead would put a
    second copy of the fallback here, and two statements of one number is
    how the /limits drift started.

    OPTIONAL, not required, and that is deliberate rather than lazy. The
    three transcription routes and both MIDI routes call this with an
    explicit max_seconds and no job_type; making job_type mandatory would
    force five call sites to pass a value that (1) above then discards.
    """
    # Resolve the per-tool cap only when the caller didn't name one. The
    # `max_seconds is None` guard is what preserves rule (1) above - a
    # route that brought its own number is never second-guessed by the
    # map.
    if max_seconds is None and job_type is not None:
        max_seconds = AUDIO_TOOL_MAX_DURATION_SECONDS.get(job_type)

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
    The bounded queue for all three transcription routes.

    IMPORTED BY routes/transcribe.py, routes/youtube_transcribe.py AND
    routes/video_transcribe.py. Same deletion hazard as the separation
    guard above.

    Same mechanism and same reasoning as
    _reject_if_separation_queue_full() above - see that docstring for the
    full argument. Two things differ and both matter:

    COUNTS ALL THREE ENDPOINTS TOGETHER. /speech-to-text,
    /youtube/transcribe and /video-to-text share a single semaphore, so
    counting one in isolation would let the others fill the queue
    unnoticed. A user uploading a file genuinely is behind everyone who
    pasted a YouTube link, and the guard has to reflect that.

    It is the same argument that made all three share ONE credits rule
    key ("transcribe") rather than three: one resource, one bucket. A
    caller who got three independent budgets for one GPU endpoint would
    be exactly the drift config.py already complains about in its note
    on the per-path separation limits.

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
    transcription guard counts all its endpoints: one semaphore, one
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


def _reject_if_midi_hq_queue_full():
    """
    The bounded queue for /audio-to-midi-hq.

    The FOURTH guard in this file, and the first one added for a tool
    that did not exist yet - the previous three were all written after
    the bug they prevent had already happened in production. Worth
    saying, because the reasoning is identical and the cost of adding it
    up front is one function:

    MAX_CONCURRENT_MIDI_HQ caps how many jobs RUN at once, but the
    semaphore enforcing it is acquired INSIDE the background task (see
    _run_tool_job's `async with semaphore`). Without this check,
    submissions past that limit are never refused - they queue in memory
    with no ceiling, each holding an uploaded file on disk and a
    job-table row, while the person watching the spinner has no way to
    know they are tenth in line.

    COUNTS ONLY audio_to_midi_hq. Deliberately NOT the free
    /audio-to-midi: that runs on _midi_semaphore against a CPU sidecar
    on this box, this runs on _midi_hq_semaphore against paid GPU
    capacity. They are different pools with different limits, and
    counting them together would let a busy midi-worker reject a paid
    job that never touches it.

    THE RATE LIMITER DOES NOT COVER THIS. It is per-IP; this is a
    whole-server capacity bound. Ten visitors each submitting their
    permitted allowance is ten queued jobs and zero rate-limit
    violations - good traffic producing the failure case, which is
    exactly the shape that made this guard necessary for the other three
    pools.

    503, not 429: the server is at capacity, the caller did nothing
    wrong, and a client deciding whether to retry needs to tell those
    apart.
    """
    depth = count_processing(MIDI_HQ_JOB_TYPES)
    if depth >= MAX_QUEUED_MIDI_HQ:
        logger.warning(
            f"[MIDI_HQ] Rejected submission - queue full "
            f"({depth}/{MAX_QUEUED_MIDI_HQ} jobs in flight)"
        )
        raise HTTPException(
            503,
            "The high-quality MIDI queue is full right now. These jobs take "
            "under a minute each - please try again shortly.",
        )


def _download_filename(job_id: str, fmt: str, suffix: str, fallback: str = "audio") -> str:
    """Content-Disposition name for a processed download: the ORIGINAL
    upload's name plus the tool's transformation - song.mp3 trimmed becomes
    song_trimmed.mp3, not trimmed.mp3.

    Falls back to a generic base only when the job has no stored title, so a
    download never 500s over a missing/odd filename. This is a header value,
    not a filesystem path, so no traversal concern; path separators and
    control chars are stripped anyway to keep the header well-formed.
    """
    job = get_job(job_id)
    title = (job.get("title") if job else None) or ""
    base = os.path.splitext(title)[0].replace("/", "_").replace("\\", "_")
    base = "".join(c for c in base if c.isprintable() and c not in '"\r\n\t').strip()
    base = base or fallback
    return f"{base}_{suffix}.{fmt}"


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

    NONE OF THESE TOOLS ARE METERED, and none should be: they are
    ffmpeg/rubberband work costing fractions of a cent, and they are the
    reason people arrive at the site. _run_tool_job's settle_or_refund()
    call is a no-op for every job submitted through here, because no
    charge row exists, and metered_tool is left at None so no metrics row
    is touched either. See credits/config.py's DEFAULT_TOOL_RULES for
    what is metered and why.
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
        # job_type is forwarded so the per-tool cap in
        # AUDIO_TOOL_MAX_DURATION_SECONDS applies - pitch and tempo are
        # 900s, everything else falls through to
        # MAX_AUDIO_TOOL_DURATION_SECONDS. max_duration_seconds still
        # wins when a caller passes one; see the precedence list in
        # _validate_duration_or_reject's docstring.
        duration = await _validate_duration_or_reject(
            job_id, input_path, max_duration_seconds, job_type=job_type
        )

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