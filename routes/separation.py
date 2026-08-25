"""
routes/separation.py - Demucs separation on a directly uploaded file:
/separate, /separate-hq (vocal/instrumental) and /stems, /stems-hq (full
4-stem). The /youtube/separate* and /youtube/stems* equivalents (which
chain a download in front of the same Demucs work) live in
routes/youtube.py instead - see that file's module docstring for why
they're grouped with the other YouTube tools rather than here.

Split out of the old monolithic routes.py (2026-08-14 restructure). Pure
move: every docstring, comment, and line of logic here is unchanged from
its original location.

Four routes sharing one model, one semaphore and one queue. The vocal
remover is NOT cheaper than the stem splitter: Demucs separates all four
sources internally either way, and --two-stems just sums three of them
for us.

--------------------------------------------------------------------------
WHAT CHANGED (2026-08-22): DOCSTRINGS, NOT BEHAVIOUR

Three route docstrings below described separation as CPU work on this
VPS - "1-5+ minutes on CPU", "roughly 5x the CPU time", "same CPU cost".
None of that has been true since the GPU migration: separation.py
submits to a RunPod Serverless worker and awaits an HTTP call, and the
only local work per job is a single ffprobe duration check. See
separation.py's own module docstring for the full architecture.

The relative claims were still right - HQ really does cost several times
what standard costs, and /stems really does cost the same as /separate -
so only the noun changed. But a docstring naming the wrong machine is
how a stale assumption survives a migration, which is exactly what
happened to MAX_CONCURRENT_SEPARATIONS: its comment argued from "4
cores" long after the cores stopped being involved, and that argument
kept a second paid RunPod worker idle. Corrected here in the same pass
that fixed the constant.

No code, status codes, or response shapes changed.
--------------------------------------------------------------------------

--------------------------------------------------------------------------
WHAT CHANGED (2026-08-25): CREDITS ON THE TWO HQ ROUTES

Three changes, all confined to the HQ tier. With PAYWALL_ENABLED unset -
which is how this ships - every one of them is inert and all four routes
behave exactly as they did yesterday.

1. THE INPUT FILE IS RETAINED. _queue_separation() now calls
   set_job_input() and passes an EMPTY cleanup_paths, so the uploaded
   source survives until the job's TTL sweep instead of being deleted
   seconds after the job finishes. That is what makes
   routes/separation_upgrade.py possible - "upgrade this to HQ" re-runs
   a finished standard job over bytes the server already has, rather
   than asking for a second upload. See jobs.py's 2026-08-25 note for
   the disk cost and the knob that bounds it.

   This applies to ALL FOUR routes, not just HQ: a standard job is
   precisely the one someone upgrades from, so it is the one that must
   keep its input.

2. THE HQ ROUTES CHARGE A CREDIT. Guarded by rule_key, which is None for
   the two standard routes - they can never charge, structurally, not
   just by configuration.

3. THE HQ RATE LIMITS ARE TIER-AWARE. tiered_rate_limit() replaces the
   partial(check_rate_limit, ...) on the two HQ routes only. Free
   callers get exactly today's 1/hour; callers holding credits get the
   looser paid limit keyed on their account. See credits/limits.py for
   why loosening it for paid callers is safe - the argument is config.py's
   own, that MAX_QUEUED_SEPARATIONS is what protects the server and this
   number never was.

WHY DURATION IS PROBED AT SUBMIT ON THE HQ PATH. _run_demucs_on_gpu()
already validates duration against MAX_SEPARATION_DURATION_SECONDS_HQ,
but it does so inside the background task - after the credit has been
taken. paywall.guard() would not return it either: the guard covers the
enqueue, and the enqueue succeeded. The credit would come back only via
the 90-minute stale-hold sweeper, which is not an acceptable answer to
"it charged me and then errored". So the HQ path pays for one extra
local ffprobe at submit time and rejects with a clean 400 before any
charge. The standard path is unchanged and does not probe here.
--------------------------------------------------------------------------
"""
import os
import time
import asyncio
from functools import partial

from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Query
from fastapi.responses import JSONResponse, FileResponse

from config import (
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
    MAX_QUEUED_SEPARATIONS,
)
from rate_limit import check_rate_limit
from jobs import (
    create_job,
    mark_complete,
    mark_stems_complete,
    mark_failed,
    set_job_input,
    get_job,
    count_processing,
    SEPARATION_JOB_TYPES,
)
from separation import run_separation, run_stem_separation, get_audio_duration_seconds, SeparationError
from utils import _separation_semaphore, run_blocking, cleanup_file
from log_stream import remember_job_tags, set_job_context, tag_from_job

# The credits package is self-contained and inert while PAYWALL_ENABLED
# is unset - importing it does not change any behaviour on these routes.
from credits import metering, paywall
from credits.identity import Identity
from credits.limits import tiered_rate_limit

from ._shared import spawn_background_task, _accept_upload, _log_queued, _reject_if_separation_queue_full, _run_tool_job

router = APIRouter()


async def _queue_separation(
    file: UploadFile,
    *,
    job_type: str,
    tool: str,
    model: str,
    overlap: float,
    timeout_seconds: int,
    max_duration_seconds: int,
    metric_label: str,
    hq: bool = False,
    identity: Identity = None,
    rule_key: str = None,
) -> JSONResponse:
    """
    Shared submit path for all four separation routes. They differ only
    in run knobs, rate limit and output shape, so the accept-and-queue
    sequence lives here once.

    Knobs are resolved by the CALLER at submission time and passed in, so
    a config change (or the HQ kill switch flipping) can never alter a
    job that is already queued - it runs with the settings it was
    accepted under.

    `hq` is explicit rather than inferred from the model name because the
    GPU budget gate and billing need a reliable tier signal that survives
    someone adding a differently-named model later.

    `rule_key` (added 2026-08-25) is the credits rule this route bills
    against - "separate-hq" or "stems-hq" - or None for the two standard
    routes. None is not a configuration choice, it is structural: a
    route that passes no rule_key cannot charge for a job no matter what
    any env var says, which is the property worth having on the tier
    that is promised free forever.

    `identity` is only meaningful alongside rule_key; it comes from the
    signed cookie via paywall.get_identity.
    """
    # `tool` here is the log-prefix string ("STEMS", "STEMS_HQ", ...),
    # which already encodes tier for historical reasons - but tool/tier
    # in the DATABASE are kept as two SEPARATE columns, deliberately, so
    # a filter for tool=STEMS matches BOTH tiers and tier=hq narrows
    # further. Stripping the suffix here is what keeps those two axes
    # from collapsing back into the single log-prefix string.
    base_tool = tool[:-3] if tool.endswith("_HQ") else tool
    set_job_context(tool=base_tool, tier="hq" if hq else "standard")

    _reject_if_separation_queue_full()

    original_filename = file.filename

    job_id = create_job(job_type=job_type)

    remember_job_tags(job_id)
    file_path, size = await _accept_upload(file, job_id, label=tool.lower())

    # Retain the source for this job's TTL so a completed job can be
    # upgraded to HQ without a second upload. Paired with the empty
    # cleanup_paths below - from here the TTL sweep owns this file, not
    # the background task. See jobs.py's set_job_input() docstring.
    set_job_input(job_id, file_path)

    # Open the metrics row for EVERY separation job - paid or free,
    # metered or not. This must NOT be conditional on the paywall being
    # on: the entire point of shipping with PAYWALL_ENABLED=false first
    # is to collect real cost data before charging anyone, and a row
    # that only exists for billable jobs collects exactly the subset
    # least useful for deciding whether the price is right.
    #
    # input_seconds is deliberately omitted here. Only the billable path
    # probes at submit; separation.py's _run_demucs_on_gpu() probes every
    # job and fills it in via record_input_duration() a moment later.
    metering.record_job_created(
        job_id=job_id,
        tool=rule_key or metric_label.lstrip("/"),
        subject_id=identity.subject_id if identity else None,
        account_id=identity.account_id if identity else None,
        ip_hash=identity.ip_hash if identity else None,
        input_bytes=size,
        charge_type=None,   # stamped below once the charge is known
    )

    # Billable routes only: probe duration NOW, before any charge, so an
    # over-length track gets an immediate 400 rather than a credit
    # followed by a background failure. See this module's 2026-08-25
    # note for why the guard cannot rescue that case.
    duration = None
    if rule_key is not None:
        try:
            duration = await run_blocking(get_audio_duration_seconds, file_path)
        except SeparationError as e:
            cleanup_file(file_path)
            mark_failed(job_id, str(e))
            raise HTTPException(400, {"kind": "unreadable_audio", "message": str(e)})

        if duration > max_duration_seconds:
            message = (
                f"This track is {int(duration // 60)} min long. Studio Quality is limited "
                f"to {max_duration_seconds // 60} min because it costs several times more "
                f"to run. Standard separation still works at full length."
            )
            cleanup_file(file_path)
            mark_failed(job_id, message)
            # Structured, not a bare string: the frontend's ApiError.kind
            # carries an explicit "branch on this, never on message"
            # contract, and this rejection has to be told apart from
            # "out of credits" (402) and from a generic 400. A reworded
            # sentence must never change frontend behaviour.
            raise HTTPException(400, {
                "kind": "hq_duration_exceeded",
                "message": message,
                "input_seconds": round(duration, 1),
                "max_seconds": max_duration_seconds,
            })

    is_stems = job_type in ("stems",)

    if is_stems:
        # No run_blocking() here - run_stem_separation() is now `async
        # def` (it awaits an HTTP call to the RunPod GPU worker, not a
        # blocking local subprocess). run_blocking() exists specifically
        # to offload BLOCKING calls off the event loop; wrapping an
        # already-async function in it would be a real bug, not a style
        # choice - see separation.py's own module docstring for the full
        # "why this changed" reasoning.
        work = lambda: run_stem_separation(
            file_path, job_id, model, overlap, timeout_seconds, max_duration_seconds,
        )
        on_success = lambda stems: mark_stems_complete(job_id, original_filename, stems)
        success_detail = lambda stems: f"{len(stems)} stems"
        generic_error = "Stem separation failed unexpectedly."
    else:
        work = lambda: run_separation(
            file_path, job_id, model, overlap, timeout_seconds, max_duration_seconds,
        )
        on_success = lambda paths: mark_complete(job_id, original_filename, paths[0], paths[1])
        success_detail = None
        generic_error = "Separation failed unexpectedly."

    def _spawn():
        spawn_background_task(_run_tool_job(
            tool=tool,
            metric=metric_label,
            job_id=job_id,
            semaphore=_separation_semaphore,
            work=work,
            on_success=on_success,
            generic_error=generic_error,
            # EMPTY, not [file_path]. The input is retained for the
            # upgrade path and reclaimed by cleanup_expired_jobs() on
            # this job's TTL. Every non-separation tool still passes its
            # input here and still deletes it immediately.
            cleanup_paths=[],
            success_detail=success_detail,
            # False: separation.py records the worker's own reported
            # gpu_seconds instead - see this function's gpu_billed docstring
            # for why counting both would double-bill the budget.
            gpu_billed=False,
        ))

    billing = None
    if rule_key is not None:
        # Charge, then enqueue INSIDE the guard: if spawning raises, the
        # credit is returned before the exception leaves the block. A 402
        # is raised before the body runs when the caller can't pay, and
        # its detail carries the pack list the frontend modal renders.
        async with paywall.guard(
            identity, job_id=job_id, tool=rule_key, input_seconds=duration
        ) as charge:
            _spawn()
        billing = {
            "charged": charge.charge_type,
            "balance": charge.balance_after,
            "free_remaining": charge.free_remaining_after,
        }
        # Stamp the outcome now that it's known. INSERT OR REPLACE, so
        # this cleanly supersedes the row opened above rather than
        # needing a separate update path.
        metering.record_job_created(
            job_id=job_id,
            tool=rule_key,
            subject_id=identity.subject_id,
            account_id=identity.account_id,
            ip_hash=identity.ip_hash,
            input_seconds=duration,
            input_bytes=size,
            charge_type=charge.charge_type,
        )
    else:
        _spawn()

    depth = count_processing(SEPARATION_JOB_TYPES)
    detail = f"model={model} queue={depth}/{MAX_QUEUED_SEPARATIONS}"
    if billing:
        detail += f" charged={billing['charged']}"
    _log_queued(tool, job_id, original_filename, size, detail)

    payload = {"job_id": job_id, "status": "processing"}
    if billing:
        payload["billing"] = billing
    return JSONResponse(payload)


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
    Accepts an audio file, returns a job_id immediately, and runs Demucs
    vocal/instrumental separation in the background on the RunPod GPU
    worker. Poll GET /separate/status/{job_id}.

    Backgrounded because it still takes longer than a comfortable request
    window - roughly 20-60 seconds on GPU, plus a cold start when no
    worker is warm. (This docstring used to say "1-5+ minutes on CPU",
    which was true before the GPU migration and is the figure a lot of
    the surrounding copy was originally sized against.)

    FREE FOREVER. No rule_key is passed, so this route has no code path
    that reaches the credit ledger regardless of configuration.
    """
    return await _queue_separation(
        file,
        job_type="separation",
        tool="SEPARATION",
        model=SEPARATION_MODEL,
        overlap=SEPARATION_OVERLAP,
        timeout_seconds=DEMUCS_TIMEOUT_SECONDS,
        max_duration_seconds=MAX_SEPARATION_DURATION_SECONDS,
        metric_label="/separate",
        hq=False,
    )


@router.post(
    "/separate-hq",
    dependencies=[Depends(tiered_rate_limit(
        "separate-hq",
        free_max=SEPARATION_HQ_RATE_LIMIT_MAX_REQUESTS,
        free_window=SEPARATION_HQ_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def separate_audio_hq(
    file: UploadFile = File(...),
    identity: Identity = Depends(paywall.get_identity),
):
    """
    High-quality separation: htdemucs_ft (a 4-model ensemble) at raised
    overlap. Roughly 5x the compute of /separate - four forward passes
    instead of one, plus the overlap increase - so it gets a longer
    timeout, a TIGHTER input duration cap, and a stricter rate limit.

    That 5x is a ratio, not a wall-clock figure: it held when this ran on
    the VPS CPU and it still holds on the GPU worker, where it works out
    at roughly 1-2 minutes rather than the 15-20 the CPU path took.

    A separate route rather than a `quality` form field because rate-limit
    dependencies are evaluated before the request body is read - a
    Depends() cannot see a Form value, so per-tier limits need per-tier
    routes.

    Costs one credit when PAYWALL_TOOL_SEPARATE_HQ_ENABLED is on; free
    and unchanged when it isn't.
    """
    if not SEPARATION_HQ_ENABLED:
        raise HTTPException(
            503,
            "High quality separation is temporarily unavailable due to server load. "
            "Please use standard separation."
        )

    return await _queue_separation(
        file,
        job_type="separation",
        tool="SEPARATION_HQ",
        model=SEPARATION_MODEL_HQ,
        overlap=SEPARATION_OVERLAP_HQ,
        timeout_seconds=DEMUCS_TIMEOUT_SECONDS_HQ,
        max_duration_seconds=MAX_SEPARATION_DURATION_SECONDS_HQ,
        metric_label="/separate-hq",
        hq=True,
        identity=identity,
        rule_key="separate-hq",
    )


@router.get("/separate/status/{job_id}")
async def separation_status(job_id: str):
    tag_from_job(job_id)
    job = get_job(job_id)
    if job is None:
        raise HTTPException(404, "Job not found (it may have expired).")
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
        "elapsed_seconds": round(time.time() - job["created_at"], 1),
    }


def _resolve_stem_path(job_id: str, stem: str) -> str:
    tag_from_job(job_id)
    if stem not in ("vocals", "instrumental"):
        raise HTTPException(400, "stem must be 'vocals' or 'instrumental'")
    job = get_job(job_id)
    if job is None:
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] == "failed":
        raise HTTPException(409, job.get("error") or "This job failed.")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    path = job["vocals_path"] if stem == "vocals" else job["instrumental_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Stem file not found (it may have expired).")
    return path


@router.get("/separate/preview/{job_id}")
async def separation_preview(job_id: str, stem: str = Query(...)):
    """Streams the audio inline for in-browser <audio> playback - no
    Content-Disposition: attachment, unlike /download below."""
    path = _resolve_stem_path(job_id, stem)
    return FileResponse(path, media_type="audio/wav")


@router.get("/separate/download/{job_id}")
async def separation_download(job_id: str, stem: str = Query(...)):
    """Same file as /preview, served as a downloadable attachment."""
    path = _resolve_stem_path(job_id, stem)
    return FileResponse(path, media_type="audio/wav", filename=f"{stem}.wav")


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
    Full 4-stem separation (vocals/drums/bass/other). Same model, same
    semaphore and the same compute cost as /separate - the only
    difference is that the four internally-separated sources are kept as
    individual files instead of three being summed into one
    instrumental.

    Worth restating because it is genuinely counterintuitive and drives
    the rate limits: --two-stems does NOT make the vocal remover cheaper.
    Demucs separates all four sources either way.

    FREE FOREVER, same as /separate - no rule_key, no path to the ledger.
    """
    return await _queue_separation(
        file,
        job_type="stems",
        tool="STEMS",
        model=SEPARATION_MODEL,
        overlap=SEPARATION_OVERLAP,
        timeout_seconds=DEMUCS_TIMEOUT_SECONDS,
        max_duration_seconds=MAX_SEPARATION_DURATION_SECONDS,
        metric_label="/stems",
        hq=False,
    )


@router.post(
    "/stems-hq",
    dependencies=[Depends(tiered_rate_limit(
        "stems-hq",
        free_max=STEMS_HQ_RATE_LIMIT_MAX_REQUESTS,
        free_window=STEMS_HQ_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def stems_route_hq(
    file: UploadFile = File(...),
    identity: Identity = Depends(paywall.get_identity),
):
    """High-quality full stem separation - same knobs and kill switch as
    /separate-hq, and the same one-credit cost when metered."""
    if not SEPARATION_HQ_ENABLED:
        raise HTTPException(
            503,
            "High quality separation is temporarily unavailable due to server load. "
            "Please use standard stem separation."
        )

    return await _queue_separation(
        file,
        job_type="stems",
        tool="STEMS_HQ",
        model=SEPARATION_MODEL_HQ,
        overlap=SEPARATION_OVERLAP_HQ,
        timeout_seconds=DEMUCS_TIMEOUT_SECONDS_HQ,
        max_duration_seconds=MAX_SEPARATION_DURATION_SECONDS_HQ,
        metric_label="/stems-hq",
        hq=True,
        identity=identity,
        rule_key="stems-hq",
    )


@router.get("/stems/status/{job_id}")
async def stems_status(job_id: str):
    """Returns the usual status fields plus the stem names actually
    available, so the frontend renders download buttons from the response
    instead of hardcoding names that would break if a different model
    were ever configured."""
    tag_from_job(job_id)
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
        "elapsed_seconds": round(time.time() - job["created_at"], 1),
    }


def _resolve_stems_file(job_id: str, stem: str) -> str:
    """Validates the requested stem against the job's OWN stem dict rather
    than a hardcoded tuple, so the valid set always follows whatever model
    produced the job."""
    tag_from_job(job_id)
    job = get_job(job_id)
    if job is None or job["job_type"] != "stems":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] == "failed":
        raise HTTPException(409, job.get("error") or "This job failed.")
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
    path = _resolve_stems_file(job_id, stem)
    return FileResponse(path, media_type="audio/wav")


@router.get("/stems/download/{job_id}")
async def stems_download(job_id: str, stem: str = Query(...)):
    path = _resolve_stems_file(job_id, stem)
    return FileResponse(path, media_type="audio/wav", filename=f"{stem}.wav")