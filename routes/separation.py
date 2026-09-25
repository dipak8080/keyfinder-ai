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

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends, Path, Query, Request, Response
from fastapi.responses import JSONResponse, FileResponse

from config import (
    logger,
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
    MAX_UPLOAD_BYTES,
    MAX_VIDEO_UPLOAD_BYTES,
    ALLOWED_VIDEO_INPUT_FORMATS,
)
from audio_common import AudioToolError
from rate_limit import check_rate_limit
from separation_limits import shared_separation_limit
from jobs import (
    create_job,
    mark_complete,
    mark_stems_complete,
    mark_failed,
    fail_if_unfinished,
    set_job_fields,
    set_job_input,
    get_job,
    count_processing,
    SEPARATION_JOB_TYPES,
)
from separation import run_separation, run_stem_separation, get_audio_duration_seconds, SeparationError, extra_stem_paths
from utils import _separation_semaphore, run_blocking, cleanup_file
from log_stream import remember_job_tags, set_job_context, tag_from_job
from redis_store import client as _redis

# The credits package is self-contained and inert while PAYWALL_ENABLED
# is unset - importing it does not change any behaviour on these routes.
from credits import metering, paywall
from credits.identity import Identity
from credits.config import get_settings as get_credit_settings
from credits.limits import tiered_rate_limit

from ._shared import stem_download_response, spawn_background_task, _accept_upload, _log_queued, _reject_if_separation_queue_full, _run_tool_job

router = APIRouter()


_COPY_TARGET = {"aac": "m4a", "mp3": "mp3", "flac": "flac", "opus": "ogg", "vorbis": "ogg", "pcm_s16le": "wav"}


def _is_video_upload(filename: str | None) -> bool:
    ext = (filename or "").rsplit(".", 1)[-1].strip().lower() if "." in (filename or "") else ""
    return ext in ALLOWED_VIDEO_INPUT_FORMATS


async def _audio_from_video(job_id: str, video_path: str) -> tuple[str, int]:
    """Keeps only the audio of an uploaded video, copied without re-encoding
    when the codec allows, otherwise as lossless FLAC. The video is deleted
    so only audio travels to the GPU worker."""
    from video_to_audio import probe_audio_stream, extract_audio

    base = video_path.rsplit(".", 1)[0]
    try:
        codec, _ = await run_blocking(probe_audio_stream, video_path)
        target = _COPY_TARGET.get(codec, "flac")
        audio_path = f"{base}_audio.{target}"
        await run_blocking(extract_audio, video_path, audio_path, target)
    except AudioToolError as e:
        cleanup_file(video_path)
        mark_failed(job_id, str(e))
        raise HTTPException(400, {"kind": "video_unreadable", "message": str(e)})
    cleanup_file(video_path)
    return audio_path, os.path.getsize(audio_path)


def youtube_studio_enabled() -> bool:
    return get_credit_settings().youtube_studio_enabled


@router.get("/studio/config")
def studio_config(response: Response) -> dict:
    """Which Studio features are switched on right now. Read at runtime by
    the frontend so an admin toggle takes effect without a redeploy."""
    response.headers["Cache-Control"] = "no-store"
    s = get_credit_settings()
    return {
        "youtube_studio": s.youtube_studio_enabled,
        "vocal_options": s.studio_vocal_options_enabled,
        "six_stems": s.studio_six_stems_enabled,
        "option_credits": s.studio_option_credits,
        "six_stem_credits": s.studio_six_stem_credits,
        "free_run_covers_extras": False,
        "preview": s.studio_preview_enabled,
        "preview_seconds": s.studio_preview_seconds,
        "preview_on_click": True,
        "google_signin": bool(s.google_client_id and s.google_client_secret),
        "video_input": {
            "formats": sorted(ALLOWED_VIDEO_INPUT_FORMATS),
            "max_mb": max(MAX_UPLOAD_BYTES, MAX_VIDEO_UPLOAD_BYTES) // (1024 * 1024),
        },
        "signup_bonus_credits": s.signup_bonus_credits,
        "free_needs_account": s.free_ops_require_account,
        "referral": {"enabled": s.referral_enabled, "reward_credits": s.referral_reward_credits},
        "library": {"enabled": s.library_enabled, "retention_days": s.library_retention_days},
        "studio_pass": {
            "available": s.studio_pass_enabled,
            "price_usd": s.studio_pass_price_usd,
            "credits_per_month": s.studio_pass_credits,
            "options_included": s.studio_pass_options_included,
            "rollover_months": s.studio_pass_rollover_months,
        },
    }


def studio_options(dereverb: bool = False, lead_back: bool = False, stem_count: int = 4):
    """Validates Studio extras against their kill switches.

    Returns (vocal_options, stem_count, extra_credits, waivable_credits),
    where waivable_credits is the part a Studio Pass covers. Raises a
    structured 400 when an extra is requested but switched off, so the
    frontend can hide it instead of failing mid-run."""
    settings = get_credit_settings()
    vocal_options = tuple(o for o, on in (("dereverb", dereverb), ("lead_back", lead_back)) if on)
    if vocal_options and not settings.studio_vocal_options_enabled:
        raise HTTPException(400, {"kind": "studio_option_unavailable",
                                  "message": "Vocal options are not available yet."})
    if stem_count not in (4, 6):
        raise HTTPException(400, {"kind": "invalid_stem_count", "message": "stem_count must be 4 or 6."})
    if stem_count == 6 and not settings.studio_six_stems_enabled:
        raise HTTPException(400, {"kind": "studio_option_unavailable",
                                  "message": "6-stem separation is not available yet."})
    extra, waivable = paywall.studio_extra(vocal_options, stem_count)
    return vocal_options, stem_count, extra, waivable


STUDIO_PREVIEW_TIMEOUT_SECONDS = 300
STUDIO_PREVIEW_START_FRACTION = 0.35


def _reserve_studio_preview(identity: Identity):
    """Takes one free preview from today's allowance. Returns None when
    granted, else the reason it was refused."""
    settings = get_credit_settings()
    if not settings.studio_preview_enabled:
        return "disabled"
    if identity is None:
        return "no_identity"
    day = time.strftime("%Y%m%d", time.gmtime())
    keys = (
        (f"af:preview:owner:{identity.owner_key}:{day}", settings.studio_preview_daily_per_subject),
        (f"af:preview:ip:{identity.ip_hash}:{day}", settings.studio_preview_daily_per_ip),
    )
    pipe = _redis.pipeline()
    for key, _ in keys:
        pipe.incr(key)
        pipe.expire(key, 2 * 86400)
    counts = pipe.execute()[0::2]
    if any(count > limit for count, (_, limit) in zip(counts, keys)):
        pipe = _redis.pipeline()
        for key, _ in keys:
            pipe.decr(key)
        pipe.execute()
        return "daily_limit"
    return None


async def _run_studio_preview(source_job_id: str, preview_id: str, file_path: str, *,
                              is_stems: bool, title: str, identity: Identity,
                              vocal_options: tuple, stem_count: int) -> None:
    """Runs a short Studio clip of a finished Standard job's input, when the
    user asks for it."""
    try:
        source = get_job(source_job_id) or {}
        if source.get("status") != "complete":
            mark_failed(preview_id, "The free result did not finish, so there is no preview.")
            set_job_fields(preview_id, preview_skip="source_failed")
            return
        try:
            duration = await run_blocking(get_audio_duration_seconds, file_path)
        except SeparationError:
            mark_failed(preview_id, "Preview unavailable for this file.")
            set_job_fields(preview_id, preview_skip="unreadable")
            return
        if duration > MAX_SEPARATION_DURATION_SECONDS_HQ:
            mark_failed(preview_id, "This track is longer than Studio Quality supports.")
            set_job_fields(preview_id, preview_skip="too_long_for_studio")
            return
        settings = get_credit_settings()
        budget = settings.free_gpu_daily_budget_usd
        if budget > 0:
            spend = await asyncio.to_thread(metering.free_gpu_spend_today)
            if spend["projected_usd"] >= budget:
                mark_failed(preview_id, "Studio previews are paused for today.")
                set_job_fields(preview_id, preview_skip="paused")
                return

        seconds = float(min(settings.studio_preview_seconds, duration))
        start = max(0.0, min(duration * STUDIO_PREVIEW_START_FRACTION, duration - seconds))
        set_job_fields(preview_id, clip={"start": round(start, 1), "seconds": round(seconds, 1),
                                         "source_duration": round(duration, 1)})
        metering.record_job_created(
            job_id=preview_id, tool="studio-preview",
            subject_id=identity.subject_id, account_id=identity.account_id,
            ip_hash=identity.ip_hash, input_seconds=seconds, charge_type=None,
        )

        if is_stems:
            work = lambda: run_stem_separation(
                file_path, preview_id, SEPARATION_MODEL_HQ, SEPARATION_OVERLAP_HQ,
                STUDIO_PREVIEW_TIMEOUT_SECONDS, MAX_SEPARATION_DURATION_SECONDS_HQ,
                vocal_options, stem_count, clip_start=start, clip_seconds=seconds,
            )
            on_success = lambda stems: mark_stems_complete(preview_id, title, stems)
        else:
            work = lambda: run_separation(
                file_path, preview_id, SEPARATION_MODEL_HQ, SEPARATION_OVERLAP_HQ,
                STUDIO_PREVIEW_TIMEOUT_SECONDS, MAX_SEPARATION_DURATION_SECONDS_HQ,
                vocal_options, clip_start=start, clip_seconds=seconds,
            )
            on_success = lambda paths: mark_complete(
                preview_id, title, paths[0], paths[1],
                extra_stem_paths(preview_id, vocal_options) or None,
            )

        await _run_tool_job(
            tool="STUDIO_PREVIEW", metric="/studio-preview", job_id=preview_id,
            semaphore=_separation_semaphore, work=work, on_success=on_success,
            generic_error="Studio preview failed.", cleanup_paths=[],
            gpu_billed=False, metered_tool="studio-preview",
        )
    finally:
        fail_if_unfinished(preview_id, "Studio preview did not finish.")


PREVIEWABLE_JOB_TYPES = ("separation", "stems", "youtube_separate", "youtube_stems")


def _preview_limit(request: Request) -> None:
    check_rate_limit(request, max_requests=20, window_seconds=3600, bucket_key="/studio/preview")


def _preview_key(vocal_options: tuple, stem_count: int) -> str:
    return f"{','.join(sorted(vocal_options)) or 'plain'}:{stem_count}"


def _unavailable(status: int, reason: str, message: str):
    raise HTTPException(status, {"kind": "preview_unavailable", "reason": reason, "message": message})


@router.post("/studio/preview/{job_id}", dependencies=[Depends(_preview_limit)])
async def studio_preview(
    job_id: str = Path(..., max_length=64),
    dereverb: bool = Query(False),
    lead_back: bool = Query(False),
    stem_count: int = Query(4),
    identity: Identity = Depends(paywall.get_identity),
):
    """Hear Studio on my song: a free clip of a finished Standard job,
    started only when the user clicks. Same options on the same job return
    the preview already made."""
    tag_from_job(job_id)
    job = get_job(job_id)
    if job is None or job.get("job_type") not in PREVIEWABLE_JOB_TYPES or not job.get("studio_previewable"):
        _unavailable(404, "not_previewable", "This result can't be previewed in Studio.")
    if job["job_type"].startswith("youtube") and not youtube_studio_enabled():
        _unavailable(403, "disabled", "Studio previews for links are switched off right now.")
    if not get_credit_settings().studio_preview_enabled:
        _unavailable(403, "disabled", "Studio previews are switched off right now.")
    if job.get("status") != "complete":
        _unavailable(409, "source_not_complete", "Wait for the free result to finish first.")
    input_path = job.get("input_path")
    if not input_path or not os.path.exists(input_path):
        _unavailable(409, "input_expired", "This upload has expired. Run it again to preview.")

    is_stems = job["job_type"] in ("stems", "youtube_stems")
    options, count, _, _ = studio_options(dereverb, lead_back, stem_count if is_stems else 4)
    key = _preview_key(options, count)
    lock = f"af:preview:lock:{job_id}:{key}"
    if not _redis.set(lock, "1", nx=True, ex=30):
        _unavailable(409, "in_progress", "That preview is already starting.")
    try:
        previews = dict(job.get("previews") or {})
        existing = previews.get(key)
        if existing:
            prior = get_job(existing)
            if prior and prior.get("status") != "failed":
                return {"job_id": existing, "status": prior.get("status"), "reused": True,
                        "seconds": get_credit_settings().studio_preview_seconds}
        skip = _reserve_studio_preview(identity)
        if skip:
            _unavailable(429 if skip == "daily_limit" else 403, skip,
                         "You've used today's free Studio previews." if skip == "daily_limit"
                         else "Studio previews are not available right now.")
        preview_id = create_job(job_type=job["job_type"])
        set_job_fields(preview_id, preview_of=job_id, preview_options=list(options), preview_stem_count=count)
        previews[key] = preview_id
        set_job_fields(job_id, previews=previews, preview_job_id=preview_id)
    finally:
        _redis.delete(lock)

    spawn_background_task(_run_studio_preview(
        job_id, preview_id, input_path, is_stems=is_stems,
        title=job.get("title") or os.path.basename(input_path), identity=identity,
        vocal_options=options, stem_count=count,
    ))
    logger.info(f"[STUDIO_PREVIEW] job={preview_id} for {job_id} options={key}")
    return {"job_id": preview_id, "status": "processing", "reused": False,
            "seconds": get_credit_settings().studio_preview_seconds}


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
    vocal_options: tuple = (),
    stem_count: int = 4,
    extra_credits: int = 0,
    waivable_credits: int = 0,
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

    if rule_key is None and identity is not None:
        await paywall.free_gate(identity, tool=metric_label.lstrip("/"))

    original_filename = file.filename

    job_id = create_job(job_type=job_type)

    remember_job_tags(job_id)
    is_video = _is_video_upload(original_filename)
    file_path, size = await _accept_upload(
        file, job_id, label=tool.lower(),
        max_bytes=max(MAX_UPLOAD_BYTES, MAX_VIDEO_UPLOAD_BYTES) if is_video else MAX_UPLOAD_BYTES,
    )
    if is_video:
        file_path, size = await _audio_from_video(job_id, file_path)

    # Retain the source for this job's TTL so a completed job can be
    # upgraded to HQ without a second upload. Paired with the empty
    # cleanup_paths below - from here the TTL sweep owns this file, not
    # the background task. See jobs.py's set_job_input() docstring.
    set_job_input(job_id, file_path)
    if rule_key is not None:
        import library
        library.mark_owner(job_id, identity)
    else:
        set_job_fields(job_id, studio_previewable=True)

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
            metering.record_job_rejected(job_id, "unreadable_audio")
            raise HTTPException(400, {"kind": "unreadable_audio", "message": str(e)})

        if duration > max_duration_seconds:
            message = (
                f"This track is {int(duration // 60)} min long. Studio Quality is limited "
                f"to {max_duration_seconds // 60} min because it costs several times more "
                f"to run. Standard separation still works at full length."
            )
            cleanup_file(file_path)
            mark_failed(job_id, message)
            metering.record_job_rejected(job_id, "hq_duration_exceeded")
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
    run_ctx = {"paid": False}

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
            vocal_options, stem_count, paid=run_ctx["paid"],
        )
        on_success = lambda stems: mark_stems_complete(job_id, original_filename, stems)
        success_detail = lambda stems: f"{len(stems)} stems"
        generic_error = "Stem separation failed unexpectedly."
    else:
        work = lambda: run_separation(
            file_path, job_id, model, overlap, timeout_seconds, max_duration_seconds,
            vocal_options, paid=run_ctx["paid"],
        )
        on_success = lambda paths: mark_complete(
            job_id, original_filename, paths[0], paths[1],
            extra_stem_paths(job_id, vocal_options) or None,
        )
        success_detail = None
        generic_error = "Separation failed unexpectedly."

    def _spawn():
        standard = _run_tool_job(
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
            # Closes the gpu_job_metrics row this route opened at submit.
            # separation.py closes it on success and on RunPodJobError, but
            # nothing else - so a cancelled or restarted job sat at
            # status='created' forever and diluted cost-per-job. Only used
            # as a flag; the runner writes job_id and status, and
            # record_job_finished COALESCEs, so separation.py's more
            # precise values survive this later write.
            metered_tool=rule_key or metric_label.lstrip("/"),
        )
        spawn_background_task(standard)

    billing = None
    if rule_key is not None:
        # Charge, then enqueue INSIDE the guard: if spawning raises, the
        # credit is returned before the exception leaves the block. A 402
        # is raised before the body runs when the caller can't pay, and
        # its detail carries the pack list the frontend modal renders.
        try:
            async with paywall.guard(
                identity, job_id=job_id, tool=rule_key, input_seconds=duration,
                extra_credits=extra_credits, waivable_credits=waivable_credits,
            ) as charge:
                run_ctx["paid"] = charge.charge_type == "credit"
                _spawn()
        except BaseException:
            metering.record_job_rejected(job_id, "blocked_at_submit")
            raise
        billing = {
            "charged": charge.charge_type,
            "credits": charge.credits,
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
    if run_ctx["paid"]:
        detail += " lane=paid"
    if vocal_options or stem_count != 4:
        detail += f" options={','.join(vocal_options) or '-'} stems={stem_count}"
    if billing:
        detail += f" charged={billing['charged']}"
    _log_queued(tool, job_id, original_filename, size, detail)

    payload = {"job_id": job_id, "status": "processing"}
    if vocal_options or stem_count != 4:
        payload["studio"] = {"vocal_options": list(vocal_options), "stem_count": stem_count}
    if billing:
        payload["billing"] = billing
    return JSONResponse(payload)


@router.post(
    "/separate",
    dependencies=[Depends(shared_separation_limit)],
)
async def separate_audio(
    file: UploadFile = File(...),
    identity: Identity = Depends(paywall.get_identity),
):
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
        identity=identity,
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
    dereverb: bool = Form(False),
    lead_back: bool = Form(False),
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
    vocal_options, _, extra_credits, waivable_credits = studio_options(dereverb, lead_back)

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
        vocal_options=vocal_options,
        extra_credits=extra_credits,
        waivable_credits=waivable_credits,
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
        "extra_stems": sorted((job.get("stems") or {}).keys()),
        "elapsed_seconds": round(time.time() - job["created_at"], 1),
        **_preview_fields(job),
    }


def _preview_fields(job: dict) -> dict:
    fields = {}
    if job.get("preview_job_id"):
        fields["studio_preview_job_id"] = job["preview_job_id"]
    if job.get("previews"):
        fields["studio_previews"] = job["previews"]
    if job.get("preview_of"):
        fields["preview_of"] = job["preview_of"]
        fields["preview_options"] = job.get("preview_options") or []
        fields["preview_stem_count"] = job.get("preview_stem_count") or 4
        fields["clip"] = job.get("clip")
        fields["preview_skip"] = job.get("preview_skip")
    return fields


def _resolve_stem_path(job_id: str, stem: str) -> str:
    tag_from_job(job_id)
    job = get_job(job_id)
    extras = (job or {}).get("stems") or {}
    if stem not in ("vocals", "instrumental") and stem not in extras:
        allowed = ", ".join(["vocals", "instrumental", *sorted(extras)])
        raise HTTPException(400, f"stem must be one of: {allowed}")
    if job is None:
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] == "failed":
        raise HTTPException(409, job.get("error") or "This job failed.")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    if stem == "vocals":
        path = job["vocals_path"]
    elif stem == "instrumental":
        path = job["instrumental_path"]
    else:
        path = extras[stem]
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
async def separation_download(
    job_id: str,
    stem: str = Query(...),
    format: str = Query("wav", pattern="^(wav|mp3)$"),
):
    """Same file as /preview, served as a downloadable attachment.
    format=mp3 encodes a 320 kbps copy on first request (stem_mp3.py)."""
    path = _resolve_stem_path(job_id, stem)
    return await stem_download_response(path, stem, format)


@router.post(
    "/stems",
    dependencies=[Depends(shared_separation_limit)],
)
async def stems_route(
    file: UploadFile = File(...),
    identity: Identity = Depends(paywall.get_identity),
):
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
        identity=identity,
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
    dereverb: bool = Form(False),
    lead_back: bool = Form(False),
    stem_count: int = Form(4),
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
    vocal_options, stem_count, extra_credits, waivable_credits = studio_options(dereverb, lead_back, stem_count)

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
        vocal_options=vocal_options,
        stem_count=stem_count,
        extra_credits=extra_credits,
        waivable_credits=waivable_credits,
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
        **_preview_fields(job),
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
async def stems_download(
    job_id: str,
    stem: str = Query(...),
    format: str = Query("wav", pattern="^(wav|mp3)$"),
):
    path = _resolve_stems_file(job_id, stem)
    return await stem_download_response(path, stem, format)