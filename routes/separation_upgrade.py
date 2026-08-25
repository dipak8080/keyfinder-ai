"""
routes/separation_upgrade.py - "Upgrade this to Studio Quality" for a
completed standard separation.

    POST /separate/upgrade/{job_id}        -> new HQ 2-stem job
    POST /stems/upgrade/{job_id}           -> new HQ 4-stem job
    GET  /separate/upgrade-info/{job_id}   -> should the CTA show?
    GET  /stems/upgrade-info/{job_id}      -> same, 4-stem

WHY THIS EXISTS RATHER THAN A FREE AUDIO PREVIEW
------------------------------------------------
The obvious way to sell HQ is a free sample: run htdemucs_ft on a 30
second excerpt. separation.py's own docstring rules it out:

    "RunPod bills for the FULL time a worker is active - cold start,
     container init, model load, and both file transfers - not just the
     Demucs subprocess this side was timing."

Cold start plus a four-model htdemucs_ft load is fixed overhead a 30
second clip pays in full. Measured on this deployment: a 4-minute
STANDARD separation cost $0.0036 of GPU time. A short HQ preview lands
nowhere near a proportional fraction of a full HQ job - closer to half -
given away to people who by definition have not paid.

The standard tier is already the preview, and it is already free. So the
sales moment is not in front of the upload form, it is on the results
page while someone is listening to vocal bleed on their own track. This
route is what that button calls. Marginal GPU cost of the demo: zero, it
already ran.

IDEMPOTENCY IS THE POINT OF THE job_upgrades TABLE
--------------------------------------------------
A double-click on a paid button is the worst bug available here, and
these routes create a NEW job per call - so nothing keyed on the new
job_id could ever catch it. The invariant is one HQ child per SOURCE
job, and migration 002 puts a primary key on exactly that.

Order matters and is not obvious: the claim row is inserted BEFORE the
credit is charged, and deleted if charging or enqueueing fails. Checking
first and inserting after would leave a window where two concurrent
clicks both see "no upgrade yet" and both charge. Letting the PRIMARY
KEY arbitrate is the only version that is safe under concurrency.

ERRORS ARE STRUCTURED, NOT PROSE
--------------------------------
Every failure carries a machine-readable `kind`. The frontend's ApiError
has an explicit comment - "Branch on this, never on message" - and
string-matching a user-facing sentence is exactly the coupling that
breaks the first time someone rewords the copy.
"""
import datetime
import os

from fastapi import APIRouter, Depends, HTTPException, Path, Response

from config import (
    logger,
    SEPARATION_MODEL_HQ,
    SEPARATION_OVERLAP_HQ,
    DEMUCS_TIMEOUT_SECONDS_HQ,
    MAX_SEPARATION_DURATION_SECONDS_HQ,
    SEPARATION_HQ_ENABLED,
    SEPARATION_HQ_RATE_LIMIT_MAX_REQUESTS,
    SEPARATION_HQ_RATE_LIMIT_WINDOW_SECONDS,
    STEMS_HQ_RATE_LIMIT_MAX_REQUESTS,
    STEMS_HQ_RATE_LIMIT_WINDOW_SECONDS,
    SEPARATION_JOB_TTL_SECONDS,
    MAX_QUEUED_SEPARATIONS,
)
from jobs import (
    create_job,
    get_job,
    mark_complete,
    mark_stems_complete,
    set_job_input,
    count_processing,
    SEPARATION_JOB_TYPES,
)
from separation import run_separation, run_stem_separation, get_audio_duration_seconds, SeparationError
from utils import _separation_semaphore, run_blocking
from log_stream import remember_job_tags, set_job_context, tag_from_job

from credits import metering, paywall
from credits.db import connect, now_iso, tx
from credits.identity import Identity
from credits.limits import tiered_rate_limit

from ._shared import spawn_background_task, _log_queued, _reject_if_separation_queue_full, _run_tool_job

router = APIRouter()

# Every value the frontend may receive in `reason`. Listed in one place
# so the enum can be read whole and kept in sync with per-case copy.
#
#   job_not_found         unknown id, or aged out of the job table
#   not_a_separation_job  id belongs to a different tool
#   job_failed            source job failed
#   source_not_complete   still processing
#   input_expired         source file swept by TTL
#   too_long_for_hq       over MAX_SEPARATION_DURATION_SECONDS_HQ
#   paywall_disabled      PAYWALL_ENABLED=false
#   tool_disabled         this route's own flag is off
#   hq_disabled           SEPARATION_HQ_ENABLED kill switch
#   already_upgraded      this source already has an HQ child
#
# hq_disabled and tool_disabled are deliberately SEPARATE: the first is
# "HQ is off for everyone right now", the second is "this route is not
# metered". Different copy, and one of them is temporary.

_BLOCKER_HTTP = {
    "job_not_found": (404, "job_not_found"),
    "not_a_separation_job": (404, "job_not_found"),
    "job_failed": (409, "job_failed"),
    "source_not_complete": (409, "source_not_complete"),
    "input_expired": (410, "input_expired"),
    "paywall_disabled": (409, "not_metered"),
    "tool_disabled": (409, "not_metered"),
    "hq_disabled": (503, "hq_disabled"),
}

_BLOCKER_MESSAGE = {
    "job_not_found": "That job wasn't found - it may have expired.",
    "not_a_separation_job": "That job wasn't found - it may have expired.",
    "job_failed": "That job failed, so there's nothing to upgrade.",
    "source_not_complete": "That job hasn't finished yet.",
    "input_expired": "The original file has expired. Please upload it again and choose Studio Quality.",
    "paywall_disabled": "Studio Quality upgrades aren't available right now.",
    "tool_disabled": "Studio Quality upgrades aren't available for this tool.",
    "hq_disabled": "Studio Quality is temporarily unavailable due to server load.",
}


def _expires_at(job: dict):
    """When the retained source input gets swept.

    Returned so the frontend can hide the CTA at the right moment
    without polling. The input lives exactly as long as the job does -
    cleanup_expired_jobs() removes the row and the file together - so
    the job's own TTL is the honest answer.
    """
    created = job.get("created_at")
    if not created:
        return None
    return datetime.datetime.utcfromtimestamp(
        created + SEPARATION_JOB_TTL_SECONDS
    ).strftime("%Y-%m-%dT%H:%M:%SZ")


def _existing_upgrade(source_job_id: str):
    with connect() as conn:
        row = conn.execute(
            "SELECT upgrade_job_id FROM job_upgrades WHERE source_job_id=?",
            (source_job_id,),
        ).fetchone()
    return row["upgrade_job_id"] if row else None


def _eligibility(job_id: str, source_type: str, rule_key: str):
    """Shared by the info route and the upgrade route.

    Returns (state, blocker). blocker is None when eligible; otherwise a
    dict with "reason" plus any extra fields. Both routes read the SAME
    function, so the CTA can never say "available" for something the POST
    would then reject - that mismatch is how a paid button produces an
    error, and it is worth one shared function to make structurally
    impossible.
    """
    from credits.config import get_settings

    settings = get_settings()
    rule = settings.rule_for(rule_key)

    job = get_job(job_id)
    if job is None:
        return ({}, {"reason": "job_not_found"})
    if job["job_type"] != source_type:
        return ({}, {"reason": "not_a_separation_job"})

    state = {"job": job, "settings": settings, "rule": rule}

    existing = _existing_upgrade(job_id)
    if existing:
        return (state, {"reason": "already_upgraded", "upgrade_job_id": existing})
    if job["status"] == "failed":
        return (state, {"reason": "job_failed"})
    if job["status"] != "complete":
        return (state, {"reason": "source_not_complete"})

    input_path = job.get("input_path")
    if not input_path or not os.path.exists(input_path):
        return (state, {"reason": "input_expired"})

    if not settings.paywall_enabled:
        return (state, {"reason": "paywall_disabled"})
    if rule is None or not rule.enabled:
        return (state, {"reason": "tool_disabled"})
    if not SEPARATION_HQ_ENABLED:
        return (state, {"reason": "hq_disabled"})

    return (state, None)


async def _upgrade_info(job_id: str, identity: Identity, *, source_type: str, rule_key: str) -> dict:
    """Never 404s. This is called to RENDER a page, and a 404 here would
    be indistinguishable from the status poll itself failing."""
    tag_from_job(job_id)
    state, blocker = _eligibility(job_id, source_type, rule_key)

    if blocker is not None:
        return {"eligible": False, "tool": rule_key, **blocker}

    job = state["job"]
    input_path = job["input_path"]

    try:
        duration = await run_blocking(get_audio_duration_seconds, input_path)
    except SeparationError:
        return {"eligible": False, "tool": rule_key, "reason": "input_expired"}

    if duration > MAX_SEPARATION_DURATION_SECONDS_HQ:
        return {
            "eligible": False, "tool": rule_key, "reason": "too_long_for_hq",
            "input_seconds": round(duration, 1),
            "max_seconds": MAX_SEPARATION_DURATION_SECONDS_HQ,
        }

    preview = paywall.preview(identity, rule_key, duration)
    return {
        "eligible": True,
        "reason": None,
        "tool": rule_key,
        "credits_needed": preview["credits_required"],
        "will_use": preview["will_use"],
        "balance": preview["balance"],
        "free_remaining": preview["free_remaining"],
        "can_run": preview["can_run"],
        "input_seconds": round(duration, 1),
        "input_expires_at": _expires_at(job),
        "already_upgraded": False,
        "upgrade_job_id": None,
    }


async def _queue_upgrade(
    job_id: str, identity: Identity, *, source_type: str, rule_key: str,
    tool: str, metric_label: str,
) -> dict:
    tag_from_job(job_id)
    state, blocker = _eligibility(job_id, source_type, rule_key)

    if blocker is not None:
        reason = blocker["reason"]

        # already_upgraded is a SUCCESS, not an error. This is the
        # double-click path: return the first call's child job so the
        # second click is indistinguishable from the first.
        if reason == "already_upgraded":
            existing = blocker["upgrade_job_id"]
            logger.info("[%s] upgrade of %s already exists as %s - returning it",
                        tool, job_id, existing)
            return {
                "job_id": existing, "status": "processing", "upgraded_from": job_id,
                "already_upgraded": True, "billing": None,
            }

        status, kind = _BLOCKER_HTTP.get(reason, (409, reason))
        raise HTTPException(status, {
            "kind": kind,
            "message": _BLOCKER_MESSAGE.get(reason, "This job can't be upgraded."),
        })

    job = state["job"]
    input_path = job["input_path"]
    original_filename = job.get("title") or os.path.basename(input_path)

    set_job_context(tool=tool.replace("_HQ", ""), tier="hq")

    # Capacity before payment: a 503 must never cost a credit.
    _reject_if_separation_queue_full()

    # Duration before payment. _run_demucs_on_gpu checks this too, but
    # inside the background task - long after the charge. paywall.guard
    # would not refund it either (the enqueue succeeded), so the credit
    # would only return via the 90-minute sweeper. Not an acceptable
    # answer to "I clicked upgrade and it charged me for an error".
    duration = await run_blocking(get_audio_duration_seconds, input_path)
    if duration > MAX_SEPARATION_DURATION_SECONDS_HQ:
        raise HTTPException(400, {
            "kind": "hq_duration_exceeded",
            "message": (
                f"This track is {int(duration // 60)} min long. Studio Quality is limited "
                f"to {MAX_SEPARATION_DURATION_SECONDS_HQ // 60} min because it costs several "
                f"times more to run. Standard separation still works at full length."
            ),
            "input_seconds": round(duration, 1),
            "max_seconds": MAX_SEPARATION_DURATION_SECONDS_HQ,
        })

    new_job_id = create_job(job_type=source_type)

    # Claim the source BEFORE charging. If two clicks race here, exactly
    # one wins the PRIMARY KEY and the loser returns the winner's job.
    # Insert-then-charge (rather than check-then-charge) is what makes
    # that true - see this module's docstring.
    try:
        with connect() as conn, tx(conn):
            conn.execute(
                "INSERT INTO job_upgrades (source_job_id, upgrade_job_id, tool, created_at)"
                " VALUES (?,?,?,?)",
                (job_id, new_job_id, rule_key, now_iso()),
            )
    except Exception:  # noqa: BLE001 - IntegrityError means we lost the race
        existing = _existing_upgrade(job_id)
        if existing:
            return {
                "job_id": existing, "status": "processing", "upgraded_from": job_id,
                "already_upgraded": True, "billing": None,
            }
        raise

    def _release_claim():
        """Charging or enqueueing failed, so the source is upgradeable
        again. Without this a 402 would permanently mark the job as
        upgraded, and the user could never retry after buying credits -
        turning a recoverable "you need credits" into a dead end."""
        try:
            with connect() as conn, tx(conn):
                conn.execute(
                    "DELETE FROM job_upgrades WHERE source_job_id=? AND upgrade_job_id=?",
                    (job_id, new_job_id),
                )
        except Exception:  # noqa: BLE001
            logger.exception("[%s] could not release upgrade claim for %s", tool, job_id)

    try:
        remember_job_tags(new_job_id)
        set_job_input(new_job_id, input_path)

        is_stems = source_type == "stems"
        if is_stems:
            work = lambda: run_stem_separation(
                input_path, new_job_id, SEPARATION_MODEL_HQ, SEPARATION_OVERLAP_HQ,
                DEMUCS_TIMEOUT_SECONDS_HQ, MAX_SEPARATION_DURATION_SECONDS_HQ,
            )
            on_success = lambda stems: mark_stems_complete(new_job_id, original_filename, stems)
            success_detail = lambda stems: f"{len(stems)} stems (upgrade)"
            generic_error = "Stem separation failed unexpectedly."
        else:
            work = lambda: run_separation(
                input_path, new_job_id, SEPARATION_MODEL_HQ, SEPARATION_OVERLAP_HQ,
                DEMUCS_TIMEOUT_SECONDS_HQ, MAX_SEPARATION_DURATION_SECONDS_HQ,
            )
            on_success = lambda paths: mark_complete(new_job_id, original_filename, paths[0], paths[1])
            success_detail = None
            generic_error = "Separation failed unexpectedly."

        async with paywall.guard(
            identity, job_id=new_job_id, tool=rule_key, input_seconds=duration
        ) as charge:
            spawn_background_task(_run_tool_job(
                tool=tool, metric=metric_label, job_id=new_job_id,
                semaphore=_separation_semaphore, work=work, on_success=on_success,
                generic_error=generic_error,
                # Empty: the input belongs to the SOURCE job and is
                # reclaimed by that job's TTL sweep, not by this task.
                cleanup_paths=[],
                success_detail=success_detail, gpu_billed=False,
            ))
    except Exception:
        _release_claim()
        raise

    metering.record_job_created(
        job_id=new_job_id, tool=rule_key,
        subject_id=identity.subject_id, account_id=identity.account_id,
        ip_hash=identity.ip_hash, input_seconds=duration,
        charge_type=charge.charge_type,
    )

    depth = count_processing(SEPARATION_JOB_TYPES)
    _log_queued(tool, new_job_id, original_filename, 0,
                f"upgrade_of={job_id} model={SEPARATION_MODEL_HQ} "
                f"charged={charge.charge_type} queue={depth}/{MAX_QUEUED_SEPARATIONS}")

    return {
        "job_id": new_job_id,
        "status": "processing",
        "upgraded_from": job_id,
        "already_upgraded": False,
        "billing": {
            "charged": charge.charge_type,
            "balance": charge.balance_after,
            "free_remaining": charge.free_remaining_after,
        },
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post(
    "/separate/upgrade/{job_id}",
    dependencies=[Depends(tiered_rate_limit(
        "separate-hq",
        free_max=SEPARATION_HQ_RATE_LIMIT_MAX_REQUESTS,
        free_window=SEPARATION_HQ_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def upgrade_separation(
    response: Response,
    job_id: str = Path(...),
    identity: Identity = Depends(paywall.get_identity),
) -> dict:
    """Re-runs a completed /separate job through htdemucs_ft. Costs one
    credit. Returns a NEW job_id - poll at /separate/status/{id}, fetch
    stems from the standard /separate/preview and /separate/download
    routes. Idempotent per source job."""
    return await _queue_upgrade(
        job_id, identity, source_type="separation", rule_key="separate-hq",
        tool="SEPARATION_HQ", metric_label="/separate/upgrade",
    )


@router.post(
    "/stems/upgrade/{job_id}",
    dependencies=[Depends(tiered_rate_limit(
        "stems-hq",
        free_max=STEMS_HQ_RATE_LIMIT_MAX_REQUESTS,
        free_window=STEMS_HQ_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def upgrade_stems(
    response: Response,
    job_id: str = Path(...),
    identity: Identity = Depends(paywall.get_identity),
) -> dict:
    """Re-runs a completed /stems job through htdemucs_ft. Costs one
    credit. Poll the new id at /stems/status/{id}. Idempotent per source."""
    return await _queue_upgrade(
        job_id, identity, source_type="stems", rule_key="stems-hq",
        tool="STEMS_HQ", metric_label="/stems/upgrade",
    )


@router.get("/separate/upgrade-info/{job_id}")
async def separate_upgrade_info(
    response: Response,
    job_id: str = Path(...),
    identity: Identity = Depends(paywall.get_identity),
) -> dict:
    """Should the Studio Quality CTA show on this result, and what will
    it cost? Never 404s - returns eligible=false with a reason."""
    response.headers["Cache-Control"] = "no-store"
    return await _upgrade_info(job_id, identity, source_type="separation", rule_key="separate-hq")


@router.get("/stems/upgrade-info/{job_id}")
async def stems_upgrade_info(
    response: Response,
    job_id: str = Path(...),
    identity: Identity = Depends(paywall.get_identity),
) -> dict:
    """Same as /separate/upgrade-info, for 4-stem jobs."""
    response.headers["Cache-Control"] = "no-store"
    return await _upgrade_info(job_id, identity, source_type="stems", rule_key="stems-hq")