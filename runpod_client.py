"""
runpod_client.py - Talks to a RunPod Serverless worker's HTTP job-queue
API (submit a job, poll until it finishes, get the result back).

WHERE THIS FITS: gpu-worker/handler.py (a separate repo/deploy target) is
the RunPod side. This file is the VPS side - separation.py calls
run_worker_job() from here instead of shelling out to a local Demucs
subprocess.

DELIBERATELY GENERIC, NOT DEMUCS-SPECIFIC: nothing here knows what a
"stem" is. A future GPU-backed tool gets its own handler.py, its own
image, its own RunPod endpoint, and calls these same functions with a
different endpoint_id and payload - no changes to this file.

--------------------------------------------------------------------------
PRODUCTION HARDENING (2026-08-11) - three real money/reliability fixes

1. CANCELLATION ON GIVE-UP (the big one - this was a live money leak).
   Previously, when poll_job() hit its own timeout it just raised and
   walked away. The RunPod job DID NOT STOP - it kept running on a
   billed GPU until it finished on its own, with nobody waiting for the
   result. A single stuck HQ job could quietly bill 30 minutes of GPU
   time for output nobody would ever read. Every give-up path now calls
   cancel_job() first. Cancellation is best-effort and never masks the
   original error: if the cancel itself fails, that's logged and the
   real failure still propagates.

2. PER-JOB EXECUTION TIMEOUT SENT TO RUNPOD.
   RunPod applies the ENDPOINT's execution timeout (set at 1800s to
   accommodate HQ) to every job regardless of tier. So a standard-tier
   job - which this VPS gives up on after DEMUCS_TIMEOUT_SECONDS (600s)
   - could keep billing for another 20 minutes on RunPod's side. The
   job payload now carries its own executionTimeout, so RunPod enforces
   the SAME deadline this side is enforcing. Belt and braces with the
   cancel above: cancellation handles "we gave up", this handles "we
   died and never got to cancel".

3. SUBMIT RETRY ON TRANSIENT NETWORK FAILURE.
   A single dropped packet during submit used to fail the whole job
   before any work started. Submits are retried with backoff, since a
   failed submit is guaranteed to have cost nothing yet - unlike a
   failed poll, where retrying blindly could mean losing track of a job
   that IS running and IS billing. Only the submit is retried, and only
   on network-level errors, never on a 4xx (which would just fail
   identically).
--------------------------------------------------------------------------
"""
import time
import base64
import asyncio
import logging
from typing import Optional

import requests

from utils import run_blocking

logger = logging.getLogger(__name__)


class RunPodJobError(Exception):
    """
    Raised for any failure submitting to, polling, or receiving an error
    from a RunPod Serverless job. Deliberately NOT tool-specific so any
    future GPU-backed tool's module can import and re-raise this the
    same way separation.py does.
    """
    pass


_RUNPOD_API_BASE = "https://api.runpod.ai/v2"

_POLL_INTERVAL_SECONDS = 2.0

# Timeout for the submit/status/cancel HTTP calls THEMSELVES - not the
# job's own execution time, which is governed by timeout_seconds below.
_HTTP_CALL_TIMEOUT_SECONDS = 30

# Submit-only retry policy. See hardening note 3 above for why polls are
# deliberately NOT retried this way.
_SUBMIT_MAX_ATTEMPTS = 3
_SUBMIT_BACKOFF_SECONDS = 1.5


def _post(url: str, headers: dict, json_body: Optional[dict], timeout: int):
    return requests.post(url, headers=headers, json=json_body, timeout=timeout)


def _get(url: str, headers: dict, timeout: int):
    return requests.get(url, headers=headers, timeout=timeout)


def _auth_headers(api_key: str) -> dict:
    return {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}


async def cancel_job(endpoint_id: str, api_key: str, job_id: str) -> bool:
    """
    Tells RunPod to stop a job we are no longer waiting for.

    THIS IS A COST CONTROL, NOT A CORRECTNESS FEATURE - the job's result
    is already unwanted by the time this is called. Its entire purpose
    is to stop the meter on a GPU that would otherwise keep running work
    nobody will read.

    Best-effort by design: returns True/False rather than raising, so a
    caller can always call it in a failure path without the cleanup
    itself becoming a second, more confusing failure that masks the real
    one.
    """
    url = f"{_RUNPOD_API_BASE}/{endpoint_id}/cancel/{job_id}"
    try:
        res = await run_blocking(_post, url, _auth_headers(api_key), None, _HTTP_CALL_TIMEOUT_SECONDS)
        if res.status_code == 200:
            logger.info(f"[RUNPOD] Cancelled job {job_id} (stopped billing for abandoned work).")
            return True
        logger.warning(
            f"[RUNPOD] Cancel request for job {job_id} returned {res.status_code}: "
            f"{res.text[:200]} - the job may keep running and billing."
        )
        return False
    except Exception as e:
        # Deliberately swallowed: this runs inside an error path already.
        # Raising here would replace a real, diagnosable failure with a
        # cleanup failure.
        logger.warning(f"[RUNPOD] Cancel request for job {job_id} failed: {e}")
        return False


async def submit_job(
    endpoint_id: str,
    api_key: str,
    input_payload: dict,
    execution_timeout_seconds: Optional[int] = None,
) -> str:
    """
    POSTs to RunPod's /run (async submit) and returns the RunPod-assigned
    job id immediately.

    execution_timeout_seconds is forwarded to RunPod as the job's own
    executionTimeout policy - see hardening note 2 in this module's
    docstring. Without it, RunPod falls back to the ENDPOINT-wide
    timeout, which is necessarily sized for the slowest tier (HQ) and
    therefore lets a stuck standard job bill for far longer than this
    side is willing to wait for it.
    """
    url = f"{_RUNPOD_API_BASE}/{endpoint_id}/run"
    body: dict = {"input": input_payload}
    if execution_timeout_seconds:
        # RunPod expects milliseconds here.
        body["policy"] = {"executionTimeout": int(execution_timeout_seconds * 1000)}

    last_error = None
    for attempt in range(1, _SUBMIT_MAX_ATTEMPTS + 1):
        try:
            res = await run_blocking(_post, url, _auth_headers(api_key), body, _HTTP_CALL_TIMEOUT_SECONDS)
        except Exception as e:
            # Network-level failure: nothing was accepted, nothing is
            # billing, so retrying is free and safe.
            last_error = e
            if attempt < _SUBMIT_MAX_ATTEMPTS:
                backoff = _SUBMIT_BACKOFF_SECONDS * attempt
                logger.warning(
                    f"[RUNPOD] Submit attempt {attempt}/{_SUBMIT_MAX_ATTEMPTS} failed "
                    f"({e}) - retrying in {backoff:.1f}s"
                )
                await asyncio.sleep(backoff)
                continue
            raise RunPodJobError(f"Failed to reach RunPod to submit job after {attempt} attempts: {e}")

        if res.status_code != 200:
            # A non-200 is RunPod rejecting the request itself (bad key,
            # bad endpoint, malformed body). Retrying reproduces it
            # identically, so fail immediately rather than burning the
            # backoff on a guaranteed repeat.
            raise RunPodJobError(f"RunPod submit failed ({res.status_code}): {res.text[:500]}")

        data = res.json()
        job_id = data.get("id")
        if not job_id:
            raise RunPodJobError(f"RunPod submit response had no job id: {data}")
        return job_id

    raise RunPodJobError(f"Failed to submit job to RunPod: {last_error}")


async def poll_job(
    endpoint_id: str,
    api_key: str,
    job_id: str,
    timeout_seconds: int,
) -> dict:
    """
    Polls /status/{job_id} until the job reaches a terminal state or
    timeout_seconds elapses.

    Returns the job's "output" dict on a clean COMPLETED.

    EVERY give-up path cancels the job first - see hardening note 1.
    Without that, this function's own timeout was a pure money leak:
    we stop waiting, the GPU keeps working, RunPod keeps billing.
    """
    url = f"{_RUNPOD_API_BASE}/{endpoint_id}/status/{job_id}"
    headers = {"Authorization": f"Bearer {api_key}"}

    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            res = await run_blocking(_get, url, headers, _HTTP_CALL_TIMEOUT_SECONDS)
        except Exception as e:
            # A poll failure is NOT proof the job stopped - it may well
            # still be running and billing, so cancel before giving up.
            await cancel_job(endpoint_id, api_key, job_id)
            raise RunPodJobError(f"Failed to reach RunPod while polling job {job_id}: {e}")

        if res.status_code != 200:
            await cancel_job(endpoint_id, api_key, job_id)
            raise RunPodJobError(f"RunPod status check failed ({res.status_code}): {res.text[:500]}")

        data = res.json()
        status = data.get("status")

        if status == "COMPLETED":
            output = data.get("output")
            if isinstance(output, dict) and output.get("error"):
                # A worker-reported error - the job is already finished,
                # so there is nothing to cancel.
                raise RunPodJobError(str(output["error"]))
            if not isinstance(output, dict):
                raise RunPodJobError(
                    f"RunPod job {job_id} completed with an unexpected output shape: {output!r}"
                )
            return output

        if status in ("FAILED", "CANCELLED", "TIMED_OUT"):
            # Already terminal - no cancel needed, nothing is billing.
            raise RunPodJobError(
                f"RunPod job {job_id} ended with status={status}: "
                f"{data.get('error') or 'no error detail returned'}"
            )

        if time.monotonic() >= deadline:
            # THE case this hardening exists for: we are out of patience
            # but the job is, as far as we know, still running on a
            # billed GPU.
            await cancel_job(endpoint_id, api_key, job_id)
            raise RunPodJobError(
                f"RunPod job {job_id} did not finish within {timeout_seconds}s "
                f"(last known status: {status}) - job cancelled to stop billing."
            )

        await asyncio.sleep(_POLL_INTERVAL_SECONDS)


async def run_worker_job(
    endpoint_id: str,
    api_key: str,
    input_payload: dict,
    timeout_seconds: int,
) -> dict:
    """
    submit_job() then poll_job(). The one function most callers need.

    timeout_seconds is used for BOTH sides of the deadline: this side
    stops polling after it, and RunPod is told to stop executing after
    it. Passing one number to both is deliberate - two independently
    configured timeouts is exactly how you end up with a job that this
    side has abandoned but RunPod is still happily billing for.

    asyncio.CancelledError gets its own handler: when the VPS shuts down
    or the task is cancelled mid-job, the RunPod job would otherwise be
    orphaned - still running, still billing, with the process that
    started it gone. Cancelling on the way out closes that hole too.
    """
    job_id = await submit_job(
        endpoint_id, api_key, input_payload,
        execution_timeout_seconds=timeout_seconds,
    )
    try:
        return await poll_job(endpoint_id, api_key, job_id, timeout_seconds)
    except asyncio.CancelledError:
        # Shutdown/redeploy while a GPU job is in flight. Without this,
        # the job keeps billing with nobody left to collect the result.
        logger.warning(
            f"[RUNPOD] Local task cancelled while job {job_id} was running - "
            f"cancelling it remotely so it stops billing."
        )
        await cancel_job(endpoint_id, api_key, job_id)
        raise


# ---------- File <-> base64 helpers ----------
# Retained for any caller that genuinely needs small inline payloads.
# NOT used by the Demucs path any more - audio moves over direct HTTP
# instead, because RunPod's job payload caps at 10MB and real audio blows
# straight past that. See gpu-worker/handler.py's docstring for the full
# story.

def file_to_b64(path: str) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("ascii")


def b64_to_file(b64_data: str, dest_path: str) -> None:
    with open(dest_path, "wb") as f:
        f.write(base64.b64decode(b64_data))