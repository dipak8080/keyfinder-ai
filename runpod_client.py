"""
runpod_client.py - Talks to a RunPod Serverless worker's HTTP job-queue
API (submit a job, poll until it finishes, get the result back).

WHERE THIS FITS: gpu-worker/handler.py (a separate repo/deploy target -
see that file's own docstring) is the RunPod side. This file is the VPS
side - separation.py imports run_worker_job() from here instead of
shelling out to a local Demucs subprocess, once that swap is made (not
done in this file - see separation.py for that change).

DELIBERATELY GENERIC, NOT DEMUCS-SPECIFIC: nothing in this file knows
what a "stem" or a "vocal" is. It only knows "submit this JSON payload to
this endpoint, poll until it's done, hand back whatever the worker
returned." That's what makes it reusable: a FUTURE GPU-backed tool (say,
Whisper) gets its own handler.py, its own Docker image, its own RunPod
endpoint, and its own small module (e.g. runpod_whisper_client-shaped
code) that calls the exact same submit_job()/poll_job()/run_worker_job()
functions here against a DIFFERENT endpoint_id and a differently-shaped
payload - no changes to this file required. One proven pattern, cheap to
repeat, rather than one growing bespoke integration.

TRANSPORT CHOICE: plain `requests` (already in requirements.txt),
dispatched through utils.run_blocking() for each individual HTTP call -
NOT an async HTTP client like httpx/aiohttp. This matches every other
blocking call already in this codebase (yt-dlp, ffmpeg, Demucs itself)
rather than introducing a second HTTP library for one module. The
polling LOOP itself is still non-blocking end to end: the outer
functions are `async def`, and the wait between polls uses
`asyncio.sleep()` (which yields the event loop), while only the actual
network request inside each iteration is offloaded to the thread pool.

WHY /run (async submit) INSTEAD OF /runsync: /runsync holds the HTTP
connection open for the entire job duration, which is fine for a
seconds-long job but fragile for something that can legitimately run up
to DEMUCS_TIMEOUT_SECONDS_HQ (30 min, see config.py) - proxies and load
balancers along the request path often have their own idle-connection
limits well under that. /run returns a job id immediately and this file
polls /status/{job_id} on its own schedule instead, mirroring the exact
IN_QUEUE -> IN_PROGRESS -> COMPLETED lifecycle already visible in
RunPod's own dashboard Requests tab - same states, just read
programmatically here instead of watched by eye.

ERROR CONTRACT: every failure path raises RunPodJobError - a network
failure, a non-200 response, a FAILED/CANCELLED/TIMED_OUT job status, a
poll that exceeds timeout_seconds, OR a nominally-COMPLETED job whose
output dict contains an "error" key (see handler.py's own docstring for
why the worker always returns a clean {"error": ...} rather than
crashing). RunPodJobError is deliberately NOT SeparationError - keeping
this file's own exception generic (no mention of separation/Demucs) is
what lets it stay reusable. separation.py is responsible for catching
RunPodJobError and re-raising it as SeparationError, so every existing
caller in routes.py keeps seeing exactly the exception type it already
handles today.

REQUIRES two new config.py values, not yet added there (see this file's
accompanying notes for the exact lines to add):
  RUNPOD_API_KEY              - from RunPod's endpoint page -> API key
  RUNPOD_DEMUCS_ENDPOINT_ID   - this worker's endpoint id (RunPod calls
                                 it "Endpoint ID" on the endpoint's
                                 Overview tab)
Named with the _DEMUCS_ prefix deliberately, not a generic
RUNPOD_ENDPOINT_ID - a future GPU tool needs its OWN endpoint id
constant (e.g. RUNPOD_WHISPER_ENDPOINT_ID) since each GPU-backed tool
gets its own independent RunPod endpoint (own GPU tier, own scaling, own
cost tracking - see gpu-worker/handler.py's docstring for why one worker
per capability, not one worker trying to do everything).
"""
import time
import base64
import asyncio
from typing import Optional

import requests

from utils import run_blocking


class RunPodJobError(Exception):
    """
    Raised for any failure submitting to, polling, or receiving an error
    from a RunPod Serverless job. Deliberately NOT tool-specific (no
    mention of separation, Demucs, or any other capability) so any
    future GPU-backed tool's own module can import and re-raise this the
    same way separation.py does, without this file needing to know
    anything about what a given job was actually FOR.
    """
    pass


_RUNPOD_API_BASE = "https://api.runpod.ai/v2"

# Polling cadence for job status. 2s is frequent enough that a fast
# standard-tier job (well under a minute on GPU) doesn't sit needlessly
# past its actual completion, without hammering RunPod's API pointlessly
# on a job that's going to take several minutes regardless.
_POLL_INTERVAL_SECONDS = 2.0

# Timeout for the submit/status HTTP calls THEMSELVES (not the job's own
# execution time, which is governed by the timeout_seconds argument
# passed into poll_job/run_worker_job below). 30s is generous for what
# should be a fast API round trip; if RunPod's own API is that slow to
# even ACK a request, something is wrong upstream of this job entirely.
_HTTP_CALL_TIMEOUT_SECONDS = 30


def _post(url: str, headers: dict, json_body: dict, timeout: int):
    """Thin sync wrapper - see module docstring's TRANSPORT CHOICE for
    why this is plain `requests` dispatched via run_blocking rather than
    an async HTTP client."""
    return requests.post(url, headers=headers, json=json_body, timeout=timeout)


def _get(url: str, headers: dict, timeout: int):
    return requests.get(url, headers=headers, timeout=timeout)


async def submit_job(endpoint_id: str, api_key: str, input_payload: dict) -> str:
    """
    POSTs to RunPod's /run (async submit) and returns the RunPod-assigned
    job id immediately - the job itself keeps running after this
    function returns; poll_job() below is what waits for it to finish.
    """
    url = f"{_RUNPOD_API_BASE}/{endpoint_id}/run"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    try:
        res = await run_blocking(_post, url, headers, {"input": input_payload}, _HTTP_CALL_TIMEOUT_SECONDS)
    except Exception as e:
        raise RunPodJobError(f"Failed to reach RunPod to submit job: {e}")

    if res.status_code != 200:
        raise RunPodJobError(f"RunPod submit failed ({res.status_code}): {res.text[:500]}")

    data = res.json()
    job_id = data.get("id")
    if not job_id:
        raise RunPodJobError(f"RunPod submit response had no job id: {data}")
    return job_id


async def poll_job(endpoint_id: str, api_key: str, job_id: str, timeout_seconds: int) -> dict:
    """
    Polls RunPod's /status/{job_id} until the job reaches a terminal
    state, or timeout_seconds elapses.

    Returns the job's "output" dict on a clean COMPLETED - exactly what
    the worker's handler() function returned (see gpu-worker/handler.py's
    own OUTPUT section for its shape, which differs by task).

    Raises RunPodJobError for:
      - FAILED / CANCELLED / TIMED_OUT status
      - this function's own timeout_seconds being exceeded
      - a worker-reported {"error": ...} inside an otherwise-COMPLETED
        job (see handler.py's ERRORS section for why the worker reports
        failures this way instead of crashing - this is where that
        choice gets unwrapped back into a normal raised exception on the
        VPS side)
    """
    url = f"{_RUNPOD_API_BASE}/{endpoint_id}/status/{job_id}"
    headers = {"Authorization": f"Bearer {api_key}"}

    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            res = await run_blocking(_get, url, headers, _HTTP_CALL_TIMEOUT_SECONDS)
        except Exception as e:
            raise RunPodJobError(f"Failed to reach RunPod while polling job {job_id}: {e}")

        if res.status_code != 200:
            raise RunPodJobError(f"RunPod status check failed ({res.status_code}): {res.text[:500]}")

        data = res.json()
        status = data.get("status")

        if status == "COMPLETED":
            output = data.get("output")
            if isinstance(output, dict) and output.get("error"):
                raise RunPodJobError(str(output["error"]))
            if not isinstance(output, dict):
                raise RunPodJobError(
                    f"RunPod job {job_id} completed with an unexpected output shape: {output!r}"
                )
            return output

        if status in ("FAILED", "CANCELLED", "TIMED_OUT"):
            raise RunPodJobError(
                f"RunPod job {job_id} ended with status={status}: "
                f"{data.get('error') or 'no error detail returned'}"
            )

        if time.monotonic() >= deadline:
            raise RunPodJobError(
                f"RunPod job {job_id} did not finish within {timeout_seconds}s "
                f"(last known status: {status})"
            )

        await asyncio.sleep(_POLL_INTERVAL_SECONDS)


async def run_worker_job(endpoint_id: str, api_key: str, input_payload: dict, timeout_seconds: int) -> dict:
    """
    Convenience wrapper: submit_job() then poll_job() in sequence. This
    is the one function most callers actually need - separation.py calls
    this directly, and any future GPU-backed tool's own module would
    call it too, against a different endpoint_id and a differently-
    shaped input_payload/output, without touching this file at all.
    """
    job_id = await submit_job(endpoint_id, api_key, input_payload)
    return await poll_job(endpoint_id, api_key, job_id, timeout_seconds)


# ---------- File <-> base64 helpers ----------
# Shared by any caller sending/receiving files through a RunPod job
# payload as base64 JSON - matches handler.py's audio_b64/*_b64 field
# convention exactly, so a caller never has to think about the encoding
# itself, only about which field names its particular worker expects.

def file_to_b64(path: str) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("ascii")


def b64_to_file(b64_data: str, dest_path: str) -> None:
    with open(dest_path, "wb") as f:
        f.write(base64.b64decode(b64_data))