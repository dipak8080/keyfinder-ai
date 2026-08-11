"""
separation.py - Runs Demucs (Meta/Facebook Research's open-source music
source separation model) over an uploaded track, in one of two output
shapes:

  run_separation()      -> (vocals_path, instrumental_path)
  run_stem_separation() -> {"vocals": path, "drums": path, ...}

Powers /separate, /separate-hq, /stems, /stems-hq and their YouTube-
chained equivalents. The public contract has not changed once across
the entire GPU migration: same return shapes, same SeparationError.

--------------------------------------------------------------------------
HOW SEPARATION ACTUALLY RUNS NOW

Demucs no longer runs on this VPS. Jobs are submitted to a RunPod
Serverless GPU worker (gpu-worker/handler.py, a separate repo and
deploy target) via runpod_client.py.

Audio bytes never travel through RunPod's job payload - that has a 10MB
limit real audio blows straight past. Instead:

  INPUT:  this file registers the input file's path (in-process, via
          gpu_internal_routes.register_gpu_input) and sends the WORKER
          a job_id, not the file. The worker then GETs the file from
          this VPS at /internal/gpu/input/{job_id}.
  OUTPUT: the worker POSTs each finished stem straight to
          /internal/gpu/upload/{job_id}/{name}, landing at the SAME
          final path this file has always used. By the time
          run_worker_job() returns, the files are ALREADY on disk -
          this file's job on the output side is just verifying they
          landed.

--------------------------------------------------------------------------
REMOVED: THE SELF-TRACKED SPEND BREAKER (and why)

An earlier version fed every job's GPU seconds into gpu_budget.py, which
enforced a self-tracked monthly dollar ceiling. That has been removed
entirely, deliberately, after it turned out to be measuring something it
could never measure accurately:

  1. RunPod bills for the FULL time a worker is active - cold start,
     container init, model load, and both file transfers - not just the
     Demucs subprocess this side was timing. The counter structurally
     undercounted, which is the dangerous direction to be wrong in: it
     implied more remaining budget than actually existed.
  2. The counter reset on every container restart, and a restart happens
     on EVERY code deploy. For an actively developed app that turned a
     "monthly cap" into something closer to "a cap since the last
     deploy".
  3. It tracked SPENDING, never BALANCE - so topping up RunPod mid-month
     did nothing to it, and hitting the cap stayed hit even with money
     in the account. That mismatch confused far more than it protected.

The ceiling is now RunPod's own account balance: load what you're willing
to spend, and when it runs out, RunPod itself refuses the work. That is
100% accurate by definition, needs no code, and cannot drift. The one
thing worth building on this side is what happens WHEN it runs out -
see _is_insufficient_balance_error below, which turns RunPod's raw
billing rejection into a clean message a user can actually read.

For deliberately pausing spend BEFORE the money runs out, the existing
SEPARATION_HQ_ENABLED kill switch in config.py is the control - a manual
lever, doing one obvious thing, rather than an estimate pretending to be
a ledger.
--------------------------------------------------------------------------
"""
import os
from typing import Dict, Tuple

from config import (
    logger,
    ALLOWED_SEPARATION_MODELS,
    MODEL_STEM_NAMES,
    SEPARATION_MODEL,
    SEPARATION_OVERLAP,
    SEPARATION_DIR,
    DEMUCS_TIMEOUT_SECONDS,
    MAX_SEPARATION_DURATION_SECONDS,
    FFMPEG_PATH,
    RUNPOD_API_KEY,
    RUNPOD_DEMUCS_ENDPOINT_ID,
)
from utils import run_blocking
from runpod_client import run_worker_job, RunPodJobError
from gpu_internal_routes import register_gpu_input, unregister_gpu_input


# Wording RunPod and payment providers actually use for "this account is
# out of money". Broad and marker-based on purpose, same reasoning as
# youtube.py's PROXY_QUOTA_ERROR_MARKERS: a false positive here just
# means some other failure gets the SAME friendly message (harmless),
# while a false negative leaks a raw billing error straight to a user -
# the exact thing this exists to prevent. Broad-matching is the safe
# direction to be wrong in.
_INSUFFICIENT_BALANCE_MARKERS = (
    "insufficient balance",
    "insufficient funds",
    "insufficient credit",
    "out of credit",
    "no credit",
    "payment required",
    "account balance",
    "spending limit",
    "402",  # HTTP 402 Payment Required
)


def _is_insufficient_balance_error(error_text: str) -> bool:
    lowered = error_text.lower()
    return any(marker in lowered for marker in _INSUFFICIENT_BALANCE_MARKERS)


class SeparationError(Exception):
    """Raised for any separation failure that should surface as a clean
    error to the caller (routes.py). Unchanged across the whole GPU
    migration - every existing `except SeparationError` in routes.py
    keeps working exactly as it did before any of this started."""
    pass


def get_audio_duration_seconds(file_path: str) -> float:
    """
    Uses ffprobe to read a file's duration WITHOUT decoding the audio.
    Still runs LOCALLY on the VPS, deliberately - rejecting an oversized
    file here costs a fast local ffprobe call, versus registering it,
    submitting a job, and paying real GPU money for a round trip only to
    have the worker reject it after fetching the whole file.
    """
    ffprobe_path = FFMPEG_PATH.replace("ffmpeg", "ffprobe")
    try:
        import subprocess
        result = subprocess.run(
            [
                ffprobe_path, "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                file_path,
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
        return float(result.stdout.strip())
    except Exception as e:
        raise SeparationError(f"Could not read audio duration: {e}")


async def _run_demucs_on_gpu(
    input_path: str,
    job_id: str,
    task: str,
    model: str,
    overlap: float,
    timeout_seconds: int,
    max_duration_seconds: int,
) -> dict:
    """
    Shared engine for both public entry points below. Validates, makes
    the input fetchable by the worker, submits the job, waits for it,
    and returns the worker's (small) output dict.
    """
    if model not in ALLOWED_SEPARATION_MODELS:
        logger.error(f"[SEPARATION] Job {job_id} rejected - disallowed model '{model}'")
        raise SeparationError("Separation failed: unsupported model requested.")

    duration = await run_blocking(get_audio_duration_seconds, input_path)
    if duration > max_duration_seconds:
        raise SeparationError(
            f"Track is {int(duration // 60)} min long, which exceeds the "
            f"{max_duration_seconds // 60} min limit for separation."
        )

    if not RUNPOD_API_KEY or not RUNPOD_DEMUCS_ENDPOINT_ID:
        raise SeparationError(
            "Separation is temporarily unavailable (GPU worker not configured). "
            "Please try again shortly."
        )

    # Makes input_path fetchable by the worker at
    # /internal/gpu/input/{job_id}, and simultaneously AUTHORISES the
    # worker's upload for this job id (see gpu_internal_routes'
    # is_job_in_flight check). The try/finally guarantees the
    # registration can never outlive the request that created it.
    register_gpu_input(job_id, input_path)
    try:
        input_payload = {
            "task": task,
            "job_id": job_id,
            "filename": os.path.basename(input_path),
            "model": model,
            "overlap": overlap,
            "max_duration_seconds": max_duration_seconds,
        }

        logger.info(
            f"[SEPARATION] Job {job_id}: submitting to RunPod GPU worker "
            f"(model={model}, overlap={overlap}, task={task}, duration={duration:.1f}s)"
        )
        try:
            output = await run_worker_job(
                RUNPOD_DEMUCS_ENDPOINT_ID, RUNPOD_API_KEY, input_payload, timeout_seconds,
            )
        except RunPodJobError as e:
            error_text = str(e)
            if _is_insufficient_balance_error(error_text):
                # This is the ACTUAL ceiling on GPU spend - RunPod's own
                # account genuinely out of money, not a self-tracked
                # estimate. Caught specifically so the user sees a clean
                # message instead of a raw billing rejection, and logged
                # at CRITICAL because it needs an operator (a top-up),
                # not a retry.
                logger.critical(
                    f"[SEPARATION] Job {job_id} rejected - RunPod balance appears "
                    f"exhausted. Top up at runpod.io to restore separation. "
                    f"Raw error: {error_text}"
                )
                raise SeparationError(
                    "Separation is temporarily unavailable. Please try again later."
                )
            logger.error(f"[SEPARATION] Job {job_id} failed on the GPU worker: {error_text}")
            raise SeparationError(error_text)
    finally:
        unregister_gpu_input(job_id)

    gpu_seconds = output.get("gpu_seconds")
    if gpu_seconds is not None:
        # Logged for visibility only - NOT tracked against any in-app
        # spend counter. See this module's REMOVED section for why.
        # Still genuinely useful: grepping these lines is how you learn
        # what a real job actually costs in GPU time.
        logger.info(f"[SEPARATION] Job {job_id}: {gpu_seconds:.1f}s of GPU compute.")

    return output


def _verify_output_files(job_id: str, expected_paths: dict) -> None:
    """
    By the time run_worker_job() returns success, the worker has already
    POSTed every stem straight to its final on-disk path. This confirms
    they're actually there before handing the paths back - the worker's
    own error contract should already catch an upload failure, but
    verifying here too means a caller can trust the returned paths are
    real without a second round of network calls.
    """
    missing = [name for name, path in expected_paths.items() if not os.path.exists(path)]
    if missing:
        logger.error(
            f"[SEPARATION] Job {job_id}: worker reported success but these "
            f"files are missing on disk: {missing}"
        )
        raise SeparationError("Separation completed but output files were not found.")


async def run_separation(
    input_path: str,
    job_id: str,
    model: str = SEPARATION_MODEL,
    overlap: float = SEPARATION_OVERLAP,
    timeout_seconds: int = DEMUCS_TIMEOUT_SECONDS,
    max_duration_seconds: int = MAX_SEPARATION_DURATION_SECONDS,
) -> Tuple[str, str]:
    """
    Two-stem (vocal remover) mode. Returns (vocals_path,
    instrumental_path) - identical shape to every prior version.
    """
    final_vocals_path = os.path.join(SEPARATION_DIR, f"{job_id}_vocals.wav")
    final_instrumental_path = os.path.join(SEPARATION_DIR, f"{job_id}_instrumental.wav")

    await _run_demucs_on_gpu(
        input_path, job_id, "separate", model, overlap, timeout_seconds, max_duration_seconds,
    )

    _verify_output_files(job_id, {
        "vocals": final_vocals_path,
        "instrumental": final_instrumental_path,
    })

    logger.info(
        f"[SEPARATION] Job {job_id} complete (model={model}, GPU): "
        f"{final_vocals_path}, {final_instrumental_path}"
    )
    return final_vocals_path, final_instrumental_path


async def run_stem_separation(
    input_path: str,
    job_id: str,
    model: str = SEPARATION_MODEL,
    overlap: float = SEPARATION_OVERLAP,
    timeout_seconds: int = DEMUCS_TIMEOUT_SECONDS,
    max_duration_seconds: int = MAX_SEPARATION_DURATION_SECONDS,
) -> Dict[str, str]:
    """
    Full multi-stem mode. Returns a {stem_name: path} dict - identical
    shape to every prior version.
    """
    expected_stems = MODEL_STEM_NAMES.get(model)
    if not expected_stems:
        logger.error(f"[STEMS] Job {job_id} rejected - no stem list configured for model '{model}'")
        raise SeparationError("Separation failed: unsupported model requested.")

    final_paths = {
        stem: os.path.join(SEPARATION_DIR, f"{job_id}_{stem}.wav")
        for stem in expected_stems
    }

    await _run_demucs_on_gpu(
        input_path, job_id, "stems", model, overlap, timeout_seconds, max_duration_seconds,
    )

    _verify_output_files(job_id, final_paths)

    logger.info(
        f"[STEMS] Job {job_id} complete (model={model}, GPU, {len(final_paths)} stems): "
        f"{', '.join(final_paths.keys())}"
    )
    return final_paths