"""
separation.py - Runs Demucs (Meta/Facebook Research's open-source music
source separation model) over an uploaded track, in one of two output
shapes:

  run_separation()      -> (vocals_path, instrumental_path)
  run_stem_separation() -> {"vocals": path, "drums": path, ...}

Powers /separate, /separate-hq, /stems, /stems-hq and their YouTube-
chained equivalents. The public contract is unchanged from the original
local-subprocess version and unchanged from the first GPU-migration pass
too: same return shapes, same SeparationError.

--------------------------------------------------------------------------
WHAT CHANGED (v2 of the GPU migration): NO MORE BASE64-IN-JSON

The first GPU pass sent the input file as base64 inside the RunPod job
payload and expected base64-encoded stems back the same way. That broke
on real audio: RunPod's Serverless /run response has a hard 10MB limit
(see gpu-worker/handler.py's own docstring for the full story and the
doc citation) - a real track's separated stems, base64-encoded, blow
past that easily, and the job would sit "stuck" with no visible error
because the worker finished fine but couldn't hand the result back
through RunPod's own payload channel.

The fix: audio bytes now travel DIRECTLY between this VPS and the GPU
worker over plain HTTP, completely bypassing RunPod's payload limit.
RunPod's job queue is used only for orchestration (submit, poll status,
a small confirmation dict).

  INPUT:  this file registers the input file's path (in-process, via
          gpu_internal_routes.register_gpu_input) and passes the WORKER
          a job_id, not the file itself. The worker then GETs the file
          from this VPS at /internal/gpu/input/{job_id} - see that
          module's own docstring for the receiving end.
  OUTPUT: the worker POSTs each finished stem straight to
          /internal/gpu/upload/{job_id}/{name} as it produces them,
          landing directly at the SAME final path this file has always
          used (SEPARATION_DIR/{job_id}_{name}.wav). By the time
          run_worker_job() returns success, the files are ALREADY on
          disk - this file's job on the output side is now just
          verifying they landed, not decoding/writing them itself.

This is a genuine simplification on the output side (no more
b64_to_file() calls here at all), and the ONE new piece of bookkeeping
is the register/unregister pair around the RunPod call, done in a
try/finally so a registered input path can never leak past the request
that created it.
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
from gpu_budget import record_gpu_seconds


class SeparationError(Exception):
    """Raised for any separation failure that should surface as a clean
    error to the caller (routes.py). Unchanged across both GPU-migration
    passes - every existing `except SeparationError` in routes.py keeps
    working exactly as it did before any of this started."""
    pass


def get_audio_duration_seconds(file_path: str) -> float:
    """
    Uses ffprobe to read a file's duration WITHOUT decoding the audio.
    Still runs LOCALLY on the VPS, deliberately - rejecting an oversized
    file here costs a fast local ffprobe call, versus registering it,
    submitting a job, and paying for a round trip only to have the GPU
    worker reject it after actually fetching the file.
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
    and returns the worker's (now small) output dict.
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
    # /internal/gpu/input/{job_id} - see gpu_internal_routes.py. The
    # try/finally below guarantees this is always cleaned up, whether
    # the job succeeds, fails, or raises unexpectedly, so a registration
    # can never outlive the request that created it.
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
            logger.error(f"[SEPARATION] Job {job_id} failed on the GPU worker: {e}")
            raise SeparationError(str(e))
    finally:
        unregister_gpu_input(job_id)

    gpu_seconds = output.get("gpu_seconds")
    if gpu_seconds is not None:
        # This is the number that actually matters for the spend
        # breaker, and it is NOT the same as this side's wall-clock
        # measurement. _run_tool_job's own timer also counts RunPod
        # queue wait, cold start, and the two file transfers - real
        # latency, but not GPU-seconds anyone is billed for. Feeding it
        # to gpu_budget would over-count spend and trip the HQ cutoff
        # early, disabling a working feature for a bill that was never
        # incurred. record_gpu_seconds() is called HERE, with the
        # worker's own measurement of just the Demucs run.
        try:
            record_gpu_seconds(gpu_seconds)
        except Exception as e:
            # Budget accounting must never fail a job that actually
            # succeeded - the user's stems are already on disk by now.
            logger.error(f"[SEPARATION] Job {job_id}: failed to record GPU seconds: {e}")
        logger.info(
            f"[SEPARATION] Job {job_id}: billed {gpu_seconds:.1f}s of GPU compute "
            f"(recorded against the monthly budget)."
        )
    else:
        # A worker that returns no timing is a real gap in cost
        # tracking, not a cosmetic one - flag it loudly rather than
        # silently under-counting spend.
        logger.warning(
            f"[SEPARATION] Job {job_id}: GPU worker returned no gpu_seconds - "
            f"this job's compute time is NOT counted against the monthly budget."
        )

    return output


def _verify_output_files(job_id: str, expected_paths: dict) -> None:
    """
    By the time run_worker_job() returns success, the worker has already
    POSTed every stem straight to its final on-disk path (see
    gpu_internal_routes.upload_gpu_result). This just confirms they're
    actually there before this function hands the paths back to its
    caller - the worker's own {"error": ...} contract should already
    catch an upload failure, but verifying on this side too means a
    caller of run_separation()/run_stem_separation() can trust the
    returned paths are real without a second round of network calls.
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
    instrumental_path) - identical shape to every prior version of this
    function.
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
    shape to every prior version of this function.
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