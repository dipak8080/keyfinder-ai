"""
separation.py - Runs Demucs (Meta/Facebook Research's open-source music
source separation model) over an uploaded track, in one of two output
shapes:

  run_separation()      -> (vocals_path, instrumental_path)
                           Uses task="separate". Powers /separate and
                           /separate-hq (the vocal remover).

  run_stem_separation() -> {"vocals": path, "drums": path, "bass": path,
                            "other": path}
                           Uses task="stems". Powers /stems and
                           /stems-hq (the full stem splitter).

WHAT CHANGED (2026-08-10): GPU MIGRATION
Both functions used to shell out to a local Demucs subprocess on this
VPS. They now submit the job to a RunPod Serverless GPU worker (see
gpu-worker/handler.py, a separate repo/deploy target) via
runpod_client.py and wait for the result.

THE PUBLIC CONTRACT IS UNCHANGED ON PURPOSE - this is the entire point
of the migration being "safe": run_separation() still returns
(vocals_path, instrumental_path), run_stem_separation() still returns a
{stem_name: path} dict, and SeparationError is still the exception every
caller in routes.py already knows to catch. Every existing caller
(_run_tool_job, mark_complete, mark_stems_complete, the `except
SeparationError` branch) needed ZERO changes because of this file - the
only thing that changed is what happens on the inside, between "input
file on disk" and "output file on disk."

ONE REAL SHAPE CHANGE, AND IT'S IN routes.py, NOT HERE: these two
functions are now `async def` instead of plain `def`, because they await
an HTTP call instead of blocking on a local subprocess. routes.py's two
call sites currently wrap them in `run_blocking(run_separation, ...)` -
that wrapping needs to change to a plain `await run_separation(...)`
once this file is deployed (run_blocking is for offloading BLOCKING
calls off the event loop; awaiting a network call needs no such
offloading, since it doesn't hold a CPU core hostage while it waits).
Calling run_blocking() around an async function today would be a real
bug, so that follow-up in routes.py is not optional - it's the very next
step, not a someday cleanup.

STORAGE: finished stems are still written to local disk (SEPARATION_DIR)
under the exact same filenames as before - jobs.py's TTL cleanup and
every preview/download route need no changes at all, since neither of
them ever cared how the file got there.

WHY GPU-SIDE ERRORS BECOME SeparationError HERE, NOT IN runpod_client.py:
runpod_client.py raises its own RunPodJobError, deliberately generic (no
mention of separation/Demucs) so it stays reusable for any future
GPU-backed tool. This file is what translates that generic error back
into the SeparationError every existing caller already expects - keeping
that translation here, in the one place that actually knows this is
"separation," is what let every downstream caller stay untouched.
"""
import os
from typing import Dict, Optional, Tuple

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
from runpod_client import run_worker_job, file_to_b64, b64_to_file, RunPodJobError


class SeparationError(Exception):
    """Raised for any separation failure that should surface as a clean
    error to the caller (routes.py) - covers RunPod submit/poll failures,
    a GPU-side Demucs failure reported back from the worker, disallowed
    models, and duration-limit rejections alike. UNCHANGED from before
    the GPU migration - every existing `except SeparationError` in
    routes.py keeps working exactly as it did."""
    pass


def get_audio_duration_seconds(file_path: str) -> float:
    """
    Uses ffprobe (bundled with ffmpeg) to read a file's duration WITHOUT
    decoding the audio. UNCHANGED, and still runs LOCALLY on the VPS,
    deliberately: rejecting an oversized file here costs nothing but a
    fast local ffprobe call, versus base64-encoding it, uploading it to
    RunPod, and paying for the round trip only to have the GPU worker
    reject it - same "fail fast, fail cheap" reasoning as before, just
    now protecting against a real dollar cost instead of only CPU time.
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
    Shared engine for both public entry points below - the async
    replacement for the old local-subprocess _run_demucs(). Validates,
    submits the job to the RunPod GPU worker, waits for it, and returns
    the worker's raw output dict (still base64-encoded audio at this
    point - the CALLER decodes it, since only the caller knows whether
    it's expecting a vocals/instrumental pair or a stems dict).

    Order of operations mirrors the original local-subprocess version
    exactly: validate the model FIRST (free), check duration SECOND
    (cheap, local, before any bytes leave the VPS), only THEN do the
    expensive part (here: base64-encode the whole file and submit it).
    """
    # Same defense-in-depth check as before the migration - config.py
    # already validates env-supplied model names, but this function is
    # reachable from route code, and an unvalidated value would be
    # forwarded straight into the GPU worker's own request payload.
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
        # Fails loudly and immediately rather than letting a misconfigured
        # deploy silently hang waiting on a request that was never going
        # to reach anywhere - a missing credential should look like a
        # clear configuration error, not a mysterious timeout ten minutes
        # later.
        raise SeparationError(
            "Separation is temporarily unavailable (GPU worker not configured). "
            "Please try again shortly."
        )

    logger.info(
        f"[SEPARATION] Job {job_id}: encoding for GPU upload "
        f"(model={model}, overlap={overlap}, task={task}, duration={duration:.1f}s)"
    )
    audio_b64 = await run_blocking(file_to_b64, input_path)

    input_payload = {
        "task": task,
        "audio_b64": audio_b64,
        "filename": os.path.basename(input_path),
        "model": model,
        "overlap": overlap,
        "max_duration_seconds": max_duration_seconds,
    }
    # audio_b64 is no longer needed in this process's memory after the
    # payload dict holds it - not explicitly deleted here since Python's
    # own reference counting handles it once input_payload and the local
    # `audio_b64` binding both go out of scope at function return; called
    # out in a comment rather than silently relied upon, since holding a
    # base64 copy of a large file in memory is exactly the kind of thing
    # worth being deliberate about on a memory-constrained VPS.

    logger.info(f"[SEPARATION] Job {job_id}: submitting to RunPod GPU worker")
    try:
        output = await run_worker_job(
            RUNPOD_DEMUCS_ENDPOINT_ID, RUNPOD_API_KEY, input_payload, timeout_seconds,
        )
    except RunPodJobError as e:
        # Translated here, and only here - see this file's own docstring
        # for why runpod_client.py's exception stays generic and this is
        # the one place that turns it back into what every existing
        # caller already expects.
        logger.error(f"[SEPARATION] Job {job_id} failed on the GPU worker: {e}")
        raise SeparationError(str(e))

    gpu_seconds = output.get("gpu_seconds")
    if gpu_seconds is not None:
        # Logged for visibility now; NOT yet wired into
        # gpu_budget.record_gpu_seconds() - that still runs off
        # _run_tool_job's own local wall-clock timer in routes.py today,
        # which is close but not exact (it also includes the base64
        # encode/upload/poll overhead this function adds). Threading the
        # real, worker-reported number through into the budget tracker
        # is a deliberate follow-up for the routes.py step, not silently
        # skipped - flagging it here in the logs is what makes the gap
        # visible in the meantime rather than invisible.
        logger.info(
            f"[SEPARATION] Job {job_id}: GPU worker reports {gpu_seconds:.1f}s of "
            f"actual compute time (this process's own timer will include some "
            f"additional overhead on top of this figure)."
        )

    return output


def _missing_output_error(job_id: str, what: str):
    """Same intent as the pre-migration version: a clear, specific error
    instead of a confusing downstream KeyError, when the GPU worker
    returned a COMPLETED status but didn't actually include the fields
    this function expected."""
    logger.error(f"[SEPARATION] Job {job_id}: GPU worker response missing expected field(s): {what}")
    raise SeparationError("Separation completed but output data was incomplete.")


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
    instrumental_path) on success - IDENTICAL return shape to the
    pre-migration version.

    NOW `async def`, not a plain blocking function - see this file's own
    module docstring for why, and why routes.py's call site needs its
    `run_blocking(run_separation, ...)` wrapper removed in favour of a
    plain `await run_separation(...)` as the very next step.

    The four tunables default to the standard (fast) tier's config
    values, same as before migration; routes.py passes the HQ set
    explicitly for /separate-hq. Resolved by the CALLER at job submission
    time, not read here - unchanged reasoning from the pre-migration
    version, still true regardless of where the actual Demucs run
    happens.

    Raises SeparationError on any failure - disallowed model, duration
    limit exceeded, GPU worker failure, or an incomplete GPU response.
    """
    output = await _run_demucs_on_gpu(
        input_path, job_id, "separate", model, overlap, timeout_seconds, max_duration_seconds,
    )

    vocals_b64 = output.get("vocals_b64")
    instrumental_b64 = output.get("instrumental_b64")
    if not vocals_b64 or not instrumental_b64:
        _missing_output_error(job_id, "vocals_b64/instrumental_b64")

    # Same final filenames as the pre-migration version - jobs.py's
    # cleanup and every preview/download route reference these paths and
    # need no awareness that they're now written from a decoded GPU
    # response instead of moved out of a local Demucs work directory.
    final_vocals_path = os.path.join(SEPARATION_DIR, f"{job_id}_vocals.wav")
    final_instrumental_path = os.path.join(SEPARATION_DIR, f"{job_id}_instrumental.wav")

    await run_blocking(b64_to_file, vocals_b64, final_vocals_path)
    await run_blocking(b64_to_file, instrumental_b64, final_instrumental_path)

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
    Full multi-stem mode. Returns a {stem_name: path} dict - IDENTICAL
    return shape to the pre-migration version: four entries
    (vocals/drums/bass/other) for htdemucs and htdemucs_ft, six if a
    6-source model is ever used.

    NOW `async def` - same reasoning as run_separation() above.

    Which stems to expect still comes from config's MODEL_STEM_NAMES,
    unchanged - this function needs no change to support a model with a
    different stem set, exactly as before migration.
    """
    expected_stems = MODEL_STEM_NAMES.get(model)
    if not expected_stems:
        logger.error(f"[STEMS] Job {job_id} rejected - no stem list configured for model '{model}'")
        raise SeparationError("Separation failed: unsupported model requested.")

    output = await _run_demucs_on_gpu(
        input_path, job_id, "stems", model, overlap, timeout_seconds, max_duration_seconds,
    )

    stems_b64 = output.get("stems")
    if not isinstance(stems_b64, dict) or not all(s in stems_b64 for s in expected_stems):
        _missing_output_error(job_id, f"stems (expected {expected_stems})")

    final_paths: Dict[str, str] = {}
    for stem in expected_stems:
        dest = os.path.join(SEPARATION_DIR, f"{job_id}_{stem}.wav")
        await run_blocking(b64_to_file, stems_b64[stem], dest)
        final_paths[stem] = dest

    logger.info(
        f"[STEMS] Job {job_id} complete (model={model}, GPU, {len(final_paths)} stems): "
        f"{', '.join(final_paths.keys())}"
    )
    return final_paths