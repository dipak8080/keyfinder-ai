"""
handler.py - RunPod Serverless entrypoint for GPU-backed Demucs separation.

Deployed as its OWN Docker image to its OWN RunPod Serverless endpoint,
completely independent of the VPS backend. See gpu-worker/Dockerfile.

--------------------------------------------------------------------------
WHAT CHANGED (v2): AUDIO NO LONGER TRAVELS THROUGH RUNPOD'S JOB PAYLOAD

v1 sent the input file as base64 inside the job's "input" dict and
returned base64-encoded stems inside the job's "output" dict. That works
for a few seconds of silence (the build-time warmup clip) but breaks on
real audio: RunPod's Serverless job payload has a hard 10MB limit on
/run responses (see docs.runpod.io/serverless/workers/handler-functions,
"Overview - Be aware of payload size limits"). A 2:38 track separated
into two WAV stems, base64-encoded, is comfortably tens of MB - the job
would sit stuck with no visible error, because the worker finishes the
actual separation fine, it just can't hand the result back through
RunPod's payload channel. The same ceiling bites on the INPUT side too,
for anything much larger than a couple of MB.

The fix: RunPod's job queue is used ONLY for orchestration now (submit,
poll status, a small confirmation dict). Actual audio bytes flow
DIRECTLY between this worker and the VPS over plain HTTP - no size limit
on that path at all, since it never touches RunPod's own response
handling.

  INPUT:  the worker GETs the audio from a URL the VPS gives it
          (VPS_BASE_URL + /internal/gpu/input/{job_id}), rather than
          receiving it inline.
  OUTPUT: the worker POSTs each finished stem's raw bytes straight to the
          VPS (VPS_BASE_URL + /internal/gpu/upload/{job_id}/{name}) as it
          produces them, and the job's own "output" dict returned to
          RunPod is now tiny - just confirmation + timing metadata, well
          under any payload limit.

Both directions are authenticated with a single shared secret
(GPU_SHARED_SECRET), set as an environment variable on THIS worker's
RunPod endpoint config AND as GPU_WORKER_SHARED_SECRET in the VPS's own
.env - the two must match. See the VPS-side gpu_internal_routes.py for
the receiving end of both calls.

INPUT (job["input"]):
  task                  "separate" | "stems"
  job_id                the VPS's own job id - used to build both the
                         input-fetch URL and the output-upload URLs
  filename               original filename - extension only, same
                         reasoning as before (never trusted as a path)
  model                 one of ALLOWED_SEPARATION_MODELS below
  overlap                float, Demucs --overlap value
  max_duration_seconds  reject cleanly if the fetched audio exceeds this

OUTPUT (small, always well under RunPod's payload limit):
  {"uploaded_stems": [...], "duration_seconds": ..., "gpu_seconds": ...}
  or {"error": "..."} - same error-shape contract as v1, see ERRORS
  below.

ERRORS: unchanged from v1 - every failure path returns {"error": ...}
rather than raising, so a bad request or a failed upload never shows up
as a crashed RunPod worker.
"""
import os
import time
import shutil
import subprocess
import tempfile

import requests
import runpod

# ---------- MIRRORS config.py's separation section, ON PURPOSE ----------
# Not imported - this worker is a separate deployable with its own image
# and no access to the VPS repo. Keep these in sync by hand with
# config.py's ALLOWED_SEPARATION_MODELS / MODEL_STEM_NAMES whenever
# either changes there - same deliberate-duplication pattern already
# used elsewhere in this codebase.
ALLOWED_SEPARATION_MODELS = ("htdemucs", "htdemucs_ft", "htdemucs_6s")

MODEL_STEM_NAMES = {
    "htdemucs": ("vocals", "drums", "bass", "other"),
    "htdemucs_ft": ("vocals", "drums", "bass", "other"),
    "htdemucs_6s": ("vocals", "drums", "bass", "other", "guitar", "piano"),
}

MAX_EXTENSION_LENGTH = 10

# Read once at cold start, not per-request - these describe THIS
# deployment, not anything that varies job to job. Both are REQUIRED;
# missing either fails every job immediately with a clear error rather
# than a mysterious timeout, so a misconfigured endpoint is obvious
# instead of silently hanging like the payload-size bug this file fixes.
VPS_BASE_URL = os.environ.get("VPS_BASE_URL", "").rstrip("/")
GPU_SHARED_SECRET = os.environ.get("GPU_SHARED_SECRET", "")

# Generous timeouts for the file transfers themselves - these move real
# audio (tens of MB), on top of whatever the VPS's own network conditions
# are, and are a completely different concern from the job's own
# max_duration_seconds / Demucs execution timeout.
_TRANSFER_TIMEOUT_SECONDS = 120

# Upload retry policy. See _upload_result's docstring for why the OUTPUT
# side is retried and the input side is not: a failed upload discards GPU
# work that has already been billed for.
_UPLOAD_MAX_ATTEMPTS = 3
_UPLOAD_BACKOFF_SECONDS = 2.0


def _safe_extension(filename: str, fallback: str = "wav") -> str:
    if not filename:
        return fallback
    ext = os.path.splitext(filename)[1].lstrip(".")
    cleaned = "".join(c for c in ext if c.isascii() and c.isalnum()).lower()
    if not cleaned or len(cleaned) > MAX_EXTENSION_LENGTH:
        return fallback
    return cleaned


def _get_duration_seconds(file_path: str) -> float:
    result = subprocess.run(
        [
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            file_path,
        ],
        capture_output=True, text=True, timeout=30, check=True,
    )
    return float(result.stdout.strip())


def _download_input(job_id: str, dest_path: str) -> None:
    """
    GETs the input audio from the VPS. Raises on any failure - the
    caller (handler()) wraps this in a try/except and turns it into a
    clean {"error": ...} return, same as every other failure path here.
    """
    url = f"{VPS_BASE_URL}/internal/gpu/input/{job_id}"
    headers = {"Authorization": f"Bearer {GPU_SHARED_SECRET}"}
    with requests.get(url, headers=headers, timeout=_TRANSFER_TIMEOUT_SECONDS, stream=True) as res:
        if res.status_code != 200:
            raise RuntimeError(f"Failed to fetch input audio (HTTP {res.status_code}): {res.text[:300]}")
        with open(dest_path, "wb") as f:
            for chunk in res.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)


def _upload_result(job_id: str, name: str, file_path: str) -> None:
    """
    POSTs one finished stem's raw bytes straight to the VPS. Streams the
    file from disk rather than reading it whole into memory.

    RETRIED, unlike the input fetch, and the asymmetry is deliberate.
    By the time this runs the GPU work is ALREADY DONE AND ALREADY PAID
    FOR - a transient network blip here throws away a completed,
    billed separation and forces the user to resubmit, paying for the
    identical compute a second time. That makes a retry here worth far
    more than one on the input side, where a failure costs only a
    cold start.

    The file handle is reopened per attempt: a streamed upload consumes
    the handle, so a retry against the same exhausted handle would post
    zero bytes and "succeed" at uploading nothing - a silent corruption
    that would surface much later as an unplayable stem.
    """
    url = f"{VPS_BASE_URL}/internal/gpu/upload/{job_id}/{name}"
    headers = {
        "Authorization": f"Bearer {GPU_SHARED_SECRET}",
        "Content-Type": "audio/wav",
    }

    last_error = None
    for attempt in range(1, _UPLOAD_MAX_ATTEMPTS + 1):
        try:
            with open(file_path, "rb") as f:
                res = requests.post(
                    url, headers=headers, data=f, timeout=_TRANSFER_TIMEOUT_SECONDS
                )
            if res.status_code == 200:
                return
            # 4xx means the VPS rejected this request on its merits
            # (bad secret, job no longer in flight, name rejected).
            # Retrying reproduces it identically, so fail fast rather
            # than burning three attempts on a guaranteed repeat.
            if 400 <= res.status_code < 500:
                raise RuntimeError(
                    f"Upload of '{name}' rejected by VPS (HTTP {res.status_code}): {res.text[:300]}"
                )
            last_error = f"HTTP {res.status_code}: {res.text[:200]}"
        except RuntimeError:
            raise
        except Exception as e:
            last_error = str(e)

        if attempt < _UPLOAD_MAX_ATTEMPTS:
            time.sleep(_UPLOAD_BACKOFF_SECONDS * attempt)

    raise RuntimeError(
        f"Failed to upload result '{name}' after {_UPLOAD_MAX_ATTEMPTS} attempts: {last_error}"
    )


def _run_demucs_gpu(input_path: str, work_dir: str, model: str, overlap: float, two_stems: bool):
    """
    Unchanged from v1: forces the GPU explicitly via `-d cuda` so a real
    CUDA problem fails loudly instead of silently falling back to (very
    expensive) CPU execution on a GPU-billed worker.
    """
    cmd = ["demucs", "-n", model, "-d", "cuda"]
    if two_stems:
        cmd += ["--two-stems", "vocals"]
    cmd += ["--overlap", str(overlap), "-o", work_dir, input_path]

    started = time.monotonic()
    result = subprocess.run(cmd, capture_output=True, text=True)
    gpu_seconds = time.monotonic() - started

    if result.returncode != 0:
        raise RuntimeError(f"Demucs failed (exit {result.returncode}): {result.stderr[-2000:]}")

    input_stem = os.path.splitext(os.path.basename(input_path))[0]
    track_dir = os.path.join(work_dir, model, input_stem)
    return track_dir, gpu_seconds


def handler(job):
    inp = job.get("input") or {}

    task = inp.get("task")
    job_id = inp.get("job_id")
    filename = inp.get("filename", "input.wav")
    model = inp.get("model", "htdemucs")
    overlap = float(inp.get("overlap", 0.25))
    max_duration_seconds = int(inp.get("max_duration_seconds", 600))

    if not VPS_BASE_URL or not GPU_SHARED_SECRET:
        # Configuration error, not a per-job problem - fails every job
        # identically and immediately rather than hanging, so a
        # misconfigured endpoint is obvious the first time it's used.
        return {"error": "Worker is not configured with VPS_BASE_URL/GPU_SHARED_SECRET."}

    if task not in ("separate", "stems"):
        return {"error": f"Invalid task '{task}' - must be 'separate' or 'stems'."}
    if not job_id:
        return {"error": "Missing required field: job_id"}
    if model not in ALLOWED_SEPARATION_MODELS:
        return {"error": f"Unsupported model '{model}'."}
    if task == "stems" and model not in MODEL_STEM_NAMES:
        return {"error": f"No stem list configured for model '{model}'."}

    work_dir = tempfile.mkdtemp(prefix="job_")
    ext = _safe_extension(filename)
    input_path = os.path.join(work_dir, f"input.{ext}")

    try:
        try:
            _download_input(job_id, input_path)
        except Exception as e:
            return {"error": f"Could not fetch input audio from VPS: {e}"}

        try:
            duration = _get_duration_seconds(input_path)
        except Exception as e:
            return {"error": f"Could not read audio duration: {e}"}

        if duration > max_duration_seconds:
            return {
                "error": (
                    f"Track is {int(duration // 60)} min long, which exceeds the "
                    f"{max_duration_seconds // 60} min limit for separation."
                )
            }

        try:
            track_dir, gpu_seconds = _run_demucs_gpu(
                input_path, work_dir, model, overlap, two_stems=(task == "separate"),
            )
        except Exception as e:
            return {"error": f"Separation failed while processing the audio: {e}"}

        if task == "separate":
            sources = {
                "vocals": os.path.join(track_dir, "vocals.wav"),
                "instrumental": os.path.join(track_dir, "no_vocals.wav"),
            }
        else:
            expected_stems = MODEL_STEM_NAMES[model]
            sources = {s: os.path.join(track_dir, f"{s}.wav") for s in expected_stems}

        if not all(os.path.exists(p) for p in sources.values()):
            return {"error": "Separation completed but output files were not found."}

        uploaded = []
        for name, path in sources.items():
            try:
                _upload_result(job_id, name, path)
                uploaded.append(name)
            except Exception as e:
                return {"error": f"Separation succeeded but uploading '{name}' back to the VPS failed: {e}"}

        return {
            "uploaded_stems": uploaded,
            "duration_seconds": duration,
            "gpu_seconds": gpu_seconds,
        }
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


runpod.serverless.start({"handler": handler})