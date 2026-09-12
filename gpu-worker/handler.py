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
ALLOWED_SEPARATION_MODELS = ("htdemucs", "htdemucs_ft", "htdemucs_6s", "melband_roformer")

MODEL_STEM_NAMES = {
    "htdemucs": ("vocals", "drums", "bass", "other"),
    "htdemucs_ft": ("vocals", "drums", "bass", "other"),
    "htdemucs_6s": ("vocals", "drums", "bass", "other", "guitar", "piano"),
    "melband_roformer": ("vocals", "drums", "bass", "other"),
}

# ---------- MelBand RoFormer (HQ vocal path) ----------
# "melband_roformer" is not a Demucs model: it runs via audio-separator
# using the MIT-licensed Kimberley Jensen MelBand RoFormer vocal weights
# (vocals SDR ~12.6 on the package's own benchmark registry vs ~10.8 for
# htdemucs_ft - the whole reason this path exists). It produces exactly
# two sources (vocals / instrumental), so:
#   task "separate": RoFormer output is the final answer.
#   task "stems":    two-stage - RoFormer extracts vocals, then Demucs
#                    splits the RoFormer INSTRUMENTAL into drums/bass/
#                    other. Running Demucs on vocal-free audio also
#                    cleans up its stems (the standard leaderboard
#                    ensemble trick). Demucs' own residual "vocals" stem
#                    from that second pass is discarded.
ROFORMER_MODEL_FILENAME = "vocals_mel_band_roformer.ckpt"
ROFORMER_STEMS_SECOND_STAGE = "htdemucs_ft"

# Loaded once per worker process and kept warm - model init is the
# expensive part, and RunPod serverless workers handle one job at a
# time, so a single global instance is both safe and the fast path for
# warm requests.
#
# The separator writes into ONE fixed directory for the life of the
# process, and each job MOVES its outputs into its own work_dir. The
# obvious-looking alternative - retargeting output_dir per job - does
# not work: the loaded model instance snapshots its config (including
# output_dir) at load_model() time, so a mutated attribute on the
# wrapper is silently ignored on warm reuse and files land in a
# previous job's already-deleted directory.
_ROFORMER = None
ROFORMER_OUTPUT_DIR = "/worker/roformer_out"


def _get_roformer():
    global _ROFORMER
    from audio_separator.separator import Separator

    if _ROFORMER is None:
        os.makedirs(ROFORMER_OUTPUT_DIR, exist_ok=True)
        sep = Separator(
            model_file_dir=os.environ.get("AUDIO_SEPARATOR_MODEL_DIR", "/worker/models"),
            output_dir=ROFORMER_OUTPUT_DIR,
            output_format="WAV",
            use_autocast=True,
        )
        sep.load_model(model_filename=ROFORMER_MODEL_FILENAME)
        _ROFORMER = sep
    return _ROFORMER


def _run_roformer_gpu(input_path: str, work_dir: str):
    """
    Returns ({"vocals": path, "instrumental": path}, gpu_seconds).

    Trusts separate()'s RETURN VALUE (the fully written output paths)
    rather than predicting filenames - stem naming varies per model
    config, and a custom_output_names key that doesn't match a stem is
    silently ignored, producing a default-named file instead.
    """
    sep = _get_roformer()

    # One job at a time per worker, so the shared dir only ever holds
    # the current job's outputs - clear leftovers from the previous one.
    for stale in os.listdir(ROFORMER_OUTPUT_DIR):
        try:
            os.remove(os.path.join(ROFORMER_OUTPUT_DIR, stale))
        except OSError:
            pass

    started = time.monotonic()
    # Keys are matched case-insensitively against the model config's own
    # stem names. This checkpoint names its stems "vocals" and "other"
    # (verified empirically); "Instrumental" is included for any future
    # checkpoint that uses it. Unmatched keys are ignored, so covering
    # both costs nothing and guarantees deterministic filenames either
    # way. NEVER classify by substring: an unmatched stem falls back to
    # a default filename that embeds the MODEL name - which for this
    # model contains the word "vocals" - and that is exactly the bug
    # that shipped vocals and instrumental swapped.
    returned = sep.separate(
        input_path,
        custom_output_names={
            "Vocals": "roformer_vocals",
            "Instrumental": "roformer_instrumental",
            "Other": "roformer_instrumental",
        },
    )
    gpu_seconds = time.monotonic() - started

    by_name = {}
    for path in returned or []:
        full = path if os.path.isabs(path) else os.path.join(ROFORMER_OUTPUT_DIR, path)
        if os.path.exists(full):
            by_name[os.path.basename(full)] = full

    vocals_src = by_name.get("roformer_vocals.wav")
    instrumental_src = by_name.get("roformer_instrumental.wav")
    if not vocals_src or not instrumental_src:
        raise RuntimeError(
            f"RoFormer outputs missing or unrecognised - expected roformer_vocals.wav "
            f"and roformer_instrumental.wav, got: {sorted(by_name)} (returned: {returned})"
        )

    sources = {
        "vocals": os.path.join(work_dir, "roformer_vocals.wav"),
        "instrumental": os.path.join(work_dir, "roformer_instrumental.wav"),
    }
    shutil.move(vocals_src, sources["vocals"])
    shutil.move(instrumental_src, sources["instrumental"])
    return sources, gpu_seconds

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

# Transfer retry policy, both directions.
#
# The input side was deliberately NOT retried until 2026-09-12, on the
# reasoning that a failed fetch costs only a cold start. True for the
# bill, wrong for the user: 2026-09-12 04:17, an 11.5 MB mp3 stopped at
# 3.0 MB ("IncompleteRead") and the separation was lost. Resubmitting
# then pays for a SECOND cold start and a full separation, so not
# retrying is the more expensive branch. A retry here is free: no GPU
# work has run yet, and it reuses the worker already running.
_UPLOAD_MAX_ATTEMPTS = 3
_UPLOAD_BACKOFF_SECONDS = 2.0
_DOWNLOAD_MAX_ATTEMPTS = 3
_DOWNLOAD_BACKOFF_SECONDS = 2.0


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
    GETs the input audio from the VPS, retried on transport failures.
    Raises on any failure - the caller (handler()) wraps this in a
    try/except and turns it into a clean {"error": ...} return, same as
    every other failure path here.

    A SHORT BODY IS A FAILURE, not a file. The VPS serves this with
    FileResponse, so Content-Length is always present and a truncated
    transfer is detectable. Without this check a partial mp3 reaches
    Demucs, which fails later with something unrelated-looking (or,
    worse, separates the first 3 MB and returns a silently truncated
    result the user pays for). 4xx is not retried - a bad secret or an
    expired job reproduces identically.
    """
    url = f"{VPS_BASE_URL}/internal/gpu/input/{job_id}"
    headers = {"Authorization": f"Bearer {GPU_SHARED_SECRET}"}
    last_error = "unknown error"

    for attempt in range(1, _DOWNLOAD_MAX_ATTEMPTS + 1):
        try:
            with requests.get(
                url, headers=headers, timeout=_TRANSFER_TIMEOUT_SECONDS, stream=True
            ) as res:
                if 400 <= res.status_code < 500:
                    raise RuntimeError(
                        f"Failed to fetch input audio (HTTP {res.status_code}): {res.text[:300]}"
                    )
                if res.status_code != 200:
                    last_error = f"HTTP {res.status_code}: {res.text[:200]}"
                    raise _RetryTransfer(last_error)

                expected = res.headers.get("Content-Length")
                written = 0
                with open(dest_path, "wb") as f:
                    for chunk in res.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
                        written += len(chunk)

            if expected is not None and written != int(expected):
                last_error = f"truncated transfer: got {written} of {expected} bytes"
                raise _RetryTransfer(last_error)
            if written == 0:
                last_error = "empty response body"
                raise _RetryTransfer(last_error)
            if attempt > 1:
                print(f"[TRANSFER] Input fetch succeeded on attempt {attempt}", flush=True)
            return

        except (RuntimeError, requests.HTTPError):
            raise
        except Exception as e:
            last_error = str(e) or type(e).__name__

        # Any partial file is removed before retrying: the next attempt
        # opens "wb" anyway, but leaving it would make an abandoned final
        # attempt look like a readable input to anything downstream.
        try:
            if os.path.exists(dest_path):
                os.remove(dest_path)
        except OSError:
            pass

        if attempt < _DOWNLOAD_MAX_ATTEMPTS:
            print(
                f"[TRANSFER] Input fetch attempt {attempt}/{_DOWNLOAD_MAX_ATTEMPTS} "
                f"failed, retrying: {last_error[:200]}",
                flush=True,
            )
            time.sleep(_DOWNLOAD_BACKOFF_SECONDS * attempt)

    raise RuntimeError(
        f"Failed to fetch input audio after {_DOWNLOAD_MAX_ATTEMPTS} attempts: {last_error}"
    )


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


class _RetryTransfer(Exception):
    """Internal: a transport-level failure worth another attempt. Never
    escapes _download_input - it is caught by the same handler as any
    other transport exception there."""


MIN_DURATION_SECONDS = 3.0


def _normalise_input(input_path: str, work_dir: str) -> str:
    """Re-encode whatever the user uploaded into plain stereo 44.1k PCM.

    Demucs crashes with an opaque AssertionError in reflect padding when
    fed audio that decodes to NaN samples or exotic layouts. One cheap
    CPU transcode up front turns every input into the one shape the
    model was trained on, and turns undecodable files into a clean,
    user-facing error instead of a GPU-side traceback.
    """
    clean_path = os.path.join(work_dir, "input_clean.wav")
    result = subprocess.run(
        ["ffmpeg", "-y", "-v", "error", "-i", input_path,
         "-ac", "2", "-ar", "44100", "-c:a", "pcm_s16le", clean_path],
        capture_output=True, text=True,
    )
    if result.returncode != 0 or not os.path.exists(clean_path) or os.path.getsize(clean_path) == 0:
        raise ValueError(
            "This file could not be decoded as audio. It may be corrupted "
            "or not actually an audio file."
        )
    return clean_path


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
        stderr = result.stderr[-2000:]
        if "AssertionError" in stderr and "pad1d" in stderr:
            # Known Demucs failure mode on degenerate input (too short,
            # silent, or NaN samples). The traceback is useless to the
            # person who uploaded the file; this message is not.
            raise RuntimeError(
                "This audio couldn't be processed - it appears to be too "
                "short or contains no usable audio data."
            )
        raise RuntimeError(f"Demucs failed (exit {result.returncode}): {stderr}")

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

        if duration < MIN_DURATION_SECONDS:
            return {
                "error": (
                    f"Track is only {duration:.1f}s long - separation needs at "
                    f"least {MIN_DURATION_SECONDS:.0f} seconds of audio."
                )
            }

        try:
            clean_path = _normalise_input(input_path, work_dir)
        except ValueError as e:
            return {"error": str(e)}
        except Exception as e:
            return {"error": f"Could not prepare the audio for separation: {e}"}

        try:
            if model == "melband_roformer":
                roformer_sources, gpu_seconds = _run_roformer_gpu(clean_path, work_dir)
                if task == "separate":
                    sources = roformer_sources
                else:
                    # Stage 2: Demucs on the vocal-free instrumental.
                    track_dir, demucs_seconds = _run_demucs_gpu(
                        roformer_sources["instrumental"], work_dir,
                        ROFORMER_STEMS_SECOND_STAGE, overlap, two_stems=False,
                    )
                    gpu_seconds += demucs_seconds
                    sources = {
                        "vocals": roformer_sources["vocals"],
                        "drums": os.path.join(track_dir, "drums.wav"),
                        "bass": os.path.join(track_dir, "bass.wav"),
                        "other": os.path.join(track_dir, "other.wav"),
                    }
            else:
                track_dir, gpu_seconds = _run_demucs_gpu(
                    clean_path, work_dir, model, overlap, two_stems=(task == "separate"),
                )
                if task == "separate":
                    sources = {
                        "vocals": os.path.join(track_dir, "vocals.wav"),
                        "instrumental": os.path.join(track_dir, "no_vocals.wav"),
                    }
                else:
                    expected_stems = MODEL_STEM_NAMES[model]
                    sources = {s: os.path.join(track_dir, f"{s}.wav") for s in expected_stems}
        except Exception as e:
            return {"error": f"Separation failed while processing the audio: {e}"}

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