"""
gpu-worker-whisper/handler.py - RunPod Serverless handler for Whisper
transcription on a GPU.

WHERE THIS FITS: this is the RUNPOD side. The VPS side is
speech_to_text_gpu.py, which submits jobs here through runpod_client.py.
It is a SEPARATE deploy target from gpu-worker/ (the Demucs worker) -
its own image, its own RunPod endpoint, its own endpoint id. They share
nothing but runpod_client.py's generic submit/poll code, exactly as that
module's docstring anticipated.

AUDIO COMES IN OVER HTTP, NOT IN THE JOB PAYLOAD. RunPod caps a job
payload at 10MB. Twenty minutes of audio blows straight past that even
compressed, so the VPS registers the file against a one-time transfer
token and this worker fetches it from
    GET {vps_base_url}/internal/gpu/input/{token}
with `Authorization: Bearer <shared secret>`. Identical mechanism to the
Demucs worker - see gpu_internal_routes.py on the VPS side.

THE RESULT GOES BACK IN THE JOB PAYLOAD, and that asymmetry is
deliberate. A transcript is a few KB of JSON even for a long recording,
so it fits the 10MB response comfortably and needs no upload route,
no second HTTP hop, and no temp file on the VPS. This is the one place
the Whisper worker is genuinely simpler than the Demucs one, which has
to ship hundreds of megabytes of stems back.

MODEL IS LOADED AT MODULE IMPORT, NOT PER JOB. RunPod keeps a warm
container alive between jobs, so an import-time load is paid once per
cold start rather than once per request. This is the single biggest
factor in perceived latency at low traffic: a cold start pays the model
load (~15-30s for `small` on a modern GPU, mostly weight download unless
the weights are baked into the image), a warm one pays nothing. The
Dockerfile bakes the weights specifically so a cold start is a load from
local disk rather than a download from HuggingFace.

FLOAT16, NOT INT8. int8 is the right choice on CPU where it buys a 3-4x
speedup; on a GPU with tensor cores, float16 is both faster AND more
accurate, so there is no trade to make. Overridable per job anyway.
"""
import base64
import os
import tempfile
import time

import requests
import runpod
from faster_whisper import WhisperModel

# ---------- CONFIG ----------
MODEL_SIZE = os.environ.get("WHISPER_MODEL_SIZE", "small")
COMPUTE_TYPE = os.environ.get("WHISPER_COMPUTE_TYPE", "float16")
DEVICE = os.environ.get("WHISPER_DEVICE", "cuda")

# How long to wait on the VPS when fetching the input file. Generous:
# the VPS is on a modest connection and a 20-minute WAV is not small.
INPUT_FETCH_TIMEOUT_SECONDS = int(os.environ.get("INPUT_FETCH_TIMEOUT_SECONDS", "300"))

# Hard ceiling on a fetched input. Guards against a misconfigured or
# hostile VPS_BASE_URL streaming unbounded data into this container's
# ephemeral disk.
MAX_INPUT_BYTES = int(os.environ.get("MAX_INPUT_BYTES", str(500 * 1024 * 1024)))  # 500 MB

_FETCH_CHUNK = 1024 * 1024


print(f"[WHISPER_GPU] Loading model '{MODEL_SIZE}' "
      f"(device={DEVICE}, compute_type={COMPUTE_TYPE})...", flush=True)
_load_started = time.monotonic()
_model = WhisperModel(MODEL_SIZE, device=DEVICE, compute_type=COMPUTE_TYPE)
print(f"[WHISPER_GPU] Model loaded in {time.monotonic() - _load_started:.1f}s", flush=True)


def _fetch_input(vps_base_url: str, token: str, secret: str, dest_path: str) -> int:
    """
    Streams the audio file from the VPS to local disk.

    Streamed in chunks rather than r.content for the same reason the VPS
    serves it with FileResponse: holding a few hundred MB in memory on a
    container sized for model weights is how a worker gets OOM-killed
    mid-job, which RunPod reports as an opaque failure with no traceback.
    """
    url = f"{vps_base_url.rstrip('/')}/internal/gpu/input/{token}"
    headers = {"Authorization": f"Bearer {secret}"}

    written = 0
    with requests.get(url, headers=headers, stream=True,
                      timeout=INPUT_FETCH_TIMEOUT_SECONDS) as r:
        if r.status_code == 404:
            raise RuntimeError(
                "The VPS has no input registered for this token - the job "
                "was probably cancelled or timed out on that side."
            )
        r.raise_for_status()

        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=_FETCH_CHUNK):
                if not chunk:
                    continue
                written += len(chunk)
                if written > MAX_INPUT_BYTES:
                    raise RuntimeError(
                        f"Input exceeded {MAX_INPUT_BYTES} bytes - refusing to "
                        f"continue downloading."
                    )
                f.write(chunk)

    if written == 0:
        raise RuntimeError("The VPS served an empty input file.")
    return written


def handler(job):
    """
    RunPod entry point.

    Expected input:
        {
          "vps_base_url": "https://api.audioforges.com",
          "token":        "<32 hex chars, one-time transfer handle>",
          "secret":       "<GPU_WORKER_SHARED_SECRET>",
          "language":     "en" | null,
          "task":         "transcribe" | "translate",
          "beam_size":    5,
          "vad_filter":   false
        }

    Returns the SAME dict shape speech_to_text.transcribe() produces on
    the VPS, so the two backends are interchangeable to every caller.
    Any divergence here would surface as a frontend bug that only
    appears when the GPU backend is enabled - the worst kind to trace.

    Errors are returned as {"error": "..."} rather than raised. RunPod
    surfaces a raised exception as a generic failure with the traceback
    buried in worker logs; an explicit error field reaches the VPS
    intact and can be logged there, where anyone is actually looking.
    """
    started = time.monotonic()
    job_input = job.get("input") or {}

    vps_base_url = job_input.get("vps_base_url")
    token = job_input.get("token")
    secret = job_input.get("secret")

    if not (vps_base_url and token and secret):
        return {"error": "Missing vps_base_url, token or secret in job input."}

    language = job_input.get("language") or None
    task = job_input.get("task") or "transcribe"
    beam_size = int(job_input.get("beam_size") or 5)
    vad_filter = bool(job_input.get("vad_filter"))

    # Suffix matters: faster-whisper hands the path to PyAV, which uses
    # the extension as a hint when the container is ambiguous. The VPS
    # sends it so this side doesn't have to guess.
    suffix = job_input.get("suffix") or ".wav"
    if not suffix.startswith("."):
        suffix = "." + suffix

    tmp_dir = tempfile.mkdtemp(prefix="whisper_")
    audio_path = os.path.join(tmp_dir, f"input{suffix}")

    try:
        fetch_started = time.monotonic()
        size = _fetch_input(vps_base_url, token, secret, audio_path)
        fetch_seconds = time.monotonic() - fetch_started
        print(f"[WHISPER_GPU] Fetched {size / (1024 * 1024):.1f}MB in "
              f"{fetch_seconds:.1f}s", flush=True)

        infer_started = time.monotonic()
        segments_iter, info = _model.transcribe(
            audio_path,
            beam_size=beam_size,
            language=language,
            task=task,
            vad_filter=vad_filter,
        )

        segments = []
        text_parts = []
        for seg in segments_iter:
            cleaned = seg.text.strip()
            if not cleaned:
                continue
            segments.append({
                "start": round(seg.start, 2),
                "end": round(seg.end, 2),
                "text": cleaned,
            })
            text_parts.append(cleaned)

        full_text = " ".join(text_parts).strip()
        if not full_text:
            # Returned as an error rather than an empty transcript so the
            # VPS can raise the same user-facing AudioToolError the local
            # backend raises for this case.
            return {"error": "NO_SPEECH_DETECTED"}

        infer_seconds = time.monotonic() - infer_started
        audio_duration = round(float(getattr(info, "duration", 0.0) or 0.0), 2)
        rtf = (audio_duration / infer_seconds) if infer_seconds > 0 else 0.0

        print(f"[WHISPER_GPU] Transcribed {audio_duration:.1f}s of audio in "
              f"{infer_seconds:.1f}s (rtf={rtf:.1f}x), {len(segments)} segments",
              flush=True)

        return {
            "text": full_text,
            # Mirrors the local backend exactly: a forced language reports
            # probability 1.0, because the model's internal figure is
            # meaningless once the language was dictated rather than
            # detected.
            "language": language or info.language,
            "language_probability": 1.0 if language else round(info.language_probability, 3),
            "language_forced": language is not None,
            "task": task,
            "duration": audio_duration,
            "segments": segments,
            # Diagnostics the VPS logs but does not pass to the client.
            "_gpu": {
                "fetch_seconds": round(fetch_seconds, 2),
                "infer_seconds": round(infer_seconds, 2),
                "total_seconds": round(time.monotonic() - started, 2),
                "rtf": round(rtf, 2),
                "model": MODEL_SIZE,
                "compute_type": COMPUTE_TYPE,
            },
        }

    except Exception as e:
        print(f"[WHISPER_GPU] FAILED: {e.__class__.__name__}: {e}", flush=True)
        return {"error": f"{e.__class__.__name__}: {e}"}

    finally:
        # Ephemeral container disk is small and a warm worker handles many
        # jobs in a row - leaking a few hundred MB per job fills it within
        # an hour of steady traffic and every subsequent job fails on a
        # full disk, which reads as a mysterious intermittent outage.
        try:
            if os.path.exists(audio_path):
                os.remove(audio_path)
            os.rmdir(tmp_dir)
        except Exception as cleanup_err:
            print(f"[WHISPER_GPU] Cleanup failed (non-fatal): {cleanup_err}", flush=True)


runpod.serverless.start({"handler": handler})