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
  stem_count            optional, 4 (default) or 6; 6 adds guitar and piano
  vocal_options         optional list: "dereverb" adds vocals_dry,
                         "lead_back" adds lead_vocals + backing_vocals
                         (melband_roformer only)

STUDIO ENGINE (v13): model "melband_roformer" is the Studio tier. With the
endpoint env STUDIO_ENGINE=sw (default) every Studio job runs one 6-stem
BS-RoFormer SW pass: 6 stems as is, 4 stems fold guitar and piano into
"other", the vocal remover's instrumental is the mix minus SW vocals.
STUDIO_ENGINE=legacy restores the v12 chain (Kim vocals, htdemucs_ft,
htdemucs_6s) without an image rollback. SW_QUALITY=max (v14) runs SW in full
precision with overlap 4; "fast" (default) is the v13 behaviour.
  clip_start, clip_seconds  optional preview window; only that slice is
                         separated and max_duration_seconds is not applied

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

# Extra passes on the RoFormer vocal stem, requested via "vocal_options".
# Each option ADDS stems; the original vocals stem is always kept.
#   dereverb -> "vocals_dry"                      (Sucial De-Reverb-Echo v2)
#   lead_back -> "lead_vocals" + "backing_vocals"  (becruily MelBand karaoke)
DEREVERB_MODEL_FILENAME = "dereverb-echo_mel_band_roformer_sdr_13.4843_v2.ckpt"
KARAOKE_MODEL_FILENAME = "mel_band_roformer_karaoke_becruily.ckpt"
ALLOWED_VOCAL_OPTIONS = ("dereverb", "lead_back")

ROFORMER_STEMS_SECOND_STAGE_6 = "htdemucs_6s"
ALLOWED_STEM_COUNTS = (4, 6)

MIN_CLIP_SECONDS = 5.0
MAX_CLIP_SECONDS = 60.0

SW_MODEL_FILENAME = "BS-Roformer-SW.ckpt"
SW_STEMS = ("vocals", "drums", "bass", "other", "guitar", "piano")
STUDIO_ENGINES = ("sw", "legacy")
STUDIO_ENGINE = os.environ.get("STUDIO_ENGINE", "sw").strip().lower()
if STUDIO_ENGINE not in STUDIO_ENGINES:
    print(f"[CONFIG] Unknown STUDIO_ENGINE {STUDIO_ENGINE!r}, using 'sw'", flush=True)
    STUDIO_ENGINE = "sw"

SW_QUALITIES = ("fast", "max")
SW_QUALITY = os.environ.get("SW_QUALITY", "fast").strip().lower()
if SW_QUALITY not in SW_QUALITIES:
    print(f"[CONFIG] Unknown SW_QUALITY {SW_QUALITY!r}, using 'fast'", flush=True)
    SW_QUALITY = "fast"
SW_MAX_OVERLAP = 4


def _separator_options(model_filename: str) -> dict:
    """fast: fp16 autocast, model's own overlap (2). max: full precision, overlap 4.
    SW stems get summed and subtracted, so per-stem peak normalisation must not
    rescale them independently (1.0 only touches stems that would clip anyway)."""
    opts = {"use_autocast": True}
    if model_filename == SW_MODEL_FILENAME:
        opts["normalization_threshold"] = 1.0
        if SW_QUALITY == "max":
            opts["use_autocast"] = False
            opts["mdxc_params"] = {
                "segment_size": 256, "override_model_segment_size": False,
                "batch_size": None, "overlap": SW_MAX_OVERLAP, "pitch_shift": 0,
            }
    return opts

# One loaded Separator per model file, kept warm for the life of the
# process. Each gets its own fixed output dir: a loaded model snapshots
# output_dir at load_model() time, so retargeting it per job does not work.
_SEPARATORS = {}
SEPARATOR_OUTPUT_ROOT = "/worker/as_out"


def _get_separator(model_filename: str):
    from audio_separator.separator import Separator

    cached = _SEPARATORS.get(model_filename)
    if cached is not None:
        return cached
    out_dir = os.path.join(SEPARATOR_OUTPUT_ROOT, os.path.splitext(model_filename)[0])
    os.makedirs(out_dir, exist_ok=True)
    sep = Separator(
        model_file_dir=os.environ.get("AUDIO_SEPARATOR_MODEL_DIR", "/worker/models"),
        output_dir=out_dir,
        output_format="WAV",
        **_separator_options(model_filename),
    )
    sep.load_model(model_filename=model_filename)
    _SEPARATORS[model_filename] = (sep, out_dir)
    return _SEPARATORS[model_filename]


def _run_separator(model_filename: str, input_path: str, work_dir: str, names: dict):
    """
    Runs one audio-separator model and returns ({basename: path}, gpu_seconds).

    `names` maps the model config's stem names (matched case-insensitively)
    to output basenames. Outputs are matched by EXACT basename from
    separate()'s return value, never by substring: an unmatched stem falls
    back to a default filename that embeds the model name, which is how
    vocals and instrumental once shipped swapped.
    """
    sep, out_dir = _get_separator(model_filename)
    for stale in os.listdir(out_dir):
        try:
            os.remove(os.path.join(out_dir, stale))
        except OSError:
            pass

    started = time.monotonic()
    returned = sep.separate(input_path, custom_output_names=names)
    gpu_seconds = time.monotonic() - started
    print(f"[TIMING] {model_filename}: {gpu_seconds:.1f}s", flush=True)

    by_name = {}
    for path in returned or []:
        full = path if os.path.isabs(path) else os.path.join(out_dir, path)
        if os.path.exists(full):
            by_name[os.path.basename(full)] = full

    results = {}
    for base in sorted(set(names.values())):
        src = by_name.get(f"{base}.wav")
        if not src:
            raise RuntimeError(
                f"{model_filename}: expected {base}.wav, got {sorted(by_name)} (returned: {returned})"
            )
        dest = os.path.join(work_dir, f"{base}.wav")
        shutil.move(src, dest)
        results[base] = dest
    return results, gpu_seconds


def _run_roformer_gpu(input_path: str, work_dir: str):
    """Returns ({"vocals": path, "instrumental": path}, gpu_seconds)."""
    out, gpu_seconds = _run_separator(
        ROFORMER_MODEL_FILENAME, input_path, work_dir,
        {
            "Vocals": "roformer_vocals",
            "Instrumental": "roformer_instrumental",
            "Other": "roformer_instrumental",
        },
    )
    return {"vocals": out["roformer_vocals"], "instrumental": out["roformer_instrumental"]}, gpu_seconds


def _run_sw_gpu(input_path: str, work_dir: str):
    """Returns ({stem: path} for all six SW stems, gpu_seconds)."""
    out, gpu_seconds = _run_separator(
        SW_MODEL_FILENAME, input_path, work_dir,
        {s.capitalize(): f"sw_{s}" for s in SW_STEMS},
    )
    return {s: out[f"sw_{s}"] for s in SW_STEMS}, gpu_seconds


def _write_combined(dest: str, plus, minus=()):
    """Sums `plus` minus `minus` sample by sample and writes 16-bit PCM."""
    import numpy as np
    import soundfile as sf

    arrays, sr = [], None
    for path in (*plus, *minus):
        data, part_sr = sf.read(path, dtype="float32", always_2d=True)
        if sr is None:
            sr = part_sr
        elif part_sr != sr:
            raise RuntimeError(f"sample rate mismatch: {path} is {part_sr}, expected {sr}")
        arrays.append(data)
    frames = min(len(a) for a in arrays)
    total = np.zeros((frames, arrays[0].shape[1]), dtype="float32")
    for i, data in enumerate(arrays):
        if i < len(plus):
            total += data[:frames]
        else:
            total -= data[:frames]
    sf.write(dest, np.clip(total, -1.0, 1.0), sr, subtype="PCM_16")
    return dest


def _studio_sw_sources(clean_path: str, work_dir: str, task: str, stem_count: int):
    """Returns (sources, vocals_path, gpu_seconds) for a Studio job on SW."""
    sw, gpu_seconds = _run_sw_gpu(clean_path, work_dir)
    if task == "separate":
        instrumental = _write_combined(
            os.path.join(work_dir, "sw_instrumental.wav"), (clean_path,), (sw["vocals"],),
        )
        return {"vocals": sw["vocals"], "instrumental": instrumental}, sw["vocals"], gpu_seconds
    if stem_count == 6:
        return {s: sw[s] for s in SW_STEMS}, sw["vocals"], gpu_seconds
    other = _write_combined(
        os.path.join(work_dir, "sw_other4.wav"), (sw["other"], sw["guitar"], sw["piano"]),
    )
    sources = {"vocals": sw["vocals"], "drums": sw["drums"], "bass": sw["bass"], "other": other}
    return sources, sw["vocals"], gpu_seconds


def _studio_legacy_sources(clean_path: str, work_dir: str, task: str, stem_count: int, overlap: float):
    """The v12 chain: Kim vocals, htdemucs_ft on the instrumental, htdemucs_6s for 6 stems."""
    roformer_sources, gpu_seconds = _run_roformer_gpu(clean_path, work_dir)
    if task == "separate":
        return dict(roformer_sources), roformer_sources["vocals"], gpu_seconds
    track_dir, demucs_seconds = _run_demucs_gpu(
        roformer_sources["instrumental"], work_dir,
        ROFORMER_STEMS_SECOND_STAGE, overlap, two_stems=False,
    )
    gpu_seconds += demucs_seconds
    sources = {"vocals": roformer_sources["vocals"]}
    for s in MODEL_STEM_NAMES[ROFORMER_STEMS_SECOND_STAGE]:
        if s != "vocals":
            sources[s] = os.path.join(track_dir, f"{s}.wav")
    if stem_count == 6:
        extra, six_seconds = _split_other_six(sources["other"], work_dir, overlap)
        gpu_seconds += six_seconds
        sources.update(extra)
    return sources, roformer_sources["vocals"], gpu_seconds


def _run_vocal_options(vocals_path: str, work_dir: str, options):
    """Returns ({stem_name: path}, gpu_seconds) for the requested extras."""
    extra = {}
    gpu_seconds = 0.0
    if "dereverb" in options:
        out, secs = _run_separator(
            DEREVERB_MODEL_FILENAME, vocals_path, work_dir,
            {"dry": "dry_vocals", "No dry": "reverb_tail"},
        )
        extra["vocals_dry"] = out["dry_vocals"]
        gpu_seconds += secs
    if "lead_back" in options:
        out, secs = _run_separator(
            KARAOKE_MODEL_FILENAME, vocals_path, work_dir,
            {"Vocals": "karaoke_lead", "Instrumental": "karaoke_backing"},
        )
        extra["lead_vocals"] = out["karaoke_lead"]
        extra["backing_vocals"] = out["karaoke_backing"]
        gpu_seconds += secs
    return extra, gpu_seconds


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

# Level 3 rather than the default 5: encode time is billed at GPU rates,
# and the extra compression past 3 is a couple of percent for several
# times the CPU.
FLAC_COMPRESSION_LEVEL = int(os.environ.get("FLAC_COMPRESSION_LEVEL", "3"))
FLAC_ENCODE_TIMEOUT_SECONDS = int(os.environ.get("FLAC_ENCODE_TIMEOUT_SECONDS", "120"))


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


def _to_flac(wav_path: str):
    """
    Demucs writes uncompressed WAV, so a 10-minute stem is ~105MB. FLAC
    is lossless - the VPS decodes it back to bit-identical PCM - and
    roughly halves what crosses the wire.

    Returns (path_to_upload, encoding). A failed encode is NOT fatal:
    the GPU work is already done and paid for, so falling back to the
    original WAV costs bandwidth but still delivers the stem.
    """
    flac_path = f"{wav_path}.flac"
    try:
        result = subprocess.run(
            ["ffmpeg", "-y", "-v", "error", "-i", wav_path,
             "-c:a", "flac", "-compression_level", str(FLAC_COMPRESSION_LEVEL), flac_path],
            capture_output=True, text=True, timeout=FLAC_ENCODE_TIMEOUT_SECONDS,
        )
    except Exception as e:
        print(f"[TRANSFER] FLAC encode failed ({e}) - uploading WAV", flush=True)
        return wav_path, "wav"

    if result.returncode != 0 or not os.path.exists(flac_path) or os.path.getsize(flac_path) == 0:
        print(f"[TRANSFER] FLAC encode failed ({result.stderr[:200]}) - uploading WAV", flush=True)
        try:
            os.remove(flac_path)
        except Exception:
            pass
        return wav_path, "wav"

    before = os.path.getsize(wav_path) / (1024 * 1024)
    after = os.path.getsize(flac_path) / (1024 * 1024)
    print(f"[TRANSFER] FLAC {before:.1f}MB -> {after:.1f}MB", flush=True)
    return flac_path, "flac"


def _upload_result(job_id: str, name: str, file_path: str) -> None:
    """
    POSTs one finished stem straight to the VPS, FLAC-encoded. Streams
    the file from disk rather than reading it whole into memory.

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

    upload_path, encoding = _to_flac(file_path)
    headers = {
        "Authorization": f"Bearer {GPU_SHARED_SECRET}",
        "Content-Type": "audio/flac" if encoding == "flac" else "audio/wav",
        "X-Stem-Encoding": encoding,
    }

    try:
        last_error = None
        for attempt in range(1, _UPLOAD_MAX_ATTEMPTS + 1):
            try:
                with open(upload_path, "rb") as f:
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
    finally:
        if upload_path != file_path:
            try:
                os.remove(upload_path)
            except Exception:
                pass


class _RetryTransfer(Exception):
    """Internal: a transport-level failure worth another attempt. Never
    escapes _download_input - it is caught by the same handler as any
    other transport exception there."""


MIN_DURATION_SECONDS = 3.0


def _normalise_input(input_path: str, work_dir: str, clip_start=None, clip_seconds=None) -> str:
    """Re-encode whatever the user uploaded into plain stereo 44.1k PCM.

    Demucs crashes with an opaque AssertionError in reflect padding when
    fed audio that decodes to NaN samples or exotic layouts. One cheap
    CPU transcode up front turns every input into the one shape the
    model was trained on, and turns undecodable files into a clean,
    user-facing error instead of a GPU-side traceback.
    """
    clean_path = os.path.join(work_dir, "input_clean.wav")
    cmd = ["ffmpeg", "-y", "-v", "error"]
    if clip_seconds is not None:
        cmd += ["-ss", f"{clip_start:.3f}", "-t", f"{clip_seconds:.3f}"]
    cmd += ["-i", input_path, "-ac", "2", "-ar", "44100", "-c:a", "pcm_s16le", clean_path]
    result = subprocess.run(cmd, capture_output=True, text=True)
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
    print(f"[TIMING] demucs {model}: {gpu_seconds:.1f}s", flush=True)

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


def _split_other_six(other_path: str, work_dir: str, overlap: float):
    """Guitar and piano from an htdemucs_6s pass on the "other" stem; the
    new "other" is what is left, so every stem still sums to the mix."""
    import numpy as np
    import soundfile as sf

    six_dir, seconds = _run_demucs_gpu(other_path, work_dir, ROFORMER_STEMS_SECOND_STAGE_6, overlap,
                                       two_stems=False)
    other, sr = sf.read(other_path, dtype="float32", always_2d=True)
    parts = {}
    for name in ("guitar", "piano"):
        data, part_sr = sf.read(os.path.join(six_dir, f"{name}.wav"), dtype="float32", always_2d=True)
        if part_sr != sr:
            raise RuntimeError(f"htdemucs_6s {name} sample rate {part_sr} != {sr}")
        parts[name] = data
    frames = min(len(other), *(len(v) for v in parts.values()))
    rest = other[:frames] - parts["guitar"][:frames] - parts["piano"][:frames]
    out = {}
    for name, data in (("guitar", parts["guitar"][:frames]), ("piano", parts["piano"][:frames]),
                       ("other", rest)):
        path = os.path.join(work_dir, f"six_{name}.wav")
        sf.write(path, np.clip(data, -1.0, 1.0), sr, subtype="PCM_16")
        out[name] = path
    return out, seconds


def handler(job):
    inp = job.get("input") or {}

    task = inp.get("task")
    job_id = inp.get("job_id")
    filename = inp.get("filename", "input.wav")
    model = inp.get("model", "htdemucs")
    overlap = float(inp.get("overlap", 0.25))
    max_duration_seconds = int(inp.get("max_duration_seconds", 600))
    vocal_options = inp.get("vocal_options") or []
    try:
        stem_count = int(inp.get("stem_count", 4))
        clip_seconds = inp.get("clip_seconds")
        clip_seconds = float(clip_seconds) if clip_seconds is not None else None
        clip_start = float(inp.get("clip_start", 0) or 0)
    except (TypeError, ValueError):
        return {"error": "Invalid stem_count or clip parameters."}

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
    if not isinstance(vocal_options, list) or any(o not in ALLOWED_VOCAL_OPTIONS for o in vocal_options):
        return {"error": f"vocal_options must be a list drawn from {ALLOWED_VOCAL_OPTIONS}."}
    if vocal_options and model != "melband_roformer":
        return {"error": "vocal_options require the melband_roformer model."}
    if stem_count not in ALLOWED_STEM_COUNTS:
        return {"error": f"stem_count must be one of {ALLOWED_STEM_COUNTS}."}
    if stem_count == 6 and (task != "stems" or model != "melband_roformer"):
        return {"error": "stem_count 6 is only for task 'stems' on melband_roformer (use htdemucs_6s for Standard)."}
    if clip_seconds is not None and not (MIN_CLIP_SECONDS <= clip_seconds <= MAX_CLIP_SECONDS):
        return {"error": f"clip_seconds must be between {MIN_CLIP_SECONDS:.0f} and {MAX_CLIP_SECONDS:.0f}."}
    if clip_start < 0:
        return {"error": "clip_start cannot be negative."}

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

        if clip_seconds is not None:
            clip_seconds = min(clip_seconds, duration)
            clip_start = max(0.0, min(clip_start, duration - clip_seconds))
        elif duration > max_duration_seconds:
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
            clean_path = _normalise_input(input_path, work_dir, clip_start, clip_seconds)
        except ValueError as e:
            return {"error": str(e)}
        except Exception as e:
            return {"error": f"Could not prepare the audio for separation: {e}"}

        try:
            if model == "melband_roformer":
                if STUDIO_ENGINE == "sw":
                    sources, vocals_path, gpu_seconds = _studio_sw_sources(
                        clean_path, work_dir, task, stem_count,
                    )
                else:
                    sources, vocals_path, gpu_seconds = _studio_legacy_sources(
                        clean_path, work_dir, task, stem_count, overlap,
                    )
                if vocal_options:
                    extra, extra_seconds = _run_vocal_options(vocals_path, work_dir, vocal_options)
                    gpu_seconds += extra_seconds
                    sources.update(extra)
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

        result = {
            "uploaded_stems": uploaded,
            "duration_seconds": clip_seconds if clip_seconds is not None else duration,
            "gpu_seconds": gpu_seconds,
        }
        if model == "melband_roformer":
            result["studio_engine"] = STUDIO_ENGINE
            if STUDIO_ENGINE == "sw":
                result["sw_quality"] = SW_QUALITY
        if clip_seconds is not None:
            result["clip"] = {"start": clip_start, "seconds": clip_seconds, "source_duration": duration}
        return result
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    print(f"[CONFIG] Studio engine: {STUDIO_ENGINE}, SW quality: {SW_QUALITY}", flush=True)
    runpod.serverless.start({"handler": handler})