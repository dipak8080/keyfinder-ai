"""
gpu-worker-piano/handler.py - The RunPod side of the piano sheet-music engine.

WHERE THIS FITS: piano_gpu.py on the VPS is the client. This file is the
worker. runpod_client.py carries the job between them and knows nothing about
MIDI - identical to how it serves Demucs, Whisper and YourMT3.

Modelled directly on gpu-worker-mt3/handler.py: one model loaded once at
container start, one file fetched per job over HTTP via a one-time token,
timing reported back under "_gpu" in the exact shape credits/metering.py
already reads. Read the two side by side; the only real difference is the
model.

--------------------------------------------------------------------------
WHY TRANSKUN, AND WHY IT IS A SEPARATE WORKER FROM YourMT3

YourMT3 (gpu-worker-mt3) is a multi-instrument generalist - strong on full
mixes, weaker on clean solo piano detail. Transkun is a piano SPECIALIST
(event-based Neural Semi-CRF, SOTA on solo-piano benchmarks) and captures
note onsets/offsets and velocity precisely. Piano is the overwhelming
majority of sheet-music demand, so the piano route uses the specialist and
everything else stays on YourMT3. The VPS picks per instrument
(runner._default_transcribe); this worker only ever sees piano audio.

Transkun has NO tunable parameters worth exposing and ships its own
checkpoint inside the pip wheel (pretrained/2.0.pt), so unlike the MT3 image
there is no filter plumbing and no build-time checkpoint download.

--------------------------------------------------------------------------
OUTPUT COMES BACK IN THE JOB PAYLOAD

Like MT3 and unlike Demucs: MIDI is kilobytes, three orders of magnitude
under RunPod's 10MB payload cap, so it is base64'd into the response rather
than POSTed back over a second channel. The INPUT still travels over HTTP,
because audio is audio - same one-time-token fetch as every other worker.
--------------------------------------------------------------------------
"""

import base64
import os
import tempfile
import time
import traceback

import numpy as np
import pretty_midi
import requests
import runpod

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# cuda on the worker; overridable only for local CPU smoke tests.
DEVICE = os.environ.get("PIANO_DEVICE", "cuda")

# Hard ceiling on one job. The VPS enforces its own duration cap before
# submitting (MAX_PIANO_DURATION_SECONDS), so reaching this means the two
# caps have drifted - a corrupt file that decodes to hours, or a runaway
# model. RunPod bills until the job ends, so an unbounded run is an
# unbounded bill.
MAX_SECONDS = int(os.environ.get("PIANO_MAX_SECONDS", "900"))

_HTTP_TIMEOUT = 120


# ---------------------------------------------------------------------------
# Model warm-up
# ---------------------------------------------------------------------------
# Transkun's CLI reloads the model on every invocation; a warm worker must
# not. Loading here, at module scope, means the ~seconds of checkpoint load
# happen once during container init instead of being billed to (and slowing
# down) the first real request of every cold start. Replicates the exact
# load sequence transkun.transcribe.main uses.
#
# A failed warm-up must NOT kill the container: RunPod would restart it, fail
# again, and bill the loop. Better to come up, report MODEL_NOT_LOADED on
# every job, and let the VPS surface a clean 503.
MODEL = None
TARGET_SR = 44100
_MODEL_READY = False

_load_started = time.monotonic()
try:
    import torch
    import moduleconf
    import pkg_resources
    from transkun.Data import writeMidi as _write_midi

    _WEIGHT = pkg_resources.resource_filename("transkun", "pretrained/2.0.pt")
    _CONF = pkg_resources.resource_filename("transkun", "pretrained/2.0.conf")

    _conf_manager = moduleconf.parseFromFile(_CONF)
    _TransKun = _conf_manager["Model"].module.TransKun
    _model_conf = _conf_manager["Model"].config

    _checkpoint = torch.load(_WEIGHT, map_location=DEVICE)
    MODEL = _TransKun(conf=_model_conf).to(DEVICE)
    _state = _checkpoint["best_state_dict"] if "best_state_dict" in _checkpoint else _checkpoint["state_dict"]
    MODEL.load_state_dict(_state, strict=False)
    MODEL.eval()
    torch.set_grad_enabled(False)

    TARGET_SR = int(MODEL.fs)

    # Two seconds of silence forces the full inference path without shipping
    # sample audio. (frames, channels) float32 is exactly the shape
    # model.transcribe expects - it transposes internally.
    _warm = np.zeros((TARGET_SR * 2, 1), dtype=np.float32)
    with torch.no_grad():
        MODEL.transcribe(torch.from_numpy(_warm).to(DEVICE))

    print(
        f"[PIANO_GPU] Transkun warm in {time.monotonic() - _load_started:.1f}s "
        f"(fs={TARGET_SR}, device={DEVICE})",
        flush=True,
    )
    _MODEL_READY = True
except Exception as exc:  # noqa: BLE001
    print(f"[PIANO_GPU] WARM-UP FAILED: {exc}", flush=True)
    traceback.print_exc()
    MODEL = None
    _MODEL_READY = False


# ---------------------------------------------------------------------------
# Input fetch (identical contract to the MT3 / Whisper workers)
# ---------------------------------------------------------------------------

def _fetch_input(vps_base_url: str, token: str, secret: str, dest_path: str) -> int:
    """Pull the audio the VPS registered against this one-time token.

    Authorization: Bearer, matching gpu_internal_routes._check_secret() on
    the VPS (constant-time compare, "Bearer " prefix required). Streamed to
    disk rather than read whole: the token points at an arbitrary user
    upload, and reading it into memory is one large file away from an OOM
    that would look like a model failure.
    """
    url = f"{vps_base_url.rstrip('/')}/internal/gpu/input/{token}"
    headers = {"Authorization": f"Bearer {secret}"}

    written = 0
    with requests.get(url, headers=headers, stream=True, timeout=_HTTP_TIMEOUT) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    written += len(chunk)
    return written


# ---------------------------------------------------------------------------
# Audio + inference
# ---------------------------------------------------------------------------

def _read_audio(path: str):
    """Decode to (sample_rate, float32 ndarray shaped [frames, channels]).

    Exactly transkun.transcribe.readAudio: pydub decodes (so ffmpeg covers
    m4a/opus/webm/etc.), samples are reshaped per channel and normalised by
    2**15. model.transcribe transposes internally, so [frames, channels] is
    the shape it wants.
    """
    import pydub

    audio = pydub.AudioSegment.from_file(path)
    y = np.array(audio.get_array_of_samples())
    y = y.reshape(-1, audio.channels)
    y = np.float32(y) / 2 ** 15
    return audio.frame_rate, y


def _run_transkun(y: np.ndarray, fs: int) -> "pretty_midi.PrettyMIDI":
    """Resample to the model rate if needed, run Transkun, return a
    pretty_midi.PrettyMIDI (writeMidi already returns one).
    """
    import torch

    if fs != TARGET_SR:
        import soxr
        y = soxr.resample(y, fs, TARGET_SR)

    y = np.ascontiguousarray(y, dtype=np.float32)
    with torch.no_grad():
        notes = MODEL.transcribe(torch.from_numpy(y).to(DEVICE))
    return _write_midi(notes)


def _summarise(midi: "pretty_midi.PrettyMIDI") -> dict:
    """Track/note counts and pitch range - the only feedback for a tool
    whose output cannot be previewed as audio. Same shape as the MT3 worker
    so the VPS and frontend need no per-engine special case.
    """
    tracks = []
    for inst in midi.instruments:
        if not inst.notes:
            continue
        tracks.append({
            "program": int(inst.program),
            "is_drum": bool(inst.is_drum),
            "name": pretty_midi.program_to_instrument_name(inst.program)
                    if not inst.is_drum else "Drums",
            "notes": len(inst.notes),
            "low": int(min(n.pitch for n in inst.notes)),
            "high": int(max(n.pitch for n in inst.notes)),
        })
    return {
        "duration_seconds": round(midi.get_end_time(), 2),
        "track_count": len(tracks),
        "note_count": sum(t["notes"] for t in tracks),
        "tracks": tracks,
    }


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------

def handler(job):
    """One piano transcription.

    Input:
        vps_base_url  str  where to fetch the audio from
        token         str  one-time transfer token
        secret        str  shared secret for the fetch
        suffix        str  file extension, e.g. ".wav"

    Returns {"error": CODE} or {"midi_b64": ..., <stats>, "_gpu": {...}}.
    Errors are returned as data, not raised: a raised exception becomes a
    RunPod FAILED status with a stringified traceback the VPS can only
    report as "it broke", where a named code maps to a message a user can
    act on.
    """
    started = time.monotonic()

    if not _MODEL_READY:
        return {"error": "MODEL_NOT_LOADED"}

    payload = job.get("input") or {}
    vps_base_url = payload.get("vps_base_url")
    token = payload.get("token")
    secret = payload.get("secret")
    if not (vps_base_url and token and secret):
        return {"error": "MISSING_TRANSFER_PARAMS"}

    suffix = payload.get("suffix") or ".wav"

    tmp_dir = tempfile.mkdtemp(prefix="piano_")
    audio_path = os.path.join(tmp_dir, f"input{suffix}")

    try:
        # ---------- fetch ----------
        fetch_started = time.monotonic()
        try:
            size = _fetch_input(vps_base_url, token, secret, audio_path)
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else 0
            print(f"[PIANO_GPU] input fetch failed ({code})", flush=True)
            return {"error": "INPUT_FETCH_FAILED"}
        except Exception as e:  # noqa: BLE001
            print(f"[PIANO_GPU] input fetch error: {e}", flush=True)
            return {"error": "INPUT_FETCH_FAILED"}
        fetch_seconds = time.monotonic() - fetch_started

        if size == 0:
            return {"error": "EMPTY_INPUT"}

        # ---------- decode ----------
        try:
            fs, y = _read_audio(audio_path)
        except Exception as e:  # noqa: BLE001
            print(f"[PIANO_GPU] could not decode audio: {e}", flush=True)
            return {"error": "EMPTY_INPUT"}

        n_frames = int(y.shape[0]) if y.ndim else 0
        duration = n_frames / float(fs) if fs and n_frames else 0.0

        if duration <= 0:
            return {"error": "EMPTY_INPUT"}
        if duration > MAX_SECONDS:
            # The VPS should have caught this; reaching it means the caps
            # have drifted. Loud enough to notice.
            print(f"[PIANO_GPU] REJECTED {duration:.1f}s > MAX_SECONDS={MAX_SECONDS}", flush=True)
            return {"error": "INPUT_TOO_LONG"}

        # ---------- inference ----------
        infer_started = time.monotonic()
        try:
            midi = _run_transkun(y, fs)
        except Exception as e:  # noqa: BLE001
            print(f"[PIANO_GPU] inference failed: {e}", flush=True)
            traceback.print_exc()
            return {"error": "TRANSCRIPTION_FAILED"}
        infer_seconds = time.monotonic() - infer_started

        if not midi.instruments or not any(i.notes for i in midi.instruments):
            return {"error": "NO_NOTES_DETECTED"}

        # ---------- serialise ----------
        # Written to a path and read back rather than via BytesIO: PrettyMIDI's
        # file-object support has varied across versions, and a silently
        # truncated write would produce a .mid that opens as an empty project
        # in a DAW - a worse failure than an error, because it looks like it
        # worked.
        out_path = os.path.join(tmp_dir, "out.mid")
        try:
            midi.write(out_path)
        except Exception as e:  # noqa: BLE001
            print(f"[PIANO_GPU] could not serialise MIDI: {e}", flush=True)
            traceback.print_exc()
            return {"error": "TRANSCRIPTION_FAILED"}
        with open(out_path, "rb") as f:
            midi_bytes = f.read()

        stats = _summarise(midi)
        total = time.monotonic() - started

        print(
            f"[PIANO_GPU] ok {duration:.1f}s audio -> {stats['note_count']} notes, "
            f"{len(midi_bytes)}B (fetch {fetch_seconds:.1f}s, infer {infer_seconds:.1f}s)",
            flush=True,
        )

        return {
            "midi_b64": base64.b64encode(midi_bytes).decode("ascii"),
            "input_seconds": round(duration, 2),
            "notes_dropped_by_filter": 0,
            **stats,
            # Same key/shape the MT3 and Whisper workers use, so
            # credits/metering.py records GPU seconds through the identical
            # path with no per-tool special case.
            "_gpu": {
                "fetch_seconds": round(fetch_seconds, 2),
                "infer_seconds": round(infer_seconds, 2),
                "total_seconds": round(total, 2),
                "rtf": round(duration / infer_seconds, 2) if infer_seconds > 0 else None,
            },
        }

    except Exception as e:  # noqa: BLE001
        print(f"[PIANO_GPU] unexpected failure: {e}", flush=True)
        traceback.print_exc()
        return {"error": "TRANSCRIPTION_FAILED"}
    finally:
        # Best-effort temp cleanup; the container is ephemeral but a
        # long-lived warm worker processes many jobs.
        try:
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    runpod.serverless.start({"handler": handler})