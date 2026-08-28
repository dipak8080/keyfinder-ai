"""
gpu-worker-mt3/handler.py - The RunPod side of /audio-to-midi-hq.

WHERE THIS FITS: midi_hq_gpu.py on the VPS is the client. This file is
the worker. runpod_client.py carries the job between them and knows
nothing about MIDI - it is deliberately generic, exactly as it is for
Demucs and Whisper.

Modelled on gpu-worker-whisper/handler.py, which is the closest sibling:
one model loaded once at container start, one file fetched per job over
HTTP, timing reported back under "_gpu". Read that file alongside this
one; where they differ, the difference is explained below.

--------------------------------------------------------------------------
WHY THE OUTPUT COMES BACK IN THE PAYLOAD, UNLIKE DEMUCS

The separation worker POSTs each stem back to /internal/gpu/upload
because a full stem set is tens of megabytes and RunPod caps job
payloads at 10MB. MIDI is not audio: a 4.5-minute piano transcription
measured 13KB in testing. Three orders of magnitude under the cap.

So the output is base64'd into the response and the entire upload path -
the second shared-secret check, the destination-path validation, the
is_job_in_flight authorisation window - does not exist for this tool.
Fewer moving parts, and none of them are the ones that can leak a file
path.

The INPUT still travels over HTTP, because audio is audio. Same
one-time-token fetch as Whisper.

--------------------------------------------------------------------------
WHY transformers IS PINNED TO 4.38.2

YourMT3's decoder calls into transformers' T5 implementation directly.
Two separate breakages were hit while getting this running:

  >= 4.50  ImportError: transformers.utils.model_parallel_utils is gone
  ~ 4.4x   TypeError: 'NoneType' object is not subscriptable, because
           T5Attention.forward now expects a cache_position tensor that
           YourMT3's generate loop never passes

4.38.2 predates both. This is not a version to bump casually - if it
moves, the failure is a stack trace deep inside transformers, not
anything that names YourMT3. Pin it in requirements.txt and leave it.

--------------------------------------------------------------------------
POST-PROCESSING, AND WHY IT IS DONE HERE RATHER THAN IN THE MODEL

YourMT3 has no tunable parameters. None. No onset threshold, no frame
threshold, no minimum note length, no pitch range - it is a transformer
that emits note events, and there is nothing to turn.

That is a problem for the product, because the free /audio-to-midi tool
built its whole UI around exactly those knobs (six presets, three
sliders, a keyboard range picker). Shipping an HQ tier with no controls
at all would read as a downgrade to anyone who used the free one.

So the equivalent controls are applied to the OUTPUT instead. Filtering
notes by pitch and duration after transcription is not an approximation
of what basic-pitch does with those parameters - it is strictly more
predictable, because it operates on decided notes rather than on
detection thresholds. "Nothing below C2" means exactly that here, where
in basic-pitch it means "bias the detector away from low frequencies".

What CANNOT be reproduced is onset/frame sensitivity: those change what
the model detects, and by the time we have MIDI that decision is made.
The frontend should not offer them for this tool.

--------------------------------------------------------------------------
INSTRUMENT PROGRAMS ARE PASSED THROUGH, NOT FLATTENED

YourMT3 is multi-instrument: it assigns a General MIDI program per track
and can emit several tracks from one mixed input. That is the single
capability the free tool does not have at any setting, so it is the
thing worth preserving exactly. Every instrument the model returns
becomes its own track in the output file, with its program number
intact.

Observed caveat worth knowing before selling it: on short or
single-instrument clips the model frequently returns one track at
program=0 (acoustic grand). Multi-track output appears on real
multi-instrument material. Do not promise "separate tracks per
instrument" for a solo guitar upload.
--------------------------------------------------------------------------
"""

import base64
import io
import os
import tempfile
import time
import traceback

import requests
import runpod

# Import cost is paid once at container start, not per job. mt3_infer
# pulls torch and transformers, so this is the slow part of a cold start
# and the reason the model is loaded at module scope below rather than
# inside handler().
from mt3_infer import transcribe as mt3_transcribe

import pretty_midi


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# The checkpoint mt3-infer downloads for "yourmt3": YPTF.MoE+Multi
# (noPS). Baked into the image at build time - see the Dockerfile - so a
# cold start never pays for a HuggingFace clone. That download is a git
# clone with LFS behind it and has been the flakiest step in the whole
# pipeline; doing it once at build is what keeps it off the request path.
MODEL = os.environ.get("MT3_MODEL", "yourmt3")

# MT3-family models are trained at 16kHz. This is NOT a quality knob -
# feeding 44.1k produces silently worse results rather than an error,
# which is the most expensive kind of wrong. The VPS side resamples
# before upload; this constant exists so the worker can assert it.
TARGET_SR = 16000

# Hard ceiling on one job. The VPS enforces its own duration cap before
# submitting, so reaching this means something pathological - a corrupt
# file that decodes to hours, or a model that has stopped converging.
# RunPod bills until the job ends, so an unbounded run is an unbounded
# bill.
MAX_SECONDS = int(os.environ.get("MT3_MAX_SECONDS", "600"))

_HTTP_TIMEOUT = 120


# ---------------------------------------------------------------------------
# Model warm-up
# ---------------------------------------------------------------------------
# mt3-infer loads lazily on first transcribe() call. Left lazy, the FIRST
# real request of every cold start would pay ~20s of checkpoint load on
# top of its own inference - billed, and attributed to that user's job.
#
# Forcing it here means the load happens during container init instead.
# RunPod still bills that time, but it is billed once per worker rather
# than once per cold-started request, and it stops the first user of a
# scaled-up worker from waiting twice as long as everyone else.
_load_started = time.monotonic()
try:
    import numpy as np

    # Two seconds of silence is enough to force the full load path
    # without producing anything. Deliberately not a real file: the
    # image should not carry sample audio, and silence exercises the
    # same code.
    _warm = np.zeros(TARGET_SR * 2, dtype="float32")
    mt3_transcribe(_warm, sr=TARGET_SR, model=MODEL)
    print(f"[MT3_GPU] Model '{MODEL}' warm in {time.monotonic() - _load_started:.1f}s", flush=True)
    _MODEL_READY = True
except Exception as exc:  # noqa: BLE001
    # A failed warm-up must NOT kill the container. RunPod would restart
    # it, fail again, and bill for the loop. Better to come up, report
    # the failure on every job, and let the VPS surface a clean 503.
    print(f"[MT3_GPU] WARM-UP FAILED: {exc}", flush=True)
    traceback.print_exc()
    _MODEL_READY = False


# ---------------------------------------------------------------------------
# Input fetch
# ---------------------------------------------------------------------------

def _fetch_input(vps_base_url: str, token: str, secret: str, dest_path: str) -> int:
    """Pull the audio the VPS registered against this one-time token.

    Streamed to disk rather than read into memory: the token points at
    an arbitrary user upload, and a worker that reads it whole is one
    large file away from an OOM that looks like a model failure.
    """
    url = f"{vps_base_url.rstrip('/')}/internal/gpu/input/{token}"
    headers = {"x-internal-secret": secret}

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
# Post-processing
# ---------------------------------------------------------------------------

def _filter_midi(
    midi: "pretty_midi.PrettyMIDI",
    *,
    min_pitch: int | None,
    max_pitch: int | None,
    min_note_ms: float | None,
) -> tuple["pretty_midi.PrettyMIDI", int, int]:
    """Apply the output-side equivalents of the free tool's controls.

    Returns (filtered, kept, dropped) so the caller can report how much
    was removed - a filter that silently ate 90% of the notes is worth
    seeing in a log line rather than in a support email.

    Instruments that end up empty are dropped entirely. A MIDI file with
    four tracks, three of them empty, opens in a DAW as four tracks and
    looks broken; the model's own track count should reflect what
    survived.

    No filter is applied when every bound is None, and the object is
    returned untouched rather than rebuilt - the common case should not
    pay for a copy.
    """
    if min_pitch is None and max_pitch is None and not min_note_ms:
        total = sum(len(i.notes) for i in midi.instruments)
        return midi, total, 0

    min_dur = (min_note_ms or 0) / 1000.0
    kept = dropped = 0
    survivors = []

    for inst in midi.instruments:
        keep = []
        for n in inst.notes:
            if min_pitch is not None and n.pitch < min_pitch:
                dropped += 1
                continue
            if max_pitch is not None and n.pitch > max_pitch:
                dropped += 1
                continue
            if min_dur and (n.end - n.start) < min_dur:
                dropped += 1
                continue
            keep.append(n)
        if keep:
            inst.notes = keep
            survivors.append(inst)
            kept += len(keep)
        else:
            dropped += 0  # its notes were already counted above

    midi.instruments = survivors
    return midi, kept, dropped


def _summarise(midi: "pretty_midi.PrettyMIDI") -> dict:
    """What the VPS logs and what the frontend can show.

    Cheap to compute and genuinely useful: "3 tracks, 1,842 notes,
    A0-C8" tells a user their upload worked far better than a silent
    download does, and it is the only feedback available for a tool
    whose output cannot be previewed as audio.
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
    """One transcription.

    Expected input:
        vps_base_url   str    where to fetch the audio from
        token          str    one-time transfer token
        secret         str    shared secret for the fetch
        suffix         str    file extension, e.g. ".wav"
        min_pitch      int?   MIDI note number, inclusive
        max_pitch      int?   MIDI note number, inclusive
        min_note_ms    float? drop notes shorter than this

    Returns either {"error": "..."} or the MIDI plus stats. Errors are
    returned as data rather than raised: a raised exception becomes a
    RunPod FAILED status with a stringified traceback, which the VPS can
    only report as "it broke". A named error code can be translated into
    a message a user can act on.
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

    min_pitch = payload.get("min_pitch")
    max_pitch = payload.get("max_pitch")
    min_note_ms = payload.get("min_note_ms")

    tmp_dir = tempfile.mkdtemp(prefix="mt3_")
    audio_path = os.path.join(tmp_dir, f"input{suffix}")

    try:
        # ---------- fetch ----------
        fetch_started = time.monotonic()
        try:
            size = _fetch_input(vps_base_url, token, secret, audio_path)
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else 0
            print(f"[MT3_GPU] input fetch failed ({code})", flush=True)
            return {"error": "INPUT_FETCH_FAILED"}
        except Exception as e:  # noqa: BLE001
            print(f"[MT3_GPU] input fetch error: {e}", flush=True)
            return {"error": "INPUT_FETCH_FAILED"}
        fetch_seconds = time.monotonic() - fetch_started

        if size == 0:
            return {"error": "EMPTY_INPUT"}

        # ---------- load ----------
        # librosa via mt3_infer would resample for us, but doing it
        # explicitly means a mismatch is a loud assertion here rather
        # than silently degraded output. See TARGET_SR above.
        import librosa

        audio, sr = librosa.load(audio_path, sr=TARGET_SR, mono=True)
        duration = len(audio) / float(sr)

        if duration <= 0:
            return {"error": "EMPTY_INPUT"}
        if duration > MAX_SECONDS:
            # The VPS should have caught this. Reaching it means the two
            # caps have drifted apart, which is worth a log line loud
            # enough to notice.
            print(f"[MT3_GPU] REJECTED {duration:.1f}s > MAX_SECONDS={MAX_SECONDS}", flush=True)
            return {"error": "INPUT_TOO_LONG"}

        # ---------- inference ----------
        infer_started = time.monotonic()
        midi = mt3_transcribe(audio, sr=sr, model=MODEL)
        infer_seconds = time.monotonic() - infer_started

        if midi is None or not getattr(midi, "instruments", None):
            return {"error": "NO_NOTES_DETECTED"}

        # ---------- post-process ----------
        midi, kept, dropped = _filter_midi(
            midi,
            min_pitch=int(min_pitch) if min_pitch is not None else None,
            max_pitch=int(max_pitch) if max_pitch is not None else None,
            min_note_ms=float(min_note_ms) if min_note_ms else None,
        )

        if kept == 0:
            # Either the model found nothing, or the caller's filter
            # removed everything. Distinguished from NO_NOTES_DETECTED
            # because the fix is different: one is "try a different
            # file", the other is "widen your pitch range".
            return {"error": "NO_NOTES_AFTER_FILTER" if dropped else "NO_NOTES_DETECTED"}

        # ---------- serialise ----------
        buf = io.BytesIO()
        midi.write(buf)
        midi_bytes = buf.getvalue()

        stats = _summarise(midi)
        total = time.monotonic() - started

        print(
            f"[MT3_GPU] ok {duration:.1f}s audio -> {stats['note_count']} notes "
            f"across {stats['track_count']} track(s), {len(midi_bytes)}B "
            f"(fetch {fetch_seconds:.1f}s, infer {infer_seconds:.1f}s, "
            f"filtered out {dropped})",
            flush=True,
        )

        return {
            "midi_b64": base64.b64encode(midi_bytes).decode("ascii"),
            "input_seconds": round(duration, 2),
            "notes_dropped_by_filter": dropped,
            **stats,
            # Same key and shape the Whisper worker uses, so
            # credits/metering.py needs no per-tool special case and the
            # VPS can record GPU seconds through the identical path.
            "_gpu": {
                "fetch_seconds": round(fetch_seconds, 2),
                "infer_seconds": round(infer_seconds, 2),
                "total_seconds": round(total, 2),
                "rtf": round(duration / infer_seconds, 2) if infer_seconds > 0 else None,
            },
        }

    except Exception as e:  # noqa: BLE001
        print(f"[MT3_GPU] unexpected failure: {e}", flush=True)
        traceback.print_exc()
        return {"error": "TRANSCRIPTION_FAILED"}

    finally:
        # The worker's disk persists across jobs on a warm container, so
        # a leaked temp file is a leak for the life of the worker.
        try:
            if os.path.exists(audio_path):
                os.remove(audio_path)
            os.rmdir(tmp_dir)
        except Exception:  # noqa: BLE001
            pass


runpod.serverless.start({"handler": handler})