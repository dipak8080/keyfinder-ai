"""
midi_hq_gpu.py - The VPS side of /audio-to-midi-hq.

WHERE THIS FITS: gpu-worker-mt3/handler.py is the RunPod side. This is
the client. runpod_client.py carries the job between them and knows
nothing about MIDI, exactly as it knows nothing about stems or speech.

Closest sibling is speech_to_text_gpu.py, and the resemblance is
deliberate - same one-time transfer token, same register/unregister in a
`finally`, same translation of worker error codes into AudioToolError
messages a user can act on. Read the two together; the differences are
called out below.

--------------------------------------------------------------------------
WHY THIS IS NOT A SECOND BACKEND BEHIND audio_to_midi.py

/audio-to-midi calls the midi-worker sidecar over HTTP on the internal
Docker network. It would be tempting to add a `quality` argument there
and fork inside, the way transcription.py forks between local and GPU.

That would be wrong here, and the reason is not style. transcription.py
forks between two implementations of THE SAME PRODUCT: identical
arguments, identical return shape, identical error wording, and the
caller is not supposed to be able to tell which ran. These two are not
the same product:

  basic-pitch    any instrument, six tunable parameters, one MIDI track
  YourMT3        multi-instrument, ZERO tunable parameters, multi-track

There is no argument list that honestly covers both. onset_threshold and
frame_threshold have no counterpart in YourMT3 - it is a transformer
that emits note events, and there is nothing to turn. Forcing one
signature over both would mean silently ignoring three of the free
tool's parameters on the HQ path, which is the kind of thing that is
discovered by a confused user rather than by a test.

So: separate module, separate route, separate rule key, separate
semaphore. They share the job system and the credits ledger and nothing
else.

--------------------------------------------------------------------------
WHAT SURVIVES OF THE FREE TOOL'S CONTROLS, AND WHAT DOES NOT

The worker applies pitch-range and minimum-note-length filtering to the
OUTPUT MIDI - see _filter_midi in handler.py for why that is more
predictable than the free tool's equivalents rather than an
approximation of them.

So of the free form's six presets, the pitch-range half carries over
exactly and the sensitivity half cannot carry over at all. The frontend
must not offer onset/frame sensitivity on this tool. Presets that only
differ in those two collapse to the same request here.

--------------------------------------------------------------------------
OUTPUT COMES BACK IN THE JOB PAYLOAD

Unlike separation, which has the worker POST stems to
/internal/gpu/upload, the MIDI arrives base64'd in the RunPod response.
A 4.5-minute piano transcription measured 13KB against RunPod's 10MB
payload cap - three orders of magnitude of headroom.

That removes the entire upload path from this tool: no second secret
check, no destination-path validation, no is_job_in_flight window. The
INPUT still travels over HTTP because audio is audio.

--------------------------------------------------------------------------
"""

import base64
import os
import time
import uuid

from config import (
    logger,
    RUNPOD_API_KEY,
    RUNPOD_MT3_ENDPOINT_ID,
    GPU_WORKER_SHARED_SECRET,
    VPS_PUBLIC_BASE_URL,
    MIDI_HQ_TIMEOUT_SECONDS,
    MAX_MIDI_HQ_DURATION_SECONDS,
)
from audio_common import AudioToolError, atomic_write_bytes
from runpod_client import run_worker_job, RunPodJobError
from gpu_internal_routes import register_gpu_input, unregister_gpu_input
from utils import run_blocking, cleanup_file
from audio_to_midi import convert_guitar_to_midi
from separation import run_stem_separation, SeparationError

# --------------------------------------------------------------------------
# INSTRUMENT ROUTING (2026-09-01)
#
# YourMT3 is trained on Slakh/MAESTRO and a little clean acoustic
# GuitarSet. It is strong on piano and full mixes and bad on electric
# guitar - riffs came back as sparse, octave-shifted "piano". So the HQ
# route now picks an engine per instrument:
#
#   auto / piano / mix   YourMT3 on RunPod (unchanged path)
#   guitar               basic-pitch sidecar in guitar mode, optionally
#                        after an htdemucs_6s guitar-stem isolation pass
#
# Same route, same credit, same result shape. "engine" in the stats says
# which one ran.
# --------------------------------------------------------------------------
INSTRUMENTS = ("auto", "piano", "mix", "guitar")
ISOLATION_MODEL = "htdemucs_6s"


# Every error code gpu-worker-mt3/handler.py can return, mapped to what
# the person who uploaded the file should read.
#
# Kept as data rather than an if-chain so adding a worker error code is
# one line here and cannot be forgotten - an unmapped code falls through
# to the generic message below, which is safe but tells the user
# nothing, so the mapping is the thing worth keeping complete.
#
# The distinction between NO_NOTES_DETECTED and NO_NOTES_AFTER_FILTER
# matters and is the reason the worker reports them separately: one
# means "try a different file", the other means "widen your pitch
# range". Collapsing them would send half of those users down the wrong
# path.
_WORKER_ERRORS = {
    "MODEL_NOT_LOADED": (
        "High-quality MIDI transcription is temporarily unavailable. "
        "Please try again later."
    ),
    "INPUT_TOO_LONG": (
        f"Audio is too long for high-quality transcription "
        f"(limit is {MAX_MIDI_HQ_DURATION_SECONDS // 60} minutes)."
    ),
    "EMPTY_INPUT": (
        "That file appears to be empty or unreadable. Please try another file."
    ),
    "NO_NOTES_DETECTED": (
        "No notes were detected in this audio. It may be silent, percussive "
        "only, or too quiet to transcribe."
    ),
    "NO_NOTES_AFTER_FILTER": (
        "Every detected note fell outside your pitch range or was shorter "
        "than your minimum note length. Try widening the range."
    ),
}

# Codes that mean OUR side is broken, not the user's file. Separated so
# they can be logged at error level and still reported to the user with
# the same neutral wording - a caller should never be shown "the shared
# secret is wrong", but an operator should be able to grep for it.
_INTERNAL_ERRORS = {
    "MISSING_TRANSFER_PARAMS",
    "INPUT_FETCH_FAILED",
}

_GENERIC_ERROR = (
    "High-quality MIDI transcription failed. Please try again in a moment."
)


def is_available() -> bool:
    """Whether the HQ path can run at all right now.

    Checked at request time rather than import time because these are
    all env vars: a missing one should degrade THIS endpoint to a clean
    503, not crash the app at boot the way an import-time assertion
    would. Same reasoning as speech_to_text_gpu.is_available().

    VPS_PUBLIC_BASE_URL is the one most likely to be forgotten, and its
    absence is the most confusing: without it the worker has no address
    to fetch audio from, so the failure surfaces on the RunPod side as a
    timeout rather than as anything naming the missing variable.
    """
    return bool(
        RUNPOD_API_KEY
        and RUNPOD_MT3_ENDPOINT_ID
        and GPU_WORKER_SHARED_SECRET
        and VPS_PUBLIC_BASE_URL
    )


def _missing_config() -> str:
    return ", ".join(
        name for name, value in (
            ("RUNPOD_API_KEY", RUNPOD_API_KEY),
            ("RUNPOD_MT3_ENDPOINT_ID", RUNPOD_MT3_ENDPOINT_ID),
            ("GPU_WORKER_SHARED_SECRET", GPU_WORKER_SHARED_SECRET),
            ("VPS_PUBLIC_BASE_URL", VPS_PUBLIC_BASE_URL),
        ) if not value
    )


def _validate_local_input(input_path: str) -> None:
    """Two checks the worker should never have to pay a round trip for.

    Deliberately NOT speech_to_text._validate_input_file, even though it
    does the same job: importing it would pull the whole Whisper module
    into this import graph for two lines of logic, and that module holds
    a resident model singleton. A tool that has nothing to do with
    speech should not be able to affect whether that model loads.
    """
    if not os.path.exists(input_path):
        raise AudioToolError("The uploaded file could not be found. Please try again.")
    if os.path.getsize(input_path) == 0:
        raise AudioToolError("That file is empty. Please upload a valid audio file.")


async def _transcribe_guitar(
    input_path: str,
    output_path: str,
    job_id: str,
    isolate: bool,
    min_pitch: int | None,
    max_pitch: int | None,
    min_note_ms: float | None,
) -> dict:
    started = time.monotonic()
    stem_paths: dict = {}
    source_path = input_path
    isolate_seconds = 0.0

    try:
        if isolate:
            iso_started = time.monotonic()
            try:
                stem_paths = await run_stem_separation(input_path, job_id, model=ISOLATION_MODEL)
            except SeparationError as e:
                logger.error(f"[MIDI_HQ] guitar isolation failed for job {job_id}: {e}")
                raise AudioToolError(
                    "Could not isolate the guitar from this mix. Try again, or upload a solo guitar recording."
                )
            isolate_seconds = time.monotonic() - iso_started
            source_path = stem_paths.get("guitar")
            if not source_path or not os.path.exists(source_path):
                raise AudioToolError("Guitar isolation produced no guitar stem for this file.")

        stats = await run_blocking(
            convert_guitar_to_midi, source_path, output_path,
            min_pitch=min_pitch, max_pitch=max_pitch, min_note_ms=min_note_ms,
        )
    finally:
        for path in stem_paths.values():
            cleanup_file(path)

    stats["engine"] = "basic-pitch-guitar"
    stats["isolated"] = bool(isolate)
    stats.setdefault("notes_dropped_by_filter", 0)
    stats["_gpu"] = {
        "fetch_seconds": 0.0,
        "infer_seconds": round(isolate_seconds, 2),
        "total_seconds": round(time.monotonic() - started, 2),
        "rtf": None,
    }
    logger.info(
        f"[MIDI_HQ] Guitar complete: {stats.get('note_count')} notes, "
        f"{stats.get('duration_seconds')}s, cleanup dropped "
        f"{stats.get('notes_dropped_by_cleanup', 0)} "
        f"(isolated={isolate}, isolate {isolate_seconds:.1f}s)"
    )
    return stats


async def transcribe_to_midi(
    input_path: str,
    output_path: str,
    min_pitch: int | None = None,
    max_pitch: int | None = None,
    min_note_ms: float | None = None,
    instrument: str = "auto",
    isolate: bool = False,
    job_id: str | None = None,
) -> dict:
    """Transcribe input_path on the GPU worker and write MIDI to output_path.

    Returns the worker's stats dict - track/note counts, per-track
    program numbers and pitch ranges, and the "_gpu" timing block. The
    caller writes nothing to disk itself; by the time this returns,
    output_path exists.

    Raises AudioToolError with a user-facing message on every failure
    path. Nothing else escapes: a caller should be able to wrap this in
    one `except AudioToolError` and be complete.

    min_pitch / max_pitch are MIDI note numbers, inclusive. min_note_ms
    drops notes shorter than that. All three are applied by the worker
    to the OUTPUT MIDI - see this module's docstring for why that is
    different from, and more predictable than, the free tool's
    equivalent parameters.
    """
    if not is_available():
        logger.error(
            f"[MIDI_HQ] Rejecting request - missing config: {_missing_config()}"
        )
        raise AudioToolError(
            "High-quality MIDI transcription is temporarily unavailable. "
            "Please try again later."
        )

    _validate_local_input(input_path)

    instrument = (instrument or "auto").lower()
    if instrument not in INSTRUMENTS:
        raise AudioToolError(f"Unknown instrument '{instrument}'.")

    if instrument == "guitar":
        return await _transcribe_guitar(
            input_path, output_path, job_id or uuid.uuid4().hex, isolate,
            min_pitch, max_pitch, min_note_ms,
        )

    # A fresh handle per job, registered immediately before submit and
    # removed in the `finally` below - so the input URL is live for
    # exactly one job and not a second longer.
    #
    # Deliberately NOT the caller's job_id, matching
    # speech_to_text_gpu's reasoning: a transfer token is a handle, not
    # an identity, and an unguessable one stays safe even if a job id
    # leaks into a log or a URL somewhere.
    token = uuid.uuid4().hex
    suffix = os.path.splitext(input_path)[1] or ".wav"

    payload = {
        "vps_base_url": VPS_PUBLIC_BASE_URL,
        "token": token,
        "secret": GPU_WORKER_SHARED_SECRET,
        "suffix": suffix,
    }
    # Omitted entirely rather than sent as null when unset. The worker
    # treats absent as "no filter", and sending an explicit None would
    # work only because of how it happens to read the dict - relying on
    # that is how a future worker change breaks this silently.
    if min_pitch is not None:
        payload["min_pitch"] = int(min_pitch)
    if max_pitch is not None:
        payload["max_pitch"] = int(max_pitch)
    if min_note_ms:
        payload["min_note_ms"] = float(min_note_ms)

    register_gpu_input(token, input_path)
    logger.info(
        f"[MIDI_HQ] Submitting token={token[:8]}... "
        f"({os.path.basename(input_path)}, "
        f"pitch={min_pitch or '-'}..{max_pitch or '-'}, "
        f"min_note={min_note_ms or '-'}ms)"
    )

    try:
        result = await run_worker_job(
            RUNPOD_MT3_ENDPOINT_ID,
            RUNPOD_API_KEY,
            payload,
            MIDI_HQ_TIMEOUT_SECONDS,
        )

    except RunPodJobError as e:
        # run_worker_job has already cancelled the remote job on every
        # give-up path, so nothing is still billing by the time this
        # runs. See runpod_client.py's hardening note 1.
        logger.error(f"[MIDI_HQ] RunPod job failed: {e}")
        raise AudioToolError(_GENERIC_ERROR)

    except Exception as e:  # noqa: BLE001
        logger.error(
            f"[MIDI_HQ] Unexpected failure talking to the GPU worker: "
            f"{e.__class__.__name__}: {e}",
            exc_info=True,
        )
        raise AudioToolError(_GENERIC_ERROR)

    finally:
        # ALWAYS, on every path including CancelledError. A leaked
        # registry entry keeps a URL serving that file for the life of
        # the process - the registry has no TTL sweep, precisely because
        # its entries are supposed to be scoped to one request.
        unregister_gpu_input(token)

    # ---------- worker-reported errors ----------
    error = result.get("error") if isinstance(result, dict) else None
    if error:
        if error in _INTERNAL_ERRORS:
            # Ours, not theirs. Logged loudly with the code intact so it
            # is greppable, reported neutrally so a caller learns
            # nothing about our internals.
            logger.error(f"[MIDI_HQ] Worker reported an internal failure: {error}")
            raise AudioToolError(_GENERIC_ERROR)

        message = _WORKER_ERRORS.get(error)
        if message is None:
            # An unmapped code. Worth a warning rather than silence: it
            # means the worker gained an error this file does not know
            # about, and the user got a message that told them nothing.
            logger.warning(f"[MIDI_HQ] Unmapped worker error code: {error}")
            raise AudioToolError(_GENERIC_ERROR)
        raise AudioToolError(message)

    # ---------- write the MIDI ----------
    midi_b64 = result.get("midi_b64") if isinstance(result, dict) else None
    if not midi_b64:
        logger.error(f"[MIDI_HQ] Malformed worker result (no midi_b64): {str(result)[:300]}")
        raise AudioToolError(_GENERIC_ERROR)

    try:
        midi_bytes = base64.b64decode(midi_b64)
    except Exception as e:  # noqa: BLE001
        logger.error(f"[MIDI_HQ] Could not decode worker MIDI payload: {e}")
        raise AudioToolError(_GENERIC_ERROR)

    if not midi_bytes:
        logger.error("[MIDI_HQ] Worker returned an empty MIDI payload")
        raise AudioToolError(_GENERIC_ERROR)

    # Written whole rather than streamed: this is kilobytes, and a
    # partial write would leave a corrupt .mid that opens as an empty
    # project in a DAW - a much worse failure than an error message,
    # because it looks like the tool worked.
    atomic_write_bytes(output_path, midi_bytes)

    stats = {k: v for k, v in result.items() if k != "midi_b64"}
    stats["engine"] = "yourmt3"
    stats["isolated"] = False
    gpu = stats.get("_gpu") or {}

    logger.info(
        f"[MIDI_HQ] Complete: {stats.get('note_count')} notes across "
        f"{stats.get('track_count')} track(s), "
        f"{stats.get('duration_seconds')}s output, {len(midi_bytes)}B "
        f"(input {stats.get('input_seconds')}s, "
        f"infer {gpu.get('infer_seconds')}s, rtf {gpu.get('rtf')}x, "
        f"filtered out {stats.get('notes_dropped_by_filter', 0)})"
    )

    return stats


def get_backend_info() -> dict:
    """Diagnostics for /admin/status - answers "is the HQ MIDI path
    actually configured?" without submitting a job to find out."""
    return {
        "backend": "gpu",
        "available": is_available(),
        "missing_config": _missing_config() or None,
        "endpoint_id": RUNPOD_MT3_ENDPOINT_ID or None,
        "timeout_seconds": MIDI_HQ_TIMEOUT_SECONDS,
        "max_duration_seconds": MAX_MIDI_HQ_DURATION_SECONDS,
    }