"""piano_gpu.py - The VPS side of the piano sheet-music engine.

Client for gpu-worker-piano/handler.py (Transkun). runpod_client.py carries
the job. Deliberately the same shape as midi_hq_gpu.py: one-time transfer
token, register/unregister in a `finally`, worker error codes translated to
AudioToolError messages, MIDI returned base64 in the job payload.

Transkun is a piano-specialist (SOTA on solo piano) and takes no tunable
parameters, so unlike the YourMT3 client there is no pitch/note filtering
here. `isolate` reuses the existing htdemucs_6s separation to pull the piano
stem first, mirroring the guitar path in midi_hq_gpu.py.
"""
import base64
import os
import time
import uuid

from config import (
    logger,
    RUNPOD_API_KEY,
    GPU_WORKER_SHARED_SECRET,
    VPS_PUBLIC_BASE_URL,
)
import config as _config
from audio_common import AudioToolError
from runpod_client import run_worker_job, RunPodJobError
from gpu_internal_routes import register_gpu_input, unregister_gpu_input
from separation import run_stem_separation, SeparationError
from utils import cleanup_file

# New env vars, read defensively so this module imports cleanly before
# config.py gains them (Step 7). Until RUNPOD_PIANO_ENDPOINT_ID is set,
# is_available() returns False and the runner falls back to YourMT3.
RUNPOD_PIANO_ENDPOINT_ID = getattr(_config, "RUNPOD_PIANO_ENDPOINT_ID", "")
PIANO_TIMEOUT_SECONDS = int(getattr(_config, "PIANO_TIMEOUT_SECONDS", 300))
MAX_PIANO_DURATION_SECONDS = int(getattr(_config, "MAX_PIANO_DURATION_SECONDS", 900))

ISOLATION_MODEL = "htdemucs_6s"

# Worker error codes -> what the uploader should read. Same discipline as
# midi_hq_gpu: data, not an if-chain, so an added code is one line and an
# unmapped one falls through to the generic message (safe but uninformative).
_WORKER_ERRORS = {
    "MODEL_NOT_LOADED": (
        "Piano sheet music is temporarily unavailable. Please try again later."
    ),
    "INPUT_TOO_LONG": (
        f"Audio is too long for piano transcription "
        f"(limit is {MAX_PIANO_DURATION_SECONDS // 60} minutes)."
    ),
    "EMPTY_INPUT": (
        "That file appears to be empty or unreadable. Please try another file."
    ),
    "NO_NOTES_DETECTED": (
        "No piano notes were detected. The audio may be silent, non-piano, "
        "or too quiet to transcribe."
    ),
}

# Codes that mean OUR side is broken, not the user's file: logged loudly,
# reported neutrally.
_INTERNAL_ERRORS = {
    "MISSING_TRANSFER_PARAMS",
    "INPUT_FETCH_FAILED",
}

_GENERIC_ERROR = "Piano transcription failed. Please try again in a moment."


def is_available() -> bool:
    """Whether the piano path can run right now. Checked at request time,
    not import, so a missing env var degrades this engine to a clean
    fallback rather than crashing the app at boot.
    """
    return bool(
        RUNPOD_API_KEY
        and RUNPOD_PIANO_ENDPOINT_ID
        and GPU_WORKER_SHARED_SECRET
        and VPS_PUBLIC_BASE_URL
    )


def _missing_config() -> str:
    return ", ".join(
        name for name, value in (
            ("RUNPOD_API_KEY", RUNPOD_API_KEY),
            ("RUNPOD_PIANO_ENDPOINT_ID", RUNPOD_PIANO_ENDPOINT_ID),
            ("GPU_WORKER_SHARED_SECRET", GPU_WORKER_SHARED_SECRET),
            ("VPS_PUBLIC_BASE_URL", VPS_PUBLIC_BASE_URL),
        ) if not value
    )


def _validate_local_input(input_path: str) -> None:
    if not os.path.exists(input_path):
        raise AudioToolError("The uploaded file could not be found. Please try again.")
    if os.path.getsize(input_path) == 0:
        raise AudioToolError("That file is empty. Please upload a valid audio file.")


async def transcribe_to_midi(
    input_path: str,
    output_path: str,
    isolate: bool = False,
    job_id: str | None = None,
) -> dict:
    """Transcribe piano audio on the GPU worker and write MIDI to
    output_path. Returns the worker's stats dict (note counts, pitch range,
    and the "_gpu" timing block) plus engine/isolated markers.

    Raises AudioToolError with a user-facing message on every failure path;
    a caller can wrap this in one `except AudioToolError` and be complete.
    """
    if not is_available():
        logger.error(f"[PIANO] Rejecting request - missing config: {_missing_config()}")
        raise AudioToolError(
            "Piano sheet music is temporarily unavailable. Please try again later."
        )

    _validate_local_input(input_path)

    started = time.monotonic()
    stem_paths: dict = {}
    source_path = input_path
    isolate_seconds = 0.0
    jid = job_id or uuid.uuid4().hex

    try:
        # ---------- optional piano isolation (VPS side, existing Demucs) ----------
        if isolate:
            iso_started = time.monotonic()
            try:
                stem_paths = await run_stem_separation(input_path, jid, model=ISOLATION_MODEL)
            except SeparationError as e:
                logger.error(f"[PIANO] isolation failed for job {jid}: {e}")
                raise AudioToolError(
                    "Could not isolate the piano from this mix. Try again, or "
                    "upload a solo piano recording."
                )
            isolate_seconds = time.monotonic() - iso_started
            source_path = stem_paths.get("piano")
            if not source_path or not os.path.exists(source_path):
                raise AudioToolError("Piano isolation produced no piano stem for this file.")

        # ---------- ship to the worker ----------
        token = uuid.uuid4().hex
        suffix = os.path.splitext(source_path)[1] or ".wav"
        payload = {
            "vps_base_url": VPS_PUBLIC_BASE_URL,
            "token": token,
            "secret": GPU_WORKER_SHARED_SECRET,
            "suffix": suffix,
        }

        register_gpu_input(token, source_path)
        logger.info(
            f"[PIANO] Submitting token={token[:8]}... "
            f"({os.path.basename(source_path)}, isolate={isolate})"
        )
        try:
            result = await run_worker_job(
                RUNPOD_PIANO_ENDPOINT_ID,
                RUNPOD_API_KEY,
                payload,
                PIANO_TIMEOUT_SECONDS,
            )
        except RunPodJobError as e:
            logger.error(f"[PIANO] RunPod job failed: {e}")
            raise AudioToolError(_GENERIC_ERROR)
        except Exception as e:  # noqa: BLE001
            logger.error(
                f"[PIANO] Unexpected failure talking to the GPU worker: "
                f"{e.__class__.__name__}: {e}",
                exc_info=True,
            )
            raise AudioToolError(_GENERIC_ERROR)
        finally:
            unregister_gpu_input(token)

        # ---------- worker-reported errors ----------
        error = result.get("error") if isinstance(result, dict) else None
        if error:
            if error in _INTERNAL_ERRORS:
                logger.error(f"[PIANO] Worker reported an internal failure: {error}")
                raise AudioToolError(_GENERIC_ERROR)
            message = _WORKER_ERRORS.get(error)
            if message is None:
                logger.warning(f"[PIANO] Unmapped worker error code: {error}")
                raise AudioToolError(_GENERIC_ERROR)
            raise AudioToolError(message)

        # ---------- decode + write MIDI ----------
        midi_b64 = result.get("midi_b64") if isinstance(result, dict) else None
        if not midi_b64:
            logger.error(f"[PIANO] Malformed worker result (no midi_b64): {str(result)[:300]}")
            raise AudioToolError(_GENERIC_ERROR)
        try:
            midi_bytes = base64.b64decode(midi_b64)
        except Exception as e:  # noqa: BLE001
            logger.error(f"[PIANO] Could not decode worker MIDI payload: {e}")
            raise AudioToolError(_GENERIC_ERROR)
        if not midi_bytes:
            logger.error("[PIANO] Worker returned an empty MIDI payload")
            raise AudioToolError(_GENERIC_ERROR)

        # Whole write, not streamed: kilobytes, and a partial write would
        # leave a corrupt .mid that opens as an empty project in a DAW.
        with open(output_path, "wb") as f:
            f.write(midi_bytes)

        stats = {k: v for k, v in result.items() if k != "midi_b64"}
        stats["engine"] = "transkun"
        stats["isolated"] = bool(isolate)
        gpu = stats.get("_gpu") or {}
        if isolate_seconds and isinstance(gpu, dict):
            # Fold isolation cost into the timing the meter records.
            gpu["isolate_seconds"] = round(isolate_seconds, 2)
            stats["_gpu"] = gpu

        logger.info(
            f"[PIANO] Complete: {stats.get('note_count')} notes, "
            f"{stats.get('duration_seconds')}s output, {len(midi_bytes)}B "
            f"(isolate {isolate_seconds:.1f}s, "
            f"infer {gpu.get('infer_seconds')}s, total "
            f"{round(time.monotonic() - started, 1)}s)"
        )
        return stats

    finally:
        for path in stem_paths.values():
            cleanup_file(path)