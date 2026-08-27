"""
speech_to_text_gpu.py - The GPU backend for transcription.

WHERE THIS FITS: this is the VPS side. gpu-worker-whisper/handler.py is
the RunPod side. transcription.py is the dispatcher that picks between
this module and speech_to_text.py based on TRANSCRIPTION_BACKEND.

It presents the SAME contract as speech_to_text.transcribe() - same
arguments, same returned dict, same AudioToolError messages - with one
unavoidable difference: this one is ASYNC, because it makes network
calls and awaiting them is correct where dispatching them to a thread
pool would not be. That is exactly why transcription.py exists: the
routes await one dispatcher and never learn which backend ran.

AUDIO MOVES OVER HTTP, NOT IN THE JOB PAYLOAD. RunPod caps job payloads
at 10MB and real audio exceeds that immediately. The file is registered
against a one-time transfer token in gpu_internal_routes.py's registry,
the worker fetches it with the shared secret, and the token is
unregistered in a `finally` - so the window in which that URL serves
anything is exactly the lifetime of one job.

WHY A RANDOM TOKEN AND NOT THE JOB ID. The registry accepts any 8-64
hex string, so the obvious move is to reuse the caller's job_id. It is
deliberately NOT done: transcribe() does not receive a job_id, and
threading one through purely for this would change the signature of the
local backend too, for a reason that has nothing to do with it. A fresh
uuid4 is a transfer handle, not an identity - and it has the useful
property of being unguessable even if a job id ever leaked.

--------------------------------------------------------------------------
CHANGED 2026-08-27: _gpu IS NO LONGER STRIPPED HERE

The worker reports its own timing under a "_gpu" key - fetch_seconds,
infer_seconds, rtf. This module used to result.pop() that before
returning, on the reasoning that operator diagnostics are not part of
the public transcript contract and letting them through would make the
two backends' responses differ in a way the frontend could accidentally
start depending on.

That reasoning is still right, and the stripping still happens - just
one layer up, in transcription.py, which is the module that knows the
job_id those numbers need to be recorded against. Popping here threw
away the only measurement of what a transcription actually costs, which
is why every transcribe row in gpu_job_metrics had a null gpu_seconds
and a null est_cost_usd while separation rows had real dollars.

The routes still never see "_gpu": transcription.py removes it on the
way past. The contract the frontend depends on is unchanged, and the
two backends still return identically-shaped dicts to every caller
outside this package.

WHY NOT JUST METER HERE. This module has no job_id and should not grow
one - see the note above on why the transfer token is deliberately not
the job id. Handing it a second identity purely so it could write a
metrics row would undo that separation for no gain, when the dispatcher
one line up already holds both halves.
--------------------------------------------------------------------------
"""
import asyncio
import os
import uuid

from config import (
    logger,
    RUNPOD_API_KEY,
    RUNPOD_WHISPER_ENDPOINT_ID,
    GPU_WORKER_SHARED_SECRET,
    VPS_PUBLIC_BASE_URL,
    WHISPER_MODEL_SIZE,
    GPU_TRANSCRIBE_TIMEOUT_SECONDS,
    WHISPER_VAD_FILTER,
    TRANSCRIPTION_MODE_BEAM_SIZES,
    DEFAULT_TRANSCRIPTION_MODE,
)
from audio_common import AudioToolError
from runpod_client import run_worker_job, RunPodJobError
from gpu_internal_routes import register_gpu_input, unregister_gpu_input

# Reused rather than reimplemented, so the two backends can never
# disagree about what a valid language code or task is. Importing the
# local module does NOT load a model when TRANSCRIPTION_BACKEND is "gpu"
# - see the conditional load at the top of speech_to_text.py.
from speech_to_text import (
    _normalize_language,
    _normalize_task,
    _normalize_mode,
    _validate_input_file,
)

# The key the worker reports its own timing under. Named here rather
# than written as a literal in two places, because transcription.py has
# to strip exactly this key and a typo on either side would silently
# leak diagnostics into the public transcript.
GPU_STATS_KEY = "_gpu"


def is_available() -> bool:
    """
    True when every piece of GPU configuration is present.

    Checked at request time rather than import time because these are all
    env vars: a missing one should degrade THIS endpoint to a clean 503,
    not crash the app at boot the way an import-time assertion would.

    VPS_PUBLIC_BASE_URL is on the list and is the one most likely to be
    forgotten - without it the worker has no address to fetch audio from,
    and the failure surfaces on the RunPod side as a timeout rather than
    as anything that names the missing variable.
    """
    return bool(
        RUNPOD_API_KEY
        and RUNPOD_WHISPER_ENDPOINT_ID
        and GPU_WORKER_SHARED_SECRET
        and VPS_PUBLIC_BASE_URL
    )


def _missing_config() -> str:
    missing = [
        name for name, value in (
            ("RUNPOD_API_KEY", RUNPOD_API_KEY),
            ("RUNPOD_WHISPER_ENDPOINT_ID", RUNPOD_WHISPER_ENDPOINT_ID),
            ("GPU_WORKER_SHARED_SECRET", GPU_WORKER_SHARED_SECRET),
            ("VPS_PUBLIC_BASE_URL", VPS_PUBLIC_BASE_URL),
        ) if not value
    ]
    return ", ".join(missing)


async def transcribe(input_path: str, language: str = None, task: str = "transcribe",
                     mode: str = None) -> dict:
    """
    Transcribes input_path on the GPU worker.

    Same arguments and same returned dict as speech_to_text.transcribe(),
    plus a "_gpu" key carrying the worker's own timing - see the module
    docstring for why that is left in now and who removes it. Raises
    AudioToolError with a user-facing message on any failure.

    Validation happens HERE as well as on the worker, deliberately: a bad
    language code should cost nothing, and discovering it after a submit,
    a cold start and a file transfer would be an expensive way to learn
    about a typo.
    """
    if not is_available():
        logger.error(
            f"[SPEECH_TO_TEXT_GPU] Rejecting request - missing config: {_missing_config()}"
        )
        raise AudioToolError(
            "Transcription is temporarily unavailable. Please try again later."
        )

    _validate_input_file(input_path)

    language = _normalize_language(language)
    task = _normalize_task(task)
    mode, beam_size = _normalize_mode(mode)

    # A fresh handle per job. Registered immediately before submit and
    # removed in the finally below, so the input URL is live for exactly
    # as long as one job runs and not a second longer.
    token = uuid.uuid4().hex
    suffix = os.path.splitext(input_path)[1] or ".wav"

    payload = {
        "vps_base_url": VPS_PUBLIC_BASE_URL,
        "token": token,
        "secret": GPU_WORKER_SHARED_SECRET,
        "language": language,
        "task": task,
        "beam_size": beam_size,
        "vad_filter": WHISPER_VAD_FILTER,
        "suffix": suffix,
    }

    register_gpu_input(token, input_path)
    logger.info(
        f"[SPEECH_TO_TEXT_GPU] Submitting token={token[:8]}... "
        f"({os.path.basename(input_path)}, language={language or 'auto'}, "
        f"task={task}, mode={mode}, beam_size={beam_size})"
    )

    try:
        result = await run_worker_job(
            RUNPOD_WHISPER_ENDPOINT_ID,
            RUNPOD_API_KEY,
            payload,
            GPU_TRANSCRIBE_TIMEOUT_SECONDS,
        )

    except asyncio.CancelledError:
        # run_worker_job already cancels the remote job on its way out,
        # so nothing keeps billing. Re-raised so the calling task
        # actually stops.
        raise

    except RunPodJobError as e:
        logger.error(f"[SPEECH_TO_TEXT_GPU] RunPod job failed: {e}")
        raise AudioToolError(
            "Transcription failed. Please try again in a moment."
        )

    except Exception as e:
        logger.error(
            f"[SPEECH_TO_TEXT_GPU] Unexpected failure talking to the GPU worker: "
            f"{e.__class__.__name__}: {e}",
            exc_info=True,
        )
        raise AudioToolError(
            "Transcription failed. Please try again in a moment."
        )

    finally:
        # ALWAYS, on every path. A leaked registry entry keeps a URL
        # serving that file for the life of the process, and the registry
        # has no TTL sweep precisely because its entries are supposed to
        # be scoped to one request.
        unregister_gpu_input(token)

    # ---------- worker-reported errors ----------
    error = result.get("error") if isinstance(result, dict) else None
    if error:
        if error == "NO_SPEECH_DETECTED":
            # Translated back into the SAME message the local backend
            # produces - a user must not be able to tell which backend
            # ran from the wording of an error.
            raise AudioToolError(
                "No speech was detected in this file. It may be silent, "
                "music-only, or too quiet to pick up."
            )
        logger.error(f"[SPEECH_TO_TEXT_GPU] Worker returned an error: {error}")
        raise AudioToolError(
            "Transcription failed. The file may be corrupt, silent, or in an "
            "unsupported format."
        )

    if not isinstance(result, dict) or not result.get("text"):
        logger.error(f"[SPEECH_TO_TEXT_GPU] Malformed worker result: {result!r}")
        raise AudioToolError("Transcription failed. Please try again in a moment.")

    # READ, not popped - see the 2026-08-27 note in the module docstring.
    # transcription.py strips this key after recording it against the
    # job, so nothing outside this package ever sees it.
    gpu_stats = result.get(GPU_STATS_KEY) or {}

    # `mode` is added here rather than on the worker. The worker receives
    # a beam_size, not a mode name - keeping the name->number mapping in
    # one place (config.py) means adding a tier never requires
    # redeploying the GPU image.
    result["mode"] = mode

    logger.info(
        f"[SPEECH_TO_TEXT_GPU] Complete: language={result.get('language')} "
        f"({result.get('language_probability')}), task={task}, mode={mode}, "
        f"audio={result.get('duration')}s, "
        f"fetch={gpu_stats.get('fetch_seconds')}s, "
        f"infer={gpu_stats.get('infer_seconds')}s, "
        f"rtf={gpu_stats.get('rtf')}x, "
        f"{len(result.get('segments') or [])} segments, "
        f"{len(result.get('text') or '')} chars"
    )

    return result


def get_backend_info() -> dict:
    """Diagnostics for /admin/status - answers "is the GPU path actually
    configured?" without needing to submit a job to find out."""
    return {
        "backend": "gpu",
        "available": is_available(),
        "missing_config": _missing_config() or None,
        "endpoint_id": RUNPOD_WHISPER_ENDPOINT_ID or None,
        "model_size": WHISPER_MODEL_SIZE,
        "timeout_seconds": GPU_TRANSCRIBE_TIMEOUT_SECONDS,
        "modes": list(TRANSCRIPTION_MODE_BEAM_SIZES.keys()),
        "default_mode": DEFAULT_TRANSCRIPTION_MODE,
    }