"""
transcription.py - The one function every transcription route calls.

WHY THIS MODULE EXISTS AT ALL. There are two backends:

    speech_to_text.py      local faster-whisper, CPU, SYNCHRONOUS
    speech_to_text_gpu.py  RunPod GPU worker,     ASYNCHRONOUS

That difference is not cosmetic and cannot be papered over inside either
module. The local one is CPU-bound and must be dispatched to a thread
pool via run_blocking() or it stalls the event loop for minutes. The GPU
one is network-bound and must be awaited - wrapping it in run_blocking()
would burn a pool thread doing nothing but waiting on HTTP, and would
need a nested event loop to run the async client at all.

So the fork lives here, in one place, behind one async function. The
three routes (/speech-to-text, /youtube/transcribe, /video-to-text) call
transcribe_job() and never learn which backend ran. Switching backends
is then a single env var and a restart, with no route code touched -
which is the whole point, because "flip a variable" is a decision you can
reverse at 2am and "edit three routes" is not.

WHAT IS SHARED REGARDLESS OF BACKEND. Language validation, the options
payload, and the shape of the returned transcript all come from
speech_to_text.py either way. The GPU module imports the same
normalizers rather than duplicating them, so a language code accepted on
CPU is accepted on GPU and vice versa - always, by construction rather
than by both files happening to agree.

IMPORTANT - the local model is NOT loaded when the backend is "gpu".
speech_to_text.py checks TRANSCRIPTION_BACKEND before constructing its
WhisperModel singleton. Without that, running on GPU would still pay
~1GB of resident RAM and several seconds of startup for a model that
never handles a single request.
"""
from config import logger, TRANSCRIPTION_BACKEND
from utils import run_blocking

import speech_to_text

_IS_GPU = TRANSCRIPTION_BACKEND == "gpu"

if _IS_GPU:
    # Imported ONLY on the GPU path. It pulls in runpod_client and
    # gpu_internal_routes, and there is no reason for a CPU-only
    # deployment to carry those imports or to fail at startup if the
    # RunPod config is absent.
    import speech_to_text_gpu
    logger.info(
        "[TRANSCRIPTION] Backend: GPU (RunPod). The local Whisper model "
        "will NOT be loaded."
    )
else:
    speech_to_text_gpu = None
    logger.info("[TRANSCRIPTION] Backend: local CPU (faster-whisper).")


async def transcribe_job(input_path: str, language: str = None,
                         task: str = "transcribe", mode: str = None) -> dict:
    """
    Transcribes input_path using whichever backend is configured.

    Returns the same dict shape either way - see speech_to_text.py's
    transcribe() docstring for the fields. Raises AudioToolError with a
    user-facing message on failure, from either backend, with identical
    wording for identical conditions.

    Awaited directly by the routes. Note the local branch still goes
    through run_blocking(): that is not optional politeness, it is what
    keeps a multi-minute CPU inference off the event loop while status
    polls are being served.
    """
    if _IS_GPU:
        return await speech_to_text_gpu.transcribe(input_path, language, task, mode)

    # Positional, not keyword: run_blocking forwards *args to the
    # executor and is not guaranteed to pass keywords through. Order must
    # match transcribe()'s signature.
    return await run_blocking(speech_to_text.transcribe, input_path, language, task, mode)


def is_available() -> bool:
    """
    Whether transcription can currently run at all.

    Local: the model loaded successfully at startup.
    GPU:   every required RunPod env var is present.

    Both failure modes are real and both should surface as a clean 503
    rather than as a job that gets accepted and then dies - which is why
    all three routes check this before creating anything.
    """
    if _IS_GPU:
        return speech_to_text_gpu.is_available()
    return speech_to_text.is_available()


def get_backend_info() -> dict:
    """Diagnostics for /admin/status. Answers "which backend is live, and
    is it actually usable?" in one call, without submitting a job to find
    out the hard way."""
    if _IS_GPU:
        return speech_to_text_gpu.get_backend_info()
    return {
        "backend": "local",
        "available": speech_to_text.is_available(),
        "model_size": speech_to_text.WHISPER_MODEL_SIZE,
        "compute_type": speech_to_text.WHISPER_COMPUTE_TYPE,
        "device": speech_to_text.WHISPER_DEVICE,
        "vad_filter": speech_to_text._vad_enabled,
    }