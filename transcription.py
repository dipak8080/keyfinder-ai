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

--------------------------------------------------------------------------
ADDED 2026-08-27: GPU SECONDS ARE RECORDED HERE

Transcription became metered, and every transcribe row in
gpu_job_metrics came back with a null gpu_seconds and therefore a null
est_cost_usd - so the cost report that exists to answer "does a credit
cover a job?" could answer it for separation and not for the tool that
turned out to cost MORE per job (about 126 GPU-seconds against
separation's ~95, measured over 57 real jobs).

The reason was a layering accident rather than a missing feature. The
worker does report its own timing, under a "_gpu" key; speech_to_text_gpu
popped that off before returning, because operator diagnostics are not
part of the public transcript contract. Correct instinct, wrong floor:
that module has no job_id to record against, and deliberately should not
have one (its transfer token is intentionally NOT the job id - see its
docstring).

THIS module is where both halves exist. It receives the job_id from the
route and the "_gpu" stats from the backend, records one against the
other, and strips the key on the way past - so the routes still receive
exactly the dict they always did, from either backend.

WHY THE JOB_ID IS OPTIONAL. Not every caller is metered, and a missing
job_id must mean "don't record" rather than "crash". That also keeps
this callable from anywhere - a script, a test, a future unmetered
route - without inventing a fake id to satisfy a signature.

WHAT GETS RECORDED, AND WHAT IT IS NOT. gpu_seconds is fetch_seconds +
infer_seconds: the worker's own measurement of the work it did. It is a
FLOOR on the bill, not the bill - RunPod also charges for cold start and
container init, and this endpoint's cold-start count has run HIGHER than
its request count. metering.py says the same thing about separation's
number for the same reason. Treat a tool whose estimate approaches its
revenue as already losing money.

THE LOCAL BACKEND RECORDS NOTHING. CPU inference costs VPS time, not
GPU-seconds, and writing wall clock into a column named gpu_seconds
would corrupt the one number the cost report exists to provide. A job
run on the local backend gets its status row from the route and no cost
figure, which is the honest answer.
--------------------------------------------------------------------------
"""
from config import logger, TRANSCRIPTION_BACKEND
from utils import run_blocking

import speech_to_text

# Recorded, never enforced - the same arrangement separation.py has. See
# credits/metering.py for why nothing reads these numbers back to make a
# decision, and why the old self-tracked spend breaker was removed.
from credits import metering

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


def _record_gpu_cost(job_id: str, gpu_stats: dict) -> None:
    """Write the worker's reported timing against this job's metrics row.

    Swallows everything, on purpose and for the same reason every
    function in credits/metering.py does: a metering failure must never
    turn a working transcription into a failed one. A missing row costs
    one data point; an exception escaping here would cost the user their
    transcript, after the GPU time had already been paid for.

    Status is 'completed' because this is only reached after the backend
    returned a transcript. The route's own `finally` writes a status too,
    and the two agree - record_job_finished COALESCEs every optional
    column, so whichever call lands second cannot blank what the first
    recorded.
    """
    try:
        fetch = gpu_stats.get("fetch_seconds") or 0
        infer = gpu_stats.get("infer_seconds") or 0
        gpu_seconds = float(fetch) + float(infer)
        if gpu_seconds <= 0:
            # An older worker image, or a malformed payload. Recording
            # zero would be worse than recording nothing: it would drag
            # the average cost-per-job down with a job that certainly
            # cost something.
            return
        metering.record_job_finished(
            job_id,
            status="completed",
            gpu_seconds=gpu_seconds,
        )
    except Exception:  # noqa: BLE001
        logger.exception("[TRANSCRIPTION] could not record GPU cost for job %s", job_id)


async def transcribe_job(input_path: str, language: str = None,
                         task: str = "transcribe", mode: str = None,
                         job_id: str = None) -> dict:
    """
    Transcribes input_path using whichever backend is configured.

    Returns the same dict shape either way - see speech_to_text.py's
    transcribe() docstring for the fields. Raises AudioToolError with a
    user-facing message on failure, from either backend, with identical
    wording for identical conditions.

    job_id is optional and used ONLY for cost metering. Pass it from any
    route that opened a gpu_job_metrics row; omit it and nothing is
    recorded. It is deliberately last and keyword-friendly so every
    existing positional call keeps working unchanged.

    Awaited directly by the routes. Note the local branch still goes
    through run_blocking(): that is not optional politeness, it is what
    keeps a multi-minute CPU inference off the event loop while status
    polls are being served.
    """
    if _IS_GPU:
        result = await speech_to_text_gpu.transcribe(input_path, language, task, mode)

        # Stripped HERE rather than in the backend - see the 2026-08-27
        # note in the module docstring. pop() rather than get(), so the
        # dict handed back to the route is byte-identical in shape to
        # what the local backend returns and the frontend can never
        # start depending on operator diagnostics.
        gpu_stats = result.pop(speech_to_text_gpu.GPU_STATS_KEY, None) or {}
        if job_id and gpu_stats:
            _record_gpu_cost(job_id, gpu_stats)
        return result

    # Positional, not keyword: run_blocking forwards *args to the
    # executor and is not guaranteed to pass keywords through. Order must
    # match transcribe()'s signature.
    #
    # job_id is NOT forwarded: the local backend does not take one, and
    # there is nothing GPU-shaped to record for a CPU run anyway.
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