"""
midi-worker/main.py - Isolated audio-to-MIDI transcription service
(Spotify basic-pitch / ICASSP 2022 model).

WHY THIS IS A SEPARATE CONTAINER, NOT A MODULE IN THE MAIN APP:
basic-pitch 0.4.0 requires tensorflow<2.15.1 on Linux/Python>=3.11,
which resolves to tensorflow==2.15.0.post1, which hard-pins
numpy<2.0.0. The main app's entire stack (essentia, librosa, demucs,
torch, scipy, scikit-learn) runs on numpy==2.3.5. Adding basic-pitch to
the main requirements.txt either fails resolution outright or forces an
app-wide numpy downgrade across every existing tool to satisfy one new
feature. numpy 2.0 was an ABI-breaking release, so that downgrade is a
real regression risk, not a formality. Verified with a real pip dry-run
against the exact pinned versions before choosing this design.

Full process isolation is the only version of this that adds ZERO risk
to the working product. Same architectural pattern as gpu-worker/, just
motivated by dependency isolation rather than GPU access.

CONCURRENCY MODEL - THE IMPORTANT PART:
predict() is fully blocking (TensorFlow inference + librosa decode).
The endpoint below is `async def` because it must `await` the streaming
upload read - so predict() MUST be dispatched via run_in_threadpool(),
never called inline. Calling it inline would block this process's event
loop for the entire transcription, meaning /health stops responding and
concurrent requests serialize behind each other invisibly.

A worker-side semaphore bounds real concurrency independently of
whatever the caller does - the main app has its own _midi_semaphore, but
this service must not rely on a well-behaved client for its own
resource safety.
"""
import os
import asyncio
import logging
import tempfile

from fastapi import FastAPI, UploadFile, File, Header, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import Response

from basic_pitch.inference import predict, Model
from basic_pitch import ICASSP_2022_MODEL_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SHARED_SECRET = os.environ.get("MIDI_WORKER_SHARED_SECRET", "")
MAX_UPLOAD_BYTES = int(os.environ.get("MIDI_MAX_UPLOAD_BYTES", str(80 * 1024 * 1024)))  # 80 MB
MAX_CONCURRENT = int(os.environ.get("MIDI_WORKER_CONCURRENCY", "2"))

app = FastAPI()

# Bounds how many transcriptions actually run at once inside THIS
# process, regardless of caller behaviour. Deliberately independent of
# the main app's _midi_semaphore: a service should never depend on its
# client being well-behaved for its own resource safety.
_semaphore = asyncio.Semaphore(MAX_CONCURRENT)

# Loaded ONCE at process startup. basic-pitch's own docs call reloading
# the model per prediction "redundant and sluggish". A failure here
# should kill the container at boot (loud, immediate, caught in deploy)
# rather than silently 500ing on every request later.
logger.info("[MIDI_WORKER] Loading model...")
_model = Model(ICASSP_2022_MODEL_PATH)
logger.info(f"[MIDI_WORKER] Model loaded (type={_model.model_type.name}), ready to serve.")


def _verify_secret(secret: str):
    if not SHARED_SECRET:
        # Misconfiguration, not an attack - fail loud so it's caught in
        # deploy testing rather than presenting as mystery 401s.
        logger.error("[MIDI_WORKER] MIDI_WORKER_SHARED_SECRET is not set on this container.")
        raise HTTPException(500, {"reason": "misconfigured", "message": "Worker is misconfigured."})
    if secret != SHARED_SECRET:
        logger.error("[MIDI_WORKER] Rejected request - invalid shared secret (env vars out of sync?).")
        raise HTTPException(401, {"reason": "unauthorized", "message": "Unauthorized"})


@app.get("/health")
async def health():
    """Deliberately does NOT acquire the semaphore or touch the model -
    a health check that queues behind real work reports "unhealthy"
    exactly when the service is busiest, which is the opposite of
    useful."""
    return {"status": "healthy", "model_type": _model.model_type.name, "concurrency": MAX_CONCURRENT}


@app.post("/convert")
async def convert(
    file: UploadFile = File(...),
    x_internal_secret: str = Header(default=""),
):
    _verify_secret(x_internal_secret)

    # Streamed to a temp file in 1MB chunks rather than read whole into
    # memory - same reasoning as the main app's upload.py. The cap is
    # enforced MID-STREAM so an oversized body is abandoned partway,
    # not fully buffered and then rejected.
    suffix = os.path.splitext(file.filename or "")[1].lower() or ".audio"
    input_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp_in:
            input_path = tmp_in.name
            total = 0
            while chunk := await file.read(1024 * 1024):
                total += len(chunk)
                if total > MAX_UPLOAD_BYTES:
                    raise HTTPException(
                        413,
                        {"reason": "too_large", "message": "File too large."},
                    )
                tmp_in.write(chunk)

        if total == 0:
            raise HTTPException(422, {"reason": "empty", "message": "Empty file."})

        async with _semaphore:
            try:
                # run_in_threadpool is MANDATORY here - see module
                # docstring. predict() is blocking TF inference; calling
                # it inline would freeze this process's event loop.
                _, midi_data, note_events = await run_in_threadpool(
                    predict, input_path, _model
                )
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"[MIDI_WORKER] Transcription failed ({total} bytes): {e}", exc_info=True)
                raise HTTPException(
                    422,
                    {
                        "reason": "transcription_failed",
                        "message": "Could not transcribe this audio. It may be corrupt or unsupported.",
                    },
                )

        if not note_events:
            # NOT a crash - a legitimate "nothing musical here" result
            # (silence, pure noise, spoken word, applause). Same 422 as
            # a real failure because 204 cannot carry a response body
            # per HTTP spec, but the structured `reason` field lets the
            # caller tell the two apart reliably and give the user an
            # accurate message instead of a generic error.
            logger.warning(f"[MIDI_WORKER] Zero notes detected ({total} bytes input)")
            raise HTTPException(
                422,
                {"reason": "no_notes", "message": "No musical notes were detected in this audio."},
            )

        out_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".mid", delete=False) as tmp_out:
                out_path = tmp_out.name
            midi_data.write(out_path)
            with open(out_path, "rb") as f:
                midi_bytes = f.read()
        finally:
            if out_path and os.path.exists(out_path):
                os.unlink(out_path)

        if not midi_bytes:
            logger.error("[MIDI_WORKER] MIDI written but file was empty")
            raise HTTPException(
                422,
                {"reason": "empty_output", "message": "Transcription produced no MIDI data."},
            )

        logger.info(
            f"[MIDI_WORKER] COMPLETE - {len(note_events)} notes, "
            f"{len(midi_bytes)} bytes MIDI from {total} bytes audio"
        )
        return Response(content=midi_bytes, media_type="audio/midi")

    finally:
        if input_path and os.path.exists(input_path):
            os.unlink(input_path)