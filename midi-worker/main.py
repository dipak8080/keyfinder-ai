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

SENSITIVITY CASCADE (added 2026-08-13):
Real production case that motivated this: a KSHMR sample-pack melody
(soft-attack, filtered synth, likely some reverb) returned ZERO note
events at basic-pitch's default onset_threshold=0.5/frame_threshold=0.3
- confirmed via a 15.9s run that completed normally and genuinely found
nothing, not a crash or a bug. basic-pitch's onset detector is looking
for a sharp energy spike to mark "a note started here"; smooth, heavily
filtered, or reverb-smeared melodies can have weak enough onsets that
the default threshold misses them entirely even though the audio is
obviously musical to a human ear.

Rather than requiring every caller to know to lower onset_threshold/
frame_threshold by hand (which meant re-testing over SSH with curl -F
params to fix one file), /convert now automatically retries at
progressively more sensitive thresholds whenever a given attempt comes
back with zero notes, stopping at the first tier that finds something.
The caller's own requested thresholds (or the defaults) are always tried
FIRST and are the ones actually used if they succeed - the cascade is
purely a safety net for the empty-result case, never a silent override
of a setting that already worked.
"""
import os
import asyncio
import logging
import tempfile
from typing import Optional

from fastapi import FastAPI, UploadFile, File, Header, HTTPException, Form
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import Response

from basic_pitch.inference import predict, Model
from basic_pitch import ICASSP_2022_MODEL_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SHARED_SECRET = os.environ.get("MIDI_WORKER_SHARED_SECRET", "")
MAX_UPLOAD_BYTES = int(os.environ.get("MIDI_MAX_UPLOAD_BYTES", str(80 * 1024 * 1024)))  # 80 MB
MAX_CONCURRENT = int(os.environ.get("MIDI_WORKER_CONCURRENCY", "2"))

# Progressively more sensitive (onset_threshold, frame_threshold) pairs
# tried, in order, after the caller's own requested setting, whenever an
# attempt returns zero note events. Kept as a fixed constant rather than
# env-configurable - these are model-tuning values, not deployment
# config, and the caller can already always override the STARTING point
# via the request's own onset_threshold/frame_threshold fields.
CASCADE_TIERS = [(0.35, 0.20), (0.25, 0.15), (0.15, 0.10)]

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
    onset_threshold: float = Form(0.5),
    frame_threshold: float = Form(0.3),
    minimum_note_length: float = Form(127.70),
    minimum_frequency: Optional[float] = Form(None),
    maximum_frequency: Optional[float] = Form(None),
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
            # Cascade: the caller's requested (onset_threshold,
            # frame_threshold) is tried first, then progressively more
            # sensitive tiers - but only tiers STRICTLY more sensitive
            # than what was requested, so a caller who already asked for
            # 0.2 doesn't get retried at a looser 0.35 first.
            cascade = [(onset_threshold, frame_threshold)] + [
                (t_onset, t_frame)
                for t_onset, t_frame in CASCADE_TIERS
                if t_onset < onset_threshold
            ]

            midi_data = None
            note_events = []
            used_onset, used_frame = onset_threshold, frame_threshold

            for tier_onset, tier_frame in cascade:
                try:
                    # run_in_threadpool is MANDATORY here - see module
                    # docstring. predict() is blocking TF inference;
                    # calling it inline would freeze this process's
                    # event loop.
                    _, tier_midi_data, tier_note_events = await run_in_threadpool(
                        lambda t_on=tier_onset, t_fr=tier_frame: predict(
                            input_path,
                            model_or_model_path=_model,
                            onset_threshold=t_on,
                            frame_threshold=t_fr,
                            minimum_note_length=minimum_note_length,
                            minimum_frequency=minimum_frequency,
                            maximum_frequency=maximum_frequency,
                        )
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

                if tier_note_events:
                    midi_data = tier_midi_data
                    note_events = tier_note_events
                    used_onset, used_frame = tier_onset, tier_frame
                    if (tier_onset, tier_frame) != (onset_threshold, frame_threshold):
                        logger.info(
                            f"[MIDI_WORKER] Empty at requested thresholds "
                            f"(onset={onset_threshold}, frame={frame_threshold}) - "
                            f"succeeded on cascade retry (onset={tier_onset}, frame={tier_frame})"
                        )
                    break

        if not note_events:
            # NOT a crash - a legitimate "nothing musical here" result,
            # confirmed empty across every sensitivity tier in the
            # cascade above, not just the first attempt (silence, pure
            # noise, spoken word, applause). Same 422 as a real failure
            # because 204 cannot carry a response body per HTTP spec,
            # but the structured `reason` field lets the caller tell the
            # two apart reliably and give the user an accurate message
            # instead of a generic error.
            logger.warning(f"[MIDI_WORKER] Zero notes detected across all cascade tiers ({total} bytes input)")
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
            f"{len(midi_bytes)} bytes MIDI from {total} bytes audio "
            f"(onset={used_onset}, frame={used_frame})"
        )
        return Response(
            content=midi_bytes,
            media_type="audio/midi",
            headers={
                "X-Onset-Threshold-Used": str(used_onset),
                "X-Frame-Threshold-Used": str(used_frame),
            },
        )

    finally:
        if input_path and os.path.exists(input_path):
            os.unlink(input_path)