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

--------------------------------------------------------------------------
THE SENSITIVITY CASCADE, AND WHY IT EXISTS

basic-pitch exposes three knobs that between them decide whether a note
survives into the output at all:

  onset_threshold      - how much energy a note ATTACK needs to register
  frame_threshold      - how much energy a SUSTAINED note needs
  minimum_note_length  - notes shorter than this are discarded outright

Its published defaults (0.5 / 0.3 / 127.7ms) are a sensible
general-purpose middle ground, but they are NOT universally right, and
two real production failures on this service proved it:

  1. A soft-attack filtered synth melody (sample-pack style, some
     reverb) returned ZERO notes at the defaults. Confirmed not a bug -
     a full 15.9s inference ran and genuinely found nothing. The onset
     detector wants a sharp energy spike; smooth/filtered/reverb-smeared
     attacks don't produce one, even though the audio is obviously
     musical to a human.

  2. After loosening onset/frame alone, the SAME file returned exactly
     ONE note. The melody was built from fast, short notes - the model
     was finding them, and then the 127.7ms minimum_note_length floor
     was throwing nearly all of them away. Sensitivity alone could never
     have fixed that; the length floor itself had to shrink.

So a single fixed-threshold attempt is structurally incapable of
handling both a slow pad and a fast arpeggio well. Rather than pushing
that problem onto the caller (who would have to know what these knobs
mean and re-upload repeatedly to find settings that work), this service
runs a CASCADE: the caller's own settings first, then progressively more
permissive tiers, keeping the RICHEST result found rather than the first
non-empty one.

Two rules make the cascade safe rather than just "loosen until
something appears":

  - The caller's requested settings are ALWAYS tier one, and are the
    ones used if they produce a good result. A deliberate, working
    custom setting is never silently overridden.
  - It stops as soon as a result is genuinely substantial (see
    _is_substantial below). Loosening past that point starts admitting
    noise as false notes, which makes the transcription worse, not
    better. "Most permissive" is NOT the goal; "best" is.
--------------------------------------------------------------------------
"""
import os
import asyncio
import logging
import tempfile
from typing import List, Optional, Tuple

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

# (onset_threshold, frame_threshold, minimum_note_length_ms), ordered
# from least to most permissive. All three loosen together because they
# fail together in practice - the same quiet/soft/fast material that
# defeats the onset detector also tends to produce short notes that the
# length floor then discards.
#
# The floor stops at 30ms deliberately. Below roughly that, basic-pitch
# starts emitting sub-perceptual fragments that read as transcription
# noise rather than notes a person played - going lower produces a
# BUSIER output, not a more accurate one.
CASCADE_TIERS: List[Tuple[float, float, float]] = [
    (0.35, 0.20, 100.0),
    (0.25, 0.15, 60.0),
    (0.15, 0.10, 30.0),
]

# A result is "substantial" (stop cascading) at this many notes per
# second of transcribed span. Deliberately DENSITY-based, not a flat
# note count: 6 notes is a rich transcription of a 3-second one-shot and
# a nearly-empty one for a 3-minute track, and a flat threshold would
# stop far too early on long files while over-searching short ones.
#
# 1.5 notes/sec is roughly eighth notes at 90bpm - comfortably "a real
# melody is present" without demanding density that a slow, sparse pad
# legitimately wouldn't have.
SUBSTANTIAL_NOTES_PER_SECOND = float(os.environ.get("MIDI_SUBSTANTIAL_NPS", "1.5"))

# Absolute floor regardless of density - a handful of notes spread over
# a long file is not a transcription worth stopping on, however sparse
# the source material genuinely is.
SUBSTANTIAL_MIN_NOTES = int(os.environ.get("MIDI_SUBSTANTIAL_MIN_NOTES", "8"))

app = FastAPI()

# Bounds how many transcriptions actually run at once inside THIS
# process, regardless of caller behaviour. Deliberately independent of
# the main app's _midi_semaphore: a service should never depend on its
# client being well-behaved for its own resource safety.
#
# Worth knowing: the cascade means ONE request can now run predict()
# multiple times, so a request's worst-case wall time is a multiple of
# what it was before. That is exactly why this semaphore matters more
# now than it did with single-shot inference.
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


def _note_span_seconds(note_events: list) -> float:
    """
    Seconds between the first note's start and the last note's end.

    Used instead of the audio file's own duration because it needs no
    second decode pass - basic-pitch already returns note_events as
    (start_time, end_time, pitch, amplitude, pitch_bends) tuples, so the
    span is free. It also measures the right thing: a 4-minute file with
    a melody only in the first 30 seconds should be judged on those 30
    seconds, not penalised for 3.5 minutes of intentional silence.
    """
    if not note_events:
        return 0.0
    starts = [n[0] for n in note_events]
    ends = [n[1] for n in note_events]
    return max(0.0, max(ends) - min(starts))


def _is_substantial(note_events: list) -> bool:
    """
    True when a transcription is good enough to stop cascading.

    Both conditions must hold: enough notes in absolute terms AND enough
    density. Requiring both is what stops the cascade settling for two
    stray notes at a permissive threshold (dense by span, trivial in
    reality) or for a long thin dribble of notes across a whole track
    (numerous, but not actually a transcription of anything).
    """
    if len(note_events) < SUBSTANTIAL_MIN_NOTES:
        return False
    span = _note_span_seconds(note_events)
    if span <= 0:
        return False
    return (len(note_events) / span) >= SUBSTANTIAL_NOTES_PER_SECOND


@app.get("/health")
async def health():
    """Deliberately does NOT acquire the semaphore or touch the model -
    a health check that queues behind real work reports "unhealthy"
    exactly when the service is busiest, which is the opposite of
    useful."""
    return {
        "status": "healthy",
        "model_type": _model.model_type.name,
        "concurrency": MAX_CONCURRENT,
        "cascade_tiers": len(CASCADE_TIERS) + 1,
    }


@app.post("/convert")
async def convert(
    file: UploadFile = File(...),
    onset_threshold: float = Form(0.5),
    frame_threshold: float = Form(0.3),
    minimum_note_length: float = Form(127.70),
    minimum_frequency: Optional[float] = Form(None),
    maximum_frequency: Optional[float] = Form(None),
    # Off by default: pitch bends make a transcription more faithful to
    # a expressive/slide-heavy performance, but they also make the MIDI
    # messier to edit in a DAW, which is the more common use for this
    # tool. Exposed so a caller who wants the expressive version can ask.
    multiple_pitch_bends: bool = Form(False),
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
            # Tier one is ALWAYS exactly what the caller asked for (or
            # the defaults). Subsequent tiers are included only if they
            # are genuinely more permissive on at least one axis than
            # what was requested - a caller who already asked for
            # onset=0.2 shouldn't be retried at a stricter 0.35.
            cascade: List[Tuple[float, float, float]] = [
                (onset_threshold, frame_threshold, minimum_note_length)
            ] + [
                (t_onset, t_frame, t_min_len)
                for t_onset, t_frame, t_min_len in CASCADE_TIERS
                if t_onset < onset_threshold or t_min_len < minimum_note_length
            ]

            best_midi_data = None
            best_note_events: list = []
            used_onset, used_frame, used_min_len = (
                onset_threshold,
                frame_threshold,
                minimum_note_length,
            )
            tiers_run = 0

            for tier_onset, tier_frame, tier_min_len in cascade:
                tiers_run += 1
                try:
                    # run_in_threadpool is MANDATORY here - see module
                    # docstring. predict() is blocking TF inference;
                    # calling it inline would freeze this process's
                    # event loop.
                    #
                    # Default-argument binding on the lambda (t_on=... )
                    # captures THIS iteration's values. A bare closure
                    # over the loop variables would evaluate them at
                    # call time, and every tier would silently run with
                    # the final tier's settings - a real bug, not style.
                    _, tier_midi_data, tier_note_events = await run_in_threadpool(
                        lambda t_on=tier_onset, t_fr=tier_frame, t_ml=tier_min_len: predict(
                            input_path,
                            model_or_model_path=_model,
                            onset_threshold=t_on,
                            frame_threshold=t_fr,
                            minimum_note_length=t_ml,
                            minimum_frequency=minimum_frequency,
                            maximum_frequency=maximum_frequency,
                            multiple_pitch_bends=multiple_pitch_bends,
                            # basic-pitch's own default, stated
                            # explicitly rather than relied on: this
                            # post-processing step meaningfully improves
                            # melodic contour, and it should be obvious
                            # here that it's deliberately ON.
                            melodia_trick=True,
                        )
                    )
                except HTTPException:
                    raise
                except Exception as e:
                    logger.error(
                        f"[MIDI_WORKER] Transcription failed ({total} bytes, "
                        f"tier {tiers_run}): {e}",
                        exc_info=True,
                    )
                    raise HTTPException(
                        422,
                        {
                            "reason": "transcription_failed",
                            "message": "Could not transcribe this audio. It may be corrupt or unsupported.",
                        },
                    )

                # Strictly better = more notes found. Tracked rather
                # than break-on-first-nonempty because an early tier
                # scraping together 1-2 notes is not a transcription,
                # and settling for it was the exact production bug this
                # replaced.
                if len(tier_note_events) > len(best_note_events):
                    best_midi_data = tier_midi_data
                    best_note_events = tier_note_events
                    used_onset, used_frame, used_min_len = tier_onset, tier_frame, tier_min_len

                if _is_substantial(tier_note_events):
                    # Good enough. Loosening further from here trades
                    # accuracy for noise - stop while the result is
                    # clean.
                    break

            midi_data = best_midi_data
            note_events = best_note_events

            if tiers_run > 1:
                logger.info(
                    f"[MIDI_WORKER] Cascade ran {tiers_run} tier(s) - best result "
                    f"{len(note_events)} notes at onset={used_onset}, "
                    f"frame={used_frame}, min_note_length={used_min_len}ms "
                    f"(requested: onset={onset_threshold}, frame={frame_threshold}, "
                    f"min_note_length={minimum_note_length}ms)"
                )

        if not note_events:
            # NOT a crash - a legitimate "nothing musical here" result,
            # and now a much stronger claim than it used to be: it means
            # EVERY tier down to the most permissive found nothing, not
            # merely that the default threshold was too strict. Silence,
            # pure noise, spoken word, applause, or heavily percussive
            # material with no pitched content all land here honestly.
            #
            # Same 422 as a real failure because 204 cannot carry a
            # response body per HTTP spec, but the structured `reason`
            # field lets the caller tell the two apart reliably.
            logger.warning(
                f"[MIDI_WORKER] Zero notes across all {tiers_run} cascade tier(s) "
                f"({total} bytes input)"
            )
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

        span = _note_span_seconds(note_events)
        density = (len(note_events) / span) if span > 0 else 0.0
        logger.info(
            f"[MIDI_WORKER] COMPLETE - {len(note_events)} notes over {span:.1f}s "
            f"({density:.2f} notes/sec), {len(midi_bytes)} bytes MIDI from "
            f"{total} bytes audio (onset={used_onset}, frame={used_frame}, "
            f"min_note_length={used_min_len}ms, tiers_run={tiers_run})"
        )
        return Response(
            content=midi_bytes,
            media_type="audio/midi",
            # Diagnostic headers - these are what let you tell, per
            # request and without reading container logs, whether the
            # cascade fired and how rich the result actually was.
            headers={
                "X-Note-Count": str(len(note_events)),
                "X-Note-Span-Seconds": f"{span:.2f}",
                "X-Notes-Per-Second": f"{density:.2f}",
                "X-Onset-Threshold-Used": str(used_onset),
                "X-Frame-Threshold-Used": str(used_frame),
                "X-Min-Note-Length-Used-Ms": str(used_min_len),
                "X-Cascade-Tiers-Run": str(tiers_run),
            },
        )

    finally:
        if input_path and os.path.exists(input_path):
            os.unlink(input_path)