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
THE SENSITIVITY CASCADE

basic-pitch exposes three knobs that between them decide whether a note
survives into the output at all: onset_threshold (attack sensitivity),
frame_threshold (sustain sensitivity), minimum_note_length (discards
short notes outright). The published defaults (0.5 / 0.3 / 127.7ms) miss
soft-attack/filtered/fast melodic material entirely or near-entirely -
confirmed in production, not theoretical.

So this runs a cascade: the caller's own settings first, then
progressively more permissive tiers, stopping at the first tier that's
"good enough" (see _is_substantial).

--------------------------------------------------------------------------
FIXED 2026-08-13: TIER SELECTION WAS PICKING THE NOISIEST RESULT

The first version of this cascade selected whichever tier returned the
MOST raw note events when no tier hit the "substantial" bar. That is
backwards: a lower threshold admits both more REAL notes and more FALSE
POSITIVES as noise gets misread as notes, and looser thresholds almost
always win a raw note-count contest regardless of accuracy. In practice
this meant the fallback path silently preferred the single noisiest,
most-permissive tier almost every time a file didn't cleanly clear the
substantial bar - a real regression from the single-attempt version this
cascade replaced, not an improvement.

The fix: results are now ranked cleanest-first, not biggest-first.

  1. Any tier that is genuinely SUBSTANTIAL - the first (least
     permissive, cleanest) one wins. The loop breaks here immediately,
     so more permissive tiers are never even run.
  2. Failing that, any tier that cleared a low MODERATE floor (see
     MODERATE_FLOOR_NOTES) - again the FIRST (cleanest) one wins, not
     the biggest.
  3. Failing that, whatever tier found the most notes at all - this is
     the true last resort, reached only when every tier including the
     most permissive one found next to nothing, so "biggest of a bad
     set" is a reasonable final fallback rather than the default
     behaviour.
  4. If every tier found zero notes, this is an honest "no notes
     detected" result, not a bug.
--------------------------------------------------------------------------
"""
import os
import asyncio
import logging
import tempfile
from dataclasses import dataclass, field
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
# from least to most permissive. Fourth tier added for genuinely soft/
# simple monophonic material (soft-attack synths, pads) that even tier 3
# can miss - this is the noisiest tier in the cascade and is only ever
# reached as a near-last-resort, which is exactly why correct SELECTION
# (see module docstring) matters more than adding more tiers does.
CASCADE_TIERS: List[Tuple[float, float, float]] = [
    (0.35, 0.20, 100.0),
    (0.25, 0.15, 60.0),
    (0.15, 0.10, 30.0),
    (0.10, 0.05, 20.0),
]

# A result is "substantial" (stop cascading, use it, don't go further)
# at this many notes per second of transcribed span, AND at least this
# many notes in absolute terms. Both conditions required - see
# _is_substantial's docstring. Lowered from the original 1.5 nps / 8
# notes: simple, sparse, correctly-transcribed melodies (a single
# instrument playing a clear line) legitimately don't need to be dense
# to be a GOOD result, and the original bar was rejecting clean tier-1/
# tier-2 transcriptions that should have been accepted immediately.
SUBSTANTIAL_NOTES_PER_SECOND = float(os.environ.get("MIDI_SUBSTANTIAL_NPS", "1.2"))
SUBSTANTIAL_MIN_NOTES = int(os.environ.get("MIDI_SUBSTANTIAL_MIN_NOTES", "5"))

# The "moderate" floor used only in the fallback ranking (step 2 in the
# module docstring) - deliberately much lower than SUBSTANTIAL_MIN_NOTES.
# This is what lets an earlier, CLEANER tier that found a handful of
# genuine notes beat a later, noisier tier that found more total notes
# but wasn't itself substantial.
MODERATE_FLOOR_NOTES = int(os.environ.get("MIDI_MODERATE_FLOOR_NOTES", "3"))

app = FastAPI()

# Bounds how many transcriptions actually run at once inside THIS
# process, regardless of caller behaviour. Deliberately independent of
# the main app's _midi_semaphore: a service should never depend on its
# client being well-behaved for its own resource safety.
#
# Worth knowing: the cascade means one request can now run predict()
# up to len(CASCADE_TIERS)+1 times, so a request's worst-case wall time
# is a multiple of what a single-shot inference used to take.
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
    True when a transcription is good enough to stop cascading on -
    the FIRST tier that satisfies this wins, deliberately, so a clean
    result from a stricter/less-permissive tier is always preferred over
    continuing to loosen further.
    """
    if len(note_events) < SUBSTANTIAL_MIN_NOTES:
        return False
    span = _note_span_seconds(note_events)
    if span <= 0:
        return False
    return (len(note_events) / span) >= SUBSTANTIAL_NOTES_PER_SECOND


@dataclass
class _TierResult:
    tier_index: int
    onset: float
    frame: float
    min_len: float
    note_events: list
    midi_data: object


def _select_best(results: List[_TierResult]) -> Optional[_TierResult]:
    """
    Ranks cleanest-first, not biggest-first - see the module docstring's
    "FIXED 2026-08-13" section for the regression this replaces and why
    "most notes" was the wrong selection criterion.

    Tiers are already in cascade order (least to most permissive), so
    "first result satisfying a condition" is equivalent to "least
    permissive / cleanest result satisfying a condition" throughout.
    """
    for r in results:
        if _is_substantial(r.note_events):
            return r
    for r in results:
        if len(r.note_events) >= MODERATE_FLOOR_NOTES:
            return r
    non_empty = [r for r in results if r.note_events]
    if non_empty:
        # True last resort: every tier including the most permissive one
        # found barely anything. "Biggest of a bad set" here, not the
        # default behaviour it was before.
        return max(non_empty, key=lambda r: len(r.note_events))
    return None


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
    multiple_pitch_bends: bool = Form(False),
    x_internal_secret: str = Header(default=""),
):
    _verify_secret(x_internal_secret)

    suffix = os.path.splitext(file.filename or "")[1].lower() or ".audio"
    input_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp_in:
            input_path = tmp_in.name
            total = 0
            while chunk := await file.read(1024 * 1024):
                total += len(chunk)
                if total > MAX_UPLOAD_BYTES:
                    raise HTTPException(413, {"reason": "too_large", "message": "File too large."})
                tmp_in.write(chunk)

        if total == 0:
            raise HTTPException(422, {"reason": "empty", "message": "Empty file."})

        async with _semaphore:
            # Tier one is ALWAYS exactly what the caller asked for (or
            # the defaults). Later tiers included only if genuinely more
            # permissive on at least one axis than what was requested.
            cascade: List[Tuple[float, float, float]] = [
                (onset_threshold, frame_threshold, minimum_note_length)
            ] + [
                (t_onset, t_frame, t_min_len)
                for t_onset, t_frame, t_min_len in CASCADE_TIERS
                if t_onset < onset_threshold or t_min_len < minimum_note_length
            ]

            results: List[_TierResult] = []

            for i, (tier_onset, tier_frame, tier_min_len) in enumerate(cascade):
                try:
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
                            melodia_trick=True,
                        )
                    )
                except HTTPException:
                    raise
                except Exception as e:
                    logger.error(
                        f"[MIDI_WORKER] Transcription failed ({total} bytes, tier {i + 1}): {e}",
                        exc_info=True,
                    )
                    raise HTTPException(
                        422,
                        {
                            "reason": "transcription_failed",
                            "message": "Could not transcribe this audio. It may be corrupt or unsupported.",
                        },
                    )

                span = _note_span_seconds(tier_note_events)
                density = (len(tier_note_events) / span) if span > 0 else 0.0
                logger.info(
                    f"[MIDI_WORKER] tier {i + 1}/{len(cascade)} "
                    f"(onset={tier_onset}, frame={tier_frame}, min_len={tier_min_len}ms) "
                    f"-> {len(tier_note_events)} notes, {density:.2f} notes/sec"
                )

                results.append(_TierResult(
                    tier_index=i,
                    onset=tier_onset,
                    frame=tier_frame,
                    min_len=tier_min_len,
                    note_events=tier_note_events,
                    midi_data=tier_midi_data,
                ))

                if _is_substantial(tier_note_events):
                    # Clean and good enough - stop here. Do NOT keep
                    # loosening past this point; every further tier only
                    # risks admitting more noise.
                    break

            best = _select_best(results)
            midi_data = best.midi_data if best else None
            note_events = best.note_events if best else []
            used_onset = best.onset if best else onset_threshold
            used_frame = best.frame if best else frame_threshold
            used_min_len = best.min_len if best else minimum_note_length
            tiers_run = len(results)

            if tiers_run > 1:
                logger.info(
                    f"[MIDI_WORKER] Cascade ran {tiers_run} tier(s) - selected "
                    f"{len(note_events)} notes at onset={used_onset}, "
                    f"frame={used_frame}, min_note_length={used_min_len}ms "
                    f"(requested: onset={onset_threshold}, frame={frame_threshold}, "
                    f"min_note_length={minimum_note_length}ms)"
                )

        if not note_events:
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