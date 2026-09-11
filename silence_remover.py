"""
silence_remover.py - Strip gaps throughout the audio, not just
leading/trailing.

Two modes:
  music  - ffmpeg silenceremove on a dB threshold (the original tool).
  speech - Silero VAD (speech_vad.py). Keeps only speech regions, so
           music beds, applause and room tone are cut and breaths
           inside a sentence are kept. threshold_db is ignored.

Output format matches input format.
"""
from config import (
    logger,
    FFMPEG_PATH,
    SILENCE_THRESHOLD_MIN_DB,
    SILENCE_THRESHOLD_MAX_DB,
    SILENCE_MIN_DURATION_SECONDS,
    SILENCE_MAX_DURATION_SECONDS,
)
from audio_common import AudioToolError, run_subprocess, probe_duration_seconds
from speech_vad import speech_spans, merge_to_limit

SILENCE_MODES = ("music", "speech")

# Keeps the aselect expression well under Linux's 128 KB single-argument limit.
_MAX_KEPT_SPANS = 2000


def _validate(threshold_db: float, min_duration_seconds: float, mode: str) -> None:
    if mode not in SILENCE_MODES:
        raise AudioToolError(f"mode must be one of: {', '.join(SILENCE_MODES)}.")
    if threshold_db < SILENCE_THRESHOLD_MIN_DB or threshold_db > SILENCE_THRESHOLD_MAX_DB:
        raise AudioToolError(
            f"threshold_db must be between {SILENCE_THRESHOLD_MIN_DB} and {SILENCE_THRESHOLD_MAX_DB}."
        )
    if min_duration_seconds < SILENCE_MIN_DURATION_SECONDS or min_duration_seconds > SILENCE_MAX_DURATION_SECONDS:
        raise AudioToolError(
            f"min_duration_seconds must be between {SILENCE_MIN_DURATION_SECONDS} and {SILENCE_MAX_DURATION_SECONDS}."
        )


def _remove_by_threshold(input_path: str, output_path: str, threshold_db: float, min_duration_seconds: float) -> None:
    silence_filter = (
        f"silenceremove="
        f"start_periods=1:start_silence={min_duration_seconds}:start_threshold={threshold_db}dB:"
        f"stop_periods=-1:stop_silence={min_duration_seconds}:stop_threshold={threshold_db}dB:"
        f"detection=peak"
    )
    run_subprocess([FFMPEG_PATH, "-y", "-i", input_path, "-af", silence_filter, output_path])


def _remove_non_speech(input_path: str, output_path: str, min_duration_seconds: float) -> int:
    spans = speech_spans(input_path, min_duration_seconds)
    if not spans:
        raise AudioToolError(
            "No speech was detected in this file. For music or other non-speech audio, use Music mode."
        )

    spans = merge_to_limit(spans, _MAX_KEPT_SPANS)
    select = "+".join(f"between(t,{start:.3f},{end:.3f})" for start, end in spans)

    # asetnsamples makes ~5 ms frames so aselect cuts close to the VAD boundaries
    audio_filter = f"asetnsamples=n=256,aselect='{select}',asetpts=N/SR/TB"
    run_subprocess([FFMPEG_PATH, "-y", "-i", input_path, "-af", audio_filter, output_path])
    return len(spans)


def remove_silence(
    input_path: str,
    output_path: str,
    threshold_db: float,
    min_duration_seconds: float,
    mode: str = "music",
) -> None:
    mode = (mode or "music").strip().lower()
    _validate(threshold_db, min_duration_seconds, mode)

    if mode == "speech":
        kept = _remove_non_speech(input_path, output_path, min_duration_seconds)
        before = probe_duration_seconds(input_path)
        after = probe_duration_seconds(output_path)
        logger.info(
            f"[SILENCE_REMOVE] {input_path} (speech, min_gap={min_duration_seconds}s, "
            f"{kept} spans, {before:.1f}s -> {after:.1f}s) -> {output_path}"
        )
        return

    _remove_by_threshold(input_path, output_path, threshold_db, min_duration_seconds)
    logger.info(
        f"[SILENCE_REMOVE] {input_path} (music, threshold={threshold_db}dB, "
        f"min_duration={min_duration_seconds}s) -> {output_path}"
    )