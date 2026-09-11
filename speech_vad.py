"""
speech_vad.py - Silero VAD (v6, bundled with faster-whisper, run on
onnxruntime) for the "speech" mode of /silence-remove and
/silence-split. Answers "is someone speaking" instead of "is it quiet",
so music beds, applause and room tone count as gaps and breaths inside
speech don't.
"""
import subprocess
from typing import List, Tuple

import numpy as np

from config import logger, FFMPEG_PATH
from audio_common import AudioToolError, as_audio_only_ffmpeg

VAD_SAMPLE_RATE = 16000
SPEECH_PAD_MS = 150
MIN_SPEECH_MS = 250
DECODE_TIMEOUT_SECONDS = 180


def _decode_mono_16k(input_path: str) -> np.ndarray:
    cmd = as_audio_only_ffmpeg([
        FFMPEG_PATH, "-v", "error",
        "-i", input_path,
        "-ac", "1", "-ar", str(VAD_SAMPLE_RATE),
        "-f", "s16le", "-",
    ])
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=DECODE_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        raise AudioToolError("Timed out while reading the audio for speech detection.")

    if result.returncode != 0 or not result.stdout:
        logger.error(f"[VAD] decode failed on {input_path}: {result.stderr.decode(errors='replace')[-1000:]}")
        raise AudioToolError("Could not read this file. It may be corrupt or in an unsupported format.")

    return np.frombuffer(result.stdout, dtype=np.int16).astype(np.float32) / 32768.0


def speech_spans(input_path: str, min_silence_seconds: float) -> List[Tuple[float, float]]:
    """(start, end) seconds of every speech region, padded by SPEECH_PAD_MS.
    Pauses shorter than min_silence_seconds stay inside a region."""
    from faster_whisper.vad import VadOptions, get_speech_timestamps

    audio = _decode_mono_16k(input_path)
    options = VadOptions(
        threshold=0.5,
        min_speech_duration_ms=MIN_SPEECH_MS,
        min_silence_duration_ms=int(min_silence_seconds * 1000),
        speech_pad_ms=SPEECH_PAD_MS,
    )
    stamps = get_speech_timestamps(audio, options, sampling_rate=VAD_SAMPLE_RATE)
    return [(s["start"] / VAD_SAMPLE_RATE, s["end"] / VAD_SAMPLE_RATE) for s in stamps]


def gaps_between(spans: List[Tuple[float, float]], total_duration: float) -> List[Tuple[float, float]]:
    """Non-speech spans, including lead-in and tail, in the same shape
    silencedetect produces."""
    gaps = []
    cursor = 0.0
    for start, end in spans:
        if start > cursor:
            gaps.append((cursor, start))
        cursor = max(cursor, end)
    if cursor < total_duration:
        gaps.append((cursor, total_duration))
    return gaps


def merge_to_limit(spans: List[Tuple[float, float]], limit: int) -> List[Tuple[float, float]]:
    """Joins spans across the shortest gaps until at most `limit` remain."""
    if len(spans) <= limit:
        return spans
    by_gap = sorted(range(len(spans) - 1), key=lambda i: spans[i + 1][0] - spans[i][1], reverse=True)
    keep_cut = set(by_gap[: limit - 1])
    merged = [list(spans[0])]
    for i in range(len(spans) - 1):
        if i in keep_cut:
            merged.append(list(spans[i + 1]))
        else:
            merged[-1][1] = spans[i + 1][1]
    return [(a, b) for a, b in merged]