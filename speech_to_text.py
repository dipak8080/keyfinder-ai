"""
speech_to_text.py - Audio transcription via faster-whisper.

ARCHITECTURALLY DIFFERENT from every other audio-tool module: this one
loads a model into the app's own process memory ONCE at import time
(module-level singleton below), not per-request. Every other tool
(convert, trim, pitch, etc.) spawns a stateless ffmpeg/rubberband
subprocess per call - cheap, no persistent memory cost. Whisper is the
opposite: loading the model is the expensive part, so it happens once
and stays resident for the app's entire lifetime, then each
transcription call reuses the already-loaded model.

CONCURRENCY: routes.py gates calls to transcribe() through a dedicated
semaphore (MAX_CONCURRENT_TRANSCRIPTIONS, default 1) - see config.py's
comment on that constant for why this is capped low and shouldn't be
casually raised on a CPU-only VPS.

Output is NOT a file - transcribe() returns a dict (text/language/
segments) that routes.py stores inline in the job via
jobs.mark_transcription_complete(), not as an output_path.
"""
from faster_whisper import WhisperModel

from config import (
    logger,
    WHISPER_MODEL_SIZE,
    WHISPER_COMPUTE_TYPE,
    WHISPER_DEVICE,
)
from audio_common import AudioToolError

# ========== MODEL SINGLETON ==========
# Loaded once at import time (i.e. once per app process, at startup) -
# NOT inside transcribe(), which would reload the model on every single
# request and make this endpoint unusably slow. This is the same
# "expensive setup once, cheap reuse per-call" pattern as utils.py's
# _executor ThreadPoolExecutor, just for a model instead of a thread pool.
logger.info(
    f"[SPEECH_TO_TEXT] Loading Whisper model '{WHISPER_MODEL_SIZE}' "
    f"(device={WHISPER_DEVICE}, compute_type={WHISPER_COMPUTE_TYPE})..."
)
_model = WhisperModel(WHISPER_MODEL_SIZE, device=WHISPER_DEVICE, compute_type=WHISPER_COMPUTE_TYPE)
logger.info("[SPEECH_TO_TEXT] Whisper model loaded successfully.")


def transcribe(input_path: str) -> dict:
    """
    Transcribes the audio at input_path and returns:
        {
            "text": str,              # full transcript, concatenated
            "language": str,          # auto-detected language code, e.g. "en"
            "language_probability": float,
            "segments": [
                {"start": float, "end": float, "text": str},
                ...
            ]
        }

    Language is auto-detected (not forced) - see config.py comments for
    why. Raises AudioToolError if transcription fails or produces no
    usable output.
    """
    try:
        segments_iter, info = _model.transcribe(input_path, beam_size=5)

        segments = []
        text_parts = []
        for seg in segments_iter:
            segments.append({
                "start": round(seg.start, 2),
                "end": round(seg.end, 2),
                "text": seg.text.strip(),
            })
            text_parts.append(seg.text.strip())

        full_text = " ".join(text_parts).strip()

        if not full_text:
            raise AudioToolError("No speech detected in this audio file.")

        logger.info(
            f"[SPEECH_TO_TEXT] Transcribed {input_path}: "
            f"language={info.language} ({info.language_probability:.2f}), "
            f"{len(segments)} segments, {len(full_text)} chars"
        )

        return {
            "text": full_text,
            "language": info.language,
            "language_probability": round(info.language_probability, 3),
            "segments": segments,
        }

    except AudioToolError:
        raise
    except Exception as e:
        logger.error(f"[SPEECH_TO_TEXT] Transcription failed for {input_path}: {e}", exc_info=True)
        raise AudioToolError("Transcription failed. The file may be corrupt, silent, or in an unsupported format.")