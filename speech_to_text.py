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

CONCURRENCY: routes/transcribe.py gates calls to transcribe() through a
dedicated semaphore (MAX_CONCURRENT_TRANSCRIPTIONS, default 1) - see
config.py's comment on that constant for why this is capped low and
shouldn't be casually raised on a CPU-only VPS.

Output is NOT a file - transcribe() returns a dict (text/language/
segments) that routes/transcribe.py stores inline in the job via
jobs.mark_transcription_complete(), not as an output_path.

SPEED TIERS: exposed via beam_size (a per-CALL argument), deliberately
NOT by loading multiple model sizes. A second resident model would cost
real RAM for the entire process lifetime on a box that also runs
torch/demucs/essentia. See TRANSCRIPTION_MODE_BEAM_SIZES in config.py.

FAILURE POLICY: a model that fails to load must NOT take down the whole
app. Every other endpoint (convert, trim, separate, ...) is independent
of Whisper, so a bad model download should degrade exactly one tool
rather than crash the process at import. _model is therefore allowed to
be None and transcribe() reports it as a clean, user-facing error.
"""
import os
import time

from faster_whisper import WhisperModel

from config import (
    logger,
    WHISPER_MODEL_SIZE,
    WHISPER_COMPUTE_TYPE,
    WHISPER_DEVICE,
    WHISPER_VAD_FILTER,
    ALLOWED_TRANSCRIPTION_TASKS,
    TRANSCRIPTION_MODE_BEAM_SIZES,
    DEFAULT_TRANSCRIPTION_MODE,
)
from audio_common import AudioToolError


# ========== SUPPORTED LANGUAGES ==========
# Read from the installed faster-whisper package rather than hardcoded,
# so this can never drift out of sync with whatever model version is
# actually deployed. The import target is a private name, hence the
# defensive fallback: if upstream moves it, we degrade to a conservative
# subset instead of failing to import the module entirely.
try:
    from faster_whisper.tokenizer import _LANGUAGE_CODES

    SUPPORTED_LANGUAGES = frozenset(_LANGUAGE_CODES)
    logger.info(
        f"[SPEECH_TO_TEXT] Loaded {len(SUPPORTED_LANGUAGES)} language codes from faster_whisper."
    )
except Exception as _lang_exc:  # pragma: no cover - depends on upstream internals
    logger.warning(
        f"[SPEECH_TO_TEXT] Could not read language codes from faster_whisper "
        f"({_lang_exc.__class__.__name__}: {_lang_exc}); falling back to a static subset. "
        f"Forced-language requests outside this subset will be rejected."
    )
    SUPPORTED_LANGUAGES = frozenset({
        "en", "es", "fr", "de", "it", "pt", "nl", "ru", "zh", "ja", "ko",
        "ar", "hi", "ne", "bn", "ur", "tr", "pl", "uk", "vi", "th", "id",
        "sv", "no", "da", "fi", "he", "el", "cs", "ro", "hu", "ta", "te",
        "ms", "fa",
    })


# Display names for the languages surfaced as primary dropdown options.
# Lives here rather than in routes/ because it is model knowledge, not
# routing concern - and because keeping one source of truth stops the
# Next.js app and the API disagreeing about what is supported.
#
# This is a UI ORDERING HINT, not a restriction: transcribe() accepts
# any code in SUPPORTED_LANGUAGES. The frontend shows this list plus a
# "more languages" expander backed by get_language_options()["all"].
PRIMARY_LANGUAGES = (
    ("en", "English"),      ("es", "Spanish"),      ("fr", "French"),
    ("de", "German"),       ("it", "Italian"),      ("pt", "Portuguese"),
    ("nl", "Dutch"),        ("ru", "Russian"),      ("zh", "Chinese"),
    ("ja", "Japanese"),     ("ko", "Korean"),       ("ar", "Arabic"),
    ("hi", "Hindi"),        ("ne", "Nepali"),       ("bn", "Bengali"),
    ("ur", "Urdu"),         ("ta", "Tamil"),        ("te", "Telugu"),
    ("tr", "Turkish"),      ("pl", "Polish"),       ("uk", "Ukrainian"),
    ("vi", "Vietnamese"),   ("th", "Thai"),         ("id", "Indonesian"),
    ("ms", "Malay"),        ("fa", "Persian"),      ("he", "Hebrew"),
    ("el", "Greek"),        ("cs", "Czech"),        ("ro", "Romanian"),
    ("hu", "Hungarian"),    ("sv", "Swedish"),      ("no", "Norwegian"),
    ("da", "Danish"),       ("fi", "Finnish"),
)


# ========== MODEL SINGLETON ==========
# Loaded once at import time (i.e. once per app process, at startup) -
# NOT inside transcribe(), which would reload the model on every single
# request and make this endpoint unusably slow. This is the same
# "expensive setup once, cheap reuse per-call" pattern as utils.py's
# _executor ThreadPoolExecutor, just for a model instead of a thread pool.
#
# On a cold container this call also DOWNLOADS the model weights, which
# is why the failure path below matters: no network at boot shouldn't
# mean no /convert, no /trim, and no /separate.
logger.info(
    f"[SPEECH_TO_TEXT] Loading Whisper model '{WHISPER_MODEL_SIZE}' "
    f"(device={WHISPER_DEVICE}, compute_type={WHISPER_COMPUTE_TYPE}, "
    f"vad_filter={WHISPER_VAD_FILTER})..."
)

_model = None
_model_load_error = None

try:
    _load_started = time.monotonic()
    _model = WhisperModel(
        WHISPER_MODEL_SIZE,
        device=WHISPER_DEVICE,
        compute_type=WHISPER_COMPUTE_TYPE,
    )
    logger.info(
        f"[SPEECH_TO_TEXT] Whisper model loaded successfully in "
        f"{time.monotonic() - _load_started:.1f}s."
    )
except Exception as _model_exc:
    _model_load_error = f"{_model_exc.__class__.__name__}: {_model_exc}"
    logger.critical(
        f"[SPEECH_TO_TEXT] FAILED to load Whisper model '{WHISPER_MODEL_SIZE}' "
        f"(device={WHISPER_DEVICE}, compute_type={WHISPER_COMPUTE_TYPE}): "
        f"{_model_load_error}. The /speech-to-text endpoint will return 503 "
        f"until this is fixed; all other tools are unaffected.",
        exc_info=True,
    )


# Whether VAD is actually usable. faster-whisper's vad_filter needs
# onnxruntime, which may not be present in every image - probing once at
# import is cheaper and far less confusing than discovering it mid-job
# on a user's upload. If unavailable we transcribe WITHOUT VAD rather
# than failing: slower and slightly more prone to hallucinating text in
# long silences, but a working transcript beats a 500.
_vad_enabled = bool(WHISPER_VAD_FILTER)
if _vad_enabled:
    try:
        import onnxruntime  # noqa: F401

        logger.info("[SPEECH_TO_TEXT] VAD filter enabled (onnxruntime present).")
    except ImportError:
        _vad_enabled = False
        logger.warning(
            "[SPEECH_TO_TEXT] WHISPER_VAD_FILTER is on but onnxruntime is not "
            "installed - VAD disabled. Transcription will be slower and more "
            "prone to hallucinated text during silence. Add 'onnxruntime' to "
            "requirements.txt to enable it."
        )
else:
    logger.info("[SPEECH_TO_TEXT] VAD filter disabled by config.")


# ========== INPUT VALIDATION ==========
# Every message raised here is shown VERBATIM to the end user, so each
# one names the problem and the fix. Internal detail (paths, exception
# classes, model internals) goes to the log, never to the response.

def _normalize_language(language):
    """None/""/"auto" -> None (auto-detect). Otherwise a validated code."""
    if language is None:
        return None
    code = str(language).strip().lower()
    if code in ("", "auto", "auto-detect", "autodetect"):
        return None
    # Tolerate locale-style input ("en-US", "pt_BR") by taking the base
    # code - browsers and third-party callers send these routinely and
    # rejecting them would be needlessly strict.
    if "-" in code or "_" in code:
        code = code.replace("_", "-").split("-", 1)[0]
    if code not in SUPPORTED_LANGUAGES:
        raise AudioToolError(
            f"'{language}' isn't a language we can transcribe. "
            f"Leave the language unset to detect it automatically."
        )
    return code


def _normalize_task(task):
    """'transcribe' (verbatim) or 'translate' (English output)."""
    value = str(task or "transcribe").strip().lower()
    if value not in ALLOWED_TRANSCRIPTION_TASKS:
        raise AudioToolError(
            f"'{task}' isn't a valid mode. Choose 'transcribe' to keep the "
            f"original language, or 'translate' to get English."
        )
    return value


def _normalize_mode(mode):
    """Speed tier -> beam_size. Returns (mode_name, beam_size)."""
    value = str(mode or DEFAULT_TRANSCRIPTION_MODE).strip().lower()
    beam_size = TRANSCRIPTION_MODE_BEAM_SIZES.get(value)
    if beam_size is None:
        valid = ", ".join(sorted(TRANSCRIPTION_MODE_BEAM_SIZES))
        raise AudioToolError(f"'{mode}' isn't a valid speed setting. Choose one of: {valid}.")
    return value, beam_size


def _validate_input_file(input_path):
    """Fail fast on a missing/empty file rather than letting it surface
    as an opaque model error several seconds into the job."""
    if not input_path or not os.path.isfile(input_path):
        logger.error(f"[SPEECH_TO_TEXT] Input file missing at transcribe time: {input_path!r}")
        raise AudioToolError("That file couldn't be read. Please upload it again.")
    if os.path.getsize(input_path) == 0:
        logger.error(f"[SPEECH_TO_TEXT] Input file is zero bytes: {input_path!r}")
        raise AudioToolError("That file appears to be empty. Please upload a different file.")


# ========== TRANSCRIPTION ==========

def transcribe(input_path: str, language: str = None, task: str = "transcribe",
               mode: str = None) -> dict:
    """
    Transcribes the audio at input_path.

    Args:
        input_path: Path to a decodable audio file.
        language:   ISO-639-1 code ("ne", "hi", ...) to FORCE a language,
                    or None/"auto" to auto-detect. Auto-detect is the
                    default, so calling transcribe(path) alone behaves
                    exactly as it did before these options existed.
        task:       "transcribe" (verbatim, source language) or
                    "translate" (model emits English regardless of the
                    spoken language). "translate" is the same forward
                    pass, not a second model or a second pass.
        mode:       Key into TRANSCRIPTION_MODE_BEAM_SIZES. Controls
                    beam_size only; the resident model is identical
                    either way.

    Returns:
        {
            "text": str,                    # full transcript, concatenated
            "language": str,                # detected OR forced code
            "language_probability": float,  # 1.0 when the language was forced
            "language_forced": bool,
            "task": str,
            "mode": str,
            "duration": float,              # audio length in seconds
            "segments": [{"start": float, "end": float, "text": str}, ...]
        }

    Raises:
        AudioToolError: with a message safe to show the end user.
    """
    if _model is None:
        logger.error(
            f"[SPEECH_TO_TEXT] Rejecting request - model never loaded: {_model_load_error}"
        )
        raise AudioToolError(
            "Transcription is temporarily unavailable. Please try again later."
        )

    _validate_input_file(input_path)

    # Validate everything BEFORE touching the model, so a bad parameter
    # costs a 400 rather than a semaphore slot and minutes of CPU.
    language = _normalize_language(language)
    task = _normalize_task(task)
    mode, beam_size = _normalize_mode(mode)

    started = time.monotonic()
    logger.info(
        f"[SPEECH_TO_TEXT] Starting: file={os.path.basename(input_path)}, "
        f"language={language or 'auto'}, task={task}, mode={mode} "
        f"(beam_size={beam_size}), vad={_vad_enabled}"
    )

    try:
        segments_iter, info = _model.transcribe(
            input_path,
            beam_size=beam_size,
            language=language,
            task=task,
            vad_filter=_vad_enabled,
        )

        # NOTE: segments_iter is a GENERATOR - the actual inference work
        # happens as it is consumed below, not on the call above. Any
        # decode/model error therefore surfaces inside this loop, which
        # is why the loop sits inside the try block.
        segments = []
        text_parts = []
        for seg in segments_iter:
            cleaned = seg.text.strip()
            if not cleaned:
                continue
            segments.append({
                "start": round(seg.start, 2),
                "end": round(seg.end, 2),
                "text": cleaned,
            })
            text_parts.append(cleaned)

        full_text = " ".join(text_parts).strip()

        if not full_text:
            # A real, common outcome (silent recording, music-only track,
            # VAD trimming everything) - not a crash. Word it as a result,
            # not a failure, and hint at the likely cause.
            logger.info(
                f"[SPEECH_TO_TEXT] No speech found in {os.path.basename(input_path)} "
                f"after {time.monotonic() - started:.1f}s "
                f"(duration={getattr(info, 'duration', 0):.1f}s, vad={_vad_enabled})"
            )
            raise AudioToolError(
                "No speech was detected in this file. It may be silent, "
                "music-only, or too quiet to pick up."
            )

        elapsed = time.monotonic() - started
        audio_duration = round(float(getattr(info, "duration", 0.0) or 0.0), 2)
        # Realtime factor: how many seconds of audio processed per second
        # of wall clock. The single most useful number for capacity
        # planning and for judging whether a mode/model change helped.
        rtf = (audio_duration / elapsed) if elapsed > 0 else 0.0

        logger.info(
            f"[SPEECH_TO_TEXT] Transcribed {os.path.basename(input_path)}: "
            f"language={info.language} ({info.language_probability:.2f}), "
            f"forced={language or 'auto'}, task={task}, mode={mode}, "
            f"audio={audio_duration:.1f}s, elapsed={elapsed:.1f}s, "
            f"rtf={rtf:.2f}x, {len(segments)} segments, {len(full_text)} chars"
        )

        return {
            "text": full_text,
            # When a language is forced, faster-whisper echoes it back
            # with a meaningless probability - report 1.0 so the frontend
            # never shows a low "confidence" for a user's own explicit
            # choice.
            "language": language or info.language,
            "language_probability": 1.0 if language else round(info.language_probability, 3),
            "language_forced": language is not None,
            "task": task,
            "mode": mode,
            "duration": audio_duration,
            "segments": segments,
        }

    except AudioToolError:
        raise

    except MemoryError:
        # Distinct from the generic path: on a memory-constrained VPS this
        # is a capacity problem, and telling the user to "try a shorter
        # file" is actionable where "the file may be corrupt" is wrong.
        logger.error(
            f"[SPEECH_TO_TEXT] Out of memory transcribing {input_path} "
            f"(mode={mode}, model={WHISPER_MODEL_SIZE})",
            exc_info=True,
        )
        raise AudioToolError(
            "This file was too large to process. Please try a shorter file."
        )

    except Exception as e:
        logger.error(
            f"[SPEECH_TO_TEXT] Transcription failed for {input_path} after "
            f"{time.monotonic() - started:.1f}s "
            f"(language={language or 'auto'}, task={task}, mode={mode}): "
            f"{e.__class__.__name__}: {e}",
            exc_info=True,
        )
        raise AudioToolError(
            "Transcription failed. The file may be corrupt, silent, or in an "
            "unsupported format."
        )


# ========== FRONTEND METADATA ==========

def get_language_options() -> dict:
    """Options payload for the frontend selector, served by
    GET /speech-to-text/languages.

    Single source of truth: the Next.js app fetches this rather than
    hardcoding a list that will silently drift when the model version
    or the installed package changes.
    """
    return {
        "auto_detect_default": True,
        "primary": [
            {"code": code, "name": name}
            for code, name in PRIMARY_LANGUAGES
            if code in SUPPORTED_LANGUAGES
        ],
        "all": sorted(SUPPORTED_LANGUAGES),
        "tasks": list(ALLOWED_TRANSCRIPTION_TASKS),
        "modes": list(TRANSCRIPTION_MODE_BEAM_SIZES.keys()),
        "default_mode": DEFAULT_TRANSCRIPTION_MODE,
    }


def is_available() -> bool:
    """False if the model failed to load. Lets /health and the admin
    dashboard report this endpoint as degraded instead of leaving it to
    be discovered by a user's failed upload."""
    return _model is not None