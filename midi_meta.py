"""
midi_meta.py - tempo and key detected from the ORIGINAL audio, written
into a transcribed MIDI.

Detection from the source audio beats inference from transcribed notes:
transcribed notes inherit every transcription mistake, the audio does
not. The free MIDI path uses apply_meta() to replace its hardcoded
120 BPM; the stems path already detects BPM and uses detect_key_quick()
plus key_signature_number() for the key.

Everything here is best-effort and non-fatal: on any failure the MIDI
file survives untouched and the job completes without the metadata.
"""

import logging

import pretty_midi

logger = logging.getLogger(__name__)

_KEYS = ["C", "C#", "D", "D#", "E", "F", "F#", "G", "G#", "A", "A#", "B"]
_ENHARMONIC = {"Db": "C#", "Eb": "D#", "Gb": "F#", "Ab": "G#", "Bb": "A#"}

_KEY_SR = 16000


def detect_key_quick(audio_path: str) -> tuple[str, str] | None:
    """("A", "minor") style tuple, or None. Blocking: call via run_blocking.

    Essentia KeyExtractor on 16 kHz mono, with the same tuned profile the
    key-finder tool uses when available. Deliberately NOT the key-finder's
    full pipeline (HPSS, librosa cross-check, relative-key correction):
    this runs inside every MIDI job, so it trades a little accuracy for a
    couple of seconds instead of ten-plus.
    """
    try:
        from essentia.standard import KeyExtractor, MonoLoader
        audio = MonoLoader(filename=audio_path, sampleRate=_KEY_SR)()
        if audio.size == 0:
            return None
        try:
            from audio_analysis import KEY_PROFILE_TYPE
            extractor = KeyExtractor(profileType=KEY_PROFILE_TYPE, sampleRate=_KEY_SR)
        except Exception:  # noqa: BLE001
            extractor = KeyExtractor(sampleRate=_KEY_SR)
        key, scale, strength = extractor(audio)
        if not key or scale not in ("major", "minor"):
            return None
        return key, scale
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[MIDI_META] key detection skipped: {e}")
        return None


def key_signature_number(key: str, scale: str) -> int | None:
    """pretty_midi key number: 0-11 major C..B, 12-23 minor c..b."""
    k = _ENHARMONIC.get(key, key)
    if k not in _KEYS:
        return None
    n = _KEYS.index(k)
    return n if scale == "major" else n + 12


def apply_meta(audio_path: str, midi_path: str) -> dict:
    """Detect BPM and key from audio_path, rewrite midi_path with the real
    tempo and a key signature event. Blocking: call via run_blocking.

    Note times are stored in seconds and carried over unchanged, so the
    audio and the notes stay in sync; only the tempo grid underneath them
    moves to the real BPM. Returns {"bpm": ..., "key": ...} for whatever
    was actually written, {} if nothing was.
    """
    bpm = None
    try:
        from audio_analysis import _tempocnn_bpm
        b = _tempocnn_bpm(audio_path)
        if b and 40 <= b <= 300:
            bpm = round(float(b), 2)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[MIDI_META] bpm detection skipped: {e}")

    detected = detect_key_quick(audio_path)

    if not bpm and not detected:
        return {}

    out: dict = {}
    try:
        src = pretty_midi.PrettyMIDI(midi_path)
        dst = pretty_midi.PrettyMIDI(initial_tempo=bpm or 120.0)
        for inst in src.instruments:
            dst.instruments.append(inst)
        if bpm:
            out["bpm"] = bpm
        if detected:
            num = key_signature_number(*detected)
            if num is not None:
                dst.key_signature_changes.append(pretty_midi.KeySignature(num, 0.0))
                out["key"] = f"{detected[0]} {detected[1]}"
        dst.write(midi_path)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[MIDI_META] rewrite skipped: {e}")
        return {}
    return out