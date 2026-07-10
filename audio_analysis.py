"""
audio_analysis.py - The whole key/BPM detection engine:
- Essentia primary detector (research-grade)
- Librosa fallback + independent cross-check
- Relative major/minor correction (bass-register chroma energy)
- BPM octave (half/double) correction
- Audio trimming to cap peak memory during analysis
"""
import subprocess
from typing import Tuple

import numpy as np
import librosa
from essentia.standard import MonoLoader, KeyExtractor, RhythmExtractor2013

from config import (
    logger,
    FFMPEG_PATH,
    TYPICAL_BPM_MIN,
    TYPICAL_BPM_MAX,
    KEY_DISAGREEMENT_CONFIDENCE_PENALTY,
    BPM_DISAGREEMENT_CONFIDENCE_PENALTY,
)
from utils import (
    release_memory_to_os,
    cleanup_file,
    normalize_key,
    PITCH_CLASSES,
    relative_minor_of_major,
    relative_major_of_minor,
)


# ========== KEY / BPM CORRECTION HELPERS ==========

def correct_relative_major_minor(audio: np.ndarray, sr: int, key: str, scale: str) -> Tuple[str, str, bool]:
    """
    Major/relative-minor pairs (e.g. C major / A minor) share identical note
    content, so profile-correlation key detectors frequently pick the wrong
    one of the pair. This checks which of the two candidate tonics has more
    energy in the BASS register specifically - the bass note is a much more
    reliable indicator of the true tonal center than the full-spectrum note
    histogram, because basslines/root motion tend to emphasize the actual
    tonic far more than incidental melody or harmony notes do.

    Returns (key, scale, was_corrected).
    """
    try:
        # Restrict to roughly C1-B3 (~33-247 Hz) - the bass register - using
        # a CQT chroma with a low fmin and a small number of octaves.
        chroma_bass = librosa.feature.chroma_cqt(
            y=audio, sr=sr,
            fmin=librosa.note_to_hz('C1'),
            n_chroma=12, n_octaves=3,
            hop_length=2048,
        )
        bass_energy = np.sum(chroma_bass, axis=1)
        total = bass_energy.sum()
        if total <= 0 or not np.isfinite(total):
            return key, scale, False
        bass_energy = bass_energy / total

        if scale == 'major':
            major_key, minor_key = key, relative_minor_of_major(key)
        else:
            major_key, minor_key = relative_major_of_minor(key), key

        major_idx = PITCH_CLASSES.index(major_key)
        minor_idx = PITCH_CLASSES.index(minor_key)

        major_bass = bass_energy[major_idx]
        minor_bass = bass_energy[minor_idx]

        # Require the alternate candidate's bass energy to clearly beat the
        # current pick (not just edge it out) before flipping - this is a
        # correction for confident mistakes, not a coin-flip tiebreaker.
        MARGIN = 1.15

        if scale == 'major' and minor_bass > major_bass * MARGIN:
            logger.info(f"Relative-key correction: {major_key} major -> {minor_key} minor "
                        f"(bass energy {minor_bass:.3f} vs {major_bass:.3f})")
            return minor_key, 'minor', True

        if scale == 'minor' and major_bass > minor_bass * MARGIN:
            logger.info(f"Relative-key correction: {minor_key} minor -> {major_key} major "
                        f"(bass energy {major_bass:.3f} vs {minor_bass:.3f})")
            return major_key, 'major', True

        return key, scale, False

    except Exception as e:
        logger.warning(f"Relative major/minor correction skipped (non-fatal): {e}")
        return key, scale, False


def correct_bpm_octave_error(bpm: int) -> Tuple[int, bool]:
    """
    Tempo detectors commonly report exactly half or double the tempo a
    listener would actually tap along to. If the raw BPM falls outside the
    typical [TYPICAL_BPM_MIN, TYPICAL_BPM_MAX] window but doubling or halving
    it lands inside that window, prefer the in-range value.

    Returns (bpm, was_corrected).
    """
    if TYPICAL_BPM_MIN <= bpm <= TYPICAL_BPM_MAX:
        return bpm, False

    doubled = bpm * 2
    halved = bpm / 2

    if bpm < TYPICAL_BPM_MIN and TYPICAL_BPM_MIN <= doubled <= TYPICAL_BPM_MAX:
        logger.info(f"BPM octave correction: {bpm} -> {doubled} (was below typical range)")
        return int(round(doubled)), True

    if bpm > TYPICAL_BPM_MAX and TYPICAL_BPM_MIN <= halved <= TYPICAL_BPM_MAX:
        logger.info(f"BPM octave correction: {bpm} -> {halved} (was above typical range)")
        return int(round(halved)), True

    # Outside typical range but no in-range octave multiple - leave as-is,
    # this is likely a genuinely very slow or very fast track.
    return bpm, False


# ========== DETECTORS ==========

def detect_key_bpm_essentia(audio_path: str, sr: int = 44100) -> Tuple[str, str, float, int, int]:
    audio = None
    try:
        # Load audio
        audio = MonoLoader(filename=audio_path, sampleRate=sr)()

        # Key detection - research-grade accuracy
        key_extractor = KeyExtractor()
        key, scale, strength = key_extractor(audio)
        key = normalize_key(key)

        # BPM detection - very accurate, handles halves/doubles well
        rhythm_extractor = RhythmExtractor2013()
        bpm, _, confidence, _, _ = rhythm_extractor(audio)
        bpm = int(round(bpm))

        # Confidence mapping
        key_conf = min(99, int(strength * 100 + 15))
        bpm_conf = min(99, int(confidence * 100 + 20))

        logger.info(f"Essentia (raw) → Key: {key} {scale} ({key_conf}%), BPM: {bpm} ({bpm_conf}%)")

        # --- Corrections ---
        key, scale, key_corrected = correct_relative_major_minor(audio, sr, key, scale)
        bpm, bpm_corrected = correct_bpm_octave_error(bpm)

        # A correction means the raw detector's first guess was likely
        # wrong; report confidence for the *corrected* value slightly more
        # conservatively than a clean, uncorrected detection would be.
        if key_corrected:
            key_conf = max(50, int(key_conf * 0.9))
        if bpm_corrected:
            bpm_conf = max(50, int(bpm_conf * 0.9))

        logger.info(f"Essentia (final) → Key: {key} {scale} ({key_conf}%), BPM: {bpm} ({bpm_conf}%)")

        return key, scale, key_conf / 100, bpm, bpm_conf

    except Exception as e:
        logger.warning(f"Essentia failed: {e} → Falling back to improved Librosa")
        return fallback_librosa_key_bpm(audio_path)
    finally:
        if audio is not None:
            del audio
        release_memory_to_os()


def fallback_librosa_key_bpm(audio_path: str) -> Tuple[str, str, float, int, int]:
    y = None
    try:
        y, sr = librosa.load(audio_path, sr=44100, mono=True)
        key, scale, key_conf, bpm, bpm_conf = _librosa_key_bpm_from_audio(y, sr)
        return key, scale, key_conf, bpm, bpm_conf
    finally:
        if y is not None:
            del y
        release_memory_to_os()


def _librosa_key_bpm_from_audio(y: np.ndarray, sr: int) -> Tuple[str, str, float, int, int]:
    """Core Librosa key/BPM estimation, factored out so it can be reused
    both as the Essentia fallback AND as a lightweight cross-check."""
    # Enhanced chroma for key (CQT + tuning correction)
    chroma = librosa.feature.chroma_cqt(y=y, sr=sr, hop_length=2048)
    chroma_mean = np.sum(chroma, axis=1)
    chroma_mean /= chroma_mean.sum() + 1e-9

    profiles = {
        'major': np.array([6.35, 2.23, 3.48, 2.33, 4.38, 4.09, 2.52, 5.19, 2.39, 3.66, 2.29, 2.88]),
        'minor': np.array([6.33, 2.68, 3.52, 5.38, 2.60, 3.53, 2.54, 4.75, 3.98, 2.69, 3.34, 3.17]),
    }

    best_score = -1
    best_key, best_scale = 'C', 'major'

    for i in range(12):
        rolled = np.roll(chroma_mean, -i)
        for scale_name, profile in profiles.items():
            corr = np.corrcoef(rolled, profile)[0, 1]
            if np.isnan(corr):
                corr = 0.0
            if corr > best_score:
                best_score = corr
                best_key = PITCH_CLASSES[i]
                best_scale = scale_name

    key_conf = min(96, int(best_score * 100 + 30))

    best_key, best_scale, key_corrected = correct_relative_major_minor(y, sr, best_key, best_scale)
    if key_corrected:
        key_conf = max(50, int(key_conf * 0.9))

    onset_env = librosa.onset.onset_strength(y=y, sr=sr, hop_length=512)
    tempo = librosa.beat.tempo(onset_envelope=onset_env, sr=sr, hop_length=512)
    bpm = int(round(tempo[0] if hasattr(tempo, '__len__') else tempo))
    bpm, bpm_corrected = correct_bpm_octave_error(bpm)
    bpm_conf = 90 if not bpm_corrected else 81

    return normalize_key(best_key), best_scale, key_conf / 100, bpm, bpm_conf


def cross_check_with_librosa(audio_path: str, key: str, scale: str, key_conf: float,
                              bpm: int, bpm_conf: int) -> Tuple[str, str, float, int, int, dict]:
    """
    Runs the Librosa estimator as an independent second opinion against the
    Essentia (primary) result. Essentia's result is always kept as the
    reported answer - Librosa here is only used to raise or lower confidence
    based on agreement, and to surface disagreement to the caller/logs for
    visibility. This never overrides Essentia's key/BPM value, it only
    adjusts how confident we say we are in it.
    """
    agreement = {"key_agrees": None, "bpm_agrees": None}
    y = None
    try:
        y, sr = librosa.load(audio_path, sr=44100, mono=True)
        lb_key, lb_scale, _, lb_bpm, _ = _librosa_key_bpm_from_audio(y, sr)

        key_agrees = (lb_key == key and lb_scale == scale)
        # Allow a small tolerance for BPM (detectors can legitimately differ
        # by a beat or two due to hop-size rounding).
        bpm_agrees = abs(lb_bpm - bpm) <= 2

        agreement["key_agrees"] = key_agrees
        agreement["bpm_agrees"] = bpm_agrees

        if not key_agrees:
            logger.info(f"Cross-check disagreement on key: Essentia={key} {scale} vs Librosa={lb_key} {lb_scale}")
            key_conf = key_conf * KEY_DISAGREEMENT_CONFIDENCE_PENALTY
        else:
            key_conf = min(0.99, key_conf * 1.05)

        if not bpm_agrees:
            logger.info(f"Cross-check disagreement on BPM: Essentia={bpm} vs Librosa={lb_bpm}")
            bpm_conf = int(bpm_conf * BPM_DISAGREEMENT_CONFIDENCE_PENALTY)
        else:
            bpm_conf = min(99, int(bpm_conf * 1.05))

        return key, scale, key_conf, bpm, bpm_conf, agreement

    except Exception as e:
        logger.warning(f"Librosa cross-check skipped (non-fatal): {e}")
        return key, scale, key_conf, bpm, bpm_conf, agreement
    finally:
        if y is not None:
            del y
        release_memory_to_os()


# ========== TRIMMING ==========

def trim_audio_for_analysis(src_path: str, max_seconds: int) -> str:
    trimmed_path = f"{src_path}.trimmed.wav"
    cmd = [
        FFMPEG_PATH, "-y",
        "-i", src_path,
        "-t", str(max_seconds),
        "-ac", "1",
        "-ar", "44100",
        trimmed_path,
    ]
    try:
        subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=60,
        )
        logger.info(f"Trimmed audio to first {max_seconds}s for analysis: {trimmed_path}")
        return trimmed_path
    except Exception as e:
        logger.warning(f"Audio trim failed ({e}), analyzing full file instead")
        cleanup_file(trimmed_path)
        return src_path