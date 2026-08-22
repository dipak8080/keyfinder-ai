"""
audio_analysis.py - The whole key/BPM detection engine:
- Essentia primary detector (research-grade)
- Librosa fallback + independent cross-check
- Relative major/minor correction (bass-register chroma energy)
- BPM octave (half/double) correction
- Audio trimming to cap peak memory during analysis

--------------------------------------------------------------------------
WHAT CHANGED (2026-08-22): ONE LINE IN trim_audio_for_analysis()

The trim command carried no -vn, so a file with embedded cover art
failed to trim and the function fell back to analysing the FULL file.
See that function's own docstring for why this was the most invisible
of the artwork bugs found that day - nothing errored, nothing was
wrong in the result, and the only symptom was memory pressure on a box
that has none to spare.

The detection engine below is untouched.
--------------------------------------------------------------------------
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

# Essentia's KeyExtractor ships several profileType options tuned for
# different musical content ('temperley', 'krumhansl', 'edma', 'edmm',
# etc.) - the previous code used the constructor's default profile, which
# is tuned more toward general/classical tonal material. 'edma' was built
# and validated by the same UPF Music Technology Group research (the lab
# behind Essentia itself, and the one Tunebat's own marketing credits) on
# electronic dance music specifically - directly matching this site's
# actual traffic (melodic house, DJ tracks). This is the single highest-
# leverage lever to try first; if accuracy doesn't improve on your own
# test tracks, 'edmm' (a close variant) is worth trying next.
KEY_PROFILE_TYPE = "bgate"
from audio_common import as_audio_only_ffmpeg
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
    try:
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

    return bpm, False


# ========== DETECTORS ==========

def detect_key_bpm_essentia(audio_path: str, sr: int = 44100) -> Tuple[str, str, float, int, int, np.ndarray, int]:
    """
    Returns (key, scale, key_conf, bpm, bpm_conf, audio, sr) - the loaded
    audio array is now part of the return value instead of being freed
    here, so cross_check_with_librosa() can reuse it rather than reloading
    and resampling the same file from disk a second time. The caller
    (routes.py) is responsible for freeing it once the cross-check is done.
    """
    audio = None
    try:
        audio = MonoLoader(filename=audio_path, sampleRate=sr)()

        try:
            key_extractor = KeyExtractor(profileType=KEY_PROFILE_TYPE)
        except Exception as profile_err:
            logger.warning(f"KeyExtractor profileType='{KEY_PROFILE_TYPE}' unavailable ({profile_err}), using default profile")
            key_extractor = KeyExtractor()
        key, scale, strength = key_extractor(audio)
        key = normalize_key(key)

        rhythm_extractor = RhythmExtractor2013(method="degara")
        bpm, _, confidence, _, _ = rhythm_extractor(audio)
        logger.info(f"Essentia raw (unrounded) BPM, degara method: {bpm:.4f}")
        bpm = int(round(bpm))

        key_conf = min(99, int(strength * 100 + 15))
        bpm_conf = min(99, int(confidence * 100 + 20))

        logger.info(f"Essentia (raw) → Key: {key} {scale} ({key_conf}%), BPM: {bpm} ({bpm_conf}%)")

        key, scale, key_corrected = correct_relative_major_minor(audio, sr, key, scale)
        bpm, bpm_corrected = correct_bpm_octave_error(bpm)

        if key_corrected:
            key_conf = max(50, int(key_conf * 0.9))
        if bpm_corrected:
            bpm_conf = max(50, int(bpm_conf * 0.9))

        logger.info(f"Essentia (final) → Key: {key} {scale} ({key_conf}%), BPM: {bpm} ({bpm_conf}%)")

        return key, scale, key_conf / 100, bpm, bpm_conf, audio, sr

    except Exception as e:
        logger.warning(f"Essentia failed: {e} → Falling back to improved Librosa")
        if audio is not None:
            del audio
        release_memory_to_os()
        key, scale, key_conf, bpm, bpm_conf = fallback_librosa_key_bpm(audio_path)
        # Fallback path re-loads independently since Essentia never produced
        # a usable array on this branch - reload once here so the return
        # shape stays consistent for the caller regardless of which path ran.
        y, fb_sr = librosa.load(audio_path, sr=44100, mono=True)
        return key, scale, key_conf, bpm, bpm_conf, y, fb_sr


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


# How much higher the second detector's own confidence needs to be,
# relative to the primary's, before we actually switch the reported
# answer to its result on disagreement. >1.0 means Essentia is favored
# as a tiebreaker when the two are close (reflecting that it's generally
# the more accurate detector) - this only overrides Essentia when Librosa
# is clearly, not marginally, more confident on this specific track.
CROSS_CHECK_OVERRIDE_MARGIN = 1.10


def cross_check_with_librosa(audio: np.ndarray, sr: int, key: str, scale: str, key_conf: float,
                              bpm: int, bpm_conf: int) -> Tuple[str, str, float, int, int, dict]:
    """
    Runs the Librosa estimator as an independent second opinion against the
    Essentia (primary) result.

    Previously, Essentia's result was ALWAYS kept as the reported answer
    regardless of disagreement - Librosa's opinion only nudged a confidence
    number, so a track where Librosa was actually correct and Essentia
    wasn't still had Essentia's wrong answer returned to the user. This is
    a genuine two-detector ensemble now: on disagreement, whichever
    detector reports meaningfully higher confidence FOR THIS SPECIFIC TRACK
    wins, rather than one detector unconditionally winning every time. The
    margin above prevents flip-flopping on marginal confidence differences
    that don't really indicate one is more trustworthy than the other.

    Takes the ALREADY-LOADED audio array (from detect_key_bpm_essentia)
    instead of a file path - previously this independently reloaded and
    resampled the same file from disk a second time per request, pure
    wasted work since Essentia's MonoLoader output (mono, same sample
    rate) is already numerically equivalent to what librosa.load here
    would have produced anyway.
    """
    agreement = {
        "key_agrees": None, "bpm_agrees": None,
        "key_switched_to_librosa": False, "bpm_switched_to_librosa": False,
    }
    try:
        y = audio
        lb_key, lb_scale, lb_key_conf, lb_bpm, lb_bpm_conf = _librosa_key_bpm_from_audio(y, sr)

        key_agrees = (lb_key == key and lb_scale == scale)
        # Allow a small tolerance for BPM (detectors can legitimately differ
        # by a beat or two due to hop-size rounding).
        bpm_agrees = abs(lb_bpm - bpm) <= 2

        agreement["key_agrees"] = key_agrees
        agreement["bpm_agrees"] = bpm_agrees

        if not key_agrees:
            logger.info(f"Cross-check disagreement on key: Essentia={key} {scale} ({key_conf:.2f}) "
                        f"vs Librosa={lb_key} {lb_scale} ({lb_key_conf:.2f})")
            if lb_key_conf > key_conf * CROSS_CHECK_OVERRIDE_MARGIN:
                logger.info(f"Cross-check override: switching key to Librosa's answer ({lb_key} {lb_scale})")
                key, scale = lb_key, lb_scale
                key_conf = lb_key_conf * KEY_DISAGREEMENT_CONFIDENCE_PENALTY
                agreement["key_switched_to_librosa"] = True
            else:
                key_conf = key_conf * KEY_DISAGREEMENT_CONFIDENCE_PENALTY
        else:
            key_conf = min(0.99, key_conf * 1.05)

        if not bpm_agrees:
            logger.info(f"Cross-check disagreement on BPM: Essentia={bpm} ({bpm_conf}) "
                        f"vs Librosa={lb_bpm} ({lb_bpm_conf})")
            if lb_bpm_conf > bpm_conf * CROSS_CHECK_OVERRIDE_MARGIN:
                logger.info(f"Cross-check override: switching BPM to Librosa's answer ({lb_bpm})")
                bpm = lb_bpm
                bpm_conf = int(lb_bpm_conf * BPM_DISAGREEMENT_CONFIDENCE_PENALTY)
                agreement["bpm_switched_to_librosa"] = True
            else:
                bpm_conf = int(bpm_conf * BPM_DISAGREEMENT_CONFIDENCE_PENALTY)
        else:
            bpm_conf = min(99, int(bpm_conf * 1.05))

        return key, scale, key_conf, bpm, bpm_conf, agreement

    except Exception as e:
        logger.warning(f"Librosa cross-check skipped (non-fatal): {e}")
        return key, scale, key_conf, bpm, bpm_conf, agreement
    finally:
        # No longer frees `audio` here - it's owned by the caller (routes.py
        # loaded it via detect_key_bpm_essentia), which is responsible for
        # freeing it exactly once after both detectors are done with it.
        release_memory_to_os()


# ========== TRIMMING ==========

def trim_audio_for_analysis(src_path: str, max_seconds: int) -> str:
    """
    Cuts the first max_seconds of the file to a mono 44.1kHz WAV and
    returns that path, falling back to the ORIGINAL path if the trim
    fails for any reason.

    THAT FALLBACK IS WHY THIS NEEDED FIXING (2026-08-22), and why the bug
    went unnoticed. The command carried no -vn, so a file with embedded
    cover art - normal for anything saved from Instagram or TikTok,
    anything tagged in iTunes, most purchased music - made ffmpeg try to
    mux a transcoded JPEG into a WAV. WAV holds one stream; it refuses.
    subprocess raised, the except branch caught it, logged at WARNING,
    and returned src_path.

    Nothing failed. The request succeeded, the key and BPM came back
    correct, and the ONLY consequence was that analysis then ran on the
    whole file instead of the first three minutes. On a 6GB box with no
    swap - where this function exists SPECIFICALLY to cap peak memory -
    a 40-minute YouTube download was silently loaded end to end into a
    numpy array, and the sole trace was one WARNING line that reads like
    a corrupt upload.

    A silent fallback that degrades RESOURCE USE rather than correctness
    is the hardest kind of bug to catch, because every symptom points
    somewhere else: memory pressure with no failing request to blame it
    on.

    The fallback itself is kept, and is still right - analysing the full
    file is slower and heavier but CORRECT, and refusing a request
    outright because a trim failed would be a worse trade. What changed
    is that it should now be rare enough that a run of these WARNING
    lines means something real.
    """
    trimmed_path = f"{src_path}.trimmed.wav"
    cmd = as_audio_only_ffmpeg([
        FFMPEG_PATH, "-y",
        "-i", src_path,
        "-t", str(max_seconds),
        "-ac", "1",
        "-ar", "44100",
        trimmed_path,
    ])
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