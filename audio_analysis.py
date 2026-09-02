"""
audio_analysis.py - key/BPM detection engine.

Key:  Essentia KeyExtractor (edma profile) on the harmonic component, plus a
      librosa profile-match with energy-weighted, segment-voted chroma;
      relative major/minor decided by tonic-triad bass energy.
BPM:  three-way consensus (Essentia RhythmExtractor, Essentia Percival,
      librosa on the percussive onset envelope) with metrical-ratio
      reconciliation. Degara's own confidence is ignored (always 0).
"""
import os
import subprocess
import threading
from typing import Dict, List, Optional, Tuple

import numpy as np
import librosa
from essentia.standard import MonoLoader, KeyExtractor, RhythmExtractor2013

try:
    from essentia.standard import PercivalBpmEstimator
except ImportError:
    PercivalBpmEstimator = None

try:
    from essentia.standard import TempoCNN
except ImportError:
    TempoCNN = None

from config import (
    logger,
    FFMPEG_PATH,
    TYPICAL_BPM_MIN,
    TYPICAL_BPM_MAX,
    KEY_DISAGREEMENT_CONFIDENCE_PENALTY,
)
from audio_common import as_audio_only_ffmpeg
from utils import (
    release_memory_to_os,
    cleanup_file,
    normalize_key,
    PITCH_CLASSES,
    relative_minor_of_major,
    relative_major_of_minor,
)

# Measured on 40 GiantSteps tracks across 6 profiles x 3 input signals:
# bgate/harmonic won (20/40); edma scored 16/40 on the same input.
KEY_PROFILE_TYPE = "bgate"

# 'multifeature' is more accurate and gives a real confidence, ~3x slower.
BPM_METHOD = "degara"

ANALYSIS_SR = 22050
HPSS_MARGIN = 2.0
KEY_SEGMENTS = 3
PREFERRED_BPM_CENTER = 120.0
BPM_MATCH_TOL = 0.04
BPM_METRICAL_RATIOS = (2.0, 0.5, 1.5, 2.0 / 3.0, 4.0 / 3.0, 0.75, 3.0, 1.0 / 3.0)

# Preferred reporting window, chosen by sweeping the GiantSteps tempo set.
# Detectors routinely lock onto the half-time pulse of fast genres (DnB reads
# ~87 instead of 174); folding the answer up into this window matches how DJs
# and Beatport label those tracks. Scores held flat from 92 upward, so the
# bound is not knife-edge. Lower it toward 80 if slow hip-hop matters more
# than fast genres for your traffic.
PREFERRED_BPM_LO = 95
PREFERRED_BPM_HI = 185

# Pretrained TempoCNN (Schreiber & Muller). Needs essentia-tensorflow plus a
# .pb weights file from https://essentia.upf.edu/models/ - set the path in
# TEMPOCNN_MODEL_PATH. Absent or unreadable, the engine just skips it.
TEMPOCNN_MODEL_PATH = os.environ.get("TEMPOCNN_MODEL_PATH", "").strip()
TEMPOCNN_SR = 11025
_tempocnn = None
_tempocnn_tried = False
REL_KEY_MARGIN = 1.05

# When Essentia's own key strength is at least this high, its major/minor call
# is left alone - the bass-energy heuristic is a tie-breaker for uncertain
# tracks, not a veto over a confident detector.
KEY_TRUST_STRENGTH = 85
CROSS_CHECK_OVERRIDE_MARGIN = 1.10

_PROFILES = {
    "major": np.array([6.35, 2.23, 3.48, 2.33, 4.38, 4.09, 2.52, 5.19, 2.39, 3.66, 2.29, 2.88]),
    "minor": np.array([6.33, 2.68, 3.52, 5.38, 2.60, 3.53, 2.54, 4.75, 3.98, 2.69, 3.34, 3.17]),
}

# HPSS is computed once per request in the Essentia stage and handed to the
# cross-check via this small id-keyed stash, so the public signatures used by
# routes/ stay unchanged.
_PREP_CACHE: Dict[int, tuple] = {}
_PREP_LOCK = threading.Lock()
_PREP_CACHE_MAX = 8


# ========== SIGNAL PREP ==========

def _prep_hpss(y: np.ndarray, sr: int) -> Tuple[np.ndarray, np.ndarray, int]:
    y22 = librosa.resample(y, orig_sr=sr, target_sr=ANALYSIS_SR) if sr != ANALYSIS_SR else y
    y22 = np.ascontiguousarray(y22, dtype=np.float32)
    harm, perc = librosa.effects.hpss(y22, margin=HPSS_MARGIN)
    return np.ascontiguousarray(harm, dtype=np.float32), np.ascontiguousarray(perc, dtype=np.float32), ANALYSIS_SR


def _stash_prep(audio: np.ndarray, prep: tuple) -> None:
    with _PREP_LOCK:
        if len(_PREP_CACHE) >= _PREP_CACHE_MAX:
            _PREP_CACHE.pop(next(iter(_PREP_CACHE)))
        _PREP_CACHE[id(audio)] = prep


def _take_prep(audio: np.ndarray) -> Optional[tuple]:
    with _PREP_LOCK:
        return _PREP_CACHE.pop(id(audio), None)


# ========== KEY HELPERS ==========

def _score_keys(vec: np.ndarray) -> Tuple[float, str, str]:
    vec = vec / (vec.sum() + 1e-9)
    best = (-2.0, "C", "major")
    for i in range(12):
        rolled = np.roll(vec, -i)
        for name, prof in _PROFILES.items():
            c = np.corrcoef(rolled, prof)[0, 1]
            if np.isnan(c):
                c = 0.0
            if c > best[0]:
                best = (float(c), PITCH_CLASSES[i], name)
    return best


def _vote_key(chroma: np.ndarray, weights: np.ndarray) -> Tuple[str, str, float, int]:
    n = min(chroma.shape[1], len(weights))
    chroma, weights = chroma[:, :n], weights[:n]
    votes: Dict[Tuple[str, str], float] = {}

    def add(vec, w):
        c, k, s = _score_keys(vec)
        votes[(k, s)] = votes.get((k, s), 0.0) + max(c, 0.0) * w
        return c, (k, s)

    full_c, full_ks = add(chroma @ weights, 1.5)
    seg = max(1, n // KEY_SEGMENTS)
    seg_hits = 0
    for i in range(KEY_SEGMENTS):
        a = i * seg
        b = n if i == KEY_SEGMENTS - 1 else (i + 1) * seg
        if b - a < 4:
            continue
        _, ks = add(chroma[:, a:b] @ weights[a:b], 1.0)
        seg_hits += ks == full_ks
    (k, s), _ = max(votes.items(), key=lambda kv: kv[1])
    return k, s, full_c, seg_hits


def _tonic_center_score(chroma_norm: np.ndarray, tonic_idx: int, scale: str) -> float:
    fifth = (tonic_idx + 7) % 12
    third = (tonic_idx + (4 if scale == "major" else 3)) % 12
    return float(chroma_norm[tonic_idx] + chroma_norm[fifth] * 0.5 + chroma_norm[third] * 0.3)


def relative_key_scores(harm: np.ndarray, hsr: int, key: str, scale: str,
                        strength: Optional[int] = None) -> Optional[dict]:
    chroma_bass = librosa.feature.chroma_cqt(
        y=harm, sr=hsr, fmin=librosa.note_to_hz("C1"),
        n_chroma=12, n_octaves=3, hop_length=2048,
    )
    bass_energy = np.sum(chroma_bass, axis=1)
    total = bass_energy.sum()
    if total <= 0 or not np.isfinite(total):
        return None
    bass_energy = bass_energy / total
    if scale == "major":
        major_key, minor_key = key, relative_minor_of_major(key)
    else:
        major_key, minor_key = relative_major_of_minor(key), key
    return {
        "raw_key": key, "raw_scale": scale, "raw_strength": strength,
        "major_key": major_key, "minor_key": minor_key,
        "major_score": round(_tonic_center_score(bass_energy, PITCH_CLASSES.index(major_key), "major"), 4),
        "minor_score": round(_tonic_center_score(bass_energy, PITCH_CLASSES.index(minor_key), "minor"), 4),
    }


def correct_relative_major_minor(audio: np.ndarray, sr: int, key: str, scale: str,
                                 harm: Optional[np.ndarray] = None,
                                 hsr: Optional[int] = None,
                                 scores: Optional[dict] = None) -> Tuple[str, str, bool]:
    try:
        if scores is None:
            if harm is None:
                harm, _, hsr = _prep_hpss(audio, sr)
            scores = relative_key_scores(harm, hsr, key, scale)
        if scores is None:
            return key, scale, False
        strength = scores.get("raw_strength")
        if strength is not None and strength >= KEY_TRUST_STRENGTH:
            logger.info(f"Relative-key correction skipped: detector confident ({strength}%) on {key} {scale}")
            return key, scale, False

        major_key, minor_key = scores["major_key"], scores["minor_key"]
        major_score, minor_score = scores["major_score"], scores["minor_score"]

        if scale == "minor" and major_score > minor_score * REL_KEY_MARGIN:
            logger.info(f"Relative-key correction: {minor_key} minor -> {major_key} major "
                        f"({major_score:.3f} vs {minor_score:.3f})")
            return major_key, "major", True
        if scale == "major" and minor_score > major_score * REL_KEY_MARGIN:
            logger.info(f"Relative-key correction: {major_key} major -> {minor_key} minor "
                        f"({minor_score:.3f} vs {major_score:.3f})")
            return minor_key, "minor", True
        return key, scale, False
    except Exception as e:
        logger.warning(f"Relative major/minor correction skipped (non-fatal): {e}")
        return key, scale, False


# ========== BPM HELPERS ==========

def correct_bpm_octave_error(bpm: int) -> Tuple[int, bool]:
    if TYPICAL_BPM_MIN <= bpm <= TYPICAL_BPM_MAX:
        return bpm, False
    for factor in (2.0, 0.5, 1.5, 2.0 / 3.0, 3.0, 1.0 / 3.0, 4.0 / 3.0, 0.75):
        cand = bpm * factor
        if TYPICAL_BPM_MIN <= cand <= TYPICAL_BPM_MAX:
            corrected = int(round(cand))
            logger.info(f"BPM range correction: {bpm} -> {corrected} (x{factor:.3f})")
            return corrected, True
    return bpm, False


def _bpm_close(a: float, b: float, tol: float = BPM_MATCH_TOL) -> bool:
    hi = max(a, b)
    return hi > 0 and abs(a - b) <= tol * hi


def _ratio_relates(a: float, b: float, tol: float = BPM_MATCH_TOL) -> bool:
    return any(_bpm_close(a * r, b, tol) for r in BPM_METRICAL_RATIOS)


def consensus_bpm(estimates: List[Tuple[str, Optional[float], int]]) -> Optional[Tuple[int, int, str, List[str]]]:
    """estimates: (name, bpm, priority), higher priority = more trusted.

    Anchors on the most trusted detector, then picks the highest metrically
    linked reading that sits in the preferred window, folding by a metrical
    ratio if nothing lands there. Returns (bpm, conf, mode, supporters).
    """
    vals = [(n, float(b), p) for n, b, p in estimates if b and b > 0 and np.isfinite(b)]
    if not vals:
        return None

    vals.sort(key=lambda t: -t[2])
    anchor_name, anchor, _ = vals[0]

    linked = [(n, b) for n, b, _ in vals if _bpm_close(anchor, b) or _ratio_relates(anchor, b)]
    agreeing = [n for n, b, _ in vals if _bpm_close(anchor, b)]

    # If both less-trusted detectors independently agree with each other and
    # both differ from the anchor, they outweigh it.
    others = [b for n, b, _ in vals[1:]]
    if (len(others) == 2 and _bpm_close(others[0], others[1])
            and not _bpm_close(anchor, others[0])
            and PREFERRED_BPM_LO <= others[0] <= PREFERRED_BPM_HI):
        return int(round(others[0])), 75, "outvoted", [n for n, _, _ in vals[1:]]

    if PREFERRED_BPM_LO <= anchor <= PREFERRED_BPM_HI:
        chosen, mode = anchor, "window"
    else:
        folded = [anchor * r for r in BPM_METRICAL_RATIOS
                  if PREFERRED_BPM_LO <= anchor * r <= PREFERRED_BPM_HI]
        if folded:
            chosen, mode = max(folded), "folded"
        else:
            chosen, _ = correct_bpm_octave_error(int(round(anchor)))
            mode = "range"

    if len(agreeing) >= 3:
        conf = 95
    elif len(agreeing) >= 2:
        conf = 88
    elif mode in ("folded", "range"):
        conf = 62
    else:
        conf = 70

    supporters = agreeing if len(agreeing) > 1 else [anchor_name]
    return int(round(chosen)), conf, mode, supporters


def _estimate_tempo(onset_env: np.ndarray, sr: int, hop_length: int, start_bpm: float) -> float:
    try:
        t = librosa.feature.tempo(onset_envelope=onset_env, sr=sr, hop_length=hop_length,
                                  start_bpm=start_bpm, std_bpm=1.0)
    except AttributeError:
        t = librosa.beat.tempo(onset_envelope=onset_env, sr=sr, hop_length=hop_length, start_bpm=start_bpm)
    return float(t[0] if hasattr(t, "__len__") else t)


def _get_tempocnn():
    global _tempocnn, _tempocnn_tried
    if _tempocnn_tried:
        return _tempocnn
    _tempocnn_tried = True
    if TempoCNN is None:
        logger.warning("TempoCNN unavailable: install essentia-tensorflow (plain essentia lacks it). "
                       "Falling back to DSP tempo detectors.")
        return None
    if not TEMPOCNN_MODEL_PATH:
        logger.warning("TEMPOCNN_MODEL_PATH unset - falling back to DSP tempo detectors.")
        return None
    if not os.path.exists(TEMPOCNN_MODEL_PATH):
        logger.warning(f"TEMPOCNN_MODEL_PATH not found: {TEMPOCNN_MODEL_PATH}")
        return None
    try:
        _tempocnn = TempoCNN(graphFilename=TEMPOCNN_MODEL_PATH)
        logger.info(f"TempoCNN loaded: {TEMPOCNN_MODEL_PATH}")
    except Exception as e:
        logger.warning(f"TempoCNN failed to load (non-fatal): {e}")
        _tempocnn = None
    return _tempocnn


def _tempocnn_bpm(audio_path: str) -> Optional[float]:
    model = _get_tempocnn()
    if model is None:
        return None
    try:
        sig = MonoLoader(filename=audio_path, sampleRate=TEMPOCNN_SR)()
        global_bpm, _, _ = model(sig)
        return float(global_bpm)
    except Exception as e:
        logger.warning(f"TempoCNN inference skipped (non-fatal): {e}")
        return None


def _percival_bpm(audio: np.ndarray, sr: int) -> Optional[float]:
    if PercivalBpmEstimator is None:
        return None
    try:
        return float(PercivalBpmEstimator(sampleRate=sr)(audio))
    except Exception as e:
        logger.warning(f"Percival BPM skipped (non-fatal): {e}")
        return None


# ========== DETECTORS ==========

def detect_key_bpm_essentia(audio_path: str, sr: int = 44100) -> Tuple[str, str, float, int, int, np.ndarray, int]:
    """Returns (key, scale, key_conf, bpm, bpm_conf, audio, sr). Audio is
    returned for cross_check_with_librosa to reuse; the caller frees it."""
    audio = None
    try:
        audio = MonoLoader(filename=audio_path, sampleRate=sr)()

        rhythm_extractor = RhythmExtractor2013(method=BPM_METHOD)
        bpm_raw, _, confidence, _, _ = rhythm_extractor(audio)
        logger.info(f"Essentia raw BPM ({BPM_METHOD}): {bpm_raw:.4f}")
        bpm = int(round(bpm_raw))
        bpm_conf = min(99, int(20 + 79 * min(1.0, confidence / 3.5))) if BPM_METHOD == "multifeature" else 60

        harm, perc, hsr = _prep_hpss(audio, sr)

        try:
            key_extractor = KeyExtractor(profileType=KEY_PROFILE_TYPE, sampleRate=hsr)
        except Exception as profile_err:
            logger.warning(f"KeyExtractor profileType='{KEY_PROFILE_TYPE}' unavailable ({profile_err}), using default")
            key_extractor = KeyExtractor(sampleRate=hsr)
        key, scale, strength = key_extractor(harm)
        key = normalize_key(key)
        key_conf = min(99, int(strength * 100 + 15))

        logger.info(f"Essentia (raw) -> Key: {key} {scale} ({key_conf}%), BPM: {bpm} ({bpm_conf}%)")

        rel_scores = relative_key_scores(harm, hsr, key, scale, strength=key_conf)
        key, scale, key_corrected = correct_relative_major_minor(audio, sr, key, scale, scores=rel_scores)
        bpm, bpm_corrected = correct_bpm_octave_error(bpm)
        if key_corrected:
            key_conf = max(50, int(key_conf * 0.9))
        if bpm_corrected:
            bpm_conf = max(50, int(bpm_conf * 0.9))

        _stash_prep(audio, (harm, perc, hsr, rel_scores, audio_path))
        logger.info(f"Essentia (final) -> Key: {key} {scale} ({key_conf}%), BPM: {bpm} ({bpm_conf}%)")
        return key, scale, key_conf / 100, bpm, bpm_conf, audio, sr

    except Exception as e:
        logger.warning(f"Essentia failed: {e} -> Falling back to Librosa")
        if audio is not None:
            del audio
        release_memory_to_os()
        key, scale, key_conf, bpm, bpm_conf = fallback_librosa_key_bpm(audio_path)
        y, fb_sr = librosa.load(audio_path, sr=44100, mono=True)
        return key, scale, key_conf, bpm, bpm_conf, y, fb_sr


def fallback_librosa_key_bpm(audio_path: str) -> Tuple[str, str, float, int, int]:
    y = None
    try:
        y, sr = librosa.load(audio_path, sr=44100, mono=True)
        return _librosa_key_bpm_from_audio(y, sr)
    finally:
        if y is not None:
            del y
        release_memory_to_os()


def _librosa_key_bpm_from_audio(y: np.ndarray, sr: int, prep: Optional[tuple] = None) -> Tuple[str, str, float, int, int]:
    harm, perc, hsr = prep if prep is not None else _prep_hpss(y, sr)

    chroma = librosa.feature.chroma_cqt(y=harm, sr=hsr, hop_length=2048)
    rms = librosa.feature.rms(y=harm, frame_length=4096, hop_length=2048)[0]
    weights = rms / (rms.max() + 1e-9)

    best_key, best_scale, corr, seg_hits = _vote_key(chroma, weights)
    key_conf = min(96, int(corr * 100 + 30) + 2 * seg_hits)

    best_key, best_scale, key_corrected = correct_relative_major_minor(y, sr, best_key, best_scale, harm=harm, hsr=hsr)
    if key_corrected:
        key_conf = max(50, int(key_conf * 0.9))

    onset_env = librosa.onset.onset_strength(y=perc, sr=hsr, hop_length=256)
    bpm = int(round(_estimate_tempo(onset_env, hsr, hop_length=256, start_bpm=PREFERRED_BPM_CENTER)))
    bpm, bpm_corrected = correct_bpm_octave_error(bpm)
    bpm_conf = 90 if not bpm_corrected else 81

    return normalize_key(best_key), best_scale, key_conf / 100, bpm, bpm_conf


def cross_check_with_librosa(audio: np.ndarray, sr: int, key: str, scale: str, key_conf: float,
                              bpm: int, bpm_conf: int) -> Tuple[str, str, float, int, int, dict]:
    """Second opinion on key (librosa) and a three-way BPM consensus
    (Essentia rhythm, Essentia Percival, librosa percussive-onset tempo)."""
    agreement = {
        "key_agrees": None, "bpm_agrees": None,
        "key_switched_to_librosa": False, "bpm_switched_to_librosa": False,
        "bpm_mode": None, "bpm_votes": {}, "rel_key_scores": None, "essentia_key": None,
    }
    try:
        stashed = _take_prep(audio)
        src_path = None
        if stashed is not None:
            harm, perc, hsr, rel_scores, src_path = stashed
        else:
            harm, perc, hsr = _prep_hpss(audio, sr)
            rel_scores = None
        prep = (harm, perc, hsr)
        agreement["rel_key_scores"] = rel_scores
        agreement["essentia_key"] = f"{key} {scale}"
        lb_key, lb_scale, lb_key_conf, lb_bpm, lb_bpm_conf = _librosa_key_bpm_from_audio(audio, sr, prep=prep)
        pv_bpm = _percival_bpm(audio, sr)
        cnn_bpm = _tempocnn_bpm(src_path) if src_path else None

        key_agrees = (lb_key == key and lb_scale == scale)
        agreement["key_agrees"] = key_agrees
        if not key_agrees:
            logger.info(f"Key disagreement: Essentia={key} {scale} ({key_conf:.2f}) "
                        f"vs Librosa={lb_key} {lb_scale} ({lb_key_conf:.2f})")
            if lb_key_conf > key_conf * CROSS_CHECK_OVERRIDE_MARGIN:
                key, scale = lb_key, lb_scale
                key_conf = lb_key_conf * KEY_DISAGREEMENT_CONFIDENCE_PENALTY
                agreement["key_switched_to_librosa"] = True
            else:
                key_conf = key_conf * KEY_DISAGREEMENT_CONFIDENCE_PENALTY
        else:
            key_conf = min(0.99, key_conf * 1.05)

        agreement["bpm_votes"] = {
            "essentia": bpm, "percival": pv_bpm and round(pv_bpm, 1),
            "librosa": lb_bpm, "tempocnn": cnn_bpm and round(cnn_bpm, 1),
        }
        if cnn_bpm and cnn_bpm > 0:
            recon = (int(round(cnn_bpm)), 93, "tempocnn", ["tempocnn"])
        else:
            recon = consensus_bpm([("essentia", bpm, 3),
                                   ("librosa", lb_bpm, 2), ("percival", pv_bpm, 1)])
        if recon is not None:
            new_bpm, new_conf, mode, supporters = recon
            logger.info(f"BPM consensus ({mode}, {supporters}): {agreement['bpm_votes']} -> {new_bpm}")
            agreement["bpm_mode"] = mode
            agreement["bpm_agrees"] = mode == "window" and len(supporters) > 1
            agreement["bpm_switched_to_librosa"] = new_bpm != bpm and "librosa" in supporters and "essentia" not in supporters
            bpm, bpm_conf = new_bpm, new_conf

        return key, scale, key_conf, bpm, bpm_conf, agreement

    except Exception as e:
        logger.warning(f"Cross-check skipped (non-fatal): {e}")
        return key, scale, key_conf, bpm, bpm_conf, agreement
    finally:
        release_memory_to_os()


# ========== TRIMMING ==========

def trim_audio_for_analysis(src_path: str, max_seconds: int) -> str:
    """First max_seconds to mono 44.1kHz WAV to cap peak memory; falls back to
    the original path if the trim fails."""
    trimmed_path = f"{src_path}.trimmed.wav"
    cmd = as_audio_only_ffmpeg([
        FFMPEG_PATH, "-y", "-i", src_path, "-t", str(max_seconds),
        "-ac", "1", "-ar", "44100", trimmed_path,
    ])
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=60)
        logger.info(f"Trimmed audio to first {max_seconds}s for analysis: {trimmed_path}")
        return trimmed_path
    except Exception as e:
        logger.warning(f"Audio trim failed ({e}), analyzing full file instead")
        cleanup_file(trimmed_path)
        return src_path