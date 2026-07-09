# main.py - ULTIMATE ACCURACY: Essentia Powered (Production Hardened + Key/BPM Corrections)
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import yt_dlp
import os
import gc
import ctypes
import subprocess
import time
import uuid
import base64
import logging
import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import librosa
from essentia.standard import MonoLoader, KeyExtractor, RhythmExtractor2013
from typing import Tuple, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="Audio Analysis API - ESSENTIA FIXED", version="12.3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ---------- PRODUCTION CONFIG ----------
MAX_UPLOAD_BYTES = 50 * 1024 * 1024  # 50 MB

YT_DLP_MAX_ATTEMPTS = 3
YT_DLP_BASE_BACKOFF_SECONDS = 1.5  # 1.5s, 3s, 6s (exponential)

YT_BOT_CHECK_MARKERS = (
    "sign in to confirm you're not a bot",
    "sign in to confirm you are not a bot",
    "requested format is not available",
)

FFMPEG_PATH = "/usr/bin/ffmpeg"

# Bumped from 120s -> 180s. Key/BPM detection doesn't need the whole track,
# but 120s was occasionally landing entirely inside an ambient/percussion-only
# intro on some tracks, which starves both detectors of tonal information.
# 180s is still a hard memory cap, just a slightly safer one. Set to None to
# disable trimming and always analyze the full file (best accuracy, highest
# memory use).
ANALYSIS_MAX_SECONDS: Optional[int] = 180

# Most tracks in club/EDM/house/pop contexts land in this BPM range. Used
# only to correct octave errors (half/double-tempo mistakes) - if a detected
# BPM falls outside this window but 2x or 0.5x of it falls inside, we prefer
# the in-range candidate. This is a heuristic, not a genre classifier: it
# will not "fix" a legitimately slow ballad or a legitimately fast DnB track,
# it only nudges values that are suspiciously outside the common range AND
# have an in-range octave-multiple.
TYPICAL_BPM_MIN = 70
TYPICAL_BPM_MAX = 180

# If Essentia and the Librosa cross-check disagree on key, how much to
# discount the reported confidence by (multiplicative).
KEY_DISAGREEMENT_CONFIDENCE_PENALTY = 0.75
BPM_DISAGREEMENT_CONFIDENCE_PENALTY = 0.80

# ---------- CONCURRENCY / LOAD-SHEDDING CONFIG ----------
# FastAPI's event loop is single-threaded for async code. yt_dlp, ffmpeg
# (via subprocess.run), and Essentia/Librosa are all blocking, CPU-bound
# calls - running them directly inside `async def` freezes the WHOLE server
# (including unrelated requests like /health) until that one call finishes.
# Every blocking call is now routed through this thread pool via
# run_blocking() below, so the event loop stays free to accept and queue
# other requests while heavy work happens in a worker thread.
#
# Size this to roughly your CPU core count. Too high just means more
# threads fighting over the same CPU with no real throughput gain - it does
# NOT increase how much work the machine can actually do at once.
THREAD_POOL_WORKERS = int(os.environ.get("THREAD_POOL_WORKERS", "4"))
_executor = ThreadPoolExecutor(max_workers=THREAD_POOL_WORKERS)


async def run_blocking(func, *args, **kwargs):
    """Runs a blocking/synchronous function in the thread pool instead of
    on the event loop, so it doesn't freeze the whole server while it runs."""
    loop = asyncio.get_running_loop()
    call = functools.partial(func, *args, **kwargs)
    return await loop.run_in_executor(_executor, call)


# Hard caps on how many /analyze and /download jobs run AT THE SAME TIME.
# This is the actual thing standing between you and an OOM crash when a lot
# of people hit the API at once - it's independent of THREAD_POOL_WORKERS
# above (that's about not freezing the event loop; this is about not
# loading 50 audio files into RAM simultaneously).
#
# Tune these to your instance's RAM. Essentia/Librosa audio buffers for a
# ~3 min trimmed track are roughly tens of MB each, so on a small Railway
# instance (512MB-1GB), keep these low (2-3) rather than generous.
MAX_CONCURRENT_ANALYSIS = int(os.environ.get("MAX_CONCURRENT_ANALYSIS", "3"))
MAX_CONCURRENT_DOWNLOADS = int(os.environ.get("MAX_CONCURRENT_DOWNLOADS", "3"))

# How long an incoming request is willing to sit in the queue waiting for a
# free analysis/download slot before we give up and return 503 instead of
# holding the connection open forever. This IS your basic "queue" - callers
# who arrive when the server is busy wait up to this long for a slot to
# free up rather than being rejected immediately or piling on unbounded.
QUEUE_WAIT_TIMEOUT_SECONDS = int(os.environ.get("QUEUE_WAIT_TIMEOUT_SECONDS", "30"))

_analysis_semaphore = asyncio.Semaphore(MAX_CONCURRENT_ANALYSIS)
_download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)


async def acquire_slot_or_503(semaphore: asyncio.Semaphore, what: str):
    """
    Waits up to QUEUE_WAIT_TIMEOUT_SECONDS for a free slot on the given
    semaphore. If one frees up in time, the caller proceeds (this IS the
    queueing behavior - excess requests wait here instead of all running
    at once). If the timeout is hit, raises a clean 503 instead of letting
    the request pile on top of an already-overloaded server.
    """
    try:
        await asyncio.wait_for(semaphore.acquire(), timeout=QUEUE_WAIT_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        logger.warning(f"Server busy: no {what} slot freed up within {QUEUE_WAIT_TIMEOUT_SECONDS}s")
        raise HTTPException(
            503,
            f"Server is at capacity ({what} slots full). Please try again shortly."
        )

try:
    _libc = ctypes.CDLL("libc.so.6")
except OSError:
    _libc = None


def release_memory_to_os():
    gc.collect()
    if _libc is not None:
        try:
            _libc.malloc_trim(0)
        except Exception as e:
            logger.warning(f"malloc_trim failed (non-fatal): {e}")


CAMELOT = {
    'C': '8B', 'Db': '3B', 'C#': '3B', 'D': '10B', 'Eb': '5B', 'D#': '5B',
    'E': '12B', 'F': '7B', 'F#': '2B', 'Gb': '2B', 'G': '9B',
    'Ab': '4B', 'G#': '4B', 'A': '11B', 'Bb': '6B', 'A#': '6B', 'B': '1B',
    'Cm': '5A', 'C#m': '12A', 'Dbm': '12A', 'Dm': '7A', 'D#m': '2A', 'Ebm': '2A',
    'Em': '9A', 'Fm': '4A', 'F#m': '11A', 'Gbm': '11A', 'Gm': '6A',
    'G#m': '1A', 'Abm': '1A', 'Am': '8A', 'A#m': '3A', 'Bbm': '3A', 'Bm': '10A'
}

ENHARMONIC = {'C#': 'Db', 'D#': 'Eb', 'F#': 'Gb', 'G#': 'Ab', 'A#': 'Bb'}

# Fixed pitch-class ordering used for all relative-key / bass-chroma math.
# Index arithmetic below relies on this exact order.
PITCH_CLASSES = ['C', 'Db', 'D', 'Eb', 'E', 'F', 'Gb', 'G', 'Ab', 'A', 'Bb', 'B']


def normalize_key(key: str) -> str:
    return ENHARMONIC.get(key, key)


def get_camelot(key: str, scale: str) -> str:
    root = key + ('m' if scale == 'minor' else '')
    return CAMELOT.get(root, "Unknown")


def relative_minor_of_major(major_key: str) -> str:
    """C major's relative minor is A minor, etc. (minor tonic = major tonic - 3 semitones)."""
    idx = PITCH_CLASSES.index(major_key)
    return PITCH_CLASSES[(idx - 3) % 12]


def relative_major_of_minor(minor_key: str) -> str:
    """A minor's relative major is C major, etc. (major tonic = minor tonic + 3 semitones)."""
    idx = PITCH_CLASSES.index(minor_key)
    return PITCH_CLASSES[(idx + 3) % 12]


def cleanup_file(filepath):
    try:
        if filepath and os.path.exists(filepath):
            os.remove(filepath)
            logger.info(f"Cleaned up temp file: {filepath}")
    except Exception as e:
        logger.warning(f"Failed to clean up {filepath}: {e}")


def is_bot_check_error(error_text: str) -> bool:
    lowered = error_text.lower()
    return any(marker in lowered for marker in YT_BOT_CHECK_MARKERS)


def extract_info_with_retry(ydl_opts: dict, url: str):
    """
    NOTE: this function is fully synchronous/blocking (yt_dlp + time.sleep
    backoff). It must always be called via run_blocking() from an async
    endpoint - never awaited or called directly from `async def` code - or
    it will freeze the event loop for the whole server during the retry
    backoff sleeps and the download itself.
    """
    last_exception = None

    for attempt in range(1, YT_DLP_MAX_ATTEMPTS + 1):
        try:
            logger.info(f"yt_dlp extract_info attempt {attempt}/{YT_DLP_MAX_ATTEMPTS} for URL: {url}")
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)
            logger.info(f"yt_dlp extract_info succeeded on attempt {attempt}")
            return info
        except Exception as e:
            last_exception = e
            error_text = str(e)

            if is_bot_check_error(error_text):
                logger.warning(f"Attempt {attempt}: YouTube bot verification triggered.")
            else:
                logger.warning(f"Attempt {attempt} failed: {error_text}")

            if attempt < YT_DLP_MAX_ATTEMPTS:
                backoff = YT_DLP_BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                logger.warning(f"Retrying in {backoff:.1f}s...")
                time.sleep(backoff)
            else:
                logger.error(f"All {YT_DLP_MAX_ATTEMPTS} attempts failed. Last error: {error_text}")

    raise last_exception


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


# ========== API ENDPOINTS ==========

@app.post("/download")
async def download_audio(url: str = Form(...), format: str = Form("mp3")):
    if format not in ["mp3", "wav"]:
        raise HTTPException(400, "Format must be 'mp3' or 'wav'")

    temp_id = str(uuid.uuid4())
    temp_path = os.path.join(UPLOAD_DIR, f"{temp_id}.%(ext)s")
    output_file = os.path.join(UPLOAD_DIR, f"{temp_id}.{format}")

    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': temp_path,
        'quiet': False,
        'verbose': True,
        'noplaylist': True,
        'ffmpeg_location': '/usr/bin/ffmpeg',
        'extractor_args': {
            'youtube': {
                'player_client': ['ios', 'android', 'mweb', 'web']
            }
        },
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': format,
            'preferredquality': '192',
        }],
    }

    cookies_path = os.environ.get('YT_COOKIES_PATH')
    if cookies_path and os.path.exists(cookies_path):
        ydl_opts['cookiefile'] = cookies_path
        logger.info(f"Using cookies file for yt-dlp: {cookies_path}")
    elif cookies_path:
        logger.warning(f"YT_COOKIES_PATH is set to '{cookies_path}' but that file doesn't exist - ignoring")

    proxy_url = os.environ.get('YT_PROXY_URL')
    if proxy_url:
        ydl_opts['proxy'] = proxy_url
        logger.info("Using configured proxy for yt-dlp")

    # Wait (up to QUEUE_WAIT_TIMEOUT_SECONDS) for a free download slot -
    # this is what keeps N simultaneous downloads bounded instead of
    # unbounded, and returns a clean 503 instead of crashing if the server
    # stays saturated past the wait window.
    await acquire_slot_or_503(_download_semaphore, "download")

    audio_data = None
    try:
        try:
            # Offloaded to the thread pool - yt_dlp + ffmpeg postprocessing
            # are fully blocking and would otherwise freeze the event loop.
            info = await run_blocking(extract_info_with_retry, ydl_opts, url)
            title = info.get('title', 'Unknown')
        except Exception as e:
            error_text = str(e)

            if is_bot_check_error(error_text):
                logger.error(f"YouTube bot verification / format restriction blocked download for URL: {url}")
                raise HTTPException(
                    503,
                    "This video is temporarily unavailable for download because YouTube is "
                    "requiring bot verification or is restricting available formats for this client. "
                    "Please try again in a few minutes."
                )

            logger.error(f"Download failed after {YT_DLP_MAX_ATTEMPTS} attempts: {error_text}")
            raise HTTPException(500, f"Failed: {error_text}")

        if not os.path.exists(output_file):
            logger.error(f"Expected output file not found after download: {output_file}")
            raise HTTPException(500, "Failed: audio file was not produced by the downloader")

        with open(output_file, "rb") as f:
            audio_bytes = f.read()
        audio_data = base64.b64encode(audio_bytes).decode('utf-8')
        del audio_bytes

        logger.info(f"Download complete: '{title}' ({format}) → {len(audio_data)} base64 chars")

        return JSONResponse({"title": title, "audio": audio_data, "format": format})

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in /download: {e}", exc_info=True)
        raise HTTPException(500, f"Failed: {str(e)}")
    finally:
        cleanup_file(output_file)
        if audio_data is not None:
            del audio_data
        release_memory_to_os()
        _download_semaphore.release()


@app.post("/analyze")
async def analyze_audio(file: UploadFile = File(...)):
    logger.info(f"Analyzing: {file.filename}")

    file_id = str(uuid.uuid4())
    file_path = os.path.join(UPLOAD_DIR, f"{file_id}_{file.filename}")
    analysis_path = file_path

    # Wait (up to QUEUE_WAIT_TIMEOUT_SECONDS) for a free analysis slot -
    # this bounds how many Essentia/Librosa audio buffers can be in memory
    # at the same time, which is the actual thing that OOM-kills the
    # container under load. Requests beyond the cap wait here (this is your
    # queue) rather than all running - and racing for RAM - at once.
    await acquire_slot_or_503(_analysis_semaphore, "analysis")

    content = None
    try:
        content = await file.read()

        if len(content) == 0:
            raise HTTPException(400, "Empty file")

        if len(content) > MAX_UPLOAD_BYTES:
            size_mb = len(content) / (1024 * 1024)
            logger.warning(f"Rejected upload '{file.filename}': {size_mb:.1f} MB exceeds 50 MB limit")
            raise HTTPException(
                400,
                f"File too large ({size_mb:.1f} MB). Maximum allowed size is 50 MB."
            )

        with open(file_path, "wb") as f:
            f.write(content)

        del content
        content = None
        release_memory_to_os()

        if ANALYSIS_MAX_SECONDS is not None:
            # ffmpeg subprocess call - blocking, offload to thread pool.
            analysis_path = await run_blocking(trim_audio_for_analysis, file_path, ANALYSIS_MAX_SECONDS)

        # Primary: Essentia (pro-level accuracy), with relative-key and
        # BPM-octave corrections already applied inside this call.
        # Offloaded - this is the single most CPU-heavy step in the request.
        key, scale, key_conf, bpm, bpm_conf = await run_blocking(detect_key_bpm_essentia, analysis_path)

        # Cross-check against Librosa as an independent second opinion.
        # This never changes the reported key/BPM - only the confidence,
        # plus an "agreement" flag the frontend can use to show a
        # "low confidence" badge if the two disagree. Also offloaded.
        key, scale, key_conf, bpm, bpm_conf, agreement = await run_blocking(
            cross_check_with_librosa, analysis_path, key, scale, key_conf, bpm, bpm_conf
        )

        camelot = get_camelot(key, scale)
        key_name = f"{key} {scale}"

        result = {
            "key": key_name,
            "camelot": camelot,
            "bpm": bpm,
            "confidence": int(min(0.99, key_conf) * 100),
            "bpm_confidence": min(99, bpm_conf),
            "cross_check": agreement,
        }

        logger.info(f"RESULT: {result}")

        return JSONResponse(result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Analysis error: {e}", exc_info=True)
        raise HTTPException(500, f"Failed: {str(e)}")
    finally:
        cleanup_file(file_path)
        if analysis_path != file_path:
            cleanup_file(analysis_path)
        if content is not None:
            del content
        release_memory_to_os()
        _analysis_semaphore.release()


@app.get("/")
async def root():
    return {
        "status": "Audio Analysis API v12.3 - ESSENTIA FIXED + KEY/BPM CORRECTIONS",
        "accuracy": "Essentia research-grade + relative major/minor correction + BPM octave correction + Librosa cross-check",
        "engine": "Essentia KeyExtractor + RhythmExtractor2013",
        "fixes": [
            "Removed invalid BPMHistogramDescriptors",
            "Proper BPM via RhythmExtractor2013 (confidence included)",
            "Robust fallback with enhanced Librosa",
            "Retry with exponential backoff on yt_dlp failures",
            "Clean 503 on YouTube bot verification / format restriction instead of raw error",
            "Guaranteed temp file cleanup via finally blocks",
            "Explicit memory freeing + gc.collect() + malloc_trim() after each request",
            "Audio trimmed to first 180s before analysis to cap peak memory",
            "50MB upload size limit",
            "Relative major/minor correction using bass-register chroma energy",
            "BPM half/double (octave) error correction against a typical tempo range",
            "Librosa cross-check adjusts confidence (and flags disagreement) without overriding Essentia's answer",
            "All blocking work (yt_dlp, ffmpeg, Essentia, Librosa) offloaded to a thread pool so it never freezes the event loop",
            "Concurrency capped via semaphores (MAX_CONCURRENT_ANALYSIS / MAX_CONCURRENT_DOWNLOADS) to bound peak memory",
            "Requests queue for a free slot up to QUEUE_WAIT_TIMEOUT_SECONDS, then return a clean 503 instead of crashing",
            "Broadened yt_dlp player_client list (ios, android, mweb, web) to reduce 'Requested format is not available' failures",
        ]
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}