"""
midi_stems.py - "Full mix" / "auto" path for /audio-to-midi-hq.

htdemucs_6s splits the upload, silent stems are skipped, each remaining
stem goes to the engine that is best at it, and the results are merged
into one multi-track MIDI with the detected tempo written in.
"""

import asyncio
import logging
import os
import tempfile
import time
import uuid

import numpy as np
import pretty_midi
import soundfile as sf

from audio_common import AudioToolError
from separation import run_stem_separation, SeparationError
from utils import run_blocking, cleanup_file
from audio_to_midi import convert_to_midi, convert_guitar_to_midi

logger = logging.getLogger(__name__)

SEPARATION_MODEL = "htdemucs_6s"
SILENCE_DBFS = -45.0
DEFAULT_BPM = 120.0

# stem -> (engine, GM program, track name, min_midi, max_midi, monophonic)
STEM_PLAN = {
    "bass":   ("basic-pitch", 33, "Bass",   28, 67, True),
    "piano":  ("transkun",     0, "Piano",  21, 108, False),
    "guitar": ("basic-pitch-guitar", 27, "Guitar", 40, 88, False),
    "vocals": ("basic-pitch", 53, "Vocals", 48, 84, True),
    "other":  ("yourmt3",    80, "Other",  24, 108, False),
}


def _stem_dbfs(path: str) -> float:
    try:
        data, _ = sf.read(path, dtype="float32", always_2d=True)
    except Exception:
        return -120.0
    if data.size == 0:
        return -120.0
    rms = float(np.sqrt(np.mean(np.square(data))))
    return 20.0 * np.log10(rms) if rms > 0 else -120.0


def _detect_bpm(path: str) -> float | None:
    try:
        from audio_analysis import _tempocnn_bpm
        bpm = _tempocnn_bpm(path)
        if bpm and 40 <= bpm <= 300:
            return round(float(bpm), 2)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[MIDI_STEMS] bpm detection skipped: {e}")
    return None


def _hz(midi_note: int) -> float:
    return float(pretty_midi.note_number_to_hz(midi_note))


def _enforce_monophony(notes: list) -> list:
    notes = sorted(notes, key=lambda n: (n.start, -n.velocity))
    out = []
    for n in notes:
        if out and n.start < out[-1].end:
            if n.start - out[-1].start < 0.03:
                if n.velocity > out[-1].velocity:
                    out[-1] = n
                continue
            out[-1].end = n.start
        out.append(n)
    return [n for n in out if n.end - n.start > 0.02]


async def _run_stem(stem: str, path: str, job_id: str, tmp_dir: str,
                    min_pitch, max_pitch, min_note_ms) -> tuple[str, str | None, dict]:
    engine, _, _, lo, hi, _ = STEM_PLAN[stem]
    lo = max(lo, min_pitch) if min_pitch is not None else lo
    hi = min(hi, max_pitch) if max_pitch is not None else hi
    out = os.path.join(tmp_dir, f"{stem}.mid")
    started = time.monotonic()
    try:
        if engine == "transkun":
            import piano_gpu
            stats = await piano_gpu.transcribe_to_midi(path, out, isolate=False, job_id=f"{job_id}-piano")
        elif engine == "yourmt3":
            import midi_hq_gpu
            stats = await midi_hq_gpu.transcribe_to_midi(
                path, out, min_pitch=lo, max_pitch=hi, min_note_ms=min_note_ms,
                instrument="yourmt3", job_id=f"{job_id}-other",
            )
        elif engine == "basic-pitch-guitar":
            stats = await run_blocking(
                convert_guitar_to_midi, path, out,
                min_pitch=lo, max_pitch=hi, min_note_ms=min_note_ms,
            )
        else:
            await run_blocking(
                convert_to_midi, path, out,
                onset_threshold=0.5, frame_threshold=0.3,
                minimum_note_length=float(min_note_ms) if min_note_ms else 80.0,
                minimum_frequency=_hz(lo) * 0.97, maximum_frequency=_hz(hi) * 1.03,
            )
            stats = {}
        stats = dict(stats or {})
        stats["seconds"] = round(time.monotonic() - started, 2)
        return stem, out, stats
    except AudioToolError as e:
        logger.warning(f"[MIDI_STEMS] {stem} via {engine} produced nothing: {e}")
        return stem, None, {"error": str(e)}
    except Exception as e:  # noqa: BLE001
        logger.error(f"[MIDI_STEMS] {stem} via {engine} failed: {e}", exc_info=True)
        return stem, None, {"error": "failed"}


def _merge(results: list, output_path: str, bpm: float,
           min_pitch, max_pitch, min_note_ms) -> dict:
    merged = pretty_midi.PrettyMIDI(initial_tempo=bpm)
    min_dur = (min_note_ms or 0) / 1000.0
    tracks = []

    for stem, path, _ in results:
        if not path or not os.path.exists(path):
            continue
        try:
            src = pretty_midi.PrettyMIDI(path)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[MIDI_STEMS] could not parse {stem} midi: {e}")
            continue
        _, program, name, lo, hi, mono = STEM_PLAN[stem]
        notes = [n for inst in src.instruments if not inst.is_drum for n in inst.notes]
        notes = [
            n for n in notes
            if lo <= n.pitch <= hi
            and (min_pitch is None or n.pitch >= min_pitch)
            and (max_pitch is None or n.pitch <= max_pitch)
            and (n.end - n.start) >= min_dur
        ]
        if mono:
            notes = _enforce_monophony(notes)
        if not notes:
            continue
        if stem == "other" and len(src.instruments) > 1:
            for inst in src.instruments:
                keep = [n for n in inst.notes if n in notes]
                if not keep:
                    continue
                track = pretty_midi.Instrument(program=inst.program, is_drum=False,
                                               name=f"Other - {pretty_midi.program_to_instrument_name(inst.program)}")
                track.notes = sorted(keep, key=lambda n: n.start)
                merged.instruments.append(track)
                tracks.append(_track_stats(track, stem))
        else:
            track = pretty_midi.Instrument(program=program, is_drum=False, name=name)
            track.notes = sorted(notes, key=lambda n: n.start)
            merged.instruments.append(track)
            tracks.append(_track_stats(track, stem))

    if not merged.instruments:
        raise AudioToolError(
            "No notes were detected in any instrument. Try a clip with clearer melodic content."
        )

    merged.write(output_path)
    return {
        "duration_seconds": round(merged.get_end_time(), 2),
        "track_count": len(tracks),
        "note_count": sum(t["notes"] for t in tracks),
        "tracks": tracks,
    }


def _track_stats(track: "pretty_midi.Instrument", stem: str) -> dict:
    return {
        "program": int(track.program),
        "is_drum": False,
        "name": track.name,
        "stem": stem,
        "notes": len(track.notes),
        "low": int(min(n.pitch for n in track.notes)),
        "high": int(max(n.pitch for n in track.notes)),
    }


async def transcribe_stems(
    input_path: str,
    output_path: str,
    job_id: str | None = None,
    min_pitch: int | None = None,
    max_pitch: int | None = None,
    min_note_ms: float | None = None,
) -> dict:
    started = time.monotonic()
    jid = job_id or uuid.uuid4().hex
    tmp_dir = tempfile.mkdtemp(prefix="midistems_")
    stem_paths: dict = {}

    try:
        sep_started = time.monotonic()
        try:
            stem_paths = await run_stem_separation(input_path, jid, model=SEPARATION_MODEL)
        except SeparationError as e:
            logger.error(f"[MIDI_STEMS] separation failed for job {jid}: {e}")
            raise AudioToolError("Could not split this track into instruments. Try again or upload a shorter clip.")
        sep_seconds = time.monotonic() - sep_started

        bpm_task = run_blocking(_detect_bpm, input_path)

        active, skipped = [], []
        for stem in STEM_PLAN:
            path = stem_paths.get(stem)
            if not path or not os.path.exists(path):
                skipped.append(stem)
                continue
            level = _stem_dbfs(path)
            if level < SILENCE_DBFS:
                skipped.append(stem)
                logger.info(f"[MIDI_STEMS] skipping {stem} ({level:.1f} dBFS)")
            else:
                active.append(stem)

        if not active:
            raise AudioToolError("This track appears to be silent or drums-only.")

        results = await asyncio.gather(*[
            _run_stem(stem, stem_paths[stem], jid, tmp_dir, min_pitch, max_pitch, min_note_ms)
            for stem in active
        ])
        bpm = await bpm_task

        stats = _merge(results, output_path, bpm or DEFAULT_BPM, min_pitch, max_pitch, min_note_ms)
        stats["engine"] = "stems"
        stats["bpm"] = bpm
        stats["stems_used"] = [s for s, p, _ in results if p]
        stats["stems_skipped"] = skipped + [s for s, p, _ in results if not p]
        stats["notes_dropped_by_filter"] = 0
        stats["_gpu"] = {
            "fetch_seconds": 0.0,
            "infer_seconds": round(sum(r[2].get("seconds", 0) for r in results), 2),
            "separate_seconds": round(sep_seconds, 2),
            "total_seconds": round(time.monotonic() - started, 2),
            "rtf": None,
        }
        logger.info(
            f"[MIDI_STEMS] job {jid} complete: {stats['note_count']} notes across "
            f"{stats['track_count']} tracks, bpm={bpm}, used={stats['stems_used']}, "
            f"skipped={stats['stems_skipped']}, {stats['_gpu']['total_seconds']}s"
        )
        return stats

    finally:
        for p in stem_paths.values():
            cleanup_file(p)
        try:
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:  # noqa: BLE001
            pass