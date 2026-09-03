"""MIDI -> clean MusicXML. Pure, I/O-free, deterministic.

Takes a MIDI (bytes or path) plus a metric grid (tempo + time signature)
and returns engravable MusicXML. Raises typed errors the route maps to
HTTP status + credit outcome; never returns silently-wrong output.
"""
from __future__ import annotations

import io
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import pretty_midi
from music21 import (
    chord,
    clef,
    duration,
    key,
    layout,
    metadata,
    meter,
    note,
    stream,
    tempo as m21tempo,
)
from music21.musicxml.m21ToXml import GeneralObjectExporter

__all__ = [
    "midi_to_musicxml",
    "NotationResult",
    "MidiToXmlError",
    "MidiParseError",
    "EmptyTranscriptionError",
    "InvalidGridError",
]


# --- errors -----------------------------------------------------------------
class MidiToXmlError(Exception):
    """Base for every failure this module raises."""


class MidiParseError(MidiToXmlError):
    """The MIDI bytes/file could not be read."""


class EmptyTranscriptionError(MidiToXmlError):
    """No usable notes — silence, noise, or too quiet to transcribe."""


class InvalidGridError(MidiToXmlError):
    """Tempo or time signature is missing or out of sane range."""


# --- limits -----------------------------------------------------------------
_MIN_BPM = 20.0
_MAX_BPM = 400.0
_ALLOWED_GRID = frozenset({1, 2, 3, 4, 6, 8})   # subdivisions per quarter note
_ALLOWED_DENOM = frozenset({1, 2, 4, 8, 16, 32})
_MIN_USEFUL_NOTES = 2
_MAX_NOTES = 60_000
_SPLIT_MIN, _SPLIT_MAX = 48, 72                 # clamp range for auto hand-split
_META_MAX_LEN = 200


@dataclass(frozen=True)
class NotationResult:
    musicxml: str
    n_notes: int
    n_measures: int
    n_staves: int
    tempo_bpm: float
    key_name: str | None


@dataclass(frozen=True)
class _Ev:
    pitch: int
    start: float   # seconds
    end: float     # seconds
    velocity: int


# --- input parsing ----------------------------------------------------------
def _load_pretty_midi(midi: bytes | str | Path) -> pretty_midi.PrettyMIDI:
    try:
        if isinstance(midi, (bytes, bytearray)):
            return pretty_midi.PrettyMIDI(io.BytesIO(bytes(midi)))
        p = Path(midi)
        if not p.is_file():
            raise MidiParseError(f"MIDI file not found: {p}")
        return pretty_midi.PrettyMIDI(str(p))
    except MidiToXmlError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise MidiParseError(f"Could not parse MIDI: {exc}") from exc


def _resolve_tempo(tempo_bpm: float | None, pm: pretty_midi.PrettyMIDI) -> float:
    """Provided tempo wins; else the MIDI's own; else fail loudly."""
    candidate = tempo_bpm
    if candidate is None:
        try:
            _, tempi = pm.get_tempo_changes()
            if len(tempi):
                candidate = float(tempi[0])
        except Exception:  # noqa: BLE001
            candidate = None
    if candidate is None or not math.isfinite(candidate):
        raise InvalidGridError("No tempo available and none could be derived from the MIDI.")
    if not (_MIN_BPM <= candidate <= _MAX_BPM):
        raise InvalidGridError(f"Tempo {candidate:.1f} BPM is outside {_MIN_BPM:.0f}-{_MAX_BPM:.0f}.")
    return float(candidate)


def _parse_time_signature(ts: str) -> tuple[int, int]:
    m = re.fullmatch(r"\s*(\d+)\s*/\s*(\d+)\s*", ts or "")
    if not m:
        raise InvalidGridError(f"Malformed time signature: {ts!r}. Expected like '4/4'.")
    num, den = int(m.group(1)), int(m.group(2))
    if not (1 <= num <= 32) or den not in _ALLOWED_DENOM:
        raise InvalidGridError(f"Unsupported time signature: {num}/{den}.")
    return num, den


def _parse_key(key_name: str | None) -> key.Key | None:
    """Best-effort. Returns None (omit key sig) rather than raising."""
    if not key_name:
        return None
    try:
        raw = key_name.strip()
        m = re.match(r"^([A-Ga-g])\s*([#b\u266f\u266d-]?)\s*(.*)$", raw)
        if not m:
            return None
        letter = m.group(1).upper()
        acc = {"#": "#", "\u266f": "#", "b": "-", "\u266d": "-", "-": "-", "": ""}[m.group(2)]
        rest = m.group(3).lower()
        mode = "minor" if ("min" in rest or "m" == rest) else "major"
        return key.Key(letter + acc, mode)
    except Exception:  # noqa: BLE001
        return None


def _clean_meta(value: str | None) -> str | None:
    if not value:
        return None
    v = " ".join(str(value).split()).strip()
    return v[:_META_MAX_LEN] or None


# --- events -----------------------------------------------------------------
def _extract_events(pm: pretty_midi.PrettyMIDI) -> list[_Ev]:
    evs: list[_Ev] = []
    for inst in pm.instruments:
        if inst.is_drum:
            continue
        for n in inst.notes:
            if n.end > n.start and 0 <= n.pitch <= 127:
                evs.append(_Ev(int(n.pitch), float(n.start), float(n.end), int(n.velocity)))
    if len(evs) > _MAX_NOTES:
        raise MidiToXmlError(f"MIDI has {len(evs)} notes (max {_MAX_NOTES}); likely not music.")
    if len(evs) < _MIN_USEFUL_NOTES:
        raise EmptyTranscriptionError("The audio produced no usable notes to notate.")
    evs.sort(key=lambda e: (e.start, e.pitch))
    return evs


def _snap(ql: float, grid: int) -> float:
    return round(ql * grid) / grid


def _quantize(events: Sequence[_Ev], bpm: float, grid: int) -> list[tuple[float, float, list[int], int]]:
    """Seconds -> quarterLength on the grid. Groups same-onset pitches into
    chords and truncates each to the next onset so a staff never overlaps.

    Returns (onset_ql, dur_ql, [pitches], velocity) sorted by onset.
    """
    step = 1.0 / grid
    per_onset: dict[float, dict] = {}
    for e in events:
        on = _snap(e.start * bpm / 60.0, grid)
        raw = _snap((e.end - e.start) * bpm / 60.0, grid)
        dur = max(raw, step)
        g = per_onset.setdefault(on, {"pitches": set(), "dur": step, "vel": 0})
        g["pitches"].add(e.pitch)
        g["dur"] = max(g["dur"], dur)
        g["vel"] = max(g["vel"], e.velocity)

    onsets = sorted(per_onset)
    out: list[tuple[float, float, list[int], int]] = []
    for i, on in enumerate(onsets):
        g = per_onset[on]
        cap = (onsets[i + 1] - on) if i + 1 < len(onsets) else g["dur"]
        dur = max(min(g["dur"], cap), step)
        out.append((on, dur, sorted(g["pitches"]), g["vel"]))
    return out


def _median_pitch(quant: Sequence[tuple[float, float, list[int], int]]) -> int:
    allp = sorted(p for _, _, ps, _ in quant for p in ps)
    return allp[len(allp) // 2] if allp else 60


def _split_staves(
    quant: Sequence[tuple[float, float, list[int], int]],
    hand_split: bool,
    split_point: int | None,
) -> tuple[list, list | None]:
    """Returns (primary, secondary). secondary is None for single staff."""
    if not hand_split:
        return list(quant), None
    sp = split_point if split_point is not None else max(_SPLIT_MIN, min(_SPLIT_MAX, _median_pitch(quant)))
    treble, bass = [], []
    for on, dur, ps, vel in quant:
        hi = [p for p in ps if p >= sp]
        lo = [p for p in ps if p < sp]
        if hi:
            treble.append((on, dur, hi, vel))
        if lo:
            bass.append((on, dur, lo, vel))
    if not treble or not bass:          # degenerate split -> single staff
        return list(quant), None
    return treble, bass


# --- notation build ---------------------------------------------------------
def _make_element(dur_ql: float, pitches: list[int], velocity: int):
    d = duration.Duration(quarterLength=dur_ql)
    el = note.Note(pitches[0], duration=d) if len(pitches) == 1 else chord.Chord(pitches, duration=d)
    try:
        el.volume.velocity = max(1, min(127, velocity))
    except Exception:  # noqa: BLE001
        pass
    return el


def _build_part(
    quant: Sequence[tuple[float, float, list[int], int]],
    ts: tuple[int, int],
    ks: key.Key | None,
    bpm: float | None,
    clef_obj,
    part_cls=stream.Part,
) -> stream.Part:
    part = part_cls()
    part.insert(0.0, clef_obj)
    part.insert(0.0, meter.TimeSignature(f"{ts[0]}/{ts[1]}"))
    if ks is not None:
        part.insert(0.0, ks)
    if bpm is not None:
        part.insert(0.0, m21tempo.MetronomeMark(number=round(bpm)))
    for on, dur, ps, vel in quant:
        part.insert(on, _make_element(dur, ps, vel))
    try:
        return part.makeNotation(inPlace=False)
    except Exception as exc:  # noqa: BLE001
        raise MidiToXmlError(f"Notation layout failed: {exc}") from exc


def _normalize_ids(xml: str) -> str:
    """Rewrite music21's random Pxxxx part ids to sequential P1, P2, ...
    in order of first appearance, keeping score-part/part references
    consistent. Makes output byte-identical across runs.
    """
    seen: dict[str, str] = {}

    def repl(mo: re.Match) -> str:
        old = mo.group(1)
        if old not in seen:
            seen[old] = f"P{len(seen) + 1}"
        return f'id="{seen[old]}"'

    return re.sub(r'id="(P[0-9a-fA-F]+)"', repl, xml)


def _to_musicxml(score: stream.Score) -> str:
    try:
        raw = GeneralObjectExporter(score).parse()
    except Exception as exc:  # noqa: BLE001
        raise MidiToXmlError(f"MusicXML export failed: {exc}") from exc
    xml = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
    # Strip the auto timestamp and normalise part ids so identical input
    # yields byte-identical output (safe to retry after a failed charge).
    xml = re.sub(r"<encoding-date>.*?</encoding-date>\s*", "", xml)
    return _normalize_ids(xml)


# --- public API -------------------------------------------------------------
def midi_to_musicxml(
    midi: bytes | str | Path,
    *,
    tempo_bpm: float | None,
    key_name: str | None = None,
    time_signature: str = "4/4",
    grid: int = 4,
    hand_split: bool = False,
    split_point: int | None = None,
    title: str | None = None,
    composer: str | None = None,
) -> NotationResult:
    """Convert a MIDI into MusicXML.

    Raises MidiParseError, EmptyTranscriptionError, InvalidGridError, or
    MidiToXmlError. Never returns partial/blank output.
    """
    if grid not in _ALLOWED_GRID:
        raise InvalidGridError(f"grid must be one of {sorted(_ALLOWED_GRID)}, got {grid}.")
    if split_point is not None and not (0 <= split_point <= 127):
        raise InvalidGridError("split_point must be a MIDI note number 0-127.")

    pm = _load_pretty_midi(midi)
    bpm = _resolve_tempo(tempo_bpm, pm)
    ts = _parse_time_signature(time_signature)
    ks = _parse_key(key_name)

    events = _extract_events(pm)
    quant = _quantize(events, bpm, grid)
    if len(quant) < _MIN_USEFUL_NOTES:
        raise EmptyTranscriptionError("Notes collapsed to nothing after quantization.")

    primary, secondary = _split_staves(quant, hand_split, split_point)

    score = stream.Score()
    md = metadata.Metadata()
    md.title = _clean_meta(title) or "Transcription"
    if _clean_meta(composer):
        md.composer = _clean_meta(composer)
    score.insert(0.0, md)

    if secondary is None:
        single_clef = clef.BassClef() if _median_pitch(quant) < 60 else clef.TrebleClef()
        score.insert(0.0, _build_part(primary, ts, ks, bpm, single_clef))
        n_staves = 1
    else:
        top = _build_part(primary, ts, ks, bpm, clef.TrebleClef(), part_cls=stream.PartStaff)
        bottom = _build_part(secondary, ts, ks, None, clef.BassClef(), part_cls=stream.PartStaff)
        score.insert(0.0, top)
        score.insert(0.0, bottom)
        score.insert(0.0, layout.StaffGroup([top, bottom], symbol="brace", barTogether=True))
        n_staves = 2

    xml = _to_musicxml(score)

    first_part = score.parts[0] if score.parts else None
    n_measures = len(first_part.getElementsByClass(stream.Measure)) if first_part else 0

    return NotationResult(
        musicxml=xml,
        n_notes=sum(len(ps) for _, _, ps, _ in quant),
        n_measures=n_measures,
        n_staves=n_staves,
        tempo_bpm=bpm,
        key_name=str(ks) if ks is not None else None,
    )