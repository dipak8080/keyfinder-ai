"""Sheet-music pipeline orchestration. GPU stages are injected, so this
module is pure and testable without a GPU.

Two entry points:
  midi_to_sheet(midi_bytes, params)   notation only (Step 1 + Step 2)
  audio_to_sheet(audio, params, ...)  full pipeline with injected stages

Every failure is a typed error the route maps to HTTP status + credit
outcome. Cheap validation runs before any injected (GPU) stage, so a bad
parameter is rejected before it costs anything.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Callable, Protocol

from .engrave import (
    EngraveError,
    EngraveInputError,
    EngraveRenderError,
    engrave,
)
from .midi_to_xml import (
    EmptyTranscriptionError,
    InvalidGridError,
    MidiParseError,
    MidiToXmlError,
    midi_to_musicxml,
)

__all__ = [
    "SheetParams",
    "GridInfo",
    "SheetResult",
    "midi_to_sheet",
    "audio_to_sheet",
    "SheetError",
    "TranscriptionError",
    "SeparationError",
    # re-exported so the route can catch the whole family from one import
    "EmptyTranscriptionError",
    "InvalidGridError",
    "MidiParseError",
    "MidiToXmlError",
    "EngraveError",
    "EngraveInputError",
    "EngraveRenderError",
]


# --- errors -----------------------------------------------------------------
class SheetError(Exception):
    """Base for pipeline-stage failures raised by this module."""


class TranscriptionError(SheetError):
    """The audio->MIDI stage failed or produced no notes."""


class SeparationError(SheetError):
    """The stem-isolation stage failed."""


# --- config -----------------------------------------------------------------
_INSTRUMENTS = frozenset({"auto", "piano", "guitar", "mix"})
_A4_W, _A4_H = 2100, 2970
_MIN_DIM, _MAX_DIM = 500, 12000
_MIN_SCALE, _MAX_SCALE = 20, 100


@dataclass(frozen=True)
class SheetParams:
    instrument: str = "auto"
    isolate: bool = False
    hand_split: bool = False
    grid: int = 4
    split_point: int | None = None
    time_signature: str = "4/4"
    tempo_bpm: float | None = None      # override; None -> analyze/derive
    key_name: str | None = None         # override; None -> analyze/omit
    want_pdf: bool = True
    want_svg: bool = True
    scale: int = 40
    page_width: int = _A4_W
    page_height: int = _A4_H
    title: str | None = None
    composer: str | None = None


@dataclass(frozen=True)
class GridInfo:
    tempo_bpm: float | None = None
    key_name: str | None = None
    time_signature: str | None = None


@dataclass(frozen=True)
class SheetResult:
    pdf: bytes | None
    svg_pages: tuple[str, ...]
    musicxml: str
    n_notes: int
    n_measures: int
    n_staves: int
    n_pages: int
    tempo_bpm: float
    key_name: str | None
    instrument: str
    separated: bool


# --- injected stage signatures (documentation only) -------------------------
class TranscribeFn(Protocol):
    def __call__(self, audio_path: str, instrument: str) -> bytes: ...


class SeparateFn(Protocol):
    def __call__(self, audio_path: str, instrument: str) -> str | None: ...


class AnalyzeFn(Protocol):
    def __call__(self, audio_path: str) -> GridInfo: ...


# --- validation (cheap, runs before any GPU stage) --------------------------
def _validate_params(p: SheetParams) -> None:
    if p.instrument not in _INSTRUMENTS:
        raise SheetError(f"instrument must be one of {sorted(_INSTRUMENTS)}.")
    if p.isolate and p.instrument not in ("guitar", "mix", "auto"):
        raise SheetError("isolate is only meaningful for a full-mix input.")
    if p.grid not in {1, 2, 3, 4, 6, 8}:
        raise InvalidGridError(f"grid must be one of [1, 2, 3, 4, 6, 8], got {p.grid}.")
    if p.split_point is not None and not (0 <= p.split_point <= 127):
        raise InvalidGridError("split_point must be a MIDI note number 0-127.")
    if not (_MIN_SCALE <= p.scale <= _MAX_SCALE):
        raise SheetError(f"scale must be between {_MIN_SCALE} and {_MAX_SCALE}.")
    if not (_MIN_DIM <= p.page_width <= _MAX_DIM and _MIN_DIM <= p.page_height <= _MAX_DIM):
        raise SheetError("page dimensions out of range.")
    if not (p.want_pdf or p.want_svg):
        raise SheetError("at least one of want_pdf / want_svg must be true.")
    # time_signature + tempo bounds are validated inside midi_to_musicxml,
    # but format-check here so a typo fails before any GPU spend.
    ts = (p.time_signature or "").strip()
    if "/" not in ts or not all(part.strip().isdigit() for part in ts.split("/", 1)):
        raise InvalidGridError(f"Malformed time signature: {p.time_signature!r}. Expected like '4/4'.")


# --- notation stage (pure, no GPU) ------------------------------------------
def midi_to_sheet(
    midi: bytes | str | Path,
    params: SheetParams,
    *,
    separated: bool = False,
) -> SheetResult:
    """MIDI -> engraved sheet. Combines Step 1 (notation) and Step 2
    (engrave) under one result and one error family.
    """
    _validate_params(params)

    notation = midi_to_musicxml(
        midi,
        tempo_bpm=params.tempo_bpm,
        key_name=params.key_name,
        time_signature=params.time_signature,
        grid=params.grid,
        hand_split=params.hand_split,
        split_point=params.split_point,
        title=params.title,
        composer=params.composer,
    )

    rendered = engrave(
        notation.musicxml,
        want_pdf=params.want_pdf,
        want_svg=params.want_svg,
        page_width=params.page_width,
        page_height=params.page_height,
        scale=params.scale,
    )

    return SheetResult(
        pdf=rendered.pdf,
        svg_pages=rendered.svg_pages,
        musicxml=notation.musicxml,
        n_notes=notation.n_notes,
        n_measures=notation.n_measures,
        n_staves=notation.n_staves,
        n_pages=rendered.n_pages,
        tempo_bpm=notation.tempo_bpm,
        key_name=notation.key_name,
        instrument=params.instrument,
        separated=separated,
    )


# --- full pipeline (audio -> sheet, GPU stages injected) --------------------
def audio_to_sheet(
    audio_path: str | Path,
    params: SheetParams,
    *,
    transcribe_fn: TranscribeFn,
    separate_fn: SeparateFn | None = None,
    analyze_fn: AnalyzeFn | None = None,
) -> SheetResult:
    """Full audio -> sheet. Cheap validation first (fails before GPU spend),
    then optional isolation, transcription, grid analysis, and notation.
    """
    _validate_params(params)

    src = Path(audio_path)
    if not src.is_file():
        raise SheetError(f"Audio file not found: {src}")

    work_path = str(src)
    separated = False

    # --- optional isolation ---
    if params.isolate:
        if separate_fn is None:
            raise SheetError("isolation requested but no separator is configured.")
        try:
            stem = separate_fn(work_path, params.instrument)
        except SeparationError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise SeparationError(f"Stem isolation failed: {exc}") from exc
        if stem:
            if not Path(stem).is_file():
                raise SeparationError("Separator returned a path that does not exist.")
            work_path = stem
            separated = True

    # --- transcription (audio -> MIDI) ---
    try:
        midi_bytes = transcribe_fn(work_path, params.instrument)
    except TranscriptionError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise TranscriptionError(f"Transcription failed: {exc}") from exc
    if not midi_bytes:
        raise TranscriptionError("Transcription produced no MIDI data.")

    # --- grid analysis (best-effort; overrides win, else derive) ---
    bpm = params.tempo_bpm
    key_name = params.key_name
    time_sig = params.time_signature
    if analyze_fn is not None and (bpm is None or key_name is None):
        try:
            info = analyze_fn(work_path)
        except Exception:  # noqa: BLE001
            info = None      # analysis is optional; midi_to_musicxml derives tempo from MIDI
        if info is not None:
            if bpm is None and info.tempo_bpm is not None:
                bpm = info.tempo_bpm
            if key_name is None and info.key_name is not None:
                key_name = info.key_name
            if info.time_signature:
                time_sig = info.time_signature

    resolved = replace(params, tempo_bpm=bpm, key_name=key_name, time_signature=time_sig)
    return midi_to_sheet(midi_bytes, resolved, separated=separated)