"""Sheet-music tool package: audio -> engraved notation (PDF/SVG/MusicXML/MIDI).

Public surface for the route layer. The pipeline is:
    runner.run_sheet_job   async job glue (transcribe -> notation -> engrave -> write)
    core.SheetParams       request options
    core.midi_to_sheet     pure MIDI -> sheet (no GPU), for tests/reuse
"""
from __future__ import annotations

from .core import (
    EmptyTranscriptionError,
    EngraveError,
    EngraveInputError,
    EngraveRenderError,
    GridInfo,
    InvalidGridError,
    MidiParseError,
    MidiToXmlError,
    SeparationError,
    SheetError,
    SheetParams,
    SheetResult,
    TranscriptionError,
    audio_to_sheet,
    midi_to_sheet,
)
from .runner import SheetJobError, run_sheet_job

__all__ = [
    "run_sheet_job",
    "midi_to_sheet",
    "audio_to_sheet",
    "SheetParams",
    "SheetResult",
    "GridInfo",
    "SheetError",
    "SheetJobError",
    "TranscriptionError",
    "SeparationError",
    "EmptyTranscriptionError",
    "InvalidGridError",
    "MidiParseError",
    "MidiToXmlError",
    "EngraveError",
    "EngraveInputError",
    "EngraveRenderError",
]