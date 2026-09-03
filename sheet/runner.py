"""Job glue for the sheet-music tool.

run_sheet_job is the async `work` coroutine handed to _run_tool_job by the
route. It does the async GPU transcription (engine-routed), runs the
blocking notation/engrave stage in a thread, writes the output files, and
returns a stats dict for mark_data_complete.

Repo clients (midi_hq_gpu, piano_gpu, audio_analysis) are imported lazily
inside the default stage functions and are injectable, so this module is
importable and testable without them and Step 5's Transkun worker slots in
without changing anything here.
"""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Awaitable, Callable

from .core import GridInfo, SheetParams, SheetResult, midi_to_sheet

log = logging.getLogger("sheet.runner")

__all__ = ["run_sheet_job", "SheetJobError"]


class SheetJobError(Exception):
    """A stage of the job failed after the pipeline handed back control."""


# --- default stage wiring (lazy repo imports) -------------------------------
async def _default_transcribe(
    input_path: str,
    out_midi_path: str,
    params: SheetParams,
    job_id: str | None,
) -> dict:
    """Route audio->MIDI to the best engine and write it to out_midi_path.

    piano  -> Transkun (piano_gpu), falling back to YourMT3 if the piano
              worker is not configured.
    others -> YourMT3 (midi_hq_gpu), which also handles isolation.

    Returns {"engine": str, "separated": bool, "stats": dict}.
    """
    instrument = (params.instrument or "auto").lower()

    if instrument == "piano":
        try:
            import piano_gpu  # Step 5; optional
            if piano_gpu.is_available():
                stats = await piano_gpu.transcribe_to_midi(
                    input_path, out_midi_path,
                    isolate=params.isolate, job_id=job_id,
                )
                return {"engine": "transkun", "separated": bool(params.isolate), "stats": stats or {}}
            log.info("[sheet] piano_gpu present but not configured; using YourMT3")
        except ImportError:
            log.info("[sheet] piano_gpu not installed yet; using YourMT3 for piano")

    import midi_hq_gpu
    stats = await midi_hq_gpu.transcribe_to_midi(
        input_path, out_midi_path,
        instrument=instrument, isolate=params.isolate, job_id=job_id,
    )
    separated = bool((stats or {}).get("isolated", params.isolate))
    return {"engine": (stats or {}).get("engine", "yourmt3"), "separated": separated, "stats": stats or {}}


# Cap analysis input: tempo/key are stable, so the first two minutes are
# plenty and keep HPSS + TempoCNN memory/time bounded on long uploads.
_ANALYSIS_MAX_SECONDS = 120


def _default_analyze(audio_path: str) -> GridInfo:
    """Tempo + key via the existing two-stage analysis stack.

    detect_key_bpm_essentia gives a first key + (Degara) BPM and returns the
    decoded audio; cross_check_with_librosa is the SECOND stage that runs
    TempoCNN and reconciles the tempo - that is where the accurate BPM comes
    from, so both must run. Best-effort: any failure falls back to librosa
    and finally to an empty GridInfo (midi_to_sheet then derives tempo from
    the MIDI). Never raises.
    """
    try:
        import audio_analysis as aa
    except Exception:  # noqa: BLE001
        return GridInfo()

    trimmed: str | None = None
    try:
        trimmed = aa.trim_audio_for_analysis(audio_path, _ANALYSIS_MAX_SECONDS)
        key, scale, key_conf, bpm, bpm_conf, audio, sr = aa.detect_key_bpm_essentia(trimmed)
        try:
            # Second stage: TempoCNN + three-way consensus. This is the
            # accurate tempo path; without it we get the Degara BPM only.
            key, scale, key_conf, bpm, bpm_conf, _agree = aa.cross_check_with_librosa(
                audio, sr, key, scale, key_conf, bpm, bpm_conf
            )
        finally:
            del audio
        return GridInfo(
            tempo_bpm=float(bpm) if bpm and bpm > 0 else None,
            key_name=_format_key(key, scale),
            time_signature=None,
        )
    except Exception:  # noqa: BLE001
        try:
            key, scale, _kc, bpm, _bc = aa.fallback_librosa_key_bpm(audio_path)
            return GridInfo(
                tempo_bpm=float(bpm) if bpm and bpm > 0 else None,
                key_name=_format_key(key, scale),
                time_signature=None,
            )
        except Exception:  # noqa: BLE001
            return GridInfo()
    finally:
        if trimmed and trimmed != audio_path:
            try:
                Path(trimmed).unlink(missing_ok=True)
            except Exception:  # noqa: BLE001
                pass


def _format_key(key: str | None, scale: str | None) -> str | None:
    if not key:
        return None
    mode = "minor" if (scale or "").lower().startswith("min") else "major"
    return f"{key} {mode}"


# --- grid resolution --------------------------------------------------------
def _resolve_grid(params: SheetParams, info: GridInfo | None) -> SheetParams:
    """Explicit params win; else fill from analysis; else leave for
    midi_to_sheet to derive from the MIDI.
    """
    bpm = params.tempo_bpm
    key_name = params.key_name
    time_sig = params.time_signature
    if info is not None:
        if bpm is None and info.tempo_bpm is not None:
            bpm = info.tempo_bpm
        if key_name is None and info.key_name is not None:
            key_name = info.key_name
        if info.time_signature:
            time_sig = info.time_signature
    from dataclasses import replace
    return replace(params, tempo_bpm=bpm, key_name=key_name, time_signature=time_sig)


# --- output writing ---------------------------------------------------------
def _write_outputs(
    result: SheetResult,
    *,
    pdf_path: str | None,
    svg_path: str | None,
    xml_path: str | None,
) -> dict:
    """Write the requested artifacts atomically-ish (temp + rename) and
    return the map of format -> path actually written.
    """
    written: dict[str, str] = {}

    if pdf_path and result.pdf is not None:
        _atomic_write_bytes(pdf_path, result.pdf)
        written["pdf"] = pdf_path
    if svg_path and result.svg_pages:
        # One combined SVG file (first page) for preview + a marker that the
        # full page list is in result_data; multi-page SVG download is served
        # per-page by the route from result.svg_pages if needed.
        _atomic_write_text(svg_path, result.svg_pages[0])
        written["svg"] = svg_path
    if xml_path and result.musicxml:
        _atomic_write_text(xml_path, result.musicxml)
        written["musicxml"] = xml_path

    if not written:
        raise SheetJobError("Pipeline produced no writable output.")
    return written


def _atomic_write_bytes(path: str, data: bytes) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "wb") as f:
        f.write(data)
    Path(tmp).replace(path)


def _unlink_quietly(path: str) -> None:
    try:
        Path(path).unlink(missing_ok=True)
    except Exception:  # noqa: BLE001
        pass


def _atomic_write_text(path: str, text: str) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    Path(tmp).replace(path)


# --- the job ----------------------------------------------------------------
async def run_sheet_job(
    *,
    job_id: str,
    input_path: str,
    pdf_path: str | None,
    svg_path: str | None,
    xml_path: str | None,
    midi_path: str | None = None,
    params: SheetParams,
    transcribe: Callable[[str, str, SheetParams, str | None], Awaitable[dict]] | None = None,
    analyze: Callable[[str], GridInfo] | None = None,
) -> dict:
    """Full audio -> sheet job. Async transcription, then the blocking
    notation/engrave stage off the event loop, then file writes.

    Returns a stats dict for mark_data_complete. Raises the pipeline's
    typed errors (from core) or SheetJobError; the runner marks the job
    failed and the paywall refunds on any raise.
    """
    transcribe = transcribe or _default_transcribe
    analyze = analyze or _default_analyze

    # If the caller wants MIDI as a downloadable format it passes a real
    # midi_path (kept). Otherwise MIDI is a transient intermediate we clean
    # up. Either way, remove any stale file at the path first so a
    # transcription that writes nothing can't be mistaken for a fresh one.
    keep_midi = midi_path is not None
    effective_midi = midi_path or f"{input_path}.transcribed.mid"
    _unlink_quietly(effective_midi)

    try:
        # 1) audio -> MIDI on the GPU (engine-routed)
        tx = await transcribe(input_path, effective_midi, params, job_id)
        if not Path(effective_midi).is_file():
            raise SheetJobError("Transcription reported success but wrote no MIDI file.")
        engine = tx.get("engine", "unknown")
        separated = bool(tx.get("separated", params.isolate))

        # 2) grid analysis (blocking CPU -> thread; best-effort inside)
        try:
            grid_info = await asyncio.to_thread(analyze, input_path)
        except Exception:  # noqa: BLE001
            grid_info = GridInfo()
        resolved = _resolve_grid(params, grid_info)

        # 3) notation + engrave (blocking CPU -> thread). midi_to_sheet raises
        #    typed errors (EmptyTranscriptionError, InvalidGridError, Engrave*)
        #    which propagate unchanged for the route to map.
        result: SheetResult = await asyncio.to_thread(midi_to_sheet, effective_midi, resolved)

        # 4) write artifacts
        written = _write_outputs(result, pdf_path=pdf_path, svg_path=svg_path, xml_path=xml_path)
        if keep_midi:
            written["midi"] = effective_midi

        log.info(
            "[sheet] job %s done: engine=%s notes=%d measures=%d staves=%d pages=%d bpm=%.1f",
            job_id, engine, result.n_notes, result.n_measures, result.n_staves,
            result.n_pages, result.tempo_bpm,
        )

        return {
            "engine": engine,
            "separated": separated,
            "instrument": result.instrument,
            "n_notes": result.n_notes,
            "n_measures": result.n_measures,
            "n_staves": result.n_staves,
            "n_pages": result.n_pages,
            "tempo_bpm": result.tempo_bpm,
            "key": result.key_name,
            "formats": sorted(written.keys()),
            "svg_pages": len(result.svg_pages),
            "_paths": written,             # route uses this to serve downloads
            "_tx_stats": tx.get("stats", {}),
        }
    finally:
        if not keep_midi:
            _unlink_quietly(effective_midi)