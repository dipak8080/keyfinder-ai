"""
audio_to_midi.py - Thin HTTP client to the isolated midi-worker sidecar.

Deliberately contains NO transcription logic and imports NOTHING from
basic-pitch or tensorflow - see midi-worker/main.py's module docstring
for the full reasoning (short version: basic-pitch's tensorflow
dependency hard-pins numpy<2.0.0, which is incompatible with this app's
numpy==2.3.5 that essentia/librosa/demucs/torch all depend on).

This module's entire job: send a file, handle every way a network call
can fail, return MIDI bytes on disk or raise AudioToolError with a
message written for the person who uploaded the file. No raw requests
exception ever escapes to the caller - _run_tool_job in routes.py maps
AudioToolError to a clean job failure, and anything else to an opaque
500, so getting this mapping right here is what determines whether the
user sees "no notes were detected" or "something went wrong".

Synchronous by design (uses `requests`, not an async client) - same
contract as every subprocess-based tool in this codebase: blocking, and
therefore dispatched via utils.run_blocking() from the async route,
never awaited directly.
"""
import os

import requests

from config import (
    logger,
    MIDI_WORKER_URL,
    MIDI_WORKER_SHARED_SECRET,
    MIDI_WORKER_TIMEOUT_SECONDS,
)
from audio_common import AudioToolError

from typing import Optional


def convert_to_midi(
    input_path: str,
    output_path: str,
    onset_threshold: float = 0.5,
    frame_threshold: float = 0.3,
    minimum_note_length: float = 127.70,
    minimum_frequency: Optional[float] = None,
    maximum_frequency: Optional[float] = None,
) -> int:
    """
    Sends input_path to the midi-worker sidecar and writes the returned
    MIDI bytes to output_path. Returns the output size in bytes (used
    for the COMPLETE log line's success detail).

    Raises AudioToolError - never a raw requests exception - for every
    failure mode: worker unreachable, timeout, auth mismatch, no notes
    detected, corrupt input, or an unexpected status.
    """
    if not MIDI_WORKER_SHARED_SECRET:
        # Deploy misconfiguration, not a user error. Logged at ERROR so
        # it surfaces in the admin log dashboard immediately rather than
        # presenting as an inexplicable run of failed jobs.
        logger.error(
            "[AUDIO_TO_MIDI] MIDI_WORKER_SHARED_SECRET is not set - refusing to call the worker. "
            "Set it in the main app's .env AND on the midi-worker container (values must match)."
        )
        raise AudioToolError("MIDI conversion is temporarily unavailable. Please try again later.")

    filename = os.path.basename(input_path)

    try:
        with open(input_path, "rb") as f:
            # Explicit (filename, fileobj) tuple rather than a bare file
            # handle: the worker sniffs the extension off the uploaded
            # filename to hand librosa a correctly-suffixed temp file,
            # and relying on requests' implicit basename derivation
            # would make that depend on an implementation detail.
            data = {
                "onset_threshold": onset_threshold,
                "frame_threshold": frame_threshold,
                "minimum_note_length": minimum_note_length,
            }
            # Omitted entirely when None - basic-pitch treats absent as
            # "no bound", which is different from any numeric value.
            if minimum_frequency is not None:
                data["minimum_frequency"] = minimum_frequency
            if maximum_frequency is not None:
                data["maximum_frequency"] = maximum_frequency

            response = requests.post(
                f"{MIDI_WORKER_URL}/convert",
                files={"file": (filename, f)},
                data=data,
                headers={"x-internal-secret": MIDI_WORKER_SHARED_SECRET},
                timeout=MIDI_WORKER_TIMEOUT_SECONDS,
            )
    except requests.exceptions.ConnectionError as e:
        logger.error(f"[AUDIO_TO_MIDI] Cannot reach midi-worker at {MIDI_WORKER_URL} - is the container up? {e}")
        raise AudioToolError("MIDI conversion service is temporarily unavailable. Please try again shortly.")
    except requests.exceptions.Timeout:
        logger.warning(f"[AUDIO_TO_MIDI] midi-worker timed out after {MIDI_WORKER_TIMEOUT_SECONDS}s")
        raise AudioToolError("MIDI conversion took too long. Try a shorter clip.")
    except requests.exceptions.RequestException as e:
        logger.error(f"[AUDIO_TO_MIDI] Unexpected transport error calling midi-worker: {e}", exc_info=True)
        raise AudioToolError("MIDI conversion failed unexpectedly.")

    if response.status_code == 200:
        content = response.content
        if not content:
            logger.error("[AUDIO_TO_MIDI] midi-worker returned 200 with an empty body")
            raise AudioToolError("MIDI conversion failed unexpectedly.")
        with open(output_path, "wb") as f:
            f.write(content)
        return len(content)

    # Structured reason extraction. FastAPI serializes HTTPException's
    # dict detail as {"detail": {...}}, so a malformed/HTML error page
    # (e.g. from a proxy in between) must not crash this - hence the
    # defensive parse rather than a bare response.json()[...] chain.
    reason = None
    try:
        detail = response.json().get("detail")
        if isinstance(detail, dict):
            reason = detail.get("reason")
    except (ValueError, AttributeError):
        pass

    if reason == "no_notes":
        # Expected outcome, not a bug - the user uploaded something with
        # no detectable pitched content.
        raise AudioToolError(
            "No musical notes were detected in this audio. This tool works best on a "
            "single instrument or clear melody - try a different section or a cleaner recording."
        )

    if response.status_code == 413 or reason == "too_large":
        raise AudioToolError("File too large for MIDI conversion.")

    if response.status_code == 401 or reason in ("unauthorized", "misconfigured"):
        logger.error(
            f"[AUDIO_TO_MIDI] midi-worker auth failure (status={response.status_code}) - "
            f"MIDI_WORKER_SHARED_SECRET does not match between the two containers."
        )
        raise AudioToolError("MIDI conversion is temporarily unavailable. Please try again later.")

    if response.status_code == 422:
        raise AudioToolError("Could not transcribe this audio. It may be corrupt or in an unsupported format.")

    logger.error(
        f"[AUDIO_TO_MIDI] midi-worker returned unexpected status {response.status_code}: "
        f"{response.text[:200]}"
    )
    raise AudioToolError("MIDI conversion failed unexpectedly.")