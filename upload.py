"""
upload.py - One place for receiving an uploaded file.

WHY THIS EXISTS:

Every route used to do the same three things inline:

    content = await file.read()          # whole body into RAM
    if len(content) > MAX_UPLOAD_BYTES:  # ...checked AFTER buffering
        raise HTTPException(400, ...)
    with open(path, "wb") as f:          # blocking write on the event loop
        f.write(content)

That has three separate problems, and it was duplicated ~25 times:

1. PEAK MEMORY IS UNBOUNDED BY THE LIMIT. The size check runs after the
   entire body is already resident. A 500MB body is fully buffered
   before being rejected. On a box with NO SWAP (this app runs on an
   Incus container VPS where swapon is not permitted) that is the
   difference between a clean 413 and an OOM kill that takes every
   in-flight request down with it.

2. THE WRITE BLOCKS THE EVENT LOOP. f.write() of a 50MB buffer is a
   synchronous syscall inside an async handler. With a single uvicorn
   worker, every other connection - including cheap status polls -
   stalls for its duration.

3. THE ERROR CONTRACT DRIFTED. Twenty-five hand-written copies produced
   at least three different message shapes for the same condition, and
   the status code was 400 (a generic "you sent something wrong")
   rather than 413 (the specific "your payload is too large"), which is
   what a frontend needs to distinguish "too big" from "wrong format".

/video-to-audio already solved this correctly by streaming in chunks.
This module is that approach extracted so every route shares it.

USAGE:

    from upload import save_upload, save_uploads

    input_path = build_temp_input_path(job_id, file.filename)
    size = await save_upload(file, input_path, MAX_UPLOAD_BYTES, label="convert")

save_upload() either returns the byte count written, or raises an
HTTPException and leaves NO partial file behind. Callers do not need
their own try/except around it for cleanup purposes.
"""
import os
import asyncio
from typing import List, Sequence, Tuple

from fastapi import HTTPException, UploadFile

from config import logger

# 1MB. Large enough that syscall overhead is negligible against the
# transfer, small enough that peak memory per in-flight upload is
# bounded by (workers x concurrent uploads x this), not by file size.
UPLOAD_CHUNK_BYTES = 1024 * 1024


def _discard(path: str) -> None:
    """Removes a partially-written upload. Never raises - this runs on
    the failure path, and a cleanup error must not mask the real error
    that got us here."""
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except Exception as e:
        logger.warning(f"[UPLOAD] Could not remove partial file {path}: {e}")


def _too_large(max_bytes: int) -> HTTPException:
    """413, not 400. The frontend needs to tell 'too big' apart from
    'unsupported format' without string-matching the message."""
    return HTTPException(
        413,
        f"File too large. Maximum allowed size is {max_bytes // (1024 * 1024)} MB.",
    )


async def save_upload(
    file: UploadFile,
    dest_path: str,
    max_bytes: int,
    label: str = "upload",
) -> int:
    """
    Streams one uploaded file to dest_path, enforcing max_bytes AS IT
    GOES rather than after the fact. Returns bytes written.

    Raises HTTPException(413) if the body exceeds max_bytes, (400) if
    the body is empty, (500) on any write failure. In every failure
    case the partial file is deleted before the exception propagates.

    The actual write is dispatched to a thread so a slow disk cannot
    stall the event loop - the read side stays async, so the socket is
    still drained cooperatively.
    """
    total = 0
    loop = asyncio.get_running_loop()

    try:
        handle = await loop.run_in_executor(None, open, dest_path, "wb")
        try:
            while True:
                chunk = await file.read(UPLOAD_CHUNK_BYTES)
                if not chunk:
                    break

                total += len(chunk)
                if total > max_bytes:
                    logger.warning(
                        f"[UPLOAD] Rejected '{file.filename}' ({label}): "
                        f"exceeded {max_bytes // (1024 * 1024)} MB mid-stream"
                    )
                    raise _too_large(max_bytes)

                await loop.run_in_executor(None, handle.write, chunk)
        finally:
            await loop.run_in_executor(None, handle.close)

    except HTTPException:
        _discard(dest_path)
        raise
    except Exception as e:
        _discard(dest_path)
        logger.error(
            f"[UPLOAD] Write failed for '{file.filename}' ({label}): {e}",
            exc_info=True,
        )
        raise HTTPException(500, "Failed to receive the uploaded file.")

    if total == 0:
        _discard(dest_path)
        raise HTTPException(400, "Empty file.")

    logger.info(
        f"[UPLOAD] Received '{file.filename}' ({label}): "
        f"{total / (1024 * 1024):.1f} MB -> {dest_path}"
    )
    return total


async def save_uploads(
    files: Sequence[UploadFile],
    dest_paths: Sequence[str],
    max_total_bytes: int,
    label: str = "upload",
) -> Tuple[List[str], int]:
    """
    Multi-file variant for /join. The cap is enforced across the WHOLE
    batch, not per file - ten 45MB files would each pass a per-file
    check and still land 450MB on a 30GB disk.

    On any failure EVERY already-written file in the batch is removed,
    not just the one that failed. Returns (written_paths, total_bytes).
    """
    if len(files) != len(dest_paths):
        raise HTTPException(500, "Internal error: upload path count mismatch.")

    written: List[str] = []
    total = 0
    loop = asyncio.get_running_loop()

    try:
        for file, dest_path in zip(files, dest_paths):
            written.append(dest_path)
            per_file = 0

            handle = await loop.run_in_executor(None, open, dest_path, "wb")
            try:
                while True:
                    chunk = await file.read(UPLOAD_CHUNK_BYTES)
                    if not chunk:
                        break

                    total += len(chunk)
                    per_file += len(chunk)

                    if total > max_total_bytes:
                        logger.warning(
                            f"[UPLOAD] Rejected batch ({label}): combined size "
                            f"exceeded {max_total_bytes // (1024 * 1024)} MB"
                        )
                        raise HTTPException(
                            413,
                            f"Combined file size too large. Maximum total is "
                            f"{max_total_bytes // (1024 * 1024)} MB.",
                        )

                    await loop.run_in_executor(None, handle.write, chunk)
            finally:
                await loop.run_in_executor(None, handle.close)

            if per_file == 0:
                raise HTTPException(400, f"'{file.filename}' is empty.")

    except HTTPException:
        for path in written:
            _discard(path)
        raise
    except Exception as e:
        for path in written:
            _discard(path)
        logger.error(f"[UPLOAD] Batch write failed ({label}): {e}", exc_info=True)
        raise HTTPException(500, "Failed to receive the uploaded files.")

    logger.info(
        f"[UPLOAD] Received {len(files)} files ({label}): "
        f"{total / (1024 * 1024):.1f} MB combined"
    )
    return written, total