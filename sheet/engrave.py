"""MusicXML -> engraved PDF + SVG via Verovio. I/O-free, deterministic.

Renders each page to SVG, converts to PDF (cairosvg) and merges (pypdf).
Returns bytes/strings; the caller writes files. Raises typed errors the
route maps to HTTP status + credit outcome.
"""
from __future__ import annotations

import io
from dataclasses import dataclass

import os

import cairosvg
import verovio
from pypdf import PdfWriter, PdfReader

# Verovio bundles its fonts/resources in <pkg>/data. The path does NOT
# auto-resolve when a toolkit is first created inside a worker thread
# (which is how the job runner calls this via asyncio.to_thread), so it
# must be set explicitly on every toolkit. Resolved once at import.
_RESOURCE_PATH = os.path.join(os.path.dirname(verovio.__file__), "data")
_HAVE_RESOURCES = os.path.isdir(_RESOURCE_PATH)

__all__ = [
    "engrave",
    "EngraveResult",
    "EngraveError",
    "EngraveInputError",
    "EngraveRenderError",
]


class EngraveError(Exception):
    """Base for every failure this module raises."""


class EngraveInputError(EngraveError):
    """Verovio could not load the MusicXML (malformed or empty)."""


class EngraveRenderError(EngraveError):
    """A page failed to render to SVG or convert to PDF."""


# --- A4 defaults, in Verovio units (1/100 mm): 210 x 297 mm ------------------
_A4_W = 2100
_A4_H = 2970
_MIN_DIM, _MAX_DIM = 500, 12000
_MIN_SCALE, _MAX_SCALE = 20, 100
_MAX_PAGES = 200


@dataclass(frozen=True)
class EngraveResult:
    pdf: bytes | None
    svg_pages: tuple[str, ...]
    n_pages: int


def _make_toolkit(page_w: int, page_h: int, scale: int) -> "verovio.toolkit":
    # toolkit(False) defers resource loading so we can point it at the
    # bundled data dir before it tries (and fails) to auto-locate fonts in
    # a worker thread. Fall back to the default constructor if the bundled
    # path is not where we expect (non-wheel install layouts).
    if _HAVE_RESOURCES:
        tk = verovio.toolkit(False)
        tk.setResourcePath(_RESOURCE_PATH)
    else:
        tk = verovio.toolkit()
    tk.setOptions(
        {
            "pageWidth": page_w,
            "pageHeight": page_h,
            "scale": scale,
            "adjustPageHeight": False,   # keep true A4 pages for print/PDF
            "breaks": "auto",
            "header": "none",
            "footer": "none",
            "svgViewBox": True,          # scalable, self-contained SVG
            "svgBoundingBoxes": False,
            "xmlIdSeed": 1,              # reproducible element ids -> deterministic SVG
        }
    )
    return tk


def _svg_to_pdf_bytes(svg: str) -> bytes:
    try:
        out = cairosvg.svg2pdf(bytestring=svg.encode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise EngraveRenderError(f"SVG->PDF conversion failed: {exc}") from exc
    if not out:
        raise EngraveRenderError("SVG->PDF conversion produced no data.")
    return out


def _merge_pdfs(pages: list[bytes]) -> bytes:
    writer = PdfWriter()
    try:
        for pg in pages:
            reader = PdfReader(io.BytesIO(pg))
            for page in reader.pages:
                writer.add_page(page)
        buf = io.BytesIO()
        writer.write(buf)
        return buf.getvalue()
    except Exception as exc:  # noqa: BLE001
        raise EngraveRenderError(f"PDF merge failed: {exc}") from exc
    finally:
        writer.close()


def engrave(
    musicxml: str,
    *,
    want_pdf: bool = True,
    want_svg: bool = True,
    page_width: int = _A4_W,
    page_height: int = _A4_H,
    scale: int = 40,
) -> EngraveResult:
    """Render MusicXML to a paged PDF and/or per-page SVG.

    Raises EngraveInputError (bad MusicXML) or EngraveRenderError.
    """
    if not musicxml or not musicxml.strip():
        raise EngraveInputError("MusicXML is empty.")
    if not (want_pdf or want_svg):
        raise EngraveError("Nothing requested: enable want_pdf or want_svg.")
    if not (_MIN_DIM <= page_width <= _MAX_DIM and _MIN_DIM <= page_height <= _MAX_DIM):
        raise EngraveError("Page dimensions out of range.")
    if not (_MIN_SCALE <= scale <= _MAX_SCALE):
        raise EngraveError("scale out of range.")

    tk = _make_toolkit(page_width, page_height, scale)

    try:
        loaded = tk.loadData(musicxml)
    except Exception as exc:  # noqa: BLE001
        raise EngraveInputError(f"Verovio could not parse the MusicXML: {exc}") from exc
    if not loaded:
        raise EngraveInputError("Verovio rejected the MusicXML (malformed or unsupported).")

    n_pages = tk.getPageCount()
    if n_pages < 1:
        raise EngraveRenderError("Verovio produced no pages to render.")
    if n_pages > _MAX_PAGES:
        raise EngraveRenderError(f"Score has {n_pages} pages (max {_MAX_PAGES}).")

    svg_pages: list[str] = []
    for pg in range(1, n_pages + 1):
        try:
            svg = tk.renderToSVG(pg)
        except Exception as exc:  # noqa: BLE001
            raise EngraveRenderError(f"Rendering page {pg} to SVG failed: {exc}") from exc
        if not svg or not svg.strip().startswith("<"):
            raise EngraveRenderError(f"Page {pg} rendered to invalid SVG.")
        svg_pages.append(svg)

    pdf_bytes: bytes | None = None
    if want_pdf:
        pdf_bytes = _merge_pdfs([_svg_to_pdf_bytes(s) for s in svg_pages])

    return EngraveResult(
        pdf=pdf_bytes,
        svg_pages=tuple(svg_pages) if want_svg else (),
        n_pages=n_pages,
    )