"""
routes/__init__.py - assembles the routes/ package into a single
FastAPI router, importable exactly the way the old monolithic routes.py
was: `from routes import router`. main.py does not change at all.

--------------------------------------------------------------------------
WHY THIS PACKAGE EXISTS (2026-08-14 restructure)

The old routes.py had grown to ~3000 lines covering every tool in the
app - YouTube download/chaining, Demucs separation, thirteen ffmpeg/
rubberband tools, MIDI transcription, Whisper transcription, video/join/
silence-split, and all admin/meta endpoints. Finding anything meant
scrolling past everything else, and touching one tool risked an
unrelated merge conflict with whoever was touching another.

This is a PURE MOVE. Every docstring, comment, and line of route logic
is unchanged from where it lived in the old routes.py - only the file
each piece lives in changed. If any behaviour differs anywhere, that is
a bug introduced by the move, not an intended change.

Target structure:

    routes/
      __init__.py        # this file - imports every sub-router, exposes one `router`
      _shared.py          # helpers used by 2+ route modules
      youtube.py           # /download, /youtube/analyze|separate|stems (+hq)
      separation.py        # /separate, /stems (+hq)
      audio_tools.py       # the 13 ffmpeg/rubberband tools + fade/channels/resample/ringtone
      midi.py               # /audio-to-midi
      transcribe.py         # /speech-to-text
      media.py               # /analyze, /video-to-audio, /join, /silence-split, /loudnorm, /trim
      admin.py                # /admin/*, /limits, /health, /

The six concurrency semaphores that used to be module-level globals in
routes.py now live in utils.py, alongside the two that were already
there - see utils.py's own "WHAT CHANGED (2026-08-14)" note. Every
sub-module below imports whichever semaphores it needs from utils.py
directly; none are redeclared here.

--------------------------------------------------------------------------
ONE DELIBERATE WRINKLE: /admin/endpoints and the full router

admin.py's GET /admin/endpoints walks every registered route to build
the tool picker the dashboard uses (see admin.py's own docstring on
admin_endpoints() for the full "why" - it was already there in the old
routes.py, unchanged in what it does). That function needs to see EVERY
route in the app, not just admin.py's own four admin routes - so it
cannot iterate admin.py's local `router` object (which, before
include_router() below runs, only knows about admin.py's own routes).

admin.py therefore imports the fully-assembled package router lazily,
INSIDE the function body: `from routes import router as full_router`.
That's not a style choice - a module-level `from routes import router`
at the top of admin.py would try to import routes/__init__.py while
routes/__init__.py is still in the middle of importing admin.py (see the
import block below), which is a circular import and would fail at
startup. Deferring the import to request time means it only runs after
routes/__init__.py has already finished executing top to bottom, at
which point `router` exists, fully assembled, with every sub-router's
routes already merged in via include_router().

--------------------------------------------------------------------------
"""
from fastapi import APIRouter

from .youtube import router as _youtube_router
from .separation import router as _separation_router
from .audio_tools import router as _audio_tools_router
from .midi import router as _midi_router
from .transcribe import router as _transcribe_router
from .media import router as _media_router
from .admin import router as _admin_router

router = APIRouter()

# Order doesn't affect routing (FastAPI matches by path/method, not
# registration order across independent routers), but it's kept in the
# same order as the target structure list above so this file reads as a
# table of contents for the whole API.
router.include_router(_youtube_router)
router.include_router(_separation_router)
router.include_router(_audio_tools_router)
router.include_router(_midi_router)
router.include_router(_transcribe_router)
router.include_router(_media_router)
router.include_router(_admin_router)