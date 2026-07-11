"""
main.py - App entrypoint. This file should stay tiny: create the app,
wire middleware/lifespan, mount routes. All logic lives elsewhere.
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import (
    logger,
    ALLOWED_ORIGINS,
    ALLOW_LOVABLE_PREVIEW_ORIGINS,
    LOVABLE_PREVIEW_ORIGIN_REGEX,
)
from utils import ensure_cookies_file
from routes import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    ensure_cookies_file()
    if ALLOWED_ORIGINS == ["*"]:
        logger.warning(
            "[CORS] ALLOWED_ORIGINS is not set - allowing ALL origins ('*'). "
            "Set ALLOWED_ORIGINS in Railway to your real domain(s) once known, "
            "e.g. 'https://audioforges.lovable.app'."
        )
    else:
        logger.info(f"[CORS] Allowed origins: {ALLOWED_ORIGINS}")

    if ALLOW_LOVABLE_PREVIEW_ORIGINS:
        logger.info(
            f"[CORS] Also allowing Lovable preview/editor origins via regex: "
            f"{LOVABLE_PREVIEW_ORIGIN_REGEX}"
        )
    yield
    # Shutdown - nothing needed


app = FastAPI(title="Audio Analysis API - ESSENTIA FIXED", version="12.6.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    # allow_origin_regex is checked IN ADDITION to allow_origins - an origin
    # only needs to match one of the two to be allowed. This is what lets
    # Lovable's random preview subdomains through without loosening
    # ALLOWED_ORIGINS itself.
    allow_origin_regex=LOVABLE_PREVIEW_ORIGIN_REGEX if ALLOW_LOVABLE_PREVIEW_ORIGINS else None,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)