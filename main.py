"""
main.py - App entrypoint. This file should stay tiny: create the app,
wire middleware/lifespan, mount routes. All logic lives elsewhere.
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import logger, ALLOWED_ORIGINS
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
    yield
    # Shutdown - nothing needed


app = FastAPI(title="Audio Analysis API - ESSENTIA FIXED", version="12.5.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)