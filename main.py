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
)
from utils import ensure_cookies_file
from routes import router
from log_stream import RequestLoggerMiddleware, router as logs_router, attach_system_log_capture
from cookie_upload import router as cookie_upload_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    attach_system_log_capture()  # starts capturing all logger.info()/error() calls app-wide
    ensure_cookies_file()
    logger.info(f"[CORS] Allowed origins: {ALLOWED_ORIGINS}")
    yield
    # Shutdown - nothing needed


app = FastAPI(title="Audio Analysis API - ESSENTIA FIXED", version="12.6.0", lifespan=lifespan)

# Logs every HTTP request (timestamp, method, path, status, duration, IP) to SQLite
app.add_middleware(RequestLoggerMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
app.include_router(logs_router)  # /admin/logs live dashboard (HTTP + system logs)
app.include_router(cookie_upload_router)  # /admin/upload-cookies - upload cookies.txt directly, no base64