"""
main.py - App entrypoint. This file should stay tiny: create the app,
wire middleware/lifespan, mount routes. All logic lives elsewhere.
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from utils import ensure_cookies_file
from routes import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    ensure_cookies_file()
    yield
    # Shutdown - nothing needed


app = FastAPI(title="Audio Analysis API - ESSENTIA FIXED", version="12.4.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)