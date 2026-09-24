# Official Python slim image - lightweight and fast
FROM python:3.11-slim

# Changes weekly, so the first deploy of each week rebuilds every layer fresh
ARG CACHE_WEEK=none

# System packages:
# - ffmpeg / rubberband-cli: audio tools
# - curl: general diagnostics
# - build-essential / python3 / pkg-config: compile any pip package without a wheel
# - Cairo stack: libcairo2 runtime for cairosvg in the audio-to-sheet engrave stage
# - fonts-dejavu-core: a text font so Verovio renders score titles/tempo
#   marks cleanly (the music glyphs ship inside the verovio wheel; this
#   is only for the surrounding text). ~1MB.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    rubberband-cli \
    curl \
    build-essential \
    python3 \
    pkg-config \
    libcairo2-dev \
    libpango1.0-dev \
    libjpeg-dev \
    libgif-dev \
    librsvg2-dev \
    fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    python -c "import onnxruntime; print('onnxruntime', onnxruntime.__version__, '- VAD filter ready')"

COPY . .

RUN chmod +x entrypoint.sh

CMD ["/app/entrypoint.sh"]