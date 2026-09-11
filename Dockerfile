# Official Python slim image - lightweight and fast
FROM python:3.11-slim

# Changes weekly, so the first deploy of each week rebuilds every layer fresh
ARG CACHE_WEEK=none

# System packages:
# - ffmpeg / rubberband-cli: audio tools
# - git: pip install yt-dlp from git + clone bgutil pot provider
# - curl / gnupg: NodeSource repo
# - unzip: Deno installer
# - build-essential / python3 / pkg-config + Cairo stack: required to
#   compile the native `canvas` dependency during bgutil's `npm ci`
#   (libcairo2-dev also satisfies cairosvg's libcairo2 runtime need for
#   the audio-to-sheet engrave stage - no extra system package required)
# - fonts-dejavu-core: a text font so Verovio renders score titles/tempo
#   marks cleanly (the music glyphs ship inside the verovio wheel; this
#   is only for the surrounding text). ~1MB.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    rubberband-cli \
    git \
    curl \
    gnupg \
    unzip \
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

# Node.js 22.x - PO Token generation for yt-dlp (bgutil 1.3.2+ requires >=22)
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Deno - yt-dlp JS challenge / n-parameter solving
RUN curl -fsSL https://deno.land/install.sh | sh
ENV PATH="/root/.deno/bin:${PATH}"

# bgutil-ytdlp-pot-provider script backend (must match requirements.txt pin)
RUN git clone --single-branch --branch 2.0.0 \
    https://github.com/Brainicism/bgutil-ytdlp-pot-provider.git \
    /root/bgutil-ytdlp-pot-provider \
    && cd /root/bgutil-ytdlp-pot-provider/server \
    && npm ci \
    && npx tsc

WORKDIR /app

# ---------- WHISPER MODEL ----------
# Baked only when transcription runs locally. deploy.yml passes
# BAKE_WHISPER_MODEL=0 when .env has TRANSCRIPTION_BACKEND=gpu, where the
# local model is never loaded and baking it only cost ~480MB and build
# time. Flip .env to local and redeploy to get it back.
#
# When baked, it MUST match the WHISPER_MODEL_SIZE / WHISPER_COMPUTE_TYPE
# the container runs with: a missing model downloads at STARTUP, which
# blows past the deploy health-check window. deploy.yml reads both from
# .env so they can't drift.
ARG WHISPER_MODEL_SIZE=small
ARG WHISPER_COMPUTE_TYPE=int8
ARG BAKE_WHISPER_MODEL=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    if [ "$BAKE_WHISPER_MODEL" = "1" ]; then \
      echo "Baking Whisper model '${WHISPER_MODEL_SIZE}' (compute_type=${WHISPER_COMPUTE_TYPE})..." && \
      python -c "from faster_whisper import WhisperModel; WhisperModel('${WHISPER_MODEL_SIZE}', device='cpu', compute_type='${WHISPER_COMPUTE_TYPE}')"; \
    else \
      echo "Skipping Whisper model bake (GPU transcription backend)."; \
    fi && \
    python -c "import onnxruntime; print('onnxruntime', onnxruntime.__version__, '- VAD filter available')"

COPY . .

RUN chmod +x entrypoint.sh

CMD ["/app/entrypoint.sh"]