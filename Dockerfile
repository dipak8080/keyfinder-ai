# Official Python slim image - lightweight and fast
FROM python:3.11-slim

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

# Node.js 20.x - PO Token generation for yt-dlp (bgutil)
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Deno - yt-dlp JS challenge / n-parameter solving
RUN curl -fsSL https://deno.land/install.sh | sh
ENV PATH="/root/.deno/bin:${PATH}"

# bgutil-ytdlp-pot-provider script backend (must match requirements.txt pin)
RUN git clone --single-branch --branch 1.3.1 \
    https://github.com/Brainicism/bgutil-ytdlp-pot-provider.git \
    /root/bgutil-ytdlp-pot-provider \
    && cd /root/bgutil-ytdlp-pot-provider/server \
    && npm ci \
    && npx tsc

WORKDIR /app

# ---------- WHISPER MODEL (baked at build time) ----------
# MUST match the WHISPER_MODEL_SIZE / WHISPER_COMPUTE_TYPE the container
# is RUN with. These were previously hardcoded to 'small'/'int8' here
# while being env-configurable at runtime - a silent trap: changing
# WHISPER_MODEL_SIZE in .env left the image holding the wrong weights,
# so the container downloaded the real model at STARTUP instead. That
# download routinely exceeds the deploy health-check window, which then
# looks like a failed deploy and triggers an automatic rollback for what
# is really just a slow first boot.
#
# Passed from the deploy workflow, which reads the values straight out of
# .env so the two can't drift. See .github/workflows/deploy.yml.
ARG WHISPER_MODEL_SIZE=small
ARG WHISPER_COMPUTE_TYPE=int8

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    echo "Baking Whisper model '${WHISPER_MODEL_SIZE}' (compute_type=${WHISPER_COMPUTE_TYPE})..." && \
    python -c "from faster_whisper import WhisperModel; WhisperModel('${WHISPER_MODEL_SIZE}', device='cpu', compute_type='${WHISPER_COMPUTE_TYPE}')" && \
    python -c "import onnxruntime; print('onnxruntime', onnxruntime.__version__, '- VAD filter available')"

COPY . .

RUN chmod +x entrypoint.sh

CMD ["/app/entrypoint.sh"]