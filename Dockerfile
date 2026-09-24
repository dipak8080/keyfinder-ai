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

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    python -c "import onnxruntime; print('onnxruntime', onnxruntime.__version__, '- VAD filter ready')"

COPY . .

RUN chmod +x entrypoint.sh

CMD ["/app/entrypoint.sh"]