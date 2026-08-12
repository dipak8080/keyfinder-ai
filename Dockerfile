# Official Python slim image - lightweight and fast
FROM python:3.11-slim

# System packages:
# - ffmpeg / rubberband-cli: audio tools
# - git: pip install yt-dlp from git + clone bgutil pot provider
# - curl / gnupg: NodeSource repo
# - unzip: Deno installer
# - build-essential / python3 / pkg-config + Cairo stack: required to
#   compile the native `canvas` dependency during bgutil's `npm ci`
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

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    python -c "from faster_whisper import WhisperModel; WhisperModel('small', device='cpu', compute_type='int8')"

COPY . .

RUN chmod +x entrypoint.sh

CMD ["/app/entrypoint.sh"]