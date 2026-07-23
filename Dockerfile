# Official Python slim image - lightweight and fast
FROM python:3.11-slim

# Install ffmpeg (includes ffprobe), rubberband-cli (audio time-stretch/pitch-shift),
# git (needed for pip to install yt-dlp from git), curl/gnupg (needed to add the
# NodeSource repo for Node.js below), and unzip (needed by Deno's install script,
# which downloads and unzips a release archive). --no-install-recommends keeps
# the image lean by skipping optional recommended packages.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    rubberband-cli \
    git \
    curl \
    gnupg \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js 20.x - required by the bgutil-ytdlp-pot-provider plugin to
# generate YouTube PO Tokens (Proof-of-Origin). Without a JS runtime, yt-dlp
# has no PO Token provider available and YouTube blocks web-client requests
# with "Sign in to confirm you're not a bot", regardless of cookies.
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install Deno - this is a SEPARATE requirement from Node.js above. Node runs
# the PO Token generation script; Deno is what yt-dlp uses as a "JS Challenge
# Provider" to solve YouTube's signature/"n-parameter" decryption for
# higher-quality formats. Without it, yt-dlp logs "Signature solving failed"
# and "n challenge solving failed", and silently falls back to lower-quality
# formats (e.g. itag 18) instead of the best available audio.
# Installs to /root/.deno/bin/deno since this container runs as root ($HOME=/root).
RUN curl -fsSL https://deno.land/install.sh | sh
ENV PATH="/root/.deno/bin:${PATH}"

# Build the bgutil-ytdlp-pot-provider "script" backend. Installing the pip
# package alone only registers the plugin with yt-dlp - it does NOT include
# the actual token-generation code. yt-dlp looks for it by default at
# $HOME/bgutil-ytdlp-pot-provider/server (i.e. /root/... since this
# container runs as root), so we clone and build it there. Version 1.3.1
# matches the bgutil-ytdlp-pot-provider version pinned in requirements.txt -
# keep these two in sync if you ever bump one.
RUN git clone --single-branch --branch 1.3.1 \
    https://github.com/Brainicism/bgutil-ytdlp-pot-provider.git \
    /root/bgutil-ytdlp-pot-provider \
    && cd /root/bgutil-ytdlp-pot-provider/server \
    && npm ci \
    && npx tsc

# Create app directory
WORKDIR /app

# Copy requirements and install Python packages + pre-download faster-whisper model
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    python -c "from faster_whisper import WhisperModel; WhisperModel('small', device='cpu', compute_type='int8')"

# Copy all your code
COPY . .

# Run the app with dynamic port
CMD uvicorn main:app --host 0.0.0.0 --port $PORT