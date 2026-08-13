FROM python:3.11-slim

# ffmpeg is REQUIRED, not optional: basic-pitch decodes audio via
# librosa.load(), which uses soundfile for common formats and falls back
# to audioread (ffmpeg) for everything else. Without ffmpeg here, .opus
# and .webm inputs fail to decode at all - verified by reading
# basic_pitch/inference.py:229.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Warms the model at BUILD time so the first request after a container
# restart doesn't pay a cold TensorFlow saved_model load. Mirrors the
# faster-whisper warm-up the main app's Dockerfile already does. Also
# acts as a build-time smoke test: if the model can't load, the image
# fails to build rather than shipping a container that 500s on every
# request.
RUN python -c "from basic_pitch.inference import Model; from basic_pitch import ICASSP_2022_MODEL_PATH; Model(ICASSP_2022_MODEL_PATH); print('model OK')"

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]