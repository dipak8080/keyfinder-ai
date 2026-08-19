# CUDA 12 + cuDNN 9 runtime. ctranslate2 4.x links against cuDNN 9
# specifically - a cuDNN 8 base image builds fine and then fails at
# MODEL LOAD time with an unresolved-symbol error, which on RunPod
# surfaces as a worker that dies during cold start with no useful
# message. Getting this line right is most of the difficulty of this
# image.
FROM nvidia/cuda:12.4.1-cudnn-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1

# PYTHON 3.11, NOT THE 3.10 THIS BASE IMAGE SHIPS.
#
# Not a preference - a hard requirement, and a deliberate one.
# onnxruntime 1.29.0 publishes no cp310 wheel (3.10 tops out at 1.23.2),
# and the VPS runs 3.11 with 1.29.0. Pinning both sides to identical
# versions is the entire reason the two backends can be trusted to
# produce identical transcripts; quietly dropping to a different
# onnxruntime here to accommodate an older interpreter would give that
# guarantee away to save five lines of Dockerfile.
#
# Ubuntu 22.04 ships no 3.11 in its own repos, hence deadsnakes.
RUN apt-get update && apt-get install -y --no-install-recommends \
    software-properties-common \
    curl \
    ca-certificates \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get update && apt-get install -y --no-install-recommends \
    python3.11 \
    python3.11-dev \
    python3.11-distutils \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# get-pip rather than the python3-pip apt package: that one installs pip
# for the SYSTEM interpreter (3.10), not for the 3.11 just added, so
# `pip install` would silently populate the wrong site-packages and the
# failure would only appear at import time inside the worker.
RUN ln -sf /usr/bin/python3.11 /usr/bin/python \
    && ln -sf /usr/bin/python3.11 /usr/bin/python3 \
    && curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---------- MODEL BAKED INTO THE IMAGE ----------
# MUST match WHISPER_MODEL_SIZE on the endpoint's environment, for the
# same reason the VPS Dockerfile takes a build arg: a mismatch means the
# worker downloads different weights at COLD START instead of loading
# them from local disk.
#
# That matters far more here than on the VPS. A cold start already costs
# 15-30s; adding a multi-GB HuggingFace download on top routinely pushes
# it past RunPod's startup window, and the worker is killed before it
# ever handles a request. At low traffic nearly every request is a cold
# start, so this single line is the difference between "GPU is fast" and
# "GPU times out".
ARG WHISPER_MODEL_SIZE=large-v3
ARG WHISPER_COMPUTE_TYPE=float16
ENV WHISPER_MODEL_SIZE=${WHISPER_MODEL_SIZE} \
    WHISPER_COMPUTE_TYPE=${WHISPER_COMPUTE_TYPE} \
    WHISPER_DEVICE=cuda

# Downloaded on CPU at build time - the build machine has no GPU, and the
# weights on disk are identical either way. Only the RUNTIME load needs
# the GPU, so compute_type here is irrelevant and int8 is simply the
# cheapest thing that forces the download.
RUN python -c "from faster_whisper import WhisperModel; WhisperModel('${WHISPER_MODEL_SIZE}', device='cpu', compute_type='int8')" \
    && python -c "import onnxruntime; print('onnxruntime', onnxruntime.__version__)"

COPY handler.py .

CMD ["python", "-u", "handler.py"]