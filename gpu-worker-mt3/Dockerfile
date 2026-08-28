# gpu-worker-mt3/Dockerfile
#
# The RunPod image for /audio-to-midi-hq. Built and pushed separately
# from the main app - this is not part of the VPS deploy and must never
# be added to .github/workflows/deploy.yml.
#
# ---------------------------------------------------------------------------
# KEEP THIS IMAGE PRIVATE.
#
# YourMT3 is GPL-3.0. GPL triggers on DISTRIBUTION, not on running
# software as a network service, so serving MIDI over HTTP from a worker
# you operate carries no source-disclosure obligation. Pushing this image
# to a PUBLIC registry is distribution, and would.
#
# Two rules follow, and both matter:
#   1. This image goes to a private registry (or RunPod's own private
#      storage). Never Docker Hub public.
#   2. YourMT3 source is never vendored into the audioforges repo, which
#      is public. It arrives via pip below and stays upstream.
#
# mt3-infer itself is MIT and the wrapper is fine either way; it is the
# YourMT3 model code it pulls in that carries GPL.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# BASE IMAGE: nvidia/cuda runtime, NOT runpod/pytorch.
#
# The first version of this file used
# runpod/pytorch:2.4.0-py3.11-cuda12.4.1-devel-ubuntu22.04 and produced a
# 22.1GB image, of which this file's own layers were 1.5GB. The other
# ~20GB was the base: the runpod/pytorch images bundle the full CUDA
# toolkit, compilers, headers, torchvision, torchaudio and a Jupyter
# stack, none of which this worker touches. Switching devel -> runtime on
# that same family changed nothing measurable, because the weight is in
# what the image SHIPS, not which variant tag it carries.
#
# Size is not cosmetic on a serverless GPU. RunPod pulls the image on
# every cold start of a scaled-up worker, and this endpoint runs at min
# workers 0 precisely so it costs nothing idle - which means at low
# traffic nearly every request IS a cold start. A 22GB pull is billed
# startup time on each one. It is also several hours of upload from the
# VPS before RunPod ever sees the image.
#
# gpu-worker-whisper/Dockerfile already solved this the right way and is
# the model here: take a plain CUDA runtime base, add exactly the
# interpreter and libraries needed, and let pip pull the torch wheel -
# which bundles its own CUDA runtime libraries and needs no toolkit,
# because nothing in this image compiles a kernel.
#
# CUDA 12.4.1 matches both existing workers, so all three agree on a
# driver generation. cudnn is included because torch links against it;
# the whisper image documents at length what a cuDNN mismatch looks like
# (an unresolved symbol at MODEL LOAD, surfacing on RunPod as a worker
# that dies during cold start with no useful message).
# ---------------------------------------------------------------------------
FROM nvidia/cuda:12.4.1-cudnn-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1

# PYTHON 3.11, matching gpu-worker-whisper. Ubuntu 22.04 ships 3.10 in
# its own repos, hence deadsnakes.
#
# Not strictly forced the way it is on the whisper image (which needs
# 3.11 for an onnxruntime wheel with no cp310 build), but kept the same
# deliberately: three workers on three interpreter versions is three sets
# of wheel-availability surprises, and the cost of matching is one apt
# repository.
#
# ffmpeg is the decode path - librosa falls back to it for anything
# soundfile cannot read (m4a, opus, webm), which is most of what people
# upload. git and git-lfs are BUILD-time only, for the checkpoint clone
# below: mt3-infer fetches YourMT3's weights from a HuggingFace Space and
# that clone is LFS-backed. Without git-lfs it fails with a bare
# "git: 'lfs' is not a git command", which names nothing useful - found
# exactly that way.
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
        git \
        git-lfs \
    && git lfs install --system \
    && rm -rf /var/lib/apt/lists/*

# get-pip rather than the python3-pip apt package: that one installs pip
# for the SYSTEM interpreter (3.10), not for the 3.11 just added, so
# `pip install` would silently populate the wrong site-packages and the
# failure would only appear at import time inside the worker.
RUN ln -sf /usr/bin/python3.11 /usr/bin/python \
    && ln -sf /usr/bin/python3.11 /usr/bin/python3 \
    && curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11

WORKDIR /app

# Torch installed EXPLICITLY and FIRST, from the cu121 index.
#
# Two reasons it is not left to mt3-infer's dependency resolution.
#
# First, size: pip's default index serves a torch wheel carrying CUDA
# libraries for every supported architecture, where the cu121 index
# serves one built against a single CUDA version.
#
# Second, and more important: installing it first means mt3-infer's
# resolution finds torch already satisfied and does not get to pick a
# different build. A worker that silently lands on a CPU-only torch wheel
# still WORKS - just twenty times slower, on a billed GPU, with nothing
# in the logs to say why. The verification line at the end of this file
# exists to catch exactly that at build time rather than in production.
RUN pip install --no-cache-dir \
        torch==2.4.0 torchaudio==2.4.0 \
        --index-url https://download.pytorch.org/whl/cu121

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---------------------------------------------------------------------------
# CHECKPOINT BAKED AT BUILD TIME
#
# mt3-infer downloads on first use by default. Left that way, every cold
# start would clone a HuggingFace Space over LFS before it could answer
# anything - on a billed GPU, on the request path, with a network step
# that has already proven to be the flakiest part of this pipeline.
#
# Same reasoning as the Whisper model bake, and the same trap that file
# documents: the model the image CONTAINS must match the model the
# container RUNS. MT3_MODEL is read from env at runtime, so if it ever
# changes, this line changes with it or the container silently
# re-downloads at startup and blows RunPod's startup window.
# ---------------------------------------------------------------------------
ENV MT3_MODEL=yourmt3
RUN mt3-infer download ${MT3_MODEL} \
    && python -c "import mt3_infer; print('mt3-infer import ok')" \
    && python -c "import torch; print('torch', torch.__version__, 'cuda build:', torch.version.cuda)"

COPY handler.py .

CMD ["python", "-u", "handler.py"]