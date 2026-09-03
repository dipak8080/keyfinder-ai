# gpu-worker-piano/Dockerfile
#
# The RunPod image for the piano sheet-music engine (Transkun). Built and
# pushed SEPARATELY from the VPS app - this is not part of the VPS deploy and
# must never be added to .github/workflows/deploy.yml.
#
# ---------------------------------------------------------------------------
# LICENCE NOTE
#
# Transkun is the authors' research code (Yan & Duan). Unlike YourMT3 (GPL,
# which is why the MT3 image must stay private), Transkun's licence is
# permissive - but verify the current LICENSE on the upstream repo before
# pushing this image anywhere PUBLIC. When in doubt, keep it in a private
# registry / RunPod's own storage, same as the other two workers.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# BASE: nvidia/cuda runtime, NOT runpod/pytorch - same decision as
# gpu-worker-mt3, and for the same reason: the runpod/pytorch images bundle
# the full CUDA toolkit, compilers, torchvision and a Jupyter stack (~20GB)
# that this worker never touches, and RunPod pulls the whole image on every
# cold start of a scaled-up worker. A plain CUDA runtime base + the torch
# wheel (which carries its own CUDA runtime libs, no toolkit needed because
# nothing here compiles a kernel) is a fraction of the size.
#
# 12.4.1-cudnn-runtime matches the MT3 and Whisper workers, so all three
# agree on a driver generation. cudnn is included because torch links it.
# ---------------------------------------------------------------------------
FROM nvidia/cuda:12.4.1-cudnn-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1

# Python 3.11 via deadsnakes (Ubuntu 22.04 ships 3.10), matching the other
# workers. ffmpeg is the decode path: pydub shells out to it for anything
# soundfile cannot read (m4a, opus, webm), which is most real uploads.
#
# NOTE what is NOT here: no git / git-lfs. Unlike the MT3 image, Transkun's
# checkpoint ships inside the pip wheel (pretrained/2.0.pt), so there is no
# HuggingFace clone to do at build time - the flakiest step of the MT3 build
# simply does not exist here.
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

# get-pip against 3.11 specifically: the python3-pip apt package installs pip
# for the SYSTEM interpreter (3.10), so `pip install` would silently populate
# the wrong site-packages and fail only at import time in the worker.
RUN ln -sf /usr/bin/python3.11 /usr/bin/python \
    && ln -sf /usr/bin/python3.11 /usr/bin/python3 \
    && curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11

WORKDIR /app

# Torch + torchaudio installed EXPLICITLY and FIRST, from the cu121 index.
# Two reasons, same as MT3:
#   1. Size - the cu121 wheel targets one CUDA version, not every arch.
#   2. Correctness - installing first means transkun's resolver finds torch
#      already satisfied and cannot substitute a CPU-only build. A CPU torch
#      wheel still runs, just ~20x slower on a billed GPU with nothing in the
#      logs to explain it. The verification step at the end catches exactly
#      that at build time.
# torchaudio is included because transkun declares it as a dependency.
RUN pip install --no-cache-dir \
        torch==2.4.0 torchaudio==2.4.0 \
        --index-url https://download.pytorch.org/whl/cu121

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---------------------------------------------------------------------------
# BUILD-TIME VERIFICATION
#
# No checkpoint download to do (weights are in the wheel), so instead assert
# the three things that must hold for the worker to run at all:
#   - transkun imports and its bundled checkpoint file is actually present
#   - torch is the CUDA build, not the CPU wheel that silently slipped in
# A failure here fails the BUILD, not the first production job.
# ---------------------------------------------------------------------------
RUN python -c "import transkun, pkg_resources, os; \
p = pkg_resources.resource_filename('transkun', 'pretrained/2.0.pt'); \
assert os.path.isfile(p), 'transkun checkpoint missing: ' + p; \
print('transkun ok, checkpoint at', p)" \
    && python -c "import torch; assert torch.version.cuda, 'CPU-only torch wheel slipped in'; \
print('torch', torch.__version__, 'cuda build:', torch.version.cuda)"

COPY handler.py .

CMD ["python", "-u", "handler.py"]