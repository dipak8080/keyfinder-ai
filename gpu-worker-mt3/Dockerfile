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

FROM runpod/pytorch:2.1.0-py3.10-cuda11.8.0-devel-ubuntu22.04

# ffmpeg for the decode path (librosa falls back to it for anything
# soundfile can't read - m4a, opus, webm). git and git-lfs are needed at
# BUILD time only, for the checkpoint clone below: mt3-infer fetches
# YourMT3's weights from a HuggingFace Space, and that clone is
# LFS-backed. Without git-lfs the download fails with a bare
# "git: 'lfs' is not a git command", which names nothing useful.
RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends \
        ffmpeg git git-lfs \
    && git lfs install --system \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /

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
# Same reasoning as the Whisper model bake in the main app's Dockerfile,
# and the same trap it documents: the model the image CONTAINS must
# match the model the container RUNS. MT3_MODEL is read from env at
# runtime, so if that ever changes, this line has to change with it or
# the container silently re-downloads at startup and blows the deploy's
# health-check window.
# ---------------------------------------------------------------------------
ENV MT3_MODEL=yourmt3
RUN mt3-infer download ${MT3_MODEL} \
    && python -c "import mt3_infer; print('mt3-infer import ok')"

COPY handler.py .

CMD ["python", "-u", "handler.py"]