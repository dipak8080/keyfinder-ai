# Dockerfile for the RunPod Serverless Demucs worker.
#
# COMPLETELY SEPARATE from the VPS backend's own Dockerfile - this image
# has no relationship to it beyond both eventually invoking Demucs.
# Build and push THIS file to its own RunPod Serverless endpoint; nothing
# here ever runs on the VPS, and nothing on the VPS is required to build
# it (see handler.py's own docstring for why config values are mirrored,
# not imported).

# RunPod's own PyTorch base image - already built with CUDA + cuDNN and a
# torch wheel correctly wired to the GPU driver stack their workers run
# on. Hand-rolling this from a bare nvidia/cuda image is a common source
# of "torch reports CUDA available but it silently isn't on this specific
# host" bugs - not worth the risk to save a smaller base image. Pinned to
# an exact tag (not :latest) so a RunPod base-image update can never
# silently change this worker's behaviour out from under it.
FROM runpod/pytorch:2.4.0-py3.11-cuda12.4.1-devel-ubuntu22.04

# ffmpeg/ffprobe - required by both Demucs itself (audio I/O) and this
# handler's own duration check (_get_duration_seconds). Nothing
# YouTube-related belongs in this image: this worker never touches
# yt-dlp, cookies, or any download logic - it only ever receives audio
# bytes the VPS already extracted, over the network.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /worker

# demucs pulls its own torch/torchaudio version constraints, which the
# base image's CUDA-enabled torch already satisfies - this does NOT
# reinstall torch. That is the opposite choice from the VPS backend's own
# requirements.txt, which deliberately installs the CPU-ONLY torch build -
# same library, opposite variant, for the opposite reason (that box has
# no GPU at all; this one exists specifically because it does).
RUN pip install --no-cache-dir demucs==4.0.1 runpod requests

# audio-separator brings the MelBand RoFormer vocal model (the HQ vocal
# path - see handler.py's _run_roformer_gpu). [gpu] pulls onnxruntime-gpu;
# torch is NOT reinstalled (the base image's CUDA build satisfies its
# constraint). Pinned exactly, same reasoning as demucs above.
# NOTE: this bumps numpy to >=2. The Demucs warmup below runs AFTER this
# install on purpose - if the numpy major bump ever breaks demucs, the
# IMAGE BUILD fails loudly at that step instead of jobs failing at runtime.
RUN pip install --no-cache-dir "audio-separator[gpu]==0.47.0"

# Deterministic, image-baked location for RoFormer weights (the package
# honours this env var at runtime too - see its Separator.__init__).
# Created explicitly: audio-separator REFUSES a model_file_dir that does
# not already exist (FileNotFoundError at Separator init) rather than
# creating it.
ENV AUDIO_SEPARATOR_MODEL_DIR=/worker/models
RUN mkdir -p /worker/models

# Bakes BOTH models this app actually uses (standard + HQ tier) into the
# IMAGE, rather than letting Demucs download them lazily on a job's first
# real request. This matters more here than it would on a normal server:
# Active Workers = 0 is the whole point of the cost plan (see
# gpu_budget.py / the RunPod endpoint config), which means "a fresh,
# empty worker" is the NORMAL state between jobs, not a rare cold path -
# every request during a quiet period could otherwise pay for a ~80MB+
# per-model weight download on top of the real cold start.
#
# Runs on CPU (-d cpu) deliberately: this step executes during the image
# BUILD, where no GPU is attached, and its only purpose is to make Demucs
# fetch and cache its own weight files - the actual separation result is
# thrown away, only the weights end up baked into the image layer.
#
# A SINE TONE, NOT SILENCE: the first attempt used a silent (all-zero)
# clip and hit a reproducible AssertionError inside Demucs' padding
# logic, regardless of clip length. Root cause: Demucs normalizes input
# audio by dividing by its own standard deviation, and pure silence has a
# standard deviation of exactly zero - that division produces NaN, and by
# IEEE754 rules NaN never equals NaN, so a later sanity-check comparing
# two slices of the (now-NaN) array fails unconditionally. A real,
# non-zero test tone has non-zero variance and never enters that path.
RUN python -c "\
import subprocess, tempfile, os; \
work = tempfile.mkdtemp(); \
tone = os.path.join(work, 'warmup.wav'); \
subprocess.run(['ffmpeg', '-f', 'lavfi', '-i', 'sine=frequency=440:duration=10', '-ac', '2', tone], check=True); \
subprocess.run(['demucs', '-n', 'htdemucs', '-d', 'cpu', '-o', work, tone], check=True); \
subprocess.run(['demucs', '-n', 'htdemucs_ft', '-d', 'cpu', '-o', work, tone], check=True); \
"

# Same bake-the-weights treatment for the RoFormer model: one CPU
# separation of a short tone at BUILD time makes audio-separator download
# the checkpoint + its yaml config into /worker/models, which then ships
# inside the image layer - a cold worker never pays for the download.
RUN python -c "\
import subprocess, tempfile, os; \
work = tempfile.mkdtemp(); \
tone = os.path.join(work, 'warmup.wav'); \
subprocess.run(['ffmpeg', '-f', 'lavfi', '-i', 'sine=frequency=440:duration=10', '-ac', '2', tone], check=True); \
sep_mod = __import__('audio_separator.separator', fromlist=['Separator']); \
sep = sep_mod.Separator(model_file_dir='/worker/models', output_dir=work, output_format='WAV'); \
sep.load_model(model_filename='vocals_mel_band_roformer.ckpt'); \
sep.separate(tone); \
"

COPY handler.py .

CMD ["python", "-u", "handler.py"]