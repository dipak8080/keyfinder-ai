import importlib
import os
import sys
import types
import unittest
from unittest import mock

import numpy as np
import soundfile as sf

for _name in ("runpod", "requests"):
    try:
        importlib.import_module(_name)
    except ImportError:
        sys.modules[_name] = types.ModuleType(_name)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import handler as H  # noqa: E402

SR = 44100
FRAMES = SR
LEVELS = {"vocals": 0.2, "drums": 0.05, "bass": 0.06, "other": 0.03, "guitar": 0.02, "piano": 0.01}
MIX_LEVEL = 0.5


def _wav(path, level):
    sf.write(path, np.full((FRAMES, 2), level, dtype="float32"), SR, subtype="PCM_16", format="WAV")
    return path


def _level(path):
    data, _ = sf.read(path, dtype="float32", always_2d=True)
    return float(data.mean())


class FakeWorker:
    def __init__(self):
        self.separator_calls = []
        self.demucs_calls = []
        self.uploads = {}
        self.normalise_args = None

    def download(self, job_id, dest):
        _wav(dest, MIX_LEVEL)

    def normalise(self, input_path, work_dir, clip_start=None, clip_seconds=None):
        self.normalise_args = (clip_start, clip_seconds)
        return _wav(os.path.join(work_dir, "input_clean.wav"), MIX_LEVEL)

    def run_separator(self, model_filename, input_path, work_dir, names):
        self.separator_calls.append((model_filename, input_path))
        out = {}
        for stem, base in names.items():
            key = stem.lower()
            level = LEVELS.get(key, 0.1)
            if key in ("instrumental", "other") and model_filename == H.ROFORMER_MODEL_FILENAME:
                level = 0.3
            out[base] = _wav(os.path.join(work_dir, f"{base}.wav"), level)
        return out, 1.0

    def run_demucs(self, input_path, work_dir, model, overlap, two_stems):
        self.demucs_calls.append((model, two_stems))
        stem = os.path.splitext(os.path.basename(input_path))[0]
        track_dir = os.path.join(work_dir, model, stem)
        os.makedirs(track_dir, exist_ok=True)
        names = ("vocals", "no_vocals") if two_stems else H.MODEL_STEM_NAMES[model]
        for n in names:
            _wav(os.path.join(track_dir, f"{n}.wav"), LEVELS.get(n, 0.1))
        return track_dir, 2.0

    def upload(self, job_id, name, path):
        self.uploads[name] = _level(path)


class HandlerTests(unittest.TestCase):
    def run_job(self, engine="sw", **inp):
        fake = FakeWorker()
        payload = {"job_id": "job1", "filename": "song.mp3", **inp}
        with mock.patch.object(H, "VPS_BASE_URL", "http://vps"), \
             mock.patch.object(H, "GPU_SHARED_SECRET", "s"), \
             mock.patch.object(H, "STUDIO_ENGINE", engine), \
             mock.patch.object(H, "_download_input", fake.download), \
             mock.patch.object(H, "_get_duration_seconds", lambda p: 240.0), \
             mock.patch.object(H, "_normalise_input", fake.normalise), \
             mock.patch.object(H, "_run_separator", fake.run_separator), \
             mock.patch.object(H, "_run_demucs_gpu", fake.run_demucs), \
             mock.patch.object(H, "_upload_result", fake.upload):
            result = H.handler({"input": payload})
        return result, fake

    def models_used(self, fake):
        return [m for m, _ in fake.separator_calls]

    def test_standard_vocal_remover(self):
        result, fake = self.run_job(task="separate", model="htdemucs")
        self.assertNotIn("error", result)
        self.assertEqual(result["uploaded_stems"], ["vocals", "instrumental"])
        self.assertEqual(fake.demucs_calls, [("htdemucs", True)])
        self.assertEqual(fake.separator_calls, [])
        self.assertNotIn("studio_engine", result)

    def test_standard_stems(self):
        result, fake = self.run_job(task="stems", model="htdemucs")
        self.assertEqual(result["uploaded_stems"], ["vocals", "drums", "bass", "other"])
        self.assertEqual(fake.demucs_calls, [("htdemucs", False)])
        self.assertEqual(fake.separator_calls, [])

    def test_studio_vocal_remover_sw(self):
        result, fake = self.run_job(task="separate", model="melband_roformer")
        self.assertNotIn("error", result)
        self.assertEqual(result["uploaded_stems"], ["vocals", "instrumental"])
        self.assertEqual(self.models_used(fake), [H.SW_MODEL_FILENAME])
        self.assertEqual(fake.demucs_calls, [])
        self.assertAlmostEqual(fake.uploads["vocals"], LEVELS["vocals"], places=3)
        self.assertAlmostEqual(fake.uploads["instrumental"], MIX_LEVEL - LEVELS["vocals"], places=3)
        self.assertEqual(result["studio_engine"], "sw")

    def test_studio_four_stems_sw(self):
        result, fake = self.run_job(task="stems", model="melband_roformer")
        self.assertEqual(result["uploaded_stems"], ["vocals", "drums", "bass", "other"])
        self.assertEqual(self.models_used(fake), [H.SW_MODEL_FILENAME])
        self.assertEqual(fake.demucs_calls, [])
        folded = LEVELS["other"] + LEVELS["guitar"] + LEVELS["piano"]
        self.assertAlmostEqual(fake.uploads["other"], folded, places=3)
        self.assertAlmostEqual(fake.uploads["drums"], LEVELS["drums"], places=3)

    def test_studio_six_stems_sw(self):
        result, fake = self.run_job(task="stems", model="melband_roformer", stem_count=6)
        self.assertEqual(result["uploaded_stems"], ["vocals", "drums", "bass", "other", "guitar", "piano"])
        self.assertEqual(self.models_used(fake), [H.SW_MODEL_FILENAME])
        self.assertEqual(fake.demucs_calls, [])
        for stem in ("other", "guitar", "piano"):
            self.assertAlmostEqual(fake.uploads[stem], LEVELS[stem], places=3)

    def test_vocal_options_run_on_sw_vocals(self):
        result, fake = self.run_job(
            task="stems", model="melband_roformer", stem_count=6,
            vocal_options=["dereverb", "lead_back"],
        )
        self.assertNotIn("error", result)
        for name in ("vocals_dry", "lead_vocals", "backing_vocals"):
            self.assertIn(name, result["uploaded_stems"])
        models = self.models_used(fake)
        self.assertEqual(models, [H.SW_MODEL_FILENAME, H.DEREVERB_MODEL_FILENAME, H.KARAOKE_MODEL_FILENAME])
        for _, input_path in fake.separator_calls[1:]:
            self.assertEqual(os.path.basename(input_path), "sw_vocals.wav")

    def test_vocal_options_on_vocal_remover(self):
        result, fake = self.run_job(task="separate", model="melband_roformer", vocal_options=["dereverb"])
        self.assertEqual(result["uploaded_stems"], ["vocals", "instrumental", "vocals_dry"])

    def test_preview_clip(self):
        result, fake = self.run_job(
            task="stems", model="melband_roformer", stem_count=6,
            clip_start=60, clip_seconds=30, max_duration_seconds=60,
        )
        self.assertNotIn("error", result)
        self.assertEqual(fake.normalise_args, (60.0, 30.0))
        self.assertEqual(result["clip"], {"start": 60.0, "seconds": 30.0, "source_duration": 240.0})
        self.assertEqual(result["duration_seconds"], 30.0)

    def test_full_track_duration_limit_still_applies(self):
        result, _ = self.run_job(task="stems", model="melband_roformer", max_duration_seconds=60)
        self.assertIn("exceeds", result["error"])

    def test_legacy_engine_vocal_remover(self):
        result, fake = self.run_job(engine="legacy", task="separate", model="melband_roformer")
        self.assertEqual(result["uploaded_stems"], ["vocals", "instrumental"])
        self.assertEqual(self.models_used(fake), [H.ROFORMER_MODEL_FILENAME])
        self.assertEqual(result["studio_engine"], "legacy")

    def test_legacy_engine_six_stems(self):
        result, fake = self.run_job(engine="legacy", task="stems", model="melband_roformer", stem_count=6)
        self.assertEqual(result["uploaded_stems"], ["vocals", "drums", "bass", "other", "guitar", "piano"])
        self.assertEqual(fake.demucs_calls, [("htdemucs_ft", False), ("htdemucs_6s", False)])

    def test_validation_unchanged(self):
        result, _ = self.run_job(task="stems", model="htdemucs", stem_count=6)
        self.assertIn("stem_count 6", result["error"])
        result, _ = self.run_job(task="separate", model="htdemucs", vocal_options=["dereverb"])
        self.assertIn("vocal_options", result["error"])
        result, _ = self.run_job(task="stems", model="bogus")
        self.assertIn("Unsupported model", result["error"])


if __name__ == "__main__":
    unittest.main(verbosity=2)