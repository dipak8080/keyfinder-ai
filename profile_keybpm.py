import sys
import time
from collections import defaultdict

import audio_analysis as A
from config import ANALYSIS_MAX_SECONDS

TIMES = defaultdict(list)


def timed(name, fn):
    def wrapper(*args, **kwargs):
        t = time.perf_counter()
        try:
            return fn(*args, **kwargs)
        finally:
            TIMES[name].append(time.perf_counter() - t)
    return wrapper


def wrap_essentia_class(name):
    cls = getattr(A, name)

    class Timed(cls):
        def __call__(self, *args, **kwargs):
            t = time.perf_counter()
            try:
                return super().__call__(*args, **kwargs)
            finally:
                TIMES[name].append(time.perf_counter() - t)

    setattr(A, name, Timed)


for cls_name in ("MonoLoader", "RhythmExtractor2013", "KeyExtractor"):
    if hasattr(A, cls_name):
        wrap_essentia_class(cls_name)

for fn_name in (
    "trim_audio_for_analysis",
    "_prep_hpss",
    "relative_key_scores",
    "correct_relative_major_minor",
    "_percival_bpm",
    "_tempocnn_bpm",
    "_librosa_key_bpm_from_audio",
    "_estimate_tempo",
    "detect_key_bpm_essentia",
    "cross_check_with_librosa",
):
    if hasattr(A, fn_name):
        setattr(A, fn_name, timed(fn_name, getattr(A, fn_name)))


def run(path):
    t0 = time.perf_counter()
    p = A.trim_audio_for_analysis(path, ANALYSIS_MAX_SECONDS) if ANALYSIS_MAX_SECONDS else path
    key, scale, kc, bpm, bc, audio, sr = A.detect_key_bpm_essentia(p)
    key, scale, kc, bpm, bc, agr = A.cross_check_with_librosa(audio, sr, key, scale, kc, bpm, bc)
    del audio
    total = time.perf_counter() - t0
    TIMES["TOTAL"].append(total)
    print(f"{path.split('/')[-1]:30} {key} {scale}/{bpm}  {total:.1f}s")


def main():
    files = sys.argv[1:]
    if not files:
        print("usage: python profile_keybpm.py track1.mp3 [track2.mp3 ...]")
        return
    run(files[0])
    TIMES.clear()
    for f in files:
        run(f)

    total = sum(TIMES["TOTAL"]) / len(TIMES["TOTAL"])
    print(f"\nANALYSIS_MAX_SECONDS={ANALYSIS_MAX_SECONDS}   tracks={len(files)} (first run excluded as warm-up)")
    print(f"{'stage':32} {'avg s':>7} {'% of total':>11}")
    for name, vals in sorted(TIMES.items(), key=lambda kv: -sum(kv[1]) / len(files)):
        avg = sum(vals) / len(files)
        print(f"{name:32} {avg:7.2f} {100 * avg / total:10.0f}%")


if __name__ == "__main__":
    main()