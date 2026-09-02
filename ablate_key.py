"""
ablate_key.py - measure key accuracy for several detector variants at once.

    python ablate_key.py giantsteps.csv [--limit 40]

For each track it loads the audio once, then runs KeyExtractor under different
profiles and input signals (raw vs HPSS-harmonic, 44.1k vs 22.05k). Prints an
accuracy table so the pipeline is chosen from data, not assumption.
"""
import argparse
import csv
import os
import sys
import time
from collections import defaultdict

import numpy as np
import librosa
from essentia.standard import MonoLoader, KeyExtractor

from utils import normalize_key, relative_minor_of_major, relative_major_of_minor
from audio_analysis import trim_audio_for_analysis, _prep_hpss
from config import ANALYSIS_MAX_SECONDS
from utils import cleanup_file

SHARP_TO_FLAT = {"C#": "Db", "D#": "Eb", "F#": "Gb", "G#": "Ab", "A#": "Bb"}
PROFILES = ["edma", "edmm", "bgate", "shaath", "temperley", "krumhansl"]


def parse_key(s):
    s = s.strip().replace("\u266f", "#").replace("\u266d", "b")
    p = s.split()
    if len(p) != 2:
        return None
    note = p[0][0].upper() + p[0][1:]
    note = SHARP_TO_FLAT.get(note, note)
    return normalize_key(note), ("minor" if p[1].lower().startswith("min") else "major")


def relative_of(k, s):
    return (relative_minor_of_major(k), "minor") if s == "major" else (relative_major_of_minor(k), "major")


def run_profile(sig, sr, profile):
    try:
        ke = KeyExtractor(profileType=profile, sampleRate=sr)
    except Exception:
        return None
    try:
        k, s, strength = ke(sig)
        return normalize_key(k), s, strength
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    rows = []
    with open(args.csv, newline="", encoding="utf-8") as f:
        for rec in csv.DictReader(f):
            if rec.get("key", "").strip() and os.path.exists(rec["path"]):
                rows.append((rec["path"], parse_key(rec["key"])))
    if args.limit:
        rows = rows[:args.limit]
    if not rows:
        print("no usable rows"); sys.exit(1)

    exact = defaultdict(int)
    rel = defaultdict(int)
    strengths = defaultdict(list)
    n = 0

    for path, truth in rows:
        ap_path = trim_audio_for_analysis(path, ANALYSIS_MAX_SECONDS) if ANALYSIS_MAX_SECONDS else path
        try:
            raw44 = MonoLoader(filename=ap_path, sampleRate=44100)()
            harm, _, hsr = _prep_hpss(raw44, 44100)
            raw22 = librosa.resample(raw44, orig_sr=44100, target_sr=22050)
            raw22 = np.ascontiguousarray(raw22, dtype=np.float32)

            variants = {
                "raw44": (raw44, 44100),
                "raw22": (raw22, 22050),
                "harm22": (harm, hsr),
            }
            for vname, (sig, sr) in variants.items():
                for prof in PROFILES:
                    got = run_profile(sig, sr, prof)
                    if not got:
                        continue
                    k, s, strength = got
                    tag = f"{prof}/{vname}"
                    if (k, s) == truth:
                        exact[tag] += 1
                    elif (k, s) == relative_of(*truth):
                        rel[tag] += 1
                    strengths[tag].append(strength)
            n += 1
            print(f"\r{n}/{len(rows)} {os.path.basename(path)[:40]:40}", end="", flush=True)
        except Exception as e:
            print(f"\nERR {os.path.basename(path)}: {e}")
        finally:
            if ap_path != path:
                cleanup_file(ap_path)

    print("\n")
    print(f"{'variant':24} {'exact':>8} {'+relative':>11} {'avg strength':>13}")
    print("-" * 60)
    ranked = sorted(exact.items(), key=lambda kv: -kv[1])
    for tag, ex in ranked:
        r = rel[tag]
        st = np.mean(strengths[tag]) if strengths[tag] else 0
        print(f"{tag:24} {ex:3}/{n} {100*ex/n:4.0f}%  {ex+r:3}/{n} {100*(ex+r)/n:4.0f}%   {st:.3f}")
    if ranked:
        print(f"\nBEST: {ranked[0][0]}  {ranked[0][1]}/{n} ({100*ranked[0][1]/n:.0f}%)")


if __name__ == "__main__":
    main()