"""
eval_keybpm.py - score the key/BPM engine against a labelled reference set.

    python eval_keybpm.py reference.csv [--out results.json] [--no-trim]

CSV columns: path,key,bpm   e.g.   /data/ref/thief.wav,F# major,150

Prints per-track hits, overall accuracy, and a sweep of REL_KEY_MARGIN so
the threshold is picked from data rather than by hand. Re-run after every
engine change and compare the summary line.
"""
import argparse
import csv
import json
import os
import sys
import time

import numpy as np

import audio_analysis as A
from audio_analysis import (
    detect_key_bpm_essentia, cross_check_with_librosa, trim_audio_for_analysis,
)
from config import ANALYSIS_MAX_SECONDS
from utils import normalize_key, cleanup_file, relative_minor_of_major, relative_major_of_minor

SHARP_TO_FLAT = {"C#": "Db", "D#": "Eb", "F#": "Gb", "G#": "Ab", "A#": "Bb"}


def parse_key(s):
    s = s.strip().replace("♯", "#").replace("♭", "b")
    parts = s.split()
    if len(parts) != 2:
        raise ValueError(f"bad key '{s}' (want e.g. 'F# major')")
    note, scale = parts[0], parts[1].lower()
    note = note[0].upper() + note[1:]
    note = SHARP_TO_FLAT.get(note, note)
    scale = "minor" if scale.startswith("min") else "major"
    return normalize_key(note), scale


def bpm_match(pred, truth, tol=0.03):
    for r in (1.0, 2.0, 0.5):
        if abs(pred - truth * r) <= tol * truth * r:
            return "exact" if r == 1.0 else "octave"
    return "miss"


def run_track(path, trim):
    analysis_path = path
    if trim and ANALYSIS_MAX_SECONDS:
        analysis_path = trim_audio_for_analysis(path, ANALYSIS_MAX_SECONDS)
    try:
        key, scale, kc, bpm, bc, audio, sr = detect_key_bpm_essentia(analysis_path)
        key, scale, kc, bpm, bc, agr = cross_check_with_librosa(audio, sr, key, scale, kc, bpm, bc)
        del audio
        return key, scale, kc, bpm, bc, agr
    finally:
        if analysis_path != path:
            cleanup_file(analysis_path)


def sweep_margin(rows):
    """Re-decide the relative major/minor flip offline for a range of margins."""
    usable = [r for r in rows if r.get("rel") and r["truth_key"]]
    if not usable:
        return []
    out = []
    for m in np.arange(0.85, 1.40, 0.025):
        hits = 0
        for r in usable:
            s = r["rel"]
            k, sc = s["raw_key"], s["raw_scale"]
            st = s.get("raw_strength")
            if st is not None and st >= A.KEY_TRUST_STRENGTH:
                hits += (k, sc) == r["truth_key"]
                continue
            if sc == "minor" and s["major_score"] > s["minor_score"] * m:
                k, sc = s["major_key"], "major"
            elif sc == "major" and s["minor_score"] > s["major_score"] * m:
                k, sc = s["minor_key"], "minor"
            hits += (k, sc) == r["truth_key"]
        out.append((round(float(m), 3), hits, len(usable)))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv")
    ap.add_argument("--out", default="eval_results.json")
    ap.add_argument("--no-trim", action="store_true")
    args = ap.parse_args()

    rows = []
    with open(args.csv, newline="") as f:
        for i, rec in enumerate(csv.DictReader(f)):
            path = rec["path"].strip()
            if not os.path.exists(path):
                print(f"SKIP missing: {path}")
                continue
            truth_key = parse_key(rec["key"]) if rec.get("key", "").strip() else None
            truth_bpm = int(float(rec["bpm"])) if rec.get("bpm", "").strip() else None

            t0 = time.time()
            try:
                key, scale, kc, bpm, bc, agr = run_track(path, not args.no_trim)
            except Exception as e:
                print(f"ERR  {os.path.basename(path)}: {e}")
                continue
            dt = time.time() - t0

            key_status = "-"
            if truth_key:
                if (key, scale) == truth_key:
                    key_status = "HIT"
                else:
                    tk, ts = truth_key
                    rel = (relative_minor_of_major(tk), "minor") if ts == "major" else (relative_major_of_minor(tk), "major")
                    key_status = "REL" if (key, scale) == rel else "MISS"
            bpm_status = bpm_match(bpm, truth_bpm) if truth_bpm else "-"

            votes = agr.get("bpm_votes", {})
            print(f"{key_status:4} {bpm_status:6} {os.path.basename(path)[:44]:44} "
                  f"pred={key} {scale}/{bpm}  truth={rec.get('key','')}/{rec.get('bpm','')}  "
                  f"ess={agr.get('essentia_key')} votes={votes} {agr.get('bpm_mode')} {dt:.1f}s")

            rows.append({
                "path": path, "truth_key": truth_key, "truth_bpm": truth_bpm,
                "pred_key": [key, scale], "pred_bpm": bpm,
                "key_conf": round(kc, 3), "bpm_conf": bc,
                "key_status": key_status, "bpm_status": bpm_status,
                "rel": agr.get("rel_key_scores"), "essentia_key": agr.get("essentia_key"),
                "bpm_votes": votes, "bpm_mode": agr.get("bpm_mode"), "seconds": round(dt, 1),
            })

    if not rows:
        print("no rows evaluated"); sys.exit(1)

    with open(args.out, "w") as f:
        json.dump(rows, f, indent=1)

    kt = [r for r in rows if r["truth_key"]]
    bt = [r for r in rows if r["truth_bpm"]]
    k_hit = sum(r["key_status"] == "HIT" for r in kt)
    k_rel = sum(r["key_status"] == "REL" for r in kt)
    b_ex = sum(r["bpm_status"] == "exact" for r in bt)
    b_oct = sum(r["bpm_status"] == "octave" for r in bt)

    print("\n================ SUMMARY ================")
    if kt:
        print(f"KEY  exact {k_hit}/{len(kt)} ({100*k_hit/len(kt):.0f}%)   "
              f"exact-or-relative {k_hit+k_rel}/{len(kt)} ({100*(k_hit+k_rel)/len(kt):.0f}%)   "
              f"wrong-notes {len(kt)-k_hit-k_rel}")
    if bt:
        print(f"BPM  exact {b_ex}/{len(bt)} ({100*b_ex/len(bt):.0f}%)   "
              f"exact-or-octave {b_ex+b_oct}/{len(bt)} ({100*(b_ex+b_oct)/len(bt):.0f}%)   "
              f"miss {len(bt)-b_ex-b_oct}")
    print(f"avg {np.mean([r['seconds'] for r in rows]):.1f}s/track   "
          f"REL_KEY_MARGIN={A.REL_KEY_MARGIN}  KEY_TRUST_STRENGTH={A.KEY_TRUST_STRENGTH}")
    raw_hits = sum(1 for r in kt if r["rel"] and (r["rel"]["raw_key"], r["rel"]["raw_scale"]) == r["truth_key"])
    scored = sum(1 for r in kt if r["rel"])
    if scored:
        print(f"     (Essentia raw key alone, no correction: {raw_hits}/{scored})")

    sw = sweep_margin(rows)
    if sw:
        best = max(sw, key=lambda t: t[1])
        print("\nREL_KEY_MARGIN sweep (margin -> key hits):")
        print("  " + "  ".join(f"{m}:{h}" for m, h, n in sw))
        print(f"  best = {best[0]} -> {best[1]}/{best[2]}   (set REL_KEY_MARGIN to this)")
    print(f"\ndetails -> {args.out}")


if __name__ == "__main__":
    main()