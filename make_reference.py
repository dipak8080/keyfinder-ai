"""
make_reference.py - build reference.csv from GiantSteps dataset folders.

    python make_reference.py --key-dir giantsteps-key-dataset-master \
                             --tempo-dir giantsteps-tempo-dataset-master \
                             --limit 40 --out reference.csv

Either dir may be omitted. Tracks present in both get key AND bpm.
Only tracks whose audio file actually exists are written.
"""
import argparse
import csv
import glob
import os
import re

NOTE_FIX = {"C#": "Db", "D#": "Eb", "F#": "Gb", "G#": "Ab", "A#": "Bb"}


def _read_label(path):
    try:
        with open(path, encoding="utf-8", errors="ignore") as f:
            lines = [l.strip() for l in f if l.strip() and not l.startswith("#")]
        return lines[-1] if lines else None
    except Exception:
        return None


def parse_key(raw):
    if not raw:
        return None
    txt = raw.replace("\t", " ").strip()
    m = re.search(r"\b([A-Ga-g][#b\u266f\u266d]?)\s*(major|minor|maj|min)\b", txt, re.I)
    if not m:
        return None
    note = m.group(1)[0].upper() + m.group(1)[1:].replace("\u266f", "#").replace("\u266d", "b")
    note = NOTE_FIX.get(note, note)
    scale = "minor" if m.group(2).lower().startswith("min") else "major"
    return f"{note} {scale}"


def parse_bpm(raw):
    if not raw:
        return None
    nums = re.findall(r"\d+(?:\.\d+)?", raw)
    if not nums:
        return None
    val = float(nums[-1])
    return str(int(round(val))) if 20 <= val <= 300 else None


def track_id(path):
    return os.path.basename(path).split(".")[0]


def collect(root, subdirs, exts, parser):
    out = {}
    if not root:
        return out
    for sub in subdirs:
        for ext in exts:
            for f in glob.glob(os.path.join(root, sub, f"*{ext}")):
                v = parser(_read_label(f))
                if v:
                    out.setdefault(track_id(f), v)
    return out


def find_audio(roots):
    idx = {}
    for root in roots:
        if not root:
            continue
        for ext in ("*.mp3", "*.wav", "*.flac", "*.m4a"):
            for f in glob.glob(os.path.join(root, "audio", ext)):
                idx.setdefault(track_id(f), os.path.abspath(f))
    return idx


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--key-dir")
    ap.add_argument("--tempo-dir")
    ap.add_argument("--limit", type=int, default=0, help="0 = all")
    ap.add_argument("--out", default="reference.csv")
    args = ap.parse_args()

    if not args.key_dir and not args.tempo_dir:
        ap.error("pass --key-dir and/or --tempo-dir")

    keys = collect(args.key_dir, ("annotations/key", "annotations/giantsteps"), (".key", ".txt"), parse_key)
    bpms = collect(args.tempo_dir, ("annotations/tempo", "annotations/giantsteps", "annotations_v2/tempo"),
                   (".bpm", ".txt"), parse_bpm)
    audio = find_audio([args.key_dir, args.tempo_dir])

    print(f"key labels: {len(keys)}   bpm labels: {len(bpms)}   audio files: {len(audio)}")
    if not audio:
        print("\nNo audio found. Run ./audio_dl.sh inside the dataset folder first.")
        return

    rows = []
    for tid, path in sorted(audio.items()):
        k, b = keys.get(tid, ""), bpms.get(tid, "")
        if k or b:
            rows.append((path, k, b))

    if args.limit:
        both = [r for r in rows if r[1] and r[2]]
        rest = [r for r in rows if not (r[1] and r[2])]
        rows = (both + rest)[:args.limit]

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["path", "key", "bpm"])
        w.writerows(rows)

    print(f"wrote {len(rows)} rows -> {args.out}")
    print(f"  with key: {sum(1 for r in rows if r[1])}   with bpm: {sum(1 for r in rows if r[2])}")
    minor = sum(1 for r in rows if "minor" in r[1])
    print(f"  minor: {minor}   major: {sum(1 for r in rows if 'major' in r[1])}")


if __name__ == "__main__":
    main()