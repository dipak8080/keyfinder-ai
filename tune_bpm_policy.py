"""
tune_bpm_policy.py - replay stored votes under different selection rules.

    python tune_bpm_policy.py eval_results.json

Reads only the saved votes + truth, so it runs instantly and needs no audio.
Use it to pick a policy from data before touching audio_analysis.py.
"""
import json
import sys

NAMES = ["tempocnn", "essentia", "percival", "librosa"]
RATIOS = (2.0, 0.5, 1.5, 2.0 / 3.0, 4.0 / 3.0, 0.75, 3.0, 1.0 / 3.0)
LO, HI = 70, 180


def close(a, b, tol=0.04):
    return a and b and abs(a - b) <= tol * max(a, b)


def linked(a, b):
    return close(a, b) or any(close(a * r, b) for r in RATIOS)


def votes_of(r):
    return [(n, r["bpm_votes"].get(n)) for n in NAMES if r["bpm_votes"].get(n)]


def p_single(name):
    def f(v):
        d = dict(v)
        return d.get(name)
    f.__name__ = f"only_{name}"
    return f


def p_priority(order):
    """First detector in `order` that has a value."""
    def f(v):
        d = dict(v)
        for n in order:
            if d.get(n):
                return d[n]
        return None
    f.__name__ = "priority_" + ">".join(x[0].upper() for x in order)
    return f


def p_majority_tiebreak(order):
    """Largest agreeing cluster; ties broken by `order`."""
    def f(v):
        best = None
        for n, b in v:
            sup = [b2 for _, b2 in v if close(b, b2)]
            rank = -order.index(n) if n in order else -len(order)
            if best is None or (len(sup), rank) > (best[0], best[1]):
                best = (len(sup), rank, b)
        return best[2] if best else None
    f.__name__ = "majority_tie_" + ">".join(x[0].upper() for x in order)
    return f


def p_anchor_confirm(order):
    """Trust order[0]; switch only if the other two agree with each other
    AND both disagree with the anchor."""
    def f(v):
        d = dict(v)
        anchor = d.get(order[0])
        if not anchor:
            return p_priority(order)(v)
        others = [d[n] for n in order[1:] if d.get(n)]
        if len(others) == 2 and close(others[0], others[1]) and not close(anchor, others[0]):
            return others[0]
        return anchor
    f.__name__ = "anchor_confirm_" + order[0][0].upper()
    return f


def p_anchor_lowest(order):
    """Trust order[0], but if another detector is metrically linked and sits
    lower while still in range, prefer that (half-time labelling)."""
    def f(v):
        d = dict(v)
        anchor = d.get(order[0])
        if not anchor:
            return p_priority(order)(v)
        cands = [anchor] + [d[n] for n in order[1:] if d.get(n) and linked(anchor, d[n])]
        inrange = [c for c in cands if LO <= c <= HI]
        return min(inrange) if inrange else anchor
    f.__name__ = "anchor_lowest_" + order[0][0].upper()
    return f


def p_anchor_highest(order):
    def f(v):
        d = dict(v)
        anchor = d.get(order[0])
        if not anchor:
            return p_priority(order)(v)
        cands = [anchor] + [d[n] for n in order[1:] if d.get(n) and linked(anchor, d[n])]
        inrange = [c for c in cands if LO <= c <= HI]
        return max(inrange) if inrange else anchor
    f.__name__ = "anchor_highest_" + order[0][0].upper()
    return f


def p_anchor_fold(order, lo, hi):
    """anchor_highest, then fold the result by a metrical ratio if that lands
    inside a preferred window (catches DnB-style half-time readings)."""
    base = p_anchor_highest(order)
    def f(v):
        b = base(v)
        if not b:
            return None
        if lo <= b <= hi:
            return b
        cands = [b * r for r in RATIOS if lo <= b * r <= hi]
        return max(cands) if cands else b
    f.__name__ = f"fold_{lo}_{hi}"
    return f


def sweep_windows(rows, order):
    print("\nPreferred-window sweep (fold anchor_highest into [lo,hi]):")
    print(f"  {'window':>14}  {'exact':>7}")
    best = None
    for lo, hi in [(70,180),(75,180),(80,180),(85,180),(90,180),(95,185),(100,190),
                   (88,175),(90,175),(92,184),(95,190),(100,200),(105,210)]:
        pol = p_anchor_fold(order, lo, hi)
        ex = sum(1 for r in rows
                 if (pr := pol(votes_of(r))) and close(pr, r["truth_bpm"]))
        print(f"  [{lo:3},{hi:3}]      {ex:3}/{len(rows)}")
        if best is None or ex > best[1]:
            best = ((lo, hi), ex)
    print(f"  best window {best[0]} -> {best[1]}/{len(rows)}")
    return best


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "eval_results.json"
    rows = [r for r in json.load(open(path)) if r.get("truth_bpm") and r.get("bpm_votes")]
    if not rows:
        print("no rows with truth_bpm + bpm_votes"); return

    E, P, L, T = "essentia", "percival", "librosa", "tempocnn"
    have_cnn = any(r["bpm_votes"].get(T) for r in rows)
    policies = [
        p_single(E), p_single(P), p_single(L),
        p_priority([E, L, P]),
        p_majority_tiebreak([E, L, P]),
        p_majority_tiebreak([P, E, L]),
        p_anchor_confirm([E, L, P]),
        p_anchor_lowest([E, L, P]),
        p_anchor_highest([E, L, P]),
    ]
    if have_cnn:
        policies += [
            p_single(T),
            p_priority([T, E, L, P]),
            p_anchor_confirm([T, E, L, P]),
            p_anchor_highest([T, E, L, P]),
        ]

    n = len(rows)
    oracle = sum(any(close(b, r["truth_bpm"]) for _, b in votes_of(r)) for r in rows)

    print(f"{n} tracks with tempo labels\n")
    print(f"{'policy':28} {'exact':>7}  {'+octave':>8}")
    print("-" * 46)
    results = []
    for pol in policies:
        ex = oc = 0
        for r in rows:
            pred = pol(votes_of(r))
            if not pred:
                continue
            t = r["truth_bpm"]
            if close(pred, t):
                ex += 1
            elif any(close(pred * m, t) for m in (2.0, 0.5)):
                oc += 1
        results.append((pol.__name__, ex, oc))
        print(f"{pol.__name__:28} {ex:3}/{n}  {ex+oc:4}/{n}")

    best = max(results, key=lambda t: t[1])
    print("-" * 46)
    sweep_windows(rows, [T, E, L, P] if have_cnn else [E, L, P])
    print(f"{'ORACLE (perfect pick)':28} {oracle:3}/{n}")
    print(f"\nbest policy: {best[0]}  ({best[1]}/{n} exact)")

    stuck = [r for r in rows if not any(close(b, r["truth_bpm"]) for _, b in votes_of(r))]
    if stuck:
        print(f"\n{len(stuck)} tracks no detector got right:")
        for r in stuck[:10]:
            print(f"  truth={r['truth_bpm']:>6}  votes={r['bpm_votes']}")


if __name__ == "__main__":
    main()