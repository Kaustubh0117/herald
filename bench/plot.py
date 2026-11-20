"""Charts for the bench/ experiments. Usage: plot.py KIND CSV PNG SUBTITLE"""
import csv
import sys
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

STAGES = ["bridge", "matcher", "registrar", "gateway", "client"]
COLORS = {"bridge": "#4C78A8", "matcher": "#F58518", "registrar": "#54A24B",
          "gateway": "#B279A2", "client": "#9D755D"}


def rows(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def latency(data, ax):
    """Stacked mean delay per stage for each batching window (Fig. 9)."""
    labels = list(dict.fromkeys(r["label"] for r in data))
    mean = defaultdict(dict)
    p99 = {}
    for r in data:
        mean[r["label"]][r["stage"]] = float(r["mean_ms"])
        if r["stage"] == "total":
            p99[r["label"]] = float(r["p99_ms"])
    bottom = [0.0] * len(labels)
    for s in STAGES:
        vals = [mean[l].get(s, 0.0) for l in labels]
        ax.bar(labels, vals, bottom=bottom, label=s, color=COLORS[s])
        bottom = [b + v for b, v in zip(bottom, vals)]
    for i, l in enumerate(labels):
        ax.annotate(f"mean {bottom[i]:.0f} ms\np99 {p99[l]:.0f} ms", (i, bottom[i]),
                    ha="center", va="bottom", fontsize=8, xytext=(0, 3), textcoords="offset points")
    ax.set_xlabel("propagator batching window")
    ax.set_ylabel("mean commit-to-client delay (ms)")
    ax.set_title("Where the time goes (paper Fig. 9)")
    ax.legend(title="stage reached", fontsize=8)
    ax.set_ylim(0, max(bottom) * 1.25)


KINDS = {"latency": latency}

if __name__ == "__main__":
    kind, src, dst, subtitle = sys.argv[1:5]
    fig, ax = plt.subplots(figsize=(7, 4.5), dpi=150)
    KINDS[kind](rows(src), ax)
    fig.text(0.5, 0.005, subtitle, ha="center", fontsize=7, color="#555")
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(dst)
    print("wrote", dst)
