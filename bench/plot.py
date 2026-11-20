"""Charts for the bench/ experiments. Usage: plot.py KIND CSV PNG SUBTITLE"""
import csv
import sys
import textwrap
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


def totals(data, xkey):
    """One point per run: x, total p50/p99, and the run's resource columns."""
    pts = []
    for r in data:
        if r["stage"] == "total":
            pts.append((float(r[xkey]), float(r["p50_ms"]), float(r["p99_ms"]),
                        float(r["server_cpu"]), float(r["server_rss_mb"]), float(r["sent_rate"])))
    return sorted(pts)


def resources_and_delay(ax, pts, xlabel, extra=None):
    """Left axis: cell CPU (cores). Right axis: delay p50/p99."""
    xs = [p[0] for p in pts]
    ax.plot(xs, [p[3] for p in pts], "o-", color="#4C78A8", label="cell CPU (cores)")
    if extra:
        extra(ax, pts)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("cell CPU (cores)", color="#4C78A8")
    ax.set_ylim(bottom=0)
    ax2 = ax.twinx()
    ax2.plot(xs, [p[1] for p in pts], "s--", color="#54A24B", label="delay p50")
    ax2.plot(xs, [p[2] for p in pts], "^--", color="#E45756", label="delay p99")
    ax2.set_ylabel("commit-to-client delay (ms)")
    ax2.set_ylim(bottom=0, top=max(p[2] for p in pts) * 1.3)
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, fontsize=8, loc="upper left")


def clients(data, ax):
    """CPU, memory and delay as clients grow (Fig. 6)."""
    pts = totals(data, "clients")

    def rss(ax, pts):
        for x, _, _, cpu, mb, _ in pts:
            ax.annotate(f"{mb:.0f} MB", (x, cpu), fontsize=7, xytext=(0, 6),
                        textcoords="offset points", ha="center", color="#4C78A8")

    resources_and_delay(ax, pts, "connected clients", rss)
    ax.set_title("Scale with clients (paper Fig. 6); labels: cell memory")


def rate(data, ax):
    """CPU and delay as the update rate grows (Fig. 7)."""
    pts = totals(data, "rate")

    def sent(ax, pts):
        for p in pts:
            if p[5] < 0.95 * p[0]:
                ax.annotate(f"sent {p[5]:.0f}/s", (p[0], p[3]), fontsize=7,
                            xytext=(0, 6), textcoords="offset points", ha="center")

    resources_and_delay(ax, pts, "UPDATEs per second", sent)
    ax.set_xscale("log")
    ax.set_title("Scale with notification rate (paper Fig. 7)")


def brownout(data, fig_ax):
    """Timeline of a partial storage outage (Fig. 11)."""
    fig = fig_ax.figure
    fig.clear()
    axes = fig.subplots(3, 1, sharex=True)
    t = [int(r["t_s"]) for r in data]

    def series(col):
        return [float(r[col] or 0) for r in data]

    def rate_of(col):
        v = series(col)
        return [0.0] + [max(b - a, 0) for a, b in zip(v, v[1:])]

    axes[0].plot(t, series("updates"), color="#999", label="UPDATEs/s")
    axes[0].plot(t, series("delivered"), color="#4C78A8", label="notifications delivered/s")
    axes[0].set_ylabel("per second")
    axes[0].legend(fontsize=7, loc="upper left")
    axes[1].bar(t, rate_of("scratch_buffered"), color="#F58518", label="parked in scratch/s")
    axes[1].bar(t, [-x for x in rate_of("scratch_drained")], color="#54A24B", label="drained/s (down)")
    axes[1].set_ylabel("versions")
    axes[1].legend(fontsize=7, loc="upper left")
    parked, drained = series("scratch_buffered")[-1], series("scratch_drained")[-1]
    axes[1].annotate(f"{parked:.0f} versions parked, {drained:.0f} scratch rows drained:\n"
                     "versions of one object overwrite one slot",
                     (0.99, 0.08), xycoords="axes fraction", ha="right", va="bottom", fontsize=7)
    axes[2].plot(t, [x / 1024 for x in series("slot_lag_bytes")], color="#E45756", label="slot lag (KiB)")
    axes[2].set_ylabel("KiB")
    axes[2].set_xlabel("seconds")
    axes[2].legend(fontsize=7, loc="upper left")
    for ax in axes:
        ax.axvspan(30, 90, color="#E45756", alpha=0.08)
    axes[0].set_title("Partial storage outage (paper Fig. 11); shaded: 25% of keys unwritable")


KINDS = {"latency": latency, "clients": clients, "rate": rate, "brownout": brownout}

if __name__ == "__main__":
    kind, src, dst, subtitle = sys.argv[1:5]
    fig, ax = plt.subplots(figsize=(7, 6.5 if kind == "brownout" else 4.5), dpi=150)
    KINDS[kind](rows(src), ax)
    fig.text(0.5, 0.005, "\n".join(textwrap.wrap(subtitle, 120)), ha="center", va="bottom", fontsize=7, color="#555")
    fig.tight_layout(rect=(0, 0.05, 1, 1))
    fig.savefig(dst)
    print("wrote", dst)
