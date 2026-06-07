"""
Diavazei ta metrics/opps JSONL apo ena experiment kai vgazei:
  - Ena summary sto stdout (counts, latency percentiles, throughput).
  - PNG plots dipla sta metrics (latency CDF, throughput timeline, ktl.)

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # headless backend — grafei PNG, den anoigei windows
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


logger = logging.getLogger("analyze")
sns.set_theme(style="whitegrid", context="talk")


def load_jsonl(path: Path) -> pd.DataFrame:
    """Diavazei ena JSONL se DataFrame. Epistrefei adeio DF an leipei i einai keno."""
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    records = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return pd.DataFrame(records)


def percentile(series: pd.Series, p: float) -> float:
    """Ypologizei to percentile p (0..100) mias series, NaN an einai adeia."""
    return float(np.percentile(series, p)) if len(series) else float("nan")


def summarise(label: str, opps: pd.DataFrame, metrics: pd.DataFrame) -> dict:
    """Ypologizei ta key numbers gia enan consumer."""
    if metrics.empty:
        return {"label": label, "batches": 0, "events": 0, "opps": 0}

    total_events = int(metrics["rows_read"].sum())
    total_opps = int(metrics["opps_found"].sum())
    batches = int(len(metrics))

    proc_ms = metrics["processing_ms"]
    proc_p50 = percentile(proc_ms, 50)
    proc_p95 = percentile(proc_ms, 95)
    proc_p99 = percentile(proc_ms, 99)

    e2e = opps["e2e_latency_ms"] if not opps.empty else pd.Series(dtype=float)
    lat_p50 = percentile(e2e, 50)
    lat_p95 = percentile(e2e, 95)
    lat_p99 = percentile(e2e, 99)

    # Throughput se events/sec: synolo events / xroniko diastima
    if len(metrics) >= 2:
        span_s = (metrics["wall_time_ms"].max() - metrics["wall_time_ms"].min()) / 1000.0
        throughput = total_events / span_s if span_s > 0 else float("nan")
    else:
        throughput = float("nan")

    return {
        "label":            label,
        "batches":          batches,
        "events":           total_events,
        "opps":             total_opps,
        "proc_ms_p50":      proc_p50,
        "proc_ms_p95":      proc_p95,
        "proc_ms_p99":      proc_p99,
        "lat_ms_p50":       lat_p50,
        "lat_ms_p95":       lat_p95,
        "lat_ms_p99":       lat_p99,
        "throughput_ev_s":  throughput,
    }


def print_summary(rows: list[dict]) -> None:
    print()
    print("=" * 78)
    print(f"{'metric':<22}  {'BATCH':>14}  {'STREAM':>14}  {'ratio batch/stream':>18}")
    print("-" * 78)

    if len(rows) < 2:
        print(" (only one consumer ran)")
        for r in rows:
            for k, v in r.items():
                print(f"{k:<22}  {v}")
        return

    b, s = rows[0], rows[1]
    def fmt(v):
        if isinstance(v, float):
            if v != v: return "n/a"  # nan
            return f"{v:>14,.1f}"
        return f"{v:>14}"

    keys = ["events", "opps", "batches",
            "proc_ms_p50", "proc_ms_p95", "proc_ms_p99",
            "lat_ms_p50", "lat_ms_p95", "lat_ms_p99",
            "throughput_ev_s"]
    for k in keys:
        bv, sv = b[k], s[k]
        try:
            ratio = bv / sv if (isinstance(bv, (int, float)) and isinstance(sv, (int, float)) and sv) else float("nan")
            ratio_s = f"{ratio:>18.2f}x" if ratio == ratio else f"{'n/a':>18}"
        except Exception:
            ratio_s = f"{'n/a':>18}"
        print(f"{k:<22}  {fmt(bv)}  {fmt(sv)}  {ratio_s}")
    print("=" * 78)
    print()


# ----------------------- Plots -----------------------

def plot_latency_cdf(batch_opps: pd.DataFrame, stream_opps: pd.DataFrame, out: Path) -> None:
    """CDF tou end-to-end latency. Stream prepei na einai polly aristera tou Batch."""
    fig, ax = plt.subplots(figsize=(10, 6))
    for name, df, color in [("Batch (Methodos A)", batch_opps, "#d62728"),
                            ("Stream (Methodos B)", stream_opps, "#1f77b4")]:
        if df.empty:
            continue
        s = np.sort(df["e2e_latency_ms"].values / 1000.0)   # se seconds
        y = np.arange(1, len(s) + 1) / len(s)
        ax.plot(s, y, label=f"{name}  (n={len(s)})", color=color, lw=2.5)
    ax.set_xlabel("End-to-end detection latency (seconds)")
    ax.set_ylabel("CDF (fraction of opportunities)")
    ax.set_title("Arbitrage detection latency — CDF")
    ax.legend(loc="lower right")
    ax.set_ylim(0, 1.02)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", out)


def plot_throughput_timeline(batch_m: pd.DataFrame, stream_m: pd.DataFrame, out: Path) -> None:
    """Events pou epeksergastikan ana batch sto xrono."""
    fig, axes = plt.subplots(2, 1, figsize=(11, 7), sharex=True)
    for ax, (name, df, color) in zip(axes, [
        ("Batch (Methodos A)", batch_m, "#d62728"),
        ("Stream (Methodos B)", stream_m, "#1f77b4"),
    ]):
        if df.empty:
            ax.text(0.5, 0.5, "no data", ha="center", transform=ax.transAxes)
            continue
        t = (df["wall_time_ms"] - df["wall_time_ms"].min()) / 1000.0
        ax.plot(t, df["rows_read"], "o-", color=color, lw=1.5, ms=4, label=name)
        ax.set_ylabel("Events read / batch")
        ax.legend(loc="upper right")
    axes[-1].set_xlabel("Time since experiment start (seconds)")
    fig.suptitle("Per-batch event volume", y=0.995)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", out)


def plot_processing_time(batch_m: pd.DataFrame, stream_m: pd.DataFrame, out: Path) -> None:
    """Box-plot tou per-batch processing time."""
    fig, ax = plt.subplots(figsize=(8, 5))
    data, labels = [], []
    if not batch_m.empty:
        data.append(batch_m["processing_ms"].values); labels.append("Batch")
    if not stream_m.empty:
        data.append(stream_m["processing_ms"].values); labels.append("Stream")
    if not data:
        return
    bp = ax.boxplot(data, labels=labels, showfliers=True, patch_artist=True,
                    boxprops=dict(linewidth=1.5),
                    medianprops=dict(linewidth=2, color="black"))
    colors = ["#d62728", "#1f77b4"]
    for patch, c in zip(bp["boxes"], colors[:len(bp["boxes"])]):
        patch.set_facecolor(c); patch.set_alpha(0.6)
    ax.set_ylabel("Processing time per batch (ms)")
    ax.set_title("Per-batch processing time distribution")
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", out)


def plot_catch_overlap(batch_opps: pd.DataFrame, stream_opps: pd.DataFrame, out: Path) -> None:
    """Venn-style summary: posa opportunities epiase i kathe methodos, posa koina?"""
    bset = set(batch_opps.get("match_id", [])) if not batch_opps.empty else set()
    sset = set(stream_opps.get("match_id", [])) if not stream_opps.empty else set()
    only_b = len(bset - sset)
    only_s = len(sset - bset)
    both = len(bset & sset)

    fig, ax = plt.subplots(figsize=(7, 5))
    labels = ["Only Batch", "Both", "Only Stream"]
    values = [only_b, both, only_s]
    colors = ["#d62728", "#9467bd", "#1f77b4"]
    bars = ax.bar(labels, values, color=colors, alpha=0.8)
    for b, v in zip(bars, values):
        ax.text(b.get_x() + b.get_width()/2, b.get_height() + max(values)*0.01,
                str(v), ha="center", fontsize=12)
    ax.set_ylabel("Unique matches with arbitrage detected")
    ax.set_title(f"Opportunity catch coverage (total unique = {len(bset | sset)})")
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", out)


# ----------------------- Main -----------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", default=None,
                        help="Analyse benchmarks/results/experiment_<TAG>/. "
                             "An leipei, koitaei to benchmarks/results/ aytousios.")
    parser.add_argument("--results-dir", type=Path,
                        default=Path(__file__).resolve().parent / "results")
    parser.add_argument("--plots-dir", type=Path, default=None,
                        help="Pou na sosei ta PNG (default: <results>/plots/)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    src_dir = args.results_dir / f"experiment_{args.tag}" if args.tag else args.results_dir
    if not src_dir.exists():
        raise SystemExit(f"Results dir not found: {src_dir}")

    plots_dir = args.plots_dir or (src_dir / "plots")
    plots_dir.mkdir(parents=True, exist_ok=True)

    batch_m = load_jsonl(src_dir / "batch_metrics.jsonl")
    batch_opps = load_jsonl(src_dir / "batch_opps.jsonl")
    stream_m = load_jsonl(src_dir / "stream_metrics.jsonl")
    stream_opps = load_jsonl(src_dir / "stream_opps.jsonl")

    rows = [
        summarise("Batch (Methodos A)", batch_opps, batch_m),
        summarise("Stream (Methodos B)", stream_opps, stream_m),
    ]
    print_summary(rows)

    # Sozoume ta summary san JSON gia tin anafora
    (plots_dir / "summary.json").write_text(json.dumps(rows, indent=2))
    logger.info("Saved %s", plots_dir / "summary.json")

    # Plots
    plot_latency_cdf(batch_opps, stream_opps, plots_dir / "latency_cdf.png")
    plot_throughput_timeline(batch_m, stream_m, plots_dir / "throughput_timeline.png")
    plot_processing_time(batch_m, stream_m, plots_dir / "processing_time_box.png")
    plot_catch_overlap(batch_opps, stream_opps, plots_dir / "catch_overlap.png")

    print(f"All plots saved to: {plots_dir}")


if __name__ == "__main__":
    main()
