"""
Side-by-side sygkrisi pollon tagged experiments.
Vgazei:
  - Ena consolidated CSV me ta summary metrics olon ton experiments.
  - Grouped-bar plot tou latency p50 / p95 ana methodos ana experiment.
  - Grouped-bar plot tou throughput ana methodos ana experiment.

Xrhsh apo to project root:
    source scripts/activate.sh
    python -m benchmarks.compare_experiments --tags fast slow_batch
"""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


logger = logging.getLogger("compare")
sns.set_theme(style="whitegrid", context="talk")


def load_summary(results_dir: Path, tag: str) -> list[dict]:
    """Fortwnei to summary.json pou egrapse to analyze_results gia ena experiment."""
    p = results_dir / f"experiment_{tag}" / "plots" / "summary.json"
    if not p.exists():
        raise SystemExit(
            f"summary.json missing for tag '{tag}'. "
            f"Run `python -m benchmarks.analyze_results --tag {tag}` first."
        )
    rows = json.loads(p.read_text())
    for r in rows:
        r["tag"] = tag
    return rows


def grouped_bars(df: pd.DataFrame, metric: str, ylabel: str, title: str, out: Path) -> None:
    """Dyo bars ana experiment (Batch + Stream) gia to sygkekrimeno metric."""
    pivoted = df.pivot(index="tag", columns="label", values=metric)
    # Diatiroume tin seira ton stilon: Batch prota, meta Stream
    desired = [c for c in ["Batch (Methodos A)", "Stream (Methodos B)"] if c in pivoted.columns]
    pivoted = pivoted[desired]

    fig, ax = plt.subplots(figsize=(max(8, 2 + 2.5 * len(pivoted)), 6))
    x = np.arange(len(pivoted))
    width = 0.38
    colors = {"Batch (Methodos A)": "#d62728", "Stream (Methodos B)": "#1f77b4"}

    for i, col in enumerate(pivoted.columns):
        offset = (i - (len(pivoted.columns) - 1) / 2) * width
        bars = ax.bar(x + offset, pivoted[col].values, width,
                      label=col, color=colors.get(col, None), alpha=0.85)
        for b in bars:
            v = b.get_height()
            if v == v:  # oxi nan
                ax.text(b.get_x() + b.get_width() / 2, v, f"{v:,.0f}",
                        ha="center", va="bottom", fontsize=9)

    ax.set_xticks(x)
    ax.set_xticklabels(pivoted.index)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Saved %s", out)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tags", nargs="+", required=True,
                        help="Experiment tags pou tha sygkrithoun")
    parser.add_argument("--results-dir", type=Path,
                        default=Path(__file__).resolve().parent / "results")
    parser.add_argument("--out-dir", type=Path, default=None)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    rows: list[dict] = []
    for tag in args.tags:
        rows.extend(load_summary(args.results_dir, tag))
    df = pd.DataFrame(rows)

    out = args.out_dir or args.results_dir / ("comparison_" + "_".join(args.tags))
    out.mkdir(parents=True, exist_ok=True)

    df.to_csv(out / "comparison.csv", index=False)
    logger.info("Saved %s", out / "comparison.csv")

    # Print enan compact pinaka
    pretty = df.set_index(["tag", "label"])[
        ["events", "opps", "lat_ms_p50", "lat_ms_p95", "proc_ms_p50",
         "proc_ms_p95", "throughput_ev_s"]
    ].rename(columns={
        "lat_ms_p50": "lat_p50_ms",
        "lat_ms_p95": "lat_p95_ms",
        "proc_ms_p50": "proc_p50_ms",
        "proc_ms_p95": "proc_p95_ms",
        "throughput_ev_s": "throughput",
    })
    print()
    print(pretty.to_string())
    print()

    # Plots
    grouped_bars(df, "lat_ms_p50",
                 "End-to-end latency p50 (ms)",
                 "Median detection latency per experiment",
                 out / "latency_p50_bars.png")
    grouped_bars(df, "lat_ms_p95",
                 "End-to-end latency p95 (ms)",
                 "p95 detection latency per experiment",
                 out / "latency_p95_bars.png")
    grouped_bars(df, "throughput_ev_s",
                 "Throughput (events / sec)",
                 "Throughput per experiment",
                 out / "throughput_bars.png")

    print(f"Comparison plots in: {out}")


if __name__ == "__main__":
    main()
