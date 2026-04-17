#!/usr/bin/env python3
"""
Plot TsFile read-path memory model from CSVs produced by read_mem_model.

Each CSV is a time-series with columns:
  n_cols, batch_size, rows_read, phase, wall_us, user_cpu_us, sys_cpu_us,
  <module bytes...>, TOTAL

Plots:
  Fig 1: Peak total memory vs batch_size (one line per N_cols)
  Fig 2: Peak total memory vs N_cols     (one line per batch_size)
  Fig 3: TSBLOCK peak vs batch_size
  Fig 4: DECODER_OBJ peak vs N_cols
  Fig 5: Memory over rows_read for selected configs (2x2 subplots)

Usage:
    python3 plot_read_memory.py [csv_prefix] [output_base]

    csv_prefix:  path prefix, default "read_memory_results"
    output_base: output file base (no extension), default "read_memory_chart"
                 use .pdf suffix for PDF output
"""

import csv
import sys
import glob
import re

COL_COUNTS  = [1, 2, 4, 6]
BATCH_SIZES = [1024, 4096, 16384, 65536]


def load_csv(path):
    """
    Load a time-series CSV.
    Returns (peak, series):
      peak:   dict column -> max int value across all rows
      series: list of row dicts with int values where possible
    """
    with open(path) as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return None, None

    parsed = []
    for row in rows:
        d = {}
        for k, v in row.items():
            if k == "phase":
                d[k] = v
            else:
                try:
                    d[k] = int(v)
                except ValueError:
                    d[k] = v
        parsed.append(d)

    numeric_keys = [k for k in parsed[0] if isinstance(parsed[0][k], int)]
    peak = {k: max(r[k] for r in parsed) for k in numeric_keys}
    return peak, parsed


def main():
    csv_prefix = sys.argv[1] if len(sys.argv) > 1 else "read_memory_results"
    out_base   = sys.argv[2] if len(sys.argv) > 2 else "read_memory_chart"

    ext  = ".pdf"
    base = out_base[:-4] if out_base.endswith((".pdf", ".png")) else out_base

    peaks  = {}   # (n_cols, batch_size) -> peak dict
    series = {}   # (n_cols, batch_size) -> list of row dicts
    pattern = f"{csv_prefix}_cols*_batch*.csv"
    found = sorted(glob.glob(pattern))
    if not found:
        print(f"No files matched: {pattern}")
        sys.exit(1)

    for path in found:
        m = re.search(r"_cols(\d+)_batch(\d+)\.csv$", path)
        if not m:
            continue
        key = (int(m.group(1)), int(m.group(2)))
        pk, sr = load_csv(path)
        if pk is not None:
            peaks[key]  = pk
            series[key] = sr

    if not peaks:
        print("No valid data loaded.")
        sys.exit(1)

    print(f"Loaded {len(peaks)} experiments from: {pattern}")

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as ticker
    except ImportError:
        print("matplotlib not found.  pip3 install matplotlib")
        sys.exit(1)

    plt.rcParams.update({
        "font.family": "sans-serif",
        "font.sans-serif": ["Songti SC", "Heiti TC", "STHeiti", "PingFang HK",
                             "Hiragino Sans GB", "DejaVu Sans"],
        "axes.unicode_minus": False,
    })

    colors = ["#4e79a7", "#f28e2b", "#e15759", "#76b7b2",
              "#59a14f", "#edc948", "#b07aa1", "#ff9da7"]

    def mb(v):
        return v / 1024 / 1024

    def save(fig, name):
        path = f"{base}_{name}{ext}"
        fig.savefig(path, dpi=150)
        plt.close(fig)
        print(f"  saved: {path}")

    # --- Figure 1: Peak total memory vs batch_size ---
    fig, ax = plt.subplots(figsize=(10, 6))
    for ci, nc in enumerate(COL_COUNTS):
        xs = [bs for bs in BATCH_SIZES if (nc, bs) in peaks]
        ys = [mb(peaks[(nc, bs)]["TOTAL"]) for bs in xs]
        if xs:
            ax.plot(xs, ys, marker="o", color=colors[ci], label=f"N_cols={nc}",
                    linewidth=2, markersize=8)
    ax.set_xscale("log", base=2)
    ax.set_xlabel("批大小（行）", fontsize=12)
    ax.set_ylabel("峰值内存（MB）", fontsize=12)
    ax.set_title("峰值总内存 vs 批大小", fontsize=14)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: str(int(x))))
    plt.tight_layout()
    save(fig, "total_vs_batch")

    # --- Figure 2: Peak total memory vs N_cols ---
    fig, ax = plt.subplots(figsize=(10, 6))
    for bi, bs in enumerate(BATCH_SIZES):
        xs = [nc for nc in COL_COUNTS if (nc, bs) in peaks]
        ys = [mb(peaks[(nc, bs)]["TOTAL"]) for nc in xs]
        if xs:
            ax.plot(xs, ys, marker="s", color=colors[bi], label=f"batch={bs}",
                    linewidth=2, markersize=8)
    ax.set_xlabel("列数", fontsize=12)
    ax.set_ylabel("峰值内存（MB）", fontsize=12)
    ax.set_title("峰值总内存 vs 列数", fontsize=14)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    save(fig, "total_vs_ncols")

    # --- Figure 3: TSBLOCK peak vs batch_size ---
    fig, ax = plt.subplots(figsize=(10, 6))
    for ci, nc in enumerate(COL_COUNTS):
        xs = [bs for bs in BATCH_SIZES if (nc, bs) in peaks]
        ys = [mb(peaks[(nc, bs)].get("TSBLOCK", 0)) for bs in xs]
        if xs:
            ax.plot(xs, ys, marker="o", color=colors[ci], label=f"N_cols={nc}",
                    linewidth=2, markersize=8)
    ax.set_xscale("log", base=2)
    ax.set_xlabel("批大小（行）", fontsize=12)
    ax.set_ylabel("TSBLOCK 内存（MB）", fontsize=12)
    ax.set_title("TSBLOCK 内存 vs 批大小", fontsize=14)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: str(int(x))))
    plt.tight_layout()
    save(fig, "tsblock_vs_batch")

    # --- Figure 4: DECODER_OBJ peak vs N_cols ---
    fig, ax = plt.subplots(figsize=(10, 6))
    for bi, bs in enumerate(BATCH_SIZES):
        xs = [nc for nc in COL_COUNTS if (nc, bs) in peaks]
        ys = [mb(peaks[(nc, bs)].get("DECODER_OBJ", 0)) for nc in xs]
        if xs:
            ax.plot(xs, ys, marker="s", color=colors[bi], label=f"batch={bs}",
                    linewidth=2, markersize=8)
    ax.set_xlabel("列数", fontsize=12)
    ax.set_ylabel("DECODER_OBJ 内存（MB）", fontsize=12)
    ax.set_title("DECODER_OBJ 内存 vs 列数", fontsize=14)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    save(fig, "decoder_vs_ncols")

    # --- Figure 5: Memory over rows_read (2x2, one subplot per N_cols) ---
    fig, axes = plt.subplots(2, 2, figsize=(14, 9), sharey=False)
    axes = axes.flatten()
    for ci, nc in enumerate(COL_COUNTS):
        ax = axes[ci]
        for bi, bs in enumerate(BATCH_SIZES):
            if (nc, bs) not in series:
                continue
            sr = series[(nc, bs)]
            xs = [r["rows_read"] / 1e6 for r in sr]
            ys = [mb(r["TOTAL"]) for r in sr]
            ax.plot(xs, ys, color=colors[bi], label=f"batch={bs}", linewidth=1.5)
        ax.set_title(f"N_cols={nc}", fontsize=11)
        ax.set_xlabel("已读行数（百万）", fontsize=10)
        ax.set_ylabel("总内存（MB）", fontsize=10)
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)
    fig.suptitle("读取过程中的总内存变化", fontsize=14)
    plt.tight_layout()
    save(fig, "timeseries")

    # --- Summary table ---
    print(f"\n{'N_cols':>6} {'batch':>8} {'peak_MB':>9} {'TSBLOCK_MB':>11} {'DECODER_MB':>11} {'CHUNK_RD_MB':>12}")
    print("-" * 60)
    for nc in COL_COUNTS:
        for bs in BATCH_SIZES:
            if (nc, bs) not in peaks:
                continue
            pk = peaks[(nc, bs)]
            print(f"{nc:>6} {bs:>8}"
                  f" {mb(pk['TOTAL']):>9.2f}"
                  f" {mb(pk.get('TSBLOCK', 0)):>11.2f}"
                  f" {mb(pk.get('DECODER_OBJ', 0)):>11.2f}"
                  f" {mb(pk.get('CHUNK_READER', 0)):>12.2f}")


if __name__ == "__main__":
    main()
