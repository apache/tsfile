#!/usr/bin/env python3
"""
Plot read benchmark results from CSV files.

Usage:
    python3 plot_read.py [results_dir]
    Default: ./results/

Generates:
    - full_scan_comparison.png      : TsFile vs Parquet full scan
    - tag_filter_comparison.png     : Tag filter pruning comparison
    - time_selectivity.png          : Throughput at different selectivities
    - batch_vs_row.png              : Batch vs row-by-row read
    - simd_thread_impact.png        : SIMD/thread configuration impact
"""

import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd

RESULTS_DIR = sys.argv[1] if len(sys.argv) > 1 else "./results"


def load_merged():
    path = os.path.join(RESULTS_DIR, "merged.csv")
    if not os.path.exists(path):
        print(f"merged.csv not found at {path}")
        sys.exit(1)
    return pd.read_csv(path)


def load_single(name="full"):
    path = os.path.join(RESULTS_DIR, f"{name}.csv")
    if not os.path.exists(path):
        return None
    return pd.read_csv(path)


def fmt_throughput(ax):
    ax.yaxis.set_major_formatter(
        ticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M" if x >= 1e6
                             else f"{x/1e3:.0f}K"))
    ax.set_ylabel("Throughput (rows/s)")


def save(fig, name):
    out = os.path.join(RESULTS_DIR, name)
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"  saved {out}")
    plt.close(fig)


# ─── Plot 1: Full scan comparison (single config) ────────────────────────────

def plot_full_scan():
    df = load_single()
    if df is None:
        return
    sub = df[df["experiment"] == "full_scan"]
    if sub.empty:
        return

    fig, ax = plt.subplots(figsize=(6, 4))
    colors = {"tsfile_batch": "#2196F3", "parquet": "#FF9800"}
    bars = ax.bar(sub["engine"], sub["rows_per_sec"],
                  color=[colors.get(e, "#999") for e in sub["engine"]])
    for bar, val in zip(bars, sub["rows_per_sec"]):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                f"{val/1e6:.2f}M", ha="center", va="bottom", fontsize=10)
    ax.set_title("Full Scan Throughput")
    fmt_throughput(ax)
    save(fig, "full_scan_comparison.png")


# ─── Plot 2: Tag filter comparison ───────────────────────────────────────────

def plot_tag_filter():
    df = load_single()
    if df is None:
        return
    sub = df[df["experiment"] == "tag_filter"]
    if sub.empty:
        return

    fig, ax = plt.subplots(figsize=(6, 4))
    colors = {"tsfile_batch": "#2196F3", "parquet": "#FF9800"}
    bars = ax.bar(sub["engine"], sub["rows_per_sec"],
                  color=[colors.get(e, "#999") for e in sub["engine"]])
    for bar, val in zip(bars, sub["rows_per_sec"]):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                f"{val/1e6:.2f}M", ha="center", va="bottom", fontsize=10)
    ax.set_title("Tag Filter (Single Device) Throughput")
    fmt_throughput(ax)
    save(fig, "tag_filter_comparison.png")


# ─── Plot 3: Time selectivity ────────────────────────────────────────────────

def plot_time_selectivity():
    df = load_single()
    if df is None:
        return
    sub = df[df["experiment"] == "time_sel"]
    if sub.empty:
        return

    fig, ax = plt.subplots(figsize=(8, 5))
    for engine in sub["engine"].unique():
        eng_df = sub[sub["engine"] == engine]
        color = "#2196F3" if "tsfile" in engine else "#FF9800"
        ax.plot(eng_df["params"], eng_df["rows_per_sec"],
                marker="o", label=engine, color=color)

    ax.set_title("Throughput vs Time Filter Selectivity")
    ax.set_xlabel("Selectivity")
    fmt_throughput(ax)
    ax.legend()
    save(fig, "time_selectivity.png")


# ─── Plot 4: Batch vs Row ────────────────────────────────────────────────────

def plot_batch_vs_row():
    df = load_single()
    if df is None:
        return
    sub = df[df["experiment"] == "batch_vs_row"]
    if sub.empty:
        return

    fig, ax = plt.subplots(figsize=(6, 4))
    colors = {"tsfile_batch": "#2196F3", "tsfile_row": "#90CAF9"}
    bars = ax.bar(sub["engine"], sub["rows_per_sec"],
                  color=[colors.get(e, "#999") for e in sub["engine"]])
    for bar, val in zip(bars, sub["rows_per_sec"]):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                f"{val/1e6:.2f}M", ha="center", va="bottom", fontsize=10)
    ax.set_title("Batch vs Row-by-Row Read")
    fmt_throughput(ax)
    save(fig, "batch_vs_row.png")


# ─── Plot 5: SIMD / Thread impact (from merged.csv) ──────────────────────────

def plot_simd_thread():
    try:
        df = load_merged()
    except SystemExit:
        return
    sub = df[df["experiment"] == "full_scan"]
    sub = sub[sub["engine"] == "tsfile_batch"]
    if sub.empty:
        return

    fig, ax = plt.subplots(figsize=(8, 5))
    configs = sub["config"].tolist()
    throughputs = sub["rows_per_sec"].tolist()
    colors = {"baseline": "#BDBDBD", "simd": "#66BB6A",
              "threads": "#42A5F5", "full": "#EF5350"}
    bars = ax.bar(configs, throughputs,
                  color=[colors.get(c, "#999") for c in configs])
    for bar, val in zip(bars, throughputs):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                f"{val/1e6:.2f}M", ha="center", va="bottom", fontsize=10)
    ax.set_title("Full Scan: Impact of SIMD & Threading")
    fmt_throughput(ax)
    save(fig, "simd_thread_impact.png")


if __name__ == "__main__":
    print(f"Reading results from: {RESULTS_DIR}")
    plot_full_scan()
    plot_tag_filter()
    plot_time_selectivity()
    plot_batch_vs_row()
    plot_simd_thread()
    print("Done.")
