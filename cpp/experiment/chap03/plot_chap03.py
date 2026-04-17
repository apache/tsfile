#!/usr/bin/env python3
"""
Chapter 3 experiment plots.

Generates all figures for the memory-model validation experiments:
  Fig 1: EOQ U-shape curve  (peak memory vs F/F_opt)
  Fig 2: Write budget       (throughput & flush count vs memory budget)
  Fig 3: Write precision     (formula vs direct measurement, PLAIN+UNCOMPRESSED)
  Fig 4: Read precision      (formula vs ModStat peak, different cols/batch)

Usage:
    python3 plot_chap03.py
"""

import os
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.sans-serif": ["Songti SC", "Heiti TC", "STHeiti", "PingFang HK",
                         "Hiragino Sans GB", "DejaVu Sans"],
    "axes.unicode_minus": False,
    "font.size": 11,
    "axes.labelsize": 12,
    "axes.titlesize": 13,
    "legend.fontsize": 10,
    "figure.dpi": 150,
})

BASE = os.path.dirname(os.path.abspath(__file__))


def mb(x):
    return x / (1024 * 1024)


# ============================================================
# Fig 1: EOQ U-shape curve
# ============================================================
def plot_eoq():
    import csv
    path = os.path.join(BASE, "E3_4_write_budget", "eoq_validate.csv")
    rows = list(csv.DictReader(open(path)))

    F = np.array([int(r["F"]) for r in rows])
    F_opt = int(rows[0]["F_opt"])
    M_min = int(rows[0]["M_min_var"])
    ratio = F / F_opt

    peak = np.array([int(r["peak_m_total"]) for r in rows])
    peak_data = np.array([int(r["peak_m_data"]) for r in rows])
    peak_meta = np.array([int(r["peak_m_meta"]) for r in rows])
    formula = np.array([int(r["formula_m_total"]) for r in rows])

    fig, ax = plt.subplots(figsize=(8, 4.5))

    # Measured
    ax.plot(ratio, mb(peak), "o-", color="#2563eb", linewidth=2,
            markersize=6, label="实测 $M_{total}$（峰值）", zorder=3)
    # Formula
    ax.plot(ratio, mb(formula), "s--", color="#dc2626", linewidth=1.5,
            markersize=5, label="公式 $M_{init}+sF+Kb$", alpha=0.8)
    # M_data and M_meta components
    ax.fill_between(ratio, 0, mb(peak_data), alpha=0.15, color="#2563eb",
                    label="$M_{data}$ 分量")
    ax.fill_between(ratio, mb(peak_data), mb(peak_data + peak_meta),
                    alpha=0.15, color="#f59e0b", label="$M_{meta}$ 分量")

    # Optimal point
    opt_idx = np.argmin(peak)
    ax.axvline(x=ratio[opt_idx], color="gray", linestyle=":", alpha=0.6)
    ax.annotate(f"min @ F/F_opt={ratio[opt_idx]:.1f}\n"
                f"peak={mb(peak[opt_idx]):.1f} MB",
                xy=(ratio[opt_idx], mb(peak[opt_idx])),
                xytext=(ratio[opt_idx] + 1.5, mb(peak[opt_idx]) + 2),
                arrowprops=dict(arrowstyle="->", color="gray"),
                fontsize=9, color="gray")

    ax.set_xlabel("$F / F_{opt}$")
    ax.set_ylabel("峰值内存（MB）")
    ax.set_title("最优批次策略验证")
    ax.set_xscale("log", base=2)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:.2f}"))
    ax.legend(loc="upper right", framealpha=0.9)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    out = os.path.join(BASE, "F3_eoq_ushape.pdf")
    fig.savefig(out)
    print(f"  -> {out}")
    plt.close(fig)


# ============================================================
# Fig 2: Write budget (throughput & flush count)
# ============================================================
def plot_write_budget():
    import csv
    path = os.path.join(BASE, "E3_4_write_budget", "write_budget.csv")
    rows = list(csv.DictReader(open(path)))

    budgets = [int(r["mem_limit_mb"]) for r in rows]
    throughput = [float(r["throughput_mrows_s"]) for r in rows]
    flushes = [int(r["flush_count"]) for r in rows]

    fig, ax1 = plt.subplots(figsize=(7, 4))
    color1 = "#2563eb"
    color2 = "#dc2626"

    files = [int(r["file_count"]) for r in rows]

    bars = ax1.bar(range(len(budgets)), throughput, color=color1, alpha=0.7,
                   label="吞吐量")
    # Annotate file count on bars where rotation happened
    for i, fc in enumerate(files):
        if fc > 1:
            ax1.text(i, throughput[i] + 0.05, f"{fc} files",
                     ha="center", fontsize=9, color="#7c3aed", fontweight="bold")
    ax1.set_xlabel("内存预算（MB）")
    ax1.set_ylabel("吞吐量（百万行/秒）", color=color1)
    ax1.set_xticks(range(len(budgets)))
    ax1.set_xticklabels([str(b) for b in budgets])
    ax1.tick_params(axis="y", labelcolor=color1)
    ax1.set_ylim(0, max(throughput) * 1.4)

    ax2 = ax1.twinx()
    ax2.plot(range(len(budgets)), flushes, "D-", color=color2, linewidth=2,
             markersize=7, label="刷写次数")
    ax2.set_ylabel("刷写次数", color=color2)
    ax2.tick_params(axis="y", labelcolor=color2)

    # Combine legends
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right")

    ax1.set_title("两级内存控制：预算合规性\n"
                  "（5000万行，10设备，PLAIN+UNCOMPRESSED）")
    ax1.grid(True, alpha=0.3, axis="y")
    fig.tight_layout()
    out = os.path.join(BASE, "F3_write_budget.pdf")
    fig.savefig(out)
    print(f"  -> {out}")
    plt.close(fig)


# ============================================================
# Fig 3: Write precision (formula vs estimate)
# ============================================================
def plot_write_precision():
    import csv
    path = os.path.join(BASE, "E3_2_write_precision", "write_precision.csv")
    rows = list(csv.DictReader(open(path)))

    batch = [int(r["batch_size"]) for r in rows]
    direct = [int(r["m_data_direct"]) for r in rows]
    formula = [int(r["m_data_formula"]) for r in rows]

    fig, ax = plt.subplots(figsize=(7, 4))
    x = np.arange(len(batch))
    w = 0.35

    ax.bar(x - w/2, [mb(v) for v in formula], w, label="公式（$s \\times F$）",
           color="#dc2626", alpha=0.7)
    ax.bar(x + w/2, [mb(v) for v in direct], w,
           label="直接估算（estimate\\_max\\_series）", color="#2563eb", alpha=0.7)

    ax.set_xlabel("批大小（行）")
    ax.set_ylabel("$M_{data}$（MB）")
    ax.set_xticks(x)
    ax.set_xticklabels([f"{b//1000}K" for b in batch])
    ax.legend()
    ax.set_title("写入内存：公式 vs 直接估算\n"
                 "（SNAPPY+TS_2DIFF，公式设计上偏高估）")
    ax.grid(True, alpha=0.3, axis="y")
    fig.tight_layout()
    out = os.path.join(BASE, "F3_write_precision.pdf")
    fig.savefig(out)
    print(f"  -> {out}")
    plt.close(fig)


# ============================================================
# Fig 4: Read precision heatmap (error % for cols x batch)
# ============================================================
def plot_read_precision():
    import csv
    path = os.path.join(BASE, "E3_2_write_precision", "read_precision.csv")
    rows = list(csv.DictReader(open(path)))

    cols_set = sorted(set(int(r["n_cols"]) for r in rows))
    batch_set = sorted(set(int(r["batch_size"]) for r in rows))

    # Build error matrix
    err = np.zeros((len(cols_set), len(batch_set)))
    peak = np.zeros_like(err)
    for r in rows:
        ci = cols_set.index(int(r["n_cols"]))
        bi = batch_set.index(int(r["batch_size"]))
        err[ci, bi] = float(r["error_pct"])
        peak[ci, bi] = int(r["peak_actual"])

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5))

    # Left: peak memory line chart
    for ci, nc in enumerate(cols_set):
        ax1.plot(batch_set, mb(peak[ci]), "o-", label=f"cols={nc}", markersize=5)
    ax1.set_xlabel("批大小")
    ax1.set_ylabel("峰值内存（MB）")
    ax1.set_xscale("log", base=2)
    ax1.xaxis.set_major_formatter(FuncFormatter(
        lambda x, _: f"{int(x)//1024}K" if x >= 1024 else str(int(x))))
    ax1.legend(title="N_cols")
    ax1.set_title("读取峰值内存")
    ax1.grid(True, alpha=0.3)

    # Right: error heatmap
    im = ax2.imshow(err, cmap="RdYlGn_r", aspect="auto",
                    vmin=0, vmax=70)
    ax2.set_xticks(range(len(batch_set)))
    ax2.set_xticklabels([f"{b//1024}K" if b >= 1024 else str(b)
                         for b in batch_set])
    ax2.set_yticks(range(len(cols_set)))
    ax2.set_yticklabels([str(c) for c in cols_set])
    ax2.set_xlabel("批大小")
    ax2.set_ylabel("列数")
    ax2.set_title("公式误差 %")
    for ci in range(len(cols_set)):
        for bi in range(len(batch_set)):
            ax2.text(bi, ci, f"{err[ci, bi]:.0f}%", ha="center", va="center",
                     fontsize=8, color="black" if err[ci, bi] < 40 else "white")
    plt.colorbar(im, ax=ax2, label="Error %")

    fig.tight_layout()
    out = os.path.join(BASE, "F3_read_precision.pdf")
    fig.savefig(out)
    print(f"  -> {out}")
    plt.close(fig)


# ============================================================
# Main
# ============================================================
if __name__ == "__main__":
    print("=== Chapter 3 Plots ===")
    plot_eoq()
    # plot_write_budget()
    # plot_write_precision()
    # plot_read_precision()
    print("Done.")
