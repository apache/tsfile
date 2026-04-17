"""
Plot write/read speedup results for E4-1/E4-2.

Reads:
  write_results_ON.csv   (C4: ENABLE_THREADS=ON, threads=1,2,4,8)
  write_results_OFF.csv  (C1: ENABLE_THREADS=OFF, serial baseline)
  read_results_ON.csv
  read_results_OFF.csv

Produces:
  fig_write_throughput.pdf  — absolute throughput vs threads
  fig_write_speedup.pdf     — speedup = T(t=1)/T(t=p), ON build only
  fig_read_throughput.pdf
  fig_read_speedup.pdf
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

# ── paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = SCRIPT_DIR

COMPRESSIONS = ["SNAPPY", "LZ4"]
COLS_LIST = [4, 8, 16]
COLORS = {4: "#1f77b4", 8: "#ff7f0e", 16: "#2ca02c"}
MARKERS = {4: "o", 8: "s", 16: "^"}

# ── load ─────────────────────────────────────────────────────────────────────

def load(op: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (df_off, df_on) for 'write' or 'read'."""
    off = pd.read_csv(os.path.join(DATA_DIR, f"{op}_results_OFF.csv"))
    on  = pd.read_csv(os.path.join(DATA_DIR, f"{op}_results_ON.csv"))
    return off, on


# ── helpers ──────────────────────────────────────────────────────────────────

def compute_speedup(df_on: pd.DataFrame) -> pd.DataFrame:
    """
    For each (cols, compression) group, normalise throughput to the t=1 row.
    Returns df_on with an extra column 'speedup'.
    """
    rows = []
    for (cols, comp), grp in df_on.groupby(["cols", "compression"]):
        base = grp.loc[grp["threads"] == 1, "throughput_mrows_s"].values
        if len(base) == 0:
            continue
        base_tp = base[0]
        grp = grp.copy()
        grp["speedup"] = grp["throughput_mrows_s"] / base_tp
        rows.append(grp)
    return pd.concat(rows, ignore_index=True)


# ── plot helpers ─────────────────────────────────────────────────────────────

def _twin_legend(axes):
    """Collect legend handles from both subplots into the first one."""
    handles, labels = [], []
    for ax in axes:
        h, l = ax.get_legend_handles_labels()
        for hi, li in zip(h, l):
            if li not in labels:
                handles.append(hi)
                labels.append(li)
    axes[0].legend(handles, labels, fontsize=8, loc="upper left")


def plot_throughput(df_off: pd.DataFrame, df_on: pd.DataFrame, op: str):
    """Absolute throughput vs threads for ON build; dashed line for OFF baseline."""
    fig, axes = plt.subplots(1, 2, figsize=(10, 4), sharey=False)
    fig.suptitle(f"{'写入' if op=='write' else '读取'} 吞吐量 vs 线程数", fontsize=12)

    for ax, comp in zip(axes, COMPRESSIONS):
        # Serial baseline (OFF, threads=1)
        for n_cols in COLS_LIST:
            row_off = df_off[(df_off["cols"] == n_cols) & (df_off["compression"] == comp)]
            if not row_off.empty:
                tp_off = row_off["throughput_mrows_s"].values[0]
                ax.axhline(tp_off, color=COLORS[n_cols], linestyle="--",
                           linewidth=0.9, alpha=0.55)

        # Parallel (ON, all thread counts)
        for n_cols in COLS_LIST:
            sub = df_on[(df_on["cols"] == n_cols) & (df_on["compression"] == comp)]
            sub = sub.sort_values("threads")
            ax.plot(sub["threads"], sub["throughput_mrows_s"],
                    color=COLORS[n_cols], marker=MARKERS[n_cols],
                    linewidth=1.5, markersize=5, label=f"cols={n_cols}")

        ax.set_title(comp, fontsize=10)
        ax.set_xlabel("线程数")
        ax.set_ylabel("吞吐量（百万行/秒）")
        ax.set_xticks(sorted(df_on["threads"].unique()))
        ax.grid(axis="y", linestyle=":", alpha=0.5)
        ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())

    _twin_legend(axes)
    fig.tight_layout()
    out = os.path.join(DATA_DIR, f"fig_{op}_throughput.pdf")
    fig.savefig(out, bbox_inches="tight")
    print(f"Saved: {out}")
    plt.close(fig)


def plot_speedup(df_on: pd.DataFrame, op: str):
    """Speedup = tp(t) / tp(1) within ON build, vs threads."""
    df_sp = compute_speedup(df_on)
    thread_counts = sorted(df_on["threads"].unique())

    fig, axes = plt.subplots(1, 2, figsize=(10, 4), sharey=True)
    fig.suptitle(f"{'写入' if op=='write' else '读取'} 加速比（相对于串行，t=1）", fontsize=12)

    for ax, comp in zip(axes, COMPRESSIONS):
        # Ideal speedup reference
        ax.plot(thread_counts, thread_counts, color="gray", linestyle=":",
                linewidth=1, label="Ideal", zorder=0)

        for n_cols in COLS_LIST:
            sub = df_sp[(df_sp["cols"] == n_cols) & (df_sp["compression"] == comp)]
            sub = sub.sort_values("threads")
            ax.plot(sub["threads"], sub["speedup"],
                    color=COLORS[n_cols], marker=MARKERS[n_cols],
                    linewidth=1.5, markersize=5, label=f"cols={n_cols}")

        ax.set_title(comp, fontsize=10)
        ax.set_xlabel("线程数")
        ax.set_ylabel("加速比")
        ax.set_xticks(thread_counts)
        ax.set_ylim(bottom=0.9)
        ax.axhline(1.0, color="black", linewidth=0.5, linestyle="--")
        ax.grid(axis="y", linestyle=":", alpha=0.5)

    _twin_legend(axes)
    fig.tight_layout()
    out = os.path.join(DATA_DIR, f"fig_{op}_speedup.pdf")
    fig.savefig(out, bbox_inches="tight")
    print(f"Saved: {out}")
    plt.close(fig)


def plot_bar_compare(df_off: pd.DataFrame, df_on: pd.DataFrame, op: str, threads: int = 8):
    """
    Bar chart: serial (C1, t=1) vs parallel (C4, t=threads) throughput,
    grouped by (cols, compression).
    """
    labels, serial_tp, parallel_tp = [], [], []
    for comp in COMPRESSIONS:
        for n_cols in COLS_LIST:
            row_off = df_off[(df_off["cols"] == n_cols) & (df_off["compression"] == comp)]
            row_on  = df_on[(df_on["cols"] == n_cols) & (df_on["compression"] == comp)
                            & (df_on["threads"] == threads)]
            if row_off.empty or row_on.empty:
                continue
            labels.append(f"{comp}\ncols={n_cols}")
            serial_tp.append(row_off["throughput_mrows_s"].values[0])
            parallel_tp.append(row_on["throughput_mrows_s"].values[0])

    x = np.arange(len(labels))
    width = 0.35
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.bar(x - width / 2, serial_tp,   width, label="C1 serial (t=1)", color="#aec7e8")
    ax.bar(x + width / 2, parallel_tp, width, label=f"C4 parallel (t={threads})", color="#1f77b4")

    for i, (s, p) in enumerate(zip(serial_tp, parallel_tp)):
        sp = p / s if s > 0 else 0
        ax.text(x[i] + width / 2, p + 0.3, f"{sp:.2f}×", ha="center",
                va="bottom", fontsize=7.5)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel("吞吐量（百万行/秒）")
    ax.set_title(f"{'写入' if op=='write' else '读取'}：C1 vs C4（t={threads}）", fontsize=11)
    ax.legend(fontsize=9)
    ax.grid(axis="y", linestyle=":", alpha=0.5)
    fig.tight_layout()
    out = os.path.join(DATA_DIR, f"fig_{op}_compare.pdf")
    fig.savefig(out, bbox_inches="tight")
    print(f"Saved: {out}")
    plt.close(fig)


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    plt.rcParams.update({
        "font.family": "sans-serif",
        "font.sans-serif": ["Songti SC", "Heiti TC", "STHeiti", "PingFang HK",
                             "Hiragino Sans GB", "DejaVu Sans"],
        "axes.unicode_minus": False,
        "font.size": 9,
        "axes.titlesize": 10,
        "figure.dpi": 150,
    })

    for op in ("write", "read"):
        off_path = os.path.join(DATA_DIR, f"{op}_results_OFF.csv")
        on_path  = os.path.join(DATA_DIR, f"{op}_results_ON.csv")
        if not os.path.exists(off_path) or not os.path.exists(on_path):
            print(f"[SKIP] missing CSV for op={op}")
            continue

        df_off, df_on = load(op)
        plot_throughput(df_off, df_on, op)
        plot_speedup(df_on, op)
        plot_bar_compare(df_off, df_on, op, threads=8)

    print("\nDone.")


if __name__ == "__main__":
    main()
