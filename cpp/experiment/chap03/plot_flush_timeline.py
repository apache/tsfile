#!/usr/bin/env python3
"""
Plot memory timeline for MemConstrainedWriter.
Uses only flush_before/flush_after points to draw clean sawtooth envelope.
"""

import os
import csv
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BASE = os.path.dirname(os.path.abspath(__file__))

plt.rcParams.update({
    "font.size": 11,
    "axes.labelsize": 12,
    "axes.titlesize": 13,
    "legend.fontsize": 9,
    "figure.dpi": 150,
})


def mb(v):
    return int(v) / (1024 * 1024)


def load_envelope(detail_csv, keep_every=1):
    """Extract flush envelope: flush_before (peak) and flush_after (valley) pairs.
    Also keep rotate and final events. Downsample flush pairs by keep_every."""
    rows = list(csv.DictReader(open(detail_csv)))
    pts = []
    flush_idx = 0
    for r in rows:
        evt = r["event"]
        if evt in ("flush_before", "flush_after"):
            flush_idx += 1
            if flush_idx % (keep_every * 2) <= 1:  # keep every Nth pair
                pts.append(r)
        elif evt in ("rotate_before", "rotate_after", "final_close"):
            pts.append(r)
        elif evt == "batch" and len(pts) == 0:
            pts.append(r)  # keep first point
    return pts


def draw_timeline(ax, pts, mem_limit_mb, title=None):
    # M_init: budget reservation used by MemConstrainedWriter
    M_INIT_MB = 900.0 / 1024  # 900 KB

    x = np.array([int(p["rows_written"]) / 1e6 for p in pts])
    m_data = np.array([mb(p["m_data_direct"]) for p in pts])
    m_meta = np.array([mb(p["m_meta_direct"]) for p in pts])

    # Stacked: M_init (base) + M_meta + M_data
    y_init = np.full_like(x, M_INIT_MB)
    y_meta_top = y_init + m_meta
    y_total = y_meta_top + m_data

    ax.fill_between(x, 0, y_init, alpha=0.15, color="#64748b",
                    linewidth=0, label="$M_{init}$")
    ax.fill_between(x, y_init, y_meta_top, alpha=0.30, color="#f59e0b",
                    linewidth=0, label="$M_{meta}$")
    ax.fill_between(x, y_meta_top, y_total, alpha=0.30, color="#2563eb",
                    linewidth=0, label="$M_{data}$")
    ax.plot(x, y_total, color="#1e293b", linewidth=0.5, alpha=0.7)

    # M_limit line
    ax.axhline(y=mem_limit_mb, color="#dc2626", linestyle="--", linewidth=2,
               alpha=0.9, label=f"$M_{{limit}}$ = {mem_limit_mb} MB")

    # Rotation markers
    rotate_xs = [int(p["rows_written"]) / 1e6
                 for p in pts if p["event"] == "rotate_before"]
    for i, rx in enumerate(rotate_xs):
        ax.axvline(x=rx, color="#7c3aed", linestyle="-", linewidth=1.5, alpha=0.4)
        y_pos = mem_limit_mb * (0.92 if i % 2 == 0 else 0.84)
        ax.text(rx + 0.3, y_pos, f"R{i+1}", fontsize=9, color="#7c3aed",
                fontweight="bold")

    ax.set_xlabel("Rows Written (millions)")
    ax.set_ylabel("Memory (MB)")
    ax.set_ylim(0, mem_limit_mb * 1.15)
    ax.set_xlim(0, max(x) * 1.02)
    ax.legend(loc="center right", framealpha=0.9)
    ax.grid(True, alpha=0.2)
    if title:
        ax.set_title(title)


def plot_single(detail_csv, mem_limit_mb, out_path, keep_every=3):
    pts = load_envelope(detail_csv, keep_every=keep_every)
    fig, ax = plt.subplots(figsize=(10, 4))
    draw_timeline(ax, pts, mem_limit_mb,
                  title=f"Memory-Constrained Write Timeline "
                        f"($M_{{limit}}$ = {mem_limit_mb} MB)")
    fig.tight_layout()
    fig.savefig(out_path)
    print(f"  -> {out_path}  ({len(pts)} points)")
    plt.close(fig)


def plot_comparison(out_path):
    configs = [
        ("write_budget_detail_2MB.csv", 2, "2 MB budget (file rotation)", 3),
        ("write_budget_detail_8MB.csv", 8, "8 MB budget (single file)", 2),
    ]
    fig, axes = plt.subplots(1, 2, figsize=(13, 4))

    for ax, (csv_file, limit, title, ke) in zip(axes, configs):
        path = os.path.join(BASE, "E3_4_write_budget", csv_file)
        pts = load_envelope(path, keep_every=ke)
        draw_timeline(ax, pts, limit, title=title)

    fig.suptitle("Memory-Safe Write: Two-Level Control in Action",
                 fontsize=14, y=1.01)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    print(f"  -> {out_path}")
    plt.close(fig)


if __name__ == "__main__":
    print("=== Flush Timeline Plots ===")
    plot_single(
        os.path.join(BASE, "E3_4_write_budget", "write_budget_detail_2MB.csv"),
        2, os.path.join(BASE, "F3_flush_timeline_2MB.pdf"), keep_every=3)
    plot_comparison(os.path.join(BASE, "F3_flush_timeline_compare.pdf"))
    print("Done.")
