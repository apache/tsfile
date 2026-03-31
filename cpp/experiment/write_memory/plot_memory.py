#!/usr/bin/env python3
"""
Plot TsFile write-path memory usage from CSV produced by write_memory.

Usage:
    python3 plot_memory.py [csv_path] [output_png]
"""

import csv
import sys
import os


def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "write_memory_stats.csv"
    out_png = sys.argv[2] if len(sys.argv) > 2 else "write_memory_chart.png"

    # --- Read CSV ---
    rows_written = []
    phases = []
    columns = {}  # name -> list of values

    with open(csv_path) as f:
        reader = csv.DictReader(f)
        col_names = [c for c in reader.fieldnames if c not in ("rows_written", "phase")]
        for c in col_names:
            columns[c] = []
        for row in reader:
            rows_written.append(int(row["rows_written"]))
            phases.append(row["phase"])
            for c in col_names:
                columns[c].append(int(row[c]))

    n = len(rows_written)
    if n == 0:
        print("No data in CSV")
        return

    # --- Extract CPU timing columns (if present) ---
    has_cpu = "wall_us" in columns and "user_cpu_us" in columns
    wall_us = None
    user_cpu_us = None
    sys_cpu_us = None
    if has_cpu:
        # These are raw int64 values in microseconds, keep as-is before MB conversion
        wall_us = [v for v in columns.pop("wall_us")]
        user_cpu_us = [v for v in columns.pop("user_cpu_us")]
        sys_cpu_us = [v for v in columns.pop("sys_cpu_us")]
        col_names = [c for c in col_names if c not in ("wall_us", "user_cpu_us", "sys_cpu_us")]

    # Convert memory columns to MB
    for c in col_names:
        columns[c] = [v / (1024 * 1024) for v in columns[c]]

    # --- Identify flush points (where TOTAL drops significantly) ---
    total = columns["TOTAL"]
    flush_indices = []
    # (before_idx, after_idx) pairs for annotation
    flush_pairs = []
    for i in range(1, n):
        if phases[i] == "after_flush":
            flush_indices.append(i)
            flush_pairs.append((i - 1, i))
        elif total[i] < total[i - 1] * 0.5 and total[i - 1] > 1.0:
            flush_indices.append(i)
            flush_pairs.append((i - 1, i))

    # --- Select top modules by peak value (exclude TOTAL) ---
    mod_names = [c for c in col_names if c != "TOTAL"]
    peak_vals = {c: max(columns[c]) for c in mod_names}
    top_mods = sorted(mod_names, key=lambda c: peak_vals[c], reverse=True)[:6]

    # --- X axis: rows in millions ---
    x = [r / 1e6 for r in rows_written]

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as ticker
    except ImportError:
        print("matplotlib not found. Install: pip3 install matplotlib")
        sys.exit(1)

    if has_cpu:
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(14, 14), sharex=True,
                                             gridspec_kw={"height_ratios": [2, 1, 1.2]})
    else:
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True,
                                        gridspec_kw={"height_ratios": [2, 1]})
        ax3 = None

    # --- Top: Total memory + stacked area of top modules ---
    ax1.plot(x, total, color="black", linewidth=2, label="TOTAL", zorder=10)

    colors = ["#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f", "#edc948"]
    for i, mod in enumerate(top_mods):
        ax1.fill_between(x, 0, columns[mod], alpha=0.3, color=colors[i % len(colors)])
        ax1.plot(x, columns[mod], linewidth=1, color=colors[i % len(colors)],
                 label=mod, alpha=0.8)

    # Mark flush points with before/after annotations
    y_max = max(total) * 1.15
    for fi_num, (bi, ai) in enumerate(flush_pairs):
        flush_x = x[ai]
        ax1.axvline(x=flush_x, color="red", linestyle="--", alpha=0.5, linewidth=0.8)

        before_mb = total[bi]
        after_mb = total[ai]
        freed_mb = before_mb - after_mb

        # Draw arrow from before to after level
        ax1.annotate("",
                     xy=(flush_x, after_mb), xytext=(flush_x, before_mb),
                     arrowprops=dict(arrowstyle="->", color="red", lw=1.5))

        # Label: before → after (freed)
        label = f"{before_mb:.1f}→{after_mb:.1f} MB\n(−{freed_mb:.1f})"
        # Alternate label position left/right to avoid overlap
        x_off = 12 if fi_num % 2 == 0 else -12
        ha = "left" if fi_num % 2 == 0 else "right"
        ax1.annotate(label,
                     xy=(flush_x, (before_mb + after_mb) / 2),
                     xytext=(x_off, 0), textcoords="offset points",
                     fontsize=7, color="red", fontweight="bold",
                     ha=ha, va="center",
                     bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="red",
                               alpha=0.85))

    ax1.set_ylabel("Memory (MB)")
    ax1.set_ylim(0, y_max)
    ax1.set_title("TsFile Write Path — Memory Usage Over Time")
    ax1.legend(loc="upper left", fontsize=8, ncol=2)
    ax1.grid(True, alpha=0.3)
    ax1.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.1f"))

    # --- Bottom: Per-module breakdown (stacked area) ---
    # Build stacked data for top modules
    bottoms = [0.0] * n
    for i, mod in enumerate(top_mods):
        vals = columns[mod]
        ax2.fill_between(x, bottoms, [b + v for b, v in zip(bottoms, vals)],
                         alpha=0.6, color=colors[i % len(colors)], label=mod)
        bottoms = [b + v for b, v in zip(bottoms, vals)]

    for fi in flush_indices:
        ax2.axvline(x=x[fi], color="red", linestyle="--", alpha=0.5, linewidth=0.8)

    ax2.set_xlabel("Rows Written (millions)")
    ax2.set_ylabel("Memory (MB)")
    ax2.set_title("Per-Module Memory Breakdown (stacked)")
    ax2.legend(loc="upper left", fontsize=8, ncol=2)
    ax2.grid(True, alpha=0.3)

    # --- Third panel: CPU utilization & throughput ---
    if ax3 is not None and has_cpu:
        # Compute per-interval deltas
        cpu_pct = []    # CPU utilization %
        throughput = []  # rows/s (millions)
        cpu_x = []
        for i in range(1, n):
            dwall = wall_us[i] - wall_us[i - 1]
            if dwall <= 0:
                continue
            duser = user_cpu_us[i] - user_cpu_us[i - 1]
            dsys = sys_cpu_us[i] - sys_cpu_us[i - 1]
            dcpu = duser + dsys
            cpu_pct.append(100.0 * dcpu / dwall)

            drows = rows_written[i] - rows_written[i - 1]
            throughput.append(drows / (dwall / 1e6))  # rows/s

            cpu_x.append((x[i] + x[i - 1]) / 2)  # midpoint

        # CPU % on left y-axis
        ax3.fill_between(cpu_x, 0, cpu_pct, alpha=0.3, color="#4e79a7")
        ax3.plot(cpu_x, cpu_pct, color="#4e79a7", linewidth=1, label="CPU %")
        ax3.set_ylabel("CPU Utilization (%)", color="#4e79a7")
        ax3.set_ylim(0, max(cpu_pct) * 1.2 if cpu_pct else 100)
        ax3.tick_params(axis="y", labelcolor="#4e79a7")

        # Throughput on right y-axis
        ax3r = ax3.twinx()
        tp_millions = [t / 1e6 for t in throughput]
        ax3r.plot(cpu_x, tp_millions, color="#e15759", linewidth=1.2,
                  label="Throughput", alpha=0.8)
        ax3r.set_ylabel("Throughput (M rows/s)", color="#e15759")
        ax3r.tick_params(axis="y", labelcolor="#e15759")

        # Mark flush points
        for fi in flush_indices:
            ax3.axvline(x=x[fi], color="red", linestyle="--", alpha=0.5, linewidth=0.8)

        ax3.set_xlabel("Rows Written (millions)")
        ax3.set_title("CPU Utilization & Write Throughput")
        ax3.grid(True, alpha=0.3)

        # Combined legend
        lines1, labels1 = ax3.get_legend_handles_labels()
        lines2, labels2 = ax3r.get_legend_handles_labels()
        ax3.legend(lines1 + lines2, labels1 + labels2, loc="upper right", fontsize=8)

        # Remove x-label from ax2 since ax3 is the bottom now
        ax2.set_xlabel("")

    plt.tight_layout()
    plt.savefig(out_png, dpi=150)
    print(f"Chart saved to: {out_png}")

    # Also print summary stats
    print(f"\nData points: {n}")
    print(f"Peak total memory: {max(total):.2f} MB")
    print(f"Final memory (after close): {total[-1]:.2f} MB")
    if flush_pairs:
        print(f"Auto-flush events detected: {len(flush_pairs)}")
        for i, (bi, ai) in enumerate(flush_pairs):
            print(f"  Flush #{i+1} at {x[ai]:.1f}M rows: "
                  f"{total[bi]:.1f} MB → {total[ai]:.1f} MB "
                  f"(freed {total[bi]-total[ai]:.1f} MB)")
    for mod in top_mods:
        print(f"  Peak {mod}: {peak_vals[mod]:.2f} MB")


if __name__ == "__main__":
    main()
