#!/usr/bin/env python3
"""
Treemap of read-path memory module composition.

Reads CSV files produced by read_mem_model ({prefix}_cols{n}_batch{b}.csv)
and generates one treemap PNG per N_cols value, with each batch_size
shown as a separate panel.

Usage:
    python3 plot_read_treemap.py [csv_prefix] [output_prefix]

    csv_prefix:    default "read_memory_results"
    output_prefix: default same as csv_prefix

Install squarify for best results:
    pip3 install squarify
"""

import csv
import sys
import os
import glob
import re

COL_COUNTS  = [1, 4, 8, 16]
BATCH_SIZES = [1024, 4096, 16384, 65536]


def load_csv(path):
    with open(path) as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    if not rows:
        return None
    return {k: int(v) for k, v in rows[0].items()}


def main():
    csv_prefix = sys.argv[1] if len(sys.argv) > 1 else "read_memory_results"
    out_prefix = sys.argv[2] if len(sys.argv) > 2 else csv_prefix

    # Load all result files
    data = {}
    pattern = f"{csv_prefix}_cols*_batch*.csv"
    for path in sorted(glob.glob(pattern)):
        m = re.search(r"_cols(\d+)_batch(\d+)\.csv$", path)
        if not m:
            continue
        row = load_csv(path)
        if row is not None:
            data[(int(m.group(1)), int(m.group(2)))] = row

    if not data:
        print(f"No files matched: {pattern}")
        sys.exit(1)

    print(f"Loaded {len(data)} experiments from: {pattern}")

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not found. Install: pip3 install matplotlib")
        sys.exit(1)

    colors = ["#4e79a7", "#f28e2b", "#e15759", "#76b7b2",
              "#59a14f", "#edc948", "#b07aa1", "#ff9da7",
              "#9c755f", "#bab0ac", "#d37295", "#fabfd2", "#8cd17d"]

    first = next(iter(data.values()))
    mod_keys = [k for k in first.keys()
                if k.endswith("_kb") and k != "peak_total_kb"]
    mod_color = {m: colors[i % len(colors)] for i, m in enumerate(mod_keys)}

    use_squarify = False
    try:
        import squarify
        use_squarify = True
    except ImportError:
        print("  (squarify not found; using stacked bar. "
              "Install with: pip3 install squarify)")

    # One PNG per N_cols: panels side by side for each batch_size
    n_panels = len(BATCH_SIZES)
    for n_cols in COL_COUNTS:
        fig, axes = plt.subplots(1, n_panels, figsize=(n_panels * 5, 7))
        fig.suptitle(f"Read-Path Memory Composition  (N_cols={n_cols})",
                     fontsize=14, y=1.01)

        for ci, bs in enumerate(BATCH_SIZES):
            ax = axes[ci]
            row = data.get((n_cols, bs))
            if row is None:
                ax.axis("off")
                ax.set_title(f"batch={bs}\n(no data)", fontsize=11)
                continue

            total_kb = row.get("peak_total_kb", 1) or 1
            items = [(m, row.get(m, 0)) for m in mod_keys if row.get(m, 0) > 0]
            items.sort(key=lambda t: t[1], reverse=True)
            if not items:
                ax.axis("off")
                continue

            names  = [m.replace("_kb", "") for m, _ in items]
            sizes  = [v for _, v in items]
            pcts   = [v / total_kb * 100 for v in sizes]
            clrs   = [mod_color[m] for m, _ in items]
            labels = [f"{n}\n{v/1024:.1f} MB\n({p:.0f}%)"
                      for n, v, p in zip(names, sizes, pcts)]

            if use_squarify:
                squarify.plot(sizes=sizes, label=labels, color=clrs,
                              ax=ax, alpha=0.85,
                              text_kwargs={"fontsize": 11})
                ax.axis("off")
            else:
                left = 0.0
                for name, size, pct, clr in zip(names, sizes, pcts, clrs):
                    w = size / total_kb
                    ax.barh(0, w, left=left, color=clr, alpha=0.85,
                            height=0.6, edgecolor="white", linewidth=2)
                    if pct > 4:
                        ax.text(left + w / 2, 0,
                                f"{name}\n{size/1024:.1f}MB\n({pct:.0f}%)",
                                ha="center", va="center", fontsize=10)
                    left += w
                ax.set_xlim(0, 1)
                ax.set_ylim(-0.5, 0.5)
                ax.axis("off")

            peak_mb = total_kb / 1024
            ax.set_title(f"batch_size={bs}\n{peak_mb:.1f} MB total", fontsize=11)

        plt.tight_layout()
        out = f"{out_prefix}_treemap_cols{n_cols}.png"
        plt.savefig(out, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"Treemap saved to: {out}")


if __name__ == "__main__":
    main()
