#!/usr/bin/env python3
"""
Plot read & write throughput from throughput_bench CSV output.

Usage:
    python3 plot_throughput.py [csv_dir]

Generates:
    fig_write_throughput.pdf  - Write throughput bar chart
    fig_read_throughput.pdf   - Read throughput bar chart
    fig_read_speedup.pdf      - Read speedup over serial baseline
    fig_write_speedup.pdf     - Write speedup over serial baseline
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import numpy as np

matplotlib.rcParams.update({
    'font.size': 11,
    'figure.figsize': (8, 4.5),
    'figure.dpi': 150,
    'axes.grid': True,
    'grid.alpha': 0.3,
})

csv_dir = sys.argv[1] if len(sys.argv) > 1 else '.'

def load(name):
    path = os.path.join(csv_dir, name)
    if not os.path.exists(path):
        print(f"Warning: {path} not found, skipping")
        return None
    return pd.read_csv(path)


def plot_throughput(df, title, outfile):
    """Grouped bar chart: throughput by (cols, compression), grouped by threads."""
    if df is None:
        return

    compressions = df['compression'].unique()
    col_counts = sorted(df['cols'].unique())
    thread_counts = sorted(df['threads'].unique())

    fig, axes = plt.subplots(1, len(compressions), figsize=(6 * len(compressions), 5),
                             sharey=True)
    if len(compressions) == 1:
        axes = [axes]

    colors = plt.cm.Set2(np.linspace(0, 1, len(thread_counts)))

    for ax, comp in zip(axes, compressions):
        sub = df[df['compression'] == comp]
        x = np.arange(len(col_counts))
        width = 0.8 / len(thread_counts)

        for i, t in enumerate(thread_counts):
            vals = []
            for c in col_counts:
                row = sub[(sub['cols'] == c) & (sub['threads'] == t)]
                vals.append(row['throughput_mrows_s'].values[0] if len(row) > 0 else 0)
            label = f't={t}' + (' (serial)' if t == 1 else '')
            bars = ax.bar(x + i * width - 0.4 + width / 2, vals, width,
                          label=label, color=colors[i], edgecolor='white', linewidth=0.5)
            for bar, val in zip(bars, vals):
                if val > 0:
                    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                            f'{val:.1f}', ha='center', va='bottom', fontsize=7)

        ax.set_xlabel('Number of Columns')
        ax.set_xticks(x)
        ax.set_xticklabels(col_counts)
        ax.set_title(f'{comp}')
        ax.legend(fontsize=8, loc='upper right')

    axes[0].set_ylabel('Throughput (Mrows/s)')
    fig.suptitle(title, fontsize=13, fontweight='bold')
    plt.tight_layout()
    plt.savefig(os.path.join(csv_dir, outfile), bbox_inches='tight')
    plt.close()
    print(f"  Saved {outfile}")


def plot_speedup(df, title, outfile):
    """Line chart: speedup relative to serial (threads=1) baseline."""
    if df is None:
        return

    compressions = df['compression'].unique()
    col_counts = sorted(df['cols'].unique())
    thread_counts = sorted(df['threads'].unique())

    fig, axes = plt.subplots(1, len(compressions), figsize=(6 * len(compressions), 4.5),
                             sharey=True)
    if len(compressions) == 1:
        axes = [axes]

    markers = ['o', 's', '^', 'D', 'v']
    colors = plt.cm.tab10(np.linspace(0, 0.5, len(col_counts)))

    for ax, comp in zip(axes, compressions):
        sub = df[df['compression'] == comp]

        for ci, c in enumerate(col_counts):
            col_data = sub[sub['cols'] == c].sort_values('threads')
            baseline = col_data[col_data['threads'] == 1]['throughput_mrows_s'].values
            if len(baseline) == 0:
                continue
            baseline = baseline[0]
            speedups = col_data['throughput_mrows_s'].values / baseline
            threads = col_data['threads'].values

            ax.plot(threads, speedups, marker=markers[ci % len(markers)],
                    color=colors[ci], label=f'{c} cols', linewidth=2, markersize=6)

        # Ideal line
        ax.plot(thread_counts, thread_counts, '--', color='gray', alpha=0.5,
                label='Ideal', linewidth=1)

        ax.set_xlabel('Thread Count')
        ax.set_xticks(thread_counts)
        ax.set_title(f'{comp}')
        ax.legend(fontsize=8)

    axes[0].set_ylabel('Speedup vs Serial')
    fig.suptitle(title, fontsize=13, fontweight='bold')
    plt.tight_layout()
    plt.savefig(os.path.join(csv_dir, outfile), bbox_inches='tight')
    plt.close()
    print(f"  Saved {outfile}")


def plot_compare(write_df, read_df, outfile):
    """Side-by-side comparison of read vs write throughput at different thread counts."""
    if write_df is None or read_df is None:
        return

    compressions = write_df['compression'].unique()
    col_counts = sorted(write_df['cols'].unique())

    fig, axes = plt.subplots(1, len(compressions), figsize=(6 * len(compressions), 5),
                             sharey=True)
    if len(compressions) == 1:
        axes = [axes]

    for ax, comp in zip(axes, compressions):
        ws = write_df[write_df['compression'] == comp]
        rs = read_df[read_df['compression'] == comp]

        x = np.arange(len(col_counts))
        width = 0.18

        # Serial write, parallel write (best), serial read, parallel read (best)
        configs = [
            ('Write t=1', ws, 1, '#1f77b4', '//'),
            ('Write best', ws, None, '#1f77b4', None),
            ('Read t=1', rs, 1, '#ff7f0e', '//'),
            ('Read best', rs, None, '#ff7f0e', None),
        ]

        for i, (label, data, t, color, hatch) in enumerate(configs):
            vals = []
            for c in col_counts:
                if t is not None:
                    row = data[(data['cols'] == c) & (data['threads'] == t)]
                else:
                    row = data[data['cols'] == c]
                    row = row.loc[row['throughput_mrows_s'].idxmax()]
                    vals.append(row['throughput_mrows_s'])
                    continue
                vals.append(row['throughput_mrows_s'].values[0] if len(row) > 0 else 0)

            bars = ax.bar(x + i * width - 0.36 + width / 2, vals, width,
                          label=label, color=color, hatch=hatch, alpha=0.8,
                          edgecolor='white', linewidth=0.5)

        ax.set_xlabel('Number of Columns')
        ax.set_xticks(x)
        ax.set_xticklabels(col_counts)
        ax.set_title(f'{comp}')
        ax.legend(fontsize=7, loc='upper right')

    axes[0].set_ylabel('Throughput (Mrows/s)')
    fig.suptitle('Read vs Write: Serial vs Best Parallel', fontsize=13, fontweight='bold')
    plt.tight_layout()
    plt.savefig(os.path.join(csv_dir, outfile), bbox_inches='tight')
    plt.close()
    print(f"  Saved {outfile}")


if __name__ == '__main__':
    print("Loading CSV data...")
    write_df = load('write_results_ON.csv')
    read_df = load('read_results_ON.csv')

    print("Generating plots...")
    plot_throughput(write_df, 'Write Throughput (ENABLE_THREADS=ON)', 'fig_write_throughput.pdf')
    plot_throughput(read_df, 'Read Throughput (ENABLE_THREADS=ON)', 'fig_read_throughput.pdf')
    plot_speedup(write_df, 'Write Speedup vs Serial', 'fig_write_speedup.pdf')
    plot_speedup(read_df, 'Read Speedup vs Serial', 'fig_read_speedup.pdf')
    plot_compare(write_df, read_df, 'fig_read_write_compare.pdf')

    print("\nDone. All figures saved to", csv_dir)
