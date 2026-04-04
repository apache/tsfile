#!/usr/bin/env python3
"""
E6-1: TsFile vs Parquet — Result Plotting.

Generates:
  F6_1a_space_cost.pdf     — File size comparison
  F6_1b_tag_filter.pdf     — Tag filter latency
  F6_1c_full_scan.pdf      — Full scan throughput
  F6_1d_time_filter.pdf    — Time filter throughput (TSBS only)
  F6_1_summary.pdf         — Combined 2x2 summary

Usage:
    python3 plot_e6_1.py [results_dir]
"""

import csv
import os
import sys

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

# ─── Style ──────────────────────────────────────────────────────────────────

plt.rcParams.update({
    'font.size': 11,
    'axes.titlesize': 13,
    'axes.labelsize': 12,
    'xtick.labelsize': 10,
    'ytick.labelsize': 10,
    'legend.fontsize': 10,
    'figure.dpi': 150,
    'savefig.bbox': 'tight',
    'savefig.pad_inches': 0.1,
})

C_TSFILE  = '#2196F3'  # blue
C_PARQUET = '#FF9800'  # orange


def read_csv(path):
    with open(path) as f:
        return list(csv.DictReader(f))


# ─── Data ───────────────────────────────────────────────────────────────────

# Space cost (MB) — manually recorded from benchmark output
SPACE = {
    'TSBS':    {'tsfile': 111, 'parquet': 110},
    'GeoLife': {'tsfile': 294, 'parquet': 416},
    'TDrive':  {'tsfile': 171, 'parquet': 147},
}

# Dataset metadata
DS_META = {
    'tsbs':    {'label': 'TSBS',    'devices': 100,   'points': '5.0M'},
    'geolife': {'label': 'GeoLife', 'devices': 182,   'points': '24.2M'},
    'tdrive':  {'label': 'TDrive',  'devices': 10295, 'points': '16.3M'},
}


def load_results(base_dir):
    path = os.path.join(base_dir, 'all_results.csv')
    if not os.path.exists(path):
        print(f"Error: {path} not found")
        sys.exit(1)
    return read_csv(path)


def get_row(rows, dataset, experiment, engine, params=''):
    for r in rows:
        if (r['dataset'] == dataset and r['experiment'] == experiment
                and r['engine'] == engine):
            if params and r['params'] != params:
                continue
            return r
    return None


# ═══════════════════════════════════════════════════════════════════════════
# F6-1a: Space Cost
# ═══════════════════════════════════════════════════════════════════════════

def plot_space_cost(base_dir):
    datasets = ['TSBS', 'GeoLife', 'TDrive']
    ts_sizes = [SPACE[d]['tsfile'] for d in datasets]
    pq_sizes = [SPACE[d]['parquet'] for d in datasets]

    fig, ax = plt.subplots(figsize=(7, 4.5))
    x = np.arange(len(datasets))
    w = 0.35

    bars_ts = ax.bar(x - w/2, ts_sizes, w, label='TsFile',
                     color=C_TSFILE, edgecolor='black', linewidth=0.5)
    bars_pq = ax.bar(x + w/2, pq_sizes, w, label='Parquet',
                     color=C_PARQUET, edgecolor='black', linewidth=0.5)

    ax.set_xticks(x)
    ax.set_xticklabels(datasets)
    ax.set_ylabel('File Size (MB)')
    ax.set_title('T6-1a: Space Cost')
    ax.legend()
    ax.set_ylim(0, max(ts_sizes + pq_sizes) * 1.3)

    for bar, val in zip(bars_ts, ts_sizes):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                f'{val}', ha='center', va='bottom', fontsize=10,
                fontweight='bold')
    for bar, val in zip(bars_pq, pq_sizes):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                f'{val}', ha='center', va='bottom', fontsize=10,
                fontweight='bold')

    plt.tight_layout()
    out = os.path.join(base_dir, 'F6_1a_space_cost.pdf')
    fig.savefig(out)
    plt.close(fig)
    print(f"  [ok] {out}")


# ═══════════════════════════════════════════════════════════════════════════
# F6-1b: Tag Filter Latency
# ═══════════════════════════════════════════════════════════════════════════

def plot_tag_filter(base_dir, rows):
    datasets = ['tsbs', 'geolife', 'tdrive']
    labels = [DS_META[d]['label'] for d in datasets]

    ts_lat = []
    pq_lat = []
    for d in datasets:
        r_ts = get_row(rows, d, 'tag_filter', 'tsfile')
        r_pq = get_row(rows, d, 'tag_filter', 'parquet')
        ts_lat.append(float(r_ts['seconds']) * 1000)  # ms
        pq_lat.append(float(r_pq['seconds']) * 1000)

    fig, ax = plt.subplots(figsize=(7, 4.5))
    x = np.arange(len(datasets))
    w = 0.35

    bars_ts = ax.bar(x - w/2, ts_lat, w, label='TsFile',
                     color=C_TSFILE, edgecolor='black', linewidth=0.5)
    bars_pq = ax.bar(x + w/2, pq_lat, w, label='Parquet',
                     color=C_PARQUET, edgecolor='black', linewidth=0.5)

    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel('Query Latency (ms)')
    ax.set_title('T6-1b: Tag Filter — Single Device Lookup')
    ax.legend()
    ax.set_ylim(0, max(pq_lat) * 1.4)

    for bar, tl, pl in zip(bars_ts, ts_lat, pq_lat):
        speedup = pl / tl if tl > 0 else 0
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                f'{tl:.1f} ms', ha='center', va='bottom', fontsize=9)
    for bar, val in zip(bars_pq, pq_lat):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                f'{val:.1f} ms', ha='center', va='bottom', fontsize=9)

    # Add speedup annotations
    for i, (tl, pl) in enumerate(zip(ts_lat, pq_lat)):
        speedup = pl / tl if tl > 0 else 0
        ax.text(x[i], max(pq_lat) * 1.2,
                f'{speedup:.1f}x', ha='center', va='bottom',
                fontsize=11, fontweight='bold', color='#1565C0')

    plt.tight_layout()
    out = os.path.join(base_dir, 'F6_1b_tag_filter.pdf')
    fig.savefig(out)
    plt.close(fig)
    print(f"  [ok] {out}")


# ═══════════════════════════════════════════════════════════════════════════
# F6-1c: Full Scan Throughput
# ═══════════════════════════════════════════════════════════════════════════

def plot_full_scan(base_dir, rows):
    datasets = ['tsbs', 'geolife', 'tdrive']
    labels = [DS_META[d]['label'] for d in datasets]
    dev_counts = [DS_META[d]['devices'] for d in datasets]

    ts_tp = []
    pq_tp = []
    for d in datasets:
        r_ts = get_row(rows, d, 'full_scan', 'tsfile')
        r_pq = get_row(rows, d, 'full_scan', 'parquet')
        ts_tp.append(int(r_ts['rows_per_sec']) / 1e6)
        pq_tp.append(int(r_pq['rows_per_sec']) / 1e6)

    fig, ax = plt.subplots(figsize=(7, 4.5))
    x = np.arange(len(datasets))
    w = 0.35

    bars_ts = ax.bar(x - w/2, ts_tp, w, label='TsFile',
                     color=C_TSFILE, edgecolor='black', linewidth=0.5)
    bars_pq = ax.bar(x + w/2, pq_tp, w, label='Parquet',
                     color=C_PARQUET, edgecolor='black', linewidth=0.5)

    ax.set_xticks(x)
    xlabels = [f'{l}\n({dev_counts[i]} dev)' for i, l in enumerate(labels)]
    ax.set_xticklabels(xlabels)
    ax.set_ylabel('Throughput (M rows/s)')
    ax.set_title('T6-1c: Full Scan Throughput')
    ax.legend()
    ax.set_ylim(0, max(ts_tp + pq_tp) * 1.3)

    for bar, val in zip(bars_ts, ts_tp):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                f'{val:.1f}', ha='center', va='bottom', fontsize=10,
                fontweight='bold')
    for bar, val in zip(bars_pq, pq_tp):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                f'{val:.1f}', ha='center', va='bottom', fontsize=10,
                fontweight='bold')

    plt.tight_layout()
    out = os.path.join(base_dir, 'F6_1c_full_scan.pdf')
    fig.savefig(out)
    plt.close(fig)
    print(f"  [ok] {out}")


# ═══════════════════════════════════════════════════════════════════════════
# F6-1d: Time Filter (TSBS only, meaningful selectivity)
# ═══════════════════════════════════════════════════════════════════════════

def plot_time_filter(base_dir, rows):
    sels = ['10%', '50%', '100%']

    ts_tp = []
    pq_tp = []
    for sel in sels:
        r_ts = get_row(rows, 'tsbs', 'time_filter', 'tsfile', sel)
        r_pq = get_row(rows, 'tsbs', 'time_filter', 'parquet', sel)
        ts_tp.append(int(r_ts['rows_per_sec']) / 1e6)
        pq_tp.append(int(r_pq['rows_per_sec']) / 1e6)

    fig, ax = plt.subplots(figsize=(7, 4.5))
    x = np.arange(len(sels))
    w = 0.35

    bars_ts = ax.bar(x - w/2, ts_tp, w, label='TsFile',
                     color=C_TSFILE, edgecolor='black', linewidth=0.5)
    bars_pq = ax.bar(x + w/2, pq_tp, w, label='Parquet',
                     color=C_PARQUET, edgecolor='black', linewidth=0.5)

    ax.set_xticks(x)
    ax.set_xticklabels(sels)
    ax.set_xlabel('Time Selectivity')
    ax.set_ylabel('Throughput (M rows/s)')
    ax.set_title('T6-1d: Time Filter Throughput (TSBS, 100 devices)')
    ax.legend()
    ax.set_ylim(0, max(ts_tp + pq_tp) * 1.3)

    for bar, val in zip(bars_ts, ts_tp):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                f'{val:.1f}', ha='center', va='bottom', fontsize=10,
                fontweight='bold')
    for bar, val in zip(bars_pq, pq_tp):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                f'{val:.1f}', ha='center', va='bottom', fontsize=10,
                fontweight='bold')

    # Speedup annotation
    for i, (tl, pl) in enumerate(zip(ts_tp, pq_tp)):
        ratio = tl / pl if pl > 0 else 0
        label = f'TsFile {ratio:.1f}x' if ratio > 1 else f'Pq {pl/tl:.1f}x'
        color = '#1565C0' if ratio > 1 else '#E65100'
        ax.text(x[i], max(ts_tp + pq_tp) * 1.15,
                label, ha='center', fontsize=10, fontweight='bold', color=color)

    plt.tight_layout()
    out = os.path.join(base_dir, 'F6_1d_time_filter.pdf')
    fig.savefig(out)
    plt.close(fig)
    print(f"  [ok] {out}")


# ═══════════════════════════════════════════════════════════════════════════
# Summary: 2x2 combined figure
# ═══════════════════════════════════════════════════════════════════════════

def plot_summary(base_dir, rows):
    datasets = ['tsbs', 'geolife', 'tdrive']
    labels = [DS_META[d]['label'] for d in datasets]
    dev_counts = [DS_META[d]['devices'] for d in datasets]

    fig, axes = plt.subplots(2, 2, figsize=(12, 9))

    # ── (0,0) Space Cost ──
    ax = axes[0][0]
    ds_labels = ['TSBS', 'GeoLife', 'TDrive']
    ts_sizes = [SPACE[d]['tsfile'] for d in ds_labels]
    pq_sizes = [SPACE[d]['parquet'] for d in ds_labels]
    x = np.arange(len(ds_labels))
    w = 0.35
    ax.bar(x - w/2, ts_sizes, w, label='TsFile',
           color=C_TSFILE, edgecolor='black', linewidth=0.5)
    ax.bar(x + w/2, pq_sizes, w, label='Parquet',
           color=C_PARQUET, edgecolor='black', linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(ds_labels)
    ax.set_ylabel('File Size (MB)')
    ax.set_title('(a) Space Cost')
    ax.legend(fontsize=9)
    ax.set_ylim(0, max(ts_sizes + pq_sizes) * 1.25)
    for i, (ts, pq) in enumerate(zip(ts_sizes, pq_sizes)):
        ax.text(x[i] - w/2, ts + 5, str(ts), ha='center', fontsize=9,
                fontweight='bold')
        ax.text(x[i] + w/2, pq + 5, str(pq), ha='center', fontsize=9,
                fontweight='bold')

    # ── (0,1) Tag Filter ──
    ax = axes[0][1]
    ts_lat = []
    pq_lat = []
    for d in datasets:
        r_ts = get_row(rows, d, 'tag_filter', 'tsfile')
        r_pq = get_row(rows, d, 'tag_filter', 'parquet')
        ts_lat.append(float(r_ts['seconds']) * 1000)
        pq_lat.append(float(r_pq['seconds']) * 1000)

    bars_ts = ax.bar(x - w/2, ts_lat, w, label='TsFile',
                     color=C_TSFILE, edgecolor='black', linewidth=0.5)
    bars_pq = ax.bar(x + w/2, pq_lat, w, label='Parquet',
                     color=C_PARQUET, edgecolor='black', linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel('Latency (ms)')
    ax.set_title('(b) Tag Filter Latency')
    ax.legend(fontsize=9)
    ax.set_ylim(0, max(pq_lat) * 1.45)
    for i, (tl, pl) in enumerate(zip(ts_lat, pq_lat)):
        ax.text(x[i] - w/2, tl + 0.3, f'{tl:.1f}', ha='center', fontsize=8)
        ax.text(x[i] + w/2, pl + 0.3, f'{pl:.1f}', ha='center', fontsize=8)
        speedup = pl / tl if tl > 0 else 0
        ax.text(x[i], max(pq_lat) * 1.25,
                f'{speedup:.1f}x', ha='center', fontsize=11,
                fontweight='bold', color='#1565C0')

    # ── (1,0) Full Scan Throughput ──
    ax = axes[1][0]
    ts_tp = []
    pq_tp = []
    for d in datasets:
        r_ts = get_row(rows, d, 'full_scan', 'tsfile')
        r_pq = get_row(rows, d, 'full_scan', 'parquet')
        ts_tp.append(int(r_ts['rows_per_sec']) / 1e6)
        pq_tp.append(int(r_pq['rows_per_sec']) / 1e6)

    ax.bar(x - w/2, ts_tp, w, label='TsFile',
           color=C_TSFILE, edgecolor='black', linewidth=0.5)
    ax.bar(x + w/2, pq_tp, w, label='Parquet',
           color=C_PARQUET, edgecolor='black', linewidth=0.5)
    ax.set_xticks(x)
    xlabels = [f'{l}\n({dev_counts[i]} dev)' for i, l in enumerate(labels)]
    ax.set_xticklabels(xlabels)
    ax.set_ylabel('Throughput (M rows/s)')
    ax.set_title('(c) Full Scan Throughput')
    ax.legend(fontsize=9)
    ax.set_ylim(0, max(ts_tp + pq_tp) * 1.3)
    for i, (ts, pq) in enumerate(zip(ts_tp, pq_tp)):
        ax.text(x[i] - w/2, ts + 0.3, f'{ts:.1f}', ha='center', fontsize=9,
                fontweight='bold')
        ax.text(x[i] + w/2, pq + 0.3, f'{pq:.1f}', ha='center', fontsize=9,
                fontweight='bold')

    # ── (1,1) Time Filter (TSBS) ──
    ax = axes[1][1]
    sels = ['10%', '50%', '100%']
    ts_tf = []
    pq_tf = []
    for sel in sels:
        r_ts = get_row(rows, 'tsbs', 'time_filter', 'tsfile', sel)
        r_pq = get_row(rows, 'tsbs', 'time_filter', 'parquet', sel)
        ts_tf.append(int(r_ts['rows_per_sec']) / 1e6)
        pq_tf.append(int(r_pq['rows_per_sec']) / 1e6)

    x2 = np.arange(len(sels))
    ax.bar(x2 - w/2, ts_tf, w, label='TsFile',
           color=C_TSFILE, edgecolor='black', linewidth=0.5)
    ax.bar(x2 + w/2, pq_tf, w, label='Parquet',
           color=C_PARQUET, edgecolor='black', linewidth=0.5)
    ax.set_xticks(x2)
    ax.set_xticklabels(sels)
    ax.set_xlabel('Time Selectivity')
    ax.set_ylabel('Throughput (M rows/s)')
    ax.set_title('(d) Time Filter (TSBS, 100 devices)')
    ax.legend(fontsize=9)
    ax.set_ylim(0, max(ts_tf + pq_tf) * 1.35)
    for i, (ts, pq) in enumerate(zip(ts_tf, pq_tf)):
        ax.text(x2[i] - w/2, ts + 0.2, f'{ts:.1f}', ha='center', fontsize=9,
                fontweight='bold')
        ax.text(x2[i] + w/2, pq + 0.2, f'{pq:.1f}', ha='center', fontsize=9,
                fontweight='bold')
        ratio = ts / pq if pq > 0 else 0
        label = f'TsFile {ratio:.1f}x' if ratio > 1 else f'Pq {pq/ts:.1f}x'
        color = '#1565C0' if ratio > 1 else '#E65100'
        ax.text(x2[i], max(ts_tf + pq_tf) * 1.18,
                label, ha='center', fontsize=9, fontweight='bold', color=color)

    fig.suptitle('E6-1: TsFile vs Parquet — End-to-End Comparison',
                 fontsize=14, fontweight='bold', y=1.01)
    plt.tight_layout()

    out = os.path.join(base_dir, 'F6_1_summary.pdf')
    fig.savefig(out)
    plt.close(fig)
    print(f"  [ok] {out}")


# ─── Main ───────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    base = sys.argv[1] if len(sys.argv) > 1 else '.'
    print("Plotting E6-1 results...")
    data = load_results(base)
    plot_space_cost(base)
    plot_tag_filter(base, data)
    plot_full_scan(base, data)
    plot_time_filter(base, data)
    plot_summary(base, data)
    print("Done!")
