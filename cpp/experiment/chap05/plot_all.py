#!/usr/bin/env python3
"""
Chapter 5 experiment result plotting.
Generates PDF figures for E5-1, E5-2, E5-4.

Usage:
    python3 plot_all.py [chap05_dir]
"""

import csv
import os
import sys

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
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

COLORS = ['#2196F3', '#FF9800', '#4CAF50', '#E91E63', '#9C27B0', '#00BCD4']
HATCH_A = '///'
HATCH_C = ''


def read_csv(path):
    with open(path) as f:
        reader = csv.DictReader(f)
        return list(reader)


# ═══════════════════════════════════════════════════════════════════════════
# E5-1: Codec Throughput
# ═══════════════════════════════════════════════════════════════════════════

def plot_e5_1(base_dir):
    codec_dir = os.path.join(base_dir, 'E5_1_codec')

    off_path = os.path.join(codec_dir, 'codec_results_OFF.csv')
    on_path = os.path.join(codec_dir, 'codec_results_ON.csv')

    if not os.path.exists(off_path):
        print(f"  [skip] E5-1: {off_path} not found")
        return

    off = read_csv(off_path)
    has_simd = os.path.exists(on_path)
    on = read_csv(on_path) if has_simd else []

    def get_tp(rows, dtype, op):
        for r in rows:
            if r['dtype'] == dtype and r['operation'] == op:
                return float(r['throughput_mrows_s'])
        return 0

    dtypes = ['INT32', 'INT64']

    # ── Left: Encoding (SIMD OFF vs ON) ──
    enc_off = [get_tp(off, d, 'encode') for d in dtypes]
    enc_on = [get_tp(on, d, 'encode') for d in dtypes] if has_simd else enc_off

    # ── Right: Decoding (per-value / batch scalar / batch SIMD) ──
    dec_pv = [get_tp(off, d, 'decode_perval') for d in dtypes]
    dec_batch_off = [get_tp(off, d, 'decode_batch') for d in dtypes]
    dec_batch_on = [get_tp(on, d, 'decode_batch') for d in dtypes] if has_simd else dec_batch_off

    fig, axes = plt.subplots(1, 2, figsize=(11, 5.5))
    x = np.arange(len(dtypes))

    # Encoding
    ax = axes[0]
    w = 0.35
    b1 = ax.bar(x - w/2, enc_off, w, label='SIMD OFF',
                 color=COLORS[0], edgecolor='black', linewidth=0.5)
    b2 = ax.bar(x + w/2, enc_on, w, label='SIMD ON',
                 color=COLORS[1], edgecolor='black', linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(dtypes)
    ax.set_ylabel('Throughput (M rows/s)')
    ax.set_title('T5-1: Encoding Throughput')
    ax.set_ylim(0, max(enc_off + enc_on) * 1.3)
    ax.legend(fontsize=9)
    for bar, val in zip(b1, enc_off):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                f'{val:.0f}', ha='center', va='bottom', fontsize=9)
    for bar, val, base in zip(b2, enc_on, enc_off):
        sp = val / base if base > 0 else 0
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                f'{val:.0f}\n({sp:.2f}x)', ha='center', va='bottom',
                fontsize=8, color='#E65100')

    # Decoding: 3-bar group
    ax = axes[1]
    w = 0.25
    b1 = ax.bar(x - w, dec_pv, w, label='Per-value',
                 color='#9E9E9E', edgecolor='black', linewidth=0.5)
    b2 = ax.bar(x, dec_batch_off, w, label='Batch (Scalar)',
                 color=COLORS[0], edgecolor='black', linewidth=0.5)
    b3 = ax.bar(x + w, dec_batch_on, w, label='Batch (SIMD)',
                 color=COLORS[1], edgecolor='black', linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(dtypes)
    ax.set_ylabel('Throughput (M rows/s)')
    ax.set_title('T5-2: Decoding Throughput')
    ax.set_ylim(0, max(dec_batch_on) * 1.35)
    ax.legend(fontsize=9)

    for bar, val in zip(b1, dec_pv):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                f'{val:.0f}', ha='center', va='bottom', fontsize=8)
    for bar, val, base in zip(b2, dec_batch_off, dec_pv):
        sp = val / base if base > 0 else 0
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                f'{val:.0f}\n({sp:.1f}x)', ha='center', va='bottom',
                fontsize=8, color='#1565C0')
    for bar, val, base in zip(b3, dec_batch_on, dec_pv):
        sp = val / base if base > 0 else 0
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                f'{val:.0f}\n({sp:.1f}x)', ha='center', va='bottom',
                fontsize=8, color='#E65100')

    fig.suptitle('E5-1: TS_2DIFF Codec — Per-value vs Batch vs Batch+SIMD',
                 y=1.02, fontsize=13)
    plt.tight_layout()
    out = os.path.join(codec_dir, 'F5_codec_throughput.pdf')
    fig.savefig(out)
    plt.close(fig)
    print(f"  [ok] {out}")


# ═══════════════════════════════════════════════════════════════════════════
# E5-2: Time Filter Throughput
# ═══════════════════════════════════════════════════════════════════════════

def plot_e5_2(base_dir):
    csv_path = os.path.join(base_dir, 'E5_2_filter_latmat',
                            'filter_results_C1.csv')
    if not os.path.exists(csv_path):
        print(f"  [skip] E5-2: {csv_path} not found")
        return

    rows = read_csv(csv_path)
    sels = [int(r['selectivity_pct']) for r in rows]
    tps = [float(r['throughput_mrows_s']) for r in rows]
    times = [float(r['time_s']) for r in rows]
    row_counts = [int(r['rows_read']) for r in rows]

    fig, axes = plt.subplots(1, 2, figsize=(10, 4.5))

    # F5-1: Throughput vs selectivity
    ax = axes[0]
    sel_labels = [f'{s}%' for s in sels]
    x = np.arange(len(sels))
    bars = ax.bar(x, tps, width=0.5, color=COLORS[0],
                  edgecolor='black', linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(sel_labels)
    ax.set_xlabel('Time Selectivity')
    ax.set_ylabel('Throughput (M rows/s)')
    ax.set_title('F5-1: Read Throughput vs Selectivity')
    ax.set_ylim(0, max(tps) * 1.25)
    for bar, val in zip(bars, tps):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                f'{val:.1f}', ha='center', va='bottom', fontsize=9)

    # Latency vs selectivity
    ax = axes[1]
    ax.plot(sels, times, 'o-', color=COLORS[1], markersize=6, linewidth=2)
    for s, t in zip(sels, times):
        ax.annotate(f'{t:.3f}s', (s, t), textcoords='offset points',
                    xytext=(0, 8), ha='center', fontsize=9)
    ax.set_xlabel('Time Selectivity (%)')
    ax.set_ylabel('Query Latency (s)')
    ax.set_title('Query Latency vs Selectivity')
    ax.set_xlim(-5, 105)
    ax.set_ylim(0, max(times) * 1.2)
    ax.grid(True, alpha=0.3)

    fig.suptitle('E5-2: Time Filter Performance (C1, 20M rows, 8 INT64 fields)',
                 y=1.02)
    plt.tight_layout()

    out = os.path.join(base_dir, 'E5_2_filter_latmat', 'F5_filter_throughput.pdf')
    fig.savefig(out)
    plt.close(fig)
    print(f"  [ok] {out}")


# ═══════════════════════════════════════════════════════════════════════════
# E5-4a: Skip Rate (Plan A vs Plan C)
# ═══════════════════════════════════════════════════════════════════════════

def plot_e5_4a(base_dir):
    csv_path = os.path.join(base_dir, 'E5_4_block_filter',
                            'skip_rate_results.csv')
    if not os.path.exists(csv_path):
        print(f"  [skip] E5-4a: {csv_path} not found")
        return

    rows = read_csv(csv_path)

    # Group by bw
    bws = sorted(set(int(r['bw']) for r in rows))
    planA_skip = []
    planC_skip = []
    phantoms = []
    for bw in bws:
        for r in rows:
            if int(r['bw']) == bw and r['method'] == 'PlanA':
                planA_skip.append(float(r['skip_rate_pct']))
            if int(r['bw']) == bw and r['method'] == 'PlanC':
                planC_skip.append(float(r['skip_rate_pct']))
                phantoms.append(int(r['phantom_blocks']))

    fig, axes = plt.subplots(1, 2, figsize=(10, 4.5))

    # F5-3: Skip rate comparison
    ax = axes[0]
    x = np.arange(len(bws))
    w = 0.35
    bars_a = ax.bar(x - w/2, planA_skip, w, label='Plan A (Conservative)',
                    color=COLORS[3], edgecolor='black', linewidth=0.5,
                    hatch=HATCH_A, alpha=0.85)
    bars_c = ax.bar(x + w/2, planC_skip, w, label='Plan C (Lookahead)',
                    color=COLORS[2], edgecolor='black', linewidth=0.5,
                    alpha=0.85)
    ax.set_xticks(x)
    ax.set_xticklabels([str(b) for b in bws])
    ax.set_xlabel('bit_width')
    ax.set_ylabel('Skip Rate (%)')
    ax.set_title('F5-3: Block Skip Rate')
    ax.set_ylim(0, 115)
    ax.legend(loc='center right')
    # Annotate
    for bar, val in zip(bars_a, planA_skip):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                f'{val:.0f}%', ha='center', va='bottom', fontsize=8)
    for bar, val in zip(bars_c, planC_skip):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                f'{val:.0f}%', ha='center', va='bottom', fontsize=8)

    # Phantom block count
    ax = axes[1]
    bars = ax.bar(x, phantoms, width=0.5, color=COLORS[4],
                  edgecolor='black', linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels([str(b) for b in bws])
    ax.set_xlabel('bit_width')
    ax.set_ylabel('Phantom Blocks (out of 1000)')
    ax.set_title('Phantom Blocks: Plan A False Positives')
    ax.set_ylim(0, max(phantoms) * 1.2 if max(phantoms) > 0 else 10)
    for bar, val in zip(bars, phantoms):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 5,
                str(val), ha='center', va='bottom', fontsize=9)

    fig.suptitle('E5-4a: Block-Level Time Filter Precision (1000 blocks)',
                 y=1.02)
    plt.tight_layout()

    out = os.path.join(base_dir, 'E5_4_block_filter', 'F5_skip_rate.pdf')
    fig.savefig(out)
    plt.close(fig)
    print(f"  [ok] {out}")


# ═══════════════════════════════════════════════════════════════════════════
# E5-4b: Query Latency (Plan A vs Plan C)
# ═══════════════════════════════════════════════════════════════════════════

def plot_e5_4b(base_dir):
    csv_path = os.path.join(base_dir, 'E5_4_block_filter',
                            'latency_results.csv')
    if not os.path.exists(csv_path):
        print(f"  [skip] E5-4b: {csv_path} not found")
        return

    rows = read_csv(csv_path)
    bws = sorted(set(int(r['bw']) for r in rows))

    a_p50 = []
    c_p50 = []
    a_p95 = []
    c_p95 = []
    for bw in bws:
        for r in rows:
            if int(r['bw']) == bw and r['method'] == 'PlanA':
                a_p50.append(float(r['latency_ms_p50']))
                a_p95.append(float(r['latency_ms_p95']))
            if int(r['bw']) == bw and r['method'] == 'PlanC':
                c_p50.append(float(r['latency_ms_p50']))
                c_p95.append(float(r['latency_ms_p95']))

    fig, ax = plt.subplots(1, 1, figsize=(7, 4.5))

    x = np.arange(len(bws))
    w = 0.3
    bars_a = ax.bar(x - w/2, a_p50, w, label='Plan A p50',
                    color=COLORS[3], edgecolor='black', linewidth=0.5,
                    hatch=HATCH_A, alpha=0.85)
    bars_c = ax.bar(x + w/2, c_p50, w, label='Plan C p50',
                    color=COLORS[2], edgecolor='black', linewidth=0.5,
                    alpha=0.85)

    # Add p95 as error bars
    ax.errorbar(x - w/2, a_p50,
                yerr=[[0]*len(bws), [a95 - a50 for a95, a50 in zip(a_p95, a_p50)]],
                fmt='none', ecolor='black', capsize=3)
    ax.errorbar(x + w/2, c_p50,
                yerr=[[0]*len(bws), [c95 - c50 for c95, c50 in zip(c_p95, c_p50)]],
                fmt='none', ecolor='black', capsize=3)

    ax.set_xticks(x)
    ax.set_xticklabels([str(b) for b in bws])
    ax.set_xlabel('bit_width')
    ax.set_ylabel('Query Latency (ms)')
    ax.set_title('E5-4b: Query Latency (10% selectivity, p50 + p95 whisker)')
    ax.legend()
    ax.set_ylim(0, max(a_p95 + c_p95) * 1.3)

    for bar, val in zip(bars_a, a_p50):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.001,
                f'{val:.3f}', ha='center', va='bottom', fontsize=8)
    for bar, val in zip(bars_c, c_p50):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.001,
                f'{val:.3f}', ha='center', va='bottom', fontsize=8)

    plt.tight_layout()

    out = os.path.join(base_dir, 'E5_4_block_filter', 'F5_query_latency.pdf')
    fig.savefig(out)
    plt.close(fig)
    print(f"  [ok] {out}")


# ═══════════════════════════════════════════════════════════════════════════
# Combined summary figure
# ═══════════════════════════════════════════════════════════════════════════

def plot_summary(base_dir):
    """A single overview figure combining key results."""
    off_path = os.path.join(base_dir, 'E5_1_codec', 'codec_results_OFF.csv')
    on_path = os.path.join(base_dir, 'E5_1_codec', 'codec_results_ON.csv')
    flt_path = os.path.join(base_dir, 'E5_2_filter_latmat', 'filter_results_C1.csv')
    skp_path = os.path.join(base_dir, 'E5_4_block_filter', 'skip_rate_results.csv')

    if not all(os.path.exists(p) for p in [off_path, flt_path, skp_path]):
        print("  [skip] summary: missing data")
        return

    off = read_csv(off_path)
    has_simd = os.path.exists(on_path)
    on = read_csv(on_path) if has_simd else []
    flt = read_csv(flt_path)
    skp = read_csv(skp_path)

    def get_tp(rows, dtype, op):
        for r in rows:
            if r['dtype'] == dtype and r['operation'] == op:
                return float(r['throughput_mrows_s'])
        return 0

    fig, axes = plt.subplots(2, 2, figsize=(11, 9))

    # (0,0) Decode: per-value vs batch vs batch+SIMD
    ax = axes[0][0]
    dtypes = ['INT32', 'INT64']
    dec_pv = [get_tp(off, d, 'decode_perval') for d in dtypes]
    dec_bs = [get_tp(off, d, 'decode_batch') for d in dtypes]
    dec_bo = [get_tp(on, d, 'decode_batch') for d in dtypes] if has_simd else dec_bs
    x = np.arange(len(dtypes))
    w = 0.25
    ax.bar(x - w, dec_pv, w, label='Per-value', color='#9E9E9E',
           edgecolor='black', linewidth=0.5)
    ax.bar(x, dec_bs, w, label='Batch', color=COLORS[0],
           edgecolor='black', linewidth=0.5)
    ax.bar(x + w, dec_bo, w, label='Batch+SIMD', color=COLORS[1],
           edgecolor='black', linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(dtypes)
    ax.set_ylabel('M rows/s')
    ax.set_title('E5-1: Decoding Throughput')
    ax.legend(fontsize=8)
    ax.set_ylim(0, max(dec_bo) * 1.25)

    # (0,1) Filter throughput
    ax = axes[0][1]
    sels = [int(r['selectivity_pct']) for r in flt]
    tps = [float(r['throughput_mrows_s']) for r in flt]
    ax.bar(range(len(sels)), tps, color=COLORS[2],
           edgecolor='black', linewidth=0.5)
    ax.set_xticks(range(len(sels)))
    ax.set_xticklabels([f'{s}%' for s in sels])
    ax.set_xlabel('Selectivity')
    ax.set_ylabel('M rows/s')
    ax.set_title('E5-2: Filter Throughput')
    ax.set_ylim(0, max(tps) * 1.2)

    # (1,0) Skip rate
    ax = axes[1][0]
    bws = sorted(set(int(r['bw']) for r in skp))
    pA = [float(r['skip_rate_pct']) for r in skp if r['method'] == 'PlanA']
    pC = [float(r['skip_rate_pct']) for r in skp if r['method'] == 'PlanC']
    x = np.arange(len(bws))
    w = 0.35
    ax.bar(x - w/2, pA, w, label='Plan A', color=COLORS[3],
           edgecolor='black', linewidth=0.5, hatch=HATCH_A, alpha=0.85)
    ax.bar(x + w/2, pC, w, label='Plan C', color=COLORS[2],
           edgecolor='black', linewidth=0.5, alpha=0.85)
    ax.set_xticks(x)
    ax.set_xticklabels([str(b) for b in bws])
    ax.set_xlabel('bit_width')
    ax.set_ylabel('Skip Rate (%)')
    ax.set_title('E5-4a: Block Skip Rate')
    ax.legend()
    ax.set_ylim(0, 115)

    # (1,1) Phantom blocks
    ax = axes[1][1]
    phantoms = [int(r['phantom_blocks']) for r in skp if r['method'] == 'PlanC']
    ax.bar(x, phantoms, width=0.5, color=COLORS[4],
           edgecolor='black', linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels([str(b) for b in bws])
    ax.set_xlabel('bit_width')
    ax.set_ylabel('Phantom Blocks')
    ax.set_title('E5-4a: Plan A False Positives')

    fig.suptitle('Chapter 5: SIMD Vectorization & Filter Acceleration — Summary',
                 fontsize=14, fontweight='bold', y=1.01)
    plt.tight_layout()

    out = os.path.join(base_dir, 'chap05_summary.pdf')
    fig.savefig(out)
    plt.close(fig)
    print(f"  [ok] {out}")


# ─── Main ───────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    base = sys.argv[1] if len(sys.argv) > 1 else '.'
    print("Plotting Chapter 5 results...")
    plot_e5_1(base)
    plot_e5_2(base)
    plot_e5_4a(base)
    plot_e5_4b(base)
    plot_summary(base)
    print("Done!")
