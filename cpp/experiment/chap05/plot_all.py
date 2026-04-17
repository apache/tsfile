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
    'font.family': 'sans-serif',
    'font.sans-serif': ['Songti SC', 'Heiti TC', 'STHeiti', 'PingFang HK',
                         'Hiragino Sans GB', 'DejaVu Sans'],
    'axes.unicode_minus': False,
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

    # ── Left: Encoding (per-value vs batch) ──
    # Use per-value if available, otherwise fall back to 'encode'
    enc_pv = [get_tp(off, d, 'encode_perval') or get_tp(off, d, 'encode')
              for d in dtypes]
    enc_batch = [get_tp(off, d, 'encode_batch') or get_tp(off, d, 'encode')
                 for d in dtypes]

    # ── Right: Decoding (per-value / batch scalar / batch SIMD) ──
    dec_pv = [get_tp(off, d, 'decode_perval') for d in dtypes]
    dec_batch_off = [get_tp(off, d, 'decode_batch') for d in dtypes]
    dec_batch_on = [get_tp(on, d, 'decode_batch') for d in dtypes] if has_simd else dec_batch_off

    fig, axes = plt.subplots(1, 2, figsize=(11, 5.5))
    x = np.arange(len(dtypes))

    # Encoding: per-value vs batch
    ax = axes[0]
    w = 0.35
    b1 = ax.bar(x - w/2, enc_pv, w, label='Per-value',
                 color='#9E9E9E', edgecolor='black', linewidth=0.5)
    b2 = ax.bar(x + w/2, enc_batch, w, label='Batch',
                 color=COLORS[0], edgecolor='black', linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(dtypes)
    ax.set_ylabel('吞吐量（百万行/秒）')
    ax.set_title('编码吞吐量')
    ax.set_ylim(0, max(enc_pv + enc_batch) * 1.3)
    ax.legend(fontsize=9)
    for bar, val in zip(b1, enc_pv):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                f'{val:.0f}', ha='center', va='bottom', fontsize=9)
    for bar, val, base in zip(b2, enc_batch, enc_pv):
        sp = val / base if base > 0 else 0
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                f'{val:.0f}\n({sp:.2f}x)', ha='center', va='bottom',
                fontsize=8, color='#1565C0')

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
    ax.set_ylabel('吞吐量（百万行/秒）')
    ax.set_title('解码吞吐量')
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

    fig.suptitle('TS_2DIFF 编解码器 — 逐值 vs 批处理 vs 批处理+SIMD',
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
    # Load C3 (has both ROW and Batch+SIMD), fall back to C1
    c3_path = os.path.join(base_dir, 'E5_2_filter_latmat',
                           'filter_results_C3.csv')
    c1_path = os.path.join(base_dir, 'E5_2_filter_latmat',
                           'filter_results_C1.csv')
    csv_path = c3_path if os.path.exists(c3_path) else c1_path
    if not os.path.exists(csv_path):
        print(f"  [skip] E5-2: no data found")
        return

    rows = read_csv(csv_path)
    row_data = [r for r in rows if r['config'] == 'ROW']
    # Batch config is C1 or C3
    batch_data = [r for r in rows if r['config'] != 'ROW']

    if not row_data or not batch_data:
        print(f"  [skip] E5-2: missing ROW or Batch data in {csv_path}")
        return

    sels = [int(r['selectivity_pct']) for r in row_data]
    row_tp = [float(r['throughput_mrows_s']) for r in row_data]
    batch_tp = [float(r['throughput_mrows_s']) for r in batch_data]

    fig, axes = plt.subplots(1, 2, figsize=(11, 5))

    # Left: Grouped bar — ROW vs Batch+SIMD
    ax = axes[0]
    x = np.arange(len(sels))
    sel_labels = [f'{s}%' for s in sels]
    w = 0.35
    b1 = ax.bar(x - w/2, row_tp, w, label='Row-by-row (Scalar)',
                 color='#9E9E9E', edgecolor='black', linewidth=0.5)
    b2 = ax.bar(x + w/2, batch_tp, w, label='Batch + SIMD',
                 color=COLORS[2], edgecolor='black', linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(sel_labels)
    ax.set_xlabel('时间选择性')
    ax.set_ylabel('吞吐量（百万行/秒）')
    ax.set_title('端到端读取吞吐量')
    ax.set_ylim(0, max(batch_tp) * 1.35)
    ax.legend(fontsize=9)
    for bar, val in zip(b1, row_tp):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.2,
                f'{val:.1f}', ha='center', va='bottom', fontsize=8)
    for bar, val, base in zip(b2, batch_tp, row_tp):
        sp = val / base if base > 0 else 0
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.2,
                f'{val:.1f}\n({sp:.1f}x)', ha='center', va='bottom',
                fontsize=8, color='#1B5E20')

    # Right: Speedup curve
    ax = axes[1]
    speedups = [b / r if r > 0 else 0 for b, r in zip(batch_tp, row_tp)]
    ax.plot(sels, speedups, 'o-', color=COLORS[2], markersize=8, linewidth=2.5)
    for s, sp in zip(sels, speedups):
        ax.annotate(f'{sp:.1f}x', (s, sp), textcoords='offset points',
                    xytext=(0, 10), ha='center', fontsize=10, fontweight='bold')
    ax.set_xlabel('时间选择性（%）')
    ax.set_ylabel('加速比（批处理+SIMD / 逐行）')
    ax.set_title('加速比 vs 选择性')
    ax.set_xlim(-5, 105)
    ax.set_ylim(0, max(speedups) * 1.3)
    ax.axhline(y=1, color='gray', linestyle='--', alpha=0.5)
    ax.grid(True, alpha=0.3)

    fig.suptitle('端到端 — 逐行 vs 批处理+SIMD（2000万行）',
                 y=1.02, fontsize=13)
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

    total_blocks = int(rows[0]['blocks_total'])

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
    ax.set_xlabel('位宽')
    ax.set_ylabel('跳过率（%）')
    ax.set_title('块跳过率')
    ax.set_ylim(0, 120)
    ax.legend(loc='center right', fontsize=9)
    # Annotate — always show value, even when bar height is 0
    for bar, val in zip(bars_a, planA_skip):
        y = max(bar.get_height(), 0) + 2
        ax.text(bar.get_x() + bar.get_width() / 2, y,
                f'{val:.0f}%', ha='center', va='bottom', fontsize=9,
                fontweight='bold', color=COLORS[3])
    for bar, val in zip(bars_c, planC_skip):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 2,
                f'{val:.0f}%', ha='center', va='bottom', fontsize=9,
                fontweight='bold', color='#2E7D32')
    # Add a horizontal reference line at 100%
    ax.axhline(y=100, color='gray', linestyle=':', alpha=0.4)

    # Phantom block count (bar chart + percentage annotation)
    ax = axes[1]
    bar_colors = [COLORS[4] if p > 0 else '#E0E0E0' for p in phantoms]
    bars = ax.bar(x, phantoms, width=0.5, color=bar_colors,
                  edgecolor='black', linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels([str(b) for b in bws])
    ax.set_xlabel('bit_width')
    ax.set_ylabel(f'幻影块（共 {total_blocks}）')
    ax.set_title('幻影块：Plan A 误报')
    ax.set_ylim(0, max(phantoms) * 1.2 if max(phantoms) > 0 else 10)
    for bar, val in zip(bars, phantoms):
        pct = 100.0 * val / total_blocks if total_blocks > 0 else 0
        label = f'{val}\n({pct:.1f}%)' if val > 0 else '0'
        ax.text(bar.get_x() + bar.get_width() / 2,
                max(bar.get_height(), 0) + total_blocks * 0.02,
                label, ha='center', va='bottom', fontsize=9)

    fig.suptitle(f'块级时间过滤精度（{total_blocks} 块）',
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
    ax.set_xlabel('位宽')
    ax.set_ylabel('查询延迟（ms）')
    ax.set_title('查询延迟（10% 选择性，p50 + p95 须线）')
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
    ax.set_ylabel('百万行/秒')
    ax.set_title('解码吞吐量')
    ax.legend(fontsize=8)
    ax.set_ylim(0, max(dec_bo) * 1.25)

    # (0,1) Filter throughput: ROW vs Batch+SIMD
    ax = axes[0][1]
    # Try C3 first, fall back to C1
    c3_flt = os.path.join(base_dir, 'E5_2_filter_latmat', 'filter_results_C3.csv')
    if os.path.exists(c3_flt):
        flt = read_csv(c3_flt)
    flt_row = [r for r in flt if r['config'] == 'ROW']
    flt_batch = [r for r in flt if r['config'] != 'ROW']
    if flt_row and flt_batch:
        sels = [int(r['selectivity_pct']) for r in flt_row]
        row_tp = [float(r['throughput_mrows_s']) for r in flt_row]
        bat_tp = [float(r['throughput_mrows_s']) for r in flt_batch]
        x = np.arange(len(sels))
        w = 0.35
        ax.bar(x - w/2, row_tp, w, label='Row', color='#9E9E9E',
               edgecolor='black', linewidth=0.5)
        ax.bar(x + w/2, bat_tp, w, label='Batch+SIMD', color=COLORS[2],
               edgecolor='black', linewidth=0.5)
        ax.set_xticks(x)
        ax.set_xticklabels([f'{s}%' for s in sels])
        ax.set_ylim(0, max(bat_tp) * 1.2)
        ax.legend(fontsize=8)
    else:
        sels = [int(r['selectivity_pct']) for r in flt]
        tps = [float(r['throughput_mrows_s']) for r in flt]
        ax.bar(range(len(sels)), tps, color=COLORS[2],
               edgecolor='black', linewidth=0.5)
        ax.set_xticks(range(len(sels)))
        ax.set_xticklabels([f'{s}%' for s in sels])
        ax.set_ylim(0, max(tps) * 1.2)
    ax.set_xlabel('选择性')
    ax.set_ylabel('百万行/秒')
    ax.set_title('端到端吞吐量')

    # (1,0) Skip rate
    ax = axes[1][0]
    bws = sorted(set(int(r['bw']) for r in skp))
    pA = [float(r['skip_rate_pct']) for r in skp if r['method'] == 'PlanA']
    pC = [float(r['skip_rate_pct']) for r in skp if r['method'] == 'PlanC']
    skp_total = int(skp[0]['blocks_total'])
    x = np.arange(len(bws))
    w = 0.35
    bars_a = ax.bar(x - w/2, pA, w, label='Plan A', color=COLORS[3],
                    edgecolor='black', linewidth=0.5, hatch=HATCH_A, alpha=0.85)
    bars_c = ax.bar(x + w/2, pC, w, label='Plan C', color=COLORS[2],
                    edgecolor='black', linewidth=0.5, alpha=0.85)
    ax.set_xticks(x)
    ax.set_xticklabels([str(b) for b in bws])
    ax.set_xlabel('位宽')
    ax.set_ylabel('跳过率（%）')
    ax.set_title(f'块跳过率（{skp_total} 块）')
    ax.legend(fontsize=8)
    ax.set_ylim(0, 120)
    ax.axhline(y=100, color='gray', linestyle=':', alpha=0.4)
    for bar, val in zip(bars_a, pA):
        ax.text(bar.get_x() + bar.get_width()/2, max(bar.get_height(), 0) + 2,
                f'{val:.0f}%', ha='center', va='bottom', fontsize=8,
                fontweight='bold', color=COLORS[3])
    for bar, val in zip(bars_c, pC):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 2,
                f'{val:.0f}%', ha='center', va='bottom', fontsize=8,
                fontweight='bold', color='#2E7D32')

    # (1,1) Phantom blocks
    ax = axes[1][1]
    phantoms = [int(r['phantom_blocks']) for r in skp if r['method'] == 'PlanC']
    bar_colors = [COLORS[4] if p > 0 else '#E0E0E0' for p in phantoms]
    bars = ax.bar(x, phantoms, width=0.5, color=bar_colors,
                  edgecolor='black', linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels([str(b) for b in bws])
    ax.set_xlabel('位宽')
    ax.set_ylabel(f'幻影块（共 {skp_total}）')
    ax.set_title('Plan A 误报')
    ax.set_ylim(0, max(phantoms) * 1.2 if max(phantoms) > 0 else 10)
    for bar, val in zip(bars, phantoms):
        pct = 100.0 * val / skp_total if skp_total > 0 else 0
        label = f'{val}\n({pct:.1f}%)' if val > 0 else '0'
        ax.text(bar.get_x() + bar.get_width()/2,
                max(bar.get_height(), 0) + skp_total * 0.02,
                label, ha='center', va='bottom', fontsize=8)

    fig.suptitle('SIMD 向量化与过滤加速 — 总结',
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
