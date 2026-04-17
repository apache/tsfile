#!/usr/bin/env python3
"""
E6-1: TsFile vs Parquet - Result Plotting.

Consumes a benchmark CSV produced by `dataset_bench.cpp` or
`py_dataset_bench.py` and generates:
  F6_1a_space_cost.pdf
  F6_1b_tag_filter.pdf
  F6_1c_full_scan.pdf
  F6_1d_time_filter.pdf
  F6_1e_write_throughput.pdf
  F6_1f_tag_time_filter.pdf
  F6_1_summary.pdf

Usage:
    python3 plot_e6_1.py [results_dir_or_csv]
"""

import csv
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


plt.rcParams.update({
    "font.family": "sans-serif",
    "font.sans-serif": ["Songti SC", "Heiti TC", "STHeiti", "PingFang HK",
                         "Hiragino Sans GB", "DejaVu Sans"],
    "axes.unicode_minus": False,
    "font.size": 11,
    "axes.titlesize": 13,
    "axes.labelsize": 12,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "legend.fontsize": 10,
    "figure.dpi": 150,
    "savefig.bbox": "tight",
    "savefig.pad_inches": 0.1,
})

C_TSFILE = "#2196F3"
C_PARQUET = "#FF9800"

DATASETS = ["tsbs", "geolife", "tdrive"]
DS_META = {
    "tsbs": {"label": "TSBS", "devices": 100, "points": "5.0M"},
    "geolife": {"label": "GeoLife", "devices": 182, "points": "24.2M"},
    "tdrive": {"label": "TDrive", "devices": 10295, "points": "16.3M"},
}

PREFERRED_FILES = [
    "all_results.csv",
    "py_all_results.csv",
    "vs_parquet_results.csv",
    "py_vs_parquet_results.csv",
]
REQUIRED_READ_ROWS = [
    ("full_scan", "tsfile", ""),
    ("full_scan", "parquet", ""),
    ("tag_filter", "tsfile", None),
    ("tag_filter", "parquet", None),
]


def read_csv(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def score_rows(rows):
    experiments = {row["experiment"] for row in rows}
    datasets = {row["dataset"] for row in rows}
    return (len(datasets), len(experiments), len(rows))


def resolve_csv_path(target):
    if os.path.isfile(target):
        return target

    candidates = []
    for name in PREFERRED_FILES:
        path = os.path.join(target, name)
        if os.path.exists(path):
            candidates.append(path)

    if not candidates:
        for name in sorted(os.listdir(target)):
            if name.endswith(".csv"):
                candidates.append(os.path.join(target, name))

    if not candidates:
        raise FileNotFoundError(f"no CSV files found under {target}")

    best = None
    best_score = None
    for path in candidates:
        rows = read_csv(path)
        score = score_rows(rows)
        if best is None or score > best_score:
            best = path
            best_score = score
    return best


def group_rows(rows):
    grouped = {}
    for row in rows:
        key = (
            row["dataset"],
            row["experiment"],
            row["engine"],
            row.get("params", ""),
        )
        grouped[key] = row
    return grouped


def get_row(grouped, dataset, experiment, engine, params="", required=True):
    key = (dataset, experiment, engine, params)
    row = grouped.get(key)
    if row is not None:
        return row
    if params is None:
        for (ds, exp, eng, prm), item in grouped.items():
            if ds == dataset and exp == experiment and eng == engine:
                return item
    if required:
        raise KeyError(
            f"missing row: dataset={dataset}, experiment={experiment}, "
            f"engine={engine}, params={params!r}"
        )
    return None


def require_rows(grouped):
    for dataset in DATASETS:
        for experiment, engine, params in REQUIRED_READ_ROWS:
            get_row(grouped, dataset, experiment, engine, params)
        get_row(grouped, dataset, "space_bytes", "tsfile")
        get_row(grouped, dataset, "space_bytes", "parquet")
        get_row(grouped, dataset, "write", "tsfile")
        get_row(grouped, dataset, "write", "parquet")
    for sel in ["10%", "25%", "50%"]:
        for ds in DATASETS:
            get_row(grouped, ds, "time_filter", "tsfile", sel)
            get_row(grouped, ds, "time_filter", "parquet", sel)


def rows_per_sec(row):
    if float(row["seconds"]) > 0:
        return float(row["result_rows"]) / float(row["seconds"])
    return float(row["rows_per_sec"])


def throughput_mrows(row):
    return rows_per_sec(row) / 1e6


def latency_ms(row):
    return float(row["seconds"]) * 1000.0


def size_mb(row):
    return float(row["result_rows"]) / (1024 * 1024)


def dataset_labels():
    return [DS_META[d]["label"] for d in DATASETS]


def annotate_bars(ax, bars, values, pad, fmt="{:.1f}", bold=True):
    for bar, val in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + pad,
            fmt.format(val),
            ha="center",
            va="bottom",
            fontsize=9,
            fontweight="bold" if bold else None,
        )


def plot_dual_bar(ax, left_vals, right_vals, labels, ylabel, title,
                  left_name="TsFile", right_name="Parquet"):
    x = np.arange(len(labels))
    w = 0.35
    bars_left = ax.bar(
        x - w / 2, left_vals, w, label=left_name,
        color=C_TSFILE, edgecolor="black", linewidth=0.5
    )
    bars_right = ax.bar(
        x + w / 2, right_vals, w, label=right_name,
        color=C_PARQUET, edgecolor="black", linewidth=0.5
    )
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend()
    return x, bars_left, bars_right


def plot_space_cost(base_dir, grouped):
    ts_sizes = [
        size_mb(get_row(grouped, d, "space_bytes", "tsfile")) for d in DATASETS
    ]
    pq_sizes = [
        size_mb(get_row(grouped, d, "space_bytes", "parquet")) for d in DATASETS
    ]
    labels = dataset_labels()

    fig, ax = plt.subplots(figsize=(7, 4.5))
    _, bars_ts, bars_pq = plot_dual_bar(
        ax, ts_sizes, pq_sizes, labels, "文件大小（MB）", "空间开销"
    )
    ax.set_ylim(0, max(ts_sizes + pq_sizes) * 1.3)
    annotate_bars(ax, bars_ts, ts_sizes, 5)
    annotate_bars(ax, bars_pq, pq_sizes, 5)

    out = os.path.join(base_dir, "F6_1a_space_cost.pdf")
    fig.savefig(out)
    plt.close(fig)
    print(f"  [ok] {out}")


def plot_tag_filter(base_dir, grouped):
    ts_lat = [latency_ms(get_row(grouped, d, "tag_filter", "tsfile", None))
              for d in DATASETS]
    pq_lat = [latency_ms(get_row(grouped, d, "tag_filter", "parquet", None))
              for d in DATASETS]
    labels = dataset_labels()

    fig, ax = plt.subplots(figsize=(7, 4.5))
    x, bars_ts, bars_pq = plot_dual_bar(
        ax, ts_lat, pq_lat, labels, "查询延迟（ms）",
        "标签过滤 - 单设备查询"
    )
    ax.set_ylim(0, max(pq_lat) * 1.4)
    annotate_bars(ax, bars_ts, ts_lat, 0.5)
    annotate_bars(ax, bars_pq, pq_lat, 0.5)
    for i, (ts_val, pq_val) in enumerate(zip(ts_lat, pq_lat)):
        speedup = pq_val / ts_val if ts_val > 0 else 0
        ax.text(
            x[i], max(pq_lat) * 1.2, f"{speedup:.1f}x",
            ha="center", va="bottom", fontsize=11,
            fontweight="bold", color="#1565C0"
        )

    out = os.path.join(base_dir, "F6_1b_tag_filter.pdf")
    fig.savefig(out)
    plt.close(fig)
    print(f"  [ok] {out}")


def plot_full_scan(base_dir, grouped):
    ts_tp = [throughput_mrows(get_row(grouped, d, "full_scan", "tsfile"))
             for d in DATASETS]
    pq_tp = [throughput_mrows(get_row(grouped, d, "full_scan", "parquet"))
             for d in DATASETS]
    labels = [
        f"{DS_META[d]['label']}\n({DS_META[d]['devices']} dev)" for d in DATASETS
    ]

    fig, ax = plt.subplots(figsize=(7, 4.5))
    _, bars_ts, bars_pq = plot_dual_bar(
        ax, ts_tp, pq_tp, labels, "吞吐量（百万行/秒）",
        "全扫描吞吐量"
    )
    ax.set_ylim(0, max(ts_tp + pq_tp) * 1.3)
    annotate_bars(ax, bars_ts, ts_tp, 0.3)
    annotate_bars(ax, bars_pq, pq_tp, 0.3)

    out = os.path.join(base_dir, "F6_1c_full_scan.pdf")
    fig.savefig(out)
    plt.close(fig)
    print(f"  [ok] {out}")


def plot_time_filter(base_dir, grouped):
    sels = ["10%", "25%", "50%"]

    fig, axes = plt.subplots(1, 3, figsize=(14, 4.5), sharey=False)

    for idx, ds in enumerate(DATASETS):
        meta = DS_META[ds]
        ts_lat = [latency_ms(get_row(grouped, ds, "time_filter", "tsfile", s))
                  for s in sels]
        pq_lat = [latency_ms(get_row(grouped, ds, "time_filter", "parquet", s))
                  for s in sels]

        ax = axes[idx]
        x, bars_ts, bars_pq = plot_dual_bar(
            ax, ts_lat, pq_lat, sels, "延迟（ms）",
            f"{meta['label']} ({meta['devices']} dev)"
        )
        ax.set_xlabel("时间选择性")
        ymax = max(ts_lat + pq_lat)
        ax.set_ylim(0, ymax * 1.5 if ymax > 0 else 1)
        annotate_bars(ax, bars_ts, ts_lat, ymax * 0.03, fmt="{:.0f}")
        annotate_bars(ax, bars_pq, pq_lat, ymax * 0.03, fmt="{:.0f}")
        for i, (ts_val, pq_val) in enumerate(zip(ts_lat, pq_lat)):
            if ts_val > 0 and pq_val > 0:
                ratio = pq_val / ts_val
                if ratio >= 1:
                    label = f"{ratio:.1f}x"
                    color = "#1565C0"
                else:
                    label = f"Pq {1/ratio:.1f}x"
                    color = "#E65100"
                ax.text(
                    x[i], ymax * 1.25, label,
                    ha="center", fontsize=10, fontweight="bold", color=color
                )

    fig.suptitle(
        "时间过滤延迟",
        fontsize=13, fontweight="bold"
    )
    plt.tight_layout()

    out = os.path.join(base_dir, "F6_1d_time_filter.pdf")
    fig.savefig(out)
    plt.close(fig)
    print(f"  [ok] {out}")


def plot_tag_time_filter(base_dir, grouped):
    """Tag + Time Filter latency across selectivities for each dataset."""
    sels = ["10%", "25%", "50%"]

    # Collect latency for each dataset × selectivity.
    # params format is e.g. "truck_0+10%"; match by suffix.
    def find_tag_time(dataset, engine, sel):
        for (ds, exp, eng, prm), item in grouped.items():
            if ds == dataset and exp == "tag_time_filter" and eng == engine:
                if prm.endswith("+" + sel):
                    return item
        return None

    fig, axes = plt.subplots(1, 3, figsize=(14, 4.5), sharey=False)

    for idx, ds in enumerate(DATASETS):
        meta = DS_META[ds]
        ts_lat = []
        pq_lat = []
        for sel in sels:
            ts_row = find_tag_time(ds, "tsfile", sel)
            pq_row = find_tag_time(ds, "parquet", sel)
            ts_lat.append(latency_ms(ts_row) if ts_row else 0)
            pq_lat.append(latency_ms(pq_row) if pq_row else 0)

        ax = axes[idx]
        x, bars_ts, bars_pq = plot_dual_bar(
            ax, ts_lat, pq_lat, sels, "延迟（ms）",
            f"{meta['label']} ({meta['devices']} dev)"
        )
        ymax = max(ts_lat + pq_lat)
        ax.set_ylim(0, ymax * 1.5 if ymax > 0 else 1)
        ax.set_xlabel("时间选择性")
        annotate_bars(ax, bars_ts, ts_lat, ymax * 0.03, fmt="{:.1f}")
        annotate_bars(ax, bars_pq, pq_lat, ymax * 0.03, fmt="{:.1f}")
        for i, (ts_val, pq_val) in enumerate(zip(ts_lat, pq_lat)):
            if ts_val > 0 and pq_val > 0:
                ratio = pq_val / ts_val
                if ratio >= 1:
                    label = f"{ratio:.1f}x"
                    color = "#1565C0"
                else:
                    label = f"Pq {1/ratio:.1f}x"
                    color = "#E65100"
                ax.text(
                    x[i], ymax * 1.25, label,
                    ha="center", fontsize=10, fontweight="bold", color=color
                )

    fig.suptitle(
        "标签+时间过滤延迟（单设备+时间窗口）",
        fontsize=13, fontweight="bold"
    )
    plt.tight_layout()

    out = os.path.join(base_dir, "F6_1f_tag_time_filter.pdf")
    fig.savefig(out)
    plt.close(fig)
    print(f"  [ok] {out}")


def plot_write_throughput(base_dir, grouped):
    ts_tp = [throughput_mrows(get_row(grouped, d, "write", "tsfile"))
             for d in DATASETS]
    pq_tp = [throughput_mrows(get_row(grouped, d, "write", "parquet"))
             for d in DATASETS]
    labels = dataset_labels()

    fig, ax = plt.subplots(figsize=(7, 4.5))
    x, bars_ts, bars_pq = plot_dual_bar(
        ax, ts_tp, pq_tp, labels, "吞吐量（百万行/秒）",
        "写入吞吐量"
    )
    ax.set_ylim(0, max(ts_tp + pq_tp) * 1.35)
    annotate_bars(ax, bars_ts, ts_tp, 0.15)
    annotate_bars(ax, bars_pq, pq_tp, 0.15)
    for i, (ts_val, pq_val) in enumerate(zip(ts_tp, pq_tp)):
        ratio = pq_val / ts_val if ts_val > 0 else 0
        ax.text(
            x[i], max(ts_tp + pq_tp) * 1.18, f"Pq {ratio:.1f}x",
            ha="center", fontsize=10, fontweight="bold", color="#E65100"
        )

    out = os.path.join(base_dir, "F6_1e_write_throughput.pdf")
    fig.savefig(out)
    plt.close(fig)
    print(f"  [ok] {out}")


def plot_summary(base_dir, grouped):
    fig, axes = plt.subplots(2, 2, figsize=(11, 9))

    ts_space = [size_mb(get_row(grouped, d, "space_bytes", "tsfile"))
                for d in DATASETS]
    pq_space = [size_mb(get_row(grouped, d, "space_bytes", "parquet"))
                for d in DATASETS]
    ts_tag = [latency_ms(get_row(grouped, d, "tag_filter", "tsfile", None))
              for d in DATASETS]
    pq_tag = [latency_ms(get_row(grouped, d, "tag_filter", "parquet", None))
              for d in DATASETS]
    ts_full = [throughput_mrows(get_row(grouped, d, "full_scan", "tsfile"))
               for d in DATASETS]
    pq_full = [throughput_mrows(get_row(grouped, d, "full_scan", "parquet"))
               for d in DATASETS]
    sels = ["10%", "25%", "50%"]
    ts_time = [throughput_mrows(get_row(grouped, "tsbs", "time_filter", "tsfile", s))
               for s in sels]
    pq_time = [throughput_mrows(get_row(grouped, "tsbs", "time_filter", "parquet", s))
               for s in sels]

    labels = dataset_labels()
    labels_with_dev = [
        f"{DS_META[d]['label']}\n({DS_META[d]['devices']} dev)" for d in DATASETS
    ]

    # (0,0) Space Cost
    _, bars_ts, bars_pq = plot_dual_bar(
        axes[0][0], ts_space, pq_space, labels,
        "文件大小（MB）", "空间开销"
    )
    axes[0][0].set_ylim(0, max(ts_space + pq_space) * 1.3)
    annotate_bars(axes[0][0], bars_ts, ts_space, 5, fmt="{:.0f}")
    annotate_bars(axes[0][0], bars_pq, pq_space, 5, fmt="{:.0f}")

    # (0,1) Tag Filter Latency
    x, bars_ts, bars_pq = plot_dual_bar(
        axes[0][1], ts_tag, pq_tag, labels,
        "查询延迟（ms）", "标签过滤延迟"
    )
    axes[0][1].set_ylim(0, max(pq_tag) * 1.45)
    annotate_bars(axes[0][1], bars_ts, ts_tag, 0.3, bold=False)
    annotate_bars(axes[0][1], bars_pq, pq_tag, 0.3, bold=False)
    for i, (ts_val, pq_val) in enumerate(zip(ts_tag, pq_tag)):
        speedup = pq_val / ts_val if ts_val > 0 else 0
        axes[0][1].text(
            x[i], max(pq_tag) * 1.22, f"{speedup:.1f}x",
            ha="center", fontsize=10, fontweight="bold", color="#1565C0"
        )

    # (1,0) Full Scan Throughput
    _, bars_ts, bars_pq = plot_dual_bar(
        axes[1][0], ts_full, pq_full, labels_with_dev,
        "吞吐量（百万行/秒）", "全扫描吞吐量"
    )
    axes[1][0].set_ylim(0, max(ts_full + pq_full) * 1.3)
    annotate_bars(axes[1][0], bars_ts, ts_full, 0.3)
    annotate_bars(axes[1][0], bars_pq, pq_full, 0.3)

    # (1,1) Time Filter (TSBS)
    x2, bars_ts, bars_pq = plot_dual_bar(
        axes[1][1], ts_time, pq_time, sels,
        "吞吐量（百万行/秒）",
        "时间过滤（TSBS, 100 devices）"
    )
    axes[1][1].set_xlabel("时间选择性")
    axes[1][1].set_ylim(0, max(ts_time + pq_time) * 1.35)
    annotate_bars(axes[1][1], bars_ts, ts_time, 0.2)
    annotate_bars(axes[1][1], bars_pq, pq_time, 0.2)
    for i, (ts_val, pq_val) in enumerate(zip(ts_time, pq_time)):
        ratio = ts_val / pq_val if pq_val > 0 else 0
        label = f"TsFile {ratio:.1f}x" if ratio > 1 else f"Pq {pq_val / ts_val:.1f}x"
        color = "#1565C0" if ratio > 1 else "#E65100"
        axes[1][1].text(
            x2[i], max(ts_time + pq_time) * 1.16, label,
            ha="center", fontsize=9, fontweight="bold", color=color
        )

    plt.tight_layout()

    out = os.path.join(base_dir, "F6_1_summary.pdf")
    fig.savefig(out)
    plt.close(fig)
    print(f"  [ok] {out}")


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "."
    CSV_PATH_USED = resolve_csv_path(target)
    OUTPUT_DIR = os.path.dirname(CSV_PATH_USED) or "."
    print(f"Plotting results from {CSV_PATH_USED}")
    rows = read_csv(CSV_PATH_USED)
    grouped = group_rows(rows)
    require_rows(grouped)
    plot_space_cost(OUTPUT_DIR, grouped)
    plot_tag_filter(OUTPUT_DIR, grouped)
    plot_full_scan(OUTPUT_DIR, grouped)
    plot_time_filter(OUTPUT_DIR, grouped)
    plot_tag_time_filter(OUTPUT_DIR, grouped)
    plot_write_throughput(OUTPUT_DIR, grouped)
    plot_summary(OUTPUT_DIR, grouped)
    print("Done!")
