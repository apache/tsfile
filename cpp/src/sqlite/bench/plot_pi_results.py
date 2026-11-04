#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""Generate the figures used by PI_REPORT.md from the archived JSON results."""

import gzip
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.patheffects as pe
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.lines import Line2D

ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "results" / "pi-20261103"
FIGURES = ROOT / "figures"

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
TARGET = "#d03b3b"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a"]
BLUE_LIGHT = "#5598e7"
BLUE_DARK = "#1c5cab"

plt.rcParams.update(
    {
        "font.family": "sans-serif",
        "font.sans-serif": [
            "Hiragino Sans GB",
            "PingFang HK",
            "Arial Unicode MS",
            "DejaVu Sans",
        ],
        "axes.unicode_minus": False,
        "figure.facecolor": SURFACE,
        "axes.facecolor": SURFACE,
        "savefig.facecolor": SURFACE,
        "text.color": INK,
        "axes.labelcolor": INK2,
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "svg.fonttype": "path",
    }
)


def load_runs():
    runs = []
    for path in sorted(RESULTS.glob("*.json.gz")):
        with gzip.open(path, "rt") as f:
            data = json.load(f)
        data["_name"] = path.name
        runs.append(data)
    return runs


def cfg(run):
    c = run["config"]
    return c["devices"], c["measurements"], c["seal_rows"]


def effective_rows(run):
    return run["rows_written"] / run["wall_s"]


def effective_points(run):
    return run["points_written"] / run["wall_s"]


def style(ax, grid_axis="y"):
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(AXIS)
        ax.spines[side].set_linewidth(0.8)
    ax.tick_params(colors=MUTED, labelsize=9, length=0)
    for label in ax.get_xticklabels() + ax.get_yticklabels():
        label.set_color(INK2)
    if grid_axis:
        ax.grid(axis=grid_axis, color=GRID, linewidth=0.8, linestyle="-")
    ax.set_axisbelow(True)


def label(ax, x, y, text, dx=8, dy=0, color=INK2, size=8.5, ha="left"):
    ax.annotate(
        text,
        xy=(x, y),
        xytext=(dx, dy),
        textcoords="offset points",
        color=color,
        fontsize=size,
        ha=ha,
        va="center",
        path_effects=[pe.withStroke(linewidth=2.2, foreground=SURFACE)],
    )


def save(fig, name):
    FIGURES.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIGURES / f"{name}.png", dpi=220, bbox_inches="tight")
    svg = FIGURES / f"{name}.svg"
    fig.savefig(svg, bbox_inches="tight")
    svg.write_text(
        "\n".join(line.rstrip() for line in svg.read_text().splitlines()) + "\n"
    )
    plt.close(fig)


def fig_throughput(main):
    by = {(d, m): r for r in main for d, m, _ in [cfg(r)]}
    devices = [100, 500, 2000]
    fig, axes = plt.subplots(1, 2, figsize=(11.2, 4.3))
    for ax, metric, ylabel, title in (
        (axes[0], "rows", "纯写行/秒", "行吞吐"),
        (axes[1], "points", "纯写点/秒", "点吞吐"),
    ):
        for i, m in enumerate((5, 15, 30)):
            ys = []
            for d in devices:
                r = by[(d, m)]
                ys.append(
                    r["write_rows_per_s"]
                    if metric == "rows"
                    else r["write_points_per_s"]
                )
            ax.plot(
                range(3),
                ys,
                color=SERIES[i],
                linewidth=2,
                marker="o",
                markersize=6,
                markerfacecolor=SERIES[i],
                markeredgecolor=SURFACE,
                markeredgewidth=2,
                label=f"M{m}",
                zorder=3,
            )
            label(ax, 2, ys[-1], f"M{m} {ys[-1]:,.0f}")
        ax.set_xticks(range(3))
        ax.set_xticklabels([f"{d:,}" for d in devices])
        ax.set_xlim(-0.1, 2.55)
        ax.set_ylim(0, max(ax.get_ylim()[1] * 1.08, 1000))
        ax.set_xlabel("设备数")
        ax.set_ylabel(ylabel)
        ax.set_title(title, loc="left", color=INK, fontsize=11, pad=8)
        style(ax)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper right",
        bbox_to_anchor=(0.99, 1.02),
        ncol=3,
        frameon=False,
        handlelength=2.4,
    )
    fig.suptitle(
        "设备数与测点数对写入吞吐的影响",
        x=0.01,
        y=1.03,
        ha="left",
        color=INK,
        fontsize=13,
    )
    save(fig, "fig1_throughput")


def fig_seal_overhead(main):
    rows = sorted(main, key=lambda r: (cfg(r)[0], cfg(r)[1]))
    fig, ax = plt.subplots(figsize=(9.8, 4.8))
    for y, r in enumerate(rows):
        pure = r["write_rows_per_s"]
        eff = effective_rows(r)
        drop = (pure - eff) / pure * 100
        ax.plot([eff, pure], [y, y], color=AXIS, linewidth=1.2, zorder=1)
        ax.scatter(
            [eff],
            [y],
            s=70,
            color=BLUE_LIGHT,
            edgecolor=SURFACE,
            linewidth=2,
            zorder=3,
        )
        ax.scatter(
            [pure],
            [y],
            s=70,
            color=BLUE_DARK,
            edgecolor=SURFACE,
            linewidth=2,
            zorder=3,
        )
        label(ax, pure, y, f"-{drop:.1f}%", dx=10)
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels([f"D{cfg(r)[0]} · M{cfg(r)[1]}" for r in rows])
    ax.invert_yaxis()
    ax.set_xlim(0, 8800)
    ax.set_xlabel("行/秒")
    ax.set_title(
        "seal 使有效写入速率下降 14.5%–35.6%",
        loc="left",
        color=INK,
        fontsize=12,
        pad=10,
    )
    style(ax, grid_axis="x")
    ax.set_xticks([0, 2000, 4000, 6000, 8000])
    ax.set_xticklabels(["0", "2K", "4K", "6K", "8K"])
    handles = [
        Line2D(
            [0],
            [0],
            marker="o",
            color="none",
            markerfacecolor=BLUE_LIGHT,
            markeredgecolor=SURFACE,
            markeredgewidth=2,
            markersize=8,
            label="有效（含 seal）",
        ),
        Line2D(
            [0],
            [0],
            marker="o",
            color="none",
            markerfacecolor=BLUE_DARK,
            markeredgecolor=SURFACE,
            markeredgewidth=2,
            markersize=8,
            label="纯写（仅 INSERT）",
        ),
    ]
    ax.legend(
        handles=handles,
        loc="lower right",
        frameon=False,
        ncol=2,
        handletextpad=0.4,
        columnspacing=1.2,
    )
    save(fig, "fig2_seal_overhead")


def fig_peak_rss(main):
    by = {(d, m): r for r in main for d, m, _ in [cfg(r)]}
    devices = [100, 500, 2000]
    fig, ax = plt.subplots(figsize=(9.2, 4.6))
    for i, m in enumerate((5, 15, 30)):
        ys = [by[(d, m)]["rss"]["peak_kb"] / 1024 for d in devices]
        ax.plot(
            range(3),
            ys,
            color=SERIES[i],
            linewidth=2,
            marker="o",
            markersize=6,
            markerfacecolor=SERIES[i],
            markeredgecolor=SURFACE,
            markeredgewidth=2,
            label=f"M{m}",
            zorder=3,
        )
        label(ax, 2, ys[-1], f"M{m} {ys[-1]:.0f}MB")
    ax.axhline(500, color=TARGET, linewidth=1, linestyle=(0, (4, 4)), zorder=2)
    ax.text(
        0.02,
        500,
        "500MB 目标",
        transform=ax.get_yaxis_transform(),
        color=INK2,
        fontsize=8.5,
        va="bottom",
    )
    ax.set_xticks(range(3))
    ax.set_xticklabels([f"{d:,}" for d in devices])
    ax.set_xlim(-0.1, 2.55)
    ax.set_ylim(0, 600)
    ax.set_xlabel("设备数")
    ax.set_ylabel("进程峰值 RSS（MB）")
    ax.set_title(
        "M30 的内存峰值随设备数明显抬升",
        loc="left",
        color=INK,
        fontsize=12,
        pad=10,
    )
    style(ax)
    ax.legend(loc="upper left", frameon=False, ncol=3, handlelength=2.4)
    save(fig, "fig3_peak_rss")


def timeline(run):
    samples = run["rss_samples"]
    times = [0.0] + [s[0] for s in samples]
    values = [samples[0][1]] + [s[1] for s in samples]
    return np.asarray(times), np.asarray(values) / 1024


def fig_rss_timeline(main, supp):
    by = {(d, m): r for r in main for d, m, _ in [cfg(r)]}
    fig, axes = plt.subplots(1, 2, figsize=(12.2, 4.4))
    ax = axes[0]
    t, rss = timeline(by[(100, 30)])
    mask = t <= 40
    ax.plot(t[mask], rss[mask], color=SERIES[0], linewidth=2)
    for tx, ty, text, dx, dy in (
        (13.5, 256.5, "第一个 seal 峰值 256MB", 10, 18),
        (31.9, 385.8, "第二个 seal 峰值 386MB", 10, 18),
    ):
        ax.scatter(
            [tx],
            [ty],
            s=42,
            color=SERIES[0],
            edgecolor=SURFACE,
            linewidth=2,
            zorder=4,
        )
        ax.annotate(
            text,
            xy=(tx, ty),
            xytext=(dx, dy),
            textcoords="offset points",
            color=INK2,
            fontsize=8.5,
            arrowprops=dict(arrowstyle="-", color=AXIS, linewidth=0.8),
            path_effects=[pe.withStroke(linewidth=2.2, foreground=SURFACE)],
        )
    ax.set_xlim(0, 40)
    ax.set_ylim(0, 450)
    ax.set_xlabel("运行时间（s）")
    ax.set_ylabel("RSS（MB）")
    ax.set_title("D100-M30：前 40 秒", loc="left", color=INK, fontsize=11)
    style(ax)

    ax = axes[1]
    for i, (run, name) in enumerate(
        (
            (by[(100, 30)], "D100-M30"),
            (supp, "D2000-M30 / 30k"),
            (by[(500, 15)], "D500-M15"),
        )
    ):
        peaks = [s["rss_peak_kb"] / 1024 for s in run["seals"]]
        xs = list(range(1, len(peaks) + 1))
        ax.plot(
            xs,
            peaks,
            color=SERIES[i],
            linewidth=2,
            label=name,
            zorder=3,
        )
        ax.scatter(
            [xs[-1]],
            [peaks[-1]],
            s=42,
            color=SERIES[i],
            edgecolor=SURFACE,
            linewidth=2,
            zorder=4,
        )
        label(
            ax,
            xs[-1],
            peaks[-1],
            f"{name} {int(run['rss']['peak_kb'] / 1024 + 0.5)}MB",
        )
    ax.set_xlim(
        0, max(len(r["seals"]) for r in (by[(100, 30)], supp, by[(500, 15)])) * 1.32
    )
    ax.set_ylim(0, 450)
    ax.set_xlabel("seal 次序")
    ax.set_ylabel("单次 seal 峰值 RSS（MB）")
    ax.set_title("每次 seal 的峰值", loc="left", color=INK, fontsize=11)
    style(ax)
    ax.legend(loc="lower left", frameon=False, handlelength=2.4)
    fig.suptitle(
        "RSS 在 seal 时跳升，而不是随写入行数线性增长",
        x=0.01,
        y=1.03,
        ha="left",
        color=INK,
        fontsize=13,
    )
    save(fig, "fig4_rss_timeline")


def fig_seal_scan(scan):
    scan = sorted(scan, key=lambda r: cfg(r)[2])
    xs = list(range(len(scan)))
    xlabels = [f"{cfg(r)[2] // 1000}k" for r in scan]
    fig, axes = plt.subplots(1, 3, figsize=(12.6, 3.9))
    panels = (
        (
            axes[0],
            [r["rss"]["peak_kb"] / 1024 for r in scan],
            "峰值 RSS（MB）",
            "内存峰值",
        ),
        (
            axes[1],
            [effective_rows(r) for r in scan],
            "有效行/秒",
            "有效写入吞吐",
        ),
        (
            axes[2],
            [r["seal_s"] for r in scan],
            "seal 总时间（s）",
            "seal 总时间",
        ),
    )
    for ax, ys, ylabel, title in panels:
        ax.plot(
            xs,
            ys,
            color=SERIES[0],
            linewidth=2,
            marker="o",
            markersize=6,
            markerfacecolor=SERIES[0],
            markeredgecolor=SURFACE,
            markeredgewidth=2,
            zorder=3,
        )
        for x, y in ((xs[0], ys[0]), (xs[-1], ys[-1])):
            label(ax, x, y, f"{y:,.0f}", dx=0, dy=10, ha="center")
        ax.set_xticks(xs)
        ax.set_xticklabels(xlabels)
        ax.set_xlim(-0.2, 2.2)
        ax.set_ylim(0, max(ys) * 1.22)
        ax.set_xlabel("seal 触发行数")
        ax.set_ylabel(ylabel)
        ax.set_title(title, loc="left", color=INK, fontsize=11)
        style(ax)
    axes[0].axhline(500, color=TARGET, linewidth=1, linestyle=(0, (4, 4)), zorder=2)
    axes[0].text(
        0.02,
        500,
        "500MB 目标",
        transform=axes[0].get_yaxis_transform(),
        color=INK2,
        fontsize=8,
        va="bottom",
    )
    fig.suptitle(
        "seal 触发量扫描：D500-M15，2,000,000 行",
        x=0.01,
        y=1.04,
        ha="left",
        color=INK,
        fontsize=13,
    )
    save(fig, "fig6_seal_threshold")


def fig_d2000_tradeoff(main, supp):
    by = {(d, m): r for r in main for d, m, _ in [cfg(r)]}
    runs = [supp, by[(2000, 30)]]
    fig, axes = plt.subplots(1, 3, figsize=(12.2, 3.9))
    metrics = (
        (
            axes[0],
            [r["rss"]["peak_kb"] / 1024 for r in runs],
            "峰值 RSS（MB）",
            "内存峰值",
        ),
        (axes[1], [r["seal_s"] for r in runs], "seal 总时间（s）", "seal 总时间"),
        (
            axes[2],
            [effective_points(r) for r in runs],
            "有效点/秒",
            "有效点吞吐",
        ),
    )
    for ax, ys, ylabel, title in metrics:
        for x, (y, run) in enumerate(zip(ys, runs)):
            color = SERIES[x]
            ax.scatter(
                [x],
                [y],
                s=80,
                color=color,
                edgecolor=SURFACE,
                linewidth=2,
                zorder=3,
            )
            label(ax, x, y, f"{y:,.0f}", dx=0, dy=12, ha="center")
        ax.plot([0, 1], ys, color=AXIS, linewidth=1.2, zorder=1)
        ax.set_xticks([0, 1])
        ax.set_xticklabels(["30k", "50k"])
        ax.set_xlim(-0.3, 1.3)
        ax.set_ylim(0, max(ys) * 1.32)
        ax.set_xlabel("seal 触发行数")
        ax.set_ylabel(ylabel)
        ax.set_title(title, loc="left", color=INK, fontsize=11)
        style(ax)
    handles = [
        Line2D(
            [0],
            [0],
            marker="o",
            color="none",
            markerfacecolor=SERIES[0],
            markeredgecolor=SURFACE,
            markeredgewidth=2,
            markersize=8,
            label="30k seal",
        ),
        Line2D(
            [0],
            [0],
            marker="o",
            color="none",
            markerfacecolor=SERIES[1],
            markeredgecolor=SURFACE,
            markeredgewidth=2,
            markersize=8,
            label="50k seal",
        ),
    ]
    fig.legend(
        handles=handles,
        loc="upper right",
        bbox_to_anchor=(0.99, 1.02),
        ncol=2,
        frameon=False,
    )
    fig.suptitle(
        "D2000-M30：30k seal 用更多 seal 次数换取更低内存",
        x=0.01,
        y=1.04,
        ha="left",
        color=INK,
        fontsize=13,
    )
    save(fig, "fig7_d2000_tradeoff")


def fig_seal_rss_cycle(main, supp):
    by = {(d, m): r for r in main for d, m, _ in [cfg(r)]}
    fig, axes = plt.subplots(1, 2, figsize=(11.6, 4.4))
    panels = (
        (axes[0], by[(100, 30)], "D100-M30 / 50k"),
        (axes[1], supp, "D2000-M30 / 30k"),
    )
    for ax, run, title in panels:
        before = [s["rss_before_kb"] / 1024 for s in run["seals"]]
        peak = [s["rss_peak_kb"] / 1024 for s in run["seals"]]
        xs = list(range(1, len(peak) + 1))
        ax.plot(xs, before, color=BLUE_LIGHT, linewidth=2, label="seal 前")
        ax.plot(xs, peak, color=BLUE_DARK, linewidth=2, label="seal 峰值")
        label(ax, xs[-1], before[-1], f"{before[-1]:.0f}MB")
        label(ax, xs[-1], peak[-1], f"{peak[-1]:.0f}MB")
        ax.set_xlim(0, max(xs) * 1.32)
        ax.set_ylim(0, 450)
        ax.set_xlabel("seal 次序")
        ax.set_ylabel("RSS（MB）")
        ax.set_title(title, loc="left", color=INK, fontsize=11)
        style(ax)
        ax.legend(loc="lower right", frameon=False, handlelength=2.4)
    fig.suptitle(
        "seal 前 RSS 与 seal 峰值：是否回落取决于配置",
        x=0.01,
        y=1.03,
        ha="left",
        color=INK,
        fontsize=13,
    )
    save(fig, "fig5_seal_rss_cycle")


def main():
    runs = load_runs()
    main_runs = [r for r in runs if "main" in r["_name"] and cfg(r)[2] == 50000]
    scan = [r for r in runs if "scan" in r["_name"]]
    supp = [r for r in runs if "supp" in r["_name"]][0]
    fig_throughput(main_runs)
    fig_seal_overhead(main_runs)
    fig_peak_rss(main_runs)
    fig_rss_timeline(main_runs, supp)
    fig_seal_scan(scan)
    fig_d2000_tradeoff(main_runs, supp)
    fig_seal_rss_cycle(main_runs, supp)
    print(
        f"wrote {len(list(FIGURES.glob('*.png')))} PNG and {len(list(FIGURES.glob('*.svg')))} SVG figures to {FIGURES}"
    )


if __name__ == "__main__":
    main()
