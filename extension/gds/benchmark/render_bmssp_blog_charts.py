#!/usr/bin/env python3
"""Render the original figures used by the NeuG BMSSP technical blog."""

import csv
import math
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.patches import Circle, FancyArrowPatch, FancyBboxPatch


HERE = Path(__file__).resolve().parent
OUT = HERE / "images" / "bmssp"
RESULTS = HERE / "bmssp_results.csv"

NAVY = "#13213c"
BLUE = "#1769e0"
CYAN = "#12a8b4"
ORANGE = "#f28e2b"
GREEN = "#2f9e67"
RED = "#d9534f"
PURPLE = "#7655d5"
MUTED = "#667085"
GRID = "#d9e1ec"
PALE_BLUE = "#eaf2ff"
PALE_GREEN = "#eaf8f0"
PALE_ORANGE = "#fff2e3"
WHITE = "#ffffff"


def configure_fonts():
    candidates = [
        "PingFang SC",
        "Hiragino Sans GB",
        "Heiti SC",
        "Arial Unicode MS",
        "DejaVu Sans",
    ]
    installed = {font.name for font in font_manager.fontManager.ttflist}
    family = next((name for name in candidates if name in installed), "DejaVu Sans")
    plt.rcParams.update(
        {
            "font.family": family,
            "axes.unicode_minus": False,
            "font.size": 12,
            "axes.titleweight": "bold",
            "axes.titlesize": 18,
            "figure.facecolor": WHITE,
            "axes.facecolor": WHITE,
            "text.color": NAVY,
            "axes.labelcolor": NAVY,
            "xtick.color": MUTED,
            "ytick.color": MUTED,
            "savefig.facecolor": WHITE,
        }
    )


def save(fig, name):
    OUT.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT / (name + ".png"), dpi=200, bbox_inches="tight")
    fig.savefig(OUT / (name + ".svg"), bbox_inches="tight")
    plt.close(fig)


def rounded(ax, xy, width, height, text, face, edge="none", size=12):
    box = FancyBboxPatch(
        xy,
        width,
        height,
        boxstyle="round,pad=0.02,rounding_size=0.04",
        linewidth=1.4,
        edgecolor=edge,
        facecolor=face,
    )
    ax.add_patch(box)
    ax.text(
        xy[0] + width / 2,
        xy[1] + height / 2,
        text,
        ha="center",
        va="center",
        fontsize=size,
        color=NAVY,
    )
    return box


def arrow(ax, start, end, color=MUTED, width=1.6, style="-|>"):
    ax.add_patch(
        FancyArrowPatch(
            start,
            end,
            arrowstyle=style,
            mutation_scale=14,
            linewidth=width,
            color=color,
            connectionstyle="arc3,rad=0",
        )
    )


def history():
    fig, ax = plt.subplots(figsize=(12, 4.3))
    ax.set_xlim(-0.5, 4.5)
    ax.set_ylim(-1.35, 1.35)
    ax.axis("off")
    ax.set_title("从 Dijkstra 到突破排序障碍：一条跨越近 70 年的时间线", pad=18)
    ax.hlines(0, 0, 4, color=GRID, linewidth=4)
    events = [
        (1956, "Dijkstra", "逐点按距离定序", BLUE, 0.68),
        (1984, "Fibonacci heap", "$O(m+n\\log n)$", PURPLE, -0.78),
        (2023, "无向图突破", "任意实数权重", CYAN, 0.68),
        (2025, "有向图突破", "$O(m\\log^{2/3}n)$\nSTOC 最佳论文", ORANGE, -0.78),
        (2026, "后续改进", "排序障碍之后仍在加速", GREEN, 0.68),
    ]
    for position, (year, title, detail, color, y) in enumerate(events):
        ax.vlines(position, 0, y * 0.73, color=color, linewidth=2)
        ax.scatter([position], [0], s=150, color=color, zorder=3, edgecolor=WHITE, linewidth=2)
        va = "bottom" if y > 0 else "top"
        ax.text(position, y, str(year), ha="center", va=va, color=color, fontweight="bold")
        dy = 0.18 if y > 0 else -0.18
        ax.text(position, y + dy, title, ha="center", va=va, color=NAVY, fontweight="bold")
        ax.text(position, y + 2 * dy, detail, ha="center", va=va, color=MUTED, fontsize=10)
    save(fig, "01-history")


def sorting_barrier():
    fig, axes = plt.subplots(1, 2, figsize=(12, 5.4))
    fig.suptitle("“排序障碍”究竟挡在哪里？", fontsize=19, fontweight="bold", y=0.98)
    for ax in axes:
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis("off")

    left, right = axes
    left.set_title("Dijkstra：维护一个全局有序前沿", fontsize=14, pad=12)
    source = (0.12, 0.52)
    left.add_patch(Circle(source, 0.055, color=BLUE))
    left.text(*source, "s", ha="center", va="center", color=WHITE, fontweight="bold")
    ys = [0.79, 0.66, 0.53, 0.40, 0.27]
    distances = ["1.2", "1.8", "2.6", "3.1", "4.7"]
    for i, (y, distance) in enumerate(zip(ys, distances), 1):
        arrow(left, (0.18, 0.52), (0.40, y), color=GRID, width=1.2)
        left.add_patch(Circle((0.44, y), 0.042, color=PALE_BLUE, ec=BLUE, lw=1.4))
        left.text(0.44, y, "v{}".format(i), ha="center", va="center", fontsize=10)
        rounded(left, (0.61, y - 0.038), 0.24, 0.076, distance, WHITE, GRID, 11)
    left.text(0.73, 0.89, "min-priority queue", ha="center", color=MUTED, fontsize=10)
    left.annotate(
        "每次取出最近顶点\n≈ 不断维护距离全序",
        xy=(0.73, 0.25),
        xytext=(0.73, 0.10),
        ha="center",
        color=RED,
        arrowprops={"arrowstyle": "->", "color": RED},
    )

    right.set_title(
        "BMSSP 思路：分层、分组，只维护必要的偏序", fontsize=14, pad=12
    )
    right.add_patch(Circle(source, 0.055, color=ORANGE))
    right.text(*source, "s", ha="center", va="center", color=WHITE, fontweight="bold")
    layer_specs = [
        (0.30, 0.13, 0.28, PALE_ORANGE, "局部探测\n少量 Bellman-Ford"),
        (0.56, 0.13, 0.28, PALE_BLUE, "选出 pivots\n代表性前沿"),
        (0.82, 0.13, 0.28, PALE_GREEN, "递归求解\n有界距离层"),
    ]
    prev = (0.18, 0.52)
    for x, y, h, face, label in layer_specs:
        rounded(right, (x, y), 0.18, h, label, face, "none", 11)
        arrow(right, prev, (x, y + h / 2), color=MUTED)
        prev = (x + 0.18, y + h / 2)
    for x, y, h, face, _ in layer_specs:
        for dy in (0.55, 0.67, 0.79):
            right.add_patch(Circle((x + 0.09, dy), 0.026, color=face, ec=ORANGE, lw=1))
    right.text(
        0.55,
        0.91,
        "不要求同一层中的顶点严格按距离一个个出队",
        ha="center",
        color=GREEN,
        fontweight="bold",
    )
    right.text(
        0.55,
        0.04,
        "少排一些序，换来更小的渐进复杂度",
        ha="center",
        color=NAVY,
        fontsize=12,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    save(fig, "02-sorting-barrier")


def adaptive_path():
    fig, ax = plt.subplots(figsize=(12, 4.8))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    ax.set_title("NeuG 的自适应 BMSSP：先走快路，必要时再进入递归分解", pad=18)

    rounded(
        ax,
        (0.03, 0.38),
        0.15,
        0.24,
        "NeuG 投影图\nCSR / 属性访问",
        PALE_BLUE,
        BLUE,
    )
    rounded(
        ax,
        (0.25, 0.38),
        0.20,
        0.24,
        "并行 sparse/dense\nfrontier probe\n最多 32 轮",
        PALE_ORANGE,
        ORANGE,
    )
    rounded(ax, (0.54, 0.64), 0.18, 0.20, "已收敛\n直接输出距离", PALE_GREEN, GREEN)
    rounded(ax, (0.54, 0.17), 0.18, 0.20, "未收敛\n丢弃 probe 标签", "#fff0f0", RED)
    rounded(
        ax,
        (0.79, 0.17),
        0.18,
        0.20,
        "构建紧凑 CSR\n递归 BMSSP\nfixed-point repair",
        "#f1edff",
        PURPLE,
    )
    rounded(
        ax,
        (0.79, 0.64),
        0.18,
        0.20,
        "sink 到查询结果\nnode + distance",
        PALE_GREEN,
        GREEN,
    )
    arrow(ax, (0.18, 0.50), (0.25, 0.50))
    arrow(ax, (0.45, 0.54), (0.54, 0.73), color=GREEN)
    arrow(ax, (0.45, 0.46), (0.54, 0.27), color=RED)
    arrow(ax, (0.72, 0.27), (0.79, 0.27), color=PURPLE)
    arrow(ax, (0.88, 0.37), (0.88, 0.64), color=PURPLE)
    arrow(ax, (0.72, 0.74), (0.79, 0.74), color=GREEN)
    ax.text(0.485, 0.69, "fast path", color=GREEN, fontsize=10, ha="center")
    ax.text(0.485, 0.27, "fallback", color=RED, fontsize=10, ha="center")
    ax.text(
        0.50,
        0.04,
        "两张 Graphalytics datagen 图都在 6 轮内完成，实测没有进入 fallback。",
        ha="center",
        color=MUTED,
        fontsize=11,
    )
    save(fig, "03-adaptive-path")


def load_results():
    with RESULTS.open(newline="") as stream:
        return list(csv.DictReader(stream))


def latency():
    rows = load_results()
    datasets = ["datagen-8_0-fb", "datagen-8_1-fb"]
    algorithms = ["bmssp", "frontier", "dijkstra"]
    colors = [ORANGE, BLUE, MUTED]
    values = {
        (row["dataset"], row["algorithm"]): float(row["median_seconds"])
        for row in rows
    }
    fig, ax = plt.subplots(figsize=(11, 5.6))
    x = range(len(datasets))
    width = 0.23
    for idx, (algo, color) in enumerate(zip(algorithms, colors)):
        offset = (idx - 1) * width
        bars = ax.bar(
            [position + offset for position in x],
            [values[(dataset, algo)] for dataset in datasets],
            width,
            label=algo.upper() if algo == "bmssp" else algo.title(),
            color=color,
        )
        for bar in bars:
            value = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                value * 1.12,
                "{:.3f}s".format(value),
                ha="center",
                va="bottom",
                fontsize=10,
                color=NAVY,
            )
    ax.set_yscale("log")
    ax.set_ylim(0.09, 6)
    ax.set_ylabel("中位耗时（秒，对数坐标）")
    ax.set_xticks(list(x), datasets)
    ax.set_title("BMSSP 在两张图上都领先 frontier，并显著快于 Dijkstra", pad=16)
    ax.grid(axis="y", color=GRID, linewidth=0.8, which="both")
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False, ncol=3, loc="upper left")
    fig.tight_layout()
    save(fig, "04-latency")


def speedup():
    rows = load_results()
    datasets = ["datagen-8_0-fb", "datagen-8_1-fb"]
    values = {
        (row["dataset"], row["algorithm"]): float(row["median_seconds"])
        for row in rows
    }
    frontier_gain = [
        (values[(d, "frontier")] - values[(d, "bmssp")])
        / values[(d, "frontier")]
        * 100
        for d in datasets
    ]
    dijkstra_speedup = [
        values[(d, "dijkstra")] / values[(d, "bmssp")] for d in datasets
    ]
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.7))
    fig.suptitle("两种基线、两个观察尺度", fontsize=18, fontweight="bold", y=1.02)
    bars = axes[0].barh(datasets, frontier_gain, color=ORANGE, height=0.46)
    axes[0].set_title("相对 frontier 的耗时下降")
    axes[0].set_xlabel("下降比例（%）")
    axes[0].set_xlim(0, 16)
    for bar, value in zip(bars, frontier_gain):
        axes[0].text(
            value + 0.35,
            bar.get_y() + bar.get_height() / 2,
            "{:.1f}%".format(value),
            va="center",
        )

    bars = axes[1].barh(datasets, dijkstra_speedup, color=GREEN, height=0.46)
    axes[1].set_title("相对 Dijkstra 的加速比")
    axes[1].set_xlabel("倍数（×）")
    axes[1].set_xlim(0, 15)
    for bar, value in zip(bars, dijkstra_speedup):
        axes[1].text(
            value + 0.25,
            bar.get_y() + bar.get_height() / 2,
            "{:.1f}×".format(value),
            va="center",
        )

    for ax in axes:
        ax.grid(axis="x", color=GRID, linewidth=0.8)
        ax.set_axisbelow(True)
        ax.spines[["top", "right", "left"]].set_visible(False)
        ax.tick_params(axis="y", length=0)
    fig.tight_layout()
    save(fig, "05-speedup")


def stability():
    rows = load_results()
    selected = [row for row in rows if row["dataset"] == "datagen-8_1-fb"]
    fig, ax = plt.subplots(figsize=(10.8, 5.1))
    mapping = {"bmssp": ORANGE, "frontier": BLUE, "dijkstra": MUTED}
    for row in selected:
        runs = [float(item) for item in row["run_seconds"].split(";")]
        color = mapping[row["algorithm"]]
        label = (
            row["algorithm"].upper()
            if row["algorithm"] == "bmssp"
            else row["algorithm"].title()
        )
        ax.plot(
            range(1, 6),
            runs,
            marker="o",
            color=color,
            linewidth=2.2,
            label=label,
        )
        median = float(row["median_seconds"])
        ax.hlines(median, 1, 5, color=color, linestyle="--", linewidth=1.2, alpha=0.8)
        ax.text(
            5.08,
            runs[-1],
            "{:.3f}s".format(runs[-1]),
            va="center",
            color=color,
            fontsize=10,
        )
    ax.set_yscale("log")
    ax.set_xticks(range(1, 6))
    ax.set_xlabel("计时轮次")
    ax.set_ylabel("耗时（秒，对数坐标）")
    ax.set_title("第二张图的五次运行：BMSSP 更快，也更稳定", pad=16)
    ax.grid(color=GRID, linewidth=0.8, which="both")
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False, ncol=3, loc="upper left")
    ax.text(
        0.01,
        0.03,
        "虚线为中位数；frontier 后两轮出现明显波动。",
        transform=ax.transAxes,
        color=MUTED,
        fontsize=10,
    )
    fig.tight_layout()
    save(fig, "06-run-stability")


def validation():
    fig, ax = plt.subplots(figsize=(11.5, 4.8))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    ax.set_title("正确性不是一句“结果一样”，而是四层证据链", pad=18)
    items = [
        ("官方小图", "27 项 Graphalytics\nconformance 全通过", BLUE),
        ("差分测试", "随机图覆盖零权边、平行边\n以及有向 / 无向模式", CYAN),
        ("fallback", "96 顶点长链\n强制越过 32 轮 probe", PURPLE),
        ("完整参考", "2,072,117 顶点逐点比对\n最大绝对误差 8.88e-16", GREEN),
    ]
    for index, (title, detail, color) in enumerate(items):
        x = 0.03 + index * 0.245
        rounded(ax, (x, 0.22), 0.21, 0.52, "", WHITE, GRID)
        ax.add_patch(Circle((x + 0.105, 0.63), 0.055, color=color))
        ax.text(
            x + 0.105,
            0.63,
            str(index + 1),
            ha="center",
            va="center",
            color=WHITE,
            fontsize=16,
            fontweight="bold",
        )
        ax.text(
            x + 0.105,
            0.49,
            title,
            ha="center",
            va="center",
            color=NAVY,
            fontweight="bold",
            fontsize=13,
        )
        ax.text(
            x + 0.105,
            0.34,
            detail,
            ha="center",
            va="center",
            color=MUTED,
            fontsize=9.5,
            linespacing=1.5,
        )
    ax.text(
        0.5,
        0.10,
        "BMSSP 串行/并行配置均与 Dijkstra 或官方参考结果对齐。",
        ha="center",
        color=NAVY,
        fontsize=11,
    )
    save(fig, "07-validation")


def main():
    configure_fonts()
    history()
    sorting_barrier()
    adaptive_path()
    latency()
    speedup()
    stability()
    validation()
    print("rendered figures to {}".format(OUT))


if __name__ == "__main__":
    main()
