#!/usr/bin/env python3
"""
生成 FlyWire 大脑模拟的可视化图表（使用预计算数据）

Prerequisites:
- Install matplotlib: pip install matplotlib numpy
"""

import os
import sys
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
import numpy as np

# 颜色方案
COLORS = {
    'sugar': '#2ecc71',      # 绿色 - 糖味
    'bitter': '#e74c3c',     # 红色 - 苦味
    'motor': '#3498db',      # 蓝色 - 运动神经元
    'neutral': '#95a5a6',    # 灰色 - 中性
    'highlight': '#f39c12',  # 橙色 - 高亮
}

# 预计算的模拟数据
SUGAR_ROUNDS = {0: 20, 1: 18, 2: 93, 3: 262, 4: 352, 5: 930, 6: 1417, 7: 3705, 
                8: 1641, 9: 608, 10: 388, 11: 172, 12: 77, 13: 218, 14: 254, 
                15: 69, 16: 40, 17: 16, 18: 12, 19: 2, 20: 1}

SUGAR_MOTOR_ROUNDS = {0: 0, 1: 1, 2: 19, 3: 75, 4: 61, 5: 112, 6: 70, 7: 49,
                      8: 21, 9: 26, 10: 6, 11: 1, 12: 4, 13: 2, 14: 2, 
                      15: 3, 16: 4, 17: 4, 18: 1, 19: 0, 20: 0}

BITTER_ROUNDS = {0: 20, 1: 16, 2: 32, 3: 81, 4: 76, 5: 170, 6: 563, 7: 805, 
                 8: 1376, 9: 2337, 10: 1491, 11: 1732, 12: 716, 13: 683, 
                 14: 167, 15: 56, 16: 11, 17: 10, 18: 4, 19: 2, 20: 3}

BITTER_MOTOR_ROUNDS = {0: 0, 1: 1, 2: 3, 3: 4, 4: 7, 5: 25, 6: 126, 7: 93,
                       8: 62, 9: 60, 10: 36, 11: 36, 12: 13, 13: 1, 
                       14: 1, 15: 0, 16: 0, 17: 0, 18: 0, 19: 1, 20: 0}


def plot_signal_propagation(output_dir):
    """Figure 1: Signal Propagation Process"""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    for rounds, color, label in [(SUGAR_ROUNDS, COLORS['sugar'], 'Sugar GRN'), 
                                   (BITTER_ROUNDS, COLORS['bitter'], 'Bitter GRN')]:
        x = sorted(rounds.keys())
        y = [rounds[r] for r in x]
        
        ax.plot(x, y, 'o-', color=color, linewidth=2.5, markersize=8, label=label)
        ax.fill_between(x, y, alpha=0.2, color=color)
    
    ax.set_xlabel('Propagation Round', fontsize=14)
    ax.set_ylabel('Neurons Spiked', fontsize=14)
    ax.set_title('Signal Propagation Through FlyWire Connectome', fontsize=16, fontweight='bold')
    ax.legend(fontsize=12, loc='upper right')
    ax.grid(True, alpha=0.3)
    ax.set_xlim(-0.5, 20.5)
    
    # Add peak annotation
    ax.annotate('Peak: 3,705 neurons', xy=(7, SUGAR_ROUNDS[7]), 
                xytext=(10, SUGAR_ROUNDS[7] + 500),
                fontsize=11, ha='center',
                arrowprops=dict(arrowstyle='->', color='gray'))
    
    plt.tight_layout()
    plt.savefig(output_dir / 'signal_propagation.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: signal_propagation.png")


def plot_cumulative_spikes(output_dir):
    """图2: 累积发放曲线"""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    for rounds, color, label in [(SUGAR_ROUNDS, COLORS['sugar'], 'Sugar GRN'), 
                                   (BITTER_ROUNDS, COLORS['bitter'], 'Bitter GRN')]:
        x = sorted(rounds.keys())
        cumulative = 0
        y_cumulative = []
        for r in x:
            cumulative += rounds[r]
            y_cumulative.append(cumulative)
        
        ax.plot(x, y_cumulative, 'o-', color=color, linewidth=2.5, markersize=8, label=label)
        ax.fill_between(x, y_cumulative, alpha=0.2, color=color)
    
    ax.set_xlabel('Propagation Round', fontsize=14)
    ax.set_ylabel('Cumulative Neurons Spiked', fontsize=14)
    ax.set_title('Cumulative Signal Spread Through Brain', fontsize=16, fontweight='bold')
    ax.legend(fontsize=12, loc='lower right')
    ax.grid(True, alpha=0.3)
    
    # 添加最终数值
    ax.axhline(y=10295, color=COLORS['sugar'], linestyle='--', alpha=0.5)
    ax.axhline(y=10351, color=COLORS['bitter'], linestyle='--', alpha=0.5)
    ax.text(20.5, 10295, '10,295', fontsize=11, va='center', color=COLORS['sugar'])
    ax.text(20.5, 10351, '10,351', fontsize=11, va='center', color=COLORS['bitter'])
    
    plt.tight_layout()
    plt.savefig(output_dir / 'cumulative_spikes.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: cumulative_spikes.png")


def plot_motor_neuron_tracking(output_dir):
    """图3: 运动神经元激活追踪"""
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    for idx, (rounds, motor_rounds, color, title) in enumerate([
        (SUGAR_ROUNDS, SUGAR_MOTOR_ROUNDS, COLORS['sugar'], 'Sugar GRN'),
        (BITTER_ROUNDS, BITTER_MOTOR_ROUNDS, COLORS['bitter'], 'Bitter GRN')
    ]):
        ax = axes[idx]
        
        x = sorted(rounds.keys())
        y_total = [rounds[r] for r in x]
        y_motor = [motor_rounds.get(r, 0) for r in x]
        y_other = [y_total[i] - y_motor[i] for i in range(len(x))]
        
        ax.bar(x, y_other, color=COLORS['neutral'], label='Interneurons', alpha=0.8)
        ax.bar(x, y_motor, bottom=y_other, color=COLORS['motor'], label='Motor Neurons', alpha=0.8)
        
        ax.set_xlabel('Propagation Round', fontsize=12)
        ax.set_ylabel('Neurons Spiked', fontsize=12)
        ax.set_title(f'{title} Stimulation', fontsize=14, fontweight='bold')
        ax.legend(fontsize=10, loc='upper right')
        ax.grid(True, alpha=0.3, axis='y')
    
    plt.suptitle('Motor Neuron Activation During Signal Propagation', fontsize=16, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(output_dir / 'motor_neuron_tracking.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: motor_neuron_tracking.png")


def plot_comparison(output_dir):
    """图4: 糖味 vs 苦味对比"""
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    
    categories = ['Sugar GRN', 'Bitter GRN']
    colors = [COLORS['sugar'], COLORS['bitter']]
    
    # 子图1: 总发放
    ax = axes[0]
    values = [10295, 10351]
    bars = ax.bar(categories, values, color=colors, alpha=0.8, edgecolor='black', linewidth=1.5)
    ax.set_ylabel('Total Neurons Spiked', fontsize=12)
    ax.set_title('Total Neural Response', fontsize=14, fontweight='bold')
    ax.set_ylim(0, max(values) * 1.15)
    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 200, 
                f'{val:,}', ha='center', fontsize=12, fontweight='bold')
    
    # 子图2: 运动神经元
    ax = axes[1]
    motor_values = [461, 469]
    bars = ax.bar(categories, motor_values, color=colors, alpha=0.8, edgecolor='black', linewidth=1.5)
    ax.set_ylabel('Motor Neurons Activated', fontsize=12)
    ax.set_title('Motor Neuron Response', fontsize=14, fontweight='bold')
    ax.set_ylim(0, max(motor_values) * 1.15)
    for bar, val in zip(bars, motor_values):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 10, 
                f'{val}', ha='center', fontsize=12, fontweight='bold')
    
    # 子图3: 传播因子
    ax = axes[2]
    factors = [514.8, 517.5]
    bars = ax.bar(categories, factors, color=colors, alpha=0.8, edgecolor='black', linewidth=1.5)
    ax.set_ylabel('Propagation Factor (x)', fontsize=12)
    ax.set_title('Signal Amplification', fontsize=14, fontweight='bold')
    ax.set_ylim(0, max(factors) * 1.15)
    for bar, val in zip(bars, factors):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 10, 
                f'{val:.1f}x', ha='center', fontsize=12, fontweight='bold')
    
    plt.suptitle('Sugar vs Bitter Stimulation Comparison', fontsize=16, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(output_dir / 'comparison.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: comparison.png")


def plot_brain_schematic(output_dir):
    """图5: 大脑信号传播示意图"""
    fig, ax = plt.subplots(figsize=(14, 8))
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 8)
    ax.axis('off')
    
    # 标题
    ax.text(7, 7.5, 'Neural Signal Flow in Drosophila Brain', 
            fontsize=18, fontweight='bold', ha='center', va='center')
    
    # 大脑轮廓
    brain = FancyBboxPatch((2, 1), 10, 5.5, boxstyle="round,pad=0.1,rounding_size=0.5",
                           facecolor='#f8f9fa', edgecolor='#2c3e50', linewidth=2)
    ax.add_patch(brain)
    
    # 感觉神经元
    sensory_box = FancyBboxPatch((2.5, 1.5), 2, 2, boxstyle="round,pad=0.05",
                                  facecolor=COLORS['sugar'], edgecolor='black', linewidth=1.5, alpha=0.7)
    ax.add_patch(sensory_box)
    ax.text(3.5, 2.5, 'Sensory\nNeurons\n(GRN)', fontsize=10, ha='center', va='center', fontweight='bold')
    
    # 中间神经元
    inter_box = FancyBboxPatch((5.5, 1.5), 3, 4, boxstyle="round,pad=0.05",
                                facecolor=COLORS['neutral'], edgecolor='black', linewidth=1.5, alpha=0.5)
    ax.add_patch(inter_box)
    ax.text(7, 3.5, 'Interneurons\n(139,255 total)', fontsize=10, ha='center', va='center', fontweight='bold')
    
    # 运动神经元
    motor_box = FancyBboxPatch((9.5, 1.5), 2, 2, boxstyle="round,pad=0.05",
                                facecolor=COLORS['motor'], edgecolor='black', linewidth=1.5, alpha=0.7)
    ax.add_patch(motor_box)
    ax.text(10.5, 2.5, 'Motor\nNeurons\n(MN9, etc.)', fontsize=10, ha='center', va='center', fontweight='bold')
    
    # 箭头
    arrow_style = dict(arrowstyle='->', color='#2c3e50', lw=2, mutation_scale=20)
    ax.annotate('', xy=(5.5, 2.5), xytext=(4.5, 2.5), arrowprops=arrow_style)
    ax.annotate('', xy=(9.5, 2.5), xytext=(8.5, 2.5), arrowprops=arrow_style)
    ax.annotate('', xy=(12.5, 2.5), xytext=(11.5, 2.5), arrowprops=arrow_style)
    
    # 标签
    ax.text(5, 3, 'ACH (+)', fontsize=9, ha='center', color=COLORS['sugar'])
    ax.text(9, 3, 'Signal\nPropagation', fontsize=9, ha='center')
    ax.text(13, 2.5, 'Behavior:\nProboscis\nExtension', fontsize=10, ha='center', va='center',
            bbox=dict(boxstyle='round', facecolor=COLORS['highlight'], alpha=0.7))
    
    # 抑制性连接
    ax.annotate('', xy=(7, 1.5), xytext=(3.5, 0.5),
                arrowprops=dict(arrowstyle='->', color=COLORS['bitter'], lw=2, 
                               connectionstyle='arc3,rad=0.3', linestyle='--'))
    ax.text(4.5, 0.8, 'Bitter: GABA/GLUT (-)', fontsize=9, ha='center', color=COLORS['bitter'])
    
    # 图例
    legend_elements = [
        mpatches.Patch(facecolor=COLORS['sugar'], edgecolor='black', label='Excitatory (ACH)'),
        mpatches.Patch(facecolor=COLORS['bitter'], edgecolor='black', label='Inhibitory (GABA/GLUT)'),
        mpatches.Patch(facecolor=COLORS['motor'], edgecolor='black', label='Motor Output'),
    ]
    ax.legend(handles=legend_elements, loc='lower left', fontsize=10)
    
    # 参数
    ax.text(7, 0.3, 'LIF Parameters: v_rest=-52mV, v_threshold=-45mV, w_syn=0.275mV',
            fontsize=10, ha='center', va='center', style='italic', color='gray')
    
    plt.tight_layout()
    plt.savefig(output_dir / 'brain_schematic.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: brain_schematic.png")


def plot_heatmap(output_dir):
    """图6: 发放热力图"""
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    for idx, (rounds, motor_rounds, cmap, title) in enumerate([
        (SUGAR_ROUNDS, SUGAR_MOTOR_ROUNDS, 'Greens', 'Sugar GRN'),
        (BITTER_ROUNDS, BITTER_MOTOR_ROUNDS, 'Reds', 'Bitter GRN')
    ]):
        ax = axes[idx]
        
        max_round = max(rounds.keys())
        data = np.zeros((2, max_round + 1))
        
        for r in range(max_round + 1):
            data[0, r] = rounds.get(r, 0)
            data[1, r] = motor_rounds.get(r, 0)
        
        im = ax.imshow(data, aspect='auto', cmap=cmap, interpolation='nearest')
        
        ax.set_yticks([0, 1])
        ax.set_yticklabels(['All Neurons', 'Motor Neurons'])
        ax.set_xlabel('Propagation Round', fontsize=12)
        ax.set_title(f'{title} Stimulation', fontsize=14, fontweight='bold')
        
        plt.colorbar(im, ax=ax, label='Neurons Spiked')
    
    plt.suptitle('Spike Activity Heatmap', fontsize=16, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(output_dir / 'heatmap.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: heatmap.png")


def plot_motor_neuron_comparison(output_dir):
    """图7: 运动神经元激活对比（科学版）"""
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    # 数据
    motor_types = ['ingestion', 'proboscis', 'neck', 'antennal', 'eye', 'haustellum', 'salivary']
    sugar_counts = [19, 16, 11, 7, 2, 1, 0]
    bitter_counts = [6, 17, 15, 6, 2, 2, 2]
    
    # 子图1: 分组柱状图
    ax = axes[0]
    x = np.arange(len(motor_types))
    width = 0.35
    
    bars1 = ax.bar(x - width/2, sugar_counts, width, label='Sugar GRN', color=COLORS['sugar'], alpha=0.8)
    bars2 = ax.bar(x + width/2, bitter_counts, width, label='Bitter GRN', color=COLORS['bitter'], alpha=0.8)
    
    ax.set_xlabel('Motor Neuron Type', fontsize=12)
    ax.set_ylabel('Spikes Count', fontsize=12)
    ax.set_title('Motor Neuron Activation by Stimulus Type', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(motor_types, rotation=45, ha='right')
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    # 高亮 ingestion 的差异
    ax.annotate('Key Finding:\n+13 difference', xy=(0, 19), xytext=(1.5, 22),
                fontsize=10, ha='center',
                arrowprops=dict(arrowstyle='->', color='gray'),
                bbox=dict(boxstyle='round', facecolor=COLORS['highlight'], alpha=0.7))
    
    # 子图2: 差异图
    ax = axes[1]
    differences = [sugar_counts[i] - bitter_counts[i] for i in range(len(motor_types))]
    colors = [COLORS['sugar'] if d > 0 else COLORS['bitter'] for d in differences]
    
    bars = ax.barh(motor_types, differences, color=colors, alpha=0.8)
    ax.axvline(x=0, color='black', linewidth=1)
    ax.set_xlabel('Difference (Sugar - Bitter)', fontsize=12)
    ax.set_title('Activation Difference by Motor Neuron Type', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3, axis='x')
    
    # 添加数值标签
    for bar, diff in zip(bars, differences):
        x_pos = bar.get_width() + 0.5 if diff >= 0 else bar.get_width() - 0.5
        ha = 'left' if diff >= 0 else 'right'
        ax.text(x_pos, bar.get_y() + bar.get_height()/2, f'{diff:+d}',
                ha=ha, va='center', fontsize=10, fontweight='bold')
    
    plt.suptitle('Motor Neuron Response: Sugar vs Bitter Stimulation', fontsize=16, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(output_dir / 'motor_neuron_comparison.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: motor_neuron_comparison.png")


def plot_limitations_explanation(output_dir):
    """图8: 限制说明图"""
    fig, ax = plt.subplots(figsize=(14, 8))
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 8)
    ax.axis('off')
    
    # 标题
    ax.text(7, 7.5, 'What We Can and Cannot Do', 
            fontsize=18, fontweight='bold', ha='center', va='center')
    
    # 左侧：我们能做的
    ax.add_patch(FancyBboxPatch((0.5, 1), 6, 5.5, boxstyle="round,pad=0.1",
                                facecolor='#d5f5e3', edgecolor='#27ae60', linewidth=2))
    ax.text(3.5, 6, 'What We CAN Do', fontsize=14, fontweight='bold', ha='center', color='#27ae60')
    
    can_do = [
        '✓ Store 139,255 neurons in NeuG',
        '✓ Run LIF simulation on the connectome',
        '✓ Track signal propagation (20 rounds)',
        '✓ Measure motor neuron activation',
        '✓ Find interesting patterns',
        '  (e.g., ingestion_motor_neuron: 19 vs 6)'
    ]
    for i, text in enumerate(can_do):
        ax.text(1, 5.2 - i*0.7, text, fontsize=11, va='center')
    
    # 右侧：我们不能做的
    ax.add_patch(FancyBboxPatch((7.5, 1), 6, 5.5, boxstyle="round,pad=0.1",
                                facecolor='#fadbd8', edgecolor='#e74c3c', linewidth=2))
    ax.text(10.5, 6, 'What We CANNOT Do', fontsize=14, fontweight='bold', ha='center', color='#e74c3c')
    
    cannot_do = [
        '✗ Precisely predict behavior',
        '✗ Identify specific MN9 neuron',
        '✗ Measure firing rate (Hz)',
        '✗ Validate with experiments',
        '',
        'These require:',
        '  - MN9 annotation from paper'
    ]
    for i, text in enumerate(cannot_do):
        ax.text(8, 5.2 - i*0.7, text, fontsize=11, va='center')
    
    # 底部说明
    ax.text(7, 0.5, 'Honest science: We show what we can do, and admit what we cannot.',
            fontsize=12, ha='center', va='center', style='italic', color='gray')
    
    plt.tight_layout()
    plt.savefig(output_dir / 'limitations.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: limitations.png")


def main():
    # Use relative path or environment variable for output directory
    output_dir = Path(os.environ.get("FLYWIRE_FIGURES_DIR", "./figures"))
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("生成 FlyWire 大脑模拟可视化图表")
    print("=" * 70)
    print(f"输出目录: {output_dir}")
    
    print("\n生成图表...")
    
    plot_signal_propagation(output_dir)
    plot_cumulative_spikes(output_dir)
    plot_motor_neuron_tracking(output_dir)
    plot_comparison(output_dir)
    plot_brain_schematic(output_dir)
    plot_heatmap(output_dir)
    plot_motor_neuron_comparison(output_dir)
    plot_limitations_explanation(output_dir)
    
    print("\n" + "=" * 70)
    print("图表生成完成!")
    print("=" * 70)
    print(f"\n生成的图表:")
    for f in sorted(output_dir.glob("*.png")):
        print(f"  - {f.name}")


if __name__ == "__main__":
    main()