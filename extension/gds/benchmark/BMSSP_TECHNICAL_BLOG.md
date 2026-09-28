---
title: "从 Lean 证明到真实图评测：我们在 NeuG 里实现了 BMSSP"
slug: neug-bmssp-breaking-the-sorting-barrier
date: 2026-09-29
summary: "Vals AI 用 Lean 证明了一种理论上更快的最短路算法。我们继续追问：把这类理论算法放进真实图数据库，究竟能跑多快？"
tags: [NeuG, GDS, SSSP, BMSSP, Graph Algorithms, LDBC Graphalytics]
---

# 从 Lean 证明到真实图评测：我们在 NeuG 里实现了 BMSSP

2026 年 9 月 20 日，Vals AI 发布了
[《A Faster Shortest Path Algorithm》](https://www.vals.ai/blogs/faster-shortest-path-algorithm)。
他们让 10 个 Claude Opus 5.5 agent 协作搜索新算法，15 小时、733 条消息之后，
得到了 C-HD，并用 Lean 完成了形式化证明。对一类稀疏图，C-HD 把理论界又向前推了一步。

这项工作不只改进了渐进界，还展示了一种新的研究工作流：agent 寻找算法，
Lean kernel 检查证明。但 Vals AI 也明确说明，他们没有在大型真实图上跑 benchmark，
形式化构造的常数也非常大。理论上更快，还不等于已经有了一份跑得更快的工程实现。

我们更关心另一个问题：理论上更优的 SSSP 算法，放进真实图数据库以后，
用公开数据和现有后端对比，实际会跑得怎么样？

但我们最后没有直接实现 C-HD。它的理论优势目前只在比本文测试数据稀疏得多的图上
得到了证明，现有成果的重点也是形式化验证，不是一套可以拿来跑大图测试的实现。

我们选择先复现去年 STOC 最佳论文里的 BMSSP。它已经过同行评议，
论文明确给出了伪代码、数据结构和正确性不变量，又直接处理 NeuG 已经支持的有向非负实数权图。
这让它更适合拿来做第一轮工程实验。

## 去年那篇 STOC 最佳论文

1956 年，Edsger Dijkstra 为 ARMAC 计算机的一次公开演示构思了最短路算法；论文到
1959 年才发表。1984 年，Fredman 和 Tarjan 在 FOCS 提出 Fibonacci heap，把非负
实数权重 SSSP 的经典复杂度推进到 $O(m+n\log n)$。此后四十多年，这个界一直是稀疏
图上的标杆。

2025 年 4 月，Ran Duan、Jiayi Mao、Xiao Mao、Xinkai Shu 和 Longhui Yin 在论文
[《Breaking the Sorting Barrier for Directed Single-Source Shortest Paths》](https://arxiv.org/abs/2504.17033)
中给出了确定性的 $O(m\log^{2/3}n)$ 算法。它处理比较—加法模型中的有向图，边权可以
是任意非负实数；对于稀疏图，这是第一次越过 Dijkstra 的
$O(m+n\log n)$ 界。论文收入 STOC 2025，并获
[Best Paper Award](https://www.sigact.sigact.hosting.acm.org/prizes/best_paper.html)。

这项结果在 2025 年引起了不少算法圈以外的关注。清华大学发布了
[成果介绍](https://www.tsinghua.edu.cn/info/1175/118821.htm)，
[Quanta Magazine](https://www.quantamagazine.org/new-method-is-the-fastest-way-to-find-the-best-routes-20250806/)
写了一篇长篇报道，之后又由
[WIRED](https://www.wired.com/story/new-method-is-the-fastest-way-to-find-the-best-routes/)
转载。讨论的核心并不是“又造了一个更快的 heap”，而是一个更基础的问题：求最短距离，
真的需要把所有顶点按距离排好顺序吗？

我们把论文中的 BMSSP 思路接进了 NeuG GDS extension，并拿两张 LDBC
Graphalytics `datagen` 图做了验证。结果先放在这里：BMSSP 相比 NeuG 原有 frontier
后端降低了 4.5% 和 13.5% 的耗时；相比 NeuG 当前的 Dijkstra 后端，分别快 13.6 倍
和 13.5 倍。

这些数字有明确的适用范围。两张图都走了自适应实现的 frontier fast path，没有进入
递归 fallback；NeuG 当前的 Dijkstra 核心路径也是串行的。因此，这组实验说明的是
“当前 NeuG 实现之间的差距”，不是对论文理论复杂度的完整复现实验。

![从 Dijkstra 到突破排序障碍的时间线](images/bmssp/01-history.png)

> 图 1：Dijkstra 于 1956 年构思算法、1959 年发表论文；Fibonacci heap 的会议论文
> 发表于 1984 年。2023 年，随机算法先在无向图上越过排序障碍；2025 年的新算法把结果
> 扩展到有向图，并且是确定性的。

## 问题出在“排序”，不只是优先队列

Dijkstra 的执行过程很熟悉：把候选顶点放进最小优先队列，每次取出距离最小的顶点，
再松弛它的出边。这个顺序很好用，也带来一个额外结果——顶点按照最短距离依次被确定。

SSSP 本身只要求源点到每个顶点的距离，并不要求输出这份全序。论文利用的就是这一区别。
它不再让所有候选顶点争夺“下一个全局最小值”，而是把前沿分成有界的距离层，在层内只
维护后续计算真正需要的偏序关系。

![Dijkstra 全序前沿与 BMSSP 偏序分层对比](images/bmssp/02-sorting-barrier.png)

> 图 2：左侧的 Dijkstra 持续维护全局最小候选；右侧只保留局部探测、pivot 和递归
> 分层所需的顺序。

论文还重新使用了 Bellman–Ford。它不依赖距离排序，但如果在整张图上反复跑到收敛，
代价会很高。新算法只做有限轮松弛，用这些结果找出 pivots：能够代表较大一片最短路树
的入口。随后，算法围绕这些 pivots 递归求解 Bounded Multi-Source Shortest Path，
也就是 BMSSP 子问题。

几个关键步骤可以概括为：

1. 做少量 Bellman–Ford 松弛，得到一片局部可达区域；
2. 从中筛出 pivots，减少真正进入递归的源点；
3. 按距离边界分批取出前沿，而不是逐点完成全局排序；
4. 递归求解每个有界子问题，再合并已经确定的距离。

当 $k=\lfloor\log^{1/3}n\rfloor$、
$t=\lfloor\log^{2/3}n\rfloor$ 时，论文证明总时间为
$O(m\log^{2/3}n)$。2023 年的无向图结果是随机算法；这篇工作同时给出了有向图突破，
并成为第一个即使在无向图上也越过该界的确定性算法。

后端之所以叫 `bmssp`，而不是 `duan`，原因也在这里。BMSSP 是论文递归子问题的名称，
能够说明算法结构；“Duan”只是五位作者中一位作者的姓氏。

## `frontier` 到底是什么

先解释一下本文反复拿来对比的 `frontier`。它不是另一个经典最短路算法的名字，
而是 NeuG 对一种并行 label-correcting 实现的命名。从算法家族看，它更接近“带活跃集合的
并行 Bellman–Ford”，不是 Dijkstra，也不是 Delta-stepping。

计算开始时，只把源点放入 frontier。每轮松弛后，距离变小的顶点进入下一轮；没有顶点
再被更新时，计算结束。活跃顶点少于全图的 5% 时，NeuG 从 frontier 出发向外“push”；
超过 5% 时，则切换成遍历所有顶点、从入边“pull”最优距离。松弛可以分配到多个线程，
也不需要像 Dijkstra 那样每次选出全局最近的顶点。

代价是同一个顶点和它的边可能在多轮中被重复处理。低直径图通常只需要少量轮次，
这种做法能吃到很好的并行度；长链或高直径图则可能跑很多轮。BMSSP 的第一阶段正是有上限的
frontier probe：32 轮内收敛就直接返回，否则才进入递归 fallback。因此，本文两张图上的
BMSSP 与 `frontier` 对比，主要是两种 frontier 工程实现的对比，而不是递归 BMSSP
与 Bellman–Ford 的直接对决。

## NeuG 没有逐行翻译论文伪代码

理论算法默认可以为图建立专用结构，数据库执行器面对的条件不同。NeuG 已经有 CSR、
属性列和图投影；如果每次查询都重新构图，额外的内存访问很可能盖过算法收益。论文中的
数据结构强调渐进界，工程实现还要考虑 cache locality、原子更新和线程调度。

我们最终采用了两段式执行路径。

![NeuG 自适应 BMSSP 执行路径](images/bmssp/03-adaptive-path.png)

> 图 3：先在 NeuG 投影图上运行有上限的并行 frontier probe。收敛就直接返回；超过
> 32 轮仍未收敛，才丢弃 probe 标签、构建紧凑 CSR，并进入递归 BMSSP 与修复阶段。

第一段是最多 32 轮的 sparse/dense frontier probe。活跃顶点少时，从前沿向外推；
活跃集合超过顶点数的 5% 时，切换为 dense pull。并行路径使用 `concurrency` 指定的
线程数。probe 收敛后，距离可以直接返回，不需要再物化一份 CSR。

第二段只在 probe 未收敛时执行。实现会先丢弃 probe 的中间标签，重新初始化状态，
然后构建紧凑 CSR，执行 pivot、frontier 和 base case 递归。最后的 fixed-point
repair 会继续传播仍然有效的松弛，直到没有边能够改进距离。

这层 repair 是正确性保护，也是当前实现与论文算法的重要差异。它保证简化的数据结构
不会把部分收敛结果交给用户，但也改变了理论工作量，因此我们没有把当前实现描述为
“复现了论文的渐进复杂度”。

两张性能图都在 32 轮 probe 上限前收敛，没有构建 fallback CSR。现有 profile 记录中，
`datagen-8_1-fb` 用了 6 轮。也就是说，本文的性能数据主要测到的是自适应 fast path，
递归 BMSSP 目前只有正确性测试，还缺少有代表性的性能样本。

## 调用方式仍然是 `sssp`

这次没有增加独立过程名。调用入口仍是 `sssp`，通过 `algo` 选择后端：

```cypher
CALL sssp('social', {
  source: '6',
  weight: 'weight',
  directed: false,
  concurrency: 8,
  algo: 'bmssp'
})
YIELD node, distance
RETURN node.id, distance;
```

目前支持四个取值：

- `auto`：默认值；普通距离查询走 frontier，需要谓词或 `path` 时走 Dijkstra；
- `frontier`：NeuG 原有的并行 sparse/dense frontier 实现；
- `dijkstra`：支持谓词和路径输出的优先队列实现；
- `bmssp`：本文的实验性自适应后端。

旧参数 `implementation` 只为兼容已有调用保留，已标记弃用；`algo` 和
`implementation` 同时出现时会报错。

## 先把正确性测扎实

最短路的错误通常不明显：一个顶点少松弛一次，结果仍可能看起来“差不多”。因此测试不只
比较几个手写样例，而是分成四层。

![BMSSP 正确性验证证据链](images/bmssp/07-validation.png)

> 图 4：小图覆盖接口和官方结果，随机图覆盖边界条件，长链强制进入 fallback，完整
> `datagen-8_1-fb` 再逐点对照官方 SSSP 输出。

- `tests/test_graphalytics.py` 整套测试结果为 `27 passed`；其中 SSSP conformance
  对 `frontier`、`dijkstra`、`bmssp` 使用同一份官方参考输出。
- 固定随机种子的 64 顶点图覆盖有向/无向、3 个 source、零权边和平行边；BMSSP 的
  1 线程和 4 线程结果均与 Dijkstra 一致。
- 96 顶点长链会超过 32 轮 probe 上限，从而真正进入 fallback；串行和并行配置均与
  Dijkstra 完全一致。
- `datagen-8_1-fb` 的 2,072,117 个顶点全部逐点比较，最大绝对误差为
  `8.88e-16`。

最后一个误差的量级接近双精度机器精度，符合不同松弛顺序可能产生的舍入差异。

## 性能结果

测试机器是 MacBook Pro（Mac16,8），Apple M4 Pro 12 核（8P + 4E）、48 GB 内存，
系统为 macOS 15.7.9 arm64。NeuG 使用 8 线程。每个后端预热 1 次，再计时 5 次，报告
中位数。

查询会完整执行 SSSP kernel，但只返回一行计数，避免把数百万行结果序列化到 Python。
首次 `COPY FROM`、图投影和 checkpoint 不计入算法时间。

| 数据集 | 顶点数 | NeuG 存储边数 | 方向 | Source |
|---|---:|---:|---|---:|
| `datagen-8_0-fb` | 1,706,561 | 107,507,376 | 无向 | 6 |
| `datagen-8_1-fb` | 2,072,117 | 134,267,822 | 无向 | 6 |

![两张 Graphalytics 图上的 SSSP 中位耗时](images/bmssp/04-latency.png)

> 图 5：纵轴为对数坐标。BMSSP 的中位耗时分别是 0.150 秒和 0.237 秒；Dijkstra
> 分别是 2.037 秒和 3.207 秒。

| 数据集 | BMSSP | Frontier | Dijkstra |
|---|---:|---:|---:|
| `datagen-8_0-fb` | **0.150 s** | 0.157 s | 2.037 s |
| `datagen-8_1-fb` | **0.237 s** | 0.274 s | 3.207 s |

![BMSSP 相对 frontier 和 Dijkstra 的性能提升](images/bmssp/05-speedup.png)

> 图 6：按 CSV 中保留到毫秒的中位数计算，BMSSP 相对 frontier 的耗时下降为
> 4.46% 和 13.50%，图中四舍五入为 4.5% 和 13.5%；Dijkstra/BMSSP 为 13.58×
> 和 13.53×，图中显示为 13.6× 和 13.5×。

三种查询虽然都传入 `concurrency: 8`，但 NeuG 当前 Dijkstra 后端的核心优先队列没有
并行化。因此 13 倍的差距不能外推为“BMSSP 普遍比并行 Dijkstra 快 13 倍”。更值得
观察的是与 frontier 的比较：两者都利用并行图扫描，BMSSP 的 fast path 在第二张图上
仍少用了约 13.5% 的时间。

`datagen-8_1-fb` 保存了五次原始计时：

![datagen-8_1-fb 五次运行波动](images/bmssp/06-run-stability.png)

> 图 7：BMSSP 为 0.233、0.237、0.254、0.238、0.236 秒；frontier 为
> 0.261、0.274、0.249、0.360、0.387 秒；Dijkstra 为
> 3.574、3.202、3.252、3.207、3.041 秒。虚线是各自中位数。

第一张图只保留了中位数，没有保存五次逐轮日志，因此原始 CSV 对应的 `run_seconds`
为空。我们没有用估算值补齐它。

## Profile 看到的瓶颈

设置 `NEUG_BMSSP_PROFILE=1` 后，实现会记录 probe 轮数和阶段耗时。
`datagen-8_1-fb` 的一次接近冷态运行输出为：

```text
BMSSP probe profile: init_ms=5.17579 rounds=6 concurrency=8 compute_ms=283.403
```

这次带 profiling 的查询总耗时约 0.311 秒，高于关闭额外日志后的 0.237 秒中位数，
所以它只用于定位阶段，没有放进性能表。日志同时确认该图没有执行 CSR 构建、递归调用
和 repair。

下一步真正需要测的是高直径道路网、稀疏有向图和长链。这些图会让前沿长时间保持稀疏，
更可能触发 fallback。到那时才能分别回答 CSR 构建、pivot 搜索、递归 frontier 和
repair 各占多少时间。

## 当前实现还缺什么

这版后端仍然标记为 experimental，主要有五个原因：

- `OrderedFrontier` 采用“每个顶点保留一个最优 key”的优先队列实现，并不是论文
  Lemma 3.3 的完整 block data structure；
- 没有执行论文中的显式常数出度图变换，以免每次查询扩大图；
- fallback 目前是单线程，并行收益主要来自前置 probe；
- fixed-point repair 保证正确性，但不满足论文原始的工作量分析；
- 当前仅支持有限、非负权重，以及无谓词投影图上的距离输出。

此外，性能样本只有两张同族 `datagen` 图。它们足以证明这条执行路径值得继续优化，
不足以说明所有图分布上都会领先。

2026 年 2 月，Duan、Mao、Shu 和 Yin 又发布了
[后续工作](https://arxiv.org/abs/2602.07868)，继续改进有向 SSSP 的复杂度。排序障碍
被越过以后，理论界显然还没有停下来；工程实现也一样，还有很长的优化清单。

## 复现实验

基准脚本本身不会编译 NeuG。先按照
[benchmark README 的“Build the current GDS extension”一节](README.md#build-the-current-gds-extension)
编译当前 checkout 的 Python binding 和 `neug_gds_extension`，并设置其中给出的
`PYTHONPATH`。否则，Python 可能加载环境里已经安装的旧版 `neug`，测到的并不是刚修改的
代码。

两张 Graphalytics 图、官方参考输出、SHA-256 清单和 ETL 脚本已经放到公开 OSS。
下载脚本会校验归档、解压，并创建 NeuG `COPY FROM` 使用的 `.csv` 别名：

```bash
python3 extension/gds/benchmark/fetch_graphalytics_data.py \
  --dataset all \
  --output ./graphalytics-data
```

然后运行三个后端：

```bash
python3 extension/gds/benchmark/graphalytics_bench.py \
  --data-root ./graphalytics-data/datagen \
  --db-root ./neug-bench-db \
  --dataset datagen-8_1-fb \
  --concurrency 8 \
  --warmup-runs 1 \
  --runs 5 \
  --sssp-only \
  --sssp-algos bmssp,frontier,dijkstra
```

相关文件：

- [NeuG 实现分支](https://github.com/longbinlai/neug/tree/codex/bmssp-graphalytics-report)
- [完整技术报告](BMSSP_TECHNICAL_REPORT.md)
- [原始结果 CSV](bmssp_results.csv)
- [公开数据与校验清单](https://neug.oss-cn-beijing.aliyuncs.com/datasets/ldbc-graphalytics/SHA256SUMS)

## 小结

这次实验最有价值的地方，不是简单地给 Dijkstra 换掉一个数据结构，而是把论文中的一个
判断带进了数据库执行器：最短路需要准确距离，但不一定需要完整的距离顺序。

NeuG 当前的答案是自适应执行。低直径图先走已有存储结构上的并行 probe；只有 probe
处理不了的图，才承担递归 BMSSP 的额外构图成本。现有两张图证明 fast path 有收益，
96 顶点长链证明 fallback 能给出正确结果。下一阶段则要在真正触发 fallback 的大图上，
把正确性证据补成性能证据。

## 参考资料

1. Vals AI,
   [A Faster Shortest Path Algorithm](https://www.vals.ai/blogs/faster-shortest-path-algorithm), 2026-09-20.
2. Ran Duan, Jiayi Mao, Xiao Mao, Xinkai Shu, Longhui Yin,
   [Breaking the Sorting Barrier for Directed Single-Source Shortest Paths](https://arxiv.org/abs/2504.17033), 2025.
3. [STOC 2025 proceedings version](https://doi.org/10.1145/3717823.3718179), pp. 36–44.
4. Michael L. Fredman, Robert Endre Tarjan,
   [Fibonacci Heaps and Their Uses in Improved Network Optimization Algorithms](https://doi.org/10.1109/SFCS.1984.715934), FOCS 1984.
5. Edsger W. Dijkstra,
   [A Note on Two Problems in Connexion with Graphs](https://doi.org/10.1007/BF01386390), 1959.
6. 清华大学，[段然团队获得 STOC 2025 最佳论文奖](https://www.tsinghua.edu.cn/info/1175/118821.htm)。
7. Ben Brubaker,
   [New Method Is the Fastest Way To Find the Best Routes](https://www.quantamagazine.org/new-method-is-the-fastest-way-to-find-the-best-routes-20250806/), Quanta Magazine, 2025-08-06.
8. Ran Duan, Xiao Mao, Xinkai Shu, Longhui Yin,
   [A Faster Directed Single-Source Shortest Path Algorithm](https://arxiv.org/abs/2602.07868), 2026.
