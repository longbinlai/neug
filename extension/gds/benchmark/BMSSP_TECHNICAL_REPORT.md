# NeuG 自适应 BMSSP：实现、正确性与 Graphalytics 性能评估

报告日期：2026-09-24
代码分支：`codex/bmssp-graphalytics-report`

## 摘要

本次工作在 NeuG 现有 `sssp` GDS 过程内增加了实验性的 `bmssp` 后端，调用接口为
`CALL sssp(..., {algo: 'bmssp'})`，没有引入新的过程名。在两张 LDBC
Graphalytics `datagen` 图、8 线程的测试中：

- `datagen-8_0-fb`：BMSSP 中位耗时 0.150 秒，较 frontier 快 4.5%，较
  Dijkstra 快 13.6 倍。
- `datagen-8_1-fb`：BMSSP 中位耗时 0.237 秒，较 frontier 快 13.5%，较
  Dijkstra 快 13.5 倍。
- 第二张图的 2,072,117 个顶点均与 Graphalytics 官方 SSSP 参考结果一致，最大
  绝对误差为 `8.88e-16`；小图 conformance、随机差分和强制 fallback 测试共
  27 项全部通过。

这里的 `bmssp` 是面向 NeuG 存储接口的自适应工程实现，而不是论文伪代码的逐行复刻。
低直径图优先运行有界的并行 sparse/dense frontier；若 32 轮内未收敛，则丢弃探测
阶段标签，转入递归 BMSSP 分解并做 fixed-point 修复。因此当前结果证明该实现对所测
工作负载有效，但不能据此宣称已经复现论文的渐进复杂度。

## 1. 背景与命名

参考工作是 Duan、Mao、Mao、Shu、Yin 的单源最短路研究。`Duan` 是作者姓氏，不能
准确描述算法；实现和用户接口统一采用论文问题结构对应的名称 `bmssp`。NeuG 对外仍
保留一个 `sssp` 过程，通过较短的 `algo` 参数选择执行后端：

```cypher
CALL sssp('g', {
  source: '6',
  weight: 'weight',
  directed: false,
  concurrency: 8,
  algo: 'bmssp'
})
YIELD node, distance
RETURN node.id, distance;
```

`algo` 可取 `auto`、`frontier`、`dijkstra`、`bmssp`。省略时为 `auto`：无过滤、
只返回距离时使用 frontier；需要谓词或 `path` 时使用 Dijkstra。旧参数
`implementation` 仅作为兼容入口保留，已弃用，不能与 `algo` 同时出现。

## 2. 实现设计

### 2.1 自适应执行路径

1. **并行 frontier 探测**：直接访问 NeuG 投影图，不额外物化 CSR；按活跃集合密度
   在 sparse 与 dense 扫描间切换，最多执行 32 轮，并遵守 `concurrency`。
2. **快速返回**：若标签在探测阶段达到 fixed point，直接输出结果。这是两张
   `datagen` 图实际走到的路径。
3. **递归 BMSSP fallback**：未收敛时丢弃探测标签，延迟构建紧凑 CSR，再执行
   pivot/frontier/base-case 递归分解。
4. **正确性修复**：fallback 后扫描仍违反松弛条件的边，并用优先队列传播更新直到
   fixed point，防止当前简化 frontier 暴露部分标签。

### 2.2 与论文算法的差异

- 当前 `OrderedFrontier` 是每个顶点保留一个最优 key 的正确性优先实现，并非论文
  Lemma 3.3 的完整 block data structure。
- 没有执行论文中的显式常数出度图变换，避免每次查询扩大图。
- fallback 当前为单线程；并行收益主要来自前置 frontier 探测。
- fixed-point repair 提供工程正确性保护，但改变了理论工作量，因此当前实现不声明
  论文的渐进时间界。
- `bmssp` 仅接受有限、非负权重，当前只支持无谓词投影图的距离输出。

## 3. 评估方法

### 3.1 环境

| 项目 | 配置 |
|---|---|
| 主机 | MacBook Pro (Mac16,8) |
| CPU | Apple M4 Pro，12 核（8P + 4E） |
| 内存 | 48 GB |
| OS | macOS 15.7.9，arm64 |
| 基线代码 | `05742c7cf9b27cb9356372e731b71915c5a8adde` 加本分支改动 |
| 并发 | 8 |
| 预热/计时 | 1 次预热，5 次计时，报告中位数 |

### 3.2 数据集

| 数据集 | 顶点数 | NeuG 存储边数 | 方向 | SSSP source |
|---|---:|---:|---|---:|
| `datagen-8_0-fb` | 1,706,561 | 107,507,376 | 无向 | 6 |
| `datagen-8_1-fb` | 2,072,117 | 134,267,822 | 无向 | 6 |

计时查询使用 `WITH node, distance LIMIT 1 RETURN count(*)`，确保算法完整执行，同时
避免将数百万行结果序列化到 Python。首次 `COPY FROM` 和图投影不计入算法时间；后续
运行复用 NeuG checkpoint。三种后端在相同图、source、权重、方向和线程配置下执行。

### 3.3 指标

- 主指标：五次 wall-clock 时间的中位数。
- 加速比：`Dijkstra median / BMSSP median`。
- 相对 frontier 改善：`(frontier - BMSSP) / frontier`。
- 正确性：与官方 Graphalytics SSSP 参考输出逐顶点比较，并用 Dijkstra 做差分测试。

## 4. 性能结果

| 数据集 | BMSSP | Frontier | Dijkstra | BMSSP vs Frontier | BMSSP vs Dijkstra |
|---|---:|---:|---:|---:|---:|
| `datagen-8_0-fb` | **0.150 s** | 0.157 s | 2.037 s | 快 4.5% | 快 13.6× |
| `datagen-8_1-fb` | **0.237 s** | 0.274 s | 3.207 s | 快 13.5% | 快 13.5× |

第二张图的五次原始时间为：

- BMSSP：0.233、0.237、0.254、0.238、0.236 秒。
- Frontier：0.261、0.274、0.249、0.360、0.387 秒。
- Dijkstra：3.574、3.202、3.252、3.207、3.041 秒。

第一张图保留了中位数结果，但本次报告没有保存其逐次原始日志，因此 CSV 中相应的
`run_seconds` 留空，避免用推测值补齐证据。

结果说明：Dijkstra 的串行优先队列在这两张大规模稠密图上明显落后；frontier 已经
很快，而自适应 BMSSP 的 fast path 进一步降低了调度和无效扫描开销。第二张图的
frontier 后两次运行波动较大，因此这里采用预先约定的中位数而非最小值。当前样本只
包含两张同族 `datagen` 图，不足以推出所有图类型上的普遍优势。

## 5. Profiling 结论

在 `datagen-8_1-fb` 上启用 `NEUG_BMSSP_PROFILE=1` 后，一次冷态附近运行记录为：

```text
BMSSP probe profile: init_ms=5.17579 rounds=6 concurrency=8 compute_ms=283.403
```

该图在 6 轮内收敛，没有进入 CSR 构建和递归 fallback。profiling 运行约 0.311 秒，
高于关闭额外日志后的热态中位数 0.237 秒，因此 profiling 数据只用于定位阶段成本，
不混入性能表。后续主要优化方向应是：

1. 将论文 block frontier 的工程实现替换当前 correctness-oriented heap frontier；
2. 并行化 fallback 的 CSR 构建、pivot 搜索和 repair；
3. 在高直径、道路网、稀疏有向图上单独测量 fallback，而不是只测 fast path；
4. 增加硬件性能计数器，拆分边扫描、原子更新、frontier 去重和内存带宽成本。

## 6. 正确性验证

- 官方 Graphalytics 小图覆盖 BFS、SSSP、PageRank、WCC、CDLP、LCC，测试结果为
  `27 passed`。
- SSSP conformance 对 `frontier`、`dijkstra`、`bmssp` 三种后端使用同一官方输出。
- 固定随机种子的 64 顶点图覆盖有向/无向、三个 source、零权边和平行边，串行及
  4 线程 BMSSP 均与 Dijkstra 一致。
- 96 顶点长链确保超过 32 轮探测上限并进入 fallback，串行和并行配置均与 Dijkstra
  完全一致。
- `datagen-8_1-fb` 完整官方参考验证覆盖 2,072,117 个顶点，最大绝对误差
  `8.88e-16`。

## 7. 数据获取与复现

数据包已发布到公开 OSS，归档内包含 `.properties`、`.v`、`.e` 以及 Graphalytics
各算法参考输出。下载脚本会校验 SHA-256、解压，并创建 NeuG `COPY FROM` 使用的
`.csv` 别名：

```bash
python3 extension/gds/benchmark/fetch_graphalytics_data.py \
  --dataset all --output /data/graphalytics

python3 extension/gds/benchmark/graphalytics_bench.py \
  --data-root /data/graphalytics/datagen \
  --db-root /data/neug-bench-db \
  --dataset datagen-8_1-fb \
  --concurrency 8 --warmup-runs 1 --runs 5 \
  --sssp-only --sssp-algos bmssp,frontier,dijkstra
```

公开对象：

- `https://neug.oss-cn-beijing.aliyuncs.com/datasets/ldbc-graphalytics/datagen-8_0-fb.tar.zst`
- `https://neug.oss-cn-beijing.aliyuncs.com/datasets/ldbc-graphalytics/datagen-8_1-fb.tar.zst`
- `https://neug.oss-cn-beijing.aliyuncs.com/datasets/ldbc-graphalytics/SHA256SUMS`
- `https://neug.oss-cn-beijing.aliyuncs.com/datasets/ldbc-graphalytics/fetch_graphalytics_data.py`
- `https://neug.oss-cn-beijing.aliyuncs.com/datasets/ldbc-graphalytics/graphalytics_bench.py`

完整原始计时记录位于 `bmssp_results.csv`。

## 8. 结论

在本次两张 Graphalytics 图上，自适应 BMSSP 同时满足完整参考结果正确性，并比 NeuG
现有 frontier 快 4.5%–13.5%、比 Dijkstra 快约 13.5 倍，具备作为实验性 SSSP 后端
继续演进的价值。合并前仍建议保留 `experimental` 标记，并以道路网或其他高直径图
补充 fallback 性能证据；只有在 block frontier 和 fallback 并行化完成后，才适合讨论
与论文理论界的一致性。
