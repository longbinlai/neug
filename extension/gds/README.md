# NeuG GDS Extension

NeuG Graph Data Science (GDS) 扩展提供了高性能的图算法库，支持 LDBC Graphalytics 基准测试中的核心算法。

## 目录

- [环境要求](#环境要求)
- [快速开始](#快速开始)
- [编译指南](#编译指南)
  - [编译 NeuG Wheel 包](#编译-neug-wheel-包)
  - [编译 GDS Extension](#编译-gds-extension)
- [使用指南](#使用指南)
  - [加载扩展](#加载扩展)
  - [算法示例](#算法示例)
- [端到端示例](#端到端示例)
- [算法参考](#算法参考)

---

## 环境要求

### 系统要求
- **操作系统**: Linux (x86_64/ARM64) 或 macOS (Apple Silicon/Intel)
- **Python**: 3.8+
- **CMake**: 3.10+
- **C++ 编译器**: 支持 C++17 的编译器 (GCC 9+, Clang 10+)

### 依赖项
```bash
# macOS
brew install cmake

# Ubuntu/Debian
sudo apt update
sudo apt install -y cmake build-essential python3-dev
```

---

## 快速开始

如果你只想快速体验，可以直接安装预编译的 wheel 包：

```bash
pip install neug
```

然后按照 [端到端示例](#端到端示例) 运行。

---

## 编译指南

### 1. 克隆代码

```bash
git clone https://github.com/alibaba/neug.git
cd neug
```

### 2. 编译 NeuG Wheel 包

```bash
# 进入 Python 绑定目录
cd tools/python_bind

# 安装 Python 依赖
make requirements

# 编译 NeuG 和 Python 绑定
make build

# 可选：构建 wheel 包
make wheel

# 返回项目根目录
cd ../..
```

编译完成后，Python 绑定位于：
```
tools/python_bind/build/lib.<platform>-<python_version>/
```

### 3. 编译 GDS Extension

```bash
# 创建并进入 build 目录
mkdir -p build && cd build

# 配置 CMake，启用 GDS 扩展
cmake .. -DBUILD_EXTENSIONS="gds" -DBUILD_TEST=ON

# 编译 GDS 扩展
make neug_gds_extension -j$(nproc)

# 返回项目根目录
cd ..
```

编译完成后，扩展库位于：
```
build/extension/gds/libgds.neug_extension
```

### 4. 安装扩展到 Python 绑定目录

```bash
# 确定你的 Python 绑定目录
# 例如：lib.macosx-11.0-arm64-cpython-312

# 创建扩展目录（如果不存在）
mkdir -p tools/python_bind/build/lib.macosx-11.0-arm64-cpython-312/extension/gds

# 复制扩展库
cp build/extension/gds/libgds.neug_extension \
   tools/python_bind/build/lib.macosx-11.0-arm64-cpython-312/extension/gds/
```

---

## 使用指南

### 加载扩展

在 Python 中使用 GDS 扩展：

```python
import sys
sys.path.insert(0, '/path/to/neug/tools/python_bind')

from neug.database import Database

# 创建数据库
db = Database('/tmp/my_graph_db', 'w')
conn = db.connect()

# 加载 GDS 扩展（需要 write 模式）
conn.execute('LOAD gds;', 'schema')
```

### 算法示例

#### 1. 创建图并加载数据

```python
# 创建图 Schema
conn.execute('CREATE NODE TABLE Person (id INT64 PRIMARY KEY, name STRING);')
conn.execute('CREATE REL TABLE KNOWS (FROM Person TO Person);')

# 插入顶点
conn.execute("CREATE (:Person {id: 1, name: 'Alice'});")
conn.execute("CREATE (:Person {id: 2, name: 'Bob'});")
conn.execute("CREATE (:Person {id: 3, name: 'Charlie'});")

# 插入边
conn.execute("MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:KNOWS]->(b);")
conn.execute("MATCH (a:Person {id: 2}), (b:Person {id: 3}) CREATE (a)-[:KNOWS]->(b);")
conn.execute("MATCH (a:Person {id: 1}), (b:Person {id: 3}) CREATE (a)-[:KNOWS]->(b);")
```

#### 2. 运行 WCC（弱连通分量）

```python
# 运行 WCC 算法
result = conn.execute('''
    CALL wcc() 
    YIELD node, component_id 
    RETURN node, component_id
    ORDER BY component_id
''')

for row in result:
    print(f"Node {row[0]} -> Component {row[1]}")
```

#### 3. 运行 PageRank

```python
# 运行 PageRank 算法
result = conn.execute('''
    CALL page_rank() 
    YIELD node, rank 
    RETURN node, rank
    ORDER BY rank DESC
    LIMIT 5
''')

print("Top 5 nodes by PageRank:")
for row in result:
    print(f"  Node {row[0]}: rank = {row[1]:.6f}")
```

#### 4. 运行 BFS（广度优先搜索）

```python
# 从节点 1 开始 BFS
result = conn.execute('''
    CALL bfs(1) 
    YIELD node, distance 
    RETURN node, distance
    ORDER BY distance
''')

print("BFS from node 1:")
for row in result:
    print(f"  Node {row[0]}: distance = {row[1]}")
```

#### 5. 运行 LCC（局部聚类系数）

```python
# 计算 LCC
result = conn.execute('''
    CALL lcc() 
    YIELD node, coefficient 
    RETURN node, coefficient
    ORDER BY coefficient DESC
''')

print("Local Clustering Coefficient:")
for row in result:
    print(f"  Node {row[0]}: LCC = {row[1]:.4f}")
```

#### 6. 运行 CDLP（标签传播社区发现）

```python
# 运行标签传播
result = conn.execute('''
    CALL label_propagation() 
    YIELD node, label 
    RETURN label, count(node) as community_size
    ORDER BY community_size DESC
''')

print("Communities found:")
for row in result:
    print(f"  Community {row[0]}: {row[1]} nodes")
```

---

## 端到端示例

以下是一个完整的端到端示例，演示如何从零开始构建图并运行所有 GDS 算法：

```python
#!/usr/bin/env python3
"""
NeuG GDS Extension - 端到端示例
演示如何加载图数据并运行图算法
"""

import sys
import os
import shutil

# 添加 NeuG Python 绑定路径
sys.path.insert(0, '/path/to/neug/tools/python_bind')

from neug.database import Database

# 配置
DB_PATH = '/tmp/neug_gds_demo'

def main():
    # 清理旧数据库
    if os.path.exists(DB_PATH):
        shutil.rmtree(DB_PATH)
    
    # 1. 创建数据库
    print("=" * 60)
    print("NeuG GDS Extension - 端到端示例")
    print("=" * 60)
    
    db = Database(DB_PATH, 'w')
    conn = db.connect()
    
    # 2. 加载 GDS 扩展
    print("\n[1/6] 加载 GDS 扩展...")
    conn.execute('LOAD gds;', 'schema')
    print("      ✓ GDS 扩展已加载")
    
    # 3. 创建图 Schema
    print("\n[2/6] 创建图 Schema...")
    conn.execute('CREATE NODE TABLE Person (id INT64 PRIMARY KEY, name STRING);')
    conn.execute('CREATE REL TABLE KNOWS (FROM Person TO Person);')
    print("      ✓ Schema 创建完成")
    
    # 4. 加载数据
    print("\n[3/6] 加载图数据...")
    
    # 创建社交网络图
    # Alice <-> Bob <-> Charlie <-> David
    # Alice <-> Charlie (形成三角形)
    people = [
        (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), 
        (4, 'David'), (5, 'Eve'), (6, 'Frank')
    ]
    
    for pid, name in people:
        conn.execute(f"CREATE (:Person {{id: {pid}, name: '{name}'}});")
    
    # 添加边（无向图用双向边表示）
    edges = [
        (1, 2), (2, 1),  # Alice <-> Bob
        (2, 3), (3, 2),  # Bob <-> Charlie
        (1, 3), (3, 1),  # Alice <-> Charlie (三角形)
        (3, 4), (4, 3),  # Charlie <-> David
        (5, 6), (6, 5),  # Eve <-> Frank (独立分量)
    ]
    
    for src, dst in edges:
        conn.execute(f"MATCH (a:Person {{id: {src}}}), (b:Person {{id: {dst}}}) CREATE (a)-[:KNOWS]->(b);")
    
    # 验证数据
    result = conn.execute("MATCH (n) RETURN count(n);")
    v_count = list(result)[0][0]
    result = conn.execute("MATCH ()-[e]->() RETURN count(e);")
    e_count = list(result)[0][0]
    print(f"      ✓ 已加载 {v_count} 个顶点, {e_count} 条边")
    
    # 5. 运行图算法
    print("\n[4/6] 运行图算法...")
    
    # 5.1 WCC - 弱连通分量
    print("\n  ▶ WCC (弱连通分量)")
    result = conn.execute('''
        CALL wcc() YIELD node, component_id
        RETURN node, component_id
        ORDER BY component_id, node
    ''')
    components = {}
    for row in result:
        node, comp = row[0], row[1]
        if comp not in components:
            components[comp] = []
        components[comp].append(node)
    
    print(f"    发现 {len(components)} 个连通分量:")
    for comp, nodes in sorted(components.items()):
        print(f"      分量 {comp}: {nodes}")
    
    # 5.2 PageRank
    print("\n  ▶ PageRank")
    result = conn.execute('''
        CALL page_rank() YIELD node, rank
        RETURN node, rank
        ORDER BY rank DESC
    ''')
    print("    节点重要性排名:")
    for row in result:
        print(f"      节点 {row[0]}: rank = {row[1]:.6f}")
    
    # 5.3 BFS
    print("\n  ▶ BFS (从节点 1 开始)")
    result = conn.execute('''
        CALL bfs(1) YIELD node, distance
        RETURN node, distance
        ORDER BY distance
    ''')
    print("    广度优先遍历结果:")
    for row in result:
        print(f"      节点 {row[0]}: 距离 = {row[1]}")
    
    # 5.4 LCC
    print("\n  ▶ LCC (局部聚类系数)")
    result = conn.execute('''
        CALL lcc() YIELD node, coefficient
        RETURN node, coefficient
        ORDER BY node
    ''')
    print("    聚类系数:")
    for row in result:
        print(f"      节点 {row[0]}: LCC = {row[1]:.4f}")
    
    # 5.5 CDLP
    print("\n  ▶ CDLP (标签传播)")
    result = conn.execute('''
        CALL label_propagation() YIELD node, label
        RETURN label, count(node) as size
        ORDER BY size DESC
    ''')
    print("    社区发现结果:")
    for row in result:
        print(f"      社区 {row[0]}: {row[1]} 个节点")
    
    # 5.6 K-Core
    print("\n  ▶ K-Core")
    result = conn.execute('''
        CALL k_core() YIELD node, core_number
        RETURN node, core_number
        ORDER BY core_number DESC, node
    ''')
    print("    核数分解:")
    for row in result:
        print(f"      节点 {row[0]}: core = {row[1]}")
    
    # 6. 清理
    print("\n[5/6] 关闭连接...")
    conn.close()
    db.close()
    print("      ✓ 连接已关闭")
    
    print("\n[6/6] 清理数据库...")
    shutil.rmtree(DB_PATH)
    print("      ✓ 数据库已清理")
    
    print("\n" + "=" * 60)
    print("示例完成！")
    print("=" * 60)

if __name__ == "__main__":
    main()
```

将上述代码保存为 `gds_demo.py`，然后运行：

```bash
python3 gds_demo.py
```

预期输出：

```
============================================================
NeuG GDS Extension - 端到端示例
============================================================

[1/6] 加载 GDS 扩展...
      ✓ GDS 扩展已加载

[2/6] 创建图 Schema...
      ✓ Schema 创建完成

[3/6] 加载图数据...
      ✓ 已加载 6 个顶点, 10 条边

[4/6] 运行图算法...

  ▶ WCC (弱连通分量)
    发现 2 个连通分量:
      分量 0: [1, 2, 3, 4]
      分量 1: [5, 6]

  ▶ PageRank
    节点重要性排名:
      节点 3: rank = 0.216667
      节点 2: rank = 0.200000
      节点 1: rank = 0.183333
      ...

  ▶ BFS (从节点 1 开始)
    广度优先遍历结果:
      节点 1: 距离 = 0
      节点 2: 距离 = 1
      节点 3: 距离 = 1
      节点 4: 距离 = 2

  ▶ LCC (局部聚类系数)
    聚类系数:
      节点 1: LCC = 1.0000
      节点 2: LCC = 1.0000
      节点 3: LCC = 0.3333
      ...

  ▶ CDLP (标签传播)
    社区发现结果:
      社区 1: 4 个节点
      社区 5: 2 个节点

  ▶ K-Core
    核数分解:
      节点 1: core = 2
      节点 2: core = 2
      节点 3: core = 2
      ...

[5/6] 关闭连接...
      ✓ 连接已关闭

[6/6] 清理数据库...
      ✓ 数据库已清理

============================================================
示例完成！
============================================================
```

---

## 算法参考

| 算法 | 函数名 | 图语义 | 描述 | 输出 |
|------|--------|--------|------|------|
| WCC | `wcc()` | 无向 | 弱连通分量 | `(node, component_id)` |
| PageRank | `page_rank()` | 有向 | 节点重要性 | `(node, rank)` |
| BFS | `bfs(source)` | 有向 | 广度优先搜索 | `(node, distance)` |
| LCC | `lcc()` | 无向 | 局部聚类系数 | `(node, coefficient)` |
| CDLP | `label_propagation()` | 无向 | 标签传播社区发现 | `(node, label)` |
| K-Core | `k_core()` | 无向 | 核数分解 | `(node, core_number)` |
| SSSP | `shortest_path(source)` | 有向 | 单源最短路径 | `(node, distance)` |
| Leiden | `leiden()` | 无向 | 高质量社区发现 | `(node, community_id)` |

### 图语义说明

- **有向图语义**：只使用出边 (`out_neighbors()`)
- **无向图语义**：使用出边和入边 (`out_neighbors() ∪ in_neighbors()`)

NeuG 底层存储为有向图，无向图算法会自动处理双向边。

---

## 性能优化

### 并行执行

GDS 扩展自动使用多线程并行执行：

```python
# 自动检测 CPU 核心数
import os
print(f"可用 CPU 核心数: {os.cpu_count()}")
```

算法内部使用 `std::thread::hardware_concurrency()` 确定线程数。

### 大规模图处理

对于大规模图（如 Graph500-23/24），建议：

1. **增加内存**：确保有足够的 RAM 存储图数据
2. **使用 SSD**：数据库文件放在 SSD 上
3. **调整并发**：根据 CPU 核心数调整线程数

---

## 故障排除

### 常见问题

**Q: `LOAD gds` 失败**

确保数据库以 write 模式打开：
```python
db = Database(DB_PATH, 'w')  # 注意 'w' 模式
```

**Q: 找不到扩展库**

检查扩展库路径是否正确：
```bash
ls tools/python_bind/build/lib.*/extension/gds/libgds.neug_extension
```

**Q: 算法结果不正确**

对于无向图算法（WCC, LCC, K-Core），确保边是双向存储的：
```python
# 无向图：添加双向边
conn.execute("MATCH (a), (b) CREATE (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a);")
```

---

## 更多资源

- [NeuG 文档](https://graphscope.io/neug/)
- [LDBC Graphalytics](https://ldbcouncil.org/benchmarks/graphalytics/)
- [GitHub Issues](https://github.com/alibaba/neug/issues)