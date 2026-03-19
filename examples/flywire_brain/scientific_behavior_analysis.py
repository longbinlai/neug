#!/usr/bin/env python3
"""
科学的行为预测方法

基于数据限制和论文方法，设计一个合理的行为预测流程：

1. 数据限制：
   - 没有 MN9 的精确标注
   - 有 proboscis_motor_neuron (24个) 和 ingestion_motor_neuron (28个)
   - 这些可以作为进食行为输出的代理

2. 改进方法：
   - 测量发放频率，而不是"是否发放"
   - 分析不同刺激下运动神经元的发放模式
   - 追踪信号传播路径

3. 科学解释：
   - 不做过度声称
   - 说明限制
   - 与论文方法对比

Prerequisites:
- Install neug Python package: pip install -e /path/to/neug/tools/python_bind
- Build lif_sim extension: cd neug/extension/lif_sim && mkdir build && cd build && cmake .. && make
"""

import os
import sys
from collections import defaultdict

import neug


def get_motor_neurons(conn):
    """获取运动神经元"""
    result = conn.execute("""
        MATCH (n:Neuron)
        WHERE n.neuron_class = 'brain_motor_neuron'
        RETURN n.root_id, n.sub_class;
    """)
    
    motor_neurons = {}
    for row in result:
        nid, sub_class = row
        if sub_class not in motor_neurons:
            motor_neurons[sub_class] = []
        motor_neurons[sub_class].append(nid)
    
    return motor_neurons


def get_sensory_neurons(conn, taste_type):
    """获取感觉神经元"""
    if taste_type == 'sugar':
        query = """
            MATCH (n:Neuron)
            WHERE n.sub_class CONTAINS 'sugar' AND n.flow = 'afferent'
            RETURN n.root_id
            LIMIT 20;
        """
    elif taste_type == 'bitter':
        query = """
            MATCH (n:Neuron)
            WHERE n.sub_class = 'bitter' AND n.flow = 'afferent'
            RETURN n.root_id
            LIMIT 20;
        """
    else:
        return []
    
    result = conn.execute(query)
    return [row[0] for row in result]


def simulate_and_measure(conn, sensory_ids, motor_neurons):
    """模拟并测量运动神经元的发放模式"""
    conn.execute("CALL LIF_INIT() RETURN *;")
    
    for nid in sensory_ids:
        conn.execute(f"CALL LIF_SET_STIMULUS({nid}, 20.0) RETURN *;")
    
    result = conn.execute("CALL LIF_SIMULATE(20) RETURN *;")
    spiked = list(result)
    
    # 分析每个运动神经元类型的发放
    motor_spikes = defaultdict(lambda: {'count': 0, 'neurons': 0, 'rounds': []})
    
    for row in spiked:
        neuron_id, spiked_flag, spike_count, spike_round = row
        for motor_type, ids in motor_neurons.items():
            if neuron_id in ids:
                motor_spikes[motor_type]['count'] += spike_count
                motor_spikes[motor_type]['neurons'] += 1
                motor_spikes[motor_type]['rounds'].append(spike_round)
    
    return motor_spikes, len(spiked)


def analyze_pathway(conn, sensory_ids, motor_ids):
    """分析感觉神经元到运动神经元的路径"""
    # 找到直接连接
    direct_connections = defaultdict(int)
    
    for sid in sensory_ids[:5]:  # 只分析前5个
        result = conn.execute(f"""
            MATCH (s:Neuron {{root_id: {sid}}})-[r:SYNAPSE*1..3]->(m:Neuron)
            WHERE m.root_id IN [{','.join(str(x) for x in list(motor_ids)[:20])}]
            RETURN m.root_id, length(r) as path_length, sum(r.syn_count) as total_syn
            ORDER BY total_syn DESC
            LIMIT 10;
        """)
        
        for row in result:
            mid, path_len, syn_count = row
            direct_connections[mid] += syn_count
    
    return direct_connections


def main():
    # Use relative path or environment variable for database path
    db_path = os.environ.get("FLYWIRE_DB_PATH", "./flywire_db")

    print("=" * 70)
    print("科学的行为预测方法")
    print("=" * 70)
    
    db = neug.Database(db_path)
    conn = db.connect()
    conn.execute("LOAD lif_sim;")
    
    # 1. 获取运动神经元
    print("\n1. 运动神经元分布:")
    print("-" * 50)
    motor_neurons = get_motor_neurons(conn)
    
    all_motor_ids = []
    for motor_type, ids in motor_neurons.items():
        print(f"  {motor_type}: {len(ids)} 个")
        all_motor_ids.extend(ids)
    
    # 2. 获取感觉神经元
    print("\n2. 感觉神经元:")
    print("-" * 50)
    sugar_ids = get_sensory_neurons(conn, 'sugar')
    bitter_ids = get_sensory_neurons(conn, 'bitter')
    
    print(f"  糖味 GRN: {len(sugar_ids)} 个")
    print(f"  苦味 GRN: {len(bitter_ids)} 个")
    
    # 3. 模拟糖味刺激
    print("\n3. 糖味刺激模拟:")
    print("-" * 50)
    sugar_motor_spikes, sugar_total = simulate_and_measure(conn, sugar_ids, motor_neurons)
    
    print(f"  总发放神经元: {sugar_total:,}")
    print(f"\n  运动神经元发放详情:")
    for motor_type, data in sorted(sugar_motor_spikes.items(), key=lambda x: -x[1]['count']):
        print(f"    {motor_type}:")
        print(f"      发放神经元数: {data['neurons']}")
        print(f"      总发放次数: {data['count']}")
        if data['rounds']:
            print(f"      发放轮次: {sorted(set(data['rounds']))}")
    
    # 4. 模拟苦味刺激
    print("\n4. 苦味刺激模拟:")
    print("-" * 50)
    bitter_motor_spikes, bitter_total = simulate_and_measure(conn, bitter_ids, motor_neurons)
    
    print(f"  总发放神经元: {bitter_total:,}")
    print(f"\n  运动神经元发放详情:")
    for motor_type, data in sorted(bitter_motor_spikes.items(), key=lambda x: -x[1]['count']):
        print(f"    {motor_type}:")
        print(f"      发放神经元数: {data['neurons']}")
        print(f"      总发放次数: {data['count']}")
        if data['rounds']:
            print(f"      发放轮次: {sorted(set(data['rounds']))}")
    
    # 5. 对比分析
    print("\n5. 对比分析:")
    print("-" * 50)
    
    print(f"\n{'运动神经元类型':<25} {'糖味发放':<15} {'苦味发放':<15} {'差异':<10}")
    print("-" * 65)
    
    all_types = set(sugar_motor_spikes.keys()) | set(bitter_motor_spikes.keys())
    for motor_type in sorted(all_types):
        sugar_count = sugar_motor_spikes.get(motor_type, {}).get('count', 0)
        bitter_count = bitter_motor_spikes.get(motor_type, {}).get('count', 0)
        diff = sugar_count - bitter_count
        diff_str = f"+{diff}" if diff > 0 else str(diff)
        print(f"{motor_type:<25} {sugar_count:<15} {bitter_count:<15} {diff_str:<10}")
    
    # 6. 科学解释
    print("\n" + "=" * 70)
    print("科学解释")
    print("=" * 70)
    
    # 计算关键指标
    sugar_proboscis = sugar_motor_spikes.get('proboscis_motor_neuron', {}).get('count', 0)
    bitter_proboscis = bitter_motor_spikes.get('proboscis_motor_neuron', {}).get('count', 0)
    
    sugar_ingestion = sugar_motor_spikes.get('ingestion_motor_neuron', {}).get('count', 0)
    bitter_ingestion = bitter_motor_spikes.get('ingestion_motor_neuron', {}).get('count', 0)
    
    print(f"""
┌─────────────────────────────────────────────────────────────────────────┐
│                         实验结果总结                                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  吻部运动神经元 (proboscis_motor_neuron):                               │
│    糖味刺激发放次数: {sugar_proboscis}                                    │
│    苦味刺激发放次数: {bitter_proboscis}                                    │
│                                                                         │
│  进食运动神经元 (ingestion_motor_neuron):                                │
│    糖味刺激发放次数: {sugar_ingestion}                                    │
│    苦味刺激发放次数: {bitter_ingestion}                                    │
│                                                                         │
├─────────────────────────────────────────────────────────────────────────┤
│                         科学解释                                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  1. 数据限制:                                                           │
│     - 我们的数据中没有 MN9 的精确标注                                    │
│     - 只有 proboscis_motor_neuron 和 ingestion_motor_neuron             │
│     - 这些是运动神经元类型，不是特定的单个神经元                          │
│                                                                         │
│  2. 模拟限制:                                                           │
│     - 当前 LIF 模拟只测量"是否发放"                                      │
│     - 论文测量的是 MN9 的发放频率 (Hz)                                   │
│     - 发放频率比"是否发放"更能反映行为强度                               │
│                                                                         │
│  3. 为什么不能简单比较数字:                                              │
│     - "糖味发放 X 次，苦味发放 Y 次" 不能直接预测行为                    │
│     - 行为由特定神经元 (MN9) 的发放频率决定                              │
│     - 不是由运动神经元群体的总发放次数决定                               │
│                                                                         │
│  4. 论文的正确方法:                                                      │
│     - 识别特定的 MN9 神经元                                             │
│     - 测量 MN9 的发放频率                                               │
│     - 通过光遗传学验证预测                                              │
│     - 预测准确率 > 90%                                                  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘

结论:
─────
我们的模拟展示了信号从感觉神经元传播到运动神经元的过程，
但不能精确预测特定行为（如伸吻反应）。

要实现论文中的行为预测，需要:
1. MN9 的精确标注（可能需要从论文补充数据获取）
2. 发放频率的测量（需要改进 LIF 模拟）
3. 实验验证

我们诚实地说明这些限制，而不是做出没有科学依据的行为预测。
""")
    
    conn.close()


if __name__ == "__main__":
    main()