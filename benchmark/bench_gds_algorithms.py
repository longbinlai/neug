#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GDS Extension Benchmark Script

This script benchmarks all implemented GDS algorithms on the FlyWire dataset
(139K neurons, 2.7M synapses) and compares with LadybugDB where applicable.
"""

import sys
import os
import time
import statistics
import shutil

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_REPO_ROOT, "tools", "python_bind"))

from neug.database import Database


def check_flywire_data():
    """Check if FlyWire data exists."""
    neurons_file = '/tmp/neurons.csv'
    synapses_file = '/tmp/synapses.csv'
    
    if not os.path.exists(neurons_file) or not os.path.exists(synapses_file):
        print("FlyWire data not found. Please extract from flywire_dataset.tar.gz")
        print("Expected files:")
        print(f"  - {neurons_file}")
        print(f"  - {synapses_file}")
        return False
    return True


def load_flywire_to_neug(db_path):
    """Load FlyWire dataset into NeuG."""
    print("\n" + "=" * 60)
    print("Loading FlyWire Dataset into NeuG")
    print("=" * 60)
    
    # Check if database already exists
    if os.path.exists(db_path):
        print(f"Using existing database at {db_path}")
        db = Database(db_path, 'w')
        conn = db.connect()
        conn.execute('LOAD gds;', 'schema')
        
        # Get stats
        result = conn.execute("MATCH (n) RETURN count(n);")
        vertex_count = list(result)[0][0]
        
        result = conn.execute("MATCH ()-[e]->() RETURN count(e);")
        edge_count = list(result)[0][0]
        
        print(f"\nDatabase has {vertex_count:,} neurons and {edge_count:,} synapses")
        
        conn.close()
        db.close()
        
        return vertex_count, edge_count
    
    # Create new database
    db = Database(db_path, 'w')
    conn = db.connect()
    
    # Load GDS extension
    conn.execute('LOAD gds;', 'schema')
    
    # Create schema
    print("Creating schema...")
    conn.execute('''
        CREATE NODE TABLE Neuron (
            root_id INT64 PRIMARY KEY,
            neuron_group STRING,
            side STRING,
            flow STRING,
            super_class STRING,
            neuron_class STRING,
            sub_class STRING,
            nt_type STRING,
            ach_avg DOUBLE,
            gaba_avg DOUBLE,
            glut_avg DOUBLE,
            input_cells INT64,
            output_cells INT64,
            input_synapses INT64,
            output_synapses INT64,
            length_nm DOUBLE,
            area_nm DOUBLE,
            size_nm DOUBLE
        );
    ''')
    
    conn.execute('''
        CREATE REL TABLE SYNAPSE (
            FROM Neuron TO Neuron,
            neuropil STRING,
            syn_count INT64,
            nt_type STRING,
            ach_avg DOUBLE,
            gaba_avg DOUBLE,
            glut_avg DOUBLE
        );
    ''')
    
    # Load data
    print("Loading neurons...")
    conn.execute("COPY Neuron FROM '/tmp/neurons.csv' (header=true);")
    
    print("Loading synapses...")
    conn.execute("COPY SYNAPSE FROM '/tmp/synapses.csv' (header=true);")
    
    # Get stats
    result = conn.execute("MATCH (n) RETURN count(n);")
    vertex_count = list(result)[0][0]
    
    result = conn.execute("MATCH ()-[e]->() RETURN count(e);")
    edge_count = list(result)[0][0]
    
    print(f"\nLoaded {vertex_count:,} neurons and {edge_count:,} synapses")
    
    conn.close()
    db.close()
    
    return vertex_count, edge_count


def benchmark_algorithm(db_path, algo_name, query, num_runs=3, warmup=True):
    """Benchmark a single algorithm."""
    db = Database(db_path, 'w')
    conn = db.connect()
    conn.execute('LOAD gds;', 'schema')
    
    # Warmup
    if warmup:
        try:
            result = conn.execute(query)
            for _ in result:
                pass
        except Exception as e:
            print(f"  Warmup failed: {e}")
            conn.close()
            db.close()
            return None, None
    
    # Benchmark
    times = []
    result_count = 0
    
    for i in range(num_runs):
        start = time.time()
        try:
            result = conn.execute(query)
            rows = list(result)
            result_count = len(rows)
        except Exception as e:
            print(f"  Run {i+1} failed: {e}")
            conn.close()
            db.close()
            return None, None
        elapsed = time.time() - start
        times.append(elapsed)
        print(f"  Run {i+1}: {elapsed:.4f}s ({result_count} results)")
    
    avg_time = statistics.mean(times)
    std_time = statistics.stdev(times) if len(times) > 1 else 0
    
    conn.close()
    db.close()
    
    return avg_time, std_time


def run_benchmarks(db_path, num_runs=3):
    """Run all algorithm benchmarks."""
    print("\n" + "=" * 60)
    print("Running GDS Algorithm Benchmarks")
    print("=" * 60)
    
    algorithms = [
        ("WCC", "CALL wcc() YIELD node, component_id RETURN component_id, count(*) as size ORDER BY size DESC LIMIT 5;"),
        ("PageRank", "CALL page_rank() YIELD node, rank RETURN node, rank ORDER BY rank DESC LIMIT 10;"),
        ("BFS", "CALL bfs(1) YIELD node, distance RETURN node, distance ORDER BY distance LIMIT 10;"),
        ("K-Core", "CALL k_core() YIELD node, core_number RETURN node, core_number ORDER BY core_number DESC LIMIT 10;"),
        ("Label Propagation", "CALL label_propagation() YIELD node, label RETURN label, count(*) as size ORDER BY size DESC LIMIT 5;"),
        ("LCC", "CALL lcc() YIELD node, coefficient RETURN node, coefficient ORDER BY coefficient DESC LIMIT 10;"),
        ("SSSP", "CALL shortest_path(1) YIELD node, distance RETURN node, distance ORDER BY distance LIMIT 10;"),
        ("Leiden", "CALL leiden() YIELD node, community_id RETURN community_id, count(*) as size ORDER BY size DESC LIMIT 5;"),
    ]
    
    results = {}
    
    for algo_name, query in algorithms:
        print(f"\n--- {algo_name} ---")
        avg_time, std_time = benchmark_algorithm(db_path, algo_name, query, num_runs)
        
        if avg_time is not None:
            results[algo_name] = {
                'avg_time': avg_time,
                'std_time': std_time
            }
            print(f"  Average: {avg_time:.4f}s (±{std_time:.4f}s)")
        else:
            print(f"  FAILED")
            results[algo_name] = {
                'avg_time': None,
                'std_time': None
            }
    
    return results


def print_summary(results, vertex_count, edge_count):
    """Print benchmark summary."""
    print("\n" + "=" * 60)
    print("BENCHMARK SUMMARY")
    print("=" * 60)
    
    print(f"\nDataset: FlyWire ({vertex_count:,} neurons, {edge_count:,} synapses)")
    print("\nAlgorithm Performance:")
    print("-" * 40)
    print(f"{'Algorithm':<25} {'Time (s)':<15}")
    print("-" * 40)
    
    for algo_name, data in results.items():
        if data['avg_time'] is not None:
            print(f"{algo_name:<25} {data['avg_time']:.4f} ± {data['std_time']:.4f}")
        else:
            print(f"{algo_name:<25} FAILED")
    
    print("-" * 40)
    
    # Calculate throughput
    print("\nThroughput (edges/second):")
    print("-" * 40)
    for algo_name, data in results.items():
        if data['avg_time'] is not None and data['avg_time'] > 0:
            throughput = edge_count / data['avg_time']
            print(f"{algo_name:<25} {throughput:,.0f} edges/s")
    
    print("-" * 40)


def compare_with_ladybugdb():
    """Compare with LadybugDB if available."""
    print("\n" + "=" * 60)
    print("LadybugDB Comparison")
    print("=" * 60)
    
    try:
        import real_ladybug
        print(f"LadybugDB version: {real_ladybug.__version__ if hasattr(real_ladybug, '__version__') else 'unknown'}")
        
        # Create in-memory database
        db = real_ladybug.Database(':memory:')
        conn = real_ladybug.Connection(db)
        
        # Install ALGO extension
        print("Installing ALGO extension...")
        conn.execute('INSTALL ALGO;')
        conn.execute('LOAD ALGO;')
        print("ALGO extension loaded!")
        
        # Check available functions
        result = conn.execute("CALL show_functions() RETURN *;")
        gds_functions = []
        for row in result:
            if any(x in str(row[0]).upper() for x in ['WCC', 'PAGERANK', 'BFS', 'KCORE', 'LEIDEN', 'LCC', 'SSSP']):
                gds_functions.append(row[0])
        
        print(f"\nAvailable GDS functions: {gds_functions}")
        
        conn.close()
        db.close()
        
        return True
        
    except ImportError:
        print("LadybugDB not available (pip install real_ladybug)")
        return False
    except Exception as e:
        print(f"LadybugDB comparison failed: {e}")
        return False


def main():
    print("=" * 60)
    print("GDS Extension Benchmark")
    print("=" * 60)
    
    # Check data
    if not check_flywire_data():
        print("\nExtracting FlyWire data from archive...")
        import tarfile
        archive_path = '/Users/robeenly/Documents/git/neug/flywire_dataset.tar.gz'
        if os.path.exists(archive_path):
            with tarfile.open(archive_path, 'r:gz') as tar:
                tar.extractall('/tmp/')
            print("Data extracted!")
        else:
            print(f"Archive not found: {archive_path}")
            return 1
    
    db_path = '/tmp/flywire'
    
    # Load data
    vertex_count, edge_count = load_flywire_to_neug(db_path)
    
    # Run benchmarks
    results = run_benchmarks(db_path, num_runs=3)
    
    # Print summary
    print_summary(results, vertex_count, edge_count)
    
    # Compare with LadybugDB
    compare_with_ladybugdb()
    
    # Cleanup
    if os.path.exists(db_path):
        shutil.rmtree(db_path)
    
    print("\n" + "=" * 60)
    print("Benchmark Complete!")
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    exit(main())