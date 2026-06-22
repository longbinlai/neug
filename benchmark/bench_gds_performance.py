#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Performance benchmark for GDS extension (WCC algorithm) on FlyWire dataset.
"""

import logging
import os
import sys
import time
import statistics

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_REPO_ROOT, "tools", "python_bind"))

from neug.database import Database

logger = logging.getLogger(__name__)


def benchmark_wcc(db_path: str, num_runs: int = 3):
    """Benchmark WCC algorithm on the given database."""
    logger.info(f"Benchmarking WCC on {db_path}")
    
    # Open database
    db = Database(db_path, "w")
    conn = db.connect()
    
    # Load GDS extension
    try:
        conn.execute("LOAD gds;", "schema")
        logger.info("GDS extension loaded successfully")
    except Exception as e:
        logger.error(f"Failed to load GDS extension: {e}")
        conn.close()
        db.close()
        return None
    
    # Warm up
    logger.info("Warming up...")
    try:
        result = conn.execute(
            "CALL wcc() YIELD node, component_id RETURN count(*);",
            "read"
        )
        for _ in result:
            pass
        logger.info("Warm up completed")
    except Exception as e:
        logger.error(f"Warm up failed: {e}")
        conn.close()
        db.close()
        return None
    
    # Run benchmark
    times = []
    component_counts = []
    
    for i in range(num_runs):
        start = time.time()
        result = conn.execute(
            "CALL wcc() YIELD node, component_id RETURN component_id, count(*) as size ORDER BY size DESC LIMIT 5;",
            "read"
        )
        
        components = []
        for record in result:
            components.append((record[0], record[1]))
        
        elapsed = time.time() - start
        times.append(elapsed)
        component_counts.append(components)
        
        logger.info(f"Run {i+1}: {elapsed:.4f}s")
        logger.info(f"  Top 5 components: {components}")
    
    # Calculate statistics
    avg_time = statistics.mean(times)
    std_time = statistics.stdev(times) if len(times) > 1 else 0
    min_time = min(times)
    max_time = max(times)
    
    logger.info(f"\n=== Benchmark Results ===")
    logger.info(f"Runs: {num_runs}")
    logger.info(f"Average time: {avg_time:.4f}s")
    logger.info(f"Std deviation: {std_time:.4f}s")
    logger.info(f"Min time: {min_time:.4f}s")
    logger.info(f"Max time: {max_time:.4f}s")
    
    # Get graph statistics
    try:
        result = conn.execute("MATCH (n) RETURN count(n);", "read")
        for record in result:
            vertex_count = record[0]
            logger.info(f"Vertex count: {vertex_count}")
        
        result = conn.execute("MATCH ()-[e]->() RETURN count(e);", "read")
        for record in result:
            edge_count = record[0]
            logger.info(f"Edge count: {edge_count}")
    except Exception as e:
        logger.warning(f"Failed to get graph statistics: {e}")
    
    conn.close()
    db.close()
    
    return {
        "avg_time": avg_time,
        "std_time": std_time,
        "min_time": min_time,
        "max_time": max_time,
        "times": times,
        "components": component_counts[0] if component_counts else []
    }


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Benchmark on modern_graph (small)
    logger.info("\n" + "="*60)
    logger.info("Benchmarking on modern_graph (small dataset)")
    logger.info("="*60)
    modern_result = benchmark_wcc("/tmp/modern_graph", num_runs=3)
    
    # Benchmark on flywire (large)
    logger.info("\n" + "="*60)
    logger.info("Benchmarking on flywire (large dataset)")
    logger.info("="*60)
    flywire_result = benchmark_wcc("/tmp/flywire", num_runs=3)
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("Summary")
    logger.info("="*60)
    
    if modern_result:
        logger.info(f"modern_graph: avg={modern_result['avg_time']:.4f}s")
    
    if flywire_result:
        logger.info(f"flywire: avg={flywire_result['avg_time']:.4f}s")
    
    # Comparison with LadybugDB (if available)
    logger.info("\n" + "="*60)
    logger.info("Note: For comparison with LadybugDB, please run the same WCC")
    logger.info("algorithm on the same dataset and compare the results.")
    logger.info("="*60)


if __name__ == "__main__":
    main()