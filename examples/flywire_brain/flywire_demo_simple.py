#!/usr/bin/env python3
"""
FlyWire Brain Simulation Demo - Simplified Version

This script demonstrates:
1. NeuG as the single source of truth for brain data
2. LIF neural simulation via lif_sim extension
3. Signal propagation through the connectome

Prerequisites:
- Install neug Python package: pip install -e /path/to/neug/tools/python_bind
- Build lif_sim extension: cd neug/extension/lif_sim && mkdir build && cd build && cmake .. && make
"""

import os
import sys
from pathlib import Path

import neug


def run_experiment(conn, neuron_type, stimulus_mv=20.0, max_rounds=20):
    """Run a stimulation experiment."""
    print(f"\n{'='*70}")
    print(f"Experiment: {neuron_type} Stimulation")
    print("=" * 70)
    
    # Find neurons
    print(f"\n1. Finding {neuron_type} neurons...")
    result = conn.execute(f"""
        MATCH (n:Neuron)
        WHERE n.sub_class CONTAINS '{neuron_type.lower()}' AND n.flow = 'afferent'
        RETURN n.root_id, n.sub_class
        LIMIT 20;
    """)
    neurons = list(result)
    print(f"   Found {len(neurons)} {neuron_type} neurons")
    
    if not neurons:
        print(f"   No {neuron_type} neurons found, skipping")
        return None
    
    # Initialize simulation
    print("\n2. Initializing simulation...")
    result = conn.execute("CALL LIF_INIT() RETURN *;")
    init_count = list(result)[0][0]
    print(f"   Initialized {init_count:,} neurons")
    
    # Set stimulus
    print(f"\n3. Setting stimulus ({stimulus_mv} mV)...")
    for row in neurons:
        neuron_id = row[0]
        conn.execute(f"CALL LIF_SET_STIMULUS({neuron_id}, {stimulus_mv}) RETURN *;")
    print(f"   Stimulated {len(neurons)} neurons")
    
    # Run simulation
    print(f"\n4. Running LIF simulation ({max_rounds} rounds)...")
    result = conn.execute(f"CALL LIF_SIMULATE({max_rounds}) RETURN *;")
    spiked = list(result)
    
    # Analyze results
    rounds = {}
    for row in spiked:
        neuron_id, spiked_flag, spike_count, spike_round = row
        if spike_round not in rounds:
            rounds[spike_round] = []
        rounds[spike_round].append(neuron_id)
    
    print(f"\n5. Results:")
    print(f"   Total spiked: {len(spiked):,} neurons")
    print(f"   Propagation factor: {len(spiked) / len(neurons):.1f}x")
    
    print(f"\n   Spike propagation by round:")
    for r in sorted(rounds.keys()):
        print(f"     Round {r:2d}: {len(rounds[r]):,} neurons spiked")
    
    return {
        "name": neuron_type,
        "stimulated": len(neurons),
        "spiked": len(spiked),
        "rounds": rounds
    }


def main():
    # Use relative path or environment variable for database path
    db_path = os.environ.get("FLYWIRE_DB_PATH", "./flywire_db")

    print("=" * 70)
    print("FlyWire Brain Simulation Demo")
    print("NeuG + lif_sim Extension")
    print("=" * 70)
    print(f"\nDatabase: {db_path}")
    
    # LIF Parameters
    print("\nLIF Parameters (Shiu et al. Nature 2024):")
    print("  v_rest      = -52.0 mV")
    print("  v_threshold = -45.0 mV")
    print("  w_syn       = 0.275 mV per synapse")
    print("  Neurotransmitter signs: ACH=+1, GABA=-1, GLUT=-1")
    
    # Open database
    print("\nOpening NeuG database...")
    db = neug.Database(db_path)
    conn = db.connect()
    
    # Load extension
    print("Loading lif_sim extension...")
    conn.execute("LOAD lif_sim;")
    print("Extension loaded successfully!")
    
    # Run experiments
    results = []
    
    # Experiment 1: Sugar GRN
    result = run_experiment(conn, "sugar", stimulus_mv=20.0, max_rounds=20)
    if result:
        results.append(result)
    
    # Experiment 2: Bitter GRN
    result = run_experiment(conn, "bitter", stimulus_mv=20.0, max_rounds=20)
    if result:
        results.append(result)
    
    # Summary
    print("\n" + "=" * 70)
    print("Demo Complete!")
    print("=" * 70)
    
    print("\nKey Results:")
    for result in results:
        print(f"\n  {result['name']}:")
        print(f"    Stimulated: {result['stimulated']} neurons")
        print(f"    Total spiked: {result['spiked']:,} neurons")
        print(f"    Propagation factor: {result['spiked'] / result['stimulated']:.1f}x")
    
    print("\nNeuG Advantages Demonstrated:")
    print("  1. Single source of truth - all data in NeuG")
    print("  2. CSR storage for efficient neighbor traversal")
    print("  3. Extension framework for custom simulation")
    print("  4. Cypher queries for data analysis")
    
    # Close connection
    conn.close()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())