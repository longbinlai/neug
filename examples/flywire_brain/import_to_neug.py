#!/usr/bin/env python3
"""
Import FlyWire data into NeuG database.

This script:
1. Creates the Neuron and SYNAPSE tables
2. Imports data from CSV files
3. Verifies data integrity

Prerequisites:
- Install neug Python package: pip install -e /path/to/neug/tools/python_bind

Usage:
    python import_to_neug.py [--data-dir DIR] [--db-path PATH]
"""

import argparse
import os
import sys
from pathlib import Path

import neug


def create_schema(conn):
    """Create Neuron and SYNAPSE tables."""
    print("Creating schema...")
    
    # Drop existing tables if they exist
    conn.execute("DROP TABLE IF EXISTS SYNAPSE;")
    conn.execute("DROP TABLE IF EXISTS Neuron;")
    
    # Create Neuron table
    # Note: 'group' and 'class' are Cypher reserved words, so we use 'neuron_group' and 'neuron_class'
    create_neuron = """
    CREATE NODE TABLE Neuron (
        root_id         INT64 PRIMARY KEY,
        neuron_group    STRING,
        side            STRING,
        flow            STRING,
        super_class     STRING,
        neuron_class    STRING,
        sub_class       STRING,
        nt_type         STRING,
        ach_avg         DOUBLE,
        gaba_avg        DOUBLE,
        glut_avg        DOUBLE,
        input_cells     INT64,
        output_cells    INT64,
        input_synapses  INT64,
        output_synapses INT64,
        length_nm       DOUBLE,
        area_nm         DOUBLE,
        size_nm         DOUBLE
    );
    """
    conn.execute(create_neuron)
    print("  Created Neuron table")
    
    # Create SYNAPSE relationship table
    create_synapse = """
    CREATE REL TABLE SYNAPSE (
        FROM Neuron TO Neuron,
        neuropil        STRING,
        syn_count       INT64,
        nt_type         STRING,
        ach_avg         DOUBLE,
        gaba_avg        DOUBLE,
        glut_avg        DOUBLE
    );
    """
    conn.execute(create_synapse)
    print("  Created SYNAPSE table")


def import_neurons(conn, data_dir: Path):
    """Import neurons from CSV."""
    neurons_file = data_dir / "neurons.csv"
    print(f"Importing neurons from {neurons_file}...")
    
    result = conn.execute(f"""
        COPY Neuron FROM "{neurons_file}" (header=true, delim=",");
    """)
    
    # Get count
    count_result = conn.execute("MATCH (n:Neuron) RETURN count(n);")
    count = list(count_result)[0][0]
    print(f"  Imported {count:,} neurons")
    return count


def import_synapses(conn, data_dir: Path):
    """Import synapses from CSV."""
    synapses_file = data_dir / "synapses.csv"
    print(f"Importing synapses from {synapses_file}...")
    
    result = conn.execute(f"""
        COPY SYNAPSE FROM "{synapses_file}" (header=true, delim=",");
    """)
    
    # Get count
    count_result = conn.execute("MATCH ()-[s:SYNAPSE]->() RETURN count(s);")
    count = list(count_result)[0][0]
    print(f"  Imported {count:,} synapses")
    return count


def verify_data(conn):
    """Verify data integrity."""
    print("\nVerifying data integrity...")
    
    # Neuron count
    result = conn.execute("MATCH (n:Neuron) RETURN count(n);")
    neuron_count = list(result)[0][0]
    
    # Synapse count
    result = conn.execute("MATCH ()-[s:SYNAPSE]->() RETURN count(s);")
    synapse_count = list(result)[0][0]
    
    # Total synapse weight
    result = conn.execute("MATCH ()-[s:SYNAPSE]->() RETURN sum(s.syn_count);")
    total_synapses = list(result)[0][0]
    
    # Neurotransmitter distribution
    result = conn.execute("""
        MATCH ()-[s:SYNAPSE]->()
        RETURN s.nt_type, count(s)
        ORDER BY count(s) DESC;
    """)
    nt_dist = list(result)
    
    # Flow distribution
    result = conn.execute("""
        MATCH (n:Neuron)
        RETURN n.flow, count(n)
        ORDER BY count(n) DESC;
    """)
    flow_dist = list(result)
    
    print(f"\n  Neurons: {neuron_count:,}")
    print(f"  Connections: {synapse_count:,}")
    print(f"  Total synapses: {total_synapses:,}")
    
    print("\n  Neurotransmitter distribution:")
    for nt, count in nt_dist:
        print(f"    {nt}: {count:,}")
    
    print("\n  Flow distribution:")
    for flow, count in flow_dist:
        print(f"    {flow}: {count:,}")
    
    return {
        "neurons": neuron_count,
        "connections": synapse_count,
        "total_synapses": total_synapses,
    }


def main():
    parser = argparse.ArgumentParser(description="Import FlyWire data into NeuG")
    parser.add_argument("--data-dir", "-d",
                        default="./flywire_dataset",
                        help="Data directory (default: ./flywire_dataset)")
    parser.add_argument("--db-path", "-p",
                        default="./flywire_db",
                        help="Database path (default: ./flywire_db)")
    parser.add_argument("--reset", "-r", action="store_true",
                        help="Reset database (delete existing)")
    
    args = parser.parse_args()
    
    data_dir = Path(args.data_dir)
    db_path = Path(args.db_path)
    
    print("=" * 70)
    print("FlyWire Data Import to NeuG")
    print("=" * 70)
    print(f"Data directory: {data_dir}")
    print(f"Database path: {db_path}")
    
    # Check data files exist
    if not (data_dir / "neurons.csv").exists():
        print(f"Error: neurons.csv not found in {data_dir}")
        return 1
    if not (data_dir / "synapses.csv").exists():
        print(f"Error: synapses.csv not found in {data_dir}")
        return 1
    
    # Reset database if requested
    if args.reset and db_path.exists():
        print(f"\nResetting database at {db_path}...")
        import shutil
        shutil.rmtree(db_path)
    
    # Create database
    print(f"\nCreating database at {db_path}...")
    db = neug.Database(str(db_path))
    conn = db.connect()
    
    # Create schema
    print()
    create_schema(conn)
    
    # Import data
    print()
    neuron_count = import_neurons(conn, data_dir)
    synapse_count = import_synapses(conn, data_dir)
    
    # Verify
    stats = verify_data(conn)
    
    # Close connection
    conn.close()
    
    print("\n" + "=" * 70)
    print("Import Complete!")
    print("=" * 70)
    print(f"Database: {db_path}")
    print(f"  Neurons: {stats['neurons']:,}")
    print(f"  Connections: {stats['connections']:,}")
    print(f"  Total synapses: {stats['total_synapses']:,}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())