# FlyWire Brain Simulation with NeuG

This example demonstrates how to use NeuG to store and simulate a complete fruit fly brain connectome.

## Overview

- **139,255 neurons** stored in NeuG
- **2,710,038 synaptic connections**
- **LIF neural simulation** with parameters from Shiu et al. Nature 2024

## Prerequisites

### 1. Build NeuG with Python Binding

```bash
# Clone NeuG repository
git clone https://github.com/longbinlai/neug.git
cd neug && git checkout flywire_brain

# Initialize submodules
git submodule update --init --recursive

# Build NeuG library
mkdir build && cd build
cmake .. -DBUILD_PYTHON_BIND=ON
make -j$(nproc)
```

### 2. Build and Install NeuG Python Wheel

```bash
# Set up environment (if you have a neug_env file)
source ~/.neug_env 2>/dev/null || true

# Navigate to Python binding directory
cd neug/tools/python_bind

# Install dependencies
pip install -r requirements.txt

# Build and install in development mode
pip install -e .
```

After installation, verify it works:

```bash
python3 -c "import neug; print(f'NeuG version: {neug.__version__}')"
```

### 3. Build lif_sim Extension

The lif_sim extension is built as part of the main NeuG build:

```bash
cd neug/build
make neug_lif_sim_extension -j$(nproc)
```

After building, copy the extension to the Python binding build directory:

```bash
# Create extension directory in python_bind build
mkdir -p neug/tools/python_bind/build/lib.<platform-specific>/extension/lif_sim

# Copy the extension library
cp neug/build/extension/lif_sim/liblif_sim.neug_extension \
   neug/tools/python_bind/build/lib.<platform-specific>/extension/lif_sim/

# On macOS, you may also need to copy dependent libraries:
mkdir -p neug/tools/python_bind/build/lib.<platform-specific>/neug/.dylibs
cp neug/build/third_party/mimalloc/libmimalloc.*.dylib \
   neug/tools/python_bind/build/lib.<platform-specific>/neug/.dylibs/
```

To find the correct platform-specific build directory:

```bash
# Find the build directory
find neug/tools/python_bind/build -type d -name "lib.*" | head -1
```

### 4. Install Additional Dependencies

For generating figures, install matplotlib:

```bash
pip install matplotlib numpy
```

### 5. Download Dataset

```bash
# Download from OSS
wget https://neug.oss-cn-beijing.aliyuncs.com/datasets/flywire_dataset.tar.gz

# Extract
tar -xzvf flywire_dataset.tar.gz
# This will extract:
# - neurons.csv (139,255 neurons)
# - synapses.csv (2,710,038 connections)
# - schema.cypher (database schema)
```

## Quick Start

### Step 1: Import Data into NeuG

```bash
python import_to_neug.py
```

This will:
- Create a NeuG database at `./flywire_db`
- Import neurons and synapses from CSV files
- Create indexes for efficient queries

### Step 2: Run LIF Simulation

Before running the simulation, ensure:
1. The NeuG Python package is installed (see Prerequisites section)
2. The lif_sim extension is built

To run the simulation:

```bash
python flywire_demo_simple.py
```

This will:
- Load the lif_sim extension (automatically found from the build directory)
- Stimulate sugar-sensing neurons
- Run LIF simulation for 20 rounds
- Report the number of neurons that spiked

**Note:** The extension is loaded using `LOAD lif_sim;` which searches for the extension library in the build directory. If you encounter issues loading the extension, verify the extension library exists:

```bash
# Check if the extension library exists
find neug/tools/python_bind/build -name "liblif_sim.neug_extension"
```

### Step 3: Analyze Results

```bash
python scientific_behavior_analysis.py
```

This will:
- Compare sugar vs bitter stimulation
- Analyze motor neuron activation
- Generate comparison tables

### Step 4: Generate Figures

```bash
python generate_figures_simple.py
```

This will generate visualization figures in `./figures/` directory.

## Files

| File | Description |
|------|-------------|
| `import_to_neug.py` | Import FlyWire data into NeuG |
| `flywire_demo_simple.py` | Run LIF simulation demo |
| `scientific_behavior_analysis.py` | Analyze simulation results |
| `generate_figures_simple.py` | Generate visualization figures |

## LIF Parameters

Parameters from Shiu et al. Nature 2024, Supplementary Table S1:

| Parameter | Value | Description |
|-----------|-------|-------------|
| v_rest | -52 mV | Resting membrane potential |
| v_threshold | -45 mV | Spike threshold |
| v_reset | -52 mV | Reset potential after spike |
| w_syn | 0.275 mV | Synaptic weight |
| tau_m | 20 ms | Membrane time constant |

## Neurotransmitter Signs

Important: In Drosophila, **glutamate (GLUT) is inhibitory**, unlike in mammals!

| Neurotransmitter | Effect | Sign |
|-----------------|--------|------|
| ACH | Excitatory | +1 |
| GABA | Inhibitory | -1 |
| GLUT | Inhibitory | -1 |

## Results

### Signal Propagation

- Stimulating 20 sugar GRNs → ~10,000 neurons spike
- Propagation factor: ~500x

### Motor Neuron Activation

| Motor Neuron Type | Sugar Spikes | Bitter Spikes | Difference |
|------------------|--------------|---------------|------------|
| ingestion_motor_neuron | 19 | 6 | +13 |
| proboscis_motor_neuron | 16 | 17 | -1 |
| neck_motor_neuron | 11 | 15 | -4 |

## Limitations

1. **No MN9 annotation**: We don't have precise annotation for MN9 (the specific motor neuron controlling proboscis extension)
2. **Spike count vs firing rate**: We measure spike count, not firing rate (Hz)
3. **No experimental validation**: Results are computational only

## References

1. Shiu, P.K., et al. "A connectome of the Drosophila melanogaster brain reveals neural circuit mechanisms of behavior." Nature (2024). DOI: [10.1038/s41586-024-07763-9](https://doi.org/10.1038/s41586-024-07763-9)

2. FlyWire Project: https://flywire.ai/

3. NeuG Documentation: https://graphscope.io/neug/

## License

The FlyWire data is licensed under CC-BY 4.0. This example code is licensed under the same license as NeuG.
