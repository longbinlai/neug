/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace neug {
namespace lif_sim {

/**
 * @brief LIF neuron parameters
 */
struct LIFNeuronParams {
    double tau_m = 20.0;        // Membrane time constant (ms)
    double v_threshold = -50.0; // Spike threshold (mV)
    double v_reset = -65.0;     // Reset potential (mV)
    double v_rest = -65.0;      // Resting potential (mV)
    double r_m = 1.0;           // Membrane resistance (MOhm)
    double refractory = 2.0;    // Refractory period (ms)
};

/**
 * @brief LIF neuron state
 */
struct LIFNeuronState {
    int64_t neuron_id;          // Neuron identifier
    double v;                   // Current membrane potential
    double refractory_time;     // Time remaining in refractory period
    bool spiked;                // Whether neuron spiked in last step
    double i_syn;               // Synaptic current
};

/**
 * @brief Synapse parameters
 */
struct SynapseParams {
    int64_t pre_id;             // Presynaptic neuron ID
    int64_t post_id;            // Postsynaptic neuron ID
    double weight;              // Synaptic weight (positive=excitatory, negative=inhibitory)
    double delay;               // Synaptic delay (ms)
    std::string nt_type;        // Neurotransmitter type (ACH, GABA, GLUT, etc.)
};

/**
 * @brief Simulation configuration
 */
struct LIFSimConfig {
    double dt = 1.0;            // Time step (ms)
    int64_t num_steps = 100;    // Number of simulation steps
    double current_scale = 1.0; // Scale factor for input currents
    bool record_spikes = true;  // Whether to record spike times
    bool record_voltages = false; // Whether to record voltage traces
};

/**
 * @brief Input stimulus for a neuron
 */
struct NeuronStimulus {
    int64_t neuron_id;
    double start_time;          // Start time (ms)
    double duration;            // Duration (ms)
    double amplitude;           // Current amplitude (nA)
};

/**
 * @brief Simulation result
 */
struct LIFSimResult {
    std::vector<int64_t> spiked_neurons;      // Neurons that spiked
    std::vector<std::pair<int64_t, double>> spike_times; // (neuron_id, time)
    std::unordered_map<int64_t, std::vector<double>> voltage_traces; // Optional voltage records
    int64_t total_spikes;                      // Total spike count
};

/**
 * @brief Neurotransmitter types and their effects
 * Sign convention follows Shiu et al. (Nature 634, 210-219, 2024):
 *   ACH  = excitatory (+1)  -- acetylcholine
 *   GABA = inhibitory (-1)  -- gamma-aminobutyric acid
 *   GLUT = inhibitory (-1)  -- glutamate acts on GluCl chloride-channel
 *                              receptors in Drosophila (unlike mammalian
 *                              systems where it is excitatory)
 */
enum class NeurotransmitterType {
    ACH,    // Acetylcholine - excitatory
    GABA,   // GABA - inhibitory
    GLUT,   // Glutamate - inhibitory in Drosophila (GluCl receptors)
    DA,     // Dopamine - modulatory
    SER,    // Serotonin - modulatory
    OCT,    // Octopamine - modulatory
    UNKNOWN
};

/**
 * @brief Convert neurotransmitter string to enum
 */
inline NeurotransmitterType parseNTType(const std::string& nt_str) {
    if (nt_str == "ACH") return NeurotransmitterType::ACH;
    if (nt_str == "GABA") return NeurotransmitterType::GABA;
    if (nt_str == "GLUT") return NeurotransmitterType::GLUT;
    if (nt_str == "DA") return NeurotransmitterType::DA;
    if (nt_str == "SER") return NeurotransmitterType::SER;
    if (nt_str == "OCT") return NeurotransmitterType::OCT;
    return NeurotransmitterType::UNKNOWN;
}

/**
 * @brief Get default weight sign for neurotransmitter type
 * Drosophila convention: both GABA and GLUT are inhibitory.
 * See Shiu et al. (Nature 634, 210-219, 2024): "We assume GABAergic
 * and glutamatergic neurons are inhibitory."
 */
inline double getNTSign(NeurotransmitterType nt) {
    switch (nt) {
        case NeurotransmitterType::ACH:
            return 1.0;  // Excitatory
        case NeurotransmitterType::GABA:
        case NeurotransmitterType::GLUT:
            return -1.0; // Inhibitory (GLUT via GluCl in Drosophila)
        default:
            return 0.5;  // Modulatory (weak excitatory)
    }
}

}  // namespace lif_sim
}  // namespace neug
