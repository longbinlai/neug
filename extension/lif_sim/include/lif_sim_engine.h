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
#include <cmath>

namespace lif_sim {

/**
 * @brief LIF neuron parameters (constant for all neurons)
 * Based on Shiu et al. Nature 2024 supplementary materials.
 */
struct LIFParams {
    double tau_m = 20.0;            // Membrane time constant (ms)
    double v_threshold = -45.0;     // Spike threshold (mV) - Shiu et al.
    double v_reset = -52.0;         // Reset potential (mV) - Shiu et al.
    double v_rest = -52.0;          // Resting potential (mV) - Shiu et al.
    double w_syn = 0.275;           // Delta-synapse weight per synapse (mV) - Shiu et al.
    double refractory_period = 2.0; // Refractory period (ms)
    double dt = 0.5;                // Simulation timestep (ms)
};

/**
 * @brief LIF neuron state (dynamic during simulation)
 */
struct LIFNeuronState {
    double membrane_potential = -52.0;  // Current membrane potential (mV) - starts at v_rest
    bool spiked = false;                 // Did spike in last timestep
    int64_t spike_count = 0;             // Total spike count
    int32_t spike_round = -1;            // Propagation round when first spiked (-1 = never)
};

/**
 * @brief Input stimulus (voltage boost for wave propagation)
 */
struct Stimulus {
    double amplitude = 60.0;    // Voltage boost (mV)
};

/**
 * @brief LIF Simulation State
 * 
 * Maintains only the dynamic state of neurons during simulation.
 * The graph structure (neurons and synapses) is accessed directly from neug.
 */
class LIFSimState {
public:
    LIFSimState() = default;
    ~LIFSimState() = default;

    /**
     * @brief Initialize state for a neuron
     */
    void initNeuron(int64_t neuron_id) {
        states_[neuron_id] = LIFNeuronState();
    }

    /**
     * @brief Check if neuron has state
     */
    bool hasNeuron(int64_t neuron_id) const {
        return states_.find(neuron_id) != states_.end();
    }

    /**
     * @brief Get neuron state
     */
    LIFNeuronState& getState(int64_t neuron_id) {
        return states_[neuron_id];
    }

    /**
     * @brief Get neuron state (const)
     */
    const LIFNeuronState& getState(int64_t neuron_id) const {
        return states_.at(neuron_id);
    }

    /**
     * @brief Set input stimulus for a neuron (voltage boost for propagation)
     */
    void setStimulus(int64_t neuron_id, double amplitude) {
        stimuli_[neuron_id] = amplitude;
    }

    /**
     * @brief Get stimulus amplitude for a neuron (0.0 if not stimulated)
     */
    double getStimulusAmplitude(int64_t neuron_id) const {
        auto it = stimuli_.find(neuron_id);
        return it != stimuli_.end() ? it->second : 0.0;
    }

    /**
     * @brief Get all stimuli (neuron_id -> amplitude)
     */
    const std::unordered_map<int64_t, double>& getAllStimuli() const {
        return stimuli_;
    }

    /**
     * @brief Get all neuron states
     */
    const std::unordered_map<int64_t, LIFNeuronState>& getAllStates() const {
        return states_;
    }

    /**
     * @brief Get neuron count
     */
    int64_t getNeuronCount() const {
        return states_.size();
    }

    /**
     * @brief Get total spike count
     */
    int64_t getTotalSpikeCount() const {
        int64_t count = 0;
        for (const auto& [id, state] : states_) {
            count += state.spike_count;
        }
        return count;
    }

    /**
     * @brief Clear all state
     */
    void clear() {
        states_.clear();
        stimuli_.clear();
    }

    /**
     * @brief Get global LIF parameters
     */
    static const LIFParams& getParams() {
        static LIFParams params;
        return params;
    }

    /**
     * @brief Get synapse sign based on neurotransmitter type
     * Drosophila convention: both GABA and GLUT are inhibitory.
     */
    static double getSynapseSign(const std::string& nt_type) {
        if (nt_type == "ACH") {
            return 1.0;  // Excitatory
        } else if (nt_type == "GABA" || nt_type == "GLUT") {
            return -1.0; // Inhibitory (GLUT via GluCl in Drosophila)
        }
        return 0.0;  // Modulatory (DA, SER, OCT)
    }

private:
    std::unordered_map<int64_t, LIFNeuronState> states_;
    std::unordered_map<int64_t, double> stimuli_;  // neuron_id -> amplitude (mV)
};

}  // namespace lif_sim
