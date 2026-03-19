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

#include "neug/compiler/function/neug_call_function.h"

namespace neug {
namespace function {

// ---------------------------------------------------------------------------
// Custom CallFuncInputBase subclasses for argument extraction
// ---------------------------------------------------------------------------

/**
 * @brief Input for LIF_SET_STIMULUS(neuron_id INT64, amplitude DOUBLE)
 */
struct LIFSetStimulusInput : public CallFuncInputBase {
    int64_t neuron_id = 0;
    double amplitude = 60.0;
};

/**
 * @brief Input for LIF_SIMULATE(max_rounds INT64)
 */
struct LIFSimulateInput : public CallFuncInputBase {
    int64_t max_rounds = 10;
};

// ---------------------------------------------------------------------------
// Function declarations
// ---------------------------------------------------------------------------

/**
 * @brief Get current neuron states from simulation
 * Returns: neuron_id, membrane_potential, spiked, spike_count, spike_round
 */
class LIFGetStateFunction {
public:
    static constexpr const char* name = "LIF_GET_STATE";
    static function_set getFunctionSet();
};

/**
 * @brief Initialize simulation state from graph
 * Reads all neurons from the Neuron table and initializes their state
 */
class LIFInitFunction {
public:
    static constexpr const char* name = "LIF_INIT";
    static function_set getFunctionSet();
};

/**
 * @brief Set input stimulus for a neuron
 * CALL LIF_SET_STIMULUS(neuron_id, amplitude)
 */
class LIFSetStimulusFunction {
public:
    static constexpr const char* name = "LIF_SET_STIMULUS";
    static function_set getFunctionSet();
};

/**
 * @brief Run LIF wave propagation on the graph
 * BFS-like signal cascade: stimulus neurons spike, signals propagate along
 * SYNAPSE edges using LIF threshold rules until no new spikes or max_rounds.
 * CALL LIF_SIMULATE(max_rounds)
 */
class LIFSimulateFunction {
public:
    static constexpr const char* name = "LIF_SIMULATE";
    static function_set getFunctionSet();
};

}  // namespace function
}  // namespace neug
