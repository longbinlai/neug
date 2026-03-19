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

#include "lif_sim_function.h"
#include "lif_sim_engine.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/csr/generic_view.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/utils/exception/exception.h"

#include <unordered_set>
#include <unordered_map>
#include <algorithm>
#include <vector>

namespace neug {
namespace function {

// Global simulation state instance
static lif_sim::LIFSimState g_sim_state;

// Mapping from internal vid_t to neuron root_id
static std::unordered_map<vid_t, int64_t> g_vid_to_neuron_id;
static std::unordered_map<int64_t, vid_t> g_neuron_id_to_vid;

// Helper: Get synapse sign from nt_type string
// Drosophila convention (Shiu et al. Nature 2024): both GABA and GLUT are
// inhibitory.  Glutamate acts on GluCl chloride-channel receptors.
static double getSynapseSign(const std::string& nt_type) {
    if (nt_type == "ACH" || nt_type == "ach") {
        return 1.0;  // Excitatory
    } else if (nt_type == "GABA" || nt_type == "gaba" ||
               nt_type == "GLUT" || nt_type == "glut") {
        return -1.0; // Inhibitory (GLUT via GluCl in Drosophila)
    }
    return 0.0;  // Modulatory
}

// ============================================================================
// LIF_GET_STATE
// Returns: neuron_id, membrane_potential, spiked, spike_count, spike_round
// ============================================================================

function_set LIFGetStateFunction::getFunctionSet() {
    auto function = std::make_unique<NeugCallFunction>(
        name,
        std::vector<common::LogicalTypeID>{},
        std::vector<std::pair<std::string, common::LogicalTypeID>>{
            {"neuron_id", common::LogicalTypeID::INT64},
            {"membrane_potential", common::LogicalTypeID::DOUBLE},
            {"spiked", common::LogicalTypeID::BOOL},
            {"spike_count", common::LogicalTypeID::INT64},
            {"spike_round", common::LogicalTypeID::INT64}
        });

    function->bindFunc = [](const Schema& schema,
                            const execution::ContextMeta& ctx_meta,
                            const ::physical::PhysicalPlan& plan,
                            int op_idx) -> std::unique_ptr<CallFuncInputBase> {
        return std::make_unique<CallFuncInputBase>();
    };

    function->execFunc = [](const CallFuncInputBase& input, IStorageInterface& graph) -> execution::Context {
        execution::Context ctx;
        const auto& states = g_sim_state.getAllStates();

        execution::ValueColumnBuilder<int64_t> id_builder;
        execution::ValueColumnBuilder<double> potential_builder;
        execution::ValueColumnBuilder<bool> spiked_builder;
        execution::ValueColumnBuilder<int64_t> count_builder;
        execution::ValueColumnBuilder<int64_t> round_builder;

        id_builder.reserve(states.size());
        potential_builder.reserve(states.size());
        spiked_builder.reserve(states.size());
        count_builder.reserve(states.size());
        round_builder.reserve(states.size());

        for (const auto& [id, state] : states) {
            id_builder.push_back_opt(id);
            potential_builder.push_back_opt(state.membrane_potential);
            spiked_builder.push_back_opt(state.spiked);
            count_builder.push_back_opt(state.spike_count);
            round_builder.push_back_opt(static_cast<int64_t>(state.spike_round));
        }

        ctx.set(0, id_builder.finish());
        ctx.set(1, potential_builder.finish());
        ctx.set(2, spiked_builder.finish());
        ctx.set(3, count_builder.finish());
        ctx.set(4, round_builder.finish());
        ctx.tag_ids = {0, 1, 2, 3, 4};
        return ctx;
    };

    function_set functionSet;
    functionSet.push_back(std::move(function));
    return functionSet;
}

// ============================================================================
// LIF_INIT
// Reads all Neuron vertices from the graph and initializes their state.
// ============================================================================

function_set LIFInitFunction::getFunctionSet() {
    auto function = std::make_unique<NeugCallFunction>(
        name,
        std::vector<common::LogicalTypeID>{},
        std::vector<std::pair<std::string, common::LogicalTypeID>>{
            {"neurons_initialized", common::LogicalTypeID::INT64}
        });

    function->bindFunc = [](const Schema& schema,
                            const execution::ContextMeta& ctx_meta,
                            const ::physical::PhysicalPlan& plan,
                            int op_idx) -> std::unique_ptr<CallFuncInputBase> {
        return std::make_unique<CallFuncInputBase>();
    };

    function->execFunc = [](const CallFuncInputBase& input, IStorageInterface& graph) -> execution::Context {
        execution::Context ctx;

        g_sim_state.clear();
        g_vid_to_neuron_id.clear();
        g_neuron_id_to_vid.clear();

        if (!graph.readable()) {
            execution::ValueColumnBuilder<int64_t> count_builder;
            count_builder.push_back_opt(0);
            ctx.set(0, count_builder.finish());
            ctx.tag_ids = {0};
            return ctx;
        }

        const auto& schema_ref = graph.schema();

        if (!schema_ref.contains_vertex_label("Neuron")) {
            execution::ValueColumnBuilder<int64_t> count_builder;
            count_builder.push_back_opt(0);
            ctx.set(0, count_builder.finish());
            ctx.tag_ids = {0};
            return ctx;
        }

        label_t neuron_label = schema_ref.get_vertex_label_id("Neuron");

        auto* reader = dynamic_cast<StorageReadInterface*>(&graph);
        if (!reader) {
            execution::ValueColumnBuilder<int64_t> count_builder;
            count_builder.push_back_opt(0);
            ctx.set(0, count_builder.finish());
            ctx.tag_ids = {0};
            return ctx;
        }

        auto vertices = reader->GetVertexSet(neuron_label);
        int64_t count = 0;

        for (vid_t vid : vertices) {
            Property id_prop = reader->GetVertexId(neuron_label, vid);
            int64_t neuron_id = id_prop.as_int64();

            g_vid_to_neuron_id[vid] = neuron_id;
            g_neuron_id_to_vid[neuron_id] = vid;

            g_sim_state.initNeuron(neuron_id);
            count++;
        }

        execution::ValueColumnBuilder<int64_t> count_builder;
        count_builder.push_back_opt(count);
        ctx.set(0, count_builder.finish());
        ctx.tag_ids = {0};
        return ctx;
    };

    function_set functionSet;
    functionSet.push_back(std::move(function));
    return functionSet;
}

// ============================================================================
// LIF_SET_STIMULUS(neuron_id INT64, amplitude DOUBLE)
// Marks a neuron to receive a voltage boost at the start of propagation.
// ============================================================================

function_set LIFSetStimulusFunction::getFunctionSet() {
    auto function = std::make_unique<NeugCallFunction>(
        name,
        std::vector<common::LogicalTypeID>{
            common::LogicalTypeID::INT64,
            common::LogicalTypeID::DOUBLE
        },
        std::vector<std::pair<std::string, common::LogicalTypeID>>{
            {"success", common::LogicalTypeID::BOOL}
        });

    function->bindFunc = [](const Schema& schema,
                            const execution::ContextMeta& ctx_meta,
                            const ::physical::PhysicalPlan& plan,
                            int op_idx) -> std::unique_ptr<CallFuncInputBase> {
        auto input = std::make_unique<LIFSetStimulusInput>();
        // Extract arguments from the Cypher CALL statement via PhysicalPlan
        auto procedurePB = plan.plan(op_idx).opr().procedure_call();
        const auto& query = procedurePB.query();
        if (query.arguments_size() >= 2) {
            input->neuron_id = query.arguments(0).const_().i64();
            input->amplitude = query.arguments(1).const_().f64();
        } else if (query.arguments_size() >= 1) {
            input->neuron_id = query.arguments(0).const_().i64();
        }
        return input;
    };

    function->execFunc = [](const CallFuncInputBase& input, IStorageInterface& graph) -> execution::Context {
        execution::Context ctx;
        const auto& args = static_cast<const LIFSetStimulusInput&>(input);

        bool ok = g_sim_state.hasNeuron(args.neuron_id);
        if (ok) {
            g_sim_state.setStimulus(args.neuron_id, args.amplitude);
        }

        execution::ValueColumnBuilder<bool> success_builder;
        success_builder.push_back_opt(ok);
        ctx.set(0, success_builder.finish());
        ctx.tag_ids = {0};
        return ctx;
    };

    function_set functionSet;
    functionSet.push_back(std::move(function));
    return functionSet;
}

// ============================================================================
// LIF_SIMULATE(max_rounds INT64)
// Wave propagation: BFS-like signal cascade through the graph.
//
// Algorithm:
//   1. Apply stimulus voltage to marked neurons; those exceeding V_threshold
//      spike and form the initial wave (round 0).
//   2. For each round (up to max_rounds):
//      a. Spiking neurons push delta-V to downstream neighbors via SYNAPSE
//         edges:  delta_V = w_syn * syn_count * sign(nt_type)
//      b. Neighbors whose membrane potential crosses V_threshold spike and
//         form the next wave.
//      c. Stop when no new neurons spike.
//
// Only neurons in the propagation path are processed -- the algorithm does
// NOT iterate over all 139K neurons at each round.
// ============================================================================

function_set LIFSimulateFunction::getFunctionSet() {
    auto function = std::make_unique<NeugCallFunction>(
        name,
        std::vector<common::LogicalTypeID>{
            common::LogicalTypeID::INT64
        },
        std::vector<std::pair<std::string, common::LogicalTypeID>>{
            {"neuron_id", common::LogicalTypeID::INT64},
            {"spiked", common::LogicalTypeID::BOOL},
            {"spike_count", common::LogicalTypeID::INT64},
            {"spike_round", common::LogicalTypeID::INT64}
        });

    function->bindFunc = [](const Schema& schema,
                            const execution::ContextMeta& ctx_meta,
                            const ::physical::PhysicalPlan& plan,
                            int op_idx) -> std::unique_ptr<CallFuncInputBase> {
        auto input = std::make_unique<LIFSimulateInput>();
        auto procedurePB = plan.plan(op_idx).opr().procedure_call();
        const auto& query = procedurePB.query();
        if (query.arguments_size() >= 1) {
            input->max_rounds = query.arguments(0).const_().i64();
        }
        return input;
    };

    function->execFunc = [](const CallFuncInputBase& input, IStorageInterface& graph) -> execution::Context {
        execution::Context ctx;
        const auto& args = static_cast<const LIFSimulateInput&>(input);
        int64_t max_rounds = args.max_rounds;

        const auto& params = lif_sim::LIFSimState::getParams();

        const auto& schema_ref = graph.schema();
        auto* reader = dynamic_cast<StorageReadInterface*>(&graph);

        if (!reader) {
            execution::ValueColumnBuilder<int64_t> id_builder;
            execution::ValueColumnBuilder<bool> spiked_builder;
            execution::ValueColumnBuilder<int64_t> count_builder;
            execution::ValueColumnBuilder<int64_t> round_builder;
            ctx.set(0, id_builder.finish());
            ctx.set(1, spiked_builder.finish());
            ctx.set(2, count_builder.finish());
            ctx.set(3, round_builder.finish());
            ctx.tag_ids = {0, 1, 2, 3};
            return ctx;
        }

        label_t neuron_label = schema_ref.get_vertex_label_id("Neuron");
        label_t synapse_label = schema_ref.get_edge_label_id("SYNAPSE");

        EdgeDataAccessor syn_count_accessor = reader->GetEdgeDataAccessor(
            neuron_label, neuron_label, synapse_label, "syn_count");
        EdgeDataAccessor nt_type_accessor = reader->GetEdgeDataAccessor(
            neuron_label, neuron_label, synapse_label, "nt_type");

        GenericView outgoing_view = reader->GetGenericOutgoingGraphView(
            neuron_label, neuron_label, synapse_label);

        auto& states = const_cast<std::unordered_map<int64_t, lif_sim::LIFNeuronState>&>(
            g_sim_state.getAllStates());
        const auto& stimuli = g_sim_state.getAllStimuli();

        // ---- Round 0: Apply stimulus, find initial spikers ----
        std::unordered_set<int64_t> current_wave;

        for (const auto& [nid, amplitude] : stimuli) {
            auto it = states.find(nid);
            if (it == states.end()) continue;
            it->second.membrane_potential += amplitude;
            if (it->second.membrane_potential >= params.v_threshold) {
                it->second.spiked = true;
                it->second.spike_count = 1;
                it->second.spike_round = 0;
                it->second.membrane_potential = params.v_reset;
                current_wave.insert(nid);
            }
        }

        // ---- Rounds 1..max_rounds: Wave propagation ----
        for (int64_t round = 1; round <= max_rounds && !current_wave.empty(); round++) {
            std::unordered_map<int64_t, double> delta_v;

            // Propagate from current wave to neighbors via graph edges
            for (int64_t nid : current_wave) {
                auto vid_it = g_neuron_id_to_vid.find(nid);
                if (vid_it == g_neuron_id_to_vid.end()) continue;
                vid_t vid = vid_it->second;

                NbrList neighbors = outgoing_view.get_edges(vid);
                for (auto it = neighbors.begin(); it != neighbors.end(); ++it) {
                    vid_t dst_vid = *it;
                    auto dst_it = g_vid_to_neuron_id.find(dst_vid);
                    if (dst_it == g_vid_to_neuron_id.end()) continue;

                    int64_t dst_neuron_id = dst_it->second;
                    int64_t syn_count = syn_count_accessor.get_typed_data<int64_t>(it);
                    Property nt_prop = nt_type_accessor.get_data(it);
                    std::string nt_type(nt_prop.as_string_view());

                    double sign = getSynapseSign(nt_type);
                    if (sign == 0.0) continue;

                    delta_v[dst_neuron_id] += params.w_syn *
                        static_cast<double>(syn_count) * sign;
                }
            }

            // Check which neighbors spike
            current_wave.clear();
            for (auto& [dst_id, dv] : delta_v) {
                auto it = states.find(dst_id);
                if (it == states.end()) continue;
                if (it->second.spike_count > 0) continue;  // already spiked

                it->second.membrane_potential += dv;
                if (it->second.membrane_potential >= params.v_threshold) {
                    it->second.spiked = true;
                    it->second.spike_count = 1;
                    it->second.spike_round = static_cast<int32_t>(round);
                    it->second.membrane_potential = params.v_reset;
                    current_wave.insert(dst_id);
                }
            }
        }

        // ---- Build output: only neurons that spiked ----
        int64_t active_count = 0;
        for (const auto& [id, state] : states) {
            if (state.spike_count > 0) active_count++;
        }

        execution::ValueColumnBuilder<int64_t> id_builder;
        execution::ValueColumnBuilder<bool> spiked_builder;
        execution::ValueColumnBuilder<int64_t> count_builder;
        execution::ValueColumnBuilder<int64_t> round_builder;

        id_builder.reserve(active_count);
        spiked_builder.reserve(active_count);
        count_builder.reserve(active_count);
        round_builder.reserve(active_count);

        for (const auto& [id, state] : states) {
            if (state.spike_count > 0) {
                id_builder.push_back_opt(id);
                spiked_builder.push_back_opt(state.spiked);
                count_builder.push_back_opt(state.spike_count);
                round_builder.push_back_opt(static_cast<int64_t>(state.spike_round));
            }
        }

        ctx.set(0, id_builder.finish());
        ctx.set(1, spiked_builder.finish());
        ctx.set(2, count_builder.finish());
        ctx.set(3, round_builder.finish());
        ctx.tag_ids = {0, 1, 2, 3};
        return ctx;
    };

    function_set functionSet;
    functionSet.push_back(std::move(function));
    return functionSet;
}

}  // namespace function
}  // namespace neug
