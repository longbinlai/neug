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

#include "label_propagation_function.h"
#include <glog/logging.h>
#include <algorithm>
#include <atomic>
#include <map>
#include <thread>
#include <unordered_map>
#include <vector>
#include "gds_param_parser.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace function {

/**
 * @brief Execute Label Propagation algorithm with parallel label updates.
 */
static execution::Context executeLabelPropagation(
    int maxIterations,
    int concurrency,
    const ProjectedSubgraph* subgraph,
    neug::IStorageInterface& storage) {

  execution::Context ctx;

  if (!storage.readable()) {
    THROW_EXTENSION_EXCEPTION("Storage is not readable");
  }

  const Schema& schema = storage.schema();
  StorageReadInterface* reader = dynamic_cast<StorageReadInterface*>(&storage);
  if (!reader) {
    THROW_EXTENSION_EXCEPTION("Failed to cast to StorageReadInterface");
  }

  // Collect all vertex labels
  std::vector<label_t> vertex_labels;
  if (subgraph && !subgraph->vertexEntries.empty()) {
    for (const auto& entry : subgraph->vertexEntries) {
      if (schema.contains_vertex_label(entry.label)) {
        vertex_labels.push_back(schema.get_vertex_label_id(entry.label));
      }
    }
  } else {
    const auto& v_schemas = schema.get_all_vertex_schemas();
    for (size_t i = 0; i < v_schemas.size(); ++i) {
      if (v_schemas[i] && !v_schemas[i]->empty()) {
        vertex_labels.push_back(static_cast<label_t>(i));
      }
    }
  }

  if (vertex_labels.empty()) {
    LOG(WARNING) << "CDLP: No vertices found in the graph";
    return ctx;
  }

  LOG(INFO) << "CDLP: Processing " << vertex_labels.size() << " vertex labels";

  // Collect all vertices
  std::vector<std::pair<label_t, vid_t>> all_vertices;
  std::unordered_map<label_t, std::vector<vid_t>> vertices_by_label;
  std::vector<int64_t> external_ids;

  for (label_t label : vertex_labels) {
    VertexSet vertices = reader->GetVertexSet(label);
    for (vid_t v : vertices) {
      all_vertices.push_back({label, v});
      vertices_by_label[label].push_back(v);

      Property external_id = reader->GetVertexId(label, v);
      external_ids.push_back(external_id.as_int64());
    }
  }

  size_t num_vertices = all_vertices.size();
  if (num_vertices == 0) {
    LOG(WARNING) << "CDLP: No vertices found after filtering";
    return ctx;
  }

  LOG(INFO) << "CDLP: Found " << num_vertices << " vertices";

  // Build vertex ID to index mapping
  auto vertex_hash = [](const std::pair<label_t, vid_t>& p) {
    return std::hash<label_t>()(p.first) ^ (std::hash<vid_t>()(p.second) << 1);
  };
  std::unordered_map<std::pair<label_t, vid_t>, size_t, decltype(vertex_hash)> vertex_to_idx(0, vertex_hash);
  for (size_t i = 0; i < num_vertices; ++i) {
    vertex_to_idx[all_vertices[i]] = i;
  }

  // Collect edge triplets
  std::vector<std::tuple<label_t, label_t, label_t>> edge_triplets;
  if (subgraph && !subgraph->edgeEntries.empty()) {
    for (const auto& entry : subgraph->edgeEntries) {
      if (schema.exist(entry.srcLabel, entry.dstLabel, entry.edgeLabel)) {
        edge_triplets.push_back({
          schema.get_vertex_label_id(entry.srcLabel),
          schema.get_edge_label_id(entry.edgeLabel),
          schema.get_vertex_label_id(entry.dstLabel)
        });
      }
    }
  } else {
    const auto& e_schemas = schema.get_all_edge_schemas();
    for (const auto& [key, edge_schema] : e_schemas) {
      if (!edge_schema || edge_schema->empty()) continue;
      label_t src_label = schema.get_vertex_label_id(edge_schema->src_label_name);
      label_t dst_label = schema.get_vertex_label_id(edge_schema->dst_label_name);
      label_t edge_label = schema.get_edge_label_id(edge_schema->edge_label_name);
      edge_triplets.push_back({src_label, edge_label, dst_label});
    }
  }

  // Build adjacency list (undirected graph semantics)
  std::vector<std::vector<size_t>> adj_list(num_vertices);

  for (const auto& [src_label, edge_label, dst_label] : edge_triplets) {
    // Outgoing edges
    auto out_view = reader->GetGenericOutgoingGraphView(src_label, dst_label, edge_label);
    for (vid_t src : vertices_by_label[src_label]) {
      auto src_idx_it = vertex_to_idx.find({src_label, src});
      if (src_idx_it == vertex_to_idx.end()) continue;

      size_t src_idx = src_idx_it->second;
      auto edges = out_view.get_edges(src);
      for (auto it = edges.begin(); it != edges.end(); ++it) {
        vid_t dst = *it;
        auto dst_idx_it = vertex_to_idx.find({dst_label, dst});
        if (dst_idx_it != vertex_to_idx.end()) {
          adj_list[src_idx].push_back(dst_idx_it->second);
        }
      }
    }

    // Incoming edges (for undirected graph)
    auto in_view = reader->GetGenericIncomingGraphView(dst_label, src_label, edge_label);
    for (vid_t dst : vertices_by_label[dst_label]) {
      auto dst_idx_it = vertex_to_idx.find({dst_label, dst});
      if (dst_idx_it == vertex_to_idx.end()) continue;

      size_t dst_idx = dst_idx_it->second;
      auto edges = in_view.get_edges(dst);
      for (auto it = edges.begin(); it != edges.end(); ++it) {
        vid_t src = *it;
        auto src_idx_it = vertex_to_idx.find({src_label, src});
        if (src_idx_it != vertex_to_idx.end()) {
          adj_list[dst_idx].push_back(src_idx_it->second);
        }
      }
    }
  }

  // Deduplicate neighbors
  for (auto& neighbors : adj_list) {
    std::sort(neighbors.begin(), neighbors.end());
    neighbors.erase(std::unique(neighbors.begin(), neighbors.end()), neighbors.end());
  }

  LOG(INFO) << "CDLP: Built adjacency structures (undirected)";

  // Initialize labels (each vertex has its own index as label)
  std::vector<int64_t> label(num_vertices);
  for (size_t i = 0; i < num_vertices; ++i) {
    label[i] = static_cast<int64_t>(i);
  }

  // Determine number of threads
  int num_threads = concurrency > 0 ? concurrency : std::thread::hardware_concurrency();
  if (num_threads < 1) num_threads = 1;
  
  LOG(INFO) << "CDLP: Using " << num_threads << " threads";

  // Iterative label propagation
  for (int iter = 0; iter < maxIterations; ++iter) {
    std::vector<int64_t> new_label(num_vertices);
    std::atomic<int> changed_count{0};

    // Parallel label update
    auto update_labels = [&](size_t start, size_t end) {
      int local_changed = 0;
      for (size_t v = start; v < end; ++v) {
        const auto& neighbors = adj_list[v];
        
        if (neighbors.empty()) {
          new_label[v] = label[v];
          continue;
        }

        // Count label frequencies
        std::map<int64_t, int> label_counts;
        for (size_t u : neighbors) {
          label_counts[label[u]]++;
        }

        // Find max frequency
        int max_count = 0;
        int64_t best_label = label[v];
        for (const auto& [l, count] : label_counts) {
          if (count > max_count || (count == max_count && l < best_label)) {
            max_count = count;
            best_label = l;
          }
        }

        new_label[v] = best_label;
        if (new_label[v] != label[v]) {
          local_changed++;
        }
      }
      changed_count.fetch_add(local_changed, std::memory_order_relaxed);
    };

    if (num_threads > 1 && num_vertices > 1000) {
      std::vector<std::thread> threads;
      size_t chunk_size = (num_vertices + num_threads - 1) / num_threads;
      
      for (int t = 0; t < num_threads; ++t) {
        size_t start = t * chunk_size;
        size_t end = std::min(start + chunk_size, num_vertices);
        if (start < end) {
          threads.emplace_back(update_labels, start, end);
        }
      }
      
      for (auto& t : threads) {
        t.join();
      }
    } else {
      update_labels(0, num_vertices);
    }

    // Swap labels
    std::swap(label, new_label);

    int changed = changed_count.load(std::memory_order_relaxed);
    LOG(INFO) << "CDLP: Iteration " << (iter + 1) << ", changed=" << changed;

    // Check convergence
    if (changed == 0) {
      LOG(INFO) << "CDLP: Converged after " << (iter + 1) << " iterations";
      break;
    }
  }

  // Build result columns
  neug::execution::ValueColumnBuilder<int64_t> node_builder;
  neug::execution::ValueColumnBuilder<int64_t> label_builder;

  for (size_t i = 0; i < num_vertices; ++i) {
    node_builder.push_back_opt(external_ids[i]);
    // Convert internal label to external ID
    label_builder.push_back_opt(external_ids[label[i]]);
  }

  ctx.set(0, node_builder.finish());
  ctx.set(1, label_builder.finish());
  ctx.tag_ids = {0, 1};

  return ctx;
}

function_set LabelPropagationFunction::getFunctionSet() {
  function_set functionSet;

  // Overload 1: CALL label_propagation() - use full graph with defaults
  {
    auto function = std::make_unique<NeugCallFunction>(
        LabelPropagationFunction::name,
        std::vector<neug::common::LogicalTypeID>{},
        std::vector<std::pair<std::string, neug::common::LogicalTypeID>>{
            {"node", common::LogicalTypeID::INT64},
            {"label", common::LogicalTypeID::INT64}
        });

    function->bindFunc = [](const neug::Schema& schema,
        const neug::execution::ContextMeta& ctx_meta,
        const ::physical::PhysicalPlan& plan,
        int op_idx) -> std::unique_ptr<CallFuncInputBase> {
      auto input = std::make_unique<LabelPropagationFuncInput>();
      input->maxIterations = 10;
      input->graphName = "";
      input->concurrency = 0;
      return input;
    };

    function->execFunc = [](const CallFuncInputBase& input, neug::IStorageInterface& graph) {
      try {
        const LabelPropagationFuncInput& lp_input = static_cast<const LabelPropagationFuncInput&>(input);
        const ProjectedSubgraph* subgraph = getSubgraph(lp_input.graphName);
        int concurrency = lp_input.concurrency > 0 ?
            lp_input.concurrency : std::thread::hardware_concurrency();
        return executeLabelPropagation(lp_input.maxIterations, concurrency, subgraph, graph);
      } catch (const std::exception& e) {
        LOG(ERROR) << "Label Propagation failed: " << e.what();
        THROW_EXTENSION_EXCEPTION("Label Propagation failed: " + std::string(e.what()));
      }
    };

    functionSet.push_back(std::move(function));
  }

  // Overload 2: CALL label_propagation(graph_name, {max_iterations: 10, concurrency: N})
  {
    auto function = std::make_unique<NeugCallFunction>(
        LabelPropagationFunction::name,
        std::vector<neug::common::LogicalTypeID>{
            common::LogicalTypeID::STRING,
            common::LogicalTypeID::MAP
        },
        std::vector<std::pair<std::string, neug::common::LogicalTypeID>>{
            {"node", common::LogicalTypeID::INT64},
            {"label", common::LogicalTypeID::INT64}
        });

    function->bindFunc = [](const neug::Schema& schema,
        const neug::execution::ContextMeta& ctx_meta,
        const ::physical::PhysicalPlan& plan,
        int op_idx) -> std::unique_ptr<CallFuncInputBase> {
      auto input = std::make_unique<LabelPropagationFuncInput>();
      input->maxIterations = 10;

      auto procedurePB = plan.plan(op_idx).opr().procedure_call();
      GDSParams params = parseGDSParams(procedurePB.query());

      input->graphName = params.graphName;
      input->concurrency = params.concurrency;
      if (params.intParam2 > 0) input->maxIterations = params.intParam2;

      return input;
    };

    function->execFunc = [](const CallFuncInputBase& input, neug::IStorageInterface& graph) {
      try {
        const LabelPropagationFuncInput& lp_input = static_cast<const LabelPropagationFuncInput&>(input);
        const ProjectedSubgraph* subgraph = getSubgraph(lp_input.graphName);
        int concurrency = lp_input.concurrency > 0 ?
            lp_input.concurrency : std::thread::hardware_concurrency();
        return executeLabelPropagation(lp_input.maxIterations, concurrency, subgraph, graph);
      } catch (const std::exception& e) {
        LOG(ERROR) << "Label Propagation failed: " << e.what();
        THROW_EXTENSION_EXCEPTION("Label Propagation failed: " + std::string(e.what()));
      }
    };

    functionSet.push_back(std::move(function));
  }

  return functionSet;
}

}  // namespace function
}  // namespace neug