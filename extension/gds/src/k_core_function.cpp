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

#include "k_core_function.h"
#include <glog/logging.h>
#include <algorithm>
#include <queue>
#include <unordered_map>
#include <vector>
#include "gds_param_parser.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace function {

/**
 * @brief Execute K-Core Decomposition algorithm on the graph.
 *
 * Uses the Batagelj-Zaversnik algorithm to compute core numbers.
 * K-Core treats the graph as undirected, using both outgoing and incoming edges.
 */
static execution::Context executeKCore(
    int64_t minK,
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
    LOG(WARNING) << "K-Core: No vertices found in the graph";
    return ctx;
  }

  LOG(INFO) << "K-Core: Processing " << vertex_labels.size() << " vertex labels";

  // Build vertex mapping
  auto vertex_hash = [](const std::pair<label_t, vid_t>& p) {
    return std::hash<label_t>()(p.first) ^ (std::hash<vid_t>()(p.second) << 1);
  };
  std::unordered_map<std::pair<label_t, vid_t>, size_t, decltype(vertex_hash)> vertex_to_idx(0, vertex_hash);
  std::vector<std::pair<label_t, vid_t>> idx_to_vertex;
  std::unordered_map<label_t, std::vector<vid_t>> vertices_by_label;

  for (label_t label : vertex_labels) {
    VertexSet vertices = reader->GetVertexSet(label);
    for (vid_t v : vertices) {
      size_t idx = idx_to_vertex.size();
      idx_to_vertex.push_back({label, v});
      vertex_to_idx[{label, v}] = idx;
      vertices_by_label[label].push_back(v);
    }
  }

  size_t num_vertices = idx_to_vertex.size();
  if (num_vertices == 0) {
    LOG(WARNING) << "K-Core: No vertices found after filtering";
    return ctx;
  }

  LOG(INFO) << "K-Core: Found " << num_vertices << " vertices";

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

  // Build degree array for K-Core (undirected graph semantics)
  // For undirected graph, degree = number of unique neighbors (out ∪ in)
  // We compute degrees directly from storage iterators without building full adjacency list

  std::vector<size_t> degree(num_vertices, 0);
  std::vector<std::vector<size_t>> adj_list(num_vertices);  // Still needed for BZ algorithm

  for (const auto& [src_label, edge_label, dst_label] : edge_triplets) {
    // Process outgoing edges
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
          // For undirected graph, add edge in both directions
          // But we'll deduplicate later
          adj_list[src_idx].push_back(dst_idx_it->second);
          adj_list[dst_idx_it->second].push_back(src_idx);
        }
      }
    }
  }

  // Deduplicate neighbors (important for undirected graph semantics)
  // This ensures each neighbor is counted only once for degree calculation
  size_t max_degree = 0;
  for (size_t i = 0; i < num_vertices; ++i) {
    std::sort(adj_list[i].begin(), adj_list[i].end());
    adj_list[i].erase(std::unique(adj_list[i].begin(), adj_list[i].end()), adj_list[i].end());
    degree[i] = adj_list[i].size();
    max_degree = std::max(max_degree, degree[i]);
  }

  LOG(INFO) << "K-Core: Built adjacency structures, max_degree=" << max_degree;

  // Batagelj-Zaversnik algorithm
  // Sort vertices by degree using bucket sort
  std::vector<std::vector<size_t>> buckets(max_degree + 1);
  std::vector<bool> removed(num_vertices, false);

  for (size_t i = 0; i < num_vertices; ++i) {
    buckets[degree[i]].push_back(i);
  }

  std::vector<int64_t> core_number(num_vertices, 0);

  for (size_t k = 0; k <= max_degree; ++k) {
    while (!buckets[k].empty()) {
      size_t v = buckets[k].back();
      buckets[k].pop_back();

      if (removed[v]) continue;

      // Assign core number
      core_number[v] = k;
      removed[v] = true;

      // Update neighbors
      for (size_t neighbor : adj_list[v]) {
        if (removed[neighbor]) continue;

        size_t& neighbor_degree = degree[neighbor];
        if (neighbor_degree > k) {
          // Move neighbor to lower degree bucket
          neighbor_degree--;
          buckets[neighbor_degree].push_back(neighbor);
        }
      }
    }
  }

  LOG(INFO) << "K-Core: Computed core numbers";

  // Build result columns
  neug::execution::ValueColumnBuilder<int64_t> node_builder;
  neug::execution::ValueColumnBuilder<int64_t> core_builder;

  size_t result_count = 0;
  for (size_t i = 0; i < num_vertices; ++i) {
    if (core_number[i] >= minK) {
      Property external_id = reader->GetVertexId(idx_to_vertex[i].first, idx_to_vertex[i].second);
      node_builder.push_back_opt(external_id.as_int64());
      core_builder.push_back_opt(core_number[i]);
      result_count++;
    }
  }

  LOG(INFO) << "K-Core: Found " << result_count << " vertices with core number >= " << minK;

  ctx.set(0, node_builder.finish());
  ctx.set(1, core_builder.finish());
  ctx.tag_ids = {0, 1};

  return ctx;
}

function_set KCoreFunction::getFunctionSet() {
  function_set functionSet;

  // Overload 1: CALL k_core() - use full graph with defaults
  {
    auto function = std::make_unique<NeugCallFunction>(
        KCoreFunction::name,
        std::vector<neug::common::LogicalTypeID>{},
        std::vector<std::pair<std::string, neug::common::LogicalTypeID>>{
            {"node", common::LogicalTypeID::INT64},
            {"core_number", common::LogicalTypeID::INT64}
        });

    function->bindFunc = [](const neug::Schema& schema,
        const neug::execution::ContextMeta& ctx_meta,
        const ::physical::PhysicalPlan& plan,
        int op_idx) -> std::unique_ptr<CallFuncInputBase> {
      auto input = std::make_unique<KCoreFuncInput>();
      input->minK = 1;
      input->graphName = "";
      input->concurrency = 0;
      return input;
    };

    function->execFunc = [](const CallFuncInputBase& input, neug::IStorageInterface& graph) {
      try {
        const KCoreFuncInput& kcore_input = static_cast<const KCoreFuncInput&>(input);
        const ProjectedSubgraph* subgraph = getSubgraph(kcore_input.graphName);
        return executeKCore(kcore_input.minK, subgraph, graph);
      } catch (const std::exception& e) {
        LOG(ERROR) << "K-Core failed: " << e.what();
        THROW_EXTENSION_EXCEPTION("K-Core failed: " + std::string(e.what()));
      }
    };

    functionSet.push_back(std::move(function));
  }

  // Overload 2: CALL k_core(graph_name, {min_k: 3, concurrency: N})
  {
    auto function = std::make_unique<NeugCallFunction>(
        KCoreFunction::name,
        std::vector<neug::common::LogicalTypeID>{
            common::LogicalTypeID::STRING,
            common::LogicalTypeID::MAP
        },
        std::vector<std::pair<std::string, neug::common::LogicalTypeID>>{
            {"node", common::LogicalTypeID::INT64},
            {"core_number", common::LogicalTypeID::INT64}
        });

    function->bindFunc = [](const neug::Schema& schema,
        const neug::execution::ContextMeta& ctx_meta,
        const ::physical::PhysicalPlan& plan,
        int op_idx) -> std::unique_ptr<CallFuncInputBase> {
      auto input = std::make_unique<KCoreFuncInput>();
      input->minK = 1;

      auto procedurePB = plan.plan(op_idx).opr().procedure_call();
      GDSParams params = parseGDSParams(procedurePB.query());

      input->graphName = params.graphName;
      input->concurrency = params.concurrency;
      if (params.intParam1 > 0) input->minK = params.intParam1;

      return input;
    };

    function->execFunc = [](const CallFuncInputBase& input, neug::IStorageInterface& graph) {
      try {
        const KCoreFuncInput& kcore_input = static_cast<const KCoreFuncInput&>(input);
        const ProjectedSubgraph* subgraph = getSubgraph(kcore_input.graphName);
        return executeKCore(kcore_input.minK, subgraph, graph);
      } catch (const std::exception& e) {
        LOG(ERROR) << "K-Core failed: " << e.what();
        THROW_EXTENSION_EXCEPTION("K-Core failed: " + std::string(e.what()));
      }
    };

    functionSet.push_back(std::move(function));
  }

  return functionSet;
}

}  // namespace function
}  // namespace neug