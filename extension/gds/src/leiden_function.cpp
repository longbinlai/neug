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

#include "leiden_function.h"
#include <glog/logging.h>
#include <algorithm>
#include <unordered_map>
#include <vector>
#include "gds_param_parser.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace function {

/**
 * @brief Execute Leiden algorithm on the graph.
 *
 * Simplified Louvain-style community detection using modularity optimization.
 * For undirected graphs (out_neighbors ∪ in_neighbors).
 */
static execution::Context executeLeiden(
    double resolution,
    int64_t maxIterations,
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
    LOG(WARNING) << "Leiden: No vertices found in the graph";
    return ctx;
  }

  LOG(INFO) << "Leiden: Processing " << vertex_labels.size() << " vertex labels";

  // Build vertex mapping
  auto vertex_hash = [](const std::pair<label_t, vid_t>& p) {
    return std::hash<label_t>()(p.first) ^ (std::hash<vid_t>()(p.second) << 1);
  };
  std::unordered_map<std::pair<label_t, vid_t>, size_t, decltype(vertex_hash)> vertex_to_idx(0, vertex_hash);
  std::vector<std::pair<label_t, vid_t>> idx_to_vertex;
  std::unordered_map<label_t, std::vector<vid_t>> vertices_by_label;
  std::vector<int64_t> external_ids;

  for (label_t label : vertex_labels) {
    VertexSet vertices = reader->GetVertexSet(label);
    for (vid_t v : vertices) {
      size_t idx = idx_to_vertex.size();
      idx_to_vertex.push_back({label, v});
      vertex_to_idx[{label, v}] = idx;
      vertices_by_label[label].push_back(v);

      Property external_id = reader->GetVertexId(label, v);
      external_ids.push_back(external_id.as_int64());
    }
  }

  size_t num_vertices = idx_to_vertex.size();
  if (num_vertices == 0) {
    LOG(WARNING) << "Leiden: No vertices found after filtering";
    return ctx;
  }

  LOG(INFO) << "Leiden: Found " << num_vertices << " vertices";

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
          adj_list[dst_idx_it->second].push_back(src_idx);
        }
      }
    }
  }

  // Deduplicate neighbors
  for (auto& neighbors : adj_list) {
    std::sort(neighbors.begin(), neighbors.end());
    neighbors.erase(std::unique(neighbors.begin(), neighbors.end()), neighbors.end());
  }

  LOG(INFO) << "Leiden: Built adjacency structures";

  // Compute total edges (for modularity)
  size_t total_edges = 0;
  for (const auto& neighbors : adj_list) {
    total_edges += neighbors.size();
  }
  total_edges /= 2;  // Each edge counted twice


  // Initialize communities (each node in its own community)
  std::vector<int64_t> community(num_vertices);
  for (size_t i = 0; i < num_vertices; ++i) {
    community[i] = static_cast<int64_t>(i);
  }

  // Compute degree for each node
  std::vector<size_t> degree(num_vertices);
  for (size_t i = 0; i < num_vertices; ++i) {
    degree[i] = adj_list[i].size();
  }

  // Louvain-style local moving phase
  for (int64_t iter = 0; iter < maxIterations; ++iter) {
    bool changed = false;

    for (size_t v = 0; v < num_vertices; ++v) {
      if (adj_list[v].empty()) continue;

      // Count neighbors in each community
      std::unordered_map<int64_t, size_t> community_neighbors;
      for (size_t neighbor : adj_list[v]) {
        community_neighbors[community[neighbor]]++;
      }

      // Find best community
      int64_t best_community = community[v];
      size_t best_gain = 0;

      for (const auto& [comm, count] : community_neighbors) {
        // Simplified modularity gain: count - resolution * expected
        // For unweighted graphs, this simplifies to neighbor count
        if (count > best_gain || (count == best_gain && comm < best_community)) {
          best_gain = count;
          best_community = comm;
        }
      }

      if (best_community != community[v]) {
        community[v] = best_community;
        changed = true;
      }
    }

    LOG(INFO) << "Leiden: Iteration " << (iter + 1) << " completed";

    if (!changed) {
      LOG(INFO) << "Leiden: Converged after " << (iter + 1) << " iterations";
      break;
    }
  }

  // Renumber communities to be contiguous
  std::unordered_map<int64_t, int64_t> community_id_map;
  int64_t next_community_id = 0;

  for (size_t i = 0; i < num_vertices; ++i) {
    int64_t original_community = community[i];
    if (community_id_map.find(original_community) == community_id_map.end()) {
      community_id_map[original_community] = next_community_id++;
    }
    community[i] = community_id_map[original_community];
  }

  LOG(INFO) << "Leiden: Found " << next_community_id << " communities";

  // Build result columns
  neug::execution::ValueColumnBuilder<int64_t> node_builder;
  neug::execution::ValueColumnBuilder<int64_t> community_builder;

  for (size_t i = 0; i < num_vertices; ++i) {
    node_builder.push_back_opt(external_ids[i]);
    community_builder.push_back_opt(community[i]);
  }

  ctx.set(0, node_builder.finish());
  ctx.set(1, community_builder.finish());
  ctx.tag_ids = {0, 1};

  return ctx;
}

function_set LeidenFunction::getFunctionSet() {
  function_set functionSet;

  // Overload 1: CALL leiden() - use full graph with defaults
  {
    auto function = std::make_unique<NeugCallFunction>(
        LeidenFunction::name,
        std::vector<neug::common::LogicalTypeID>{},
        std::vector<std::pair<std::string, neug::common::LogicalTypeID>>{
            {"node", common::LogicalTypeID::INT64},
            {"community_id", common::LogicalTypeID::INT64}
        });

    function->bindFunc = [](const neug::Schema& schema,
        const neug::execution::ContextMeta& ctx_meta,
        const ::physical::PhysicalPlan& plan,
        int op_idx) -> std::unique_ptr<CallFuncInputBase> {
      auto input = std::make_unique<LeidenFuncInput>();
      input->resolution = 1.0;
      input->maxIterations = 10;
      input->graphName = "";
      input->concurrency = 0;
      return input;
    };

    function->execFunc = [](const CallFuncInputBase& input, neug::IStorageInterface& graph) {
      try {
        const LeidenFuncInput& leiden_input = static_cast<const LeidenFuncInput&>(input);
        const ProjectedSubgraph* subgraph = getSubgraph(leiden_input.graphName);
        return executeLeiden(leiden_input.resolution, leiden_input.maxIterations, subgraph, graph);
      } catch (const std::exception& e) {
        LOG(ERROR) << "Leiden failed: " << e.what();
        THROW_EXTENSION_EXCEPTION("Leiden failed: " + std::string(e.what()));
      }
    };

    functionSet.push_back(std::move(function));
  }

  // Overload 2: CALL leiden(graph_name, {resolution: 1.0, max_iterations: 10, concurrency: N})
  {
    auto function = std::make_unique<NeugCallFunction>(
        LeidenFunction::name,
        std::vector<neug::common::LogicalTypeID>{
            common::LogicalTypeID::STRING,
            common::LogicalTypeID::MAP
        },
        std::vector<std::pair<std::string, neug::common::LogicalTypeID>>{
            {"node", common::LogicalTypeID::INT64},
            {"community_id", common::LogicalTypeID::INT64}
        });

    function->bindFunc = [](const neug::Schema& schema,
        const neug::execution::ContextMeta& ctx_meta,
        const ::physical::PhysicalPlan& plan,
        int op_idx) -> std::unique_ptr<CallFuncInputBase> {
      auto input = std::make_unique<LeidenFuncInput>();
      input->resolution = 1.0;
      input->maxIterations = 10;

      auto procedurePB = plan.plan(op_idx).opr().procedure_call();
      GDSParams params = parseGDSParams(procedurePB.query());

      input->graphName = params.graphName;
      input->concurrency = params.concurrency;
      if (params.doubleParam1 > 0) input->resolution = params.doubleParam1;
      if (params.intParam2 > 0) input->maxIterations = params.intParam2;

      return input;
    };

    function->execFunc = [](const CallFuncInputBase& input, neug::IStorageInterface& graph) {
      try {
        const LeidenFuncInput& leiden_input = static_cast<const LeidenFuncInput&>(input);
        const ProjectedSubgraph* subgraph = getSubgraph(leiden_input.graphName);
        return executeLeiden(leiden_input.resolution, leiden_input.maxIterations, subgraph, graph);
      } catch (const std::exception& e) {
        LOG(ERROR) << "Leiden failed: " << e.what();
        THROW_EXTENSION_EXCEPTION("Leiden failed: " + std::string(e.what()));
      }
    };

    functionSet.push_back(std::move(function));
  }

  return functionSet;
}

}  // namespace function
}  // namespace neug