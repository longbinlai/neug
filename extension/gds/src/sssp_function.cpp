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

#include "sssp_function.h"
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
 * @brief Execute SSSP algorithm on the graph.
 *
 * Uses BFS for unweighted graphs, Dijkstra for weighted graphs.
 * Currently implements BFS-based unweighted SSSP.
 */
static execution::Context executeSSSP(
    int64_t sourceNode,
    int64_t targetNode,
    const std::string& weightProperty,
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
    LOG(WARNING) << "SSSP: No vertices found in the graph";
    return ctx;
  }

  LOG(INFO) << "SSSP: Processing " << vertex_labels.size() << " vertex labels";

  // Build vertex mapping
  auto vertex_hash = [](const std::pair<label_t, vid_t>& p) {
    return std::hash<label_t>()(p.first) ^ (std::hash<vid_t>()(p.second) << 1);
  };
  std::unordered_map<std::pair<label_t, vid_t>, size_t, decltype(vertex_hash)> vertex_to_idx(0, vertex_hash);
  std::vector<std::pair<label_t, vid_t>> idx_to_vertex;
  std::unordered_map<label_t, std::vector<vid_t>> vertices_by_label;
  std::unordered_map<int64_t, std::pair<label_t, vid_t>> external_to_internal;

  for (label_t label : vertex_labels) {
    VertexSet vertices = reader->GetVertexSet(label);
    for (vid_t v : vertices) {
      size_t idx = idx_to_vertex.size();
      idx_to_vertex.push_back({label, v});
      vertex_to_idx[{label, v}] = idx;
      vertices_by_label[label].push_back(v);

      Property external_id = reader->GetVertexId(label, v);
      external_to_internal[external_id.as_int64()] = {label, v};
    }
  }

  size_t num_vertices = idx_to_vertex.size();
  if (num_vertices == 0) {
    LOG(WARNING) << "SSSP: No vertices found after filtering";
    return ctx;
  }

  LOG(INFO) << "SSSP: Found " << num_vertices << " vertices";

  // Find source vertex
  auto source_it = external_to_internal.find(sourceNode);
  if (source_it == external_to_internal.end()) {
    LOG(WARNING) << "SSSP: Source node " << sourceNode << " not found";
    return ctx;
  }

  auto [source_label, source_vid] = source_it->second;
  auto source_idx_it = vertex_to_idx.find({source_label, source_vid});
  if (source_idx_it == vertex_to_idx.end()) {
    LOG(WARNING) << "SSSP: Source vertex index not found";
    return ctx;
  }
  size_t source_idx = source_idx_it->second;

  // Find target vertex if specified
  size_t target_idx = std::numeric_limits<size_t>::max();
  bool has_target = targetNode >= 0;
  if (has_target) {
    auto target_it = external_to_internal.find(targetNode);
    if (target_it != external_to_internal.end()) {
      auto target_idx_it = vertex_to_idx.find(target_it->second);
      if (target_idx_it != vertex_to_idx.end()) {
        target_idx = target_idx_it->second;
      }
    }
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

  // Build adjacency list (directed graph - outgoing edges only)
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
        }
      }
    }
  }

  LOG(INFO) << "SSSP: Built adjacency list";

  // BFS-based SSSP (unweighted)
  // For weighted graphs, we would use Dijkstra's algorithm
  const double INF = std::numeric_limits<double>::infinity();
  std::vector<double> distance(num_vertices, INF);

  std::queue<size_t> queue;
  distance[source_idx] = 0.0;
  queue.push(source_idx);

  while (!queue.empty()) {
    size_t curr = queue.front();
    queue.pop();

    // Early termination if we found the target
    if (has_target && curr == target_idx) {
      break;
    }

    for (size_t neighbor : adj_list[curr]) {
      if (distance[neighbor] == INF) {
        distance[neighbor] = distance[curr] + 1.0;
        queue.push(neighbor);
      }
    }
  }

  LOG(INFO) << "SSSP: Computed shortest paths";

  // Build result columns
  neug::execution::ValueColumnBuilder<int64_t> node_builder;
  neug::execution::ValueColumnBuilder<double> distance_builder;

  for (size_t i = 0; i < num_vertices; ++i) {
    if (distance[i] < INF) {  // Only include reachable nodes
      Property external_id = reader->GetVertexId(idx_to_vertex[i].first, idx_to_vertex[i].second);
      node_builder.push_back_opt(external_id.as_int64());
      distance_builder.push_back_opt(distance[i]);
    }
  }

  ctx.set(0, node_builder.finish());
  ctx.set(1, distance_builder.finish());
  ctx.tag_ids = {0, 1};

  return ctx;
}

function_set SSSPFunction::getFunctionSet() {
  function_set functionSet;

  // Overload 1: CALL shortest_path(source) - use full graph
  {
    auto function = std::make_unique<NeugCallFunction>(
        SSSPFunction::name,
        std::vector<neug::common::LogicalTypeID>{
            common::LogicalTypeID::INT64  // source node ID
        },
        std::vector<std::pair<std::string, neug::common::LogicalTypeID>>{
            {"node", common::LogicalTypeID::INT64},
            {"distance", common::LogicalTypeID::DOUBLE}
        });

    function->bindFunc = [](const neug::Schema& schema,
        const neug::execution::ContextMeta& ctx_meta,
        const ::physical::PhysicalPlan& plan,
        int op_idx) -> std::unique_ptr<CallFuncInputBase> {
      auto input = std::make_unique<SSSPFuncInput>();
      input->sourceNode = 0;
      input->targetNode = -1;
      input->graphName = "";

      auto procedurePB = plan.plan(op_idx).opr().procedure_call();
      const auto& args = procedurePB.query().arguments();
      if (!args.empty() && args[0].has_const_()) {
        const auto& const_val = args[0].const_();
        if (const_val.has_i64()) {
          input->sourceNode = const_val.i64();
        } else if (const_val.has_i32()) {
          input->sourceNode = const_val.i32();
        }
      }
      return input;
    };

    function->execFunc = [](const CallFuncInputBase& input, neug::IStorageInterface& graph) {
      try {
        const SSSPFuncInput& sssp_input = static_cast<const SSSPFuncInput&>(input);
        const ProjectedSubgraph* subgraph = getSubgraph(sssp_input.graphName);
        return executeSSSP(sssp_input.sourceNode, sssp_input.targetNode,
            sssp_input.weightProperty, subgraph, graph);
      } catch (const std::exception& e) {
        LOG(ERROR) << "SSSP failed: " << e.what();
        THROW_EXTENSION_EXCEPTION("SSSP failed: " + std::string(e.what()));
      }
    };

    functionSet.push_back(std::move(function));
  }

  // Overload 2: CALL shortest_path(graph_name, {source: X, target: Y, weight_property: 'weight'})
  {
    auto function = std::make_unique<NeugCallFunction>(
        SSSPFunction::name,
        std::vector<neug::common::LogicalTypeID>{
            common::LogicalTypeID::STRING,
            common::LogicalTypeID::MAP
        },
        std::vector<std::pair<std::string, neug::common::LogicalTypeID>>{
            {"node", common::LogicalTypeID::INT64},
            {"distance", common::LogicalTypeID::DOUBLE}
        });

    function->bindFunc = [](const neug::Schema& schema,
        const neug::execution::ContextMeta& ctx_meta,
        const ::physical::PhysicalPlan& plan,
        int op_idx) -> std::unique_ptr<CallFuncInputBase> {
      auto input = std::make_unique<SSSPFuncInput>();
      input->sourceNode = 0;
      input->targetNode = -1;

      auto procedurePB = plan.plan(op_idx).opr().procedure_call();
      GDSParams params = parseGDSParams(procedurePB.query());

      input->graphName = params.graphName;
      input->sourceNode = params.intParam1;
      input->targetNode = params.intParam2 > 0 ? params.intParam2 : -1;
      input->weightProperty = params.stringParam1;

      return input;
    };

    function->execFunc = [](const CallFuncInputBase& input, neug::IStorageInterface& graph) {
      try {
        const SSSPFuncInput& sssp_input = static_cast<const SSSPFuncInput&>(input);
        const ProjectedSubgraph* subgraph = getSubgraph(sssp_input.graphName);
        return executeSSSP(sssp_input.sourceNode, sssp_input.targetNode,
            sssp_input.weightProperty, subgraph, graph);
      } catch (const std::exception& e) {
        LOG(ERROR) << "SSSP failed: " << e.what();
        THROW_EXTENSION_EXCEPTION("SSSP failed: " + std::string(e.what()));
      }
    };

    functionSet.push_back(std::move(function));
  }

  return functionSet;
}

}  // namespace function
}  // namespace neug