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

#include "bfs_function.h"
#include <glog/logging.h>
#include <algorithm>
#include <atomic>
#include <queue>
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
 * @brief Execute BFS algorithm with parallel level processing.
 */
static execution::Context executeBFS(
    int64_t sourceNode,
    int64_t maxDepth,
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
    LOG(WARNING) << "BFS: No vertices found in the graph";
    return ctx;
  }

  LOG(INFO) << "BFS: Processing " << vertex_labels.size() << " vertex labels";

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
    LOG(WARNING) << "BFS: No vertices found after filtering";
    return ctx;
  }

  LOG(INFO) << "BFS: Found " << num_vertices << " vertices";

  // Find source vertex
  auto source_it = external_to_internal.find(sourceNode);
  if (source_it == external_to_internal.end()) {
    LOG(WARNING) << "BFS: Source node " << sourceNode << " not found";
    return ctx;
  }

  auto [source_label, source_vid] = source_it->second;
  auto source_idx_it = vertex_to_idx.find({source_label, source_vid});
  if (source_idx_it == vertex_to_idx.end()) {
    LOG(WARNING) << "BFS: Source vertex index not found";
    return ctx;
  }
  size_t source_idx = source_idx_it->second;

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

  LOG(INFO) << "BFS: Built adjacency list (undirected)";

  // Determine number of threads
  int num_threads = concurrency > 0 ? concurrency : std::thread::hardware_concurrency();
  if (num_threads < 1) num_threads = 1;
  
  LOG(INFO) << "BFS: Using " << num_threads << " threads";

  // BFS traversal with parallel level processing
  std::vector<std::atomic<int64_t>> distance(num_vertices);
  for (size_t i = 0; i < num_vertices; ++i) {
    distance[i].store(-1, std::memory_order_relaxed);
  }
  distance[source_idx].store(0, std::memory_order_relaxed);

  // Current frontier and next frontier
  std::vector<size_t> current_frontier;
  std::vector<std::atomic<bool>> in_next_frontier(num_vertices);
  for (size_t i = 0; i < num_vertices; ++i) {
    in_next_frontier[i].store(false, std::memory_order_relaxed);
  }
  
  current_frontier.push_back(source_idx);
  int64_t current_distance = 0;

  while (!current_frontier.empty() && (maxDepth <= 0 || current_distance < maxDepth)) {
    current_distance++;
    
    // Parallel expansion of current frontier
    size_t frontier_size = current_frontier.size();
    std::vector<std::vector<size_t>> local_next_frontiers(num_threads);
    
    auto expand_frontier = [&](int thread_id, size_t start, size_t end) {
      std::vector<size_t>& local_next = local_next_frontiers[thread_id];
      
      for (size_t i = start; i < end; ++i) {
        size_t curr = current_frontier[i];
        for (size_t neighbor : adj_list[curr]) {
          // Try to claim this vertex
          int64_t expected = -1;
          if (distance[neighbor].compare_exchange_strong(expected, current_distance, 
              std::memory_order_relaxed)) {
            // Successfully claimed, add to local frontier
            local_next.push_back(neighbor);
          }
        }
      }
    };

    if (num_threads > 1 && frontier_size > 100) {
      std::vector<std::thread> threads;
      size_t chunk_size = (frontier_size + num_threads - 1) / num_threads;
      
      for (int t = 0; t < num_threads; ++t) {
        size_t start = t * chunk_size;
        size_t end = std::min(start + chunk_size, frontier_size);
        if (start < end) {
          threads.emplace_back(expand_frontier, t, start, end);
        }
      }
      
      for (auto& t : threads) {
        t.join();
      }
    } else {
      expand_frontier(0, 0, frontier_size);
    }

    // Merge local frontiers
    current_frontier.clear();
    for (int t = 0; t < num_threads; ++t) {
      current_frontier.insert(current_frontier.end(), 
                               local_next_frontiers[t].begin(), 
                               local_next_frontiers[t].end());
    }
  }

  LOG(INFO) << "BFS: Finished traversal";

  // Build result columns
  neug::execution::ValueColumnBuilder<int64_t> node_builder;
  neug::execution::ValueColumnBuilder<int64_t> distance_builder;

  size_t reachable_count = 0;
  for (size_t i = 0; i < num_vertices; ++i) {
    int64_t dist = distance[i].load(std::memory_order_relaxed);
    if (dist >= 0) {
      Property external_id = reader->GetVertexId(idx_to_vertex[i].first, idx_to_vertex[i].second);
      node_builder.push_back_opt(external_id.as_int64());
      distance_builder.push_back_opt(dist);
      reachable_count++;
    }
  }

  LOG(INFO) << "BFS: Found " << reachable_count << " reachable nodes";

  ctx.set(0, node_builder.finish());
  ctx.set(1, distance_builder.finish());
  ctx.tag_ids = {0, 1};

  return ctx;
}

function_set BFSFunction::getFunctionSet() {
  function_set functionSet;

  // Overload 1: CALL bfs(source) - use full graph
  {
    auto function = std::make_unique<NeugCallFunction>(
        BFSFunction::name,
        std::vector<neug::common::LogicalTypeID>{
            common::LogicalTypeID::INT64  // source vertex
        },
        std::vector<std::pair<std::string, neug::common::LogicalTypeID>>{
            {"node", common::LogicalTypeID::INT64},
            {"distance", common::LogicalTypeID::INT64}
        });

    function->bindFunc = [](const neug::Schema& schema,
        const neug::execution::ContextMeta& ctx_meta,
        const ::physical::PhysicalPlan& plan,
        int op_idx) -> std::unique_ptr<CallFuncInputBase> {
      auto input = std::make_unique<BFSFuncInput>();
      input->sourceNode = 0;
      input->maxDepth = -1;
      input->graphName = "";
      input->concurrency = 0;

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
        const BFSFuncInput& bfs_input = static_cast<const BFSFuncInput&>(input);
        const ProjectedSubgraph* subgraph = getSubgraph(bfs_input.graphName);
        int concurrency = bfs_input.concurrency > 0 ?
            bfs_input.concurrency : std::thread::hardware_concurrency();
        return executeBFS(bfs_input.sourceNode, bfs_input.maxDepth, concurrency, subgraph, graph);
      } catch (const std::exception& e) {
        LOG(ERROR) << "BFS failed: " << e.what();
        THROW_EXTENSION_EXCEPTION("BFS failed: " + std::string(e.what()));
      }
    };

    functionSet.push_back(std::move(function));
  }

  // Overload 2: CALL bfs(graph_name, {source: X, max_depth: N, concurrency: M})
  {
    auto function = std::make_unique<NeugCallFunction>(
        BFSFunction::name,
        std::vector<neug::common::LogicalTypeID>{
            common::LogicalTypeID::STRING,
            common::LogicalTypeID::MAP
        },
        std::vector<std::pair<std::string, neug::common::LogicalTypeID>>{
            {"node", common::LogicalTypeID::INT64},
            {"distance", common::LogicalTypeID::INT64}
        });

    function->bindFunc = [](const neug::Schema& schema,
        const neug::execution::ContextMeta& ctx_meta,
        const ::physical::PhysicalPlan& plan,
        int op_idx) -> std::unique_ptr<CallFuncInputBase> {
      auto input = std::make_unique<BFSFuncInput>();
      input->sourceNode = 0;
      input->maxDepth = -1;

      auto procedurePB = plan.plan(op_idx).opr().procedure_call();
      GDSParams params = parseGDSParams(procedurePB.query());

      input->graphName = params.graphName;
      input->concurrency = params.concurrency;
      input->sourceNode = params.intParam1;
      input->maxDepth = params.intParam2 > 0 ? params.intParam2 : -1;

      return input;
    };

    function->execFunc = [](const CallFuncInputBase& input, neug::IStorageInterface& graph) {
      try {
        const BFSFuncInput& bfs_input = static_cast<const BFSFuncInput&>(input);
        const ProjectedSubgraph* subgraph = getSubgraph(bfs_input.graphName);
        int concurrency = bfs_input.concurrency > 0 ?
            bfs_input.concurrency : std::thread::hardware_concurrency();
        return executeBFS(bfs_input.sourceNode, bfs_input.maxDepth, concurrency, subgraph, graph);
      } catch (const std::exception& e) {
        LOG(ERROR) << "BFS failed: " << e.what();
        THROW_EXTENSION_EXCEPTION("BFS failed: " + std::string(e.what()));
      }
    };

    functionSet.push_back(std::move(function));
  }

  return functionSet;
}

}  // namespace function
}  // namespace neug