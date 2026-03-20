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

#include "page_rank_function.h"
#include <glog/logging.h>
#include <algorithm>
#include <atomic>
#include <cmath>
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
 * @brief Execute PageRank algorithm with parallel iteration.
 */
static execution::Context executePageRank(
    double damping,
    int maxIterations,
    double tolerance,
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
    LOG(WARNING) << "PageRank: No vertices found in the graph";
    return ctx;
  }

  LOG(INFO) << "PageRank: Processing " << vertex_labels.size() << " vertex labels";

  // Collect all vertices
  std::vector<std::pair<label_t, vid_t>> all_vertices;
  std::unordered_map<label_t, std::vector<vid_t>> vertices_by_label;

  for (label_t label : vertex_labels) {
    VertexSet vertices = reader->GetVertexSet(label);
    for (vid_t v : vertices) {
      all_vertices.push_back({label, v});
      vertices_by_label[label].push_back(v);
    }
  }

  size_t num_vertices = all_vertices.size();
  if (num_vertices == 0) {
    LOG(WARNING) << "PageRank: No vertices found after filtering";
    return ctx;
  }

  LOG(INFO) << "PageRank: Found " << num_vertices << " vertices";

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

  // Build vertex ID to index mapping
  auto vertex_hash = [](const std::pair<label_t, vid_t>& p) {
    return std::hash<label_t>()(p.first) ^ (std::hash<vid_t>()(p.second) << 1);
  };
  std::unordered_map<std::pair<label_t, vid_t>, size_t, decltype(vertex_hash)> vertex_to_idx(0, vertex_hash);
  for (size_t i = 0; i < num_vertices; ++i) {
    vertex_to_idx[all_vertices[i]] = i;
  }

  // Build adjacency structures (undirected graph semantics)
  std::vector<size_t> degree(num_vertices, 0);
  std::vector<std::vector<size_t>> neighbors(num_vertices);

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
          size_t dst_idx = dst_idx_it->second;
          degree[src_idx]++;
          neighbors[dst_idx].push_back(src_idx);
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
          size_t src_idx = src_idx_it->second;
          degree[dst_idx]++;
          neighbors[src_idx].push_back(dst_idx);
        }
      }
    }
  }

  LOG(INFO) << "PageRank: Built adjacency structures (undirected)";

  // Initialize PageRank values
  double initial_rank = 1.0 / num_vertices;
  std::vector<double> rank(num_vertices, initial_rank);
  std::vector<double> new_rank(num_vertices);

  // Identify sink vertices
  std::vector<size_t> sinks;
  for (size_t i = 0; i < num_vertices; ++i) {
    if (degree[i] == 0) {
      sinks.push_back(i);
    }
  }

  LOG(INFO) << "PageRank: Found " << sinks.size() << " sink vertices";

  // Determine number of threads
  int num_threads = concurrency > 0 ? concurrency : std::thread::hardware_concurrency();
  if (num_threads < 1) num_threads = 1;
  
  LOG(INFO) << "PageRank: Using " << num_threads << " threads";

  // Iterative PageRank computation with parallel updates
  for (int iter = 0; iter < maxIterations; ++iter) {
    // Calculate sink contribution (sequential, small)
    double sink_contribution = 0.0;
    for (size_t sink_idx : sinks) {
      sink_contribution += rank[sink_idx];
    }
    sink_contribution /= num_vertices;

    // Base rank
    double base_rank = (1.0 - damping) / num_vertices + damping * sink_contribution;

    // Parallel rank computation
    std::atomic<double> max_diff{0.0};
    
    auto compute_ranks = [&](size_t start, size_t end) {
      double local_max_diff = 0.0;
      for (size_t v = start; v < end; ++v) {
        double sum = 0.0;
        for (size_t u : neighbors[v]) {
          if (degree[u] > 0) {
            sum += rank[u] / degree[u];
          }
        }
        new_rank[v] = base_rank + damping * sum;

        double diff = std::abs(new_rank[v] - rank[v]);
        if (diff > local_max_diff) {
          local_max_diff = diff;
        }
      }
      
      // Update global max_diff atomically
      double old_max = max_diff.load(std::memory_order_relaxed);
      while (local_max_diff > old_max && 
             !max_diff.compare_exchange_weak(old_max, local_max_diff, std::memory_order_relaxed)) {
        // Retry
      }
    };

    if (num_threads > 1 && num_vertices > 10000) {
      std::vector<std::thread> threads;
      size_t chunk_size = (num_vertices + num_threads - 1) / num_threads;
      
      for (int t = 0; t < num_threads; ++t) {
        size_t start = t * chunk_size;
        size_t end = std::min(start + chunk_size, num_vertices);
        if (start < end) {
          threads.emplace_back(compute_ranks, start, end);
        }
      }
      
      for (auto& t : threads) {
        t.join();
      }
    } else {
      compute_ranks(0, num_vertices);
    }

    // Swap ranks
    std::swap(rank, new_rank);

    double current_max_diff = max_diff.load(std::memory_order_relaxed);
    LOG(INFO) << "PageRank: Iteration " << (iter + 1) << ", max_diff=" << current_max_diff;

    if (current_max_diff < tolerance) {
      LOG(INFO) << "PageRank: Converged after " << (iter + 1) << " iterations";
      break;
    }
  }

  // Build result columns
  neug::execution::ValueColumnBuilder<int64_t> node_builder;
  neug::execution::ValueColumnBuilder<double> rank_builder;

  node_builder.reserve(num_vertices);
  rank_builder.reserve(num_vertices);

  for (size_t i = 0; i < num_vertices; ++i) {
    Property external_id = reader->GetVertexId(all_vertices[i].first, all_vertices[i].second);
    int64_t node_id = external_id.as_int64();
    node_builder.push_back_opt(node_id);
    rank_builder.push_back_opt(rank[i]);
  }

  ctx.set(0, node_builder.finish());
  ctx.set(1, rank_builder.finish());
  ctx.tag_ids = {0, 1};

  return ctx;
}

function_set PageRankFunction::getFunctionSet() {
  function_set functionSet;

  // Overload 1: CALL page_rank() - use full graph with defaults
  {
    auto function = std::make_unique<NeugCallFunction>(
        PageRankFunction::name,
        std::vector<neug::common::LogicalTypeID>{},  // No parameters
        std::vector<std::pair<std::string, neug::common::LogicalTypeID>>{
            {"node", common::LogicalTypeID::INT64},
            {"rank", common::LogicalTypeID::DOUBLE}
        });

    function->bindFunc = [](const neug::Schema& schema,
        const neug::execution::ContextMeta& ctx_meta,
        const ::physical::PhysicalPlan& plan,
        int op_idx) -> std::unique_ptr<CallFuncInputBase> {
      auto input = std::make_unique<PageRankFuncInput>();
      input->dampingFactor = 0.85;
      input->maxIterations = 20;
      input->tolerance = 1e-6;
      input->graphName = "";
      input->concurrency = 0;
      return input;
    };

    function->execFunc = [](const CallFuncInputBase& input, neug::IStorageInterface& graph) {
      try {
        const PageRankFuncInput& pr_input = static_cast<const PageRankFuncInput&>(input);
        const ProjectedSubgraph* subgraph = getSubgraph(pr_input.graphName);
        int concurrency = pr_input.concurrency > 0 ?
            pr_input.concurrency : std::thread::hardware_concurrency();
        return executePageRank(pr_input.dampingFactor, pr_input.maxIterations,
            pr_input.tolerance, concurrency, subgraph, graph);
      } catch (const std::exception& e) {
        LOG(ERROR) << "PageRank failed: " << e.what();
        THROW_EXTENSION_EXCEPTION("PageRank failed: " + std::string(e.what()));
      }
    };

    functionSet.push_back(std::move(function));
  }

  // Overload 2: CALL page_rank(graph_name, {damping: 0.85, max_iterations: 20, ...})
  {
    auto function = std::make_unique<NeugCallFunction>(
        PageRankFunction::name,
        std::vector<neug::common::LogicalTypeID>{
            common::LogicalTypeID::STRING,
            common::LogicalTypeID::MAP
        },
        std::vector<std::pair<std::string, neug::common::LogicalTypeID>>{
            {"node", common::LogicalTypeID::INT64},
            {"rank", common::LogicalTypeID::DOUBLE}
        });

    function->bindFunc = [](const neug::Schema& schema,
        const neug::execution::ContextMeta& ctx_meta,
        const ::physical::PhysicalPlan& plan,
        int op_idx) -> std::unique_ptr<CallFuncInputBase> {
      auto input = std::make_unique<PageRankFuncInput>();
      input->dampingFactor = 0.85;
      input->maxIterations = 20;
      input->tolerance = 1e-6;

      auto procedurePB = plan.plan(op_idx).opr().procedure_call();
      GDSParams params = parseGDSParams(procedurePB.query());

      input->graphName = params.graphName;
      input->concurrency = params.concurrency;
      if (params.doubleParam1 > 0) input->dampingFactor = params.doubleParam1;
      if (params.intParam2 > 0) input->maxIterations = params.intParam2;
      if (params.doubleParam2 > 0) input->tolerance = params.doubleParam2;

      return input;
    };

    function->execFunc = [](const CallFuncInputBase& input, neug::IStorageInterface& graph) {
      try {
        const PageRankFuncInput& pr_input = static_cast<const PageRankFuncInput&>(input);
        const ProjectedSubgraph* subgraph = getSubgraph(pr_input.graphName);
        int concurrency = pr_input.concurrency > 0 ?
            pr_input.concurrency : std::thread::hardware_concurrency();
        return executePageRank(pr_input.dampingFactor, pr_input.maxIterations,
            pr_input.tolerance, concurrency, subgraph, graph);
      } catch (const std::exception& e) {
        LOG(ERROR) << "PageRank failed: " << e.what();
        THROW_EXTENSION_EXCEPTION("PageRank failed: " + std::string(e.what()));
      }
    };

    functionSet.push_back(std::move(function));
  }

  return functionSet;
}

}  // namespace function
}  // namespace neug