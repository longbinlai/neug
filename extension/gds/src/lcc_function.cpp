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

#include "lcc_function.h"
#include <glog/logging.h>
#include <algorithm>
#include <atomic>
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
 * @brief Execute LCC algorithm with parallel triangle counting.
 */
static execution::Context executeLCC(
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
    LOG(WARNING) << "LCC: No vertices found in the graph";
    return ctx;
  }

  LOG(INFO) << "LCC: Processing " << vertex_labels.size() << " vertex labels";

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
    LOG(WARNING) << "LCC: No vertices found after filtering";
    return ctx;
  }

  LOG(INFO) << "LCC: Found " << num_vertices << " vertices";

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

  // Build undirected adjacency list
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

  LOG(INFO) << "LCC: Built adjacency structures";

  // Compute degrees
  std::vector<size_t> degree(num_vertices);
  for (size_t v = 0; v < num_vertices; ++v) {
    degree[v] = adj_list[v].size();
  }

  // Create vertex ordering based on degree
  std::vector<size_t> rank(num_vertices);
  std::vector<size_t> vertices_by_rank(num_vertices);
  for (size_t i = 0; i < num_vertices; ++i) {
    vertices_by_rank[i] = i;
  }
  
  std::sort(vertices_by_rank.begin(), vertices_by_rank.end(),
            [&degree](size_t a, size_t b) {
              return degree[a] < degree[b] || (degree[a] == degree[b] && a < b);
            });
  
  for (size_t r = 0; r < num_vertices; ++r) {
    rank[vertices_by_rank[r]] = r;
  }

  LOG(INFO) << "LCC: Computed vertex ranking by degree";

  // Build directed graph for triangle counting
  std::vector<std::vector<size_t>> directed_adj(num_vertices);
  
  for (size_t v = 0; v < num_vertices; ++v) {
    for (size_t u : adj_list[v]) {
      if (rank[v] < rank[u]) {
        directed_adj[v].push_back(u);
      }
    }
    std::sort(directed_adj[v].begin(), directed_adj[v].end());
  }

  LOG(INFO) << "LCC: Built directed graph for triangle counting";

  // Determine number of threads
  int num_threads = concurrency > 0 ? concurrency : std::thread::hardware_concurrency();
  if (num_threads < 1) num_threads = 1;
  
  LOG(INFO) << "LCC: Using " << num_threads << " threads";

  // Count triangles in parallel using local buffers
  std::vector<size_t> triangle_count(num_vertices, 0);

  auto count_triangles = [&](size_t thread_id, size_t start, size_t end) {
    // Local triangle count buffer
    std::vector<size_t> local_count(num_vertices, 0);
    
    for (size_t v = start; v < end; ++v) {
      const auto& out_neighbors = directed_adj[v];
      
      for (size_t i = 0; i < out_neighbors.size(); ++i) {
        size_t u = out_neighbors[i];
        for (size_t j = i + 1; j < out_neighbors.size(); ++j) {
          size_t w = out_neighbors[j];
          
          // Check if edge exists between u and w
          if (rank[u] < rank[w]) {
            const auto& u_neighbors = directed_adj[u];
            if (std::binary_search(u_neighbors.begin(), u_neighbors.end(), w)) {
              local_count[v]++;
              local_count[u]++;
              local_count[w]++;
            }
          } else {
            const auto& w_neighbors = directed_adj[w];
            if (std::binary_search(w_neighbors.begin(), w_neighbors.end(), u)) {
              local_count[v]++;
              local_count[u]++;
              local_count[w]++;
            }
          }
        }
      }
    }
    
    // Merge local counts to global (sequential, but fast)
    for (size_t i = 0; i < num_vertices; ++i) {
      if (local_count[i] > 0) {
        __sync_fetch_and_add(&triangle_count[i], local_count[i]);
      }
    }
  };

  if (num_threads > 1 && num_vertices > 1000) {
    std::vector<std::thread> threads;
    size_t chunk_size = (num_vertices + num_threads - 1) / num_threads;
    
    for (int t = 0; t < num_threads; ++t) {
      size_t start = t * chunk_size;
      size_t end = std::min(start + chunk_size, num_vertices);
      if (start < end) {
        threads.emplace_back(count_triangles, t, start, end);
      }
    }
    
    for (auto& t : threads) {
      t.join();
    }
  } else {
    count_triangles(0, 0, num_vertices);
  }

  LOG(INFO) << "LCC: Counted triangles";

  // Compute LCC for each vertex
  std::vector<double> lcc_values(num_vertices, 0.0);

  for (size_t v = 0; v < num_vertices; ++v) {
    size_t k = degree[v];

    if (k <= 1) {
      lcc_values[v] = 0.0;
      continue;
    }

    double possible_edges = static_cast<double>(k * (k - 1)) / 2.0;
    lcc_values[v] = static_cast<double>(triangle_count[v]) / possible_edges;
  }

  LOG(INFO) << "LCC: Computed clustering coefficients";

  // Build result columns
  neug::execution::ValueColumnBuilder<int64_t> node_builder;
  neug::execution::ValueColumnBuilder<double> coefficient_builder;

  for (size_t i = 0; i < num_vertices; ++i) {
    node_builder.push_back_opt(external_ids[i]);
    coefficient_builder.push_back_opt(lcc_values[i]);
  }

  ctx.set(0, node_builder.finish());
  ctx.set(1, coefficient_builder.finish());
  ctx.tag_ids = {0, 1};

  return ctx;
}

function_set LCCFunction::getFunctionSet() {
  function_set functionSet;

  // Overload 1: CALL lcc() - use full graph with defaults
  {
    auto function = std::make_unique<NeugCallFunction>(
        LCCFunction::name,
        std::vector<neug::common::LogicalTypeID>{},
        std::vector<std::pair<std::string, neug::common::LogicalTypeID>>{
            {"node", common::LogicalTypeID::INT64},
            {"coefficient", common::LogicalTypeID::DOUBLE}
        });

    function->bindFunc = [](const neug::Schema& schema,
        const neug::execution::ContextMeta& ctx_meta,
        const ::physical::PhysicalPlan& plan,
        int op_idx) -> std::unique_ptr<CallFuncInputBase> {
      auto input = std::make_unique<LCCFuncInput>();
      input->graphName = "";
      input->concurrency = 0;
      return input;
    };

    function->execFunc = [](const CallFuncInputBase& input, neug::IStorageInterface& graph) {
      try {
        const LCCFuncInput& lcc_input = static_cast<const LCCFuncInput&>(input);
        const ProjectedSubgraph* subgraph = getSubgraph(lcc_input.graphName);
        int concurrency = lcc_input.concurrency > 0 ?
            lcc_input.concurrency : std::thread::hardware_concurrency();
        return executeLCC(concurrency, subgraph, graph);
      } catch (const std::exception& e) {
        LOG(ERROR) << "LCC failed: " << e.what();
        THROW_EXTENSION_EXCEPTION("LCC failed: " + std::string(e.what()));
      }
    };

    functionSet.push_back(std::move(function));
  }

  // Overload 2: CALL lcc(graph_name, {concurrency: N})
  {
    auto function = std::make_unique<NeugCallFunction>(
        LCCFunction::name,
        std::vector<neug::common::LogicalTypeID>{
            common::LogicalTypeID::STRING,
            common::LogicalTypeID::MAP
        },
        std::vector<std::pair<std::string, neug::common::LogicalTypeID>>{
            {"node", common::LogicalTypeID::INT64},
            {"coefficient", common::LogicalTypeID::DOUBLE}
        });

    function->bindFunc = [](const neug::Schema& schema,
        const neug::execution::ContextMeta& ctx_meta,
        const ::physical::PhysicalPlan& plan,
        int op_idx) -> std::unique_ptr<CallFuncInputBase> {
      auto input = std::make_unique<LCCFuncInput>();
      auto procedurePB = plan.plan(op_idx).opr().procedure_call();
      GDSParams params = parseGDSParams(procedurePB.query());
      input->graphName = params.graphName;
      input->concurrency = params.concurrency;
      return input;
    };

    function->execFunc = [](const CallFuncInputBase& input, neug::IStorageInterface& graph) {
      try {
        const LCCFuncInput& lcc_input = static_cast<const LCCFuncInput&>(input);
        const ProjectedSubgraph* subgraph = getSubgraph(lcc_input.graphName);
        int concurrency = lcc_input.concurrency > 0 ?
            lcc_input.concurrency : std::thread::hardware_concurrency();
        return executeLCC(concurrency, subgraph, graph);
      } catch (const std::exception& e) {
        LOG(ERROR) << "LCC failed: " << e.what();
        THROW_EXTENSION_EXCEPTION("LCC failed: " + std::string(e.what()));
      }
    };

    functionSet.push_back(std::move(function));
  }

  return functionSet;
}

}  // namespace function
}  // namespace neug