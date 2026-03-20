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

#include "wcc_function.h"
#include <glog/logging.h>
#include <algorithm>
#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include "gds_param_parser.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace function {

// Hash function for std::pair
template<typename T1, typename T2>
struct pair_hash {
  size_t operator()(const std::pair<T1, T2>& p) const {
    return std::hash<T1>()(p.first) ^ (std::hash<T2>()(p.second) << 1);
  }
};

/**
 * @brief Thread-safe Union-Find data structure using atomic operations.
 */
class AtomicUnionFind {
 public:
  explicit AtomicUnionFind(size_t n) : parent_(n), rank_(n) {
    for (size_t i = 0; i < n; ++i) {
      parent_[i].store(i, std::memory_order_relaxed);
      rank_[i].store(0, std::memory_order_relaxed);
    }
  }

  // Find with path compression (not fully parallel, but works well in practice)
  size_t find(size_t x) {
    size_t parent = parent_[x].load(std::memory_order_relaxed);
    if (parent == x) {
      return x;
    }
    // Path compression
    size_t root = find(parent);
    if (parent != root) {
      parent_[x].compare_exchange_weak(parent, root, std::memory_order_relaxed);
    }
    return root;
  }

  // Union by rank with CAS
  bool unite(size_t x, size_t y) {
    while (true) {
      size_t px = find(x);
      size_t py = find(y);
      
      if (px == py) {
        return false;  // Already in the same set
      }
      
      // Always attach smaller tree to larger tree
      size_t rank_px = rank_[px].load(std::memory_order_relaxed);
      size_t rank_py = rank_[py].load(std::memory_order_relaxed);
      
      size_t smaller, larger;
      if (rank_px < rank_py || (rank_px == rank_py && px < py)) {
        smaller = px;
        larger = py;
      } else {
        smaller = py;
        larger = px;
      }
      
      // Try to attach smaller to larger
      size_t expected = smaller;
      if (parent_[smaller].compare_exchange_weak(expected, larger, std::memory_order_relaxed)) {
        // Update rank if they were equal
        if (rank_px == rank_py) {
          size_t old_rank = rank_[larger].load(std::memory_order_relaxed);
          while (!rank_[larger].compare_exchange_weak(old_rank, old_rank + 1, std::memory_order_relaxed)) {
            // Retry
          }
        }
        return true;
      }
      // CAS failed, retry the whole operation
    }
  }

 private:
  std::vector<std::atomic<size_t>> parent_;
  std::vector<std::atomic<size_t>> rank_;
};

/**
 * @brief Execute WCC algorithm on the graph with optional parallelism.
 */
static execution::Context executeWCC(
    const std::string& graphName,
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
    LOG(WARNING) << "WCC: No vertices found in the graph";
    return ctx;
  }

  LOG(INFO) << "WCC: Processing " << vertex_labels.size() << " vertex labels";

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
    LOG(WARNING) << "WCC: No vertices found after filtering";
    return ctx;
  }

  LOG(INFO) << "WCC: Found " << num_vertices << " vertices";

  // Create atomic Union-Find structure
  AtomicUnionFind uf(num_vertices);

  // Create vertex ID to index mapping
  std::unordered_map<std::pair<label_t, vid_t>, size_t, pair_hash<label_t, vid_t>> vertex_to_idx;
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

  LOG(INFO) << "WCC: Processing " << edge_triplets.size() << " edge types";

  // Determine number of threads
  int num_threads = concurrency > 0 ? concurrency : std::thread::hardware_concurrency();
  if (num_threads < 1) num_threads = 1;
  
  LOG(INFO) << "WCC: Using " << num_threads << " threads";

  // Process edges in parallel
  for (const auto& [src_label, edge_label, dst_label] : edge_triplets) {
    auto out_view = reader->GetGenericOutgoingGraphView(src_label, dst_label, edge_label);
    
    const auto& src_vertices = vertices_by_label[src_label];
    size_t num_src_vertices = src_vertices.size();
    
    // Parallel edge processing
    auto process_vertices = [&](size_t start, size_t end) {
      for (size_t i = start; i < end; ++i) {
        vid_t src = src_vertices[i];
        auto src_idx_it = vertex_to_idx.find({src_label, src});
        if (src_idx_it == vertex_to_idx.end()) continue;
        
        size_t src_idx = src_idx_it->second;
        auto edges = out_view.get_edges(src);
        
        for (auto it = edges.begin(); it != edges.end(); ++it) {
          vid_t dst = *it;
          auto dst_idx_it = vertex_to_idx.find({dst_label, dst});
          if (dst_idx_it != vertex_to_idx.end()) {
            uf.unite(src_idx, dst_idx_it->second);
          }
        }
      }
    };

    // Process incoming edges for undirected semantics
    auto process_incoming = [&](size_t start, size_t end) {
      if (src_label == dst_label) return;  // Skip if same label
      
      auto in_view = reader->GetGenericIncomingGraphView(dst_label, src_label, edge_label);
      const auto& dst_vertices = vertices_by_label[dst_label];
      
      for (size_t i = start; i < end; ++i) {
        vid_t dst = dst_vertices[i];
        auto dst_idx_it = vertex_to_idx.find({dst_label, dst});
        if (dst_idx_it == vertex_to_idx.end()) continue;
        
        size_t dst_idx = dst_idx_it->second;
        auto edges = in_view.get_edges(dst);
        
        for (auto it = edges.begin(); it != edges.end(); ++it) {
          vid_t src = *it;
          auto src_idx_it = vertex_to_idx.find({src_label, src});
          if (src_idx_it != vertex_to_idx.end()) {
            uf.unite(src_idx_it->second, dst_idx);
          }
        }
      }
    };

    if (num_threads > 1 && num_src_vertices > 1000) {
      // Parallel processing
      std::vector<std::thread> threads;
      size_t chunk_size = (num_src_vertices + num_threads - 1) / num_threads;
      
      for (int t = 0; t < num_threads; ++t) {
        size_t start = t * chunk_size;
        size_t end = std::min(start + chunk_size, num_src_vertices);
        if (start < end) {
          threads.emplace_back(process_vertices, start, end);
        }
      }
      
      for (auto& t : threads) {
        t.join();
      }
      
      // Process incoming edges in parallel
      if (src_label != dst_label) {
        const auto& dst_vertices = vertices_by_label[dst_label];
        size_t num_dst_vertices = dst_vertices.size();
        chunk_size = (num_dst_vertices + num_threads - 1) / num_threads;
        
        threads.clear();
        for (int t = 0; t < num_threads; ++t) {
          size_t start = t * chunk_size;
          size_t end = std::min(start + chunk_size, num_dst_vertices);
          if (start < end) {
            threads.emplace_back(process_incoming, start, end);
          }
        }
        
        for (auto& t : threads) {
          t.join();
        }
      }
    } else {
      // Sequential processing
      process_vertices(0, num_src_vertices);
      
      if (src_label != dst_label) {
        const auto& dst_vertices = vertices_by_label[dst_label];
        process_incoming(0, dst_vertices.size());
      }
    }
  }

  LOG(INFO) << "WCC: Finished processing edges";

  // Build result columns (sequential, as it's fast)
  neug::execution::ValueColumnBuilder<int64_t> node_builder;
  neug::execution::ValueColumnBuilder<int64_t> component_builder;

  node_builder.reserve(num_vertices);
  component_builder.reserve(num_vertices);

  // Assign component IDs
  std::unordered_map<size_t, int64_t> component_id_map;
  int64_t next_component_id = 0;

  for (size_t i = 0; i < num_vertices; ++i) {
    size_t root = uf.find(i);

    auto it = component_id_map.find(root);
    int64_t component_id;
    if (it == component_id_map.end()) {
      component_id = next_component_id++;
      component_id_map[root] = component_id;
    } else {
      component_id = it->second;
    }

    Property external_id = reader->GetVertexId(all_vertices[i].first, all_vertices[i].second);
    node_builder.push_back_opt(external_id.as_int64());
    component_builder.push_back_opt(component_id);
  }

  LOG(INFO) << "WCC: Found " << next_component_id << " connected components";

  ctx.set(0, node_builder.finish());
  ctx.set(1, component_builder.finish());
  ctx.tag_ids = {0, 1};

  return ctx;
}

function_set WCCFunction::getFunctionSet() {
  function_set functionSet;

  // Overload 1: CALL wcc() - use full graph with defaults
  {
    auto function = std::make_unique<NeugCallFunction>(
        WCCFunction::name,
        std::vector<neug::common::LogicalTypeID>{},  // No parameters
        std::vector<std::pair<std::string, neug::common::LogicalTypeID>>{
            {"node", common::LogicalTypeID::INT64},
            {"component_id", common::LogicalTypeID::INT64}
        });

    function->bindFunc = [](const neug::Schema& schema,
        const neug::execution::ContextMeta& ctx_meta,
        const ::physical::PhysicalPlan& plan,
        int op_idx) -> std::unique_ptr<CallFuncInputBase> {
      auto input = std::make_unique<WCCFuncInput>();
      input->graphName = "";
      input->concurrency = 0;  // Auto-detect
      return input;
    };

    function->execFunc = [](const CallFuncInputBase& input, neug::IStorageInterface& graph) {
      try {
        const WCCFuncInput& wcc_input = static_cast<const WCCFuncInput&>(input);
        const ProjectedSubgraph* subgraph = getSubgraph(wcc_input.graphName);
        int concurrency = wcc_input.concurrency > 0 ?
            wcc_input.concurrency : std::thread::hardware_concurrency();
        return executeWCC(wcc_input.graphName, concurrency, subgraph, graph);
      } catch (const std::exception& e) {
        LOG(ERROR) << "WCC failed: " << e.what();
        THROW_EXTENSION_EXCEPTION("WCC failed: " + std::string(e.what()));
      }
    };

    functionSet.push_back(std::move(function));
  }

  // Overload 2: CALL wcc(graph_name, {concurrency: N}) - use projected subgraph with options
  {
    auto function = std::make_unique<NeugCallFunction>(
        WCCFunction::name,
        std::vector<neug::common::LogicalTypeID>{
            common::LogicalTypeID::STRING,   // graph_name
            common::LogicalTypeID::MAP       // options
        },
        std::vector<std::pair<std::string, neug::common::LogicalTypeID>>{
            {"node", common::LogicalTypeID::INT64},
            {"component_id", common::LogicalTypeID::INT64}
        });

    function->bindFunc = [](const neug::Schema& schema,
        const neug::execution::ContextMeta& ctx_meta,
        const ::physical::PhysicalPlan& plan,
        int op_idx) -> std::unique_ptr<CallFuncInputBase> {
      auto input = std::make_unique<WCCFuncInput>();
      auto procedurePB = plan.plan(op_idx).opr().procedure_call();
      GDSParams params = parseGDSParams(procedurePB.query());
      input->graphName = params.graphName;
      input->concurrency = params.concurrency;
      return input;
    };

    function->execFunc = [](const CallFuncInputBase& input, neug::IStorageInterface& graph) {
      try {
        const WCCFuncInput& wcc_input = static_cast<const WCCFuncInput&>(input);
        const ProjectedSubgraph* subgraph = getSubgraph(wcc_input.graphName);
        int concurrency = wcc_input.concurrency > 0 ?
            wcc_input.concurrency : std::thread::hardware_concurrency();
        return executeWCC(wcc_input.graphName, concurrency, subgraph, graph);
      } catch (const std::exception& e) {
        LOG(ERROR) << "WCC failed: " << e.what();
        THROW_EXTENSION_EXCEPTION("WCC failed: " + std::string(e.what()));
      }
    };

    functionSet.push_back(std::move(function));
  }

  return functionSet;
}

function_set ConnectedComponentsFunction::getFunctionSet() {
  function_set functionSet;

  // Overload 1: CALL connected_components() - use full graph with defaults
  {
    auto function = std::make_unique<NeugCallFunction>(
        ConnectedComponentsFunction::name,
        std::vector<neug::common::LogicalTypeID>{},  // No parameters
        std::vector<std::pair<std::string, neug::common::LogicalTypeID>>{
            {"node", common::LogicalTypeID::INT64},
            {"component_id", common::LogicalTypeID::INT64}
        });

    function->bindFunc = [](const neug::Schema& schema,
        const neug::execution::ContextMeta& ctx_meta,
        const ::physical::PhysicalPlan& plan,
        int op_idx) -> std::unique_ptr<CallFuncInputBase> {
      auto input = std::make_unique<WCCFuncInput>();
      input->graphName = "";
      input->concurrency = 0;
      return input;
    };

    function->execFunc = [](const CallFuncInputBase& input, neug::IStorageInterface& graph) {
      try {
        const WCCFuncInput& wcc_input = static_cast<const WCCFuncInput&>(input);
        const ProjectedSubgraph* subgraph = getSubgraph(wcc_input.graphName);
        int concurrency = wcc_input.concurrency > 0 ?
            wcc_input.concurrency : std::thread::hardware_concurrency();
        return executeWCC(wcc_input.graphName, concurrency, subgraph, graph);
      } catch (const std::exception& e) {
        LOG(ERROR) << "Connected Components failed: " << e.what();
        THROW_EXTENSION_EXCEPTION("Connected Components failed: " + std::string(e.what()));
      }
    };

    functionSet.push_back(std::move(function));
  }

  // Overload 2: CALL connected_components(graph_name, {concurrency: N})
  {
    auto function = std::make_unique<NeugCallFunction>(
        ConnectedComponentsFunction::name,
        std::vector<neug::common::LogicalTypeID>{
            common::LogicalTypeID::STRING,
            common::LogicalTypeID::MAP
        },
        std::vector<std::pair<std::string, neug::common::LogicalTypeID>>{
            {"node", common::LogicalTypeID::INT64},
            {"component_id", common::LogicalTypeID::INT64}
        });

    function->bindFunc = [](const neug::Schema& schema,
        const neug::execution::ContextMeta& ctx_meta,
        const ::physical::PhysicalPlan& plan,
        int op_idx) -> std::unique_ptr<CallFuncInputBase> {
      auto input = std::make_unique<WCCFuncInput>();
      auto procedurePB = plan.plan(op_idx).opr().procedure_call();
      GDSParams params = parseGDSParams(procedurePB.query());
      input->graphName = params.graphName;
      input->concurrency = params.concurrency;
      return input;
    };

    function->execFunc = [](const CallFuncInputBase& input, neug::IStorageInterface& graph) {
      try {
        const WCCFuncInput& wcc_input = static_cast<const WCCFuncInput&>(input);
        const ProjectedSubgraph* subgraph = getSubgraph(wcc_input.graphName);
        int concurrency = wcc_input.concurrency > 0 ?
            wcc_input.concurrency : std::thread::hardware_concurrency();
        return executeWCC(wcc_input.graphName, concurrency, subgraph, graph);
      } catch (const std::exception& e) {
        LOG(ERROR) << "Connected Components failed: " << e.what();
        THROW_EXTENSION_EXCEPTION("Connected Components failed: " + std::string(e.what()));
      }
    };

    functionSet.push_back(std::move(function));
  }

  return functionSet;
}

}  // namespace function
}  // namespace neug