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

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include "neug/compiler/function/neug_call_function.h"
#include "neug/execution/common/context.h"

namespace neug {
namespace function {

/**
 * @brief Vertex entry for projected subgraph.
 * Contains label name and optional predicate expression.
 */
struct VertexEntry {
  std::string label;
  std::string predicate;  // Optional filter predicate, e.g., "n.age > 20"
};

/**
 * @brief Edge entry for projected subgraph.
 * Contains source label, edge label, destination label and optional predicate.
 */
struct EdgeEntry {
  std::string srcLabel;
  std::string edgeLabel;
  std::string dstLabel;
  std::string predicate;  // Optional filter predicate on edge properties
};

/**
 * @brief Projected subgraph metadata.
 * Stores the definition of a named subgraph for use by GDS algorithms.
 */
struct ProjectedSubgraph {
  std::string graphName;
  std::vector<VertexEntry> vertexEntries;
  std::vector<EdgeEntry> edgeEntries;
};

/**
 * @brief Thread-local registry for projected subgraphs.
 * 
 * This provides a connection-scoped storage for subgraph metadata.
 * Each thread (connection) has its own registry, ensuring isolation
 * between different connections.
 * 
 * Note: This is a simplified implementation for the demo. A production
 * implementation would integrate directly with ClientContext::graphEntrySet.
 */
class ProjectedSubgraphRegistry {
 public:
  static ProjectedSubgraphRegistry& instance() {
    static thread_local ProjectedSubgraphRegistry registry;
    return registry;
  }

  void registerSubgraph(const std::string& name, const ProjectedSubgraph& subgraph) {
    std::lock_guard<std::mutex> lock(mutex_);
    subgraphs_[name] = subgraph;
  }

  const ProjectedSubgraph* getSubgraph(const std::string& name) const {
    auto it = subgraphs_.find(name);
    return it != subgraphs_.end() ? &it->second : nullptr;
  }

  bool hasSubgraph(const std::string& name) const {
    return subgraphs_.find(name) != subgraphs_.end();
  }

  void dropSubgraph(const std::string& name) {
    subgraphs_.erase(name);
  }

  void clear() { subgraphs_.clear(); }

  std::vector<std::string> listSubgraphs() const {
    std::vector<std::string> names;
    for (const auto& [name, _] : subgraphs_) {
      names.push_back(name);
    }
    return names;
  }

 private:
  ProjectedSubgraphRegistry() = default;
  mutable std::mutex mutex_;
  std::unordered_map<std::string, ProjectedSubgraph> subgraphs_;
};

/**
 * @brief Input for project_graph function.
 */
struct ProjectGraphFuncInput : public CallFuncInputBase {
  std::string graphName;
  ProjectedSubgraph subgraph;
};

/**
 * @brief project_graph function definition.
 * Projects a named subgraph for use by GDS algorithms.
 */
struct ProjectGraphFunction {
  static constexpr const char* name = "project_graph";

  static function_set getFunctionSet();
};

}  // namespace function
}  // namespace neug