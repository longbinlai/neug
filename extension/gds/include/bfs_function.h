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
#include <string>
#include <vector>
#include "neug/compiler/function/neug_call_function.h"
#include "neug/execution/common/context.h"
#include "project_graph_function.h"

namespace neug {
namespace function {

/**
 * @brief Input for BFS function.
 * 
 * BFS performs breadth-first traversal from a source node, expanding by layers.
 * Graph semantics: Directed - only uses out_neighbors() for traversal.
 */
struct BFSFuncInput : public CallFuncInputBase {
  std::string graphName;           // Name of the projected subgraph
  int64_t sourceNode = 0;          // Source node identifier (required)
  int64_t maxDepth = -1;           // Maximum traversal depth (-1 = unlimited)
  int concurrency = 0;             // Number of threads (0 = auto)
  ProjectedSubgraph subgraph;
};

/**
 * @brief BFS algorithm function definition.
 * 
 * Breadth-First Search traverses the graph from a source node, visiting nodes
 * level by level. Each level contains nodes at the same distance (number of edges)
 * from the source.
 * 
 * Output: (node, distance) where distance is the number of edges from source.
 */
struct BFSFunction {
  static constexpr const char* name = "bfs";

  static function_set getFunctionSet();
};

}  // namespace function
}  // namespace neug