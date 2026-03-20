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
 * @brief Input for SSSP function.
 * 
 * Single-source shortest paths computes the shortest distance from a source node.
 * Graph semantics: Directed - only uses out_neighbors().
 */
struct SSSPFuncInput : public CallFuncInputBase {
  std::string graphName;           // Name of the projected subgraph
  int64_t sourceNode = 0;          // Source node identifier (required)
  int64_t targetNode = -1;         // Target node (-1 = all nodes)
  std::string weightProperty;      // Edge weight property (empty = unweighted)
  ProjectedSubgraph subgraph;
};

/**
 * @brief SSSP function definition.
 * 
 * Computes shortest paths from a source node to all other nodes (or a specific target).
 * Uses Dijkstra's algorithm for weighted graphs, BFS for unweighted graphs.
 * 
 * Output: (node, distance)
 */
struct SSSPFunction {
  static constexpr const char* name = "shortest_path";

  static function_set getFunctionSet();
};

}  // namespace function
}  // namespace neug