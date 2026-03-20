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
 * @brief Input for Leiden function.
 * 
 * Leiden is an improved version of Louvain for community detection.
 * Graph semantics: Undirected - uses out_neighbors() ∪ in_neighbors().
 */
struct LeidenFuncInput : public CallFuncInputBase {
  std::string graphName;           // Name of the projected subgraph
  double resolution = 1.0;         // Resolution parameter
  int64_t maxIterations = 10;      // Maximum iterations
  std::string weightProperty;      // Edge weight property (empty = unweighted)
  int64_t concurrency = 0;         // Concurrency (0 = auto)
  ProjectedSubgraph subgraph;
};

/**
 * @brief Leiden community detection function definition.
 * 
 * Leiden algorithm finds communities by optimizing modularity.
 * This is a simplified implementation based on Louvain-style optimization.
 * 
 * Output: (node, community_id)
 */
struct LeidenFunction {
  static constexpr const char* name = "leiden";

  static function_set getFunctionSet();
};

}  // namespace function
}  // namespace neug