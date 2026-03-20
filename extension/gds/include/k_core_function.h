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
 * @brief Input for K-Core function.
 * 
 * K-Core decomposition computes the core number for each vertex.
 * Graph semantics: Undirected - uses out_neighbors() ∪ in_neighbors().
 */
struct KCoreFuncInput : public CallFuncInputBase {
  std::string graphName;           // Name of the projected subgraph
  int64_t minK = 1;                // Minimum core number to return
  int64_t concurrency = 0;         // Concurrency (0 = auto)
  ProjectedSubgraph subgraph;
};

/**
 * @brief K-Core Decomposition function definition.
 * 
 * A k-core is a subgraph where each vertex has at least k neighbors.
 * K-Core decomposition computes the maximum k value (core number) for each vertex.
 * 
 * Output: (node, core_number)
 */
struct KCoreFunction {
  static constexpr const char* name = "k_core";

  static function_set getFunctionSet();
};

}  // namespace function
}  // namespace neug