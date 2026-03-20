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
 * @brief Input for Label Propagation function.
 * 
 * Label Propagation is a fast community detection algorithm.
 * Graph semantics: Undirected - uses out_neighbors() ∪ in_neighbors().
 */
struct LabelPropagationFuncInput : public CallFuncInputBase {
  std::string graphName;           // Name of the projected subgraph
  int64_t maxIterations = 10;      // Maximum iterations
  int64_t concurrency = 0;         // Concurrency (0 = auto)
  ProjectedSubgraph subgraph;
};

/**
 * @brief Label Propagation function definition.
 * 
 * Each node adopts the most frequent label among its neighbors.
 * If there are ties, the smallest label is selected.
 * 
 * Output: (node, label)
 */
struct LabelPropagationFunction {
  static constexpr const char* name = "label_propagation";

  static function_set getFunctionSet();
};

}  // namespace function
}  // namespace neug