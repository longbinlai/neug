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
 * @brief Input for LCC function.
 * 
 * Local Clustering Coefficient measures how connected a vertex's neighbors are.
 * Graph semantics: Undirected - uses out_neighbors() ∪ in_neighbors().
 */
struct LCCFuncInput : public CallFuncInputBase {
  std::string graphName;           // Name of the projected subgraph
  int64_t concurrency = 0;         // Concurrency (0 = auto)
  ProjectedSubgraph subgraph;
};

/**
 * @brief Local Clustering Coefficient function definition.
 * 
 * LCC(v) = |{(u,w) | u,w ∈ N(v) ∧ (u,w) ∈ E}| / |{(u,w) | u,w ∈ N(v)}|
 * 
 * If |N(v)| ≤ 1, LCC(v) = 0.
 * 
 * Output: (node, coefficient)
 */
struct LCCFunction {
  static constexpr const char* name = "lcc";

  static function_set getFunctionSet();
};

}  // namespace function
}  // namespace neug