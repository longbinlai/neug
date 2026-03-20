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
 * @brief Input for PageRank function.
 * 
 * PageRank computes the importance score for each node in a directed graph.
 * Graph semantics: Directed - only uses out_neighbors() for rank propagation.
 */
struct PageRankFuncInput : public CallFuncInputBase {
  std::string graphName;           // Name of the projected subgraph
  double dampingFactor = 0.85;    // Default damping factor (d)
  int maxIterations = 20;         // Maximum iterations
  double tolerance = 1e-6;        // Convergence tolerance
  int concurrency = 0;            // Number of threads (0 = auto)
  ProjectedSubgraph subgraph;
};

/**
 * @brief PageRank algorithm function definition.
 * 
 * PageRank is an iterative algorithm that assigns a ranking value (importance
 * score) to each vertex. The edge direction represents a "vote" relationship:
 * A→B means A passes part of its rank to B.
 * 
 * Formula:
 * PR_i(v) = (1-d)/|V| + d * (sum of PR(u)/out_degree(u) for u in in_neighbors(v))
 *         + d * (sum of PR(w)/|V| for w in sinks)
 * 
 * Where d is the damping factor, and sinks are vertices with no outgoing edges.
 */
struct PageRankFunction {
  static constexpr const char* name = "page_rank";

  static function_set getFunctionSet();
};

}  // namespace function
}  // namespace neug