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
 * @brief Input for WCC function.
 */
struct WCCFuncInput : public CallFuncInputBase {
  std::string graphName;
  int concurrency = 0;  // 0 means auto-detect
  ProjectedSubgraph subgraph;  // The projected subgraph to operate on
};

/**
 * @brief WCC (Weakly Connected Components) function definition.
 * Computes weakly connected components on a projected subgraph.
 */
struct WCCFunction {
  static constexpr const char* name = "wcc";

  static function_set getFunctionSet();
};

/**
 * @brief Connected Components alias for WCC.
 */
struct ConnectedComponentsFunction {
  static constexpr const char* name = "connected_components";

  static function_set getFunctionSet();
};

}  // namespace function
}  // namespace neug