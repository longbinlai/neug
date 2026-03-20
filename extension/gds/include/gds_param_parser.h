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

#include <string>
#include <thread>
#include <unordered_map>
#include "neug/generated/proto/plan/physical.pb.h"
#include "project_graph_function.h"

namespace neug {
namespace function {

/**
 * @brief GDS algorithm parameters parsed from ProcedureCall.
 *
 * According to the GDS spec, the function signature should be:
 *   CALL algo_name(graph_name, {param1: value1, concurrency: 4, ...})
 *
 * - graph_name: Name of the projected subgraph (empty string "" means full graph)
 * - options: A map of algorithm-specific parameters plus common config
 */
struct GDSParams {
  std::string graphName;  // Empty string means use full graph
  int concurrency = 0;    // 0 means auto-detect

  // Algorithm-specific parameters
  int64_t intParam1 = 0;   // Generic int parameter (e.g., source for BFS, min_k for K-Core)
  int64_t intParam2 = 0;   // Second int parameter (e.g., max_depth for BFS)
  double doubleParam1 = 0.0;  // Generic double parameter (e.g., damping for PageRank)
  double doubleParam2 = 0.0;  // Second double parameter (e.g., tolerance)
  std::string stringParam1;   // Generic string parameter (e.g., weight_property)

  // Get effective concurrency (auto-detect if 0)
  int getConcurrency() const {
    return concurrency > 0 ? concurrency : std::thread::hardware_concurrency();
  }
};

/**
 * @brief Helper to extract int64 from Value.
 */
inline int64_t extractInt64(const ::common::Value& val) {
  if (val.has_i64()) return val.i64();
  if (val.has_i32()) return val.i32();
  if (val.has_u64()) return static_cast<int64_t>(val.u64());
  if (val.has_u32()) return static_cast<int64_t>(val.u32());
  return 0;
}

/**
 * @brief Helper to extract double from Value.
 */
inline double extractDouble(const ::common::Value& val) {
  if (val.has_f64()) return val.f64();
  if (val.has_f32()) return val.f32();
  return 0.0;
}

/**
 * @brief Helper to extract string from Value.
 */
inline std::string extractString(const ::common::Value& val) {
  if (val.has_str()) return val.str();
  return "";
}

/**
 * @brief Parse options from a PairArray (MAP type).
 */
inline void parseOptions(const ::common::PairArray& pairs, GDSParams& params) {
  for (const auto& pair : pairs.item()) {
    if (!pair.key().has_str()) continue;

    const std::string& key = pair.key().str();
    const ::common::Value& val = pair.val();

    if (key == "concurrency") {
      params.concurrency = extractInt64(val);
    } else if (key == "source") {
      params.intParam1 = extractInt64(val);
    } else if (key == "max_depth") {
      params.intParam2 = extractInt64(val);
    } else if (key == "min_k") {
      params.intParam1 = extractInt64(val);
    } else if (key == "max_iterations") {
      params.intParam2 = extractInt64(val);
    } else if (key == "damping" || key == "damping_factor") {
      params.doubleParam1 = extractDouble(val);
    } else if (key == "tolerance") {
      params.doubleParam2 = extractDouble(val);
    } else if (key == "resolution") {
      params.doubleParam1 = extractDouble(val);
    } else if (key == "weight_property") {
      params.stringParam1 = extractString(val);
    } else if (key == "target") {
      params.intParam2 = extractInt64(val);
    }
  }
}

/**
 * @brief Parse GDS parameters from ProcedureCall.
 *
 * Expected argument order:
 *   1. graph_name (STRING) - optional, empty means full graph
 *   2. options (MAP) - optional, contains algorithm-specific params and concurrency
 *
 * For backward compatibility, also supports:
 *   - Single INT64 argument (e.g., source vertex for BFS)
 *   - No arguments (use defaults)
 */
inline GDSParams parseGDSParams(
    const ::procedure::Query& query,
    const GDSParams& defaults = GDSParams{}) {

  GDSParams params = defaults;
  const auto& args = query.arguments();

  // Parse arguments based on position and type
  for (size_t i = 0; i < args.size(); ++i) {
    const auto& arg = args[i];

    if (!arg.has_const_()) {
      continue;
    }

    const auto& const_val = arg.const_();

    // First argument: could be graph_name (STRING) or intParam1 (INT64)
    if (i == 0) {
      if (const_val.has_str()) {
        params.graphName = const_val.str();
      } else if (const_val.has_i64()) {
        params.intParam1 = const_val.i64();
      } else if (const_val.has_i32()) {
        params.intParam1 = const_val.i32();
      } else if (const_val.has_pair_array()) {
        // Options map as first argument
        parseOptions(const_val.pair_array(), params);
      }
    }
    // Second argument: could be options (MAP) or intParam2
    else if (i == 1) {
      if (const_val.has_pair_array()) {
        parseOptions(const_val.pair_array(), params);
      } else if (const_val.has_i64()) {
        params.intParam2 = const_val.i64();
      } else if (const_val.has_i32()) {
        params.intParam2 = const_val.i32();
      }
    }
    // Third argument: typically options
    else if (i == 2) {
      if (const_val.has_pair_array()) {
        parseOptions(const_val.pair_array(), params);
      }
    }
  }

  return params;
}

/**
 * @brief Get the projected subgraph by name.
 *
 * @param graphName The name of the projected subgraph (empty means full graph)
 * @return Pointer to the subgraph, or nullptr if using full graph
 */
inline const ProjectedSubgraph* getSubgraph(const std::string& graphName) {
  if (graphName.empty()) {
    return nullptr;
  }
  const ProjectedSubgraph* subgraph =
      ProjectedSubgraphRegistry::instance().getSubgraph(graphName);
  if (!subgraph) {
    LOG(WARNING) << "Subgraph '" << graphName << "' not found, using full graph";
  }
  return subgraph;
}

}  // namespace function
}  // namespace neug