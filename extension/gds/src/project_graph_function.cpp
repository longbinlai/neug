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

#include "project_graph_function.h"
#include <glog/logging.h>
#include "neug/execution/common/columns/value_columns.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace function {

function_set ProjectGraphFunction::getFunctionSet() {
  // project_graph(graph_name, node_entries, edge_entries)
  // node_entries: MAP<STRING, STRING> - label -> predicate
  // edge_entries: MAP<STRING, STRING> - edge_label -> predicate
  auto function = std::make_unique<NeugCallFunction>(
      ProjectGraphFunction::name,
      std::vector<neug::common::LogicalTypeID>{
          common::LogicalTypeID::STRING,   // graph_name
          common::LogicalTypeID::MAP,      // node_entries (optional)
          common::LogicalTypeID::MAP       // edge_entries (optional)
      },
      std::vector<std::pair<std::string, neug::common::LogicalTypeID>>{
          {"status", common::LogicalTypeID::STRING},
          {"graph_name", common::LogicalTypeID::STRING}
      });

  function->bindFunc = [](const neug::Schema& schema,
      const neug::execution::ContextMeta& ctx_meta,
      const ::physical::PhysicalPlan& plan,
      int op_idx) -> std::unique_ptr<CallFuncInputBase> {
    auto input = std::make_unique<ProjectGraphFuncInput>();
    
    // Parse the physical plan to extract graph name and entries
    const auto& opr = plan.plan(op_idx);
    // The actual parsing would require access to the expression evaluator
    // For now, we'll set a default graph name
    input->graphName = "projected_graph";
    
    return input;
  };

  function->execFunc = [](const CallFuncInputBase& input, neug::IStorageInterface& graph) {
    try {
      const ProjectGraphFuncInput& pg_input = 
          static_cast<const ProjectGraphFuncInput&>(input);
      
      neug::execution::Context ctx;

      // Register the subgraph in the thread-local registry
      ProjectedSubgraphRegistry::instance().registerSubgraph(
          pg_input.graphName, pg_input.subgraph);

      // Return status
      neug::execution::ValueColumnBuilder<std::string> status_builder;
      neug::execution::ValueColumnBuilder<std::string> name_builder;

      status_builder.push_back_opt("OK");
      name_builder.push_back_opt(pg_input.graphName);

      ctx.set(0, status_builder.finish());
      ctx.set(1, name_builder.finish());
      ctx.tag_ids = {0, 1};

      return ctx;
    } catch (const std::exception& e) {
      LOG(ERROR) << "project_graph failed: " << e.what();
      THROW_EXTENSION_EXCEPTION("project_graph failed: " + std::string(e.what()));
    }
  };

  function_set functionSet;
  functionSet.push_back(std::move(function));
  return functionSet;
}

}  // namespace function
}  // namespace neug