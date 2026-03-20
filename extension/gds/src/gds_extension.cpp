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
#include "wcc_function.h"
#include "page_rank_function.h"
#include "bfs_function.h"
#include "k_core_function.h"
#include "label_propagation_function.h"
#include "lcc_function.h"
#include "sssp_function.h"
#include "leiden_function.h"
#include "neug/compiler/extension/extension_api.h"
#include "neug/utils/exception/exception.h"

extern "C" {

void Init() {
  try {
    // Register project_graph function
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::ProjectGraphFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);

    // Register WCC function
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::WCCFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);

    // Register connected_components alias
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::ConnectedComponentsFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);

    // Register PageRank function
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::PageRankFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);

    // Register BFS function
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::BFSFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);

    // Register K-Core function
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::KCoreFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);

    // Register Label Propagation function
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::LabelPropagationFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);

    // Register LCC function
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::LCCFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);

    // Register SSSP function
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::SSSPFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);

    // Register Leiden function
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::LeidenFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);

    // Register extension info
    neug::extension::ExtensionAPI::registerExtension(
        neug::extension::ExtensionInfo{
            "gds", "Graph Data Science algorithms (WCC, PageRank, BFS, K-Core, LP, LCC, SSSP, Leiden)"});
  } catch (const std::exception& e) {
    THROW_EXCEPTION_WITH_FILE_LINE("[gds extension] registration failed: " +
                                   std::string(e.what()));
  } catch (...) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "[gds extension] registration failed: unknown exception");
  }
}

const char* Name() { return "GDS"; }

}  // extern "C"