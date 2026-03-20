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

#include <gtest/gtest.h>
#include <memory>
#include <vector>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/context.h"

#include "project_graph_function.h"
#include "wcc_function.h"

namespace neug {
namespace test {

class GDSTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // No setup needed for basic function tests
  }
};

TEST_F(GDSTest, TestProjectGraphFunctionSet) {
  auto functionSet = neug::function::ProjectGraphFunction::getFunctionSet();
  EXPECT_EQ(functionSet.size(), 1);
  EXPECT_EQ(functionSet[0]->name, "project_graph");
}

TEST_F(GDSTest, TestWCCFunctionSet) {
  auto functionSet = neug::function::WCCFunction::getFunctionSet();
  EXPECT_EQ(functionSet.size(), 1);
  EXPECT_EQ(functionSet[0]->name, "wcc");
}

TEST_F(GDSTest, TestConnectedComponentsFunctionSet) {
  auto functionSet = neug::function::ConnectedComponentsFunction::getFunctionSet();
  EXPECT_EQ(functionSet.size(), 1);
  EXPECT_EQ(functionSet[0]->name, "connected_components");
}

TEST_F(GDSTest, TestWCCFunctionOutputColumns) {
  auto functionSet = neug::function::WCCFunction::getFunctionSet();
  auto* func = dynamic_cast<neug::function::NeugCallFunction*>(functionSet[0].get());
  ASSERT_NE(func, nullptr);
  EXPECT_EQ(func->outputColumns.size(), 2);
  EXPECT_EQ(func->outputColumns[0].first, "node");
  EXPECT_EQ(func->outputColumns[1].first, "component_id");
}

TEST_F(GDSTest, TestProjectGraphFunctionInputTypes) {
  auto functionSet = neug::function::ProjectGraphFunction::getFunctionSet();
  auto* func = dynamic_cast<neug::function::NeugCallFunction*>(functionSet[0].get());
  ASSERT_NE(func, nullptr);
  EXPECT_EQ(func->parameterTypeIDs.size(), 3);
}

}  // namespace test
}  // namespace neug