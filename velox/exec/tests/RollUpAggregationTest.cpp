/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

#include <iostream>

namespace facebook::velox::exec::test {

using core::QueryConfig;
using namespace common::testutil;

class RollUpAggregationTest : public OperatorTestBase {
 protected:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
    TestValue::enable();
  }

  void SetUp() override {
    OperatorTestBase::SetUp();
    filesystems::registerLocalFileSystem();
  }

  RowTypePtr rowType_;
  folly::Random::DefaultGenerator rng_;
};

TEST_F(RollUpAggregationTest, rollUpAggregationInvalid) {
  auto a = makeFlatVector<int64_t>({1, 1, 2, 1});
  auto b = makeFlatVector<int64_t>({1, 2, 1, 1});
  auto c = makeFlatVector<int64_t>({2, 3, 4, 5});

  auto vectors = makeRowVector({"a", "b", "c"}, {a, b, c});

  createDuckDbTable({vectors});

  std::vector<std::string> aggregates = {"sum(c)", "min(c)", "max(c)"};

  // Throws error as no project below RollUp
  EXPECT_THROW(
      {
        auto op = PlanBuilder()
                      .values({vectors})
                      .rollupAggregation({"a", "b"}, aggregates, false, {})
                      .planNode();
      },
      VeloxRuntimeError);

  // Throws error as the project doesn't have constant last projection
  EXPECT_THROW(
      {
        auto op = PlanBuilder()
                      .values({vectors})
                      .project({"a", "b", "c"})
                      .rollupAggregation({"a", "b"}, aggregates, false, {})
                      .planNode();
      },
      VeloxRuntimeError);

  // Throws error as the constant projection is not 0
  EXPECT_THROW(
      {
        auto op =
            PlanBuilder()
                .values({vectors})
                .project({"a", "b", "c", "1 as id"})
                .rollupAggregation({"a", "b", "id"}, aggregates, false, {})
                .planNode();
      },
      VeloxRuntimeError);
}

TEST_F(RollUpAggregationTest, rollUpAggregation) {
  auto a = makeFlatVector<int64_t>({1, 1, 2, 1});
  auto b = makeFlatVector<int64_t>({1, 2, 1, 1});
  auto c = makeFlatVector<int64_t>({2, 3, 4, 5});

  auto vectors = makeRowVector({"a", "b", "c"}, {a, b, c});

  createDuckDbTable({vectors});

  std::vector<std::string> aggregates = {"sum(c)", "min(c)", "max(c)"};
  auto op = PlanBuilder()
                .values({vectors})
                .project({"a", "b", "c", "0 as id"})
                .rollupAggregation({"a", "b", "id"}, aggregates, false, {})
                .project({"a", "b", "a0", "a1", "a2"})
                .planNode();

  assertQuery(
      op, "select a, b, sum(c), min(c), max(c) from tmp group by rollup(a, b)");
}

} // namespace facebook::velox::exec::test
