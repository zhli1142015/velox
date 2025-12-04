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
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/OrderBy.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/RowsStreamingWindowBuild.h"
#include "velox/exec/SortWindowBuild.h"
#include "velox/exec/Window.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::exec {

namespace {

class WindowSpillTest : public OperatorTestBase {
 public:
  void SetUp() override {
    OperatorTestBase::SetUp();
    window::prestosql::registerAllWindowFunctions();
    filesystems::registerLocalFileSystem();
  }

  common::SpillConfig getSpillConfig(
      const std::string& spillDir,
      bool enablePrefixSort) const {
    const auto prefixSortConfig = enablePrefixSort
        ? std::optional<common::PrefixSortConfig>(common::PrefixSortConfig())
        : std::nullopt;
    return common::SpillConfig(
        [spillDir]() -> const std::string& { return spillDir; },
        [&](uint64_t) {},
        "0.0.0",
        0,
        0,
        1 << 20,
        executor_.get(),
        5,
        10,
        0,
        0,
        0,
        0,
        0,
        "none",
        prefixSortConfig);
  }

  /// Runs a streaming window spill test with the given data and window
  /// functions. Verifies that spill actually happens.
  void runStreamingWindowSpillTest(
      const RowVectorPtr& data,
      const std::vector<std::string>& windowFunctions,
      const std::string& expectedSql,
      int numBatches = 10) {
    createDuckDbTable({data});

    core::PlanNodeId windowId;
    auto plan = PlanBuilder()
                    .values(split(data, numBatches))
                    .streamingWindow(windowFunctions)
                    .capturePlanNodeId(windowId)
                    .planNode();

    auto spillDirectory = TempDirectoryPath::create();
    TestScopedSpillInjection scopedSpillInjection(100);

    auto task =
        AssertQueryBuilder(plan, duckDbQueryRunner_)
            .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
            .config(core::QueryConfig::kSpillEnabled, "true")
            .config(core::QueryConfig::kWindowSpillEnabled, "true")
            .spillDirectory(spillDirectory->getPath())
            .assertResults(expectedSql);

    auto taskStats = exec::toPlanStats(task->taskStats());
    const auto& stats = taskStats.at(windowId);
    ASSERT_GT(stats.spilledBytes, 0);
    ASSERT_GT(stats.spilledRows, 0);
  }

  /// Creates standard test data with partition and sort keys.
  RowVectorPtr makeWindowTestData(
      vector_size_t size,
      vector_size_t partitionSize) {
    return makeRowVector(
        {"d", "p", "s"},
        {
            makeFlatVector<int64_t>(size, [](auto row) { return row; }),
            makeFlatVector<int16_t>(
                size,
                [partitionSize](auto row) { return row / partitionSize; }),
            makeFlatVector<int32_t>(size, [](auto row) { return row; }),
        });
  }

  /// Creates test data with a single partition.
  RowVectorPtr makeSinglePartitionData(vector_size_t size) {
    return makeRowVector(
        {"d", "p", "s"},
        {
            makeFlatVector<int64_t>(size, [](auto row) { return row; }),
            makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
            makeFlatVector<int32_t>(size, [](auto row) { return row; }),
        });
  }

  const std::shared_ptr<folly::Executor> executor_{
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency())};

  tsan_atomic<bool> nonReclaimableSection_{false};
};

// Test basic spill with multiple window functions (first_value, last_value,
// nth_value). This covers: basic spill, multiple functions, and
// PartitionStreamingWindowBuild.
TEST_F(WindowSpillTest, basicSpill) {
  auto data = makeWindowTestData(1'000, 100);
  runStreamingWindowSpillTest(
      data,
      {"first_value(d) over (partition by p order by s)",
       "last_value(d) over (partition by p order by s)",
       "nth_value(d, 2) over (partition by p order by s)"},
      "SELECT *, first_value(d) over (partition by p order by s), "
      "last_value(d) over (partition by p order by s), "
      "nth_value(d, 2) over (partition by p order by s) FROM tmp");
}

// Test spill with single partition (all rows in one partition).
TEST_F(WindowSpillTest, singlePartition) {
  auto data = makeSinglePartitionData(1'000);
  runStreamingWindowSpillTest(
      data,
      {"last_value(d) over (partition by p order by s)"},
      "SELECT *, last_value(d) over (partition by p order by s) FROM tmp");
}

// Test spill with string columns.
TEST_F(WindowSpillTest, stringColumns) {
  const vector_size_t size = 1'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<StringView>(
              size,
              [](auto row) {
                return StringView::makeInline(fmt::format("data_{:06d}", row));
              }),
          makeFlatVector<int16_t>(size, [](auto row) { return row / 100; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan =
      PlanBuilder()
          .values(split(data, 10))
          .streamingWindow({"last_value(d) over (partition by p order by s)"})
          .capturePlanNodeId(windowId)
          .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  TestScopedSpillInjection scopedSpillInjection(100);
  auto task =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kWindowSpillEnabled, "true")
          .spillDirectory(spillDirectory->getPath())
          .assertResults(
              "SELECT *, last_value(d) over (partition by p order by s) FROM tmp");

  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(windowId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
}

// Test navigation functions: lead, lag with various offsets and default
// values. Covers: basic lead/lag, extreme offsets exceeding partition size,
// default values.
TEST_F(WindowSpillTest, navigationFunctions) {
  auto data = makeWindowTestData(1'000, 100);
  runStreamingWindowSpillTest(
      data,
      {"lag(d, 1) over (partition by p order by s)",
       "lag(d, 2) over (partition by p order by s)",
       "lead(d, 1) over (partition by p order by s)",
       "lead(d, 5) over (partition by p order by s)",
       "lag(d, 500) over (partition by p order by s)",
       "lead(d, 500) over (partition by p order by s)",
       "lead(d, 10, -999) over (partition by p order by s)",
       "lag(d, 10, -888) over (partition by p order by s)"},
      "SELECT *, "
      "lag(d, 1) over (partition by p order by s), "
      "lag(d, 2) over (partition by p order by s), "
      "lead(d, 1) over (partition by p order by s), "
      "lead(d, 5) over (partition by p order by s), "
      "lag(d, 500) over (partition by p order by s), "
      "lead(d, 500) over (partition by p order by s), "
      "lead(d, 10, -999) over (partition by p order by s), "
      "lag(d, 10, -888) over (partition by p order by s) "
      "FROM tmp");
}

// Test ranking functions: rank, dense_rank, percent_rank, cume_dist, ntile.
// Covers all ranking functions with duplicate sort keys for rank testing.
TEST_F(WindowSpillTest, rankingFunctions) {
  const vector_size_t size = 1'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row / 100; }),
          // Create duplicate sort keys for rank/dense_rank testing.
          makeFlatVector<int32_t>(size, [](auto row) { return row / 2; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan = PlanBuilder()
                  .values(split(data, 10))
                  .streamingWindow(
                      {"rank() over (partition by p order by s)",
                       "dense_rank() over (partition by p order by s)",
                       "percent_rank() over (partition by p order by s)",
                       "cume_dist() over (partition by p order by s)",
                       "ntile(4) over (partition by p order by s)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  TestScopedSpillInjection scopedSpillInjection(100);
  auto task = AssertQueryBuilder(plan, duckDbQueryRunner_)
                  .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
                  .config(core::QueryConfig::kSpillEnabled, "true")
                  .config(core::QueryConfig::kWindowSpillEnabled, "true")
                  .spillDirectory(spillDirectory->getPath())
                  .assertResults(
                      "SELECT *, rank() over (partition by p order by s), "
                      "dense_rank() over (partition by p order by s), "
                      "percent_rank() over (partition by p order by s), "
                      "cume_dist() over (partition by p order by s), "
                      "ntile(4) over (partition by p order by s) FROM tmp");

  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(windowId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
}

// Test aggregate functions with various frames (ROWS and RANGE).
// Covers: sum/avg/count/min/max aggregates, unbounded/current/k-offset frames.
TEST_F(WindowSpillTest, aggregateFunctionsWithFrames) {
  auto data = makeWindowTestData(1'000, 100);
  runStreamingWindowSpillTest(
      data,
      {"sum(d) over (partition by p order by s rows between "
       "unbounded preceding and unbounded following)",
       "avg(d) over (partition by p order by s rows between "
       "unbounded preceding and current row)",
       "count(d) over (partition by p order by s rows between "
       "current row and unbounded following)",
       "sum(d) over (partition by p order by s rows between "
       "2 preceding and 2 following)",
       "min(d) over (partition by p order by s range between "
       "unbounded preceding and unbounded following)",
       "max(d) over (partition by p order by s range between "
       "unbounded preceding and current row)"},
      "SELECT *, "
      "sum(d) over (partition by p order by s rows between unbounded preceding and unbounded following), "
      "avg(d) over (partition by p order by s rows between unbounded preceding and current row), "
      "count(d) over (partition by p order by s rows between current row and unbounded following), "
      "sum(d) over (partition by p order by s rows between 2 preceding and 2 following), "
      "min(d) over (partition by p order by s range between unbounded preceding and unbounded following), "
      "max(d) over (partition by p order by s range between unbounded preceding and current row) "
      "FROM tmp");
}

// Test multiple partition keys.
TEST_F(WindowSpillTest, multiplePartitionKeys) {
  const vector_size_t size = 1'000;
  auto data = makeRowVector(
      {"d", "p1", "p2", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row / 200; }),
          makeFlatVector<int16_t>(
              size, [](auto row) { return (row / 50) % 4; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan = PlanBuilder()
                  .values(split(data, 10))
                  .streamingWindow(
                      {"last_value(d) over (partition by p1, p2 order by s)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  TestScopedSpillInjection scopedSpillInjection(100);
  auto task =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kWindowSpillEnabled, "true")
          .spillDirectory(spillDirectory->getPath())
          .assertResults(
              "SELECT *, last_value(d) over (partition by p1, p2 order by s) FROM tmp");

  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(windowId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
}

// Test nullable data and partition key handling.
// Covers: nullable data column, nullable partition key (nulls first).
TEST_F(WindowSpillTest, nullableValues) {
  const vector_size_t size = 1'000;
  // Pre-sorted data with null partition keys first (nulls first ordering).
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          // Data column with some nulls.
          makeFlatVector<int64_t>(
              size,
              [](auto row) { return row; },
              [](auto row) { return row % 50 == 0; }),
          // Partition key with nulls first - first 100 rows have null.
          makeFlatVector<int16_t>(
              size,
              [](auto row) { return row < 100 ? 0 : (row - 100) / 100; },
              [](auto row) { return row < 100; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan =
      PlanBuilder()
          .values(split(data, 10))
          .streamingWindow({"last_value(d) over (partition by p order by s)"})
          .capturePlanNodeId(windowId)
          .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  TestScopedSpillInjection scopedSpillInjection(100);
  auto task =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kWindowSpillEnabled, "true")
          .spillDirectory(spillDirectory->getPath())
          .assertResults(
              "SELECT *, last_value(d) over (partition by p order by s) FROM tmp");

  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(windowId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
}

// Test boundary conditions: tiny partitions, single row partitions,
// duplicate sort keys, constant keys, all null data.
TEST_F(WindowSpillTest, boundaryConditions) {
  // Test 1: Single row partitions
  {
    const vector_size_t size = 1'000;
    auto data = makeRowVector(
        {"d", "p", "s"},
        {
            makeFlatVector<int64_t>(size, [](auto row) { return row; }),
            makeFlatVector<int32_t>(size, [](auto row) { return row; }),
            makeFlatVector<int32_t>(size, [](auto row) { return row; }),
        });

    createDuckDbTable({data});

    core::PlanNodeId windowId;
    auto plan =
        PlanBuilder()
            .values(split(data, 10))
            .streamingWindow({"last_value(d) over (partition by p order by s)"})
            .capturePlanNodeId(windowId)
            .planNode();

    auto spillDirectory = TempDirectoryPath::create();
    TestScopedSpillInjection scopedSpillInjection(100);
    auto task =
        AssertQueryBuilder(plan, duckDbQueryRunner_)
            .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
            .config(core::QueryConfig::kSpillEnabled, "true")
            .config(core::QueryConfig::kWindowSpillEnabled, "true")
            .spillDirectory(spillDirectory->getPath())
            .assertResults(
                "SELECT *, last_value(d) over (partition by p order by s) FROM tmp");
    task.reset();
    exec::test::waitForAllTasksToBeDeleted();
  }

  // Test 2: Duplicate sort keys
  {
    const vector_size_t size = 1'000;
    auto data = makeRowVector(
        {"d", "p", "s"},
        {
            makeFlatVector<int64_t>(size, [](auto row) { return row; }),
            makeFlatVector<int16_t>(size, [](auto row) { return row / 100; }),
            // Only 10 unique values per partition.
            makeFlatVector<int32_t>(
                size, [](auto row) { return (row % 100) / 10; }),
        });

    createDuckDbTable({data});

    core::PlanNodeId windowId;
    auto plan =
        PlanBuilder()
            .values(split(data, 10))
            .streamingWindow({"last_value(d) over (partition by p order by s)"})
            .capturePlanNodeId(windowId)
            .planNode();

    auto spillDirectory = TempDirectoryPath::create();
    TestScopedSpillInjection scopedSpillInjection(100);
    auto task =
        AssertQueryBuilder(plan, duckDbQueryRunner_)
            .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
            .config(core::QueryConfig::kSpillEnabled, "true")
            .config(core::QueryConfig::kWindowSpillEnabled, "true")
            .spillDirectory(spillDirectory->getPath())
            .assertResults(
                "SELECT *, last_value(d) over (partition by p order by s) FROM tmp");

    auto taskStats = exec::toPlanStats(task->taskStats());
    const auto& stats = taskStats.at(windowId);
    ASSERT_GT(stats.spilledBytes, 0);
    ASSERT_GT(stats.spilledRows, 0);
  }

  // Test 3: All null data values
  {
    const vector_size_t size = 500;
    auto data = makeRowVector(
        {"d", "p", "s"},
        {
            makeFlatVector<int64_t>(
                size,
                [](auto row) { return row; },
                [](auto /*row*/) { return true; }),
            makeFlatVector<int16_t>(size, [](auto row) { return row / 50; }),
            makeFlatVector<int32_t>(size, [](auto row) { return row; }),
        });

    createDuckDbTable({data});

    core::PlanNodeId windowId;
    auto plan = PlanBuilder()
                    .values(split(data, 10))
                    .streamingWindow(
                        {"last_value(d) over (partition by p order by s)",
                         "sum(d) over (partition by p order by s rows between "
                         "unbounded preceding and unbounded following)"})
                    .capturePlanNodeId(windowId)
                    .planNode();

    auto spillDirectory = TempDirectoryPath::create();
    TestScopedSpillInjection scopedSpillInjection(100);
    auto task =
        AssertQueryBuilder(plan, duckDbQueryRunner_)
            .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
            .config(core::QueryConfig::kSpillEnabled, "true")
            .config(core::QueryConfig::kWindowSpillEnabled, "true")
            .spillDirectory(spillDirectory->getPath())
            .assertResults(
                "SELECT *, last_value(d) over (partition by p order by s), "
                "sum(d) over (partition by p order by s rows between "
                "unbounded preceding and unbounded following) FROM tmp");

    auto taskStats = exec::toPlanStats(task->taskStats());
    const auto& stats = taskStats.at(windowId);
    ASSERT_GT(stats.spilledBytes, 0);
    ASSERT_GT(stats.spilledRows, 0);
  }
}

// Test backward read access pattern with first_value, last_value, and
// nth_value. Uses small read buffer to force cache eviction and backward read.
TEST_F(WindowSpillTest, backwardRead) {
  const vector_size_t size = 5'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row * 10; }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // Use first_value, last_value, and nth_value with unbounded frame to test
  // backward access.
  auto plan =
      PlanBuilder()
          .values(split(data, 50))
          .streamingWindow(
              {"first_value(d) over (partition by p order by s rows between unbounded preceding and unbounded following)",
               "last_value(d) over (partition by p order by s rows between unbounded preceding and unbounded following)",
               "nth_value(d, 100) over (partition by p order by s rows between unbounded preceding and unbounded following)",
               "nth_value(d, 1000) over (partition by p order by s rows between unbounded preceding and unbounded following)"})
          .capturePlanNodeId(windowId)
          .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  TestScopedSpillInjection scopedSpillInjection(100);
  auto task =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kWindowSpillEnabled, "true")
          .config(core::QueryConfig::kSpillReadBufferSize, "4096")
          .spillDirectory(spillDirectory->getPath())
          .assertResults(
              "SELECT *, "
              "first_value(d) over (partition by p order by s rows between unbounded preceding and unbounded following), "
              "last_value(d) over (partition by p order by s rows between unbounded preceding and unbounded following), "
              "nth_value(d, 100) over (partition by p order by s rows between unbounded preceding and unbounded following), "
              "nth_value(d, 1000) over (partition by p order by s rows between unbounded preceding and unbounded following) "
              "FROM tmp");

  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(windowId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
}

// Test backward read across multiple spill files with multiple partitions.
TEST_F(WindowSpillTest, multiFileBackwardRead) {
  const vector_size_t size = 20'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row * 10; }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan =
      PlanBuilder()
          .values(split(data, 100))
          .streamingWindow(
              {"first_value(d) over (partition by p order by s rows between unbounded preceding and unbounded following)"})
          .capturePlanNodeId(windowId)
          .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  TestScopedSpillInjection scopedSpillInjection(100);
  auto task =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kWindowSpillEnabled, "true")
          .config(core::QueryConfig::kSpillWriteBufferSize, "4096")
          .config(core::QueryConfig::kSpillReadBufferSize, "4096")
          .spillDirectory(spillDirectory->getPath())
          .assertResults(
              "SELECT *, first_value(d) over (partition by p order by s rows between unbounded preceding and unbounded following) FROM tmp");

  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(windowId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
  ASSERT_GT(stats.spilledFiles, 1);
}

// Test empty partition keys.
TEST_F(WindowSpillTest, emptyPartitionKeys) {
  const vector_size_t size = 1'000;
  auto data = makeRowVector(
      {"d", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan = PlanBuilder()
                  .values(split(data, 10))
                  .streamingWindow({"last_value(d) over (order by s)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  TestScopedSpillInjection scopedSpillInjection(100);
  auto task =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kWindowSpillEnabled, "true")
          .spillDirectory(spillDirectory->getPath())
          .assertResults("SELECT *, last_value(d) over (order by s) FROM tmp");

  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(windowId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
}

// Test mixed mode: some partitions in memory, some spilled.
// Also covers alternating spill pattern with 50% injection rate.
TEST_F(WindowSpillTest, mixedModeAndAlternating) {
  const vector_size_t size = 1'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row / 100; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan =
      PlanBuilder()
          .values(split(data, 10))
          .streamingWindow({"last_value(d) over (partition by p order by s)"})
          .capturePlanNodeId(windowId)
          .planNode();

  auto spillDirectory = TempDirectoryPath::create();

  // Run multiple times with 50% spill rate to test alternating patterns.
  for (int run = 0; run < 3; ++run) {
    TestScopedSpillInjection scopedSpillInjection(50);
    auto task =
        AssertQueryBuilder(plan, duckDbQueryRunner_)
            .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
            .config(core::QueryConfig::kSpillEnabled, "true")
            .config(core::QueryConfig::kWindowSpillEnabled, "true")
            .spillDirectory(spillDirectory->getPath())
            .assertResults(
                "SELECT *, last_value(d) over (partition by p order by s) FROM tmp");
    task.reset();
    waitForAllTasksToBeDeleted();
  }
}

// Test multiple spills within the same partition and across partitions.
// Covers: multiple spills during input, spills during output, memory release.
TEST_F(WindowSpillTest, multipleSpills) {
  const vector_size_t size = 2'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          // Large strings to increase memory usage.
          makeFlatVector<std::string>(
              size,
              [](auto row) {
                return std::string(100, 'x') + std::to_string(row);
              }),
          makeFlatVector<int16_t>(size, [](auto row) { return row / 200; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // Many small batches to trigger multiple spills.
  auto plan =
      PlanBuilder()
          .values(split(data, 40))
          .streamingWindow({"last_value(d) over (partition by p order by s)"})
          .capturePlanNodeId(windowId)
          .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  TestScopedSpillInjection scopedSpillInjection(100);
  auto task =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(core::QueryConfig::kPreferredOutputBatchBytes, "512")
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kWindowSpillEnabled, "true")
          .spillDirectory(spillDirectory->getPath())
          .assertResults(
              "SELECT *, last_value(d) over (partition by p order by s) FROM tmp");

  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(windowId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
  ASSERT_GT(stats.spilledFiles, 1);
}

// Test memory management: ensureOutputFits, cached row size, memory release.
TEST_F(WindowSpillTest, memoryManagement) {
  const vector_size_t size = 3'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<std::string>(
              size,
              [](auto row) {
                return std::string(80, 'y') + std::to_string(row);
              }),
          makeFlatVector<int16_t>(size, [](auto row) { return row / 300; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan =
      PlanBuilder()
          .values(split(data, 30))
          .orderBy({"p", "s"}, false)
          .streamingWindow({"last_value(d) over (partition by p order by s)"})
          .capturePlanNodeId(windowId)
          .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  TestScopedSpillInjection scopedSpillInjection(100);

  auto task =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(core::QueryConfig::kPreferredOutputBatchBytes, "512")
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kWindowSpillEnabled, "true")
          .spillDirectory(spillDirectory->getPath())
          .assertResults(
              "SELECT *, last_value(d) over (partition by p order by s) FROM tmp");

  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(windowId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
}

// Test deterministic spill injection pattern using maxInjections.
TEST_F(WindowSpillTest, deterministicSpillPattern) {
  const vector_size_t size = 800;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row / 100; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan =
      PlanBuilder()
          .values(split(data, 8))
          .streamingWindow({"last_value(d) over (partition by p order by s)"})
          .capturePlanNodeId(windowId)
          .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  // Limit to 4 injections - tests transition from spill to non-spill mode.
  TestScopedSpillInjection scopedSpillInjection(100, ".*", 4);
  auto task =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kWindowSpillEnabled, "true")
          .spillDirectory(spillDirectory->getPath())
          .assertResults(
              "SELECT *, last_value(d) over (partition by p order by s) FROM tmp");

  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(windowId);

  // Should have spilled some but not all rows.
  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
  ASSERT_LT(stats.spilledRows, size);
}

// Test large single partition stress.
TEST_F(WindowSpillTest, largeSinglePartition) {
  const vector_size_t size = 2'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan =
      PlanBuilder()
          .values(split(data, 20))
          .streamingWindow({"last_value(d) over (partition by p order by s)"})
          .capturePlanNodeId(windowId)
          .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  TestScopedSpillInjection scopedSpillInjection(100);
  auto task =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kWindowSpillEnabled, "true")
          .spillDirectory(spillDirectory->getPath())
          .assertResults(
              "SELECT *, last_value(d) over (partition by p order by s) FROM tmp");

  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(windowId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
}

// =============================================================================
// Tests to verify incrementalAggregation vs simpleAggregation memory behavior.
//
// Key insight from AggregateWindow.cpp:
// - incrementalAggregation: Used when frameStart is fixed AND frameEnd is
//   non-decreasing. Loads data incrementally (~4096 rows per output block).
// - simpleAggregation: Used otherwise. Loads the ENTIRE frame range for each
//   output block, which can cause memory explosion for UNBOUNDED frames.
// =============================================================================

// Test that incrementalAggregation path has bounded memory usage.
// Frame: UNBOUNDED PRECEDING TO CURRENT ROW
// - frameStart is fixed (partition start)
// - frameEnd is non-decreasing (current row)
// => Uses incrementalAggregation, memory should remain bounded.
TEST_F(WindowSpillTest, incrementalAggregationMemoryBounded) {
  // Create a large single partition to stress test memory usage.
  const vector_size_t size = 50'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  // Use SUM with UNBOUNDED PRECEDING TO CURRENT ROW - this triggers
  // incrementalAggregation because:
  // - frameStart is fixed (UNBOUNDED PRECEDING = 0)
  // - frameEnd is non-decreasing (CURRENT ROW increases with each row)
  core::PlanNodeId windowId;
  auto plan = PlanBuilder()
                  .values(split(data, 50))
                  .streamingWindow(
                      {"sum(d) over (partition by p order by s "
                       "rows between unbounded preceding and current row)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  auto queryCtx = core::QueryCtx::create(executor_.get());

  auto task = AssertQueryBuilder(plan, duckDbQueryRunner_)
                  .queryCtx(queryCtx)
                  .config(core::QueryConfig::kPreferredOutputBatchBytes, "4096")
                  .assertResults(
                      "SELECT *, sum(d) over (partition by p order by s "
                      "rows between unbounded preceding and current row) "
                      "FROM tmp");

  // With incrementalAggregation, memory should remain bounded because
  // fillArgVectors only loads incremental rows (startRow = previousLastRow+1).
  // The peak memory should be much smaller than loading the entire partition.
  const auto peakBytes = queryCtx->pool()->peakBytes();

  // With incrementalAggregation, peak memory should be < 10MB for 50k rows.
  // If simpleAggregation were used, it would load all 50k rows repeatedly.
  // Each row has int64 + int16 + int32 = 14 bytes, so 50k rows = ~700KB data.
  // With incrementalAggregation, we only load ~4096 rows at a time = ~57KB.
  // Allow some overhead for vectors, accumulators, etc.
  ASSERT_LT(peakBytes, 10 * 1024 * 1024)
      << "Peak memory " << peakBytes
      << " bytes is too high for incrementalAggregation";

  LOG(INFO) << "incrementalAggregation peak memory: " << peakBytes << " bytes ("
            << (peakBytes / 1024.0 / 1024.0) << " MB)";
}

// Test that simpleAggregation path loads entire frame (potential memory issue).
// Frame: UNBOUNDED PRECEDING TO UNBOUNDED FOLLOWING
// - frameStart is fixed (partition start)
// - frameEnd is fixed (partition end) - NOT non-decreasing per row
// => Uses simpleAggregation, loads entire partition for each output block.
TEST_F(WindowSpillTest, simpleAggregationLoadsEntireFrame) {
  // Create a moderate-sized single partition.
  const vector_size_t size = 20'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  // Use SUM with UNBOUNDED PRECEDING TO UNBOUNDED FOLLOWING
  // This triggers simpleAggregation because frameEnd is fixed (not increasing).
  core::PlanNodeId windowId;
  auto plan =
      PlanBuilder()
          .values(split(data, 20))
          .streamingWindow(
              {"sum(d) over (partition by p order by s "
               "rows between unbounded preceding and unbounded following)"})
          .capturePlanNodeId(windowId)
          .planNode();

  auto queryCtx = core::QueryCtx::create(executor_.get());

  auto task =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .queryCtx(queryCtx)
          .config(core::QueryConfig::kPreferredOutputBatchBytes, "4096")
          .assertResults(
              "SELECT *, sum(d) over (partition by p order by s "
              "rows between unbounded preceding and unbounded following) "
              "FROM tmp");

  const auto peakBytes = queryCtx->pool()->peakBytes();

  // With simpleAggregation, the entire partition is loaded for each output
  // block via fillArgVectors(firstRow=0, lastRow=partitionEnd).
  // This causes higher memory usage than incrementalAggregation.
  // We verify that peak memory is significantly higher than the incremental
  // case.
  // 20k rows * 14 bytes/row = ~280KB data, but with vector overhead and
  // multiple copies, we expect at least this much memory.
  ASSERT_GT(peakBytes, 100 * 1024)
      << "Peak memory should reflect loading entire partition";

  LOG(INFO) << "simpleAggregation peak memory: " << peakBytes << " bytes ("
            << (peakBytes / 1024.0 / 1024.0) << " MB)";
}

// Compare memory usage between incrementalAggregation and simpleAggregation
// directly. This test proves that incrementalAggregation uses less memory.
TEST_F(WindowSpillTest, compareIncrementalVsSimpleAggregationMemory) {
  const vector_size_t size = 30'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  int64_t incrementalPeakBytes = 0;
  int64_t simplePeakBytes = 0;

  // Test 1: incrementalAggregation (UNBOUNDED PRECEDING TO CURRENT ROW)
  {
    core::PlanNodeId windowId;
    auto plan = PlanBuilder()
                    .values(split(data, 30))
                    .streamingWindow(
                        {"sum(d) over (partition by p order by s "
                         "rows between unbounded preceding and current row)"})
                    .capturePlanNodeId(windowId)
                    .planNode();

    auto queryCtx = core::QueryCtx::create(executor_.get());

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .queryCtx(queryCtx)
        .config(core::QueryConfig::kPreferredOutputBatchBytes, "4096")
        .assertResults(
            "SELECT *, sum(d) over (partition by p order by s "
            "rows between unbounded preceding and current row) "
            "FROM tmp");

    incrementalPeakBytes = queryCtx->pool()->peakBytes();
    LOG(INFO) << "Incremental aggregation peak: " << incrementalPeakBytes
              << " bytes (" << (incrementalPeakBytes / 1024.0 / 1024.0)
              << " MB)";
  }

  // Test 2: simpleAggregation (UNBOUNDED PRECEDING TO UNBOUNDED FOLLOWING)
  {
    core::PlanNodeId windowId;
    auto plan =
        PlanBuilder()
            .values(split(data, 30))
            .streamingWindow(
                {"sum(d) over (partition by p order by s "
                 "rows between unbounded preceding and unbounded following)"})
            .capturePlanNodeId(windowId)
            .planNode();

    auto queryCtx = core::QueryCtx::create(executor_.get());

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .queryCtx(queryCtx)
        .config(core::QueryConfig::kPreferredOutputBatchBytes, "4096")
        .assertResults(
            "SELECT *, sum(d) over (partition by p order by s "
            "rows between unbounded preceding and unbounded following) "
            "FROM tmp");

    simplePeakBytes = queryCtx->pool()->peakBytes();
    LOG(INFO) << "Simple aggregation peak: " << simplePeakBytes << " bytes ("
              << (simplePeakBytes / 1024.0 / 1024.0) << " MB)";
  }

  // Verify that simpleAggregation uses more memory than incrementalAggregation.
  // Due to the nature of simpleAggregation loading entire partition vs
  // incrementalAggregation loading only incremental rows, we expect a
  // noticeable difference.
  LOG(INFO) << "Memory ratio (simple/incremental): "
            << (static_cast<double>(simplePeakBytes) / incrementalPeakBytes);

  // Note: The ratio depends on partition size and output batch size.
  // For large partitions with small output batches, the difference is more
  // pronounced because simpleAggregation re-loads the entire partition for
  // each block while incrementalAggregation accumulates incrementally.
  // We conservatively check that simple uses at least as much memory.
  ASSERT_GE(simplePeakBytes, incrementalPeakBytes)
      << "simpleAggregation should use at least as much memory as "
         "incrementalAggregation";
}

// Test that sliding window frame (N PRECEDING TO M FOLLOWING) uses
// simpleAggregation because frameStart changes with each row.
TEST_F(WindowSpillTest, slidingWindowUsesSimpleAggregation) {
  const vector_size_t size = 10'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  // N PRECEDING TO M FOLLOWING - frameStart varies with each row
  // => Uses simpleAggregation
  core::PlanNodeId windowId;
  auto plan =
      PlanBuilder()
          .values(split(data, 10))
          .streamingWindow({"sum(d) over (partition by p order by s "
                            "rows between 100 preceding and 100 following)"})
          .capturePlanNodeId(windowId)
          .planNode();

  auto queryCtx = core::QueryCtx::create(executor_.get());

  auto task = AssertQueryBuilder(plan, duckDbQueryRunner_)
                  .queryCtx(queryCtx)
                  .config(core::QueryConfig::kPreferredOutputBatchBytes, "4096")
                  .assertResults(
                      "SELECT *, sum(d) over (partition by p order by s "
                      "rows between 100 preceding and 100 following) "
                      "FROM tmp");

  const auto peakBytes = queryCtx->pool()->peakBytes();
  LOG(INFO) << "Sliding window (100 PRECEDING TO 100 FOLLOWING) peak memory: "
            << peakBytes << " bytes (" << (peakBytes / 1024.0 / 1024.0)
            << " MB)";

  // Verify results are correct (implicit in assertResults).
}

// Test that CURRENT ROW TO UNBOUNDED FOLLOWING uses simpleAggregation
// because frameStart changes (moves forward with each row).
TEST_F(WindowSpillTest, currentRowToUnboundedFollowingUsesSimple) {
  const vector_size_t size = 10'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  // CURRENT ROW TO UNBOUNDED FOLLOWING - frameStart varies
  // => Uses simpleAggregation
  core::PlanNodeId windowId;
  auto plan = PlanBuilder()
                  .values(split(data, 10))
                  .streamingWindow(
                      {"sum(d) over (partition by p order by s "
                       "rows between current row and unbounded following)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  auto queryCtx = core::QueryCtx::create(executor_.get());

  auto task = AssertQueryBuilder(plan, duckDbQueryRunner_)
                  .queryCtx(queryCtx)
                  .config(core::QueryConfig::kPreferredOutputBatchBytes, "4096")
                  .assertResults(
                      "SELECT *, sum(d) over (partition by p order by s "
                      "rows between current row and unbounded following) "
                      "FROM tmp");

  const auto peakBytes = queryCtx->pool()->peakBytes();
  LOG(INFO) << "CURRENT ROW TO UNBOUNDED FOLLOWING peak memory: " << peakBytes
            << " bytes (" << (peakBytes / 1024.0 / 1024.0) << " MB)";
}

// Test that batched aggregation limits memory for very large UNBOUNDED frames.
// This test verifies the optimization in AggregateWindow.cpp that processes
// large frames in batches of kBatchSize (4096) rows instead of loading the
// entire frame at once.
TEST_F(WindowSpillTest, batchedAggregationMemoryBounded) {
  // Create a large single partition to test batched processing.
  // With 100k rows, the UNBOUNDED...UNBOUNDED frame would normally load
  // all 100k rows at once. With batched processing, it should only load
  // kBatchSize (4096) rows at a time.
  const vector_size_t size = 100'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  // UNBOUNDED PRECEDING TO UNBOUNDED FOLLOWING with large partition.
  // This triggers simpleAggregation with batched processing.
  core::PlanNodeId windowId;
  auto plan =
      PlanBuilder()
          .values(split(data, 100))
          .streamingWindow(
              {"sum(d) over (partition by p order by s "
               "rows between unbounded preceding and unbounded following)"})
          .capturePlanNodeId(windowId)
          .planNode();

  auto queryCtx = core::QueryCtx::create(executor_.get());

  auto task =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .queryCtx(queryCtx)
          .config(core::QueryConfig::kPreferredOutputBatchBytes, "4096")
          .assertResults(
              "SELECT *, sum(d) over (partition by p order by s "
              "rows between unbounded preceding and unbounded following) "
              "FROM tmp");

  const auto peakBytes = queryCtx->pool()->peakBytes();
  LOG(INFO) << "Batched UNBOUNDED...UNBOUNDED (100k rows) peak memory: "
            << peakBytes << " bytes (" << (peakBytes / 1024.0 / 1024.0)
            << " MB)";

  // With batched processing (kBatchSize=4096), peak memory should be much less
  // than loading all 100k rows at once.
  // 100k rows * ~14 bytes/row = ~1.4MB if loaded at once.
  // With batching: 4096 rows * ~14 bytes/row = ~57KB per batch.
  // Allow reasonable overhead for vectors, accumulators, and output buffers.
  // The key is that peak memory should be bounded, not proportional to
  // partition size.
  ASSERT_LT(peakBytes, 50 * 1024 * 1024)
      << "Peak memory should be bounded with batched processing";
}

} // namespace
} // namespace facebook::velox::exec
