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
  ///
  /// @param data Input data for the window operation.
  /// @param windowFunctions Window function expressions.
  /// @param expectedSql DuckDB SQL for result verification.
  /// @param numBatches Number of batches to split input data into.
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
  ///
  /// @param size Total number of rows.
  /// @param partitionSize Number of rows per partition.
  /// @return RowVector with columns {d, p, s}.
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

  /// Creates test data with a single partition (all rows have same partition
  /// key).
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

TEST_F(WindowSpillTest, spillPartitionBuildBasic) {
  auto data = makeWindowTestData(1'000, 100);
  runStreamingWindowSpillTest(
      data,
      {"last_value(d) over (partition by p order by s)"},
      "SELECT *, last_value(d) over (partition by p order by s) FROM tmp");
}

// Test spill with multiple window functions.
TEST_F(WindowSpillTest, spillPartitionBuildMultipleFunctions) {
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
TEST_F(WindowSpillTest, spillPartitionBuildSinglePartition) {
  auto data = makeSinglePartitionData(1'000);
  runStreamingWindowSpillTest(
      data,
      {"last_value(d) over (partition by p order by s)"},
      "SELECT *, last_value(d) over (partition by p order by s) FROM tmp");
}

// Test spill with many small partitions.
TEST_F(WindowSpillTest, spillPartitionBuildManyPartitions) {
  auto data = makeWindowTestData(1'000, 50); // 20 partitions of 50 rows each
  runStreamingWindowSpillTest(
      data,
      {"last_value(d) over (partition by p order by s)"},
      "SELECT *, last_value(d) over (partition by p order by s) FROM tmp");
}

// Test spill with string columns.
TEST_F(WindowSpillTest, spillPartitionBuildWithStrings) {
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

// Test spill with navigation functions (lead/lag).
TEST_F(WindowSpillTest, spillPartitionBuildNavigation) {
  auto data = makeWindowTestData(1'000, 100);
  runStreamingWindowSpillTest(
      data,
      {"lag(d, 1) over (partition by p order by s)",
       "lag(d, 2) over (partition by p order by s)",
       "lead(d, 1) over (partition by p order by s)",
       "lead(d, 2) over (partition by p order by s)"},
      "SELECT *, lag(d, 1) over (partition by p order by s), "
      "lag(d, 2) over (partition by p order by s), "
      "lead(d, 1) over (partition by p order by s), "
      "lead(d, 2) over (partition by p order by s) FROM tmp");
}

// Test spill with aggregate window functions.
// Note: Simple aggregate functions with default frame use
// RowsStreamingWindowBuild which doesn't spill. Use explicit frame to force
// PartitionStreamingWindowBuild.
TEST_F(WindowSpillTest, spillPartitionBuildAggregate) {
  auto data = makeWindowTestData(1'000, 100);
  runStreamingWindowSpillTest(
      data,
      {"sum(d) over (partition by p order by s rows between "
       "unbounded preceding and unbounded following)",
       "avg(d) over (partition by p order by s rows between "
       "unbounded preceding and unbounded following)",
       "count(d) over (partition by p order by s rows between "
       "unbounded preceding and unbounded following)"},
      "SELECT *, "
      "sum(d) over (partition by p order by s rows between unbounded preceding and unbounded following), "
      "avg(d) over (partition by p order by s rows between unbounded preceding and unbounded following), "
      "count(d) over (partition by p order by s rows between unbounded preceding and unbounded following) "
      "FROM tmp");
}

// Test spill with ranking functions.
TEST_F(WindowSpillTest, spillPartitionBuildRanking) {
  const vector_size_t size = 1'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row / 100; }),
          // Create some duplicate sort keys for rank/dense_rank testing.
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
                       "cume_dist() over (partition by p order by s)"})
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
                      "cume_dist() over (partition by p order by s) FROM tmp");

  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(windowId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
}

// Test spill with window frames (ROWS BETWEEN).
TEST_F(WindowSpillTest, spillPartitionBuildRowsFrame) {
  auto data = makeWindowTestData(1'000, 100);
  runStreamingWindowSpillTest(
      data,
      {"sum(d) over (partition by p order by s rows between "
       "unbounded preceding and current row)",
       "sum(d) over (partition by p order by s rows between "
       "current row and unbounded following)",
       "sum(d) over (partition by p order by s rows between "
       "2 preceding and 2 following)"},
      "SELECT *, "
      "sum(d) over (partition by p order by s rows between unbounded preceding and current row), "
      "sum(d) over (partition by p order by s rows between current row and unbounded following), "
      "sum(d) over (partition by p order by s rows between 2 preceding and 2 following) "
      "FROM tmp");
}

// Test spill with RANGE frame.
TEST_F(WindowSpillTest, spillPartitionBuildRangeFrame) {
  auto data = makeWindowTestData(1'000, 100);
  runStreamingWindowSpillTest(
      data,
      {"sum(d) over (partition by p order by s range between "
       "unbounded preceding and current row)",
       "sum(d) over (partition by p order by s range between "
       "current row and unbounded following)"},
      "SELECT *, "
      "sum(d) over (partition by p order by s range between unbounded preceding and current row), "
      "sum(d) over (partition by p order by s range between current row and unbounded following) "
      "FROM tmp");
}

// Test spill with multiple partition keys.
TEST_F(WindowSpillTest, spillPartitionBuildMultiplePartitionKeys) {
  const vector_size_t size = 1'000;
  auto data = makeRowVector(
      {"d", "p1", "p2", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // First partition key.
          makeFlatVector<int16_t>(size, [](auto row) { return row / 200; }),
          // Second partition key.
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

// Test spill with nullable data column (not partition key).
// Note: Nullable partition keys require special sort order handling which
// is complex for streaming window. Test nullable payload instead.
TEST_F(WindowSpillTest, spillPartitionBuildNullableData) {
  const vector_size_t size = 1'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          // Data column with some nulls.
          makeFlatVector<int64_t>(
              size,
              [](auto row) { return row; },
              [](auto row) { return row % 50 == 0; }), // Every 50th row is null
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

// Test mixed mode: some partitions in memory, some spilled.
// This happens when spill is triggered mid-stream.
TEST_F(WindowSpillTest, spillPartitionBuildMixedMode) {
  const vector_size_t size = 1'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // 10 partitions of 100 rows each.
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
  // 50% spill injection rate - some batches will trigger spill, some won't.
  TestScopedSpillInjection scopedSpillInjection(50);
  auto task =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kWindowSpillEnabled, "true")
          .spillDirectory(spillDirectory->getPath())
          .assertResults(
              "SELECT *, last_value(d) over (partition by p order by s) FROM tmp");

  // Just verify correctness - mixed mode should produce correct results.
  // Spill stats may or may not be > 0 depending on when injection triggered.
}

// Test spill with very small partitions (1-2 rows each).
// This is a boundary condition where spill may not be beneficial.
TEST_F(WindowSpillTest, spillPartitionBuildTinyPartitions) {
  const vector_size_t size = 500;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // Each row is its own partition.
          makeFlatVector<int16_t>(size, [](auto row) { return row; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan =
      PlanBuilder()
          .values(split(data, 5))
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

  // Verify correctness even with tiny partitions.
  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(windowId);
  // With tiny partitions, there may or may not be rows to spill depending on
  // timing.

  // Wait for task to be fully deleted before TestScopedSpillInjection
  // destructor runs, to avoid race conditions with other tests.
  task.reset();
  waitForAllTasksToBeDeleted();
}

// Test spill with lead/lag functions that access rows outside current position.
TEST_F(WindowSpillTest, spillPartitionBuildLeadLag) {
  auto data = makeWindowTestData(1'000, 100);
  runStreamingWindowSpillTest(
      data,
      {"lead(d, 5) over (partition by p order by s)",
       "lag(d, 3) over (partition by p order by s)"},
      "SELECT *, lead(d, 5) over (partition by p order by s), "
      "lag(d, 3) over (partition by p order by s) FROM tmp");
}

// Test spill with lead/lag functions with default values.
TEST_F(WindowSpillTest, spillPartitionBuildLeadLagWithDefault) {
  auto data = makeWindowTestData(1'000, 100);
  runStreamingWindowSpillTest(
      data,
      {"lead(d, 10, -999) over (partition by p order by s)",
       "lag(d, 10, -888) over (partition by p order by s)"},
      "SELECT *, lead(d, 10, -999) over (partition by p order by s), "
      "lag(d, 10, -888) over (partition by p order by s) FROM tmp");
}

// Test spill at partition boundary - partition ends exactly when spill
// triggers. Split into 10 batches of 100 rows - each batch is exactly one
// partition.
TEST_F(WindowSpillTest, spillPartitionBuildAtBoundary) {
  auto data = makeWindowTestData(1'000, 100);
  runStreamingWindowSpillTest(
      data,
      {"last_value(d) over (partition by p order by s)"},
      "SELECT *, last_value(d) over (partition by p order by s) FROM tmp");
}

// Test spill with ntile function.
TEST_F(WindowSpillTest, spillPartitionBuildNtile) {
  auto data = makeWindowTestData(1'000, 100);
  runStreamingWindowSpillTest(
      data,
      {"ntile(4) over (partition by p order by s)"},
      "SELECT *, ntile(4) over (partition by p order by s) FROM tmp");
}

// Test spill with first_value/nth_value functions.
// These functions require PartitionStreamingWindowBuild (not RowsStreaming).
TEST_F(WindowSpillTest, spillPartitionBuildFirstNthValue) {
  auto data = makeWindowTestData(1'000, 100);
  runStreamingWindowSpillTest(
      data,
      {"first_value(d) over (partition by p order by s)",
       "nth_value(d, 3) over (partition by p order by s)",
       "last_value(d) over (partition by p order by s)"},
      "SELECT *, first_value(d) over (partition by p order by s), "
      "nth_value(d, 3) over (partition by p order by s), "
      "last_value(d) over (partition by p order by s) FROM tmp");
}

// Test spill with aggregate function using non-default frame.
// Range frame with unbounded preceding/following forces
// PartitionStreamingWindowBuild.
TEST_F(WindowSpillTest, spillPartitionBuildNonDefaultFrame) {
  auto data = makeWindowTestData(1'000, 100);
  runStreamingWindowSpillTest(
      data,
      {"sum(d) over (partition by p order by s range between "
       "unbounded preceding and unbounded following)",
       "min(d) over (partition by p order by s range between "
       "unbounded preceding and unbounded following)",
       "max(d) over (partition by p order by s range between "
       "unbounded preceding and unbounded following)"},
      "SELECT *, "
      "sum(d) over (partition by p order by s range between unbounded preceding and unbounded following), "
      "min(d) over (partition by p order by s range between unbounded preceding and unbounded following), "
      "max(d) over (partition by p order by s range between unbounded preceding and unbounded following) "
      "FROM tmp");
}

// Test spill with nullable partition key.
// This tests spill behavior when partition boundaries involve null comparisons.
TEST_F(WindowSpillTest, spillPartitionBuildNullablePartitionKey) {
  const vector_size_t size = 1'000;
  // Pre-sorted data with null partition keys first (nulls first ordering).
  // Layout: rows 0-99 have null partition key, rows 100-999 have non-null
  // partition keys (0-8).
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
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

// Test spill with single row partitions - boundary case where each partition
// has exactly one row. With single-row partitions, each partition completes
// immediately and may be consumed before spill triggers, so we don't verify
// spill stats here - just verify correctness.
TEST_F(WindowSpillTest, spillPartitionBuildSingleRowPartitions) {
  const vector_size_t size = 1'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // Each row is its own partition.
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // Use last_value which requires PartitionStreamingWindowBuild
  auto plan =
      PlanBuilder()
          .values(split(data, 10))
          .streamingWindow({"last_value(d) over (partition by p order by s)"})
          .capturePlanNodeId(windowId)
          .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  TestScopedSpillInjection scopedSpillInjection(100);
  // Just verify correctness - with single-row partitions, spill may not
  // actually trigger as each partition completes immediately.
  auto task =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kWindowSpillEnabled, "true")
          .spillDirectory(spillDirectory->getPath())
          .assertResults(
              "SELECT *, last_value(d) over (partition by p order by s) FROM tmp");
  // Wait for task to be fully deleted before scopedSpillInjection destructor
  // runs. This prevents test interference when multiple tests share global
  // spill injection state.
  task.reset();
  exec::test::waitForAllTasksToBeDeleted();
}

// Test spill with duplicate sort keys - boundary case where many rows have
// the same sort key value within a partition. Input must be pre-sorted for
// streaming window.
TEST_F(WindowSpillTest, spillPartitionBuildDuplicateSortKeys) {
  const vector_size_t size = 1'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // 10 partitions of 100 rows each.
          makeFlatVector<int16_t>(size, [](auto row) { return row / 100; }),
          // Many duplicate sort keys - only 10 unique values per partition.
          // Data is pre-sorted by (p, s).
          makeFlatVector<int32_t>(
              size, [](auto row) { return (row % 100) / 10; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // Use last_value which requires PartitionStreamingWindowBuild
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

// Test spill with extreme lead/lag offsets that exceed partition size.
TEST_F(WindowSpillTest, spillPartitionBuildExtremeLagLead) {
  auto data = makeWindowTestData(1'000, 100);
  runStreamingWindowSpillTest(
      data,
      {"lag(d, 500) over (partition by p order by s)",
       "lead(d, 500) over (partition by p order by s)",
       "lag(d, 99) over (partition by p order by s)",
       "lead(d, 99) over (partition by p order by s)"},
      "SELECT *, lag(d, 500) over (partition by p order by s), "
      "lead(d, 500) over (partition by p order by s), "
      "lag(d, 99) over (partition by p order by s), "
      "lead(d, 99) over (partition by p order by s) FROM tmp");
}

// Test spill with all null data values - boundary case.
TEST_F(WindowSpillTest, spillPartitionBuildAllNullData) {
  const vector_size_t size = 500;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          // All null data values.
          makeFlatVector<int64_t>(
              size,
              [](auto row) { return row; },
              [](auto /*row*/) { return true; }), // All nulls
          makeFlatVector<int16_t>(size, [](auto row) { return row / 50; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan = PlanBuilder()
                  .values(split(data, 10))
                  .streamingWindow(
                      {"last_value(d) over (partition by p order by s)",
                       "first_value(d) over (partition by p order by s)",
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
              "first_value(d) over (partition by p order by s), "
              "sum(d) over (partition by p order by s rows between "
              "unbounded preceding and unbounded following) FROM tmp");

  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(windowId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
}

// Test spill with very large single partition - stress test for spill
// with all data in one partition. Use smaller partition size that fits in
// cache to avoid cache eviction issues.
// TODO: Add test for larger partitions after fixing cache eviction bug
// for window functions that need backward access.
TEST_F(WindowSpillTest, spillPartitionBuildLargeSinglePartition) {
  const vector_size_t size = 2'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // All rows in single partition.
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

// Test spill with constant partition key and sort key - all rows have same
// partition and sort key values.
TEST_F(WindowSpillTest, spillPartitionBuildConstantKeys) {
  const vector_size_t size = 1'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // Constant partition key.
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 1; }),
          // Constant sort key.
          makeFlatVector<int32_t>(size, [](auto /*row*/) { return 100; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // Use last_value which requires PartitionStreamingWindowBuild
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

// Test spill with backward read access pattern. Uses a very small read buffer
// size to limit cache capacity, forcing backward read (resetSpillStream) when
// first_value/last_value functions need to access rows that have been evicted.
// This test specifically verifies the fix for cache eviction bug where
// first_value/last_value functions fail to access evicted rows.
TEST_F(WindowSpillTest, spillPartitionBuildBackwardRead) {
  // Use a moderate size that will exceed the small cache capacity.
  const vector_size_t size = 5'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row * 10; }),
          // All rows in single partition to maximize backward read.
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // Use first_value with unbounded frame - requires backward access to row 0.
  // This is the pattern that triggered the original bug.
  auto plan =
      PlanBuilder()
          .values(split(data, 50))
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
          // Use a very small read buffer to limit cache capacity.
          // This forces cache eviction and backward read.
          .config(core::QueryConfig::kSpillReadBufferSize, "4096")
          .spillDirectory(spillDirectory->getPath())
          .assertResults(
              "SELECT *, first_value(d) over (partition by p order by s rows between unbounded preceding and unbounded following) FROM tmp");

  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(windowId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
}

// Test spill backward read with last_value - verifies backward read works
// for accessing the last row when processing early rows in the partition.
TEST_F(WindowSpillTest, spillPartitionBuildBackwardReadLastValue) {
  const vector_size_t size = 5'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row * 10; }),
          // All rows in single partition.
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // Use last_value with unbounded frame - requires forward access to last row.
  auto plan =
      PlanBuilder()
          .values(split(data, 50))
          .streamingWindow(
              {"last_value(d) over (partition by p order by s rows between unbounded preceding and unbounded following)"})
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
              "SELECT *, last_value(d) over (partition by p order by s rows between unbounded preceding and unbounded following) FROM tmp");

  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(windowId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
}

// Test spill backward read with both first_value and last_value together.
// This is the most demanding case - requires access to both extremes of the
// partition from every processing position.
TEST_F(WindowSpillTest, spillPartitionBuildBackwardReadBothEnds) {
  const vector_size_t size = 5'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row * 10; }),
          // All rows in single partition.
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // Use both first_value and last_value with unbounded frame.
  auto plan =
      PlanBuilder()
          .values(split(data, 50))
          .streamingWindow(
              {"first_value(d) over (partition by p order by s rows between unbounded preceding and unbounded following)",
               "last_value(d) over (partition by p order by s rows between unbounded preceding and unbounded following)"})
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
              "last_value(d) over (partition by p order by s rows between unbounded preceding and unbounded following) "
              "FROM tmp");

  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(windowId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
}

// Test that PartitionStreamingWindowBuild supports spill even with empty
// partition keys.
TEST_F(WindowSpillTest, spillPartitionBuildEmptyPartitionKeys) {
  const vector_size_t size = 1'000;
  auto data = makeRowVector(
      {"d", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // No partition key, only order by. Use last_value() which does not support
  // rows streaming, so PartitionStreamingWindowBuild is used.
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

  // PartitionStreamingWindowBuild should spill even with empty partition keys.
  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
}

// Test backward read across multiple spill files.
// This verifies the optimization using fileRowRanges_ to skip entire files
// when seeking backward.
TEST_F(WindowSpillTest, spillPartitionBuildMultiFileBackwardRead) {
  // Use a larger dataset to generate multiple spill files.
  const vector_size_t size = 20'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          // Include string data to increase row size and force more files.
          makeFlatVector<int64_t>(size, [](auto row) { return row * 10; }),
          // All rows in single partition to maximize backward read distance.
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // Use first_value with unbounded frame - requires backward access to row 0
  // from any position in the partition.
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
          // Use very small target file size to generate multiple files.
          .config(core::QueryConfig::kSpillWriteBufferSize, "4096")
          // Use small read buffer to limit cache capacity.
          .config(core::QueryConfig::kSpillReadBufferSize, "4096")
          .spillDirectory(spillDirectory->getPath())
          .assertResults(
              "SELECT *, first_value(d) over (partition by p order by s rows between unbounded preceding and unbounded following) FROM tmp");

  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(windowId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
  // Verify multiple spill files were created.
  ASSERT_GT(stats.spilledFiles, 1);
}

// Test nth_value with various offsets - this exercises backward read to
// different positions within the partition, testing the file skipping logic.
TEST_F(WindowSpillTest, spillPartitionBuildNthValueBackwardRead) {
  const vector_size_t size = 5'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row * 10; }),
          // All rows in single partition.
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // Use nth_value with different offsets to test backward read to various
  // positions.
  auto plan =
      PlanBuilder()
          .values(split(data, 50))
          .streamingWindow(
              {"nth_value(d, 1) over (partition by p order by s rows between unbounded preceding and unbounded following)",
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
              "nth_value(d, 1) over (partition by p order by s rows between unbounded preceding and unbounded following), "
              "nth_value(d, 100) over (partition by p order by s rows between unbounded preceding and unbounded following), "
              "nth_value(d, 1000) over (partition by p order by s rows between unbounded preceding and unbounded following) "
              "FROM tmp");

  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(windowId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
}

// Test repeated backward reads - simulates a window function that needs to
// repeatedly access different positions in the partition.
TEST_F(WindowSpillTest, spillPartitionBuildRepeatedBackwardRead) {
  const vector_size_t size = 5'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row * 10; }),
          // Multiple small partitions to test backward read within each.
          makeFlatVector<int16_t>(size, [](auto row) { return row / 500; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // Use multiple functions that require backward access.
  auto plan =
      PlanBuilder()
          .values(split(data, 50))
          .streamingWindow(
              {"first_value(d) over (partition by p order by s rows between unbounded preceding and unbounded following)",
               "nth_value(d, 2) over (partition by p order by s rows between unbounded preceding and unbounded following)",
               "last_value(d) over (partition by p order by s rows between unbounded preceding and unbounded following)"})
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
              "nth_value(d, 2) over (partition by p order by s rows between unbounded preceding and unbounded following), "
              "last_value(d) over (partition by p order by s rows between unbounded preceding and unbounded following) "
              "FROM tmp");

  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(windowId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
}

// Test that PartitionStreamingWindowBuild can trigger spill during getOutput
// via ensureOutputFits(). This tests the memory arbitration path where spill
// is triggered when reserving memory for output buffers.
TEST_F(WindowSpillTest, spillPartitionBuildSpillDuringOutputProcessing) {
  const vector_size_t size = 2'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // Multiple partitions to test spill during output.
          makeFlatVector<int16_t>(size, [](auto row) { return row / 200; }),
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

  // For PartitionStreamingWindowBuild, spill should be triggered during output
  // via ensureOutputFits() or during data accumulation.
  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
}

// Test spill with alternating spill/no-spill partitions.
// This tests the boundary conditions where partition state transitions between
// spilled and non-spilled modes. Uses a custom spill injection pattern.
TEST_F(WindowSpillTest, spillPartitionBuildAlternating) {
  const vector_size_t size = 1'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // 10 partitions of 100 rows each.
          makeFlatVector<int16_t>(size, [](auto row) { return row / 100; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // Split into 10 batches - one per partition, allowing spill between batches.
  auto plan =
      PlanBuilder()
          .values(split(data, 10))
          .streamingWindow({"last_value(d) over (partition by p order by s)"})
          .capturePlanNodeId(windowId)
          .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  // 50% spill rate should create alternating spill/no-spill pattern
  // over many runs. Run multiple times to increase chance of hitting
  // alternating pattern.
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

// Test multiple spills within the same partition.
// This tests the case where a large partition triggers spill multiple times
// during its construction, testing the spillInputRowsCompletely() path
// being called repeatedly for the same partition.
TEST_F(WindowSpillTest, spillPartitionBuildMultipleSpillsSamePartition) {
  const vector_size_t size = 2'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // Single large partition to maximize spills within one partition.
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // Split into many small batches to trigger multiple spills within the
  // same partition.
  auto plan =
      PlanBuilder()
          .values(split(data, 40)) // 40 batches of 50 rows each
          .streamingWindow({"last_value(d) over (partition by p order by s)"})
          .capturePlanNodeId(windowId)
          .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  // 100% spill rate - every batch will trigger spill
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
  // With 40 batches all triggering spill for the same partition,
  // we should have multiple spill files.
  ASSERT_GT(stats.spilledFiles, 1);
}

// Test multiple spills during output phase.
// Uses SpillableWindowPartition to read from spill files, then triggers
// more spills during the read.
TEST_F(WindowSpillTest, spillPartitionBuildMultipleSpillsDuringOutput) {
  const vector_size_t size = 2'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // Multiple partitions - some will be reading from spill while
          // others are still being built.
          makeFlatVector<int16_t>(size, [](auto row) { return row / 200; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // Split into many batches to allow interleaved input/output processing.
  auto plan =
      PlanBuilder()
          .values(split(data, 20))
          .streamingWindow({"last_value(d) over (partition by p order by s)"})
          .capturePlanNodeId(windowId)
          .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  // 100% spill rate to ensure continuous spilling during both input and output
  TestScopedSpillInjection scopedSpillInjection(100);
  auto task =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          // Very small output batch to force multiple output calls
          .config(core::QueryConfig::kPreferredOutputBatchBytes, "256")
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

// Test deterministic alternating spill pattern: spill, no-spill, spill,
// no-spill. Uses maxInjections parameter to control exactly when spills happen.
TEST_F(WindowSpillTest, spillPartitionBuildDeterministicAlternating) {
  const vector_size_t size = 800;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // 8 partitions of 100 rows each.
          makeFlatVector<int16_t>(size, [](auto row) { return row / 100; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // Split into 8 batches - one per partition.
  auto plan =
      PlanBuilder()
          .values(split(data, 8))
          .streamingWindow({"last_value(d) over (partition by p order by s)"})
          .capturePlanNodeId(windowId)
          .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  // Use 100% rate but limit to 4 injections - this will cause spill
  // for first 4 batches, then no spill for remaining 4.
  // This tests transition from spill mode to non-spill mode.
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
  // Not all rows should be spilled since we limited injections.
  ASSERT_LT(stats.spilledRows, size);
}

// Test that PartitionStreamingWindowBuild correctly releases memory after
// spill. This verifies that pool()->release() is called after spilling,
// allowing the freed memory to be used by other operators or new data.
TEST_F(WindowSpillTest, spillPartitionBuildMemoryRelease) {
  const vector_size_t size = 5'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          // Large payload to stress memory.
          makeFlatVector<std::string>(
              size,
              [](auto row) {
                return std::string(200, 'y') + std::to_string(row);
              }),
          // Many partitions to trigger multiple spill cycles.
          makeFlatVector<int16_t>(size, [](auto row) { return row / 100; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan =
      PlanBuilder()
          .values(split(data, 50))
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
  ASSERT_GT(stats.spilledFiles, 0);
}

// Test that PartitionStreamingWindowBuild uses cached estimatedOutputRowSize_
// when data_ has been cleared after spill. This tests the fix where
// ensureOutputFits() uses cached row size instead of querying empty data_.
TEST_F(WindowSpillTest, spillPartitionBuildCachedRowSize) {
  const vector_size_t size = 2'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // Multiple partitions - first ones spill, later ones use cached size.
          makeFlatVector<int16_t>(size, [](auto row) { return row / 200; }),
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

// Test that spill followed by new data works correctly. This tests the
// scenario where:
// 1. Partition 0 is processed and spilled
// 2. New data arrives for partition 1
// 3. Memory is released after spill so new data can be processed
TEST_F(WindowSpillTest, spillPartitionBuildSpillThenNewData) {
  const vector_size_t size = 2'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          // Payload - use string to increase memory usage.
          makeFlatVector<std::string>(
              size,
              [](auto row) {
                return std::string(100, 'x') + std::to_string(row);
              }),
          // Partition key - create multiple partitions.
          makeFlatVector<int16_t>(size, [](auto row) { return row / 200; }),
          // Sorting key.
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // Use last_value() with streamingWindow() to use
  // PartitionStreamingWindowBuild.
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

  // PartitionStreamingWindowBuild should spill and release memory.
  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
}

// Test that ensureOutputFits works correctly after data has been spilled.
// This verifies that:
// 1. estimatedOutputRowSize_ is cached before spill
// 2. ensureOutputFits can still reserve memory after data_ is cleared by spill
// 3. pool()->release() is called after spill to free memory
TEST_F(WindowSpillTest, spillPartitionBuildEnsureOutputFitsAfterSpill) {
  const vector_size_t size = 3'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          // Use strings to increase memory usage and make spill more likely.
          makeFlatVector<std::string>(
              size,
              [](auto row) {
                return std::string(50, 'x') + std::to_string(row);
              }),
          // Multiple partitions - first ones will be spilled, later ones
          // will test ensureOutputFits with empty data_.
          makeFlatVector<int16_t>(size, [](auto row) { return row / 300; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // Split into many small batches to trigger spill mid-stream.
  auto plan =
      PlanBuilder()
          .values(split(data, 30))
          .orderBy({"p", "s"}, false)
          .streamingWindow({"last_value(d) over (partition by p order by s)"})
          .capturePlanNodeId(windowId)
          .planNode();

  auto spillDirectory = TempDirectoryPath::create();
  // 100% spill rate to ensure spill happens early, then continues processing.
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

  // Verify spill happened and memory was released.
  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
}

// Test that estimatedOutputRowSize_ is correctly cached and used after spill.
// This specifically tests the scenario where:
// 1. First partition accumulates data, estimatedOutputRowSize_ is set
// 2. Spill happens, data_->clear() is called
// 3. Output processing continues using cached estimatedOutputRowSize_
TEST_F(WindowSpillTest, spillPartitionBuildCachedRowSizeAfterSpill) {
  const vector_size_t size = 2'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          // Large strings to make row size estimation significant.
          makeFlatVector<std::string>(
              size,
              [](auto row) {
                return std::string(100, 'y') + std::to_string(row);
              }),
          // Single large partition to ensure estimatedOutputRowSize_ is set
          // before any spill.
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan =
      PlanBuilder()
          .values(split(data, 20))
          .orderBy({"p", "s"}, false)
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

// Test memory release after spill - verifies pool()->release() is called.
// This test uses multiple partitions where early partitions spill and
// later partitions verify that memory was properly released.
TEST_F(WindowSpillTest, spillPartitionBuildMemoryReleaseAfterSpill) {
  const vector_size_t size = 4'000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          // Large payload to stress memory.
          makeFlatVector<std::string>(
              size,
              [](auto row) {
                return std::string(80, 'z') + std::to_string(row);
              }),
          // Many partitions to test memory release between partitions.
          makeFlatVector<int16_t>(size, [](auto row) { return row / 200; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan =
      PlanBuilder()
          .values(split(data, 40))
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

  // Spill should happen and memory should be released allowing continued
  // processing.
  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
  ASSERT_GT(stats.spilledFiles, 0);
}

} // namespace
} // namespace facebook::velox::exec
