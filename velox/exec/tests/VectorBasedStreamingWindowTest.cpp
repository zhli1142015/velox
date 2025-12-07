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

/// Tests for VectorBasedWindowPartition and VectorRowsStreamingWindowBuild.
/// These tests cover various code paths including:
/// - extractColumn (contiguous and random access)
/// - extractNulls
/// - arePeers (same block and cross-block comparison)
/// - computePeerBuffers
/// - addRows / removeProcessedRows
/// - findBlock (binary search across multiple blocks)
/// - Cross-batch partition and peer group detection
/// - Edge cases: empty partitions, single row, NULL values
///
/// Note: streamingWindow requires input to be pre-sorted by partition and
/// order by keys. Tests use orderBy() before streamingWindow() to ensure this.

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::exec {

namespace {

class VectorBasedStreamingWindowTest : public OperatorTestBase {
 public:
  void SetUp() override {
    OperatorTestBase::SetUp();
    window::prestosql::registerAllWindowFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
  }

  /// Helper to run streaming window query with pre-sorted data.
  /// Data is first sorted by partition keys then by order keys.
  /// orderByKeys should NOT include frame clause. Use windowFunction for that.
  void runStreamingWindowTest(
      const RowVectorPtr& data,
      const std::string& windowFunction,
      const std::vector<std::string>& partitionKeys,
      const std::vector<std::string>& orderByKeys) {
    createDuckDbTable({data});

    // Build the ORDER BY clause for sorting (partition keys first, then order
    // keys) Extract just the column name from orderByKey (remove ASC/DESC/NULLS
    // modifiers)
    std::vector<std::string> sortKeys;
    for (const auto& pk : partitionKeys) {
      sortKeys.push_back(pk);
    }
    for (const auto& ok : orderByKeys) {
      // Extract column name (first word) for sorting
      auto spacePos = ok.find(' ');
      if (spacePos != std::string::npos) {
        sortKeys.push_back(ok.substr(0, spacePos));
      } else {
        sortKeys.push_back(ok);
      }
    }

    // Build the OVER clause
    std::string overClause;
    if (!partitionKeys.empty()) {
      overClause = "partition by " + folly::join(", ", partitionKeys);
    }
    if (!orderByKeys.empty()) {
      if (!overClause.empty()) {
        overClause += " ";
      }
      overClause += "order by " + folly::join(", ", orderByKeys);
    }

    auto windowExpr = fmt::format("{} over ({})", windowFunction, overClause);

    core::PlanNodeId windowId;
    auto plan = PlanBuilder()
                    .values({data})
                    .orderBy(sortKeys, false)
                    .streamingWindow({windowExpr})
                    .capturePlanNodeId(windowId)
                    .planNode();

    auto duckQuery = fmt::format(
        "SELECT *, {} over ({}) FROM tmp", windowFunction, overClause);

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
        .assertResults(duckQuery);
  }
};

// =============================================================================
// Basic Functionality Tests
// =============================================================================

/// Test basic row_number with multiple partitions.
TEST_F(VectorBasedStreamingWindowTest, basicRowNumber) {
  const vector_size_t size = 500;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row % 5; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  runStreamingWindowTest(data, "row_number()", {"p"}, {"s"});
}

/// Test rank function which uses peer groups.
TEST_F(VectorBasedStreamingWindowTest, rankWithPeerGroups) {
  const vector_size_t size = 200;
  // Create data with duplicate sort keys to form peer groups
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row % 4; }),
          // Sort key has duplicates: 0,0,1,1,2,2,3,3,...
          makeFlatVector<int32_t>(size, [](auto row) { return row / 2; }),
      });

  runStreamingWindowTest(data, "rank()", {"p"}, {"s"});
  runStreamingWindowTest(data, "dense_rank()", {"p"}, {"s"});
}

/// Test sum aggregation with default frame (UNBOUNDED PRECEDING to CURRENT
/// ROW).
TEST_F(VectorBasedStreamingWindowTest, sumAggregation) {
  const vector_size_t size = 300;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row % 100; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row % 3; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan = PlanBuilder()
                  .values({data})
                  .orderBy({"p", "s"}, false)
                  .streamingWindow(
                      {"sum(d) over (partition by p order by s "
                       "rows between unbounded preceding and current row)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
      .assertResults(
          "SELECT *, sum(d) over (partition by p order by s "
          "rows between unbounded preceding and current row) FROM tmp");
}

// =============================================================================
// Cross-Batch Boundary Tests
// =============================================================================

/// Test partition boundary crossing multiple batches.
TEST_F(VectorBasedStreamingWindowTest, crossBatchPartitionBoundary) {
  // Create data where partition boundary falls exactly at batch boundary
  const vector_size_t size = 100;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // Partition changes at row 50
          makeFlatVector<int16_t>(
              size, [](auto row) { return row < 50 ? 0 : 1; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  runStreamingWindowTest(data, "row_number()", {"p"}, {"s"});
}

/// Test peer group spanning multiple batches.
TEST_F(VectorBasedStreamingWindowTest, crossBatchPeerGroup) {
  const vector_size_t size = 100;
  // All rows have the same sort key value - one giant peer group
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(
              size, [](auto /*row*/) { return 0; }), // Same value
      });

  runStreamingWindowTest(data, "rank()", {"p"}, {"s"});
}

// =============================================================================
// Multiple Blocks and findBlock Tests
// =============================================================================

/// Test multiple blocks within a single partition (tests findBlock binary
/// search).
TEST_F(VectorBasedStreamingWindowTest, multipleBlocksInPartition) {
  // Create large single partition that will span multiple blocks
  const vector_size_t size = 10000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(
              size, [](auto /*row*/) { return 0; }), // Single partition
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  runStreamingWindowTest(data, "row_number()", {"p"}, {"s"});
}

/// Test extractColumn spanning multiple blocks.
TEST_F(VectorBasedStreamingWindowTest, extractColumnAcrossBlocks) {
  const vector_size_t size = 500;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row * 2; }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan = PlanBuilder()
                  .values({data})
                  .orderBy({"p", "s"}, false)
                  .streamingWindow(
                      {"sum(d) over (partition by p order by s "
                       "rows between unbounded preceding and current row)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
      .assertResults(
          "SELECT *, sum(d) over (partition by p order by s "
          "rows between unbounded preceding and current row) FROM tmp");
}

/// Test extractColumn optimization: batch processing across multiple blocks.
/// This test verifies that the optimized extractColumn correctly handles
/// data spanning multiple blocks without calling findBlock for each row.
TEST_F(VectorBasedStreamingWindowTest, extractColumnMultiBlockOptimization) {
  // Create a large dataset that will span multiple blocks
  // Use small batch size to ensure multiple blocks are created
  const vector_size_t size = 2000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(
              size, [](auto /*row*/) { return 0; }), // Single partition
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // Use first_value which requires extractColumn across blocks
  auto plan =
      PlanBuilder()
          .values({data})
          .orderBy({"p", "s"}, false)
          .streamingWindow({"first_value(d) over (partition by p order by s "
                            "rows between 100 preceding and current row)"})
          .capturePlanNodeId(windowId)
          .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kPreferredOutputBatchBytes, "512")
      .assertResults(
          "SELECT *, first_value(d) over (partition by p order by s "
          "rows between 100 preceding and current row) FROM tmp");
}

/// Test extractNulls optimization across multiple blocks.
/// Verifies that NULL extraction works correctly when data spans blocks.
TEST_F(VectorBasedStreamingWindowTest, extractNullsMultiBlockOptimization) {
  const vector_size_t size = 1000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          // Create data with NULLs scattered throughout
          makeFlatVector<int64_t>(
              size,
              [](auto row) { return row; },
              [](auto row) { return row % 7 == 0; }), // NULLs every 7th row
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // count(d) will use extractNulls to handle NULL values
  auto plan = PlanBuilder()
                  .values({data})
                  .orderBy({"p", "s"}, false)
                  .streamingWindow(
                      {"count(d) over (partition by p order by s "
                       "rows between unbounded preceding and current row)",
                       "sum(d) over (partition by p order by s "
                       "rows between unbounded preceding and current row)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kPreferredOutputBatchBytes, "512")
      .assertResults(
          "SELECT *, "
          "count(d) over (partition by p order by s rows between unbounded preceding and current row), "
          "sum(d) over (partition by p order by s rows between unbounded preceding and current row) "
          "FROM tmp");
}

/// Test extractColumn with random access (rowNumbers variant).
/// This tests the second extractColumn overload that takes row numbers.
TEST_F(VectorBasedStreamingWindowTest, extractColumnRandomAccess) {
  const vector_size_t size = 500;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row * 3; }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // nth_value uses random access extractColumn
  auto plan = PlanBuilder()
                  .values({data})
                  .orderBy({"p", "s"}, false)
                  .streamingWindow(
                      {"nth_value(d, 5) over (partition by p order by s "
                       "rows between unbounded preceding and current row)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
      .assertResults(
          "SELECT *, nth_value(d, 5) over (partition by p order by s "
          "rows between unbounded preceding and current row) FROM tmp");
}

/// Test nth_value with offset larger than frame size (produces NULLs).
/// This exercises the kNullRow handling in extractColumn(rowNumbers).
TEST_F(VectorBasedStreamingWindowTest, extractColumnWithNullRows) {
  const vector_size_t size = 100;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // nth_value with large offset will produce NULL for early rows
  // because the frame doesn't have enough rows yet.
  // e.g., for row 3, frame is [0,3], nth_value(d, 10) returns NULL
  auto plan = PlanBuilder()
                  .values({data})
                  .orderBy({"p", "s"}, false)
                  .streamingWindow(
                      {"nth_value(d, 10) over (partition by p order by s "
                       "rows between unbounded preceding and current row)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kPreferredOutputBatchBytes, "512")
      .assertResults(
          "SELECT *, nth_value(d, 10) over (partition by p order by s "
          "rows between unbounded preceding and current row) FROM tmp");
}

/// Test extractColumn spanning exactly at block boundaries.
TEST_F(VectorBasedStreamingWindowTest, extractColumnBlockBoundary) {
  // Create multiple small batches to ensure block boundaries are hit
  std::vector<RowVectorPtr> batches;
  for (int batch = 0; batch < 10; ++batch) {
    const vector_size_t batchSize = 100;
    auto batchData = makeRowVector(
        {"d", "p", "s"},
        {
            makeFlatVector<int64_t>(
                batchSize, [batch](auto row) { return batch * 100 + row; }),
            makeFlatVector<int16_t>(batchSize, [](auto /*row*/) { return 0; }),
            makeFlatVector<int32_t>(
                batchSize, [batch](auto row) { return batch * 100 + row; }),
        });
    batches.push_back(batchData);
  }

  // Combine all batches for DuckDB
  auto allData = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(1000, [](auto row) { return row; }),
          makeFlatVector<int16_t>(1000, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(1000, [](auto row) { return row; }),
      });
  createDuckDbTable({allData});

  core::PlanNodeId windowId;
  auto plan = PlanBuilder()
                  .values(batches)
                  .orderBy({"p", "s"}, false)
                  .streamingWindow(
                      {"sum(d) over (partition by p order by s "
                       "rows between unbounded preceding and current row)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kPreferredOutputBatchBytes, "512")
      .assertResults(
          "SELECT *, sum(d) over (partition by p order by s "
          "rows between unbounded preceding and current row) FROM tmp");
}

// =============================================================================
// removeProcessedRows Tests
// =============================================================================

/// Test incremental processing with removeProcessedRows.
TEST_F(VectorBasedStreamingWindowTest, incrementalProcessing) {
  // Large partition to trigger multiple output batches and removeProcessedRows
  const vector_size_t size = 50000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  auto queryCtx = core::QueryCtx::create(executor_.get());

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan = PlanBuilder()
                  .values({data})
                  .orderBy({"p", "s"}, false)
                  .streamingWindow(
                      {"sum(d) over (partition by p order by s "
                       "rows between unbounded preceding and current row)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  auto task = AssertQueryBuilder(plan, duckDbQueryRunner_)
                  .queryCtx(queryCtx)
                  .config(core::QueryConfig::kPreferredOutputBatchBytes, "4096")
                  .assertResults(
                      "SELECT *, sum(d) over (partition by p order by s "
                      "rows between unbounded preceding and current row) "
                      "FROM tmp");

  // Verify memory is bounded (removeProcessedRows is working)
  const auto peakBytes = queryCtx->pool()->peakBytes();
  ASSERT_LT(peakBytes, 20 * 1024 * 1024)
      << "Peak memory too high: " << peakBytes << " bytes";
}

/// Test partial block removal in removeProcessedRows.
TEST_F(VectorBasedStreamingWindowTest, partialBlockRemoval) {
  // Create data that will cause partial block removal
  const vector_size_t size = 1000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // Multiple partitions of varying sizes
          makeFlatVector<int16_t>(
              size,
              [](auto row) {
                if (row < 150)
                  return (int16_t)0;
                if (row < 400)
                  return (int16_t)1;
                if (row < 550)
                  return (int16_t)2;
                return (int16_t)3;
              }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  runStreamingWindowTest(data, "row_number()", {"p"}, {"s"});
}

// =============================================================================
// NULL Value Handling Tests
// =============================================================================

/// Test NULL values in partition key.
TEST_F(VectorBasedStreamingWindowTest, nullPartitionKey) {
  const vector_size_t size = 100;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // Some NULL partition keys
          makeFlatVector<int16_t>(
              size,
              [](auto row) { return row % 3; },
              [](auto row) { return row % 7 == 0; }), // NULL every 7th row
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  runStreamingWindowTest(data, "row_number()", {"p"}, {"s"});
}

/// Test NULL values in sort key.
TEST_F(VectorBasedStreamingWindowTest, nullSortKey) {
  const vector_size_t size = 100;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row % 2; }),
          // Some NULL sort keys - use modulo 11 to create sparse NULLs
          makeFlatVector<int32_t>(
              size,
              [](auto row) { return row; },
              [](auto row) { return row % 11 == 0; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // Use rank() instead of row_number() since row_number() is non-deterministic
  // for tied/peer rows (same sort key value). rank() gives the same rank to
  // all peers, making results deterministic.
  auto plan = PlanBuilder()
                  .values({data})
                  .orderBy({"p ASC NULLS LAST", "s ASC NULLS LAST"}, false)
                  .streamingWindow({"rank() over (partition by p order by s)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
      .assertResults(
          "SELECT *, rank() over (partition by p order by s) FROM tmp");
}

/// Test NULL values in aggregation input.
TEST_F(VectorBasedStreamingWindowTest, nullAggregationInput) {
  const vector_size_t size = 200;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          // Some NULL data values
          makeFlatVector<int64_t>(
              size,
              [](auto row) { return row; },
              [](auto row) { return row % 5 == 0; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row % 3; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan = PlanBuilder()
                  .values({data})
                  .orderBy({"p", "s"}, false)
                  .streamingWindow(
                      {"sum(d) over (partition by p order by s "
                       "rows between unbounded preceding and current row)",
                       "count(d) over (partition by p order by s "
                       "rows between unbounded preceding and current row)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
      .assertResults(
          "SELECT *, "
          "sum(d) over (partition by p order by s rows between unbounded preceding and current row), "
          "count(d) over (partition by p order by s rows between unbounded preceding and current row) "
          "FROM tmp");
}

// =============================================================================
// Edge Cases and Boundary Conditions
// =============================================================================

/// Test empty partition keys (entire data is one partition).
TEST_F(VectorBasedStreamingWindowTest, emptyPartitionKey) {
  const vector_size_t size = 500;
  auto data = makeRowVector(
      {"d", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan = PlanBuilder()
                  .values({data})
                  .orderBy({"s"}, false)
                  .streamingWindow({"row_number() over (order by s)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
      .assertResults("SELECT *, row_number() over (order by s) FROM tmp");
}

/// Test single row partitions.
TEST_F(VectorBasedStreamingWindowTest, singleRowPartitions) {
  const vector_size_t size = 100;
  // Each row is its own partition
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(
              size, [](auto row) { return row; }), // Unique partition key
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  runStreamingWindowTest(data, "row_number()", {"p"}, {"s"});

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan = PlanBuilder()
                  .values({data})
                  .orderBy({"p", "s"}, false)
                  .streamingWindow(
                      {"sum(d) over (partition by p order by s "
                       "rows between unbounded preceding and current row)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
      .assertResults(
          "SELECT *, sum(d) over (partition by p order by s "
          "rows between unbounded preceding and current row) FROM tmp");
}

/// Test single row in entire dataset.
TEST_F(VectorBasedStreamingWindowTest, singleRow) {
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>({42}),
          makeFlatVector<int16_t>({1}),
          makeFlatVector<int32_t>({100}),
      });

  runStreamingWindowTest(data, "row_number()", {"p"}, {"s"});
}

/// Test large partitions that exceed output batch size.
TEST_F(VectorBasedStreamingWindowTest, largePartitions) {
  const vector_size_t size = 20000;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // Only 2 partitions, each with 10000 rows
          makeFlatVector<int16_t>(
              size, [](auto row) { return row < 10000 ? 0 : 1; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  runStreamingWindowTest(data, "row_number()", {"p"}, {"s"});
}

// =============================================================================
// Data Type Tests
// =============================================================================

/// Test with string columns.
TEST_F(VectorBasedStreamingWindowTest, stringColumns) {
  const vector_size_t size = 200;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<std::string>(
              size, [](auto row) { return fmt::format("data_{}", row); }),
          makeFlatVector<std::string>(
              size,
              [](auto row) { return fmt::format("partition_{}", row % 5); }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  runStreamingWindowTest(data, "row_number()", {"p"}, {"s"});
}

/// Test with multiple data types.
TEST_F(VectorBasedStreamingWindowTest, mixedDataTypes) {
  const vector_size_t size = 300;
  auto data = makeRowVector(
      {"a", "b", "c", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<double>(size, [](auto row) { return row * 1.5; }),
          makeFlatVector<std::string>(
              size, [](auto row) { return fmt::format("str_{}", row); }),
          makeFlatVector<int16_t>(size, [](auto row) { return row % 4; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan =
      PlanBuilder()
          .values({data})
          .orderBy({"p", "s"}, false)
          .streamingWindow(
              {"row_number() over (partition by p order by s)",
               "sum(a) over (partition by p order by s rows between unbounded preceding and current row)"})
          .capturePlanNodeId(windowId)
          .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
      .assertResults(
          "SELECT *, "
          "row_number() over (partition by p order by s), "
          "sum(a) over (partition by p order by s rows between unbounded preceding and current row) "
          "FROM tmp");
}

// =============================================================================
// Multiple Window Functions Tests
// =============================================================================

/// Test multiple window functions in same query.
TEST_F(VectorBasedStreamingWindowTest, multipleWindowFunctions) {
  const vector_size_t size = 200;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row % 50; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row % 4; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan =
      PlanBuilder()
          .values({data})
          .orderBy({"p", "s"}, false)
          .streamingWindow(
              {"row_number() over (partition by p order by s)",
               "rank() over (partition by p order by s)",
               "sum(d) over (partition by p order by s rows between unbounded preceding and current row)"})
          .capturePlanNodeId(windowId)
          .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
      .assertResults(
          "SELECT *, "
          "row_number() over (partition by p order by s), "
          "rank() over (partition by p order by s), "
          "sum(d) over (partition by p order by s rows between unbounded preceding and current row) "
          "FROM tmp");
}

// =============================================================================
// Sort Order Tests
// =============================================================================

/// Test descending sort order.
TEST_F(VectorBasedStreamingWindowTest, descendingSortOrder) {
  const vector_size_t size = 200;
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row % 4; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan = PlanBuilder()
                  .values({data})
                  .orderBy({"p", "s desc"}, false)
                  .streamingWindow(
                      {"row_number() over (partition by p order by s desc)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
      .assertResults(
          "SELECT *, row_number() over (partition by p order by s desc) FROM tmp");
}

/// Test multiple sort keys.
TEST_F(VectorBasedStreamingWindowTest, multipleSortKeys) {
  const vector_size_t size = 200;
  auto data = makeRowVector(
      {"d", "p", "s1", "s2"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row % 4; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row / 10; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row % 10; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan = PlanBuilder()
                  .values({data})
                  .orderBy({"p", "s1", "s2"}, false)
                  .streamingWindow(
                      {"row_number() over (partition by p order by s1, s2)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
      .assertResults(
          "SELECT *, row_number() over (partition by p order by s1, s2) FROM tmp");
}

/// Test multiple partition keys.
TEST_F(VectorBasedStreamingWindowTest, multiplePartitionKeys) {
  const vector_size_t size = 300;
  auto data = makeRowVector(
      {"d", "p1", "p2", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row % 3; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row % 5; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan = PlanBuilder()
                  .values({data})
                  .orderBy({"p1", "p2", "s"}, false)
                  .streamingWindow(
                      {"row_number() over (partition by p1, p2 order by s)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
      .assertResults(
          "SELECT *, row_number() over (partition by p1, p2 order by s) FROM tmp");
}

// =============================================================================
// arePeers Tests (cross-block peer comparison)
// =============================================================================

/// Test arePeers with peers spanning multiple blocks.
TEST_F(VectorBasedStreamingWindowTest, peersAcrossMultipleBlocks) {
  const vector_size_t size = 500;
  // Create peer groups that span block boundaries
  auto data = makeRowVector(
      {"d", "p", "s"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto /*row*/) { return 0; }),
          // Large peer groups: s = 0 for first 100 rows, then 1 for next 100,
          // etc.
          makeFlatVector<int32_t>(size, [](auto row) { return row / 100; }),
      });

  runStreamingWindowTest(data, "rank()", {"p"}, {"s"});
}

// =============================================================================
// Encoding Tests (Dictionary, Constant)
// =============================================================================

/// Test with dictionary encoded vectors.
TEST_F(VectorBasedStreamingWindowTest, dictionaryEncodedInput) {
  const vector_size_t size = 200;

  // Create base vectors
  auto baseData = makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
  auto basePartition = makeFlatVector<int16_t>({0, 0, 1, 1, 2, 2, 3, 3, 4, 4});
  auto baseSort = makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

  // Create dictionary indices
  auto indices = allocateIndices(size, pool());
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < size; ++i) {
    rawIndices[i] = i % 10;
  }

  // Create dictionary encoded vectors
  auto dictData =
      BaseVector::wrapInDictionary(nullptr, indices, size, baseData);
  auto dictPartition =
      BaseVector::wrapInDictionary(nullptr, indices, size, basePartition);
  auto dictSort =
      BaseVector::wrapInDictionary(nullptr, indices, size, baseSort);

  auto data =
      makeRowVector({"d", "p", "s"}, {dictData, dictPartition, dictSort});

  runStreamingWindowTest(data, "row_number()", {"p"}, {"s"});
}

// =============================================================================
// K-Range Frame Tests (searchFrameValue, computeKRangeFrameBounds)
// =============================================================================

/// Test RANGE frame with column-based preceding/following bounds.
/// This exercises searchFrameValue() and computeKRangeFrameBounds().
/// Note: Velox requires the frame bound column to contain pre-computed
/// (orderByValue +/- offset) values, not raw offset values.
TEST_F(VectorBasedStreamingWindowTest, kRangeFrame) {
  const vector_size_t size = 100;
  // For RANGE BETWEEN 5 PRECEDING AND 5 FOLLOWING:
  // - preceding bound column should contain (s - 5)
  // - following bound column should contain (s + 5)
  auto data = makeRowVector(
      {"d", "p", "s", "bound_start", "bound_end"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row % 2; }),
          // Sort key with values 0, 1, 2, ... 99
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // Pre-computed: s - 5 (for PRECEDING)
          makeFlatVector<int64_t>(
              size, [](auto row) { return static_cast<int64_t>(row) - 5; }),
          // Pre-computed: s + 5 (for FOLLOWING)
          makeFlatVector<int64_t>(
              size, [](auto row) { return static_cast<int64_t>(row) + 5; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // RANGE BETWEEN bound_start PRECEDING AND bound_end FOLLOWING
  // Velox interprets bound_start as the actual frame start value (not offset)
  // and bound_end as the actual frame end value (not offset)
  auto plan =
      PlanBuilder()
          .values({data})
          .orderBy({"p", "s"}, false)
          .streamingWindow(
              {"sum(d) over (partition by p order by s "
               "range between bound_start preceding and bound_end following)"})
          .capturePlanNodeId(windowId)
          .planNode();

  // For comparison with DuckDB: DuckDB uses raw offset values (5), not bound
  // values. Velox uses bound_start and bound_end which contain (s-5) and (s+5).
  // The actual frame should be rows where sort_key is in [current_s - 5,
  // current_s + 5]. This is equivalent to DuckDB's "RANGE BETWEEN 5 PRECEDING
  // AND 5 FOLLOWING".
  auto result =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
          .assertResults(
              "SELECT *, sum(d) over (partition by p order by s "
              "range between 5 preceding and 5 following) FROM tmp");
}

/// Test RANGE frame with k preceding only.
/// For RANGE BETWEEN x PRECEDING AND CURRENT ROW, Velox expects the frame
/// bound column to contain (orderByValue - x).
TEST_F(VectorBasedStreamingWindowTest, kRangePrecedingOnly) {
  const vector_size_t size = 100;
  // Pre-compute frame bound: s - 10
  auto data = makeRowVector(
      {"d", "p", "s", "bound_start"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row % 3; }),
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // Pre-computed: s - 10 (for PRECEDING)
          makeFlatVector<int64_t>(
              size, [](auto row) { return static_cast<int64_t>(row) - 10; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan = PlanBuilder()
                  .values({data})
                  .orderBy({"p", "s"}, false)
                  .streamingWindow(
                      {"sum(d) over (partition by p order by s "
                       "range between bound_start preceding and current row)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  // Compare with DuckDB using equivalent offset of 10
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
      .assertResults(
          "SELECT *, sum(d) over (partition by p order by s "
          "range between 10 preceding and current row) FROM tmp");
}

/// Test RANGE frame with k following only.
/// For RANGE BETWEEN CURRENT ROW AND x FOLLOWING, Velox expects the frame
/// bound column to contain (orderByValue + x).
TEST_F(VectorBasedStreamingWindowTest, kRangeFollowingOnly) {
  const vector_size_t size = 100;
  // Pre-compute frame bound: s + 10
  auto data = makeRowVector(
      {"d", "p", "s", "bound_end"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row % 3; }),
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // Pre-computed: s + 10 (for FOLLOWING)
          makeFlatVector<int64_t>(
              size, [](auto row) { return static_cast<int64_t>(row) + 10; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan = PlanBuilder()
                  .values({data})
                  .orderBy({"p", "s"}, false)
                  .streamingWindow(
                      {"sum(d) over (partition by p order by s "
                       "range between current row and bound_end following)"})
                  .capturePlanNodeId(windowId)
                  .planNode();

  // Compare with DuckDB using equivalent offset of 10
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
      .assertResults(
          "SELECT *, sum(d) over (partition by p order by s "
          "range between current row and 10 following) FROM tmp");
}

/// Test RANGE frame with large k values spanning multiple blocks.
TEST_F(VectorBasedStreamingWindowTest, kRangeLargeSpan) {
  const vector_size_t size = 500;
  // Pre-compute frame bounds: s - 50 and s + 50
  auto data = makeRowVector(
      {"d", "p", "s", "bound_start", "bound_end"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(
              size, [](auto /*row*/) { return 0; }), // single partition
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // Pre-computed: s - 50 (for PRECEDING)
          makeFlatVector<int64_t>(
              size, [](auto row) { return static_cast<int64_t>(row) - 50; }),
          // Pre-computed: s + 50 (for FOLLOWING)
          makeFlatVector<int64_t>(
              size, [](auto row) { return static_cast<int64_t>(row) + 50; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  // Large range span to exercise cross-block searches
  auto plan =
      PlanBuilder()
          .values({data})
          .orderBy({"p", "s"}, false)
          .streamingWindow(
              {"sum(d) over (partition by p order by s "
               "range between bound_start preceding and bound_end following)"})
          .capturePlanNodeId(windowId)
          .planNode();

  // Compare with DuckDB using equivalent offsets
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
      .assertResults(
          "SELECT *, sum(d) over (partition by p order by s "
          "range between 50 preceding and 50 following) FROM tmp");
}

/// Test RANGE frame with descending order.
TEST_F(VectorBasedStreamingWindowTest, kRangeDescending) {
  const vector_size_t size = 100;
  // For descending order, PRECEDING means higher values, FOLLOWING means lower
  // Pre-compute frame bounds: s + 5 (preceding in desc) and s - 5 (following in
  // desc)
  auto data = makeRowVector(
      {"d", "p", "s", "bound_start", "bound_end"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<int16_t>(size, [](auto row) { return row % 2; }),
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          // For DESC: PRECEDING means higher values, so bound = s + 5
          makeFlatVector<int64_t>(
              size, [](auto row) { return static_cast<int64_t>(row) + 5; }),
          // For DESC: FOLLOWING means lower values, so bound = s - 5
          makeFlatVector<int64_t>(
              size, [](auto row) { return static_cast<int64_t>(row) - 5; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId windowId;
  auto plan =
      PlanBuilder()
          .values({data})
          .orderBy({"p", "s desc"}, false)
          .streamingWindow(
              {"sum(d) over (partition by p order by s desc "
               "range between bound_start preceding and bound_end following)"})
          .capturePlanNodeId(windowId)
          .planNode();

  // Compare with DuckDB using equivalent offsets
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
      .assertResults(
          "SELECT *, sum(d) over (partition by p order by s desc "
          "range between 5 preceding and 5 following) FROM tmp");
}

} // namespace

} // namespace facebook::velox::exec
