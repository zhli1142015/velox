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

#include "velox/exec/SpillableWindowPartition.h"

#include <gtest/gtest.h>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/Spill.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::velox::exec::test {

class SpillableWindowPartitionTest : public OperatorTestBase {
 protected:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
    if (!isRegisteredVectorSerde()) {
      serializer::presto::PrestoVectorSerde::registerVectorSerde();
    }
    if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kPresto)) {
      serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
    }
  }

  void SetUp() override {
    OperatorTestBase::SetUp();
    filesystems::registerLocalFileSystem();
    spillDir_ = TempDirectoryPath::create();
    updateSpillStats_ = [](uint64_t) {};
  }

  void TearDown() override {
    spillDir_.reset();
    OperatorTestBase::TearDown();
  }

  // Creates test data with specified number of rows.
  RowVectorPtr makeTestData(vector_size_t numRows) {
    return makeRowVector(
        {"c0", "c1", "c2"},
        {
            // Data column (int64).
            makeFlatVector<int64_t>(numRows, [](auto row) { return row * 10; }),
            // Sort key column (int32).
            makeFlatVector<int32_t>(numRows, [](auto row) { return row; }),
            // String column for variety.
            makeFlatVector<StringView>(
                numRows,
                [](auto row) {
                  return StringView::makeInline(fmt::format("row_{}", row));
                }),
        });
  }

  // Creates a RowContainer matching the test data schema.
  std::unique_ptr<RowContainer> createRowContainer() {
    std::vector<TypePtr> types = {BIGINT(), INTEGER(), VARCHAR()};
    return std::make_unique<RowContainer>(types, pool_.get());
  }

  // Creates a SpillConfig for testing memory mode partition.
  common::SpillConfig makeSpillConfig() {
    return common::SpillConfig(
        [this]() -> std::string_view { return spillDir_->getPath(); },
        [](uint64_t) {},
        "test",
        0, // maxFileSize
        0, // writeBufferSize
        1 << 20, // readBufferSize
        nullptr, // executor
        0, // minSpillableReservationPct
        0, // spillableReservationGrowthPct
        0, // startPartitionBit
        0, // numPartitionBits
        0, // maxSpillLevel
        0, // maxSpillRunRows
        0, // writerFlushThresholdSize
        "none"); // compressionKind
  }

  // Creates spill files from the given data.
  // Returns the spill files and total row count.
  std::pair<SpillFiles, vector_size_t> spillDataAndCreateFiles(
      const RowVectorPtr& data,
      vector_size_t batchSize = 100) {
    const std::optional<common::PrefixSortConfig> prefixSortConfig =
        std::nullopt;
    spillState_ = std::make_unique<SpillState>(
        [this]() -> const std::string& { return spillDir_->getPath(); },
        updateSpillStats_,
        "test",
        SpillState::makeSortingKeys(
            std::vector<CompareFlags>{{true, true, false}}),
        1024 * 1024, // targetFileSize
        0, // writeBufferSize
        common::CompressionKind::CompressionKind_NONE,
        prefixSortConfig,
        pool_.get(),
        &spillStats_);

    SpillPartitionId partitionId{0};
    spillState_->setPartitionSpilled(partitionId);

    // Write data in batches to spill files.
    auto numRows = data->size();
    for (vector_size_t offset = 0; offset < numRows; offset += batchSize) {
      auto size = std::min(batchSize, numRows - offset);
      auto batch =
          std::dynamic_pointer_cast<RowVector>(data->slice(offset, size));
      spillState_->appendToPartition(partitionId, batch);
    }

    spillState_->finishFile(partitionId);

    // Get spill files.
    auto spillFiles = spillState_->finish(partitionId);

    return {std::move(spillFiles), numRows};
  }

  // Helper to create SpillableWindowPartition for testing.
  // This version takes SpillFiles directly (new API).
  std::unique_ptr<SpillableWindowPartition> createPartition(
      RowContainer* data,
      SpillFiles spillFiles,
      vector_size_t totalRows,
      uint64_t readBufferSize,
      uint64_t maxSpillCacheBytes = 1ULL << 30,
      uint64_t maxSpillCacheRows = 100000) {
    return std::make_unique<SpillableWindowPartition>(
        data,
        std::move(spillFiles),
        totalRows,
        kInputMapping,
        kSortKeyInfo,
        readBufferSize,
        maxSpillCacheBytes,
        maxSpillCacheRows,
        &spillStats_);
  }

  // Creates a memory-mode partition from test data.
  // Returns {partition, rowContainer, rows} to keep ownership.
  struct MemoryModePartition {
    std::unique_ptr<SpillableWindowPartition> partition;
    std::unique_ptr<RowContainer> rowContainer;
    std::vector<char*> rows;
    common::SpillConfig spillConfig;
  };

  MemoryModePartition createMemoryModePartition(
      const RowVectorPtr& data,
      const RowTypePtr& inputType = nullptr) {
    MemoryModePartition result;
    result.rowContainer = createRowContainer();
    result.spillConfig = makeSpillConfig();

    auto numRows = data->size();
    result.rows.reserve(numRows);
    for (vector_size_t i = 0; i < numRows; ++i) {
      auto row = result.rowContainer->newRow();
      result.rowContainer->store(DecodedVector(*data->childAt(0)), i, row, 0);
      result.rowContainer->store(DecodedVector(*data->childAt(1)), i, row, 1);
      result.rowContainer->store(DecodedVector(*data->childAt(2)), i, row, 2);
      result.rows.push_back(row);
    }

    auto type = inputType ? inputType : kInputType;
    result.partition = std::make_unique<SpillableWindowPartition>(
        result.rowContainer.get(),
        folly::Range<char**>(result.rows.data(), result.rows.size()),
        kInputMapping,
        kSortKeyInfo,
        type,
        &result.spillConfig,
        &spillStats_);

    return result;
  }

  // Verifies that extracted column values match expected data.
  template <typename T>
  void verifyColumnValues(
      const VectorPtr& result,
      const VectorPtr& expected,
      vector_size_t startRow = 0,
      const std::string& context = "") {
    auto actual = result->asFlatVector<T>();
    auto expectedFlat = expected->asFlatVector<T>();
    for (vector_size_t i = 0; i < result->size(); ++i) {
      EXPECT_EQ(actual->valueAt(i), expectedFlat->valueAt(startRow + i))
          << context << " mismatch at row " << (startRow + i);
    }
  }

  // Verifies string column values.
  void verifyStringValues(
      const VectorPtr& result,
      const VectorPtr& expected,
      vector_size_t startRow = 0) {
    auto actual = result->asFlatVector<StringView>();
    auto expectedFlat = expected->asFlatVector<StringView>();
    for (vector_size_t i = 0; i < result->size(); ++i) {
      EXPECT_EQ(
          actual->valueAt(i).str(), expectedFlat->valueAt(startRow + i).str())
          << "String mismatch at row " << (startRow + i);
    }
  }

  // Creates a spill-mode partition from test data.
  // Returns {partition, rowContainer} to keep ownership.
  struct SpillModePartition {
    std::unique_ptr<SpillableWindowPartition> partition;
    std::unique_ptr<RowContainer> rowContainer;
  };

  SpillModePartition createSpillModePartition(
      const RowVectorPtr& data,
      vector_size_t batchSize = 100,
      uint64_t readBufferSize = 1 << 20) {
    SpillModePartition result;
    result.rowContainer = createRowContainer();
    auto [spillFiles, totalRows] = spillDataAndCreateFiles(data, batchSize);
    result.partition = createPartition(
        result.rowContainer.get(),
        std::move(spillFiles),
        totalRows,
        readBufferSize);
    return result;
  }

  // Extracts column and verifies values against expected data.
  template <typename T>
  void extractAndVerifyColumn(
      SpillableWindowPartition* partition,
      column_index_t column,
      vector_size_t startRow,
      vector_size_t numRows,
      const VectorPtr& expected,
      const TypePtr& type) {
    auto result = BaseVector::create(type, numRows, pool_.get());
    partition->extractColumn(column, startRow, numRows, 0, result);
    verifyColumnValues<T>(result, expected, startRow);
  }

  // Extracts and verifies BIGINT column (most common case).
  void extractAndVerifyBigintColumn(
      SpillableWindowPartition* partition,
      vector_size_t startRow,
      vector_size_t numRows,
      const VectorPtr& expected) {
    extractAndVerifyColumn<int64_t>(
        partition, 0, startRow, numRows, expected, BIGINT());
  }

  // Tests a sliding window pattern on the given partition.
  void testSlidingWindowPattern(
      SpillableWindowPartition* partition,
      const VectorPtr& expected,
      vector_size_t numRows,
      vector_size_t batchSize,
      vector_size_t lookAhead = 0) {
    auto expectedFlat = expected->asFlatVector<int64_t>();
    for (vector_size_t batchStart = 0; batchStart < numRows;
         batchStart += batchSize) {
      auto endRow = std::min(batchStart + batchSize + lookAhead, numRows);
      auto size = endRow - batchStart;
      auto result = BaseVector::create(BIGINT(), size, pool_.get());
      partition->extractColumn(0, batchStart, size, 0, result);

      auto actual = result->asFlatVector<int64_t>();
      EXPECT_EQ(actual->valueAt(0), expectedFlat->valueAt(batchStart));
    }
  }

  // Tests lead/lag pattern on the given partition.
  void testLeadLagPattern(
      SpillableWindowPartition* partition,
      const VectorPtr& expected,
      vector_size_t numRows,
      vector_size_t batchSize,
      int32_t offset) { // positive for lead, negative for lag
    auto expectedFlat = expected->asFlatVector<int64_t>();
    auto absOffset = std::abs(offset);
    auto startBatch = offset > 0 ? 0 : absOffset;
    auto endRow = offset > 0 ? numRows - offset : numRows;

    for (vector_size_t batchStart = startBatch; batchStart < endRow;
         batchStart += batchSize) {
      auto size = std::min(batchSize, endRow - batchStart);
      std::vector<vector_size_t> rowNumbers(size);
      std::iota(rowNumbers.begin(), rowNumbers.end(), batchStart + offset);

      auto result = BaseVector::create(BIGINT(), size, pool_.get());
      partition->extractColumn(
          0,
          folly::Range<const vector_size_t*>(
              rowNumbers.data(), rowNumbers.size()),
          0,
          result);

      auto actual = result->asFlatVector<int64_t>();
      for (vector_size_t i = 0; i < size; ++i) {
        EXPECT_EQ(
            actual->valueAt(i), expectedFlat->valueAt(batchStart + offset + i));
      }
    }
  }

  // Verifies peer buffers for distinct values.
  void verifyDistinctPeerBuffers(
      const std::vector<vector_size_t>& peerStarts,
      const std::vector<vector_size_t>& peerEnds,
      vector_size_t startRow,
      vector_size_t numRows) {
    for (vector_size_t i = 0; i < numRows; ++i) {
      EXPECT_EQ(peerStarts[i], startRow + i);
      EXPECT_EQ(peerEnds[i], startRow + i);
    }
  }

  // Verifies peer buffers for grouped values (every groupSize rows same key).
  void verifyGroupedPeerBuffers(
      const std::vector<vector_size_t>& peerStarts,
      const std::vector<vector_size_t>& peerEnds,
      vector_size_t numRows,
      vector_size_t groupSize) {
    for (vector_size_t i = 0; i < numRows; ++i) {
      auto groupStart = (i / groupSize) * groupSize;
      auto groupEnd = groupStart + groupSize - 1;
      EXPECT_EQ(peerStarts[i], groupStart);
      EXPECT_EQ(peerEnds[i], groupEnd);
    }
  }

  // Standard type definitions used across tests.
  static inline const RowTypePtr kInputType =
      ROW({"c0", "c1", "c2"}, {BIGINT(), INTEGER(), VARCHAR()});
  static inline const std::vector<column_index_t> kInputMapping = {0, 1, 2};
  static inline const std::vector<std::pair<column_index_t, core::SortOrder>>
      kSortKeyInfo = {{1, core::SortOrder(true, true)}};

  std::shared_ptr<TempDirectoryPath> spillDir_;
  folly::Synchronized<common::SpillStats> spillStats_;
  std::function<void(uint64_t)> updateSpillStats_;
  std::unique_ptr<SpillState> spillState_;
};

// Test basic construction and numRows.
TEST_F(SpillableWindowPartitionTest, basicConstruction) {
  const vector_size_t numRows = 1000;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data);

  EXPECT_EQ(sp.partition->numRows(), numRows);
  EXPECT_TRUE(sp.partition->isSpilled());
}

// Test loading rows sequentially without eviction.
TEST_F(SpillableWindowPartitionTest, loadRowsSequentially) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data);

  extractAndVerifyBigintColumn(
      sp.partition.get(), 0, numRows, data->childAt(0));
}

// Test loading rows in chunks.
TEST_F(SpillableWindowPartitionTest, loadRowsInChunks) {
  const vector_size_t numRows = 500;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 50);

  // Extract and verify rows in chunks.
  const vector_size_t chunkSize = 100;
  for (vector_size_t offset = 0; offset < numRows; offset += chunkSize) {
    auto size = std::min(chunkSize, numRows - offset);
    extractAndVerifyBigintColumn(
        sp.partition.get(), offset, size, data->childAt(0));
  }
}

// Test backward access to previously loaded rows.
TEST_F(SpillableWindowPartitionTest, backwardAccess) {
  const vector_size_t numRows = 500;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 50);

  // Load first 100 rows.
  auto result1 = BaseVector::create(BIGINT(), 100, pool_.get());
  sp.partition->extractColumn(0, 0, 100, 0, result1);

  // Load next 100 rows.
  sp.partition->extractColumn(0, 100, 100, 0, result1);

  // Backward access - should succeed (resets spill stream).
  extractAndVerifyBigintColumn(sp.partition.get(), 0, 10, data->childAt(0));
}

// Test sliding window access pattern (typical for Window operator).
TEST_F(SpillableWindowPartitionTest, slidingWindowAccessPattern) {
  const vector_size_t numRows = 1000;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 100);

  // Simulate Window operator processing: process 100 rows at a time,
  // but need to look ahead 50 rows for frame computation.
  testSlidingWindowPattern(
      sp.partition.get(), data->childAt(0), numRows, 100, 50);
}

// Test with cache capacity equal to total rows.
TEST_F(SpillableWindowPartitionTest, cacheHoldsAllRows) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data);

  // Should be able to access any row without eviction.
  auto result = BaseVector::create(BIGINT(), 10, pool_.get());

  // Access first 10, last 10, middle 10.
  sp.partition->extractColumn(0, 0, 10, 0, result);
  sp.partition->extractColumn(0, numRows - 10, 10, 0, result);
  sp.partition->extractColumn(0, 45, 10, 0, result);

  // Verify last access.
  verifyColumnValues<int64_t>(result, data->childAt(0), 45);
}

// Test with string column.
TEST_F(SpillableWindowPartitionTest, stringColumn) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data);

  // Extract string column (column 2).
  auto result = BaseVector::create(VARCHAR(), numRows, pool_.get());
  sp.partition->extractColumn(2, 0, numRows, 0, result);

  verifyStringValues(result, data->childAt(2));
}

// Test computePeerBuffers with distinct values.
TEST_F(SpillableWindowPartitionTest, computePeerBuffersDistinct) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows); // All values are distinct.
  auto sp = createSpillModePartition(data);

  std::vector<vector_size_t> peerStarts(10);
  std::vector<vector_size_t> peerEnds(10);

  sp.partition->computePeerBuffers(
      0, 10, 0, 0, peerStarts.data(), peerEnds.data());

  // With distinct values, each row is its own peer group.
  verifyDistinctPeerBuffers(peerStarts, peerEnds, 0, 10);
}

// Test computePeerBuffers with duplicate values.
TEST_F(SpillableWindowPartitionTest, computePeerBuffersDuplicates) {
  const vector_size_t numRows = 100;
  // Create data where sort key has duplicates (every 5 rows have same value).
  auto dataWithDups = makeRowVector(
      {"c0", "c1", "c2"},
      {
          makeFlatVector<int64_t>(numRows, [](auto row) { return row * 10; }),
          makeFlatVector<int32_t>(numRows, [](auto row) { return row / 5; }),
          makeFlatVector<StringView>(
              numRows,
              [](auto row) {
                return StringView::makeInline(fmt::format("row_{}", row));
              }),
      });

  auto sp = createSpillModePartition(dataWithDups);

  std::vector<vector_size_t> peerStarts(15);
  std::vector<vector_size_t> peerEnds(15);

  sp.partition->computePeerBuffers(
      0, 15, 0, 0, peerStarts.data(), peerEnds.data());

  // Verify peer groups (every 5 rows have same sort key).
  verifyGroupedPeerBuffers(peerStarts, peerEnds, 15, 5);
}

// Test row_number/rank/dense_rank pattern: no data access needed.
// These functions only need the row position, not the actual data values.
TEST_F(SpillableWindowPartitionTest, rankingFunctionPattern) {
  const vector_size_t numRows = 1000;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 100);

  EXPECT_EQ(sp.partition->numRows(), numRows);

  // Process in batches, computing peer buffers.
  const vector_size_t batchSize = 100;
  vector_size_t prevPeerStart = 0;
  vector_size_t prevPeerEnd = 0;
  std::vector<vector_size_t> peerStarts(batchSize);
  std::vector<vector_size_t> peerEnds(batchSize);

  for (vector_size_t batchStart = 0; batchStart < numRows;
       batchStart += batchSize) {
    auto size = std::min(batchSize, numRows - batchStart);
    std::tie(prevPeerStart, prevPeerEnd) = sp.partition->computePeerBuffers(
        batchStart,
        batchStart + size,
        prevPeerStart,
        prevPeerEnd,
        peerStarts.data(),
        peerEnds.data());
  }
}

// Test lead(N)/lag(N) pattern: contiguous access with offset.
// Uses extractColumn with rowNumbers array that forms a contiguous sequence.
TEST_F(SpillableWindowPartitionTest, leadLagPattern) {
  const vector_size_t numRows = 500;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 50);

  // Simulate lead(20): for each row at position i, access row at i+20.
  testLeadLagPattern(sp.partition.get(), data->childAt(0), numRows, 100, 20);
}

// Test first_value(UNBOUNDED PRECEDING) pattern: only access first row.
// For first_value(UNBOUNDED PRECEDING), extract and cache the first value
// before processing.
TEST_F(SpillableWindowPartitionTest, firstValueUnboundedPattern) {
  const vector_size_t numRows = 200;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 50);

  auto expected = data->childAt(0)->asFlatVector<int64_t>();
  auto firstValue = expected->valueAt(0);

  // Extract and cache the first value BEFORE processing.
  auto firstValueResult = BaseVector::create(BIGINT(), 1, pool_.get());
  sp.partition->extractColumn(0, 0, 1, 0, firstValueResult);
  EXPECT_EQ(firstValueResult->asFlatVector<int64_t>()->valueAt(0), firstValue);

  // Process remaining rows with sliding window.
  const vector_size_t batchSize = 50;
  for (vector_size_t batchStart = batchSize; batchStart < numRows;
       batchStart += batchSize) {
    auto size = std::min(batchSize, numRows - batchStart);
    extractAndVerifyBigintColumn(
        sp.partition.get(), batchStart, size, data->childAt(0));
  }
}

// Test sum() ROWS BETWEEN K PRECEDING AND M FOLLOWING pattern.
TEST_F(SpillableWindowPartitionTest, rowsFramePattern) {
  const vector_size_t numRows = 500;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 50);

  const vector_size_t kPreceding = 10;
  const vector_size_t mFollowing = 20;
  const vector_size_t batchSize = 100;
  auto expected = data->childAt(0)->asFlatVector<int64_t>();

  for (vector_size_t batchStart = 0; batchStart < numRows;
       batchStart += batchSize) {
    auto size = std::min(batchSize, numRows - batchStart);
    auto frameStart = batchStart > kPreceding ? batchStart - kPreceding : 0;
    auto frameEnd = std::min(batchStart + size + mFollowing, numRows);
    auto frameSize = frameEnd - frameStart;

    auto result = BaseVector::create(BIGINT(), frameSize, pool_.get());
    sp.partition->extractColumn(0, frameStart, frameSize, 0, result);
    verifyColumnValues<int64_t>(result, data->childAt(0), frameStart);
  }
}

// Test sum() UNBOUNDED PRECEDING TO CURRENT ROW (running sum) pattern.
TEST_F(SpillableWindowPartitionTest, runningAggregatePattern) {
  const vector_size_t numRows = 500;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 50);

  const vector_size_t batchSize = 100;

  for (vector_size_t batchStart = 0; batchStart < numRows;
       batchStart += batchSize) {
    auto size = std::min(batchSize, numRows - batchStart);
    extractAndVerifyBigintColumn(
        sp.partition.get(), batchStart, size, data->childAt(0));
  }
}

// Test nth_value(N) pattern: access specific row within frame.
TEST_F(SpillableWindowPartitionTest, nthValuePattern) {
  const vector_size_t numRows = 500;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 50);

  const vector_size_t batchSize = 100;
  const vector_size_t nthOffset = 5; // nth_value(5)
  auto expected = data->childAt(0)->asFlatVector<int64_t>();

  for (vector_size_t batchStart = 0; batchStart < numRows;
       batchStart += batchSize) {
    auto size = std::min(batchSize, numRows - batchStart);

    for (vector_size_t i = 0; i < size && batchStart + i + nthOffset < numRows;
         ++i) {
      auto targetRow = batchStart + i + nthOffset;
      auto result = BaseVector::create(BIGINT(), 1, pool_.get());
      sp.partition->extractColumn(0, targetRow, 1, 0, result);
      EXPECT_EQ(
          result->asFlatVector<int64_t>()->valueAt(0),
          expected->valueAt(targetRow));
    }
  }
}

// Test contiguity optimization: rowNumbers array that is contiguous.
TEST_F(SpillableWindowPartitionTest, contiguousRowNumbersOptimization) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data);

  // Create contiguous rowNumbers like lead/lag does.
  std::vector<vector_size_t> rowNumbers(50);
  std::iota(rowNumbers.begin(), rowNumbers.end(), 10); // [10, 11, 12, ..., 59]

  auto result = BaseVector::create(BIGINT(), 50, pool_.get());
  sp.partition->extractColumn(
      0,
      folly::Range<const vector_size_t*>(rowNumbers.data(), rowNumbers.size()),
      0,
      result);

  verifyColumnValues<int64_t>(result, data->childAt(0), 10);
}

// Test non-contiguous rowNumbers access (rare but possible).
TEST_F(SpillableWindowPartitionTest, nonContiguousRowNumbers) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data);

  // Non-contiguous access: every other row.
  std::vector<vector_size_t> rowNumbers = {10, 12, 14, 16, 18};

  auto result = BaseVector::create(BIGINT(), 5, pool_.get());
  sp.partition->extractColumn(
      0,
      folly::Range<const vector_size_t*>(rowNumbers.data(), rowNumbers.size()),
      0,
      result);

  auto expected = data->childAt(0)->asFlatVector<int64_t>();
  auto actual = result->asFlatVector<int64_t>();
  for (size_t i = 0; i < rowNumbers.size(); ++i) {
    EXPECT_EQ(actual->valueAt(i), expected->valueAt(rowNumbers[i]));
  }
}

// Test last_value(UNBOUNDED FOLLOWING) pattern: need to access last row.
// For UNBOUNDED FOLLOWING, cache the last value when we reach the end.
TEST_F(SpillableWindowPartitionTest, lastValueUnboundedPattern) {
  const vector_size_t numRows = 200;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 50);

  auto expected = data->childAt(0)->asFlatVector<int64_t>();
  auto lastValue = expected->valueAt(numRows - 1);

  // Process in batches until we reach the last row.
  int64_t cachedLastValue = 0;
  bool lastValueCached = false;
  const vector_size_t batchSize = 50;

  for (vector_size_t batchStart = 0; batchStart < numRows;
       batchStart += batchSize) {
    auto size = std::min(batchSize, numRows - batchStart);

    // Load and process current batch.
    auto result = BaseVector::create(BIGINT(), size, pool_.get());
    sp.partition->extractColumn(0, batchStart, size, 0, result);

    // Check if this batch contains the last row.
    if (batchStart + size >= numRows && !lastValueCached) {
      auto lastResult = BaseVector::create(BIGINT(), 1, pool_.get());
      sp.partition->extractColumn(0, numRows - 1, 1, 0, lastResult);
      cachedLastValue = lastResult->asFlatVector<int64_t>()->valueAt(0);
      lastValueCached = true;
    }
  }

  EXPECT_TRUE(lastValueCached);
  EXPECT_EQ(cachedLastValue, lastValue);
}

// Test lag(N) pattern: access rows BEFORE current row.
// Unlike lead(N), lag requires keeping N preceding rows in cache.
TEST_F(SpillableWindowPartitionTest, lagPattern) {
  const vector_size_t numRows = 500;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 50);

  // Simulate lag(20): for each row at position i, access row at i-20.
  testLeadLagPattern(sp.partition.get(), data->childAt(0), numRows, 100, -20);
}

// Test CURRENT ROW to UNBOUNDED FOLLOWING frame pattern.
TEST_F(SpillableWindowPartitionTest, currentRowToUnboundedFollowingPattern) {
  const vector_size_t numRows = 200;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 50);

  auto expected = data->childAt(0)->asFlatVector<int64_t>();

  // For row i, frame is [i, numRows). Verify first and last values in frame.
  const vector_size_t batchSize = 50;
  for (vector_size_t batchStart = 0; batchStart < numRows;
       batchStart += batchSize) {
    auto size = std::min(batchSize, numRows - batchStart);

    for (vector_size_t i = 0; i < size; ++i) {
      auto currentRow = batchStart + i;
      auto frameSize = numRows - currentRow;

      auto result = BaseVector::create(BIGINT(), frameSize, pool_.get());
      sp.partition->extractColumn(0, currentRow, frameSize, 0, result);

      auto actual = result->asFlatVector<int64_t>();
      EXPECT_EQ(actual->valueAt(0), expected->valueAt(currentRow));
      EXPECT_EQ(actual->valueAt(frameSize - 1), expected->valueAt(numRows - 1));
    }
  }
}

// Test RANGE BETWEEN value PRECEDING AND value FOLLOWING frame.
TEST_F(SpillableWindowPartitionTest, rangeFramePattern) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data);

  const vector_size_t startRow = 0;
  const vector_size_t numBatchRows = 20;

  std::vector<vector_size_t> peerStarts(numBatchRows);
  std::vector<vector_size_t> peerEnds(numBatchRows);
  sp.partition->computePeerBuffers(
      startRow,
      startRow + numBatchRows,
      0,
      0,
      peerStarts.data(),
      peerEnds.data());

  // Distinct values - each row is its own peer group.
  verifyDistinctPeerBuffers(peerStarts, peerEnds, startRow, numBatchRows);

  // Test computeKRangeFrameBounds for PRECEDING start.
  std::vector<vector_size_t> frameBounds(numBatchRows);
  SelectivityVector validFrames(numBatchRows, false);

  sp.partition->computeKRangeFrameBounds(
      true,
      true,
      0,
      startRow,
      numBatchRows,
      peerStarts.data(),
      frameBounds.data(),
      validFrames);

  EXPECT_GT(validFrames.countSelected(), 0);
}

// Test UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING (entire partition frame).
TEST_F(SpillableWindowPartitionTest, unboundedToUnboundedPattern) {
  const vector_size_t numRows = 200;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 50);

  // Access entire partition and verify.
  extractAndVerifyBigintColumn(
      sp.partition.get(), 0, numRows, data->childAt(0));

  // Also test batch access with no eviction.
  const vector_size_t batchSize = 50;
  auto expected = data->childAt(0)->asFlatVector<int64_t>();
  for (vector_size_t batchStart = 0; batchStart < numRows;
       batchStart += batchSize) {
    auto batchResult = BaseVector::create(BIGINT(), numRows, pool_.get());
    sp.partition->extractColumn(0, 0, numRows, 0, batchResult);

    auto batchActual = batchResult->asFlatVector<int64_t>();
    EXPECT_EQ(batchActual->valueAt(0), expected->valueAt(0));
    EXPECT_EQ(
        batchActual->valueAt(numRows - 1), expected->valueAt(numRows - 1));
  }
}

//=============================================================================
// Memory Mode Tests (new unified design)
//=============================================================================

// Test memory mode construction and basic operations.
TEST_F(SpillableWindowPartitionTest, memoryModeBasicConstruction) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows);
  auto mp = createMemoryModePartition(data);

  EXPECT_FALSE(mp.partition->isSpilled());
  EXPECT_EQ(mp.partition->numRows(), numRows);

  auto result = BaseVector::create(BIGINT(), numRows, pool_.get());
  mp.partition->extractColumn(0, 0, numRows, 0, result);
  verifyColumnValues<int64_t>(result, data->childAt(0));
}

// Test memory mode with peer buffer computation.
TEST_F(SpillableWindowPartitionTest, memoryModeComputePeerBuffers) {
  auto data = makeTestData(50);
  auto mp = createMemoryModePartition(data);

  std::vector<vector_size_t> peerStarts(10);
  std::vector<vector_size_t> peerEnds(10);

  mp.partition->computePeerBuffers(
      0, 10, 0, 0, peerStarts.data(), peerEnds.data());

  // With distinct sort key values, each row is its own peer group.
  for (vector_size_t i = 0; i < 10; ++i) {
    EXPECT_EQ(peerStarts[i], i);
    EXPECT_EQ(peerEnds[i], i);
  }
}

// Test spill() method - transition from memory mode to spill mode.
TEST_F(SpillableWindowPartitionTest, spillMethodTransition) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows);
  auto mp = createMemoryModePartition(data);

  EXPECT_FALSE(mp.partition->isSpilled());
  EXPECT_EQ(mp.partition->numRows(), numRows);

  // Extract some data before spill.
  auto resultBefore = BaseVector::create(BIGINT(), 10, pool_.get());
  mp.partition->extractColumn(0, 0, 10, 0, resultBefore);

  mp.partition->spill();

  EXPECT_TRUE(mp.partition->isSpilled());
  EXPECT_EQ(mp.partition->numRows(), numRows);

  // Extract and verify data after spill.
  auto resultAfter = BaseVector::create(BIGINT(), numRows, pool_.get());
  mp.partition->extractColumn(0, 0, numRows, 0, resultAfter);
  verifyColumnValues<int64_t>(resultAfter, data->childAt(0));

  // Calling spill() again should be a no-op.
  mp.partition->spill();
  EXPECT_TRUE(mp.partition->isSpilled());
}

// Test that data is consistent before and after spill.
TEST_F(SpillableWindowPartitionTest, dataConsistencyAcrossSpill) {
  const vector_size_t numRows = 200;
  auto data = makeTestData(numRows);
  auto mp = createMemoryModePartition(data);

  // Extract all columns before spill.
  auto col0Before = BaseVector::create(BIGINT(), numRows, pool_.get());
  auto col1Before = BaseVector::create(INTEGER(), numRows, pool_.get());
  auto col2Before = BaseVector::create(VARCHAR(), numRows, pool_.get());

  mp.partition->extractColumn(0, 0, numRows, 0, col0Before);
  mp.partition->extractColumn(1, 0, numRows, 0, col1Before);
  mp.partition->extractColumn(2, 0, numRows, 0, col2Before);

  mp.partition->spill();

  // Extract and verify all columns after spill.
  auto col0After = BaseVector::create(BIGINT(), numRows, pool_.get());
  auto col1After = BaseVector::create(INTEGER(), numRows, pool_.get());
  auto col2After = BaseVector::create(VARCHAR(), numRows, pool_.get());

  mp.partition->extractColumn(0, 0, numRows, 0, col0After);
  mp.partition->extractColumn(1, 0, numRows, 0, col1After);
  mp.partition->extractColumn(2, 0, numRows, 0, col2After);

  verifyColumnValues<int64_t>(col0After, col0Before);
  verifyColumnValues<int32_t>(col1After, col1Before);
  verifyStringValues(col2After, col2Before);
}

// Test peer buffer computation across spill transition.
TEST_F(SpillableWindowPartitionTest, peerBufferComputationAcrossSpill) {
  const vector_size_t numRows = 50;

  // Create data with duplicates in sort key (every 5 rows have same value).
  auto dataWithDups = makeRowVector(
      {"c0", "c1", "c2"},
      {
          makeFlatVector<int64_t>(numRows, [](auto row) { return row * 10; }),
          makeFlatVector<int32_t>(numRows, [](auto row) { return row / 5; }),
          makeFlatVector<StringView>(
              numRows,
              [](auto row) {
                return StringView::makeInline(fmt::format("row_{}", row));
              }),
      });

  auto mp = createMemoryModePartition(dataWithDups);

  // Compute peer buffers before spill.
  std::vector<vector_size_t> peerStartsBefore(15);
  std::vector<vector_size_t> peerEndsBefore(15);
  mp.partition->computePeerBuffers(
      0, 15, 0, 0, peerStartsBefore.data(), peerEndsBefore.data());

  mp.partition->spill();

  // Compute peer buffers after spill.
  std::vector<vector_size_t> peerStartsAfter(15);
  std::vector<vector_size_t> peerEndsAfter(15);
  mp.partition->computePeerBuffers(
      0, 15, 0, 0, peerStartsAfter.data(), peerEndsAfter.data());

  // Verify peer buffers are consistent across spill.
  for (vector_size_t i = 0; i < 15; ++i) {
    EXPECT_EQ(peerStartsAfter[i], peerStartsBefore[i]);
    EXPECT_EQ(peerEndsAfter[i], peerEndsBefore[i]);
  }

  // Verify expected peer groups (every 5 rows have same sort key).
  for (vector_size_t i = 0; i < 5; ++i) {
    EXPECT_EQ(peerStartsAfter[i], 0);
    EXPECT_EQ(peerEndsAfter[i], 4);
  }
}

// Test access after spill.
TEST_F(SpillableWindowPartitionTest, accessAfterSpill) {
  const vector_size_t numRows = 300;
  auto data = makeTestData(numRows);
  auto mp = createMemoryModePartition(data);

  mp.partition->spill();
  EXPECT_TRUE(mp.partition->isSpilled());

  // Load first 100 rows.
  auto result1 = BaseVector::create(BIGINT(), 100, pool_.get());
  mp.partition->extractColumn(0, 0, 100, 0, result1);

  // Access rows 50-99 should work.
  extractAndVerifyBigintColumn(mp.partition.get(), 50, 50, data->childAt(0));

  // Backward access should also work.
  extractAndVerifyBigintColumn(mp.partition.get(), 0, 10, data->childAt(0));
}

// Test spill() on empty partition (edge case).
TEST_F(SpillableWindowPartitionTest, spillEmptyPartition) {
  auto rowContainer = createRowContainer();
  std::vector<char*> rows;
  auto spillConfig = makeSpillConfig();

  VELOX_ASSERT_THROW(
      std::make_unique<SpillableWindowPartition>(
          rowContainer.get(),
          folly::Range<char**>(rows.data(), rows.size()),
          kInputMapping,
          kSortKeyInfo,
          kInputType,
          &spillConfig,
          &spillStats_),
      "Rows cannot be empty");
}

// Test spill stats are updated correctly.
TEST_F(SpillableWindowPartitionTest, spillStatsUpdate) {
  auto data = makeTestData(100);
  auto mp = createMemoryModePartition(data);

  auto statsBefore = spillStats_.copy();
  mp.partition->spill();
  auto statsAfter = spillStats_.copy();

  EXPECT_GT(statsAfter.spilledBytes, statsBefore.spilledBytes);
  EXPECT_GT(statsAfter.spilledFiles, statsBefore.spilledFiles);
}

// Test isSpilled() state transitions.
TEST_F(SpillableWindowPartitionTest, isSpilledStateTransitions) {
  auto data = makeTestData(50);

  // Test memory mode partition.
  auto mp = createMemoryModePartition(data);
  EXPECT_FALSE(mp.partition->isSpilled());

  mp.partition->spill();
  EXPECT_TRUE(mp.partition->isSpilled());

  // Calling spill() again doesn't change state.
  mp.partition->spill();
  EXPECT_TRUE(mp.partition->isSpilled());

  // Test spill mode constructor - partition is already spilled.
  auto rowContainer2 = createRowContainer();
  auto [spillFiles, totalRows] = spillDataAndCreateFiles(data);
  auto spilledPartition = createPartition(
      rowContainer2.get(), std::move(spillFiles), totalRows, 1 << 20);
  EXPECT_TRUE(spilledPartition->isSpilled());
}

// Test that memory mode partition correctly delegates extractNulls.
TEST_F(SpillableWindowPartitionTest, memoryModeExtractNulls) {
  const vector_size_t numRows = 20;

  auto dataWithNulls = makeRowVector(
      {"c0", "c1", "c2"},
      {
          makeNullableFlatVector<int64_t>({1,
                                           std::nullopt,
                                           3,
                                           std::nullopt,
                                           5,
                                           6,
                                           std::nullopt,
                                           8,
                                           9,
                                           10,
                                           11,
                                           12,
                                           13,
                                           std::nullopt,
                                           15,
                                           16,
                                           17,
                                           18,
                                           std::nullopt,
                                           20}),
          makeFlatVector<int32_t>(numRows, [](auto row) { return row; }),
          makeFlatVector<StringView>(
              numRows,
              [](auto row) {
                return StringView::makeInline(fmt::format("row_{}", row));
              }),
      });

  auto mp = createMemoryModePartition(dataWithNulls);

  // Extract nulls before spill.
  auto nullsBefore = AlignedBuffer::allocate<bool>(numRows, pool_.get(), false);
  mp.partition->extractNulls(0, 0, numRows, nullsBefore);

  auto* rawNullsBefore = nullsBefore->as<uint64_t>();
  EXPECT_TRUE(bits::isBitSet(rawNullsBefore, 1));
  EXPECT_TRUE(bits::isBitSet(rawNullsBefore, 3));
  EXPECT_TRUE(bits::isBitSet(rawNullsBefore, 6));
  EXPECT_TRUE(bits::isBitSet(rawNullsBefore, 13));
  EXPECT_TRUE(bits::isBitSet(rawNullsBefore, 18));

  mp.partition->spill();

  // Extract nulls after spill - should be same.
  auto nullsAfter = AlignedBuffer::allocate<bool>(numRows, pool_.get(), false);
  mp.partition->extractNulls(0, 0, numRows, nullsAfter);

  auto* rawNullsAfter = nullsAfter->as<uint64_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    EXPECT_EQ(
        bits::isBitSet(rawNullsAfter, i), bits::isBitSet(rawNullsBefore, i));
  }
}

// Test numRowsForProcessing in memory mode.
TEST_F(SpillableWindowPartitionTest, memoryModeNumRowsForProcessing) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows);
  auto mp = createMemoryModePartition(data);

  EXPECT_EQ(mp.partition->numRowsForProcessing(0), numRows);
  EXPECT_EQ(mp.partition->numRowsForProcessing(50), numRows - 50);
  EXPECT_EQ(mp.partition->numRowsForProcessing(99), 1);

  mp.partition->spill();

  EXPECT_EQ(mp.partition->numRowsForProcessing(0), numRows);
  EXPECT_EQ(mp.partition->numRowsForProcessing(50), numRows - 50);
  EXPECT_EQ(mp.partition->numRowsForProcessing(99), 1);
}

// Test extractColumn with rowNumbers array (used for lead/lag) in memory mode.
TEST_F(SpillableWindowPartitionTest, memoryModeExtractColumnWithRowNumbers) {
  auto data = makeTestData(50);
  auto mp = createMemoryModePartition(data);

  std::vector<vector_size_t> rowNumbers = {5, 10, 15, 20, 25};
  auto result = BaseVector::create(BIGINT(), rowNumbers.size(), pool_.get());

  mp.partition->extractColumn(
      0,
      folly::Range<const vector_size_t*>(rowNumbers.data(), rowNumbers.size()),
      0,
      result);

  auto expected = data->childAt(0)->asFlatVector<int64_t>();
  auto actual = result->asFlatVector<int64_t>();
  for (size_t i = 0; i < rowNumbers.size(); ++i) {
    EXPECT_EQ(actual->valueAt(i), expected->valueAt(rowNumbers[i]));
  }
}

// Test extractColumn with rowNumbers after spill transition.
TEST_F(SpillableWindowPartitionTest, extractColumnWithRowNumbersAcrossSpill) {
  auto data = makeTestData(50);
  auto mp = createMemoryModePartition(data);

  std::vector<vector_size_t> rowNumbers = {0, 5, 10, 15, 20};
  auto resultBefore =
      BaseVector::create(BIGINT(), rowNumbers.size(), pool_.get());
  mp.partition->extractColumn(
      0,
      folly::Range<const vector_size_t*>(rowNumbers.data(), rowNumbers.size()),
      0,
      resultBefore);

  mp.partition->spill();

  auto resultAfter =
      BaseVector::create(BIGINT(), rowNumbers.size(), pool_.get());
  mp.partition->extractColumn(
      0,
      folly::Range<const vector_size_t*>(rowNumbers.data(), rowNumbers.size()),
      0,
      resultAfter);

  verifyColumnValues<int64_t>(resultAfter, resultBefore);
}

// Note: computeKRangeFrameBounds tests are covered by rangeFramePattern test
// which tests the full integration with proper frame column setup.

// Test sliding window processing pattern with spill during processing.
TEST_F(SpillableWindowPartitionTest, slidingWindowWithMidProcessingSpill) {
  const vector_size_t numRows = 200;
  auto data = makeTestData(numRows);
  auto mp = createMemoryModePartition(data);

  const vector_size_t batchSize = 20;

  // Process first half in memory mode.
  for (vector_size_t batchStart = 0; batchStart < 100;
       batchStart += batchSize) {
    extractAndVerifyBigintColumn(
        mp.partition.get(), batchStart, batchSize, data->childAt(0));
  }

  EXPECT_FALSE(mp.partition->isSpilled());
  mp.partition->spill();
  EXPECT_TRUE(mp.partition->isSpilled());

  // Continue processing second half in spill mode.
  for (vector_size_t batchStart = 100; batchStart < numRows;
       batchStart += batchSize) {
    auto size = std::min(batchSize, numRows - batchStart);
    extractAndVerifyBigintColumn(
        mp.partition.get(), batchStart, size, data->childAt(0));
  }
}

// Test spill with string data consistency (complex type).
TEST_F(SpillableWindowPartitionTest, spillStringDataConsistency) {
  const vector_size_t numRows = 100;

  // Create data with longer strings to test string handling.
  // Use std::string instead of StringView::makeInline for strings > 12 bytes.
  auto dataWithStrings = makeRowVector(
      {"c0", "c1", "c2"},
      {
          makeFlatVector<int64_t>(numRows, [](auto row) { return row * 10; }),
          makeFlatVector<int32_t>(numRows, [](auto row) { return row; }),
          makeFlatVector<std::string>(
              numRows,
              [](auto row) {
                return fmt::format(
                    "row_{}_with_longer_string_data_{}", row, row * 100);
              }),
      });

  auto mp = createMemoryModePartition(dataWithStrings);

  // Extract strings before spill.
  auto stringsBefore = BaseVector::create(VARCHAR(), numRows, pool_.get());
  mp.partition->extractColumn(2, 0, numRows, 0, stringsBefore);

  mp.partition->spill();

  // Extract and verify strings after spill.
  auto stringsAfter = BaseVector::create(VARCHAR(), numRows, pool_.get());
  mp.partition->extractColumn(2, 0, numRows, 0, stringsAfter);

  verifyStringValues(stringsAfter, stringsBefore);
}

//=============================================================================
// Extreme Value Tests
//=============================================================================

// Test single row partition - minimum possible partition size.
TEST_F(SpillableWindowPartitionTest, singleRowPartition) {
  const vector_size_t numRows = 1;
  auto data = makeTestData(numRows);

  // Test spill mode with single row.
  {
    auto sp = createSpillModePartition(data, 1);
    EXPECT_EQ(sp.partition->numRows(), 1);

    auto result = BaseVector::create(BIGINT(), 1, pool_.get());
    sp.partition->extractColumn(0, 0, 1, 0, result);
    verifyColumnValues<int64_t>(result, data->childAt(0));

    // Peer buffers for single row.
    std::vector<vector_size_t> peerStarts(1);
    std::vector<vector_size_t> peerEnds(1);
    sp.partition->computePeerBuffers(
        0, 1, 0, 0, peerStarts.data(), peerEnds.data());
    EXPECT_EQ(peerStarts[0], 0);
    EXPECT_EQ(peerEnds[0], 0);
  }

  // Test memory mode with single row and spill.
  {
    auto mp = createMemoryModePartition(data);
    EXPECT_EQ(mp.partition->numRows(), 1);
    EXPECT_FALSE(mp.partition->isSpilled());

    mp.partition->spill();
    EXPECT_TRUE(mp.partition->isSpilled());

    extractAndVerifyBigintColumn(mp.partition.get(), 0, 1, data->childAt(0));
  }
}

// Test very large partition (10000+ rows) - stress test.
TEST_F(SpillableWindowPartitionTest, veryLargePartition) {
  const vector_size_t numRows = 10000;
  auto data = makeTestData(numRows);

  // Test spill mode with large partition.
  auto sp = createSpillModePartition(data, 500);
  EXPECT_EQ(sp.partition->numRows(), numRows);

  // Extract all rows in chunks.
  const vector_size_t chunkSize = 1000;
  for (vector_size_t offset = 0; offset < numRows; offset += chunkSize) {
    auto size = std::min(chunkSize, numRows - offset);
    extractAndVerifyBigintColumn(
        sp.partition.get(), offset, size, data->childAt(0));
  }

  // Test memory mode with large partition and spill.
  auto mp = createMemoryModePartition(data);
  EXPECT_EQ(mp.partition->numRows(), numRows);

  mp.partition->spill();

  // Verify random access after spill.
  extractAndVerifyBigintColumn(mp.partition.get(), 0, 100, data->childAt(0));
  extractAndVerifyBigintColumn(mp.partition.get(), 5000, 100, data->childAt(0));
  extractAndVerifyBigintColumn(mp.partition.get(), 9900, 100, data->childAt(0));
}

// Test with very small read buffer size (force frequent cache eviction).
TEST_F(SpillableWindowPartitionTest, tinyReadBufferSize) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows);

  // Create partition with tiny read buffer (4KB).
  SpillModePartition sp;
  sp.rowContainer = createRowContainer();
  auto [spillFiles, totalRows] = spillDataAndCreateFiles(data, 10);
  sp.partition = createPartition(
      sp.rowContainer.get(), std::move(spillFiles), totalRows, 4096);

  EXPECT_EQ(sp.partition->numRows(), numRows);

  // Process with sliding window pattern - should work even with tiny buffer.
  testSlidingWindowPattern(
      sp.partition.get(), data->childAt(0), numRows, 10, 5);
}

// Test reading exactly to the end (endRow == totalRows boundary).
TEST_F(SpillableWindowPartitionTest, readToEndBoundary) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data);

  // Read exactly to the last row.
  extractAndVerifyBigintColumn(
      sp.partition.get(), 0, numRows, data->childAt(0));

  // Read last 10 rows (endRow == numRows).
  extractAndVerifyBigintColumn(sp.partition.get(), 90, 10, data->childAt(0));

  // Read single last row.
  auto result = BaseVector::create(BIGINT(), 1, pool_.get());
  sp.partition->extractColumn(0, numRows - 1, 1, 0, result);
  EXPECT_EQ(
      result->asFlatVector<int64_t>()->valueAt(0),
      data->childAt(0)->asFlatVector<int64_t>()->valueAt(numRows - 1));
}

//=============================================================================
// Memory + Spill Combination Tests
//=============================================================================

// Test spill after partial processing, then backward read.
TEST_F(
    SpillableWindowPartitionTest,
    spillAfterPartialProcessingThenBackwardRead) {
  const vector_size_t numRows = 200;
  auto data = makeTestData(numRows);
  auto mp = createMemoryModePartition(data);

  // Process first 100 rows in memory mode.
  for (vector_size_t i = 0; i < 100; i += 20) {
    auto result = BaseVector::create(BIGINT(), 20, pool_.get());
    mp.partition->extractColumn(0, i, 20, 0, result);
  }

  // Spill after partial processing.
  mp.partition->spill();
  EXPECT_TRUE(mp.partition->isSpilled());

  // Backward read to previously processed rows.
  extractAndVerifyBigintColumn(mp.partition.get(), 0, 50, data->childAt(0));

  // Continue forward processing.
  extractAndVerifyBigintColumn(mp.partition.get(), 100, 100, data->childAt(0));

  // Another backward read.
  extractAndVerifyBigintColumn(mp.partition.get(), 50, 50, data->childAt(0));
}

// Test spill during computePeerBuffers processing.
TEST_F(SpillableWindowPartitionTest, spillDuringPeerBufferComputation) {
  const vector_size_t numRows = 100;
  // Create data with duplicates (every 10 rows have same sort key).
  auto dataWithDups = makeRowVector(
      {"c0", "c1", "c2"},
      {
          makeFlatVector<int64_t>(numRows, [](auto row) { return row * 10; }),
          makeFlatVector<int32_t>(numRows, [](auto row) { return row / 10; }),
          makeFlatVector<StringView>(
              numRows,
              [](auto row) {
                return StringView::makeInline(fmt::format("row_{}", row));
              }),
      });

  auto mp = createMemoryModePartition(dataWithDups);

  // Compute peer buffers for first batch.
  std::vector<vector_size_t> peerStarts(20);
  std::vector<vector_size_t> peerEnds(20);
  auto [prevStart, prevEnd] = mp.partition->computePeerBuffers(
      0, 20, 0, 0, peerStarts.data(), peerEnds.data());

  // Spill in the middle of processing.
  mp.partition->spill();

  // Continue computing peer buffers after spill.
  std::vector<vector_size_t> peerStarts2(20);
  std::vector<vector_size_t> peerEnds2(20);
  mp.partition->computePeerBuffers(
      20, 40, prevStart, prevEnd, peerStarts2.data(), peerEnds2.data());

  // Verify peer groups are still correct after spill.
  // Rows 20-29 should be in peer group [20, 29].
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(peerStarts2[i], 20);
    EXPECT_EQ(peerEnds2[i], 29);
  }
}

// Test forward then backward read.
TEST_F(SpillableWindowPartitionTest, forwardThenBackwardRead) {
  const vector_size_t numRows = 300;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 50);

  // Process forward.
  for (vector_size_t batch = 0; batch < numRows; batch += 50) {
    auto result = BaseVector::create(BIGINT(), 50, pool_.get());
    sp.partition->extractColumn(0, batch, 50, 0, result);
  }

  // After processing all rows, do backward reads.
  // Should reset stream and reload from beginning.
  extractAndVerifyBigintColumn(sp.partition.get(), 0, 50, data->childAt(0));
  extractAndVerifyBigintColumn(sp.partition.get(), 100, 50, data->childAt(0));
  extractAndVerifyBigintColumn(sp.partition.get(), 50, 50, data->childAt(0));
}

// Test immediate backward read after spill (no forward progress).
TEST_F(SpillableWindowPartitionTest, immediateBackwardReadAfterSpill) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows);
  auto mp = createMemoryModePartition(data);

  // Read some rows in memory mode.
  auto result1 = BaseVector::create(BIGINT(), 50, pool_.get());
  mp.partition->extractColumn(0, 0, 50, 0, result1);

  // Spill.
  mp.partition->spill();

  // Immediately read the same rows again (backward/same position, no forward).
  extractAndVerifyBigintColumn(mp.partition.get(), 0, 50, data->childAt(0));

  // Read earlier rows (true backward).
  extractAndVerifyBigintColumn(mp.partition.get(), 10, 20, data->childAt(0));
}

// Test spill then backward read then forward read pattern.
TEST_F(SpillableWindowPartitionTest, spillBackwardForwardPattern) {
  const vector_size_t numRows = 200;
  auto data = makeTestData(numRows);
  auto mp = createMemoryModePartition(data);

  // Process forward to row 100.
  for (vector_size_t i = 0; i < 100; i += 20) {
    auto result = BaseVector::create(BIGINT(), 20, pool_.get());
    mp.partition->extractColumn(0, i, 20, 0, result);
  }

  // Spill.
  mp.partition->spill();

  // Backward read to row 20.
  extractAndVerifyBigintColumn(mp.partition.get(), 20, 30, data->childAt(0));

  // Forward read to row 150.
  extractAndVerifyBigintColumn(mp.partition.get(), 100, 50, data->childAt(0));

  // Backward read again to row 0.
  extractAndVerifyBigintColumn(mp.partition.get(), 0, 20, data->childAt(0));

  // Forward read to end.
  extractAndVerifyBigintColumn(mp.partition.get(), 150, 50, data->childAt(0));
}

//=============================================================================
// Special Data Pattern Tests
//=============================================================================

// Test with all NULL data column.
TEST_F(SpillableWindowPartitionTest, allNullDataColumn) {
  const vector_size_t numRows = 50;
  auto dataWithNulls = makeRowVector(
      {"c0", "c1", "c2"},
      {
          // All NULL data column.
          makeNullableFlatVector<int64_t>(
              std::vector<std::optional<int64_t>>(numRows, std::nullopt)),
          makeFlatVector<int32_t>(numRows, [](auto row) { return row; }),
          makeFlatVector<StringView>(
              numRows,
              [](auto row) {
                return StringView::makeInline(fmt::format("row_{}", row));
              }),
      });

  // Test spill mode.
  auto sp = createSpillModePartition(dataWithNulls);
  auto result = BaseVector::create(BIGINT(), numRows, pool_.get());
  sp.partition->extractColumn(0, 0, numRows, 0, result);

  // All values should be null.
  for (vector_size_t i = 0; i < numRows; ++i) {
    EXPECT_TRUE(result->isNullAt(i)) << "Row " << i << " should be null";
  }

  // Test memory mode with spill.
  auto mp = createMemoryModePartition(dataWithNulls);
  mp.partition->spill();

  auto resultAfterSpill = BaseVector::create(BIGINT(), numRows, pool_.get());
  mp.partition->extractColumn(0, 0, numRows, 0, resultAfterSpill);

  for (vector_size_t i = 0; i < numRows; ++i) {
    EXPECT_TRUE(resultAfterSpill->isNullAt(i));
  }
}

// Test where all rows form a single peer group (all sort keys identical).
TEST_F(SpillableWindowPartitionTest, singlePeerGroupAllRows) {
  const vector_size_t numRows = 100;
  // All rows have the same sort key value (0).
  auto dataWithSameSortKey = makeRowVector(
      {"c0", "c1", "c2"},
      {
          makeFlatVector<int64_t>(numRows, [](auto row) { return row * 10; }),
          makeFlatVector<int32_t>(numRows, [](auto /*row*/) { return 0; }),
          makeFlatVector<StringView>(
              numRows,
              [](auto row) {
                return StringView::makeInline(fmt::format("row_{}", row));
              }),
      });

  auto sp = createSpillModePartition(dataWithSameSortKey);

  // All rows should be in the same peer group [0, numRows-1].
  std::vector<vector_size_t> peerStarts(numRows);
  std::vector<vector_size_t> peerEnds(numRows);
  sp.partition->computePeerBuffers(
      0, numRows, 0, 0, peerStarts.data(), peerEnds.data());

  for (vector_size_t i = 0; i < numRows; ++i) {
    EXPECT_EQ(peerStarts[i], 0) << "Peer start at row " << i;
    EXPECT_EQ(peerEnds[i], numRows - 1) << "Peer end at row " << i;
  }

  // Test with memory mode and spill.
  auto mp = createMemoryModePartition(dataWithSameSortKey);

  // Compute peer buffers before spill.
  std::vector<vector_size_t> peerStartsBefore(50);
  std::vector<vector_size_t> peerEndsBefore(50);
  auto [prevStart, prevEnd] = mp.partition->computePeerBuffers(
      0, 50, 0, 0, peerStartsBefore.data(), peerEndsBefore.data());

  mp.partition->spill();

  // Compute peer buffers after spill, using previous call's return values.
  std::vector<vector_size_t> peerStartsAfter(50);
  std::vector<vector_size_t> peerEndsAfter(50);
  mp.partition->computePeerBuffers(
      50,
      100,
      prevStart,
      prevEnd,
      peerStartsAfter.data(),
      peerEndsAfter.data());

  // All should still be in same peer group.
  // Note: peerEnd returned by computePeerBuffers is exclusive (100),
  // but stored in rawPeerEnds is inclusive (numRows - 1 = 99).
  for (vector_size_t i = 0; i < 50; ++i) {
    EXPECT_EQ(peerStartsAfter[i], 0);
    EXPECT_EQ(peerEndsAfter[i], numRows - 1);
  }
}

// Test with all rows having identical data values (not just sort key).
TEST_F(SpillableWindowPartitionTest, allIdenticalValues) {
  const vector_size_t numRows = 50;
  // All rows have identical values.
  auto dataAllSame = makeRowVector(
      {"c0", "c1", "c2"},
      {
          makeFlatVector<int64_t>(numRows, [](auto /*row*/) { return 42; }),
          makeFlatVector<int32_t>(numRows, [](auto /*row*/) { return 100; }),
          makeFlatVector<StringView>(
              numRows,
              [](auto /*row*/) {
                return StringView::makeInline("same_value");
              }),
      });

  auto mp = createMemoryModePartition(dataAllSame);

  // Extract before spill.
  auto resultBefore = BaseVector::create(BIGINT(), numRows, pool_.get());
  mp.partition->extractColumn(0, 0, numRows, 0, resultBefore);

  mp.partition->spill();

  // Extract after spill.
  auto resultAfter = BaseVector::create(BIGINT(), numRows, pool_.get());
  mp.partition->extractColumn(0, 0, numRows, 0, resultAfter);

  // All values should be 42.
  auto beforeFlat = resultBefore->asFlatVector<int64_t>();
  auto afterFlat = resultAfter->asFlatVector<int64_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    EXPECT_EQ(beforeFlat->valueAt(i), 42);
    EXPECT_EQ(afterFlat->valueAt(i), 42);
  }

  // Peer buffers: all should be in single group.
  std::vector<vector_size_t> peerStarts(numRows);
  std::vector<vector_size_t> peerEnds(numRows);
  mp.partition->computePeerBuffers(
      0, numRows, 0, 0, peerStarts.data(), peerEnds.data());

  for (vector_size_t i = 0; i < numRows; ++i) {
    EXPECT_EQ(peerStarts[i], 0);
    EXPECT_EQ(peerEnds[i], numRows - 1);
  }
}

// Test with partially null data (some nulls, some values).
TEST_F(SpillableWindowPartitionTest, partiallyNullData) {
  const vector_size_t numRows = 100;
  // Every 5th row is null.
  auto dataPartialNulls = makeRowVector(
      {"c0", "c1", "c2"},
      {
          makeFlatVector<int64_t>(
              numRows,
              [](auto row) { return row * 10; },
              [](auto row) { return row % 5 == 0; }), // null every 5 rows
          makeFlatVector<int32_t>(numRows, [](auto row) { return row; }),
          makeFlatVector<StringView>(
              numRows,
              [](auto row) {
                return StringView::makeInline(fmt::format("row_{}", row));
              }),
      });

  auto mp = createMemoryModePartition(dataPartialNulls);

  // Extract and verify nulls before spill.
  auto nullsBefore = AlignedBuffer::allocate<bool>(numRows, pool_.get(), false);
  mp.partition->extractNulls(0, 0, numRows, nullsBefore);

  mp.partition->spill();

  // Extract and verify nulls after spill.
  auto nullsAfter = AlignedBuffer::allocate<bool>(numRows, pool_.get(), false);
  mp.partition->extractNulls(0, 0, numRows, nullsAfter);

  auto* rawBefore = nullsBefore->as<uint64_t>();
  auto* rawAfter = nullsAfter->as<uint64_t>();

  for (vector_size_t i = 0; i < numRows; ++i) {
    bool expectedNull = (i % 5 == 0);
    EXPECT_EQ(bits::isBitSet(rawBefore, i), expectedNull) << "Before at " << i;
    EXPECT_EQ(bits::isBitSet(rawAfter, i), expectedNull) << "After at " << i;
  }
}

// Test alternating null and non-null in sort key.
TEST_F(SpillableWindowPartitionTest, alternatingNullSortKey) {
  const vector_size_t numRows = 50;
  // Alternating null sort keys (should still form valid peer groups).
  auto dataAlternatingNulls = makeRowVector(
      {"c0", "c1", "c2"},
      {
          makeFlatVector<int64_t>(numRows, [](auto row) { return row * 10; }),
          makeFlatVector<int32_t>(
              numRows,
              [](auto row) { return row; },
              [](auto row) { return row % 2 == 0; }), // null every other row
          makeFlatVector<StringView>(
              numRows,
              [](auto row) {
                return StringView::makeInline(fmt::format("row_{}", row));
              }),
      });

  auto sp = createSpillModePartition(dataAlternatingNulls);

  // Peer computation should still work.
  std::vector<vector_size_t> peerStarts(numRows);
  std::vector<vector_size_t> peerEnds(numRows);
  sp.partition->computePeerBuffers(
      0, numRows, 0, 0, peerStarts.data(), peerEnds.data());

  // Null sort keys should form one peer group, non-null keys are distinct.
  // (behavior depends on sortKeyInfo null handling - here we just verify no
  // crash)
  EXPECT_EQ(sp.partition->numRows(), numRows);
}

// Test large peer groups with spill.
TEST_F(SpillableWindowPartitionTest, largePeerGroupsWithSpill) {
  const vector_size_t numRows = 1000;
  // Create 10 large peer groups of 100 rows each.
  auto dataLargePeerGroups = makeRowVector(
      {"c0", "c1", "c2"},
      {
          makeFlatVector<int64_t>(numRows, [](auto row) { return row * 10; }),
          makeFlatVector<int32_t>(numRows, [](auto row) { return row / 100; }),
          makeFlatVector<StringView>(
              numRows,
              [](auto row) {
                return StringView::makeInline(fmt::format("row_{}", row));
              }),
      });

  auto mp = createMemoryModePartition(dataLargePeerGroups);

  // Compute peer buffers for first group (rows 0-99).
  std::vector<vector_size_t> peerStarts1(100);
  std::vector<vector_size_t> peerEnds1(100);
  auto [prevStart1, prevEnd1] = mp.partition->computePeerBuffers(
      0, 100, 0, 0, peerStarts1.data(), peerEnds1.data());

  // All 100 rows should be in peer group [0, 99].
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(peerStarts1[i], 0);
    EXPECT_EQ(peerEnds1[i], 99);
  }

  // Spill in the middle.
  mp.partition->spill();

  // Compute peer buffers for second group (rows 100-199).
  std::vector<vector_size_t> peerStarts2(100);
  std::vector<vector_size_t> peerEnds2(100);
  auto [prevStart2, prevEnd2] = mp.partition->computePeerBuffers(
      100, 200, prevStart1, prevEnd1, peerStarts2.data(), peerEnds2.data());

  // Rows 100-199 should be in peer group [100, 199].
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(peerStarts2[i], 100);
    EXPECT_EQ(peerEnds2[i], 199);
  }

  // Continue with third group (rows 200-299).
  std::vector<vector_size_t> peerStarts3(100);
  std::vector<vector_size_t> peerEnds3(100);
  mp.partition->computePeerBuffers(
      200, 300, prevStart2, prevEnd2, peerStarts3.data(), peerEnds3.data());

  // Rows 200-299 should be in peer group [200, 299].
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(peerStarts3[i], 200);
    EXPECT_EQ(peerEnds3[i], 299);
  }
}

//=============================================================================
// Memory Limit and Auto-Eviction Tests
//=============================================================================

// Test that accessing rows in different ranges works correctly.
TEST_F(SpillableWindowPartitionTest, randomRangeAccess) {
  const vector_size_t numRows = 500;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 50);

  // Load rows 0-100.
  auto result1 = BaseVector::create(BIGINT(), 100, pool_.get());
  sp.partition->extractColumn(0, 0, 100, 0, result1);

  // Load rows 200-300.
  auto result2 = BaseVector::create(BIGINT(), 100, pool_.get());
  sp.partition->extractColumn(0, 200, 100, 0, result2);

  // Verify data correctness.
  auto expected = data->childAt(0)->as<FlatVector<int64_t>>();
  auto actual1 = result1->as<FlatVector<int64_t>>();
  auto actual2 = result2->as<FlatVector<int64_t>>();

  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(actual1->valueAt(i), expected->valueAt(i));
    EXPECT_EQ(actual2->valueAt(i), expected->valueAt(200 + i));
  }
}

// Test memory-efficient processing pattern with interleaved reads.
// Simulates a window function that needs to access frame boundaries.
TEST_F(SpillableWindowPartitionTest, memoryEfficientFrameAccess) {
  const vector_size_t numRows = 500;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 50);

  // Simulate frame access pattern: for each row, access a window of rows.
  // This tests that the cache efficiently handles overlapping ranges.
  for (vector_size_t row = 0; row < numRows - 50; row += 50) {
    // Access current row's frame (row to row+50).
    auto result = BaseVector::create(BIGINT(), 50, pool_.get());
    sp.partition->extractColumn(0, row, 50, 0, result);

    // Verify data.
    auto expected = data->childAt(0)->as<FlatVector<int64_t>>();
    auto actual = result->as<FlatVector<int64_t>>();
    for (int i = 0; i < 50; ++i) {
      EXPECT_EQ(actual->valueAt(i), expected->valueAt(row + i));
    }
  }
}

// Test that accessing all rows and then backward access works.
TEST_F(SpillableWindowPartitionTest, accessAllRowsThenBackward) {
  const vector_size_t numRows = 500;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 50);

  // Load all rows.
  auto result = BaseVector::create(BIGINT(), numRows, pool_.get());
  sp.partition->extractColumn(0, 0, numRows, 0, result);

  // Access second half.
  auto result2 = BaseVector::create(BIGINT(), 250, pool_.get());
  sp.partition->extractColumn(0, 250, 250, 0, result2);

  // Verify data correctness.
  auto expected = data->childAt(0)->as<FlatVector<int64_t>>();
  auto actual = result2->as<FlatVector<int64_t>>();
  for (int i = 0; i < 250; ++i) {
    EXPECT_EQ(actual->valueAt(i), expected->valueAt(250 + i));
  }

  // Backward access should work.
  extractAndVerifyBigintColumn(sp.partition.get(), 0, 100, data->childAt(0));
}

// Test with very large string data to stress memory usage.
TEST_F(SpillableWindowPartitionTest, largeStringDataMemoryManagement) {
  const vector_size_t numRows = 200;
  // Create data with large strings to increase memory pressure.
  // Use std::string instead of StringView to ensure strings are copied
  // properly.
  auto dataWithLargeStrings = makeRowVector(
      {"c0", "c1", "c2"},
      {
          makeFlatVector<int64_t>(numRows, [](auto row) { return row; }),
          makeFlatVector<int32_t>(numRows, [](auto row) { return row; }),
          makeFlatVector<std::string>(
              numRows,
              [](auto row) {
                // Create strings of varying sizes (100-500 bytes).
                return std::string(100 + (row % 5) * 100, 'x') +
                    std::to_string(row);
              }),
      });

  auto sp = createSpillModePartition(dataWithLargeStrings, 20);

  // Process all rows.
  for (vector_size_t batch = 0; batch < numRows; batch += 50) {
    auto batchSize = std::min(50, static_cast<int>(numRows - batch));
    auto result = BaseVector::create(VARCHAR(), batchSize, pool_.get());
    sp.partition->extractColumn(2, batch, batchSize, 0, result);
  }

  // Backward access should still work.
  auto result = BaseVector::create(VARCHAR(), 50, pool_.get());
  sp.partition->extractColumn(2, 0, 50, 0, result);

  // Verify first few values.
  auto actual = result->as<FlatVector<StringView>>();
  for (int i = 0; i < 10; ++i) {
    std::string expected =
        std::string(100 + (i % 5) * 100, 'x') + std::to_string(i);
    EXPECT_EQ(actual->valueAt(i).getString(), expected);
  }
}

// Test first_value pattern: always need row 0, process forward.
TEST_F(SpillableWindowPartitionTest, firstValuePatternWithForwardProcessing) {
  const vector_size_t numRows = 500;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 50);

  // Simulate first_value pattern: always need row 0 (first row of partition).
  // Access row 0 first.
  auto result0 = BaseVector::create(BIGINT(), 1, pool_.get());
  sp.partition->extractColumn(0, 0, 1, 0, result0);

  // Now process forward through the partition.
  for (vector_size_t batch = 0; batch < numRows; batch += 50) {
    auto batchSize = std::min(50, static_cast<int>(numRows - batch));
    auto result = BaseVector::create(BIGINT(), batchSize, pool_.get());

    // Extract current batch.
    sp.partition->extractColumn(0, batch, batchSize, 0, result);

    // Also extract row 0 (simulating first_value access).
    auto firstVal = BaseVector::create(BIGINT(), 1, pool_.get());
    sp.partition->extractColumn(0, 0, 1, 0, firstVal);

    // Verify row 0 is still accessible and correct (c0 = row * 10).
    EXPECT_EQ(firstVal->as<FlatVector<int64_t>>()->valueAt(0), 0);
  }
}

// Test sliding window pattern with preceding rows.
TEST_F(SpillableWindowPartitionTest, slidingWindowWithPrecedingRows) {
  const vector_size_t numRows = 500;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 50);

  // Simulate sum() with rows between 10 preceding and current row.
  for (vector_size_t currentRow = 0; currentRow < numRows; currentRow += 10) {
    // Frame: [currentRow - 10, currentRow] (or [0, currentRow] for first rows).
    vector_size_t frameStart = currentRow > 10 ? currentRow - 10 : 0;
    vector_size_t frameEnd = currentRow + 1;
    auto frameSize = frameEnd - frameStart;

    auto result = BaseVector::create(BIGINT(), frameSize, pool_.get());
    sp.partition->extractColumn(0, frameStart, frameSize, 0, result);

    // Verify frame data. c0 column value is row * 10.
    auto actual = result->as<FlatVector<int64_t>>();
    for (vector_size_t i = 0; i < frameSize; ++i) {
      EXPECT_EQ(actual->valueAt(i), (frameStart + i) * 10);
    }
  }
}

// Test that we never fail even if the required frame is very large.
// This tests the "allow temporary memory overage" behavior.
TEST_F(SpillableWindowPartitionTest, largeFrameDoesNotFail) {
  // Create data - not huge but enough to test the pattern.
  const vector_size_t numRows = 1000;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 100);

  // Simulate unbounded preceding to current row.
  // This requires keeping all rows from 0 to current, which can be large.
  for (vector_size_t currentRow = 0; currentRow < numRows; currentRow += 100) {
    // Frame: [0, currentRow + 1) - unbounded preceding to current.
    auto frameSize = currentRow + 1;

    auto result = BaseVector::create(BIGINT(), frameSize, pool_.get());

    // This should never fail, even if it means using more memory than the
    // soft limit.
    EXPECT_NO_THROW(sp.partition->extractColumn(0, 0, frameSize, 0, result));

    // Verify first and last values in frame. c0 column value is row * 10.
    auto actual = result->as<FlatVector<int64_t>>();
    EXPECT_EQ(actual->valueAt(0), 0);
    EXPECT_EQ(actual->valueAt(currentRow), currentRow * 10);
  }
}

//=============================================================================
// Eviction Tests - Test cache eviction behavior with configurable limits.
//=============================================================================

// Test that row-count-based eviction triggers when maxSpillCacheRows is small.
TEST_F(SpillableWindowPartitionTest, rowCountBasedEviction) {
  const vector_size_t numRows = 500;
  auto data = makeTestData(numRows);

  // Create partition with maxSpillCacheRows = 100.
  SpillModePartition sp;
  sp.rowContainer = createRowContainer();
  auto [spillFiles, totalRows] = spillDataAndCreateFiles(data, 10);
  // Use a small maxSpillCacheRows to trigger row-count-based eviction.
  sp.partition = createPartition(
      sp.rowContainer.get(),
      std::move(spillFiles),
      totalRows,
      1 << 20, // 1MB read buffer
      1ULL << 30, // 1GB memory limit (won't trigger)
      100); // maxSpillCacheRows = 100

  // Process rows forward. As we load more rows, old rows should be evicted.
  for (vector_size_t batch = 0; batch < numRows; batch += 50) {
    auto result = BaseVector::create(BIGINT(), 50, pool_.get());
    sp.partition->extractColumn(0, batch, 50, 0, result);

    // Verify batch data is correct.
    auto expected = data->childAt(0)->as<FlatVector<int64_t>>();
    auto actual = result->as<FlatVector<int64_t>>();
    for (int i = 0; i < 50; ++i) {
      EXPECT_EQ(actual->valueAt(i), expected->valueAt(batch + i));
    }
  }

  // Backward access should still work (via seekToRow).
  extractAndVerifyBigintColumn(sp.partition.get(), 0, 50, data->childAt(0));
}

// Test that eviction never removes rows that extractColumn still needs.
// This is the key correctness test for minRequiredRow protection.
TEST_F(SpillableWindowPartitionTest, evictionRespectsMinRequiredRow) {
  const vector_size_t numRows = 500;
  auto data = makeTestData(numRows);

  SpillModePartition sp;
  sp.rowContainer = createRowContainer();
  auto [spillFiles, totalRows] = spillDataAndCreateFiles(data, 10);
  // Very small cache to force aggressive eviction.
  sp.partition = createPartition(
      sp.rowContainer.get(),
      std::move(spillFiles),
      totalRows,
      1 << 20,
      1ULL << 30,
      50); // maxSpillCacheRows = 50

  // Simulate first_value pattern: always need row 0.
  // Request range [0, 100) - rows 0-99.
  auto result = BaseVector::create(BIGINT(), 100, pool_.get());
  sp.partition->extractColumn(0, 0, 100, 0, result);

  // Verify all 100 rows are correct (eviction should not remove rows 0-99).
  auto expected = data->childAt(0)->as<FlatVector<int64_t>>();
  auto actual = result->as<FlatVector<int64_t>>();
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(actual->valueAt(i), expected->valueAt(i))
        << "Row " << i << " was incorrectly evicted";
  }
}

// Test scattered row access with eviction - ensures non-contiguous access
// correctly calculates minRequiredRow.
TEST_F(SpillableWindowPartitionTest, evictionWithScatteredAccess) {
  const vector_size_t numRows = 200;
  auto data = makeTestData(numRows);

  SpillModePartition sp;
  sp.rowContainer = createRowContainer();
  auto [spillFiles, totalRows] = spillDataAndCreateFiles(data, 10);
  sp.partition = createPartition(
      sp.rowContainer.get(),
      std::move(spillFiles),
      totalRows,
      1 << 20,
      1ULL << 30,
      50); // maxSpillCacheRows = 50

  // Request scattered rows: [10, 50, 100, 150].
  std::vector<vector_size_t> rowNumbers = {10, 50, 100, 150};
  auto result = BaseVector::create(BIGINT(), 4, pool_.get());
  sp.partition->extractColumn(
      0,
      folly::Range<const vector_size_t*>(rowNumbers.data(), rowNumbers.size()),
      0,
      result);

  // Verify all requested rows are correct.
  auto expected = data->childAt(0)->as<FlatVector<int64_t>>();
  auto actual = result->as<FlatVector<int64_t>>();
  for (size_t i = 0; i < rowNumbers.size(); ++i) {
    EXPECT_EQ(actual->valueAt(i), expected->valueAt(rowNumbers[i]))
        << "Scattered row " << rowNumbers[i] << " incorrect";
  }
}

// Test that backward access works correctly after aggressive eviction.
TEST_F(SpillableWindowPartitionTest, backwardAccessWithAggressiveEviction) {
  const vector_size_t numRows = 300;
  auto data = makeTestData(numRows);

  SpillModePartition sp;
  sp.rowContainer = createRowContainer();
  auto [spillFiles, totalRows] = spillDataAndCreateFiles(data, 10);
  sp.partition = createPartition(
      sp.rowContainer.get(),
      std::move(spillFiles),
      totalRows,
      1 << 20,
      1ULL << 30,
      30); // Very small cache: 30 rows

  // Process forward to the end.
  for (vector_size_t batch = 0; batch < numRows; batch += 50) {
    auto batchSize = std::min(50, static_cast<int>(numRows - batch));
    auto result = BaseVector::create(BIGINT(), batchSize, pool_.get());
    sp.partition->extractColumn(0, batch, batchSize, 0, result);
  }

  // Now do multiple backward accesses - each should trigger seekToRow.
  extractAndVerifyBigintColumn(sp.partition.get(), 200, 50, data->childAt(0));
  extractAndVerifyBigintColumn(sp.partition.get(), 100, 50, data->childAt(0));
  extractAndVerifyBigintColumn(sp.partition.get(), 0, 50, data->childAt(0));

  // Forward again.
  extractAndVerifyBigintColumn(sp.partition.get(), 250, 50, data->childAt(0));
}

// Test that the frame always gets correct data even with tiny cache.
// This simulates a worst-case scenario where cache is smaller than frame.
TEST_F(SpillableWindowPartitionTest, frameLargerThanCache) {
  const vector_size_t numRows = 200;
  auto data = makeTestData(numRows);

  SpillModePartition sp;
  sp.rowContainer = createRowContainer();
  auto [spillFiles, totalRows] = spillDataAndCreateFiles(data, 10);
  sp.partition = createPartition(
      sp.rowContainer.get(),
      std::move(spillFiles),
      totalRows,
      1 << 20,
      1ULL << 30,
      20); // Cache can only hold 20 rows

  // Request 100 rows at once - much larger than cache.
  auto result = BaseVector::create(BIGINT(), 100, pool_.get());
  sp.partition->extractColumn(0, 50, 100, 0, result);

  // All 100 rows should be correct.
  auto expected = data->childAt(0)->as<FlatVector<int64_t>>();
  auto actual = result->as<FlatVector<int64_t>>();
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(actual->valueAt(i), expected->valueAt(50 + i));
  }
}

// Test that sequential access benefits from cache - rows loaded in one batch
// should still be available for subsequent accesses within the cache window.
TEST_F(SpillableWindowPartitionTest, prefetchSequentialAccessCacheHit) {
  const vector_size_t numRows = 500;
  auto data = makeTestData(numRows);

  SpillModePartition sp;
  sp.rowContainer = createRowContainer();
  auto [spillFiles, totalRows] = spillDataAndCreateFiles(data, 10);
  // Cache can hold 200 rows.
  sp.partition = createPartition(
      sp.rowContainer.get(),
      std::move(spillFiles),
      totalRows,
      1 << 20,
      1ULL << 30,
      200);

  // Access rows 0-49, then 50-99, then 100-149.
  // All should be served from cache after first load if cache is big enough.
  for (int batch = 0; batch < 3; ++batch) {
    auto start = batch * 50;
    auto result = BaseVector::create(BIGINT(), 50, pool_.get());
    sp.partition->extractColumn(0, start, 50, 0, result);

    auto expected = data->childAt(0)->as<FlatVector<int64_t>>();
    auto actual = result->as<FlatVector<int64_t>>();
    for (int i = 0; i < 50; ++i) {
      EXPECT_EQ(actual->valueAt(i), expected->valueAt(start + i));
    }
  }
}

// Test that small batch sequential access works correctly - simulates
// Window operator processing rows in small batches.
TEST_F(SpillableWindowPartitionTest, prefetchSmallBatchSequentialAccess) {
  const vector_size_t numRows = 500;
  auto data = makeTestData(numRows);

  SpillModePartition sp;
  sp.rowContainer = createRowContainer();
  auto [spillFiles, totalRows] = spillDataAndCreateFiles(data, 10);
  sp.partition = createPartition(
      sp.rowContainer.get(),
      std::move(spillFiles),
      totalRows,
      1 << 20,
      1ULL << 30,
      200);

  // Process in very small batches (10 rows at a time).
  // Cache should accumulate rows efficiently.
  for (int batch = 0; batch < 50; ++batch) {
    auto start = batch * 10;
    auto result = BaseVector::create(BIGINT(), 10, pool_.get());
    sp.partition->extractColumn(0, start, 10, 0, result);

    auto expected = data->childAt(0)->as<FlatVector<int64_t>>();
    auto actual = result->as<FlatVector<int64_t>>();
    for (int i = 0; i < 10; ++i) {
      EXPECT_EQ(actual->valueAt(i), expected->valueAt(start + i));
    }
  }
}

// Test that cache does not load rows beyond partition boundary.
TEST_F(SpillableWindowPartitionTest, prefetchRespectsPartitionEnd) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows);

  SpillModePartition sp;
  sp.rowContainer = createRowContainer();
  auto [spillFiles, totalRows] = spillDataAndCreateFiles(data, 10);
  // Large cache - could hold more than partition size.
  sp.partition = createPartition(
      sp.rowContainer.get(),
      std::move(spillFiles),
      totalRows,
      1 << 20,
      1ULL << 30,
      500);

  EXPECT_EQ(sp.partition->numRows(), numRows);

  // Access last 10 rows - should not try to load beyond partition.
  auto result = BaseVector::create(BIGINT(), 10, pool_.get());
  sp.partition->extractColumn(0, 90, 10, 0, result);

  auto expected = data->childAt(0)->as<FlatVector<int64_t>>();
  auto actual = result->as<FlatVector<int64_t>>();
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(actual->valueAt(i), expected->valueAt(90 + i));
  }
}

// Test cache behavior with look-ahead access pattern (like frame computation).
// Window functions often need to look ahead to compute frame boundaries.
TEST_F(SpillableWindowPartitionTest, prefetchLookAheadPattern) {
  const vector_size_t numRows = 500;
  auto data = makeTestData(numRows);

  SpillModePartition sp;
  sp.rowContainer = createRowContainer();
  auto [spillFiles, totalRows] = spillDataAndCreateFiles(data, 10);
  sp.partition = createPartition(
      sp.rowContainer.get(),
      std::move(spillFiles),
      totalRows,
      1 << 20,
      1ULL << 30,
      150);

  // Simulate frame computation: for each batch, need current + look-ahead.
  const int batchSize = 50;
  const int lookAhead = 30;

  for (int batch = 0; batch < 8; ++batch) {
    auto batchStart = batch * batchSize;
    auto frameEnd = std::min(batchStart + batchSize + lookAhead, numRows);
    auto size = frameEnd - batchStart;

    auto result = BaseVector::create(BIGINT(), size, pool_.get());
    sp.partition->extractColumn(0, batchStart, size, 0, result);

    auto expected = data->childAt(0)->as<FlatVector<int64_t>>();
    auto actual = result->as<FlatVector<int64_t>>();
    for (int i = 0; i < size; ++i) {
      EXPECT_EQ(actual->valueAt(i), expected->valueAt(batchStart + i));
    }
  }
}

// Test that backward access after forward processing works with cache.
// This simulates lag() function needing to access earlier rows.
TEST_F(SpillableWindowPartitionTest, prefetchBackwardAfterForward) {
  const vector_size_t numRows = 300;
  auto data = makeTestData(numRows);

  SpillModePartition sp;
  sp.rowContainer = createRowContainer();
  auto [spillFiles, totalRows] = spillDataAndCreateFiles(data, 10);
  sp.partition = createPartition(
      sp.rowContainer.get(),
      std::move(spillFiles),
      totalRows,
      1 << 20,
      1ULL << 30,
      100);

  // Forward pass.
  for (int batch = 0; batch < 6; ++batch) {
    auto start = batch * 50;
    auto result = BaseVector::create(BIGINT(), 50, pool_.get());
    sp.partition->extractColumn(0, start, 50, 0, result);
  }

  // Backward access - should trigger seek and reload.
  extractAndVerifyBigintColumn(sp.partition.get(), 50, 50, data->childAt(0));
  extractAndVerifyBigintColumn(sp.partition.get(), 0, 50, data->childAt(0));
}

// Test cache efficiency with overlapping frame accesses.
// This simulates sum() over rows between N preceding and M following.
TEST_F(SpillableWindowPartitionTest, prefetchOverlappingFrames) {
  const vector_size_t numRows = 200;
  auto data = makeTestData(numRows);

  SpillModePartition sp;
  sp.rowContainer = createRowContainer();
  auto [spillFiles, totalRows] = spillDataAndCreateFiles(data, 10);
  sp.partition = createPartition(
      sp.rowContainer.get(),
      std::move(spillFiles),
      totalRows,
      1 << 20,
      1ULL << 30,
      80);

  const int preceding = 10;
  const int following = 10;

  // Process rows 20-150, each with frame [row-10, row+10].
  for (int row = 20; row < 150; row += 5) {
    auto frameStart = row - preceding;
    auto frameEnd = std::min(row + following + 1, numRows);
    auto frameSize = frameEnd - frameStart;

    auto result = BaseVector::create(BIGINT(), frameSize, pool_.get());
    sp.partition->extractColumn(0, frameStart, frameSize, 0, result);

    auto expected = data->childAt(0)->as<FlatVector<int64_t>>();
    auto actual = result->as<FlatVector<int64_t>>();
    // Verify first and last values in frame.
    EXPECT_EQ(actual->valueAt(0), expected->valueAt(frameStart));
    EXPECT_EQ(actual->valueAt(frameSize - 1), expected->valueAt(frameEnd - 1));
  }
}

// Test prefetch behavior when accessing rows near cache boundary.
TEST_F(SpillableWindowPartitionTest, prefetchNearCacheBoundary) {
  const vector_size_t numRows = 300;
  auto data = makeTestData(numRows);

  SpillModePartition sp;
  sp.rowContainer = createRowContainer();
  auto [spillFiles, totalRows] = spillDataAndCreateFiles(data, 10);
  sp.partition = createPartition(
      sp.rowContainer.get(),
      std::move(spillFiles),
      totalRows,
      1 << 20,
      1ULL << 30,
      50);

  // Load rows 0-49.
  auto result1 = BaseVector::create(BIGINT(), 50, pool_.get());
  sp.partition->extractColumn(0, 0, 50, 0, result1);

  // Now access rows 40-89 - overlaps with cache but extends beyond.
  auto result2 = BaseVector::create(BIGINT(), 50, pool_.get());
  sp.partition->extractColumn(0, 40, 50, 0, result2);

  auto expected = data->childAt(0)->as<FlatVector<int64_t>>();
  auto actual = result2->as<FlatVector<int64_t>>();
  for (int i = 0; i < 50; ++i) {
    EXPECT_EQ(actual->valueAt(i), expected->valueAt(40 + i));
  }
}

// Test that eviction during prefetch preserves data correctness.
TEST_F(SpillableWindowPartitionTest, prefetchWithEvictionDuringLoad) {
  const vector_size_t numRows = 500;
  auto data = makeTestData(numRows);

  SpillModePartition sp;
  sp.rowContainer = createRowContainer();
  auto [spillFiles, totalRows] = spillDataAndCreateFiles(data, 10);
  // Very small cache to force eviction during load.
  sp.partition = createPartition(
      sp.rowContainer.get(),
      std::move(spillFiles),
      totalRows,
      1 << 20,
      1ULL << 30,
      30);

  // Access pattern that forces eviction: forward with large gaps.
  std::vector<std::pair<int, int>> accessPattern = {
      {0, 30}, {50, 30}, {100, 30}, {150, 30}, {200, 30}};

  for (auto& [start, size] : accessPattern) {
    auto result = BaseVector::create(BIGINT(), size, pool_.get());
    sp.partition->extractColumn(0, start, size, 0, result);

    auto expected = data->childAt(0)->as<FlatVector<int64_t>>();
    auto actual = result->as<FlatVector<int64_t>>();
    for (int i = 0; i < size; ++i) {
      EXPECT_EQ(actual->valueAt(i), expected->valueAt(start + i))
          << "Mismatch at access [" << start << ", " << (start + size) << ")";
    }
  }
}

// Test prefetch with scattered row access (like lead/lag with varying offsets).
TEST_F(SpillableWindowPartitionTest, prefetchScatteredRowAccess) {
  const vector_size_t numRows = 200;
  auto data = makeTestData(numRows);

  SpillModePartition sp;
  sp.rowContainer = createRowContainer();
  auto [spillFiles, totalRows] = spillDataAndCreateFiles(data, 10);
  sp.partition = createPartition(
      sp.rowContainer.get(),
      std::move(spillFiles),
      totalRows,
      1 << 20,
      1ULL << 30,
      100);

  // Scattered access: rows at various positions.
  std::vector<vector_size_t> rowNumbers = {5, 25, 50, 75, 100, 125, 150, 175};
  auto result = BaseVector::create(BIGINT(), rowNumbers.size(), pool_.get());
  sp.partition->extractColumn(
      0,
      folly::Range<const vector_size_t*>(rowNumbers.data(), rowNumbers.size()),
      0,
      result);

  auto expected = data->childAt(0)->as<FlatVector<int64_t>>();
  auto actual = result->as<FlatVector<int64_t>>();
  for (size_t i = 0; i < rowNumbers.size(); ++i) {
    EXPECT_EQ(actual->valueAt(i), expected->valueAt(rowNumbers[i]));
  }
}

// Test that multiple small accesses to same region benefit from cache.
TEST_F(SpillableWindowPartitionTest, prefetchRepeatedSmallAccess) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows);

  SpillModePartition sp;
  sp.rowContainer = createRowContainer();
  auto [spillFiles, totalRows] = spillDataAndCreateFiles(data, 10);
  sp.partition = createPartition(
      sp.rowContainer.get(),
      std::move(spillFiles),
      totalRows,
      1 << 20,
      1ULL << 30,
      100);

  // Access same region multiple times with small batches.
  for (int round = 0; round < 3; ++round) {
    for (int start = 0; start < 50; start += 10) {
      auto result = BaseVector::create(BIGINT(), 10, pool_.get());
      sp.partition->extractColumn(0, start, 10, 0, result);

      auto expected = data->childAt(0)->as<FlatVector<int64_t>>();
      auto actual = result->as<FlatVector<int64_t>>();
      for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(actual->valueAt(i), expected->valueAt(start + i));
      }
    }
  }
}

// Test prefetch with peer buffer computation which may need to look ahead.
TEST_F(SpillableWindowPartitionTest, prefetchWithPeerBufferLookAhead) {
  const vector_size_t numRows = 200;
  // Create data with duplicates - every 10 rows have same sort key.
  auto dataWithDups = makeRowVector(
      {"c0", "c1", "c2"},
      {
          makeFlatVector<int64_t>(numRows, [](auto row) { return row * 10; }),
          makeFlatVector<int32_t>(numRows, [](auto row) { return row / 10; }),
          makeFlatVector<StringView>(
              numRows,
              [](auto row) {
                return StringView::makeInline(fmt::format("row_{}", row));
              }),
      });

  SpillModePartition sp;
  sp.rowContainer = createRowContainer();
  auto [spillFiles, totalRows] = spillDataAndCreateFiles(dataWithDups, 10);
  sp.partition = createPartition(
      sp.rowContainer.get(),
      std::move(spillFiles),
      totalRows,
      1 << 20,
      1ULL << 30,
      50);

  // Compute peer buffers in batches.
  // Peer group computation needs to look ahead to find group boundaries.
  std::vector<vector_size_t> peerStarts(30);
  std::vector<vector_size_t> peerEnds(30);
  vector_size_t prevStart = 0, prevEnd = 0;

  for (int batch = 0; batch < 5; ++batch) {
    auto batchStart = batch * 30;
    auto batchEnd = std::min(batchStart + 30, numRows);
    auto batchSize = batchEnd - batchStart;

    std::tie(prevStart, prevEnd) = sp.partition->computePeerBuffers(
        batchStart,
        batchEnd,
        prevStart,
        prevEnd,
        peerStarts.data(),
        peerEnds.data());

    // Verify peer groups are computed correctly.
    for (int i = 0; i < batchSize; ++i) {
      auto row = batchStart + i;
      auto expectedGroupStart = (row / 10) * 10;
      auto expectedGroupEnd = expectedGroupStart + 9;
      EXPECT_EQ(peerStarts[i], expectedGroupStart)
          << "Peer start mismatch at row " << row;
      EXPECT_EQ(peerEnds[i], expectedGroupEnd)
          << "Peer end mismatch at row " << row;
    }
  }
}

} // namespace facebook::velox::exec::test
