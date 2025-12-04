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
            makeFlatVector<int64_t>(numRows, [](auto row) { return row * 10; }),
            makeFlatVector<int32_t>(numRows, [](auto row) { return row; }),
            makeFlatVector<StringView>(
                numRows,
                [](auto row) {
                  return StringView::makeInline(fmt::format("row_{}", row));
                }),
        });
  }

  // Creates test data with duplicate sort keys (every groupSize rows same).
  RowVectorPtr makeTestDataWithDuplicates(
      vector_size_t numRows,
      vector_size_t groupSize) {
    return makeRowVector(
        {"c0", "c1", "c2"},
        {
            makeFlatVector<int64_t>(numRows, [](auto row) { return row * 10; }),
            makeFlatVector<int32_t>(
                numRows, [groupSize](auto row) { return row / groupSize; }),
            makeFlatVector<StringView>(
                numRows,
                [](auto row) {
                  return StringView::makeInline(fmt::format("row_{}", row));
                }),
        });
  }

  std::unique_ptr<RowContainer> createRowContainer() {
    std::vector<TypePtr> types = {BIGINT(), INTEGER(), VARCHAR()};
    return std::make_unique<RowContainer>(types, pool_.get());
  }

  common::SpillConfig makeSpillConfig() {
    return common::SpillConfig(
        [this]() -> std::string_view { return spillDir_->getPath(); },
        [](uint64_t) {},
        "test",
        0,
        0,
        1 << 20,
        nullptr,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        "none");
  }

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
        1024 * 1024,
        0,
        common::CompressionKind::CompressionKind_NONE,
        prefixSortConfig,
        pool_.get(),
        &spillStats_);

    SpillPartitionId partitionId{0};
    spillState_->setPartitionSpilled(partitionId);

    auto numRows = data->size();
    for (vector_size_t offset = 0; offset < numRows; offset += batchSize) {
      auto size = std::min(batchSize, numRows - offset);
      auto batch =
          std::dynamic_pointer_cast<RowVector>(data->slice(offset, size));
      spillState_->appendToPartition(partitionId, batch);
    }

    spillState_->finishFile(partitionId);
    return {spillState_->finish(partitionId), numRows};
  }

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

  struct SpillModePartition {
    std::unique_ptr<SpillableWindowPartition> partition;
    std::unique_ptr<RowContainer> rowContainer;
  };

  SpillModePartition createSpillModePartition(
      const RowVectorPtr& data,
      vector_size_t batchSize = 100,
      uint64_t readBufferSize = 1 << 20,
      uint64_t maxSpillCacheRows = 100000) {
    SpillModePartition result;
    result.rowContainer = createRowContainer();
    auto [spillFiles, totalRows] = spillDataAndCreateFiles(data, batchSize);
    result.partition = createPartition(
        result.rowContainer.get(),
        std::move(spillFiles),
        totalRows,
        readBufferSize,
        1ULL << 30,
        maxSpillCacheRows);
    return result;
  }

  void extractAndVerifyBigintColumn(
      SpillableWindowPartition* partition,
      vector_size_t startRow,
      vector_size_t numRows,
      const VectorPtr& expected) {
    auto result = BaseVector::create(BIGINT(), numRows, pool_.get());
    partition->extractColumn(0, startRow, numRows, 0, result);
    verifyColumnValues<int64_t>(result, expected, startRow);
  }

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
      EXPECT_EQ(
          result->asFlatVector<int64_t>()->valueAt(0),
          expectedFlat->valueAt(batchStart));
    }
  }

  void testLeadLagPattern(
      SpillableWindowPartition* partition,
      const VectorPtr& expected,
      vector_size_t numRows,
      vector_size_t batchSize,
      int32_t offset) {
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

  void verifyGroupedPeerBuffers(
      const std::vector<vector_size_t>& peerStarts,
      const std::vector<vector_size_t>& peerEnds,
      vector_size_t startRow,
      vector_size_t numRows,
      vector_size_t groupSize,
      vector_size_t totalRows) {
    for (vector_size_t i = 0; i < numRows; ++i) {
      auto row = startRow + i;
      auto groupStart = (row / groupSize) * groupSize;
      auto groupEnd = std::min(groupStart + groupSize - 1, totalRows - 1);
      EXPECT_EQ(peerStarts[i], groupStart) << "at row " << row;
      EXPECT_EQ(peerEnds[i], groupEnd) << "at row " << row;
    }
  }

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

//=============================================================================
// Core Spill Mode Tests
//=============================================================================

// Comprehensive test for spill mode: construction, loading, backward access,
// string columns, cache behavior.
TEST_F(SpillableWindowPartitionTest, spillModeBasic) {
  const vector_size_t numRows = 500;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 50);

  // Basic construction.
  EXPECT_EQ(sp.partition->numRows(), numRows);
  EXPECT_TRUE(sp.partition->isSpilled());

  // Load all rows sequentially in chunks.
  const vector_size_t chunkSize = 100;
  for (vector_size_t offset = 0; offset < numRows; offset += chunkSize) {
    auto size = std::min(chunkSize, numRows - offset);
    extractAndVerifyBigintColumn(
        sp.partition.get(), offset, size, data->childAt(0));
  }

  // Backward access.
  extractAndVerifyBigintColumn(sp.partition.get(), 0, 50, data->childAt(0));

  // String column verification.
  auto strResult = BaseVector::create(VARCHAR(), numRows, pool_.get());
  sp.partition->extractColumn(2, 0, numRows, 0, strResult);
  verifyStringValues(strResult, data->childAt(2));

  // Access patterns: first, middle, last.
  auto result = BaseVector::create(BIGINT(), 10, pool_.get());
  sp.partition->extractColumn(0, 0, 10, 0, result);
  sp.partition->extractColumn(0, numRows - 10, 10, 0, result);
  sp.partition->extractColumn(0, 245, 10, 0, result);
  verifyColumnValues<int64_t>(result, data->childAt(0), 245);
}

// Test window access patterns: sliding window, lead/lag.
TEST_F(SpillableWindowPartitionTest, windowAccessPatterns) {
  const vector_size_t numRows = 500;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 50);

  // Sliding window with look-ahead.
  testSlidingWindowPattern(
      sp.partition.get(), data->childAt(0), numRows, 100, 50);

  // Lead pattern (positive offset).
  testLeadLagPattern(sp.partition.get(), data->childAt(0), numRows, 100, 20);

  // Lag pattern (negative offset).
  testLeadLagPattern(sp.partition.get(), data->childAt(0), numRows, 100, -20);
}

// Test peer buffer computation with distinct and duplicate values.
TEST_F(SpillableWindowPartitionTest, peerBufferComputation) {
  // Test with distinct values.
  {
    auto data = makeTestData(100);
    auto sp = createSpillModePartition(data);

    std::vector<vector_size_t> peerStarts(20);
    std::vector<vector_size_t> peerEnds(20);
    sp.partition->computePeerBuffers(
        0, 20, 0, 0, peerStarts.data(), peerEnds.data());
    verifyDistinctPeerBuffers(peerStarts, peerEnds, 0, 20);
  }

  // Test with duplicates (every 5 rows same sort key).
  {
    auto dataWithDups = makeTestDataWithDuplicates(100, 5);
    auto sp = createSpillModePartition(dataWithDups);

    std::vector<vector_size_t> peerStarts(15);
    std::vector<vector_size_t> peerEnds(15);
    sp.partition->computePeerBuffers(
        0, 15, 0, 0, peerStarts.data(), peerEnds.data());
    verifyGroupedPeerBuffers(peerStarts, peerEnds, 0, 15, 5, 100);
  }

  // Test ranking function pattern: process in batches with peer buffers.
  {
    auto data = makeTestData(500);
    auto sp = createSpillModePartition(data, 50);

    const vector_size_t batchSize = 100;
    vector_size_t prevStart = 0, prevEnd = 0;
    std::vector<vector_size_t> peerStarts(batchSize);
    std::vector<vector_size_t> peerEnds(batchSize);

    for (vector_size_t batchStart = 0; batchStart < 500;
         batchStart += batchSize) {
      auto size = std::min(batchSize, 500 - batchStart);
      std::tie(prevStart, prevEnd) = sp.partition->computePeerBuffers(
          batchStart,
          batchStart + size,
          prevStart,
          prevEnd,
          peerStarts.data(),
          peerEnds.data());
    }
  }
}

// Test first_value and last_value patterns.
TEST_F(SpillableWindowPartitionTest, firstLastValuePatterns) {
  const vector_size_t numRows = 200;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 50);
  auto expected = data->childAt(0)->asFlatVector<int64_t>();

  // first_value: cache first row, process forward.
  auto firstResult = BaseVector::create(BIGINT(), 1, pool_.get());
  sp.partition->extractColumn(0, 0, 1, 0, firstResult);
  EXPECT_EQ(
      firstResult->asFlatVector<int64_t>()->valueAt(0), expected->valueAt(0));

  // Process forward and verify first row still accessible.
  for (vector_size_t batch = 50; batch < numRows; batch += 50) {
    auto result = BaseVector::create(BIGINT(), 50, pool_.get());
    sp.partition->extractColumn(0, batch, 50, 0, result);

    // Verify first row.
    sp.partition->extractColumn(0, 0, 1, 0, firstResult);
    EXPECT_EQ(
        firstResult->asFlatVector<int64_t>()->valueAt(0), expected->valueAt(0));
  }

  // last_value: access last row.
  auto lastResult = BaseVector::create(BIGINT(), 1, pool_.get());
  sp.partition->extractColumn(0, numRows - 1, 1, 0, lastResult);
  EXPECT_EQ(
      lastResult->asFlatVector<int64_t>()->valueAt(0),
      expected->valueAt(numRows - 1));
}

// Test rowNumbers-based extraction (contiguous and scattered).
TEST_F(SpillableWindowPartitionTest, rowNumbersExtraction) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data);
  auto expected = data->childAt(0)->asFlatVector<int64_t>();

  // Contiguous rowNumbers.
  {
    std::vector<vector_size_t> rowNumbers(50);
    std::iota(rowNumbers.begin(), rowNumbers.end(), 10);
    auto result = BaseVector::create(BIGINT(), 50, pool_.get());
    sp.partition->extractColumn(
        0,
        folly::Range<const vector_size_t*>(
            rowNumbers.data(), rowNumbers.size()),
        0,
        result);
    verifyColumnValues<int64_t>(result, data->childAt(0), 10);
  }

  // Scattered rowNumbers.
  {
    std::vector<vector_size_t> rowNumbers = {10, 25, 50, 75, 90};
    auto result = BaseVector::create(BIGINT(), 5, pool_.get());
    sp.partition->extractColumn(
        0,
        folly::Range<const vector_size_t*>(
            rowNumbers.data(), rowNumbers.size()),
        0,
        result);
    auto actual = result->asFlatVector<int64_t>();
    for (size_t i = 0; i < rowNumbers.size(); ++i) {
      EXPECT_EQ(actual->valueAt(i), expected->valueAt(rowNumbers[i]));
    }
  }
}

// Test RANGE frame bounds computation.
TEST_F(SpillableWindowPartitionTest, rangeFrameBounds) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data);

  std::vector<vector_size_t> peerStarts(20);
  std::vector<vector_size_t> peerEnds(20);
  sp.partition->computePeerBuffers(
      0, 20, 0, 0, peerStarts.data(), peerEnds.data());
  verifyDistinctPeerBuffers(peerStarts, peerEnds, 0, 20);

  // Test computeKRangeFrameBounds.
  std::vector<vector_size_t> frameBounds(20);
  SelectivityVector validFrames(20, false);
  sp.partition->computeKRangeFrameBounds(
      true, true, 0, 0, 20, peerStarts.data(), frameBounds.data(), validFrames);
  EXPECT_GT(validFrames.countSelected(), 0);
}

//=============================================================================
// Memory Mode Tests
//=============================================================================

// Comprehensive memory mode test: construction, spill transition, data
// consistency.
TEST_F(SpillableWindowPartitionTest, memoryModeAndSpillTransition) {
  const vector_size_t numRows = 200;
  auto data = makeTestData(numRows);
  auto mp = createMemoryModePartition(data);

  // Basic memory mode.
  EXPECT_FALSE(mp.partition->isSpilled());
  EXPECT_EQ(mp.partition->numRows(), numRows);

  // Extract all columns before spill.
  auto col0Before = BaseVector::create(BIGINT(), numRows, pool_.get());
  auto col1Before = BaseVector::create(INTEGER(), numRows, pool_.get());
  auto col2Before = BaseVector::create(VARCHAR(), numRows, pool_.get());
  mp.partition->extractColumn(0, 0, numRows, 0, col0Before);
  mp.partition->extractColumn(1, 0, numRows, 0, col1Before);
  mp.partition->extractColumn(2, 0, numRows, 0, col2Before);

  // Spill.
  auto statsBefore = spillStats_.copy();
  mp.partition->spill();
  auto statsAfter = spillStats_.copy();

  EXPECT_TRUE(mp.partition->isSpilled());
  EXPECT_GT(statsAfter.spilledBytes, statsBefore.spilledBytes);
  EXPECT_GT(statsAfter.spilledFiles, statsBefore.spilledFiles);

  // Verify data consistency after spill.
  auto col0After = BaseVector::create(BIGINT(), numRows, pool_.get());
  auto col1After = BaseVector::create(INTEGER(), numRows, pool_.get());
  auto col2After = BaseVector::create(VARCHAR(), numRows, pool_.get());
  mp.partition->extractColumn(0, 0, numRows, 0, col0After);
  mp.partition->extractColumn(1, 0, numRows, 0, col1After);
  mp.partition->extractColumn(2, 0, numRows, 0, col2After);

  verifyColumnValues<int64_t>(col0After, col0Before);
  verifyColumnValues<int32_t>(col1After, col1Before);
  verifyStringValues(col2After, col2Before);

  // Calling spill() again is a no-op.
  mp.partition->spill();
  EXPECT_TRUE(mp.partition->isSpilled());
}

// Test peer buffer computation across spill transition.
TEST_F(SpillableWindowPartitionTest, peerBufferAcrossSpill) {
  auto dataWithDups = makeTestDataWithDuplicates(50, 5);
  auto mp = createMemoryModePartition(dataWithDups);

  std::vector<vector_size_t> peerStartsBefore(15);
  std::vector<vector_size_t> peerEndsBefore(15);
  mp.partition->computePeerBuffers(
      0, 15, 0, 0, peerStartsBefore.data(), peerEndsBefore.data());

  mp.partition->spill();

  std::vector<vector_size_t> peerStartsAfter(15);
  std::vector<vector_size_t> peerEndsAfter(15);
  mp.partition->computePeerBuffers(
      0, 15, 0, 0, peerStartsAfter.data(), peerEndsAfter.data());

  for (vector_size_t i = 0; i < 15; ++i) {
    EXPECT_EQ(peerStartsAfter[i], peerStartsBefore[i]);
    EXPECT_EQ(peerEndsAfter[i], peerEndsBefore[i]);
  }
}

// Test numRowsForProcessing and extractColumn with rowNumbers across spill.
TEST_F(SpillableWindowPartitionTest, memoryModeAPIs) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows);
  auto mp = createMemoryModePartition(data);

  // numRowsForProcessing.
  EXPECT_EQ(mp.partition->numRowsForProcessing(0), numRows);
  EXPECT_EQ(mp.partition->numRowsForProcessing(50), 50);

  // extractColumn with rowNumbers before spill.
  std::vector<vector_size_t> rowNumbers = {5, 10, 15, 20, 25};
  auto resultBefore =
      BaseVector::create(BIGINT(), rowNumbers.size(), pool_.get());
  mp.partition->extractColumn(
      0,
      folly::Range<const vector_size_t*>(rowNumbers.data(), rowNumbers.size()),
      0,
      resultBefore);

  mp.partition->spill();

  // Verify after spill.
  EXPECT_EQ(mp.partition->numRowsForProcessing(0), numRows);

  auto resultAfter =
      BaseVector::create(BIGINT(), rowNumbers.size(), pool_.get());
  mp.partition->extractColumn(
      0,
      folly::Range<const vector_size_t*>(rowNumbers.data(), rowNumbers.size()),
      0,
      resultAfter);

  verifyColumnValues<int64_t>(resultAfter, resultBefore);
}

// Test sliding window with mid-processing spill.
TEST_F(SpillableWindowPartitionTest, slidingWindowWithMidProcessingSpill) {
  const vector_size_t numRows = 200;
  auto data = makeTestData(numRows);
  auto mp = createMemoryModePartition(data);

  // Process first half in memory mode.
  for (vector_size_t batch = 0; batch < 100; batch += 20) {
    extractAndVerifyBigintColumn(
        mp.partition.get(), batch, 20, data->childAt(0));
  }

  mp.partition->spill();

  // Continue processing second half.
  for (vector_size_t batch = 100; batch < numRows; batch += 20) {
    auto size = std::min(20, static_cast<int>(numRows - batch));
    extractAndVerifyBigintColumn(
        mp.partition.get(), batch, size, data->childAt(0));
  }

  // Backward access after spill.
  extractAndVerifyBigintColumn(mp.partition.get(), 0, 50, data->childAt(0));
}

// Test empty partition error.
TEST_F(SpillableWindowPartitionTest, emptyPartitionError) {
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

// Test extractNulls across spill.
TEST_F(SpillableWindowPartitionTest, extractNullsAcrossSpill) {
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

  auto nullsBefore = AlignedBuffer::allocate<bool>(numRows, pool_.get(), false);
  mp.partition->extractNulls(0, 0, numRows, nullsBefore);

  mp.partition->spill();

  auto nullsAfter = AlignedBuffer::allocate<bool>(numRows, pool_.get(), false);
  mp.partition->extractNulls(0, 0, numRows, nullsAfter);

  auto* rawBefore = nullsBefore->as<uint64_t>();
  auto* rawAfter = nullsAfter->as<uint64_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    EXPECT_EQ(bits::isBitSet(rawAfter, i), bits::isBitSet(rawBefore, i));
  }
}

//=============================================================================
// Edge Cases and Special Data Patterns
//=============================================================================

// Test extreme partition sizes: single row and very large.
TEST_F(SpillableWindowPartitionTest, extremePartitionSizes) {
  // Single row partition.
  {
    auto data = makeTestData(1);
    auto sp = createSpillModePartition(data, 1);
    EXPECT_EQ(sp.partition->numRows(), 1);

    auto result = BaseVector::create(BIGINT(), 1, pool_.get());
    sp.partition->extractColumn(0, 0, 1, 0, result);
    verifyColumnValues<int64_t>(result, data->childAt(0));

    std::vector<vector_size_t> peerStarts(1), peerEnds(1);
    sp.partition->computePeerBuffers(
        0, 1, 0, 0, peerStarts.data(), peerEnds.data());
    EXPECT_EQ(peerStarts[0], 0);
    EXPECT_EQ(peerEnds[0], 0);

    // Memory mode with spill.
    auto mp = createMemoryModePartition(data);
    mp.partition->spill();
    extractAndVerifyBigintColumn(mp.partition.get(), 0, 1, data->childAt(0));
  }

  // Very large partition.
  {
    const vector_size_t numRows = 10000;
    auto data = makeTestData(numRows);
    auto sp = createSpillModePartition(data, 500);
    EXPECT_EQ(sp.partition->numRows(), numRows);

    // Verify random positions.
    extractAndVerifyBigintColumn(sp.partition.get(), 0, 100, data->childAt(0));
    extractAndVerifyBigintColumn(
        sp.partition.get(), 5000, 100, data->childAt(0));
    extractAndVerifyBigintColumn(
        sp.partition.get(), 9900, 100, data->childAt(0));
  }
}

// Test special data patterns: nulls, identical values, single peer group.
TEST_F(SpillableWindowPartitionTest, specialDataPatterns) {
  const vector_size_t numRows = 50;

  // All NULL data column.
  {
    auto dataWithNulls = makeRowVector(
        {"c0", "c1", "c2"},
        {
            makeNullableFlatVector<int64_t>(
                std::vector<std::optional<int64_t>>(numRows, std::nullopt)),
            makeFlatVector<int32_t>(numRows, [](auto row) { return row; }),
            makeFlatVector<StringView>(
                numRows,
                [](auto row) {
                  return StringView::makeInline(fmt::format("row_{}", row));
                }),
        });

    auto sp = createSpillModePartition(dataWithNulls);
    auto result = BaseVector::create(BIGINT(), numRows, pool_.get());
    sp.partition->extractColumn(0, 0, numRows, 0, result);
    for (vector_size_t i = 0; i < numRows; ++i) {
      EXPECT_TRUE(result->isNullAt(i));
    }
  }

  // Partially null data (every 5th row null).
  {
    auto dataPartialNulls = makeRowVector(
        {"c0", "c1", "c2"},
        {
            makeFlatVector<int64_t>(
                numRows,
                [](auto row) { return row * 10; },
                [](auto row) { return row % 5 == 0; }),
            makeFlatVector<int32_t>(numRows, [](auto row) { return row; }),
            makeFlatVector<StringView>(
                numRows,
                [](auto row) {
                  return StringView::makeInline(fmt::format("row_{}", row));
                }),
        });

    auto mp = createMemoryModePartition(dataPartialNulls);
    auto nullsBefore =
        AlignedBuffer::allocate<bool>(numRows, pool_.get(), false);
    mp.partition->extractNulls(0, 0, numRows, nullsBefore);

    mp.partition->spill();

    auto nullsAfter =
        AlignedBuffer::allocate<bool>(numRows, pool_.get(), false);
    mp.partition->extractNulls(0, 0, numRows, nullsAfter);

    auto* rawAfter = nullsAfter->as<uint64_t>();
    for (vector_size_t i = 0; i < numRows; ++i) {
      EXPECT_EQ(bits::isBitSet(rawAfter, i), (i % 5 == 0));
    }
  }

  // All identical values (single peer group).
  {
    auto dataAllSame = makeRowVector(
        {"c0", "c1", "c2"},
        {
            makeFlatVector<int64_t>(numRows, [](auto) { return 42; }),
            makeFlatVector<int32_t>(numRows, [](auto) { return 0; }),
            makeFlatVector<StringView>(
                numRows, [](auto) { return StringView::makeInline("same"); }),
        });

    auto sp = createSpillModePartition(dataAllSame);
    std::vector<vector_size_t> peerStarts(numRows), peerEnds(numRows);
    sp.partition->computePeerBuffers(
        0, numRows, 0, 0, peerStarts.data(), peerEnds.data());

    for (vector_size_t i = 0; i < numRows; ++i) {
      EXPECT_EQ(peerStarts[i], 0);
      EXPECT_EQ(peerEnds[i], numRows - 1);
    }
  }
}

// Test large peer groups with spill.
TEST_F(SpillableWindowPartitionTest, largePeerGroupsWithSpill) {
  const vector_size_t numRows = 500;
  auto dataLargePeerGroups = makeTestDataWithDuplicates(numRows, 100);
  auto mp = createMemoryModePartition(dataLargePeerGroups);

  // Compute peer buffers for first group.
  std::vector<vector_size_t> peerStarts1(100);
  std::vector<vector_size_t> peerEnds1(100);
  auto [prevStart1, prevEnd1] = mp.partition->computePeerBuffers(
      0, 100, 0, 0, peerStarts1.data(), peerEnds1.data());

  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(peerStarts1[i], 0);
    EXPECT_EQ(peerEnds1[i], 99);
  }

  mp.partition->spill();

  // Continue with second group after spill.
  std::vector<vector_size_t> peerStarts2(100);
  std::vector<vector_size_t> peerEnds2(100);
  mp.partition->computePeerBuffers(
      100, 200, prevStart1, prevEnd1, peerStarts2.data(), peerEnds2.data());

  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(peerStarts2[i], 100);
    EXPECT_EQ(peerEnds2[i], 199);
  }
}

//=============================================================================
// Cache Eviction and Prefetch Tests
//=============================================================================

// Test eviction respects minRequiredRow - never evict rows still needed.
TEST_F(SpillableWindowPartitionTest, evictionRespectsMinRequiredRow) {
  const vector_size_t numRows = 500;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 10, 1 << 20, 50);

  // Request range [0, 100) - rows 0-99.
  auto result = BaseVector::create(BIGINT(), 100, pool_.get());
  sp.partition->extractColumn(0, 0, 100, 0, result);

  // All 100 rows should be correct.
  auto expected = data->childAt(0)->as<FlatVector<int64_t>>();
  auto actual = result->as<FlatVector<int64_t>>();
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(actual->valueAt(i), expected->valueAt(i));
  }
}

// Test backward access with aggressive eviction.
TEST_F(SpillableWindowPartitionTest, backwardAccessWithEviction) {
  const vector_size_t numRows = 300;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 10, 1 << 20, 30);

  // Process forward to the end.
  for (vector_size_t batch = 0; batch < numRows; batch += 50) {
    auto result = BaseVector::create(BIGINT(), 50, pool_.get());
    sp.partition->extractColumn(0, batch, 50, 0, result);
  }

  // Backward accesses - each triggers seek and reload.
  extractAndVerifyBigintColumn(sp.partition.get(), 200, 50, data->childAt(0));
  extractAndVerifyBigintColumn(sp.partition.get(), 100, 50, data->childAt(0));
  extractAndVerifyBigintColumn(sp.partition.get(), 0, 50, data->childAt(0));
}

// Test frame larger than cache.
TEST_F(SpillableWindowPartitionTest, frameLargerThanCache) {
  const vector_size_t numRows = 200;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 10, 1 << 20, 20);

  // Request 100 rows - much larger than cache (20 rows).
  auto result = BaseVector::create(BIGINT(), 100, pool_.get());
  sp.partition->extractColumn(0, 50, 100, 0, result);

  auto expected = data->childAt(0)->as<FlatVector<int64_t>>();
  auto actual = result->as<FlatVector<int64_t>>();
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(actual->valueAt(i), expected->valueAt(50 + i));
  }
}

// Test prefetch patterns: sequential, look-ahead, overlapping frames.
TEST_F(SpillableWindowPartitionTest, prefetchPatterns) {
  const vector_size_t numRows = 500;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data, 10, 1 << 20, 150);
  auto expected = data->childAt(0)->as<FlatVector<int64_t>>();

  // Sequential access with look-ahead.
  for (int batch = 0; batch < 8; ++batch) {
    auto batchStart = batch * 50;
    auto frameEnd = std::min(batchStart + 50 + 30, numRows);
    auto size = frameEnd - batchStart;

    auto result = BaseVector::create(BIGINT(), size, pool_.get());
    sp.partition->extractColumn(0, batchStart, size, 0, result);
    EXPECT_EQ(
        result->asFlatVector<int64_t>()->valueAt(0),
        expected->valueAt(batchStart));
  }

  // Backward access after forward.
  extractAndVerifyBigintColumn(sp.partition.get(), 50, 50, data->childAt(0));
  extractAndVerifyBigintColumn(sp.partition.get(), 0, 50, data->childAt(0));

  // Overlapping frame access.
  for (int row = 20; row < 150; row += 10) {
    auto frameStart = row - 10;
    auto frameEnd = std::min(row + 11, numRows);
    auto frameSize = frameEnd - frameStart;

    auto result = BaseVector::create(BIGINT(), frameSize, pool_.get());
    sp.partition->extractColumn(0, frameStart, frameSize, 0, result);
    EXPECT_EQ(
        result->asFlatVector<int64_t>()->valueAt(0),
        expected->valueAt(frameStart));
  }
}

// Test prefetch with peer buffer look-ahead.
TEST_F(SpillableWindowPartitionTest, prefetchWithPeerBufferLookAhead) {
  auto dataWithDups = makeTestDataWithDuplicates(200, 10);
  auto sp = createSpillModePartition(dataWithDups, 10, 1 << 20, 50);

  std::vector<vector_size_t> peerStarts(30);
  std::vector<vector_size_t> peerEnds(30);
  vector_size_t prevStart = 0, prevEnd = 0;

  for (int batch = 0; batch < 5; ++batch) {
    auto batchStart = batch * 30;
    auto batchEnd = std::min(batchStart + 30, 200);
    auto batchSize = batchEnd - batchStart;

    std::tie(prevStart, prevEnd) = sp.partition->computePeerBuffers(
        batchStart,
        batchEnd,
        prevStart,
        prevEnd,
        peerStarts.data(),
        peerEnds.data());

    for (int i = 0; i < batchSize; ++i) {
      auto row = batchStart + i;
      auto expectedGroupStart = (row / 10) * 10;
      auto expectedGroupEnd = expectedGroupStart + 9;
      EXPECT_EQ(peerStarts[i], expectedGroupStart);
      EXPECT_EQ(peerEnds[i], expectedGroupEnd);
    }
  }
}

//=============================================================================
// Batch Processing and Scattered Access Tests
//=============================================================================

// Test scattered row access (non-contiguous row numbers) used by LEAD/LAG.
TEST_F(SpillableWindowPartitionTest, scatteredRowAccess) {
  const vector_size_t numRows = 200;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data);
  auto expected = data->childAt(0)->as<FlatVector<int64_t>>();

  // Test 1: Simple scattered access - every 3rd row.
  {
    std::vector<vector_size_t> rowNumbers;
    for (vector_size_t i = 0; i < numRows; i += 3) {
      rowNumbers.push_back(i);
    }
    auto result = BaseVector::create(BIGINT(), rowNumbers.size(), pool_.get());
    sp.partition->extractColumn(
        0,
        folly::Range<const vector_size_t*>(
            rowNumbers.data(), rowNumbers.size()),
        0,
        result);

    auto actual = result->as<FlatVector<int64_t>>();
    for (size_t i = 0; i < rowNumbers.size(); ++i) {
      EXPECT_EQ(actual->valueAt(i), expected->valueAt(rowNumbers[i]));
    }
  }

  // Test 2: Reverse order access.
  {
    std::vector<vector_size_t> rowNumbers;
    for (vector_size_t i = numRows - 1; i >= 0 && i < numRows; --i) {
      rowNumbers.push_back(i);
    }
    auto result = BaseVector::create(BIGINT(), rowNumbers.size(), pool_.get());
    sp.partition->extractColumn(
        0,
        folly::Range<const vector_size_t*>(
            rowNumbers.data(), rowNumbers.size()),
        0,
        result);

    auto actual = result->as<FlatVector<int64_t>>();
    for (size_t i = 0; i < rowNumbers.size(); ++i) {
      EXPECT_EQ(actual->valueAt(i), expected->valueAt(rowNumbers[i]));
    }
  }

  // Test 3: Random sparse access pattern.
  {
    std::vector<vector_size_t> rowNumbers = {5, 50, 150, 10, 100, 199, 0, 75};
    auto result = BaseVector::create(BIGINT(), rowNumbers.size(), pool_.get());
    sp.partition->extractColumn(
        0,
        folly::Range<const vector_size_t*>(
            rowNumbers.data(), rowNumbers.size()),
        0,
        result);

    auto actual = result->as<FlatVector<int64_t>>();
    for (size_t i = 0; i < rowNumbers.size(); ++i) {
      EXPECT_EQ(actual->valueAt(i), expected->valueAt(rowNumbers[i]));
    }
  }
}

// Test negative row numbers (used by LEAD/LAG for out-of-range values).
TEST_F(SpillableWindowPartitionTest, negativeRowNumbers) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data);

  // Mixed positive and negative row numbers.
  std::vector<vector_size_t> rowNumbers = {
      static_cast<vector_size_t>(-1), // null marker
      0,
      10,
      static_cast<vector_size_t>(-1), // null marker
      50,
      static_cast<vector_size_t>(-1), // null marker
      99};

  auto result = BaseVector::create(BIGINT(), rowNumbers.size(), pool_.get());
  sp.partition->extractColumn(
      0,
      folly::Range<const vector_size_t*>(rowNumbers.data(), rowNumbers.size()),
      0,
      result);

  auto expected = data->childAt(0)->as<FlatVector<int64_t>>();
  // Negative indices should produce nulls.
  EXPECT_TRUE(result->isNullAt(0));
  EXPECT_EQ(
      result->as<FlatVector<int64_t>>()->valueAt(1), expected->valueAt(0));
  EXPECT_EQ(
      result->as<FlatVector<int64_t>>()->valueAt(2), expected->valueAt(10));
  EXPECT_TRUE(result->isNullAt(3));
  EXPECT_EQ(
      result->as<FlatVector<int64_t>>()->valueAt(4), expected->valueAt(50));
  EXPECT_TRUE(result->isNullAt(5));
  EXPECT_EQ(
      result->as<FlatVector<int64_t>>()->valueAt(6), expected->valueAt(99));
}

// Test all negative row numbers (all nulls scenario).
TEST_F(SpillableWindowPartitionTest, allNegativeRowNumbers) {
  const vector_size_t numRows = 50;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data);

  std::vector<vector_size_t> rowNumbers(10, static_cast<vector_size_t>(-1));
  auto result = BaseVector::create(BIGINT(), rowNumbers.size(), pool_.get());
  sp.partition->extractColumn(
      0,
      folly::Range<const vector_size_t*>(rowNumbers.data(), rowNumbers.size()),
      0,
      result);

  for (size_t i = 0; i < rowNumbers.size(); ++i) {
    EXPECT_TRUE(result->isNullAt(i));
  }
}

// Test contiguous range detection optimization.
TEST_F(SpillableWindowPartitionTest, contiguousRangeOptimization) {
  const vector_size_t numRows = 200;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data);
  auto expected = data->childAt(0)->as<FlatVector<int64_t>>();

  // Contiguous range should use optimized path.
  std::vector<vector_size_t> contiguous(50);
  std::iota(contiguous.begin(), contiguous.end(), 25); // rows 25-74

  auto result = BaseVector::create(BIGINT(), contiguous.size(), pool_.get());
  sp.partition->extractColumn(
      0,
      folly::Range<const vector_size_t*>(contiguous.data(), contiguous.size()),
      0,
      result);

  auto actual = result->as<FlatVector<int64_t>>();
  for (size_t i = 0; i < contiguous.size(); ++i) {
    EXPECT_EQ(actual->valueAt(i), expected->valueAt(25 + i));
  }
}

// Test LEAD pattern: access rows offset forward.
TEST_F(SpillableWindowPartitionTest, leadAccessPattern) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data);
  auto expected = data->childAt(0)->as<FlatVector<int64_t>>();

  const int offset = 5;
  std::vector<vector_size_t> rowNumbers;
  for (vector_size_t row = 0; row < numRows; ++row) {
    auto leadRow = row + offset;
    // Use -1 for out of range (LEAD default behavior).
    rowNumbers.push_back(
        leadRow < numRows ? leadRow : static_cast<vector_size_t>(-1));
  }

  auto result = BaseVector::create(BIGINT(), rowNumbers.size(), pool_.get());
  sp.partition->extractColumn(
      0,
      folly::Range<const vector_size_t*>(rowNumbers.data(), rowNumbers.size()),
      0,
      result);

  auto actual = result->as<FlatVector<int64_t>>();
  for (vector_size_t row = 0; row < numRows; ++row) {
    if (row + offset < numRows) {
      EXPECT_EQ(actual->valueAt(row), expected->valueAt(row + offset));
    } else {
      EXPECT_TRUE(result->isNullAt(row));
    }
  }
}

// Test LAG pattern: access rows offset backward.
TEST_F(SpillableWindowPartitionTest, lagAccessPattern) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data);
  auto expected = data->childAt(0)->as<FlatVector<int64_t>>();

  const int offset = 3;
  std::vector<vector_size_t> rowNumbers;
  for (vector_size_t row = 0; row < numRows; ++row) {
    // Use -1 for out of range (LAG default behavior).
    rowNumbers.push_back(
        row >= offset ? row - offset : static_cast<vector_size_t>(-1));
  }

  auto result = BaseVector::create(BIGINT(), rowNumbers.size(), pool_.get());
  sp.partition->extractColumn(
      0,
      folly::Range<const vector_size_t*>(rowNumbers.data(), rowNumbers.size()),
      0,
      result);

  auto actual = result->as<FlatVector<int64_t>>();
  for (vector_size_t row = 0; row < numRows; ++row) {
    if (row >= offset) {
      EXPECT_EQ(actual->valueAt(row), expected->valueAt(row - offset));
    } else {
      EXPECT_TRUE(result->isNullAt(row));
    }
  }
}

// Test extractNulls with large ranges triggering batch processing.
TEST_F(SpillableWindowPartitionTest, extractNullsLargeRange) {
  const vector_size_t numRows = 500;
  // Create data with nulls at specific positions.
  auto dataWithNulls = makeRowVector(
      {"c0", "c1", "c2"},
      {
          makeFlatVector<int64_t>(
              numRows,
              [](auto row) { return row * 10; },
              [](auto row) { return row % 7 == 0; }), // Every 7th row is null.
          makeFlatVector<int32_t>(numRows, [](auto row) { return row; }),
          makeFlatVector<StringView>(
              numRows,
              [](auto row) {
                return StringView::makeInline(fmt::format("row_{}", row));
              }),
      });

  auto mp = createMemoryModePartition(dataWithNulls);
  mp.partition->spill();

  // Extract nulls for the entire range.
  auto nulls = AlignedBuffer::allocate<bool>(numRows, pool_.get(), false);
  mp.partition->extractNulls(0, 0, numRows, nulls);

  auto* rawNulls = nulls->as<uint64_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    EXPECT_EQ(bits::isBitSet(rawNulls, i), (i % 7 == 0))
        << "Mismatch at row " << i;
  }
}

// Test extractNulls with frame-based access pattern.
TEST_F(SpillableWindowPartitionTest, extractNullsWithFrames) {
  const vector_size_t numRows = 200;
  auto dataWithNulls = makeRowVector(
      {"c0", "c1", "c2"},
      {
          makeFlatVector<int64_t>(
              numRows,
              [](auto row) { return row * 10; },
              [](auto row) { return row % 10 == 0; }),
          makeFlatVector<int32_t>(numRows, [](auto row) { return row; }),
          makeFlatVector<StringView>(
              numRows,
              [](auto row) {
                return StringView::makeInline(fmt::format("row_{}", row));
              }),
      });

  auto mp = createMemoryModePartition(dataWithNulls);
  mp.partition->spill();

  // Create frame buffers for testing.
  auto frameStarts = AlignedBuffer::allocate<vector_size_t>(50, pool_.get());
  auto frameEnds = AlignedBuffer::allocate<vector_size_t>(50, pool_.get());
  auto* rawStarts = frameStarts->asMutable<vector_size_t>();
  auto* rawEnds = frameEnds->asMutable<vector_size_t>();

  // Set up sliding window frames: frame[i] = [max(0, i*4-10), min(numRows,
  // i*4+10)]
  for (int i = 0; i < 50; ++i) {
    rawStarts[i] = std::max(0, i * 4 - 10);
    rawEnds[i] = std::min(static_cast<int>(numRows), i * 4 + 10);
  }

  SelectivityVector validRows(50, true);
  BufferPtr nulls;
  auto result =
      mp.partition->extractNulls(0, validRows, frameStarts, frameEnds, &nulls);

  EXPECT_TRUE(result.has_value());
  auto [minRow, nullCount] = *result;
  EXPECT_GE(nullCount, 1);
}

// Combined test for scattered access with different column types.
TEST_F(SpillableWindowPartitionTest, scatteredAccessMultipleColumns) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data);

  std::vector<vector_size_t> rowNumbers = {10, 30, 50, 70, 90};

  // Test BIGINT column (c0).
  {
    auto result = BaseVector::create(BIGINT(), rowNumbers.size(), pool_.get());
    sp.partition->extractColumn(
        0,
        folly::Range<const vector_size_t*>(
            rowNumbers.data(), rowNumbers.size()),
        0,
        result);
    auto expected = data->childAt(0)->as<FlatVector<int64_t>>();
    auto actual = result->as<FlatVector<int64_t>>();
    for (size_t i = 0; i < rowNumbers.size(); ++i) {
      EXPECT_EQ(actual->valueAt(i), expected->valueAt(rowNumbers[i]));
    }
  }

  // Test INTEGER column (c1).
  {
    auto result = BaseVector::create(INTEGER(), rowNumbers.size(), pool_.get());
    sp.partition->extractColumn(
        1,
        folly::Range<const vector_size_t*>(
            rowNumbers.data(), rowNumbers.size()),
        0,
        result);
    auto expected = data->childAt(1)->as<FlatVector<int32_t>>();
    auto actual = result->as<FlatVector<int32_t>>();
    for (size_t i = 0; i < rowNumbers.size(); ++i) {
      EXPECT_EQ(actual->valueAt(i), expected->valueAt(rowNumbers[i]));
    }
  }

  // Test VARCHAR column (c2).
  {
    auto result = BaseVector::create(VARCHAR(), rowNumbers.size(), pool_.get());
    sp.partition->extractColumn(
        2,
        folly::Range<const vector_size_t*>(
            rowNumbers.data(), rowNumbers.size()),
        0,
        result);
    auto expected = data->childAt(2)->as<FlatVector<StringView>>();
    auto actual = result->as<FlatVector<StringView>>();
    for (size_t i = 0; i < rowNumbers.size(); ++i) {
      EXPECT_EQ(
          actual->valueAt(i).str(), expected->valueAt(rowNumbers[i]).str());
    }
  }
}

// Test scattered access with nulls in data.
TEST_F(SpillableWindowPartitionTest, scatteredAccessWithNulls) {
  const vector_size_t numRows = 50;
  auto dataWithNulls = makeRowVector(
      {"c0", "c1", "c2"},
      {
          makeFlatVector<int64_t>(
              numRows,
              [](auto row) { return row * 10; },
              [](auto row) { return row % 5 == 0; }), // Every 5th is null.
          makeFlatVector<int32_t>(numRows, [](auto row) { return row; }),
          makeFlatVector<StringView>(
              numRows,
              [](auto row) {
                return StringView::makeInline(fmt::format("row_{}", row));
              }),
      });

  auto mp = createMemoryModePartition(dataWithNulls);
  mp.partition->spill();

  // Access rows including nulls.
  std::vector<vector_size_t> rowNumbers = {0, 5, 10, 15, 20, 3, 7, 12};
  auto result = BaseVector::create(BIGINT(), rowNumbers.size(), pool_.get());
  mp.partition->extractColumn(
      0,
      folly::Range<const vector_size_t*>(rowNumbers.data(), rowNumbers.size()),
      0,
      result);

  for (size_t i = 0; i < rowNumbers.size(); ++i) {
    auto row = rowNumbers[i];
    if (row % 5 == 0) {
      EXPECT_TRUE(result->isNullAt(i)) << "Expected null at index " << i;
    } else {
      EXPECT_FALSE(result->isNullAt(i)) << "Unexpected null at index " << i;
      EXPECT_EQ(result->as<FlatVector<int64_t>>()->valueAt(i), row * 10);
    }
  }
}

// Test empty row numbers array.
TEST_F(SpillableWindowPartitionTest, emptyRowNumbers) {
  const vector_size_t numRows = 50;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data);

  std::vector<vector_size_t> rowNumbers;
  auto result = BaseVector::create(BIGINT(), 0, pool_.get());
  // Should not crash with empty input.
  sp.partition->extractColumn(
      0,
      folly::Range<const vector_size_t*>(rowNumbers.data(), rowNumbers.size()),
      0,
      result);
  EXPECT_EQ(result->size(), 0);
}

// Test single row scattered access.
TEST_F(SpillableWindowPartitionTest, singleRowScatteredAccess) {
  const vector_size_t numRows = 100;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data);
  auto expected = data->childAt(0)->as<FlatVector<int64_t>>();

  for (vector_size_t row = 0; row < numRows; row += 17) {
    std::vector<vector_size_t> rowNumbers = {row};
    auto result = BaseVector::create(BIGINT(), 1, pool_.get());
    sp.partition->extractColumn(
        0,
        folly::Range<const vector_size_t*>(
            rowNumbers.data(), rowNumbers.size()),
        0,
        result);
    EXPECT_EQ(
        result->as<FlatVector<int64_t>>()->valueAt(0), expected->valueAt(row));
  }
}

// Test result offset parameter for scattered access.
TEST_F(SpillableWindowPartitionTest, scatteredAccessWithResultOffset) {
  const vector_size_t numRows = 50;
  auto data = makeTestData(numRows);
  auto sp = createSpillModePartition(data);
  auto expected = data->childAt(0)->as<FlatVector<int64_t>>();

  std::vector<vector_size_t> rowNumbers = {5, 15, 25, 35, 45};
  const vector_size_t resultOffset = 10;

  auto result = BaseVector::create(
      BIGINT(), resultOffset + rowNumbers.size(), pool_.get());
  // Initialize with sentinel values.
  auto* resultFlat = result->asFlatVector<int64_t>();
  for (int i = 0; i < resultOffset; ++i) {
    resultFlat->set(i, -999);
  }

  sp.partition->extractColumn(
      0,
      folly::Range<const vector_size_t*>(rowNumbers.data(), rowNumbers.size()),
      resultOffset,
      result);

  // Verify sentinel values unchanged.
  for (int i = 0; i < resultOffset; ++i) {
    EXPECT_EQ(resultFlat->valueAt(i), -999);
  }
  // Verify extracted values at offset.
  for (size_t i = 0; i < rowNumbers.size(); ++i) {
    EXPECT_EQ(
        resultFlat->valueAt(resultOffset + i),
        expected->valueAt(rowNumbers[i]));
  }
}

} // namespace facebook::velox::exec::test
