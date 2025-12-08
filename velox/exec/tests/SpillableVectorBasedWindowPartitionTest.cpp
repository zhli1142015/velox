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

#include "velox/exec/SpillableVectorBasedWindowPartition.h"
#include <gtest/gtest.h>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/serializers/PrestoSerializer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

class SpillableVectorBasedWindowPartitionTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    filesystems::registerLocalFileSystem();
    if (!isRegisteredVectorSerde()) {
      serializer::presto::PrestoVectorSerde::registerVectorSerde();
    }
    rng_.seed(42);
    spillStats_ = std::make_unique<folly::Synchronized<common::SpillStats>>();
  }

  void TearDown() override {
    spillStats_.reset();
    OperatorTestBase::TearDown();
  }

  // --- Test Data Creation Helpers ---

  /// Create simple test data with consecutive values.
  RowVectorPtr makeTestData(vector_size_t numRows) {
    return makeRowVector({
        makeFlatVector<int64_t>(numRows, [](auto row) { return row; }),
        makeFlatVector<double>(numRows, [](auto row) { return row * 1.5; }),
        makeFlatVector<std::string>(
            numRows, [](auto row) { return fmt::format("str_{}", row); }),
    });
  }

  /// Create test data with duplicate values for peer testing.
  RowVectorPtr makeTestDataWithDuplicates(
      vector_size_t numRows,
      vector_size_t groupSize) {
    return makeRowVector({
        makeFlatVector<int64_t>(
            numRows, [groupSize](auto row) { return row / groupSize; }),
        makeFlatVector<double>(
            numRows, [groupSize](auto row) { return (row / groupSize) * 1.5; }),
        makeFlatVector<std::string>(
            numRows,
            [groupSize](auto row) {
              return fmt::format("str_{}", row / groupSize);
            }),
    });
  }

  /// Create test data with nulls based on a predicate.
  RowVectorPtr makeTestDataWithNulls(
      vector_size_t numRows,
      std::function<bool(vector_size_t)> isNullFn) {
    return makeRowVector({
        makeFlatVector<int64_t>(
            numRows, [](auto row) { return row; }, isNullFn),
        makeFlatVector<double>(
            numRows, [](auto row) { return row * 1.5; }, isNullFn),
        makeFlatVector<std::string>(
            numRows,
            [](auto row) { return fmt::format("str_{}", row); },
            isNullFn),
    });
  }

  /// Create blocks from a RowVector.
  std::vector<RowBlock> createBlocks(
      const RowVectorPtr& data,
      vector_size_t batchSize) {
    std::vector<RowBlock> blocks;
    vector_size_t offset = 0;
    while (offset < data->size()) {
      vector_size_t numRows = std::min(batchSize, data->size() - offset);
      blocks.push_back(RowBlock{data, offset, offset + numRows});
      offset += numRows;
    }
    return blocks;
  }

  // --- SpillConfig Creation Helper ---

  common::SpillConfig makeSpillConfig() {
    return common::SpillConfig(
        [this]() -> const std::string& { return tempSpillPath_; },
        [](uint64_t) {},
        "test",
        0, // maxFileSize
        0, // writeBufferSize
        10 << 20, // readBufferSize (10 MB)
        nullptr, // executor
        10, // minSpillableReservationPct
        10, // spillableReservationGrowthPct
        0, // startPartitionBit
        0, // numPartitionBits
        0, // maxSpillLevel
        0, // maxSpillRunRows
        0, // writerFlushThresholdSize
        "none"); // compressionKind
  }

  // --- Partition Creation Helpers ---

  std::unique_ptr<SpillableVectorBasedWindowPartition>
  createMemoryModePartition(
      const std::vector<RowBlock>& blocks,
      const std::string& spillDir = "") {
    RowTypePtr rowType;
    if (blocks.empty()) {
      // For empty partition, create with simple type.
      rowType = makeRowType({BIGINT(), DOUBLE(), VARCHAR()});
    } else {
      rowType =
          std::dynamic_pointer_cast<const RowType>(blocks[0].input->type());
    }
    return createMemoryModePartitionWithType(blocks, rowType, spillDir);
  }

  std::unique_ptr<SpillableVectorBasedWindowPartition>
  createMemoryModePartitionWithType(
      const std::vector<RowBlock>& blocks,
      const RowTypePtr& rowType,
      const std::string& spillDir) {
    std::vector<column_index_t> inputMapping;
    for (size_t i = 0; i < rowType->size(); ++i) {
      inputMapping.push_back(i);
    }

    std::vector<std::pair<column_index_t, core::SortOrder>> sortKeyInfo;
    sortKeyInfo.push_back({0, core::SortOrder(true, true)});

    const common::SpillConfig* spillConfigPtr = nullptr;
    if (!spillDir.empty()) {
      tempSpillPath_ = spillDir; // Store the path in a member variable
      tempSpillConfig_ =
          std::make_unique<common::SpillConfig>(makeSpillConfig());
      spillConfigPtr = tempSpillConfig_.get();
    }

    return std::make_unique<SpillableVectorBasedWindowPartition>(
        blocks,
        inputMapping,
        sortKeyInfo,
        rowType,
        spillConfigPtr,
        spillStats_.get(),
        pool());
  }

  std::pair<
      std::unique_ptr<SpillableVectorBasedWindowPartition>,
      std::shared_ptr<TempDirectoryPath>>
  createSpilledPartition(const std::vector<RowBlock>& blocks) {
    auto tempDir = exec::test::TempDirectoryPath::create();
    auto partition = createMemoryModePartition(blocks, tempDir->getPath());
    partition->spill();
    return {std::move(partition), tempDir};
  }

  // --- Verification Helpers ---

  template <typename T>
  void verifyColumnValues(
      const VectorPtr& result,
      const VectorPtr& expected,
      vector_size_t startRow = 0,
      vector_size_t count = std::numeric_limits<vector_size_t>::max()) {
    auto flatResult = result->as<FlatVector<T>>();
    auto flatExpected = expected->as<FlatVector<T>>();
    ASSERT_NE(flatResult, nullptr);
    ASSERT_NE(flatExpected, nullptr);

    vector_size_t endRow = std::min(
        startRow + count,
        std::min(
            startRow + static_cast<vector_size_t>(result->size()),
            static_cast<vector_size_t>(flatExpected->size())));

    for (vector_size_t i = 0; i < endRow - startRow; ++i) {
      vector_size_t expectedIdx = startRow + i;
      bool resultNull = flatResult->isNullAt(i);
      bool expectedNull = flatExpected->isNullAt(expectedIdx);
      ASSERT_EQ(resultNull, expectedNull)
          << "Null mismatch at position " << i << " (expected row "
          << expectedIdx << ")";
      if (!resultNull) {
        EXPECT_EQ(flatResult->valueAt(i), flatExpected->valueAt(expectedIdx))
            << "Value mismatch at position " << i << " (expected row "
            << expectedIdx << ")";
      }
    }
  }

  void verifyStringValues(
      const VectorPtr& result,
      const VectorPtr& expected,
      vector_size_t startRow = 0,
      vector_size_t count = std::numeric_limits<vector_size_t>::max()) {
    auto flatResult = result->asFlatVector<StringView>();
    auto flatExpected = expected->asFlatVector<StringView>();
    ASSERT_NE(flatResult, nullptr);
    ASSERT_NE(flatExpected, nullptr);

    vector_size_t endRow = std::min(
        startRow + count,
        std::min(
            startRow + static_cast<vector_size_t>(result->size()),
            static_cast<vector_size_t>(flatExpected->size())));

    for (vector_size_t i = 0; i < endRow - startRow; ++i) {
      vector_size_t expectedIdx = startRow + i;
      bool resultNull = flatResult->isNullAt(i);
      bool expectedNull = flatExpected->isNullAt(expectedIdx);
      ASSERT_EQ(resultNull, expectedNull)
          << "Null mismatch at position " << i << " (expected row "
          << expectedIdx << ")";
      if (!resultNull) {
        EXPECT_EQ(
            flatResult->valueAt(i).str(),
            flatExpected->valueAt(expectedIdx).str())
            << "Value mismatch at position " << i << " (expected row "
            << expectedIdx << ")";
      }
    }
  }

  /// Verify extracted data matches expected data for all columns.
  void verifyExtractedData(
      const RowVectorPtr& result,
      const RowVectorPtr& expected,
      vector_size_t startRow,
      vector_size_t count = std::numeric_limits<vector_size_t>::max()) {
    verifyColumnValues<int64_t>(
        result->childAt(0), expected->childAt(0), startRow, count);
    verifyColumnValues<double>(
        result->childAt(1), expected->childAt(1), startRow, count);
    verifyStringValues(
        result->childAt(2), expected->childAt(2), startRow, count);
  }

  /// Run extraction test in both memory and spill modes.
  void runExtractTest(
      const RowVectorPtr& data,
      vector_size_t batchSize,
      const std::vector<std::pair<vector_size_t, vector_size_t>>&
          extractRanges) {
    auto blocks = createBlocks(data, batchSize);

    // Helper to pre-allocate result vectors
    auto allocateResults = [&](vector_size_t count) {
      std::vector<VectorPtr> result(3);
      result[0] = BaseVector::create(BIGINT(), count, pool());
      result[1] = BaseVector::create(DOUBLE(), count, pool());
      result[2] = BaseVector::create(VARCHAR(), count, pool());
      return result;
    };

    // Test memory mode
    {
      SCOPED_TRACE("Memory mode");
      auto partition = createMemoryModePartition(blocks);
      for (const auto& [start, count] : extractRanges) {
        auto result = allocateResults(count);
        partition->extractColumn(0, start, count, 0, result[0]);
        partition->extractColumn(1, start, count, 0, result[1]);
        partition->extractColumn(2, start, count, 0, result[2]);

        auto rowResult = makeRowVector(result);
        verifyExtractedData(rowResult, data, start, count);
      }
    }

    // Test spill mode
    {
      SCOPED_TRACE("Spill mode");
      auto [partition, tempDir] = createSpilledPartition(blocks);
      for (const auto& [start, count] : extractRanges) {
        auto result = allocateResults(count);
        partition->extractColumn(0, start, count, 0, result[0]);
        partition->extractColumn(1, start, count, 0, result[1]);
        partition->extractColumn(2, start, count, 0, result[2]);

        auto rowResult = makeRowVector(result);
        verifyExtractedData(rowResult, data, start, count);
      }
    }
  }

  /// Run null extraction test in both modes.
  void runExtractNullsTest(
      const RowVectorPtr& data,
      vector_size_t batchSize,
      const std::vector<std::pair<vector_size_t, vector_size_t>>&
          extractRanges) {
    auto blocks = createBlocks(data, batchSize);

    auto verifyNulls = [&](SpillableVectorBasedWindowPartition* partition,
                           vector_size_t start,
                           vector_size_t count) {
      auto nulls = AlignedBuffer::allocate<bool>(count, pool());
      auto* rawNulls = nulls->asMutable<uint64_t>();

      // Test each column
      for (column_index_t col = 0; col < 3; ++col) {
        partition->extractNulls(col, start, count, nulls);
        auto expectedCol = data->childAt(col);
        for (vector_size_t i = 0; i < count; ++i) {
          bool isNull = bits::isBitNull(rawNulls, i);
          bool expectedNull = expectedCol->isNullAt(start + i);
          EXPECT_EQ(isNull, expectedNull)
              << "Column " << col << " null mismatch at position " << i
              << " (row " << start + i << ")";
        }
      }
    };

    // Test memory mode
    {
      SCOPED_TRACE("Memory mode");
      auto partition = createMemoryModePartition(blocks);
      for (const auto& [start, count] : extractRanges) {
        verifyNulls(partition.get(), start, count);
      }
    }

    // Test spill mode
    {
      SCOPED_TRACE("Spill mode");
      auto [partition, tempDir] = createSpilledPartition(blocks);
      for (const auto& [start, count] : extractRanges) {
        verifyNulls(partition.get(), start, count);
      }
    }
  }

  folly::Random::DefaultGenerator rng_;
  std::unique_ptr<folly::Synchronized<common::SpillStats>> spillStats_;
  std::unique_ptr<common::SpillConfig> tempSpillConfig_;
  std::string
      tempSpillPath_; // Store path to ensure it outlives the SpillConfig
};

// =============================================================================
// Boundary Condition Tests
// =============================================================================

TEST_F(SpillableVectorBasedWindowPartitionTest, emptyPartition) {
  // Create an empty data vector (0 rows)
  auto data = makeTestData(0);
  auto blocks = createBlocks(data, 10);

  EXPECT_TRUE(blocks.empty());

  auto partition = createMemoryModePartition(blocks);

  EXPECT_EQ(partition->numRows(), 0);
}

TEST_F(SpillableVectorBasedWindowPartitionTest, singleRow) {
  auto data = makeTestData(1);
  runExtractTest(data, 10, {{0, 1}});
}

TEST_F(SpillableVectorBasedWindowPartitionTest, singleRowWithNull) {
  auto data = makeTestDataWithNulls(1, [](auto) { return true; });
  runExtractTest(data, 10, {{0, 1}});
  runExtractNullsTest(data, 10, {{0, 1}});
}

TEST_F(SpillableVectorBasedWindowPartitionTest, exactBlockBoundary) {
  // Create data that fills exactly one block
  constexpr vector_size_t kBlockSize = 100;
  auto data = makeTestData(kBlockSize);
  auto blocks = createBlocks(data, kBlockSize);

  EXPECT_EQ(blocks.size(), 1);

  // Test extract at exact boundary
  runExtractTest(
      data,
      kBlockSize,
      {
          {0, kBlockSize}, // Full block
          {0, 1}, // First element
          {kBlockSize - 1, 1}, // Last element
      });
}

TEST_F(SpillableVectorBasedWindowPartitionTest, extractAtBlockBoundaries) {
  constexpr vector_size_t kBlockSize = 50;
  constexpr vector_size_t kNumRows = 200;
  auto data = makeTestData(kNumRows);

  // Test extractions that cross block boundaries
  runExtractTest(
      data,
      kBlockSize,
      {
          {45, 10}, // Cross first boundary (50)
          {95, 10}, // Cross second boundary (100)
          {145, 10}, // Cross third boundary (150)
          {40, 70}, // Span multiple blocks
          {0, 200}, // All rows
      });
}

TEST_F(SpillableVectorBasedWindowPartitionTest, extractZeroRows) {
  auto data = makeTestData(100);
  auto blocks = createBlocks(data, 50);
  auto partition = createMemoryModePartition(blocks);

  auto result = BaseVector::create(BIGINT(), 0, pool());
  partition->extractColumn(0, 50, 0, 0, result);
  EXPECT_EQ(result->size(), 0);
}

// =============================================================================
// Null Pattern Tests
// =============================================================================

TEST_F(SpillableVectorBasedWindowPartitionTest, allNulls) {
  auto data = makeTestDataWithNulls(100, [](auto) { return true; });
  runExtractTest(data, 25, {{0, 50}, {50, 50}});
  runExtractNullsTest(data, 25, {{0, 50}, {50, 50}});
}

TEST_F(SpillableVectorBasedWindowPartitionTest, noNulls) {
  auto data = makeTestDataWithNulls(100, [](auto) { return false; });
  runExtractTest(data, 25, {{0, 50}, {50, 50}});
  runExtractNullsTest(data, 25, {{0, 50}, {50, 50}});
}

TEST_F(SpillableVectorBasedWindowPartitionTest, alternatingNulls) {
  auto data = makeTestDataWithNulls(100, [](auto row) { return row % 2 == 0; });
  runExtractTest(data, 25, {{0, 100}});
  runExtractNullsTest(data, 25, {{0, 100}});
}

TEST_F(SpillableVectorBasedWindowPartitionTest, nullsAtBoundaries) {
  // Nulls at block boundaries
  constexpr vector_size_t kBlockSize = 50;
  auto data = makeTestDataWithNulls(200, [=](auto row) {
    return row == 0 || row == 49 || row == 50 || row == 99 || row == 100 ||
        row == 149 || row == 150 || row == 199;
  });

  runExtractTest(data, kBlockSize, {{0, 200}});
  runExtractNullsTest(data, kBlockSize, {{0, 200}});
}

TEST_F(SpillableVectorBasedWindowPartitionTest, sparseNulls) {
  // Very few nulls scattered throughout
  auto data = makeTestDataWithNulls(1000, [](auto row) {
    return row == 7 || row == 123 || row == 456 || row == 789 || row == 999;
  });

  runExtractTest(data, 100, {{0, 500}, {500, 500}});
  runExtractNullsTest(data, 100, {{0, 500}, {500, 500}});
}

// =============================================================================
// Peer Buffer Tests
// =============================================================================

TEST_F(SpillableVectorBasedWindowPartitionTest, peerBuffersDistinctValues) {
  auto data = makeTestData(100);
  auto blocks = createBlocks(data, 25);

  auto testPeers = [&](SpillableVectorBasedWindowPartition* partition) {
    auto peerStarts = AlignedBuffer::allocate<vector_size_t>(100, pool());
    auto peerEnds = AlignedBuffer::allocate<vector_size_t>(100, pool());
    auto* rawStarts = peerStarts->asMutable<vector_size_t>();
    auto* rawEnds = peerEnds->asMutable<vector_size_t>();

    partition->computePeerBuffers(0, 100, 0, 0, rawStarts, rawEnds);

    // With distinct values, each row is its own peer group
    // peerEnd is inclusive, so each row's end is itself
    for (vector_size_t i = 0; i < 100; ++i) {
      EXPECT_EQ(rawStarts[i], i) << "Peer start mismatch at " << i;
      EXPECT_EQ(rawEnds[i], i) << "Peer end mismatch at " << i;
    }
  };

  // Memory mode
  {
    SCOPED_TRACE("Memory mode");
    auto partition = createMemoryModePartition(blocks);
    testPeers(partition.get());
  }

  // Spill mode
  {
    SCOPED_TRACE("Spill mode");
    auto [partition, tempDir] = createSpilledPartition(blocks);
    testPeers(partition.get());
  }
}

TEST_F(SpillableVectorBasedWindowPartitionTest, peerBuffersDuplicates) {
  // Groups of 5 identical values
  auto data = makeTestDataWithDuplicates(100, 5);
  auto blocks = createBlocks(data, 25);

  auto testPeers = [&](SpillableVectorBasedWindowPartition* partition) {
    auto peerStarts = AlignedBuffer::allocate<vector_size_t>(100, pool());
    auto peerEnds = AlignedBuffer::allocate<vector_size_t>(100, pool());
    auto* rawStarts = peerStarts->asMutable<vector_size_t>();
    auto* rawEnds = peerEnds->asMutable<vector_size_t>();

    partition->computePeerBuffers(0, 100, 0, 0, rawStarts, rawEnds);

    // peerEnd is inclusive
    for (vector_size_t i = 0; i < 100; ++i) {
      vector_size_t expectedStart = (i / 5) * 5;
      vector_size_t expectedEnd = expectedStart + 4; // Inclusive end
      EXPECT_EQ(rawStarts[i], expectedStart) << "Peer start mismatch at " << i;
      EXPECT_EQ(rawEnds[i], expectedEnd) << "Peer end mismatch at " << i;
    }
  };

  // Memory mode
  {
    SCOPED_TRACE("Memory mode");
    auto partition = createMemoryModePartition(blocks);
    testPeers(partition.get());
  }

  // Spill mode
  {
    SCOPED_TRACE("Spill mode");
    auto [partition, tempDir] = createSpilledPartition(blocks);
    testPeers(partition.get());
  }
}

TEST_F(SpillableVectorBasedWindowPartitionTest, peerBuffersCrossBlockBoundary) {
  // Create peer groups that span block boundaries
  constexpr vector_size_t kBlockSize = 50;
  constexpr vector_size_t kGroupSize = 7; // Doesn't divide evenly
  auto data = makeTestDataWithDuplicates(105, kGroupSize);
  auto blocks = createBlocks(data, kBlockSize);

  auto testPeers = [&](SpillableVectorBasedWindowPartition* partition) {
    auto peerStarts = AlignedBuffer::allocate<vector_size_t>(105, pool());
    auto peerEnds = AlignedBuffer::allocate<vector_size_t>(105, pool());
    auto* rawStarts = peerStarts->asMutable<vector_size_t>();
    auto* rawEnds = peerEnds->asMutable<vector_size_t>();

    partition->computePeerBuffers(0, 105, 0, 0, rawStarts, rawEnds);

    // peerEnd is inclusive
    for (vector_size_t i = 0; i < 105; ++i) {
      vector_size_t expectedStart = (i / kGroupSize) * kGroupSize;
      // Inclusive end: min(start + groupSize - 1, 104)
      vector_size_t expectedEnd = std::min(
          expectedStart + kGroupSize - 1, static_cast<vector_size_t>(104));
      EXPECT_EQ(rawStarts[i], expectedStart) << "Peer start mismatch at " << i;
      EXPECT_EQ(rawEnds[i], expectedEnd) << "Peer end mismatch at " << i;
    }
  };

  auto partition = createMemoryModePartition(blocks);
  testPeers(partition.get());

  auto [spilledPartition, tempDir] = createSpilledPartition(blocks);
  testPeers(spilledPartition.get());
}

// =============================================================================
// Random Access Pattern Tests
// =============================================================================

TEST_F(SpillableVectorBasedWindowPartitionTest, randomAccessPattern) {
  auto data = makeTestData(500);

  // Random access patterns
  std::vector<std::pair<vector_size_t, vector_size_t>> ranges;
  ranges.push_back({300, 50}); // Start from middle
  ranges.push_back({50, 100}); // Jump back
  ranges.push_back({400, 100}); // Jump forward
  ranges.push_back({0, 50}); // Back to start
  ranges.push_back({250, 100}); // Back to middle

  runExtractTest(data, 50, ranges);
}

TEST_F(SpillableVectorBasedWindowPartitionTest, backwardAccess) {
  auto data = makeTestData(200);

  // Access in reverse order
  std::vector<std::pair<vector_size_t, vector_size_t>> ranges;
  for (int i = 3; i >= 0; --i) {
    ranges.push_back({static_cast<vector_size_t>(i * 50), 50});
  }

  runExtractTest(data, 50, ranges);
}

TEST_F(SpillableVectorBasedWindowPartitionTest, interleavedAccess) {
  auto data = makeTestData(400);

  // Interleaved access pattern
  runExtractTest(
      data,
      50,
      {
          {0, 50}, // Block 0
          {200, 50}, // Block 4
          {50, 50}, // Block 1
          {250, 50}, // Block 5
          {100, 50}, // Block 2
          {300, 50}, // Block 6
          {150, 50}, // Block 3
          {350, 50}, // Block 7
      });
}

// =============================================================================
// Spill Transition Tests
// =============================================================================

TEST_F(SpillableVectorBasedWindowPartitionTest, spillTransitionBasic) {
  auto data = makeTestData(200);
  auto blocks = createBlocks(data, 50);
  auto tempDir = exec::test::TempDirectoryPath::create();

  // Helper to allocate result vectors
  auto allocateResults = [&](vector_size_t count) {
    std::vector<VectorPtr> result(3);
    result[0] = BaseVector::create(BIGINT(), count, pool());
    result[1] = BaseVector::create(DOUBLE(), count, pool());
    result[2] = BaseVector::create(VARCHAR(), count, pool());
    return result;
  };

  // Create partition in memory mode first
  auto partition = createMemoryModePartition(blocks, tempDir->getPath());

  // Verify data before spill
  auto beforeSpill = allocateResults(100);
  partition->extractColumn(0, 0, 100, 0, beforeSpill[0]);
  partition->extractColumn(1, 0, 100, 0, beforeSpill[1]);
  partition->extractColumn(2, 0, 100, 0, beforeSpill[2]);

  auto beforeResult = makeRowVector(beforeSpill);
  verifyExtractedData(beforeResult, data, 0, 100);

  // Trigger spill
  partition->spill();
  EXPECT_TRUE(partition->isSpilled());

  // Verify data after spill
  auto afterSpill = allocateResults(100);
  partition->extractColumn(0, 0, 100, 0, afterSpill[0]);
  partition->extractColumn(1, 0, 100, 0, afterSpill[1]);
  partition->extractColumn(2, 0, 100, 0, afterSpill[2]);

  auto afterResult = makeRowVector(afterSpill);
  verifyExtractedData(afterResult, data, 0, 100);

  // Also verify later rows
  auto laterRows = allocateResults(100);
  partition->extractColumn(0, 100, 100, 0, laterRows[0]);
  partition->extractColumn(1, 100, 100, 0, laterRows[1]);
  partition->extractColumn(2, 100, 100, 0, laterRows[2]);

  auto laterResult = makeRowVector(laterRows);
  verifyExtractedData(laterResult, data, 100, 100);
}

TEST_F(SpillableVectorBasedWindowPartitionTest, spillTransitionWithNulls) {
  auto data = makeTestDataWithNulls(200, [](auto row) { return row % 3 == 0; });
  auto blocks = createBlocks(data, 50);
  auto tempDir = exec::test::TempDirectoryPath::create();

  auto partition = createMemoryModePartition(blocks, tempDir->getPath());

  // Verify nulls before spill
  auto verifyNullsBefore = AlignedBuffer::allocate<bool>(100, pool());
  partition->extractNulls(0, 0, 100, verifyNullsBefore);

  // Spill
  partition->spill();

  // Verify nulls after spill
  auto verifyNullsAfter = AlignedBuffer::allocate<bool>(100, pool());
  partition->extractNulls(0, 0, 100, verifyNullsAfter);

  auto* beforeRaw = verifyNullsBefore->as<uint64_t>();
  auto* afterRaw = verifyNullsAfter->as<uint64_t>();

  for (vector_size_t i = 0; i < 100; ++i) {
    EXPECT_EQ(bits::isBitNull(beforeRaw, i), bits::isBitNull(afterRaw, i))
        << "Null mismatch at row " << i;
  }
}

// =============================================================================
// Large Data Tests
// =============================================================================

TEST_F(SpillableVectorBasedWindowPartitionTest, largePartition) {
  constexpr vector_size_t kNumRows = 10000;
  auto data = makeTestData(kNumRows);

  // Test with various block sizes
  runExtractTest(
      data,
      100,
      {
          {0, 1000},
          {5000, 1000},
          {9000, 1000},
          {0, kNumRows},
      });
}

TEST_F(SpillableVectorBasedWindowPartitionTest, manySmallBlocks) {
  constexpr vector_size_t kNumRows = 1000;
  constexpr vector_size_t kBlockSize = 10;
  auto data = makeTestData(kNumRows);

  // 100 blocks
  runExtractTest(
      data,
      kBlockSize,
      {
          {0, 100},
          {500, 100},
          {0, kNumRows},
      });
}

TEST_F(SpillableVectorBasedWindowPartitionTest, fewLargeBlocks) {
  constexpr vector_size_t kNumRows = 1000;
  constexpr vector_size_t kBlockSize = 500;
  auto data = makeTestData(kNumRows);

  // 2 blocks
  runExtractTest(
      data,
      kBlockSize,
      {
          {0, 500},
          {500, 500},
          {0, kNumRows},
      });
}

// =============================================================================
// Result Buffer Offset Tests
// =============================================================================

TEST_F(SpillableVectorBasedWindowPartitionTest, extractWithResultOffset) {
  auto data = makeTestData(100);
  auto blocks = createBlocks(data, 25);

  auto testOffset = [&](SpillableVectorBasedWindowPartition* partition) {
    // Pre-allocate a larger result buffer
    auto result = makeFlatVector<int64_t>(150);

    // Fill with sentinel values
    for (vector_size_t i = 0; i < 150; ++i) {
      result->set(i, -999);
    }

    VectorPtr resultPtr = result;

    // Extract 50 rows starting at partition row 0, put at result offset 50
    partition->extractColumn(0, 0, 50, 50, resultPtr);

    auto flatResult = resultPtr->as<FlatVector<int64_t>>();

    // First 50 should be untouched (-999)
    for (vector_size_t i = 0; i < 50; ++i) {
      EXPECT_EQ(flatResult->valueAt(i), -999);
    }

    // Next 50 should be extracted values
    for (vector_size_t i = 50; i < 100; ++i) {
      EXPECT_EQ(flatResult->valueAt(i), i - 50);
    }

    // Last 50 should be untouched (-999)
    for (vector_size_t i = 100; i < 150; ++i) {
      EXPECT_EQ(flatResult->valueAt(i), -999);
    }
  };

  auto partition = createMemoryModePartition(blocks);
  testOffset(partition.get());

  auto [spilledPartition, tempDir] = createSpilledPartition(blocks);
  testOffset(spilledPartition.get());
}

// =============================================================================
// Combination Tests
// =============================================================================

TEST_F(SpillableVectorBasedWindowPartitionTest, multipleOperationsSequence) {
  auto data = makeTestDataWithNulls(500, [](auto row) { return row % 7 == 0; });
  auto blocks = createBlocks(data, 100);

  auto testSequence = [&](SpillableVectorBasedWindowPartition* partition) {
    // Interleave different operations
    for (int iteration = 0; iteration < 3; ++iteration) {
      // Extract columns - pre-allocate result vectors
      std::vector<VectorPtr> result(3);
      result[0] = BaseVector::create(BIGINT(), 100, pool());
      result[1] = BaseVector::create(DOUBLE(), 100, pool());
      result[2] = BaseVector::create(VARCHAR(), 100, pool());
      partition->extractColumn(0, iteration * 100, 100, 0, result[0]);
      partition->extractColumn(1, iteration * 100, 100, 0, result[1]);
      partition->extractColumn(2, iteration * 100, 100, 0, result[2]);

      auto rowResult = makeRowVector(result);
      verifyExtractedData(rowResult, data, iteration * 100, 100);

      // Extract nulls
      auto nulls = AlignedBuffer::allocate<bool>(100, pool());
      partition->extractNulls(0, iteration * 100, 100, nulls);

      // Compute peer buffers
      auto peerStarts = AlignedBuffer::allocate<vector_size_t>(100, pool());
      auto peerEnds = AlignedBuffer::allocate<vector_size_t>(100, pool());
      auto* rawStarts = peerStarts->asMutable<vector_size_t>();
      auto* rawEnds = peerEnds->asMutable<vector_size_t>();
      partition->computePeerBuffers(
          iteration * 100, (iteration + 1) * 100, 0, 0, rawStarts, rawEnds);
    }
  };

  auto partition = createMemoryModePartition(blocks);
  testSequence(partition.get());

  auto [spilledPartition, tempDir] = createSpilledPartition(blocks);
  testSequence(spilledPartition.get());
}

TEST_F(SpillableVectorBasedWindowPartitionTest, spillDuringOperations) {
  auto data = makeTestData(300);
  auto blocks = createBlocks(data, 50);
  auto tempDir = exec::test::TempDirectoryPath::create();

  // Helper to allocate result vectors
  auto allocateResults = [&](vector_size_t count) {
    std::vector<VectorPtr> result(3);
    result[0] = BaseVector::create(BIGINT(), count, pool());
    result[1] = BaseVector::create(DOUBLE(), count, pool());
    result[2] = BaseVector::create(VARCHAR(), count, pool());
    return result;
  };

  auto partition = createMemoryModePartition(blocks, tempDir->getPath());

  // Do some operations in memory mode
  auto result1 = allocateResults(100);
  partition->extractColumn(0, 0, 100, 0, result1[0]);

  auto peerStarts = AlignedBuffer::allocate<vector_size_t>(100, pool());
  auto peerEnds = AlignedBuffer::allocate<vector_size_t>(100, pool());
  auto* rawStarts = peerStarts->asMutable<vector_size_t>();
  auto* rawEnds = peerEnds->asMutable<vector_size_t>();
  partition->computePeerBuffers(0, 100, 0, 0, rawStarts, rawEnds);

  // Spill
  partition->spill();

  // Continue operations after spill
  auto result2 = allocateResults(100);
  partition->extractColumn(0, 100, 100, 0, result2[0]);
  partition->extractColumn(1, 100, 100, 0, result2[1]);
  partition->extractColumn(2, 100, 100, 0, result2[2]);

  auto rowResult2 = makeRowVector(result2);
  verifyExtractedData(rowResult2, data, 100, 100);

  // Re-access earlier rows
  auto result3 = allocateResults(100);
  partition->extractColumn(0, 0, 100, 0, result3[0]);
  partition->extractColumn(1, 0, 100, 0, result3[1]);
  partition->extractColumn(2, 0, 100, 0, result3[2]);

  auto rowResult3 = makeRowVector(result3);
  verifyExtractedData(rowResult3, data, 0, 100);
}

// =============================================================================
// Edge Case Tests
// =============================================================================

TEST_F(SpillableVectorBasedWindowPartitionTest, singleBlockMultipleExtracts) {
  auto data = makeTestData(50);
  auto blocks = createBlocks(data, 100); // All data in one block

  EXPECT_EQ(blocks.size(), 1);

  // Multiple extractions from the same block
  runExtractTest(
      data,
      100,
      {
          {0, 10},
          {10, 10},
          {20, 10},
          {30, 10},
          {40, 10},
          {0, 50},
      });
}

TEST_F(SpillableVectorBasedWindowPartitionTest, overlappingExtracts) {
  auto data = makeTestData(100);

  // Overlapping extraction ranges
  runExtractTest(
      data,
      25,
      {
          {0, 50},
          {25, 50}, // Overlaps with previous
          {50, 50}, // Overlaps with previous
          {40, 30}, // Contained within previous extractions
      });
}

TEST_F(SpillableVectorBasedWindowPartitionTest, exactRowCountBlocks) {
  // Test where total rows is exact multiple of block size
  constexpr vector_size_t kBlockSize = 25;
  constexpr vector_size_t kNumBlocks = 4;
  auto data = makeTestData(kBlockSize * kNumBlocks);

  auto blocks = createBlocks(data, kBlockSize);
  EXPECT_EQ(blocks.size(), kNumBlocks);

  runExtractTest(
      data,
      kBlockSize,
      {
          {0, kBlockSize * kNumBlocks},
          {kBlockSize - 1, 2}, // Cross boundary
          {kBlockSize * 2 - 1, 2}, // Cross boundary
      });
}

// Tests for streaming extraction optimization

TEST_F(SpillableVectorBasedWindowPartitionTest, streamingExtract) {
  // Test streaming extraction with various scenarios:
  // - Large contiguous ranges (unbounded frame simulation)
  // - Multiple batches with forward/backward seeks
  // - Scattered row access with sorting optimization
  // - Null extraction

  constexpr vector_size_t kTotalRows = 5000;
  constexpr vector_size_t kBlockSize = 100;

  // Test with nulls at every 7th row
  auto data = makeTestDataWithNulls(
      kTotalRows, [](vector_size_t row) { return row % 7 == 0; });
  auto blocks = createBlocks(data, kBlockSize);

  auto tempDir = exec::test::TempDirectoryPath::create();
  auto partition = createMemoryModePartition(blocks, tempDir->getPath());
  partition->spill();

  auto flatExpected = data->childAt(0)->as<FlatVector<int64_t>>();

  // Helper to verify contiguous range extraction
  auto verifyRange = [&](vector_size_t start, vector_size_t count) {
    auto result = BaseVector::create(BIGINT(), count, pool());
    partition->extractColumn(0, start, count, 0, result);
    auto flatResult = result->as<FlatVector<int64_t>>();
    for (vector_size_t i = 0; i < count; ++i) {
      if (flatExpected->isNullAt(start + i)) {
        EXPECT_TRUE(flatResult->isNullAt(i));
      } else {
        EXPECT_EQ(flatResult->valueAt(i), flatExpected->valueAt(start + i));
      }
    }
  };

  // Test 1: Large contiguous range (unbounded frame)
  verifyRange(0, kTotalRows);

  // Test 2: Multiple ranges with forward/backward seeks
  verifyRange(0, 1000);
  verifyRange(2000, 1000); // Forward jump
  verifyRange(500, 500); // Backward seek
  verifyRange(4000, 1000); // Forward again

  // Test 3: Null extraction for entire partition
  auto nulls = AlignedBuffer::allocate<bool>(kTotalRows, pool());
  auto* rawNulls = nulls->asMutable<uint64_t>();
  partition->extractNulls(0, 0, kTotalRows, nulls);
  for (vector_size_t i = 0; i < kTotalRows; ++i) {
    EXPECT_EQ(bits::isBitNull(rawNulls, i), data->childAt(0)->isNullAt(i));
  }

  // Test 4: Scattered rows (including kNullRow and reverse order)
  std::vector<vector_size_t> rowNumbers;
  for (vector_size_t i = 0; i < 100; ++i) {
    if (i % 5 == 0) {
      rowNumbers.push_back(-1); // kNullRow
    } else {
      rowNumbers.push_back((99 - i) * 40); // Reverse order
    }
  }

  auto scatteredResult =
      BaseVector::create(BIGINT(), rowNumbers.size(), pool());
  partition->extractColumn(
      0,
      folly::Range<const vector_size_t*>(rowNumbers.data(), rowNumbers.size()),
      0,
      scatteredResult);

  auto flatScattered = scatteredResult->as<FlatVector<int64_t>>();
  for (size_t i = 0; i < rowNumbers.size(); ++i) {
    if (rowNumbers[i] < 0) {
      EXPECT_TRUE(scatteredResult->isNullAt(i));
    } else if (flatExpected->isNullAt(rowNumbers[i])) {
      EXPECT_TRUE(flatScattered->isNullAt(i));
    } else {
      EXPECT_EQ(
          flatScattered->valueAt(i), flatExpected->valueAt(rowNumbers[i]));
    }
  }
}
