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
#pragma once

#include "velox/exec/WindowPartition.h"

namespace facebook::velox::exec {

/// Represents a contiguous range of rows within an input batch.
struct RowBlock {
  RowVectorPtr input; // Input vector (shared_ptr, automatic memory management)
  vector_size_t startRow; // Start row (inclusive)
  vector_size_t endRow; // End row (exclusive)

  vector_size_t size() const {
    return endRow - startRow;
  }
};

/// Represents a reference to a single row (used for cross-batch comparison).
struct RowReference {
  RowVectorPtr input;
  vector_size_t row;

  bool isValid() const {
    return input != nullptr;
  }
};

/// VectorBasedWindowPartition inherits WindowPartition and uses direct Vector
/// storage instead of RowContainer. This eliminates the overhead of copying
/// data into RowContainer format.
class VectorBasedWindowPartition : public WindowPartition {
 public:
  /// Constructor for partial mode - rows will be added via addRows().
  VectorBasedWindowPartition(
      const std::vector<column_index_t>& inputMapping,
      const std::vector<std::pair<column_index_t, core::SortOrder>>&
          sortKeyInfo,
      bool partial);

  /// Constructor for complete mode - blocks are provided at construction.
  /// This constructor does not require addRows() and sets complete_=true.
  VectorBasedWindowPartition(
      const std::vector<RowBlock>& blocks,
      const std::vector<column_index_t>& inputMapping,
      const std::vector<std::pair<column_index_t, core::SortOrder>>&
          sortKeyInfo);

  vector_size_t numRows() const override {
    return totalRows_;
  }

  /// Returns the number of rows remaining for processing.
  vector_size_t numRowsForProcessing(
      vector_size_t partitionOffset) const override {
    // For VectorBasedWindowPartition, we always return totalRows_ since
    // removeProcessedRows already adjusts the row count.
    return totalRows_;
  }

  /// Extracts column data for a contiguous range of rows.
  void extractColumn(
      int32_t columnIndex,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      vector_size_t resultOffset,
      const VectorPtr& result) const override;

  /// Extracts column data for specific row numbers.
  void extractColumn(
      int32_t columnIndex,
      folly::Range<const vector_size_t*> rowNumbers,
      vector_size_t resultOffset,
      const VectorPtr& result) const override;

  /// Extracts null positions for a range of rows.
  void extractNulls(
      int32_t columnIndex,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      const BufferPtr& nullsBuffer) const override;

  /// Computes peer group start and end positions for a range of rows.
  /// Note: rawPeerEnds stores inclusive end indices (peerEnd - 1).
  std::pair<vector_size_t, vector_size_t> computePeerBuffers(
      vector_size_t start,
      vector_size_t end,
      vector_size_t prevPeerStart,
      vector_size_t prevPeerEnd,
      vector_size_t* rawPeerStarts,
      vector_size_t* rawPeerEnds) override;

  /// Computes k-range frame bounds.
  void computeKRangeFrameBounds(
      bool isStartBound,
      bool isPreceding,
      column_index_t frameColumn,
      vector_size_t startRow,
      vector_size_t numRows,
      const vector_size_t* rawPeerStarts,
      vector_size_t* rawFrameBounds,
      SelectivityVector& validFrames) const override;

  /// Adds new rows in partial mode (VectorBasedWindowPartition specific).
  void addRows(const RowBlock& block);

  /// Override base class addRows - not supported for
  /// VectorBasedWindowPartition.
  void addRows(const std::vector<char*>& rows) override {
    VELOX_FAIL("VectorBasedWindowPartition does not support char* rows");
  }

  /// Removes processed rows from the front of the partition.
  void removeProcessedRows(vector_size_t numRows) override;

  /// Checks if two rows are peers (have the same sort key values).
  bool arePeers(vector_size_t lhsRow, vector_size_t rhsRow) const;

 private:
  /// O(log n) lookup to find the block containing a global row index.
  /// Returns {blockIndex, localRowWithinBlock}.
  std::pair<size_t, vector_size_t> findBlock(vector_size_t globalRow) const;

  /// Rebuilds prefix sums after rows are removed.
  void rebuildPrefixSums();

  /// Searches for a frame value using binary search.
  vector_size_t searchFrameValue(
      bool firstMatch,
      vector_size_t start,
      vector_size_t end,
      vector_size_t currentRow,
      column_index_t orderByColumn,
      const VectorPtr& frameValue,
      vector_size_t frameValueIndex,
      core::SortOrder sortOrder) const;

  /// Linear search for exact frame boundary after binary search.
  vector_size_t linearSearchFrameValue(
      bool firstMatch,
      vector_size_t start,
      vector_size_t end,
      column_index_t orderByColumn,
      const VectorPtr& frameValue,
      vector_size_t frameValueIndex,
      const CompareFlags& flags) const;

  std::vector<RowBlock> blocks_;
  std::vector<vector_size_t> blockPrefixSums_; // For O(log n) row lookup
  vector_size_t totalRows_ = 0;
  // startRow_ tracks the number of rows that have been removed.
  // partitionOffset from Window is the absolute offset within the partition.
  // To access current blocks_, we use (partitionOffset - startRow_).
  vector_size_t startRow_ = 0;
};

} // namespace facebook::velox::exec
