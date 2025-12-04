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

#include "velox/exec/Spill.h"
#include "velox/exec/SpillFile.h"
#include "velox/exec/Spiller.h"
#include "velox/exec/WindowPartition.h"

namespace facebook::velox::exec {

/// A WindowPartition that supports spilling to disk for huge partitions.
///
/// This class operates in two modes:
/// - Memory mode: Wraps an internal WindowPartition and delegates all
///   operations. Created via the memory-mode constructor. Can transition
///   to spill mode by calling spill().
/// - Spill mode: Reads rows from spill files with on-demand caching.
///   Created via the spill-mode constructor, or after spill() is called.
///
/// Key design decisions:
/// - Uses a dedicated spillRowContainer_ for spill cache, isolated from the
///   shared data_ which may be cleared by external code.
/// - Saves spillFiles_ to support resetSpillStream() for backward access
///   when window functions need earlier rows (e.g., first_value).
class SpillableWindowPartition : public WindowPartition {
 public:
  /// Constructs a SpillableWindowPartition in memory mode.
  ///
  /// Creates an internal WindowPartition to which all operations are
  /// delegated. Can transition to spill mode by calling spill().
  ///
  /// @param data The RowContainer containing row data (owned by WindowBuild).
  /// @param rows Row pointers for this partition.
  /// @param inputMapping Column index mapping from Window input to
  ///        RowContainer columns.
  /// @param sortKeyInfo ORDER BY columns and their sort orders.
  /// @param inputType Schema used for spiller creation.
  /// @param spillConfig Configuration for spilling.
  /// @param spillStats Shared statistics for spill operations.
  SpillableWindowPartition(
      RowContainer* data,
      folly::Range<char**> rows,
      const std::vector<column_index_t>& inputMapping,
      const std::vector<std::pair<column_index_t, core::SortOrder>>&
          sortKeyInfo,
      const RowTypePtr& inputType,
      const common::SpillConfig* spillConfig,
      folly::Synchronized<common::SpillStats>* spillStats);

  /// Constructs a SpillableWindowPartition in spill mode.
  ///
  /// Reads rows from spill files with on-demand caching. Creates a dedicated
  /// spillRowContainer_ for cache isolation from the shared data_.
  ///
  /// @param data The RowContainer used for type info and memory pool. Note
  ///        that this is NOT used for data storage in spill mode.
  /// @param spillFiles List of spill files containing pre-sorted data.
  /// @param totalRows Total number of rows in this partition.
  /// @param inputMapping Column index mapping from Window input to
  ///        RowContainer columns.
  /// @param sortKeyInfo ORDER BY columns and their sort orders.
  /// @param readBufferSize Buffer size for reading spill files.
  /// @param maxSpillCacheBytes Maximum memory (bytes) for caching spilled data.
  /// @param maxSpillCacheRows Maximum number of rows to cache.
  /// @param spillStats Shared statistics for spill operations.
  SpillableWindowPartition(
      RowContainer* data,
      SpillFiles spillFiles,
      vector_size_t totalRows,
      const std::vector<column_index_t>& inputMapping,
      const std::vector<std::pair<column_index_t, core::SortOrder>>&
          sortKeyInfo,
      uint64_t readBufferSize,
      uint64_t maxSpillCacheBytes,
      uint64_t maxSpillCacheRows,
      folly::Synchronized<common::SpillStats>* spillStats);

  ~SpillableWindowPartition() override = default;

  /// Transitions this partition from memory mode to spill mode.
  ///
  /// Writes all rows to spill files, creates a dedicated spillRowContainer_
  /// for caching, and destroys inMemoryPartition_. This method is a no-op
  /// if the partition is already in spill mode.
  void spill();

  /// Returns true if this partition is in spill mode (reading from disk).
  bool isSpilled() const {
    return inMemoryPartition_ == nullptr;
  }

  /// Returns the spill files for this partition. Only valid when isSpilled()
  /// returns true.
  const SpillFiles& spillFiles() const {
    return spillFiles_;
  }

  /// Returns the total number of rows in this partition (valid in spill mode).
  vector_size_t totalRowCount() const {
    return totalRows_;
  }

  vector_size_t numRows() const override;

  vector_size_t numRowsForProcessing(
      vector_size_t partitionOffset) const override;

  void extractColumn(
      int32_t columnIndex,
      folly::Range<const vector_size_t*> rowNumbers,
      vector_size_t resultOffset,
      const VectorPtr& result) const override;

  void extractColumn(
      int32_t columnIndex,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      vector_size_t resultOffset,
      const VectorPtr& result) const override;

  void extractNulls(
      int32_t columnIndex,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      const BufferPtr& nullsBuffer) const override;

  std::optional<std::pair<vector_size_t, vector_size_t>> extractNulls(
      column_index_t col,
      const SelectivityVector& validRows,
      const BufferPtr& frameStarts,
      const BufferPtr& frameEnds,
      BufferPtr* nulls) const override;

  std::pair<vector_size_t, vector_size_t> computePeerBuffers(
      vector_size_t start,
      vector_size_t end,
      vector_size_t prevPeerStart,
      vector_size_t prevPeerEnd,
      vector_size_t* rawPeerStarts,
      vector_size_t* rawPeerEnds) override;

  void computeKRangeFrameBounds(
      bool isStartBound,
      bool isPreceding,
      column_index_t frameColumn,
      vector_size_t startRow,
      vector_size_t numRows,
      const vector_size_t* rawPeerStarts,
      vector_size_t* rawFrameBounds,
      SelectivityVector& validFrames) const override;

 private:
  // Ensures rows in range [startRow, endRow) are loaded into the cache.
  // Seeks to the correct file if backward access is needed. Only used in
  // spill mode.
  void ensureRowsLoaded(vector_size_t startRow, vector_size_t endRow) const;

  // Reads rows from spill files until cacheEndRow_ >= targetEndRow.
  // Updates fileRowRanges_ when crossing file boundaries.
  // minRequiredRow indicates the minimum row that must not be evicted.
  void loadRowsFromSpill(
      vector_size_t targetEndRow,
      vector_size_t minRequiredRow) const;

  // Writes all in-memory rows to spill files.
  void spillRowsToFiles();

  // Transitions to spill mode by creating spillRowContainer_ and streams.
  void transitionToSpillMode();

  // Initializes spill state: creates spillRowContainer_ and spill streams.
  void initializeSpillState();

  // Extracts a column for scattered (non-contiguous) row numbers.
  void extractColumnScattered(
      int32_t columnIndex,
      folly::Range<const vector_size_t*> rowNumbers,
      vector_size_t resultOffset,
      const VectorPtr& result) const;

  // Extracts a column from the cache for a contiguous range.
  void extractColumnFromCache(
      int32_t columnIndex,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      vector_size_t resultOffset,
      const VectorPtr& result) const;

  // Seeks to the specified target row for backward access. Uses binary search
  // on fileRowRanges_ to find the correct file, skipping unnecessary files.
  void seekToRow(vector_size_t targetRow) const;

  // Creates FileSpillMergeStream objects for all spill files starting from
  // the specified file index.
  void createSpillStreams(size_t startFileIndex = 0) const;

  // Clears the cached rows and releases memory.
  void clearCache() const;

  // Tries to evict old rows if memory usage exceeds kMaxSpillCacheBytes
  // or if loading rowsToLoad new rows would exceed kMaxSpillCacheRows.
  // Only evicts rows before minRequiredRow, which are no longer needed.
  // This is called before loading new rows to proactively control memory.
  void tryEvictOldRows(vector_size_t minRequiredRow, vector_size_t rowsToLoad)
      const;

  // Finds the file containing the target row using fileRowRanges_.
  // Returns {fileIndex, fileStartRow}.
  std::pair<size_t, vector_size_t> findFileForRow(
      vector_size_t targetRow) const;

  // Resets file row ranges for files starting from startFileIndex.
  void resetFileRowRanges(size_t startFileIndex, vector_size_t fileStartRow)
      const;

  // Skips rows in the current file until reaching targetRow.
  void skipRowsInFile(size_t startFileIndex, vector_size_t targetRow) const;

  // Finds the end of the peer group starting at startRow.
  vector_size_t findPeerGroupEnd(vector_size_t startRow) const;

  // Computes a conservative k-range frame bound for a single row.
  vector_size_t computeKRangeFrameBound(
      bool isStartBound,
      bool isPreceding,
      vector_size_t currentRow,
      vector_size_t peerStart) const;

  // Computes the min/max row range from frame bounds.
  std::pair<vector_size_t, vector_size_t> computeRowRange(
      const SelectivityVector& validRows,
      const BufferPtr& frameStarts,
      const BufferPtr& frameEnds) const;

  // Returns {true, startRow} if rowNumbers form a contiguous sequence,
  // or {false, 0} otherwise.
  static std::pair<bool, vector_size_t> checkContiguity(
      folly::Range<const vector_size_t*> rowNumbers);

  // Checks if row numbers form a contiguous sequence.
  static bool isContiguousRange(folly::Range<const vector_size_t*> rowNumbers);

  // Compares sort keys between two rows to determine if they are peers.
  // Uses spillRowContainer_ in spill mode, data_ in memory mode.
  bool arePeerRows(const char* lhs, const char* rhs) const;

  // Returns the cached row pointer for the given partition row index.
  // The row must be within the cached range [cacheStartRow_, cacheEndRow_).
  char* getRowPtr(vector_size_t partitionRow) const {
    VELOX_DCHECK_GE(partitionRow, cacheStartRow_);
    VELOX_DCHECK_LT(partitionRow, cacheEndRow_);
    return cachedRows_[partitionRow - cacheStartRow_];
  }

  // The delegate partition for memory mode operations. When non-null, this
  // partition is in memory mode; when null, it is in spill mode.
  std::unique_ptr<WindowPartition> inMemoryPartition_;

  // Row pointers for the spill() transition. Needed because the base class
  // WindowPartition::partition_ is protected.
  folly::Range<char**> inMemoryRows_;

  // Schema type used for creating the spiller during memory-to-spill
  // transition.
  RowTypePtr inputType_;

  // Configuration for spill operations.
  const common::SpillConfig* spillConfig_{nullptr};

  // Shared statistics for spill operations across partitions.
  folly::Synchronized<common::SpillStats>* spillStats_{nullptr};

  // Row pointers for cached rows from spill files. cachedRows_[i] corresponds
  // to partition row (cacheStartRow_ + i). Points to rows stored in
  // spillRowContainer_, NOT in data_.
  mutable std::vector<char*> cachedRows_;

  // Index of the first row currently in the cache.
  mutable vector_size_t cacheStartRow_{0};

  // Index one past the last row currently in the cache.
  mutable vector_size_t cacheEndRow_{0};

  // Streams for reading spill files. Each stream corresponds to one file.
  // Using separate streams allows efficient seeking by skipping files.
  mutable std::vector<std::unique_ptr<SpillMergeStream>> spillStreams_;

  // Current file index being read. Points to the file in spillStreams_
  // that is currently being consumed.
  mutable size_t currentFileIndex_{0};

  // Row ranges for each spill file. fileRowRanges_[i] = {startRow, endRow}
  // where startRow is the first row in file i, and endRow is one past the
  // last row. This is populated during loadRowsFromSpill() and used by
  // seekToRow() to efficiently locate the file containing a target row.
  mutable std::vector<std::pair<vector_size_t, vector_size_t>> fileRowRanges_;

  // Dedicated RowContainer for cached rows, isolated from the shared data_
  // which may be cleared by external code.
  mutable std::unique_ptr<RowContainer> spillRowContainer_;

  // Total number of rows in this partition.
  vector_size_t totalRows_{0};

  // Hint for cache capacity (not strictly enforced).
  vector_size_t cacheCapacity_{0};

  // Spill files for this partition, saved for resetSpillStream().
  SpillFiles spillFiles_;

  // Buffer size for reading spill files.
  uint64_t readBufferSize_{0};

  // Maximum memory (bytes) for caching spilled window data.
  uint64_t maxSpillCacheBytes_{1ULL << 30}; // 1GB default

  // Maximum number of rows to cache for spilled window data.
  uint64_t maxSpillCacheRows_{100000};
};

} // namespace facebook::velox::exec
