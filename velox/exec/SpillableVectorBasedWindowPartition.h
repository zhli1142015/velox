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
#include "velox/exec/VectorBasedWindowPartition.h"

namespace facebook::velox::exec {

/// A WindowPartition that uses Vector-based storage and supports spilling.
///
/// This class operates in two modes:
/// - Memory mode: Uses VectorBasedWindowPartition for in-memory storage.
///   Created via the memory-mode constructor. Can transition to spill mode
///   by calling spill().
/// - Spill mode: Reads rows from spill files with on-demand caching using
///   RowBlock cache instead of RowContainer. Created via the spill-mode
///   constructor, or after spill() is called.
///
/// Key differences from SpillableWindowPartition:
/// - Uses RowBlock cache instead of RowContainer for spill cache
/// - Uses NoRowContainerSpiller instead of SortOutputSpiller
/// - Stores RowVectorPtr directly instead of char* rows
class SpillableVectorBasedWindowPartition : public WindowPartition {
 public:
  /// Constructs a SpillableVectorBasedWindowPartition in memory mode.
  /// The partition is created as complete (not partial).
  ///
  /// @param blocks The RowBlocks containing partition data.
  /// @param inputMapping Column index mapping from Window output to input.
  /// @param sortKeyInfo ORDER BY columns and their sort orders.
  /// @param inputType Schema used for spiller creation.
  /// @param spillConfig Configuration for spilling.
  /// @param spillStats Shared statistics for spill operations.
  /// @param pool Memory pool for allocations.
  SpillableVectorBasedWindowPartition(
      const std::vector<RowBlock>& blocks,
      const std::vector<column_index_t>& inputMapping,
      const std::vector<std::pair<column_index_t, core::SortOrder>>&
          sortKeyInfo,
      const RowTypePtr& inputType,
      const common::SpillConfig* spillConfig,
      folly::Synchronized<common::SpillStats>* spillStats,
      memory::MemoryPool* pool);

  /// Constructs a SpillableVectorBasedWindowPartition in spill mode.
  ///
  /// @param spillFiles List of spill files containing pre-sorted data.
  /// @param totalRows Total number of rows in this partition.
  /// @param inputMapping Column index mapping from Window output to input.
  /// @param sortKeyInfo ORDER BY columns and their sort orders.
  /// @param inputType Schema of the input data.
  /// @param readBufferSize Buffer size for reading spill files.
  /// @param maxSpillCacheBytes Maximum memory (bytes) for caching.
  /// @param maxSpillCacheRows Maximum number of rows to cache.
  /// @param pool Memory pool for allocations.
  /// @param spillStats Shared statistics for spill operations.
  SpillableVectorBasedWindowPartition(
      SpillFiles spillFiles,
      vector_size_t totalRows,
      const std::vector<column_index_t>& inputMapping,
      const std::vector<std::pair<column_index_t, core::SortOrder>>&
          sortKeyInfo,
      const RowTypePtr& inputType,
      uint64_t readBufferSize,
      uint64_t maxSpillCacheBytes,
      uint64_t maxSpillCacheRows,
      memory::MemoryPool* pool,
      folly::Synchronized<common::SpillStats>* spillStats);

  ~SpillableVectorBasedWindowPartition() override = default;

  /// Transitions this partition from memory mode to spill mode.
  void spill();

  /// Returns true if this partition is in spill mode.
  bool isSpilled() const {
    return spilled_;
  }

  /// Returns the spill files for this partition.
  const SpillFiles& spillFiles() const {
    return spillFiles_;
  }

  /// Returns the total number of rows in this partition.
  vector_size_t totalRowCount() const {
    return totalRows_;
  }

  vector_size_t numRows() const override;

  vector_size_t numRowsForProcessing(
      vector_size_t partitionOffset) const override;

  void extractColumn(
      int32_t columnIndex,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      vector_size_t resultOffset,
      const VectorPtr& result) const override;

  void extractColumn(
      int32_t columnIndex,
      folly::Range<const vector_size_t*> rowNumbers,
      vector_size_t resultOffset,
      const VectorPtr& result) const override;

  void extractNulls(
      int32_t columnIndex,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      const BufferPtr& nullsBuffer) const override;

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

  /// Adds rows in partial mode (delegates to inMemoryPartition_).
  void addRows(const RowBlock& block);

  /// Not supported for VectorBased partition.
  void addRows(const std::vector<char*>& rows) override {
    VELOX_FAIL(
        "SpillableVectorBasedWindowPartition does not support char* rows");
  }

  /// Removes processed rows from the front of the partition.
  void removeProcessedRows(vector_size_t numRows) override;

 private:
  /// Writes all in-memory blocks to spill files.
  void spillRowsToFiles();

  /// Transitions to spill mode after spilling.
  void transitionToSpillMode();

  /// Creates FileSpillMergeStream objects for reading spill files.
  void createSpillStreams(size_t startFileIndex = 0) const;

  /// Clears the cache and releases memory.
  void clearCache() const;

  /// Seeks to the specified target row for backward access.
  void seekToRow(vector_size_t targetRow) const;

  /// Skips rows in the current file until reaching targetRow.
  void skipRowsInFile(size_t startFileIndex, vector_size_t targetRow) const;

  /// Finds the file containing the target row.
  std::pair<size_t, vector_size_t> findFileForRow(
      vector_size_t targetRow) const;

  /// Ensures rows in range [startRow, startRow + numRows) are in cache.
  void ensureRowsCached(vector_size_t startRow, vector_size_t numRows) const;

  /// Loads rows from spill files into cache.
  void loadRowsIntoCache(
      vector_size_t targetEndRow,
      vector_size_t minRequiredRow) const;

  /// Tries to evict old rows to control cache size.
  void tryEvictOldRows(vector_size_t minRequiredRow, vector_size_t rowsToLoad)
      const;

  /// Extracts column data from cache.
  void extractFromCache(
      int32_t columnIndex,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      vector_size_t resultOffset,
      const VectorPtr& result) const;

  /// Streaming extraction: loads and extracts in batches without requiring
  /// all rows to be cached first. More efficient for large ranges like
  /// unbounded frames.
  void streamingExtractColumn(
      int32_t columnIndex,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      vector_size_t resultOffset,
      const VectorPtr& result) const;

  /// Streaming null extraction: loads and extracts nulls in batches.
  void streamingExtractNulls(
      int32_t columnIndex,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      const BufferPtr& nullsBuffer) const;

  /// Finds the end of the peer group starting at startRow.
  vector_size_t findPeerGroupEnd(vector_size_t startRow) const;

  /// Checks if two cached rows are peers.
  bool arePeersCached(vector_size_t lhsRow, vector_size_t rhsRow) const;

  /// Finds a block in cache containing the given global row.
  std::pair<size_t, vector_size_t> findBlockInCache(
      vector_size_t globalRow) const;

  /// Estimates the memory usage of the cache.
  uint64_t estimateCacheBytes() const;

  /// Deep copies a RowBlock to a standalone RowVector.
  RowVectorPtr copyBlockToRowVector(const RowBlock& block) const;

  // Memory mode: delegate partition for in-memory operations.
  std::unique_ptr<VectorBasedWindowPartition> inMemoryPartition_;

  // Original blocks saved for spilling (memory mode only).
  std::vector<RowBlock> originalBlocks_;

  // Spill mode: cached blocks read from spill files.
  mutable std::vector<RowBlock> cache_;
  mutable vector_size_t cacheStartRow_{0};
  mutable vector_size_t cacheEndRow_{0};
  mutable vector_size_t cacheRowCount_{0};

  // Spill state.
  bool spilled_{false};
  SpillFiles spillFiles_;
  vector_size_t totalRows_{0};

  // Spill file reading state.
  mutable std::vector<std::unique_ptr<SpillMergeStream>> spillStreams_;
  mutable size_t currentFileIndex_{0};
  mutable std::vector<std::pair<vector_size_t, vector_size_t>> fileRowRanges_;

  // Configuration and resources.
  RowTypePtr inputType_;
  const common::SpillConfig* spillConfig_{nullptr};
  uint64_t readBufferSize_{0};
  uint64_t maxSpillCacheBytes_{1ULL << 30}; // 1GB default
  uint64_t maxSpillCacheRows_{100000};
  memory::MemoryPool* pool_{nullptr};
  folly::Synchronized<common::SpillStats>* spillStats_{nullptr};

  // Column mappings for spill mode operations.
  //
  // Data format context:
  // - Original input: User's column order (e.g., {d, p, s})
  // - Reordered input: Window operator's order (e.g., {p, s, d})
  //   (partition keys first, then sort keys, then the rest)
  //
  // Storage format:
  // - Memory mode (VectorBasedWindowPartition): stores ORIGINAL input vectors
  // - Spill mode (spill files / cache_): stores REORDERED vectors
  //
  // reorderedToOriginal_[reorderedPos] = originalColumnIndex
  // Used in copyBlockToRowVector() to create reordered vectors from original.
  std::vector<column_index_t> reorderedToOriginal_;

  // originalToReordered_[originalCol] = reorderedPosition
  // Used in extractFromCache/extractNulls to map API's original column index
  // to the reordered position in cache.
  std::vector<column_index_t> originalToReordered_;

  std::vector<std::pair<column_index_t, core::SortOrder>> sortKeyInfo_;
};

} // namespace facebook::velox::exec
