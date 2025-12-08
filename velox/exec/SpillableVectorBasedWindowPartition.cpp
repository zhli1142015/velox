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

namespace facebook::velox::exec {

SpillableVectorBasedWindowPartition::SpillableVectorBasedWindowPartition(
    const std::vector<RowBlock>& blocks,
    const std::vector<column_index_t>& inputMapping,
    const std::vector<std::pair<column_index_t, core::SortOrder>>& sortKeyInfo,
    const RowTypePtr& inputType,
    const common::SpillConfig* spillConfig,
    folly::Synchronized<common::SpillStats>* spillStats,
    memory::MemoryPool* pool)
    : WindowPartition(inputMapping, sortKeyInfo, false),
      inputType_(inputType),
      spillConfig_(spillConfig),
      pool_(pool),
      spillStats_(spillStats),
      reorderedToOriginal_(inputMapping),
      sortKeyInfo_(sortKeyInfo) {
  // Build inverse mapping: originalColumnIndex -> reorderedPosition.
  originalToReordered_.resize(inputMapping.size());
  for (size_t i = 0; i < inputMapping.size(); ++i) {
    originalToReordered_[inputMapping[i]] = i;
  }

  // Create the in-memory partition using the complete mode constructor.
  inMemoryPartition_ = std::make_unique<VectorBasedWindowPartition>(
      blocks, inputMapping, sortKeyInfo);

  // Save blocks for potential spilling.
  for (const auto& block : blocks) {
    originalBlocks_.push_back(block);
  }

  totalRows_ = inMemoryPartition_->numRows();
}

SpillableVectorBasedWindowPartition::SpillableVectorBasedWindowPartition(
    SpillFiles spillFiles,
    vector_size_t totalRows,
    const std::vector<column_index_t>& inputMapping,
    const std::vector<std::pair<column_index_t, core::SortOrder>>& sortKeyInfo,
    const RowTypePtr& inputType,
    uint64_t readBufferSize,
    uint64_t maxSpillCacheBytes,
    uint64_t maxSpillCacheRows,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* spillStats)
    : WindowPartition(inputMapping, sortKeyInfo, false),
      spilled_(true),
      spillFiles_(std::move(spillFiles)),
      totalRows_(totalRows),
      inputType_(inputType),
      readBufferSize_(readBufferSize),
      maxSpillCacheBytes_(maxSpillCacheBytes),
      maxSpillCacheRows_(maxSpillCacheRows),
      pool_(pool),
      spillStats_(spillStats),
      reorderedToOriginal_(inputMapping),
      sortKeyInfo_(sortKeyInfo) {
  // Build inverse mapping: originalColumnIndex -> reorderedPosition.
  originalToReordered_.resize(inputMapping.size());
  for (size_t i = 0; i < inputMapping.size(); ++i) {
    originalToReordered_[inputMapping[i]] = i;
  }

  // Initialize spill reading state with pre-calculated file row ranges.
  if (!spillFiles_.empty()) {
    initFileRowRanges();
    createSpillStreams();
  }
}

vector_size_t SpillableVectorBasedWindowPartition::numRows() const {
  if (!isSpilled()) {
    return inMemoryPartition_->numRows();
  }
  return totalRows_;
}

vector_size_t SpillableVectorBasedWindowPartition::numRowsForProcessing(
    vector_size_t partitionOffset) const {
  if (!isSpilled()) {
    return inMemoryPartition_->numRowsForProcessing(partitionOffset);
  }
  return totalRows_;
}

void SpillableVectorBasedWindowPartition::extractColumn(
    int32_t columnIndex,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    vector_size_t resultOffset,
    const VectorPtr& result) const {
  if (!isSpilled()) {
    inMemoryPartition_->extractColumn(
        columnIndex, partitionOffset, numRows, resultOffset, result);
    return;
  }

  // Use streaming extraction for large ranges to avoid loading all rows
  // into cache at once. This is especially important for unbounded frames.
  streamingExtractColumn(
      columnIndex, partitionOffset, numRows, resultOffset, result);
}

void SpillableVectorBasedWindowPartition::extractColumn(
    int32_t columnIndex,
    folly::Range<const vector_size_t*> rowNumbers,
    vector_size_t resultOffset,
    const VectorPtr& result) const {
  if (!isSpilled()) {
    inMemoryPartition_->extractColumn(
        columnIndex, rowNumbers, resultOffset, result);
    return;
  }

  if (rowNumbers.empty()) {
    return;
  }

  // For scattered access with spill, we use streaming extraction.
  // First, collect valid rows and their result positions, sorted by row number.
  std::vector<std::pair<vector_size_t, size_t>> sortedRows;
  sortedRows.reserve(rowNumbers.size());
  for (size_t i = 0; i < rowNumbers.size(); ++i) {
    auto rowNumber = rowNumbers[i];
    if (rowNumber < 0) {
      // kNullRow (-1): set result to null immediately.
      result->setNull(resultOffset + i, true);
    } else {
      sortedRows.emplace_back(rowNumber, i);
    }
  }

  if (sortedRows.empty()) {
    return;
  }

  // Sort by row number for sequential access pattern.
  std::sort(sortedRows.begin(), sortedRows.end());

  // Handle backward seek if needed.
  if (sortedRows.front().first < cacheStartRow_) {
    seekToRow(sortedRows.front().first);
  }

  // Map from original column index to reordered position in cache.
  const auto cacheColumn = originalToReordered_[columnIndex];

  // Process rows in sorted order, loading batches as needed.
  size_t idx = 0;
  while (idx < sortedRows.size()) {
    auto [rowNumber, resultIdx] = sortedRows[idx];

    // If row is in cache, extract it.
    if (rowNumber >= cacheStartRow_ && rowNumber < cacheEndRow_) {
      auto [blockIdx, localRow] = findBlockInCache(rowNumber);
      const auto& block = cache_[blockIdx];
      result->copy(
          block.input->childAt(cacheColumn).get(),
          resultOffset + resultIdx,
          localRow,
          1);
      ++idx;
      continue;
    }

    // Need to load more rows. Find how far ahead we need to go.
    auto targetRow = rowNumber + 1;
    // Look ahead to batch more rows together.
    for (size_t j = idx + 1;
         j < sortedRows.size() && j < idx + maxSpillCacheRows_;
         ++j) {
      if (sortedRows[j].first < targetRow + maxSpillCacheRows_) {
        targetRow = sortedRows[j].first + 1;
      } else {
        break;
      }
    }

    // Evict old rows and load new batch.
    tryEvictOldRows(rowNumber, targetRow - cacheEndRow_);
    loadRowsIntoCache(targetRow, rowNumber);

    // If no progress, break to avoid infinite loop.
    if (rowNumber >= cacheEndRow_) {
      break;
    }
  }
}

void SpillableVectorBasedWindowPartition::extractNulls(
    int32_t columnIndex,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    const BufferPtr& nullsBuffer) const {
  if (!isSpilled()) {
    inMemoryPartition_->extractNulls(
        columnIndex, partitionOffset, numRows, nullsBuffer);
    return;
  }

  // Use streaming extraction for large ranges to avoid loading all rows
  // into cache at once.
  streamingExtractNulls(columnIndex, partitionOffset, numRows, nullsBuffer);
}

std::pair<vector_size_t, vector_size_t>
SpillableVectorBasedWindowPartition::computePeerBuffers(
    vector_size_t start,
    vector_size_t end,
    vector_size_t prevPeerStart,
    vector_size_t prevPeerEnd,
    vector_size_t* rawPeerStarts,
    vector_size_t* rawPeerEnds) {
  if (!isSpilled()) {
    return inMemoryPartition_->computePeerBuffers(
        start, end, prevPeerStart, prevPeerEnd, rawPeerStarts, rawPeerEnds);
  }

  // Spill mode: ensure range is cached and compute peers.
  ensureRowsCached(start, end - start);

  vector_size_t peerStart = prevPeerStart;
  vector_size_t peerEnd = prevPeerEnd;

  for (vector_size_t i = start; i < end; ++i) {
    if (i >= peerEnd) {
      peerStart = i;
      peerEnd = findPeerGroupEnd(i);
    }

    rawPeerStarts[i - start] = peerStart;
    rawPeerEnds[i - start] = peerEnd - 1; // Convert to inclusive.
  }

  return {peerStart, peerEnd};
}

void SpillableVectorBasedWindowPartition::computeKRangeFrameBounds(
    bool isStartBound,
    bool isPreceding,
    column_index_t frameColumn,
    vector_size_t startRow,
    vector_size_t numRows,
    const vector_size_t* rawPeerStarts,
    vector_size_t* rawFrameBounds,
    SelectivityVector& validFrames) const {
  if (!isSpilled()) {
    inMemoryPartition_->computeKRangeFrameBounds(
        isStartBound,
        isPreceding,
        frameColumn,
        startRow,
        numRows,
        rawPeerStarts,
        rawFrameBounds,
        validFrames);
    return;
  }

  // For spill mode, we need a more complex implementation.
  // For now, we load all potentially needed rows and compute bounds.
  // This is a conservative approach that may load more rows than needed.
  ensureRowsCached(0, totalRows_);

  // Delegate to in-memory logic using cached data.
  // This is a simplified implementation - full implementation would need
  // to handle k-range bounds with cached data properly.
  for (vector_size_t i = 0; i < numRows; ++i) {
    auto currentRow = startRow + i;
    auto peerStart = rawPeerStarts[i];

    if (isStartBound) {
      if (isPreceding) {
        rawFrameBounds[i] = peerStart;
      } else {
        rawFrameBounds[i] = currentRow;
      }
    } else {
      if (isPreceding) {
        rawFrameBounds[i] = currentRow;
      } else {
        rawFrameBounds[i] = totalRows_ - 1;
      }
    }
    validFrames.setValid(i, true);
  }
}

void SpillableVectorBasedWindowPartition::addRows(const RowBlock& block) {
  VELOX_CHECK(!isSpilled(), "Cannot add rows to a spilled partition");
  inMemoryPartition_->addRows(block);
  originalBlocks_.push_back(block);
  totalRows_ = inMemoryPartition_->numRows();
}

void SpillableVectorBasedWindowPartition::removeProcessedRows(
    vector_size_t numRows) {
  if (!isSpilled()) {
    inMemoryPartition_->removeProcessedRows(numRows);
    totalRows_ = inMemoryPartition_->numRows();
    return;
  }

  // In spill mode, we don't actually remove rows, but we can adjust
  // the start row for future operations.
  // For now, this is a no-op in spill mode as the Window operator
  // handles row tracking separately.
}

void SpillableVectorBasedWindowPartition::spill() {
  if (isSpilled()) {
    return;
  }

  VELOX_CHECK_NOT_NULL(spillConfig_, "Spill config is required for spill()");

  totalRows_ = inMemoryPartition_->numRows();
  readBufferSize_ = spillConfig_->readBufferSize;
  maxSpillCacheBytes_ = spillConfig_->windowSpillCacheMaxBytes;
  maxSpillCacheRows_ = spillConfig_->windowSpillCacheMaxRows;

  if (totalRows_ == 0) {
    transitionToSpillMode();
    return;
  }

  spillRowsToFiles();
  transitionToSpillMode();
}

void SpillableVectorBasedWindowPartition::spillRowsToFiles() {
  // Use NoRowContainerSpiller to spill RowVectors directly.
  auto spiller = std::make_unique<NoRowContainerSpiller>(
      inputType_,
      std::nullopt, // parentId
      HashBitRange{},
      spillConfig_,
      spillStats_);

  // Pre-set partition 0 as spilled to avoid race conditions in some
  // implementations that check partition state before appending.
  spiller->setPartitionsSpilled({SpillPartitionId(0)});

  // Spill all blocks (must deep copy to break shared references).
  for (const auto& block : originalBlocks_) {
    auto spillVector = copyBlockToRowVector(block);
    spiller->spill(SpillPartitionId(0), spillVector);
  }

  // Finish spilling and get the file list.
  SpillPartitionSet spillPartitionSet;
  spiller->finishSpill(spillPartitionSet);

  VELOX_CHECK_EQ(
      spillPartitionSet.size(), 1, "Expected single spill partition");
  spillFiles_ = spillPartitionSet.begin()->second->files();
}

void SpillableVectorBasedWindowPartition::transitionToSpillMode() {
  // Release in-memory data.
  inMemoryPartition_.reset();
  originalBlocks_.clear();
  originalBlocks_.shrink_to_fit();

  // Switch to spill mode.
  spilled_ = true;

  // Initialize spill reading state with pre-calculated file row ranges.
  if (!spillFiles_.empty()) {
    initFileRowRanges();
    createSpillStreams();
    cacheStartRow_ = 0;
    cacheEndRow_ = 0;
    cache_.clear();
  }
}

RowVectorPtr SpillableVectorBasedWindowPartition::copyBlockToRowVector(
    const RowBlock& block) const {
  auto numRows = block.size();

  // Always deep copy to ensure shared references are broken.
  // inputType_ is reordered format, block.input is original format.
  // Map from reordered position (i) to original column index.
  auto result = BaseVector::create<RowVector>(inputType_, numRows, pool_);

  for (auto i = 0; i < inputType_->size(); ++i) {
    auto originalCol = reorderedToOriginal_[i];
    result->childAt(i)->copy(
        block.input->childAt(originalCol).get(),
        0, // targetIndex
        block.startRow, // sourceIndex
        numRows); // count
  }

  return result;
}

void SpillableVectorBasedWindowPartition::createSpillStreams(
    size_t startFileIndex) const {
  VELOX_CHECK(!spillFiles_.empty(), "No spill files available");

  batchStreams_.clear();
  batchStreams_.reserve(spillFiles_.size() - startFileIndex);

  for (size_t i = startFileIndex; i < spillFiles_.size(); ++i) {
    auto readFile = SpillReadFile::create(
        spillFiles_[i], readBufferSize_, pool_, spillStats_);
    batchStreams_.push_back(FileSpillBatchStream::create(std::move(readFile)));
  }

  currentFileIndex_ = 0;
  currentBatch_.reset();
  currentBatchIndex_ = 0;
}

void SpillableVectorBasedWindowPartition::clearCache() const {
  cache_.clear();
  cacheRowCount_ = 0;
  currentBatch_.reset();
  currentBatchIndex_ = 0;
}

void SpillableVectorBasedWindowPartition::seekToRow(
    vector_size_t targetRow) const {
  clearCache();

  // Find the file containing the target row using pre-calculated ranges.
  auto [fileIndex, fileStartRow] = findFileForRow(targetRow);

  // Recreate streams starting from that file.
  createSpillStreams(fileIndex);

  cacheStartRow_ = cacheEndRow_ = fileStartRow;

  // Skip rows in file until target row.
  skipRowsInFile(fileIndex, targetRow);

  cacheStartRow_ = cacheEndRow_;
}

void SpillableVectorBasedWindowPartition::skipRowsInFile(
    size_t startFileIndex,
    vector_size_t targetRow) const {
  while (cacheEndRow_ < targetRow && currentFileIndex_ < batchStreams_.size()) {
    auto fileIdx = startFileIndex + currentFileIndex_;
    auto [fileStart, fileEnd] = fileRowRanges_[fileIdx];

    // If targetRow is at or beyond this file's end, skip the entire file
    // without reading any data - just move to the next file.
    if (targetRow >= fileEnd) {
      cacheEndRow_ = fileEnd;
      ++currentFileIndex_;
      currentBatch_.reset();
      currentBatchIndex_ = 0;
      continue;
    }

    // Target is within this file. Use batch operations to skip efficiently.
    auto& stream = batchStreams_[currentFileIndex_];

    // Load batch if needed.
    if (!currentBatch_) {
      if (!stream->nextBatch(currentBatch_)) {
        // No more data in this file.
        ++currentFileIndex_;
        currentBatch_.reset();
        currentBatchIndex_ = 0;
        continue;
      }
      currentBatchIndex_ = 0;
    }

    // Calculate how many rows to skip in this batch.
    auto remainingInBatch = currentBatch_->size() - currentBatchIndex_;
    auto rowsToSkip = std::min(
        static_cast<vector_size_t>(remainingInBatch), targetRow - cacheEndRow_);

    currentBatchIndex_ += rowsToSkip;
    cacheEndRow_ += rowsToSkip;

    // If batch exhausted, clear it to load next one.
    if (currentBatchIndex_ >= currentBatch_->size()) {
      currentBatch_.reset();
      currentBatchIndex_ = 0;
    }
  }
}

void SpillableVectorBasedWindowPartition::initFileRowRanges() const {
  fileRowRanges_.resize(spillFiles_.size());
  vector_size_t currentRow = 0;
  for (size_t i = 0; i < spillFiles_.size(); ++i) {
    fileRowRanges_[i] = {currentRow, currentRow + spillFiles_[i].numRows};
    currentRow += spillFiles_[i].numRows;
  }
}

std::pair<size_t, vector_size_t>
SpillableVectorBasedWindowPartition::findFileForRow(
    vector_size_t targetRow) const {
  if (targetRow == 0) {
    return {0, 0};
  }

  // Since fileRowRanges_ is pre-calculated from spillFiles_.numRows,
  // use binary search for O(log n) lookup.
  size_t low = 0;
  size_t high = fileRowRanges_.size();

  while (low < high) {
    size_t mid = low + (high - low) / 2;
    auto [fileStart, fileEnd] = fileRowRanges_[mid];

    if (targetRow < fileStart) {
      high = mid;
    } else if (targetRow >= fileEnd) {
      low = mid + 1;
    } else {
      // targetRow is in [fileStart, fileEnd)
      return {mid, fileStart};
    }
  }

  // Should not reach here if targetRow < totalRows_.
  VELOX_FAIL("Target row {} not found in any spill file", targetRow);
}

void SpillableVectorBasedWindowPartition::ensureRowsCached(
    vector_size_t startRow,
    vector_size_t numRows) const {
  auto endRow = startRow + numRows;

  // Check if already in cache.
  if (startRow >= cacheStartRow_ && endRow <= cacheEndRow_) {
    return;
  }

  // Backward access: need to seek.
  if (startRow < cacheStartRow_) {
    seekToRow(startRow);
  }

  // Forward access: load more rows.
  if (endRow > cacheEndRow_) {
    loadRowsIntoCache(endRow, startRow);
  }
}

void SpillableVectorBasedWindowPartition::loadRowsIntoCache(
    vector_size_t targetEndRow,
    vector_size_t minRequiredRow) const {
  // Calculate rows to load.
  vector_size_t rowsToLoad =
      targetEndRow > cacheEndRow_ ? targetEndRow - cacheEndRow_ : 0;

  // Try to evict old rows before loading new ones.
  tryEvictOldRows(minRequiredRow, rowsToLoad);

  // Load rows from spill files using batch operations (no pop() needed).
  while (cacheEndRow_ < targetEndRow &&
         currentFileIndex_ < batchStreams_.size()) {
    auto& stream = batchStreams_[currentFileIndex_];

    // Load batch if needed.
    if (!currentBatch_) {
      if (!stream->nextBatch(currentBatch_)) {
        // Current file exhausted, move to next.
        ++currentFileIndex_;
        currentBatch_.reset();
        currentBatchIndex_ = 0;
        continue;
      }
      currentBatchIndex_ = 0;
    }

    // Calculate remaining rows in this batch.
    auto remainingInBatch = currentBatch_->size() - currentBatchIndex_;

    // Calculate how many rows to load from this batch.
    auto rowsNeeded = targetEndRow - cacheEndRow_;
    auto rowsToLoadFromBatch =
        std::min(static_cast<vector_size_t>(remainingInBatch), rowsNeeded);

    // Add batch (or slice of it) to cache - no copy needed!
    if (currentBatchIndex_ == 0 &&
        rowsToLoadFromBatch == currentBatch_->size()) {
      // Use entire batch directly.
      cache_.push_back({currentBatch_, 0, rowsToLoadFromBatch});
    } else {
      // Use partial batch - store reference with offset.
      cache_.push_back(
          {currentBatch_,
           currentBatchIndex_,
           currentBatchIndex_ + rowsToLoadFromBatch});
    }

    cacheEndRow_ += rowsToLoadFromBatch;
    cacheRowCount_ += rowsToLoadFromBatch;
    currentBatchIndex_ += rowsToLoadFromBatch;

    // If batch exhausted, clear it to load next one.
    if (currentBatchIndex_ >= currentBatch_->size()) {
      currentBatch_.reset();
      currentBatchIndex_ = 0;
    }
  }
}

void SpillableVectorBasedWindowPartition::tryEvictOldRows(
    vector_size_t minRequiredRow,
    vector_size_t rowsToLoad) const {
  if (cache_.empty() || minRequiredRow <= cacheStartRow_) {
    return;
  }

  // Calculate safe rows to evict (before minRequiredRow).
  const auto safeToEvict = minRequiredRow - cacheStartRow_;
  if (safeToEvict == 0) {
    return;
  }

  // Determine how many rows to evict.
  vector_size_t rowsToEvict = 0;
  if (estimateCacheBytes() > maxSpillCacheBytes_) {
    rowsToEvict = std::max<vector_size_t>(1, safeToEvict / 2);
  }
  if (rowsToLoad > 0 && cacheRowCount_ + rowsToLoad > maxSpillCacheRows_) {
    const vector_size_t needed =
        cacheRowCount_ + rowsToLoad - maxSpillCacheRows_;
    rowsToEvict = std::max(rowsToEvict, std::min(needed, safeToEvict));
  }

  if (rowsToEvict == 0) {
    return;
  }

  // Evict blocks from the front of cache.
  vector_size_t evicted = 0;
  while (!cache_.empty() && evicted < rowsToEvict) {
    const auto& block = cache_.front();
    auto blockRows = block.size();

    if (evicted + blockRows <= rowsToEvict) {
      // Evict entire block.
      cache_.erase(cache_.begin());
      evicted += blockRows;
      cacheStartRow_ += blockRows;
      cacheRowCount_ -= blockRows;
    } else {
      // Partial eviction (modify block's startRow).
      auto toEvictFromBlock = rowsToEvict - evicted;
      cache_.front().startRow += toEvictFromBlock;
      cacheStartRow_ += toEvictFromBlock;
      cacheRowCount_ -= toEvictFromBlock;
      evicted += toEvictFromBlock;
    }
  }
}

void SpillableVectorBasedWindowPartition::extractFromCache(
    int32_t columnIndex,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    vector_size_t resultOffset,
    const VectorPtr& result) const {
  // Calculate offset within cache.
  auto cacheOffset = partitionOffset - cacheStartRow_;

  vector_size_t remaining = numRows;
  vector_size_t destOffset = resultOffset;
  vector_size_t currentOffset = cacheOffset;

  for (const auto& block : cache_) {
    if (remaining == 0) {
      break;
    }

    if (currentOffset >= block.size()) {
      currentOffset -= block.size();
      continue;
    }

    auto startInBlock = currentOffset;
    auto available = block.size() - startInBlock;
    auto toCopy = std::min(available, remaining);

    // In spill mode, cache_ contains data in reordered format (inputType_).
    // Map from original column index to reordered position in cache.
    const auto cacheColumn = originalToReordered_[columnIndex];
    result->copy(
        block.input->childAt(cacheColumn).get(),
        destOffset,
        block.startRow + startInBlock,
        toCopy);

    destOffset += toCopy;
    remaining -= toCopy;
    currentOffset = 0; // Subsequent blocks start from beginning.
  }
}

void SpillableVectorBasedWindowPartition::streamingExtractColumn(
    int32_t columnIndex,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    vector_size_t resultOffset,
    const VectorPtr& result) const {
  const auto endRow = partitionOffset + numRows;

  // Handle backward seek if needed.
  if (partitionOffset < cacheStartRow_) {
    seekToRow(partitionOffset);
  }

  // Map from original column index to reordered position in cache.
  const auto cacheColumn = originalToReordered_[columnIndex];

  vector_size_t currentRow = partitionOffset;
  vector_size_t destOffset = resultOffset;

  while (currentRow < endRow) {
    // First, extract any rows already in cache.
    if (currentRow >= cacheStartRow_ && currentRow < cacheEndRow_) {
      auto cacheOffset = currentRow - cacheStartRow_;
      vector_size_t remaining = std::min(cacheEndRow_, endRow) - currentRow;
      vector_size_t currentOffset = cacheOffset;

      for (const auto& block : cache_) {
        if (remaining == 0) {
          break;
        }

        if (currentOffset >= block.size()) {
          currentOffset -= block.size();
          continue;
        }

        auto startInBlock = currentOffset;
        auto available = block.size() - startInBlock;
        auto toCopy = std::min(available, remaining);

        result->copy(
            block.input->childAt(cacheColumn).get(),
            destOffset,
            block.startRow + startInBlock,
            toCopy);

        destOffset += toCopy;
        currentRow += toCopy;
        remaining -= toCopy;
        currentOffset = 0;
      }
    }

    // If we still need more rows, load a batch and continue.
    if (currentRow < endRow) {
      // Calculate batch size: load at most maxSpillCacheRows_ at a time,
      // but enough to make progress.
      auto rowsNeeded = endRow - currentRow;
      auto batchSize =
          std::min(rowsNeeded, static_cast<vector_size_t>(maxSpillCacheRows_));

      // Evict cache before loading to keep memory bounded.
      // We only need currentRow onwards, so everything before is safe to evict.
      tryEvictOldRows(currentRow, batchSize);

      // Load the next batch of rows.
      loadRowsIntoCache(currentRow + batchSize, currentRow);

      // If no progress was made (e.g., no more data), break to avoid infinite
      // loop.
      if (currentRow >= cacheEndRow_) {
        break;
      }
    }
  }
}

void SpillableVectorBasedWindowPartition::streamingExtractNulls(
    int32_t columnIndex,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    const BufferPtr& nullsBuffer) const {
  const auto endRow = partitionOffset + numRows;

  // Handle backward seek if needed.
  if (partitionOffset < cacheStartRow_) {
    seekToRow(partitionOffset);
  }

  // Map from original column index to reordered position in cache.
  const auto cacheColumn = originalToReordered_[columnIndex];
  auto* rawNulls = nullsBuffer->asMutable<uint64_t>();

  vector_size_t currentRow = partitionOffset;
  vector_size_t resultIdx = 0;

  while (currentRow < endRow) {
    // First, extract nulls from any rows already in cache.
    if (currentRow >= cacheStartRow_ && currentRow < cacheEndRow_) {
      auto cacheOffset = currentRow - cacheStartRow_;
      vector_size_t remaining = std::min(cacheEndRow_, endRow) - currentRow;
      vector_size_t currentOffset = cacheOffset;

      for (const auto& block : cache_) {
        if (remaining == 0) {
          break;
        }

        if (currentOffset >= block.size()) {
          currentOffset -= block.size();
          continue;
        }

        auto startInBlock = currentOffset;
        auto available = block.size() - startInBlock;
        auto toCopy = std::min(available, remaining);

        auto child = block.input->childAt(cacheColumn);
        for (vector_size_t j = 0; j < toCopy; ++j) {
          auto srcRow = block.startRow + startInBlock + j;
          bits::setNull(rawNulls, resultIdx + j, child->isNullAt(srcRow));
        }

        resultIdx += toCopy;
        currentRow += toCopy;
        remaining -= toCopy;
        currentOffset = 0;
      }
    }

    // If we still need more rows, load a batch and continue.
    if (currentRow < endRow) {
      auto rowsNeeded = endRow - currentRow;
      auto batchSize =
          std::min(rowsNeeded, static_cast<vector_size_t>(maxSpillCacheRows_));

      tryEvictOldRows(currentRow, batchSize);
      loadRowsIntoCache(currentRow + batchSize, currentRow);

      if (currentRow >= cacheEndRow_) {
        break;
      }
    }
  }
}

vector_size_t SpillableVectorBasedWindowPartition::findPeerGroupEnd(
    vector_size_t startRow) const {
  for (vector_size_t peerEnd = startRow + 1; peerEnd < totalRows_; ++peerEnd) {
    // Load more rows if needed (keep startRow in cache).
    if (peerEnd >= cacheEndRow_) {
      loadRowsIntoCache(peerEnd + 1, startRow);
    }

    // Check if we've reached the end of available data.
    if (peerEnd >= cacheEndRow_) {
      return peerEnd;
    }

    // Check if startRow and peerEnd are peers.
    if (!arePeersCached(startRow, peerEnd)) {
      return peerEnd;
    }
  }
  return totalRows_;
}

bool SpillableVectorBasedWindowPartition::arePeersCached(
    vector_size_t lhsRow,
    vector_size_t rhsRow) const {
  VELOX_DCHECK_GE(lhsRow, cacheStartRow_);
  VELOX_DCHECK_LT(lhsRow, cacheEndRow_);
  VELOX_DCHECK_GE(rhsRow, cacheStartRow_);
  VELOX_DCHECK_LT(rhsRow, cacheEndRow_);

  // Find both rows in cache.
  auto [lhsBlockIdx, lhsLocalRow] = findBlockInCache(lhsRow);
  auto [rhsBlockIdx, rhsLocalRow] = findBlockInCache(rhsRow);

  const auto& lhsBlock = cache_[lhsBlockIdx];
  const auto& rhsBlock = cache_[rhsBlockIdx];

  for (const auto& [column, order] : sortKeyInfo_) {
    // In spill mode, cache contains data in reordered format (inputType_).
    // sortKeyInfo_[i].first is the reordered position, so use it directly.
    auto lhsChild = lhsBlock.input->childAt(column);
    auto rhsChild = rhsBlock.input->childAt(column);

    // Use equalValueAt to check equality.
    if (!lhsChild->equalValueAt(rhsChild.get(), lhsLocalRow, rhsLocalRow)) {
      return false;
    }
  }
  return true;
}

std::pair<size_t, vector_size_t>
SpillableVectorBasedWindowPartition::findBlockInCache(
    vector_size_t globalRow) const {
  auto cacheOffset = globalRow - cacheStartRow_;
  vector_size_t currentOffset = 0;

  for (size_t i = 0; i < cache_.size(); ++i) {
    const auto& block = cache_[i];
    if (cacheOffset < currentOffset + block.size()) {
      auto localRow = block.startRow + (cacheOffset - currentOffset);
      return {i, localRow};
    }
    currentOffset += block.size();
  }

  VELOX_FAIL(
      "Row {} not found in cache [{}, {})",
      globalRow,
      cacheStartRow_,
      cacheEndRow_);
}

uint64_t SpillableVectorBasedWindowPartition::estimateCacheBytes() const {
  uint64_t bytes = 0;
  for (const auto& block : cache_) {
    bytes += block.input->retainedSize();
  }
  return bytes;
}

} // namespace facebook::velox::exec
