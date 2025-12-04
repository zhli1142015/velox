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

#include "velox/common/base/Exceptions.h"
#include "velox/exec/SpillFile.h"

namespace facebook::velox::exec {

namespace {
// Minimum cache capacity to ensure reasonable batch sizes.
constexpr vector_size_t kMinCacheCapacity = 100;

// Computes cache capacity based on buffer size and row size.
vector_size_t computeCacheCapacity(
    uint64_t bufferSize,
    const RowContainer* container) {
  auto rowSize = container->fixedRowSize() + container->rowSizeOffset();
  auto capacity = static_cast<vector_size_t>(bufferSize / rowSize);
  return std::max(capacity, kMinCacheCapacity);
}
} // namespace

SpillableWindowPartition::SpillableWindowPartition(
    RowContainer* data,
    folly::Range<char**> rows,
    const std::vector<column_index_t>& inputMapping,
    const std::vector<std::pair<column_index_t, core::SortOrder>>& sortKeyInfo,
    const RowTypePtr& inputType,
    const common::SpillConfig* spillConfig,
    folly::Synchronized<common::SpillStats>* spillStats)
    : WindowPartition(data, inputMapping, sortKeyInfo, true /* spillable */),
      inMemoryRows_(rows),
      inputType_(inputType),
      spillConfig_(spillConfig),
      spillStats_(spillStats) {
  VELOX_CHECK_NOT_NULL(data, "RowContainer cannot be null.");
  VELOX_CHECK(!rows.empty(), "Rows cannot be empty.");
  VELOX_CHECK_NOT_NULL(inputType_, "Input type is required for spill support.");
  // spillConfig_ can be nullptr if spill is not enabled. The check is deferred
  // to spill() method.

  inMemoryPartition_ =
      std::make_unique<WindowPartition>(data, rows, inputMapping, sortKeyInfo);
}

SpillableWindowPartition::SpillableWindowPartition(
    RowContainer* data,
    SpillFiles spillFiles,
    vector_size_t totalRows,
    const std::vector<column_index_t>& inputMapping,
    const std::vector<std::pair<column_index_t, core::SortOrder>>& sortKeyInfo,
    uint64_t readBufferSize,
    uint64_t maxSpillCacheBytes,
    uint64_t maxSpillCacheRows,
    folly::Synchronized<common::SpillStats>* spillStats)
    : WindowPartition(data, inputMapping, sortKeyInfo, true /* spillable */),
      spillStats_(spillStats),
      totalRows_(totalRows),
      spillFiles_(std::move(spillFiles)),
      readBufferSize_(readBufferSize),
      maxSpillCacheBytes_(maxSpillCacheBytes),
      maxSpillCacheRows_(maxSpillCacheRows) {
  VELOX_CHECK(!spillFiles_.empty(), "Spill files cannot be empty.");
  VELOX_CHECK_GT(totalRows_, 0, "Total rows must be positive.");
  VELOX_CHECK_GT(readBufferSize_, 0, "Read buffer size must be positive.");

  initializeSpillState();
}

void SpillableWindowPartition::spill() {
  if (isSpilled()) {
    return;
  }

  VELOX_CHECK_NOT_NULL(inMemoryPartition_);
  VELOX_CHECK_NOT_NULL(spillConfig_, "Spill config is required for spill().");

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

void SpillableWindowPartition::spillRowsToFiles() {
  auto spiller = std::make_unique<SortOutputSpiller>(
      data_, inputType_, spillConfig_, spillStats_);

  SpillerBase::SpillRows spillRows(
      inMemoryRows_.begin(),
      inMemoryRows_.end(),
      SpillerBase::SpillRows::allocator_type(*memory::spillMemoryPool()));
  spiller->spill(spillRows);

  SpillPartitionSet spillPartitionSet;
  spiller->finishSpill(spillPartitionSet);

  VELOX_CHECK_EQ(
      spillPartitionSet.size(), 1, "Expected single spill partition.");
  spillFiles_ = spillPartitionSet.begin()->second->files();
}

void SpillableWindowPartition::transitionToSpillMode() {
  inMemoryPartition_.reset();
  inMemoryRows_ = folly::Range<char**>();

  if (!spillFiles_.empty()) {
    initializeSpillState();
  } else {
    // Empty partition - just set cache capacity.
    cacheCapacity_ = computeCacheCapacity(readBufferSize_, data_);
  }
}

void SpillableWindowPartition::initializeSpillState() {
  // Create dedicated RowContainer for spill cache, isolated from shared data_.
  spillRowContainer_ = std::make_unique<RowContainer>(
      std::vector<TypePtr>{}, data_->columnTypes(), data_->pool());

  cacheCapacity_ =
      computeCacheCapacity(readBufferSize_, spillRowContainer_.get());
  fileRowRanges_.resize(spillFiles_.size(), {0, 0});
  createSpillStreams();

  cacheStartRow_ = 0;
  cacheEndRow_ = 0;
  cachedRows_.clear();
}

vector_size_t SpillableWindowPartition::numRows() const {
  return isSpilled() ? totalRows_ : inMemoryPartition_->numRows();
}

vector_size_t SpillableWindowPartition::numRowsForProcessing(
    vector_size_t partitionOffset) const {
  return isSpilled()
      ? totalRows_ - partitionOffset
      : inMemoryPartition_->numRowsForProcessing(partitionOffset);
}

void SpillableWindowPartition::extractColumn(
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

  // Check for contiguous row numbers - use optimized sequential path.
  if (isContiguousRange(rowNumbers)) {
    extractColumn(
        columnIndex, rowNumbers[0], rowNumbers.size(), resultOffset, result);
    return;
  }

  extractColumnScattered(columnIndex, rowNumbers, resultOffset, result);
}

void SpillableWindowPartition::extractColumnScattered(
    int32_t columnIndex,
    folly::Range<const vector_size_t*> rowNumbers,
    vector_size_t resultOffset,
    const VectorPtr& result) const {
  // Find valid row range (negative values are special markers).
  vector_size_t minRow = totalRows_, maxRow = 0;
  for (auto rowNum : rowNumbers) {
    if (rowNum >= 0) {
      minRow = std::min(minRow, rowNum);
      maxRow = std::max(maxRow, rowNum);
    }
  }

  if (minRow > maxRow) {
    // All rows are invalid - set all to null.
    result->resize(resultOffset + rowNumbers.size());
    for (vector_size_t i = 0; i < rowNumbers.size(); ++i) {
      result->setNull(resultOffset + i, true);
    }
    return;
  }

  ensureRowsLoaded(minRow, maxRow + 1);

  // Build row pointers (nullptr for negative row numbers).
  std::vector<const char*> rowPtrs(rowNumbers.size());
  for (size_t i = 0; i < rowNumbers.size(); ++i) {
    rowPtrs[i] = rowNumbers[i] >= 0 ? getRowPtr(rowNumbers[i]) : nullptr;
  }

  std::vector<vector_size_t> indices(rowNumbers.size());
  std::iota(indices.begin(), indices.end(), 0);

  spillRowContainer_->extractColumn(
      rowPtrs.data(),
      folly::Range<const vector_size_t*>(indices.data(), indices.size()),
      inputMapping_[columnIndex],
      resultOffset,
      result);
}

void SpillableWindowPartition::extractColumn(
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

  if (numRows == 0) {
    return;
  }

  ensureRowsLoaded(partitionOffset, partitionOffset + numRows);
  extractColumnFromCache(
      columnIndex, partitionOffset, numRows, resultOffset, result);
}

void SpillableWindowPartition::extractColumnFromCache(
    int32_t columnIndex,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    vector_size_t resultOffset,
    const VectorPtr& result) const {
  VELOX_DCHECK_GE(partitionOffset, cacheStartRow_);
  VELOX_DCHECK_LE(partitionOffset + numRows, cacheEndRow_);

  vector_size_t cacheOffset = partitionOffset - cacheStartRow_;
  spillRowContainer_->extractColumn(
      reinterpret_cast<const char* const*>(cachedRows_.data() + cacheOffset),
      numRows,
      inputMapping_[columnIndex],
      resultOffset,
      result);
}

void SpillableWindowPartition::extractNulls(
    int32_t columnIndex,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    const BufferPtr& nullsBuffer) const {
  if (!isSpilled()) {
    inMemoryPartition_->extractNulls(
        columnIndex, partitionOffset, numRows, nullsBuffer);
    return;
  }

  if (numRows == 0) {
    return;
  }

  ensureRowsLoaded(partitionOffset, partitionOffset + numRows);
  VELOX_DCHECK_GE(partitionOffset, cacheStartRow_);
  VELOX_DCHECK_LE(partitionOffset + numRows, cacheEndRow_);

  vector_size_t cacheOffset = partitionOffset - cacheStartRow_;
  spillRowContainer_->extractNulls(
      reinterpret_cast<const char* const*>(cachedRows_.data() + cacheOffset),
      numRows,
      inputMapping_[columnIndex],
      nullsBuffer);
}

std::optional<std::pair<vector_size_t, vector_size_t>>
SpillableWindowPartition::extractNulls(
    column_index_t col,
    const SelectivityVector& validRows,
    const BufferPtr& frameStarts,
    const BufferPtr& frameEnds,
    BufferPtr* nulls) const {
  if (!isSpilled()) {
    return inMemoryPartition_->extractNulls(
        col, validRows, frameStarts, frameEnds, nulls);
  }

  auto [minRow, maxRow] = computeRowRange(validRows, frameStarts, frameEnds);
  if (minRow >= maxRow) {
    return std::nullopt;
  }

  ensureRowsLoaded(minRow, maxRow);

  // Check if any nulls exist in the range.
  bool hasNulls = false;
  vector_size_t nullCount = 0;

  validRows.applyToSelected([&](auto i) {
    auto* rawFrameStarts = frameStarts->as<vector_size_t>();
    auto* rawFrameEnds = frameEnds->as<vector_size_t>();
    for (auto row = rawFrameStarts[i]; row < rawFrameEnds[i]; ++row) {
      if (row >= cacheStartRow_ && row < cacheEndRow_) {
        if (spillRowContainer_->isNullAt(getRowPtr(row), columns_[col])) {
          hasNulls = true;
          ++nullCount;
        }
      }
    }
  });

  if (!hasNulls) {
    return std::nullopt;
  }

  // Build null bitmap for cached rows.
  auto numCachedRows = cacheEndRow_ - cacheStartRow_;
  *nulls = AlignedBuffer::allocate<bool>(
      numCachedRows, spillRowContainer_->pool(), false);
  auto rawNulls = (*nulls)->asMutable<uint64_t>();

  for (vector_size_t i = 0; i < numCachedRows; ++i) {
    auto isNull = spillRowContainer_->isNullAt(cachedRows_[i], columns_[col]);
    bits::setBit(rawNulls, i, isNull);
  }

  return std::make_pair(cacheStartRow_, nullCount);
}

std::pair<vector_size_t, vector_size_t>
SpillableWindowPartition::computeRowRange(
    const SelectivityVector& validRows,
    const BufferPtr& frameStarts,
    const BufferPtr& frameEnds) const {
  auto* rawFrameStarts = frameStarts->as<vector_size_t>();
  auto* rawFrameEnds = frameEnds->as<vector_size_t>();

  vector_size_t minRow = totalRows_;
  vector_size_t maxRow = 0;

  validRows.applyToSelected([&](auto i) {
    minRow = std::min(minRow, rawFrameStarts[i]);
    maxRow = std::max(maxRow, rawFrameEnds[i]);
  });

  return {minRow, maxRow};
}

bool SpillableWindowPartition::isContiguousRange(
    folly::Range<const vector_size_t*> rowNumbers) {
  if (rowNumbers.empty()) {
    return true;
  }

  vector_size_t startRow = rowNumbers[0];
  if (startRow < 0) {
    return false;
  }

  for (size_t i = 1; i < rowNumbers.size(); ++i) {
    if (rowNumbers[i] != startRow + static_cast<vector_size_t>(i)) {
      return false;
    }
  }
  return true;
}

bool SpillableWindowPartition::arePeerRows(const char* lhs, const char* rhs)
    const {
  if (lhs == rhs) {
    return true;
  }
  auto* container = isSpilled() ? spillRowContainer_.get() : data_;
  for (const auto& key : sortKeyInfo_) {
    if (container->compare(
            lhs,
            rhs,
            key.first,
            {key.second.isNullsFirst(), key.second.isAscending(), false})) {
      return false;
    }
  }
  return true;
}

std::pair<vector_size_t, vector_size_t>
SpillableWindowPartition::computePeerBuffers(
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

  ensureRowsLoaded(start, end);

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

vector_size_t SpillableWindowPartition::findPeerGroupEnd(
    vector_size_t startRow) const {
  for (vector_size_t peerEnd = startRow + 1; peerEnd < totalRows_; ++peerEnd) {
    if (peerEnd >= cacheEndRow_) {
      loadRowsFromSpill(peerEnd + 1, startRow);
    }
    if (peerEnd >= cacheEndRow_ ||
        !arePeerRows(getRowPtr(startRow), getRowPtr(peerEnd))) {
      return peerEnd;
    }
  }
  return totalRows_;
}

void SpillableWindowPartition::computeKRangeFrameBounds(
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

  ensureRowsLoaded(startRow, startRow + numRows);

  // Use conservative bounds for k-range frame computation.
  for (vector_size_t i = 0; i < numRows; ++i) {
    auto currentRow = startRow + i;
    if (currentRow < cacheStartRow_ || currentRow >= cacheEndRow_) {
      continue;
    }

    rawFrameBounds[i] = computeKRangeFrameBound(
        isStartBound, isPreceding, currentRow, rawPeerStarts[i]);
    validFrames.setValid(i, true);
  }
  validFrames.updateBounds();
}

vector_size_t SpillableWindowPartition::computeKRangeFrameBound(
    bool isStartBound,
    bool isPreceding,
    vector_size_t currentRow,
    vector_size_t peerStart) const {
  if (isStartBound) {
    return isPreceding ? peerStart : currentRow;
  } else {
    return isPreceding ? currentRow : totalRows_;
  }
}

void SpillableWindowPartition::ensureRowsLoaded(
    vector_size_t startRow,
    vector_size_t endRow) const {
  VELOX_DCHECK(isSpilled());
  VELOX_CHECK_LE(endRow, totalRows_);

  if (startRow >= endRow) {
    return;
  }

  // Backward access: seek to target row.
  if (startRow < cacheStartRow_) {
    seekToRow(startRow);
  }

  // Forward: load more rows if needed.
  if (endRow > cacheEndRow_) {
    loadRowsFromSpill(endRow, startRow);
    VELOX_CHECK_LE(endRow, cacheEndRow_);
  }
}

void SpillableWindowPartition::loadRowsFromSpill(
    vector_size_t targetEndRow,
    vector_size_t minRequiredRow) const {
  VELOX_DCHECK_NOT_NULL(spillRowContainer_);

  // Calculate how many new rows we need to load.
  vector_size_t rowsToLoad =
      targetEndRow > cacheEndRow_ ? targetEndRow - cacheEndRow_ : 0;

  // Try to evict old rows before loading new ones to control memory usage.
  // Pass the number of rows we're about to load so eviction can be proactive.
  tryEvictOldRows(minRequiredRow, rowsToLoad);

  while (cacheEndRow_ < targetEndRow) {
    // Find a stream with data.
    while (currentFileIndex_ < spillStreams_.size() &&
           !spillStreams_[currentFileIndex_]->hasData()) {
      // Record the end of the exhausted file.
      if (fileRowRanges_[currentFileIndex_].second == 0) {
        fileRowRanges_[currentFileIndex_].second = cacheEndRow_;
      }
      ++currentFileIndex_;
      // Record the start of the next file.
      if (currentFileIndex_ < spillStreams_.size()) {
        fileRowRanges_[currentFileIndex_].first = cacheEndRow_;
      }
    }

    if (currentFileIndex_ >= spillStreams_.size()) {
      break;
    }

    auto& stream = spillStreams_[currentFileIndex_];
    const auto& rowVector = stream->current();
    auto rowIndex = stream->currentIndex();

    auto* newRow = spillRowContainer_->newRow();
    cachedRows_.push_back(newRow);

    for (column_index_t col = 0; col < rowVector.childrenSize(); ++col) {
      spillRowContainer_->store(stream->decoded(col), rowIndex, newRow, col);
    }

    ++cacheEndRow_;
    stream->pop();
  }

  // Update the end row of the current file.
  if (currentFileIndex_ < fileRowRanges_.size()) {
    fileRowRanges_[currentFileIndex_].second = cacheEndRow_;
  }
}

void SpillableWindowPartition::createSpillStreams(size_t startFileIndex) const {
  VELOX_CHECK(!spillFiles_.empty(), "No spill files available.");
  VELOX_DCHECK_NOT_NULL(spillRowContainer_);

  spillStreams_.clear();
  spillStreams_.reserve(spillFiles_.size() - startFileIndex);

  for (size_t i = startFileIndex; i < spillFiles_.size(); ++i) {
    auto readFile = SpillReadFile::create(
        spillFiles_[i],
        readBufferSize_,
        spillRowContainer_->pool(),
        spillStats_);
    spillStreams_.push_back(FileSpillMergeStream::create(std::move(readFile)));
  }

  currentFileIndex_ = 0;
}

void SpillableWindowPartition::seekToRow(vector_size_t targetRow) const {
  clearCache();

  auto [fileIndex, fileStartRow] = findFileForRow(targetRow);
  createSpillStreams(fileIndex);
  resetFileRowRanges(fileIndex, fileStartRow);

  cacheStartRow_ = cacheEndRow_ = fileStartRow;
  skipRowsInFile(fileIndex, targetRow);
  cacheStartRow_ = cacheEndRow_;
}

void SpillableWindowPartition::clearCache() const {
  if (cachedRows_.empty()) {
    return;
  }
  spillRowContainer_->eraseRows(
      folly::Range<char**>(cachedRows_.data(), cachedRows_.size()));
  cachedRows_.clear();
}

std::pair<size_t, vector_size_t> SpillableWindowPartition::findFileForRow(
    vector_size_t targetRow) const {
  if (targetRow == 0) {
    return {0, 0};
  }

  for (size_t i = 0; i < fileRowRanges_.size(); ++i) {
    auto [fileStart, fileEnd] = fileRowRanges_[i];

    // File has been read and contains target row.
    if (fileEnd > 0 && fileStart <= targetRow && targetRow < fileEnd) {
      return {i, fileStart};
    }

    // Target is after this file, continue searching.
    if (fileEnd > 0 && targetRow >= fileEnd) {
      continue;
    }

    // File not yet read or target is before known range.
    vector_size_t startRow = (i > 0 && fileRowRanges_[i - 1].second > 0)
        ? fileRowRanges_[i - 1].second
        : 0;
    return {i, startRow};
  }

  return {0, 0};
}

void SpillableWindowPartition::resetFileRowRanges(
    size_t startFileIndex,
    vector_size_t fileStartRow) const {
  for (size_t i = startFileIndex; i < fileRowRanges_.size(); ++i) {
    fileRowRanges_[i] = {0, 0};
  }
  if (startFileIndex < fileRowRanges_.size()) {
    fileRowRanges_[startFileIndex].first = fileStartRow;
  }
}

void SpillableWindowPartition::skipRowsInFile(
    size_t startFileIndex,
    vector_size_t targetRow) const {
  while (cacheEndRow_ < targetRow && currentFileIndex_ < spillStreams_.size()) {
    auto& stream = spillStreams_[currentFileIndex_];
    if (!stream->hasData()) {
      auto fileIdx = startFileIndex + currentFileIndex_;
      if (fileRowRanges_[fileIdx].second == 0) {
        fileRowRanges_[fileIdx].second = cacheEndRow_;
      }
      ++currentFileIndex_;
      if (currentFileIndex_ < spillStreams_.size()) {
        fileRowRanges_[startFileIndex + currentFileIndex_].first = cacheEndRow_;
      }
      continue;
    }
    stream->pop();
    ++cacheEndRow_;
  }
}

void SpillableWindowPartition::tryEvictOldRows(
    vector_size_t minRequiredRow,
    vector_size_t rowsToLoad) const {
  if (cachedRows_.empty() || minRequiredRow <= cacheStartRow_) {
    return;
  }

  // Only evict rows before minRequiredRow (rows that are no longer needed).
  const auto safeToEvict = std::min(
      minRequiredRow - cacheStartRow_,
      static_cast<vector_size_t>(cachedRows_.size()));
  if (safeToEvict == 0) {
    return;
  }

  // Determine how many rows to evict:
  // 1. Memory limit exceeded: evict half of safe rows
  // 2. Row count limit would be exceeded: evict enough to stay under limit
  vector_size_t rowsToEvict = 0;
  if (spillRowContainer_->pool()->usedBytes() > maxSpillCacheBytes_) {
    rowsToEvict = std::max<vector_size_t>(1, safeToEvict / 2);
  }
  if (rowsToLoad > 0 && cachedRows_.size() + rowsToLoad > maxSpillCacheRows_) {
    const vector_size_t needed =
        static_cast<vector_size_t>(cachedRows_.size()) + rowsToLoad -
        maxSpillCacheRows_;
    rowsToEvict = std::max(rowsToEvict, std::min(needed, safeToEvict));
  }

  if (rowsToEvict > 0) {
    spillRowContainer_->eraseRows(
        folly::Range<char**>(cachedRows_.data(), rowsToEvict));
    cachedRows_.erase(cachedRows_.begin(), cachedRows_.begin() + rowsToEvict);
    cacheStartRow_ += rowsToEvict;
  }
}

} // namespace facebook::velox::exec
