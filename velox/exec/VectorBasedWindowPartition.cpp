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

#include "velox/exec/VectorBasedWindowPartition.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::exec {

VectorBasedWindowPartition::VectorBasedWindowPartition(
    const std::vector<column_index_t>& inputMapping,
    const std::vector<std::pair<column_index_t, core::SortOrder>>& sortKeyInfo,
    bool partial)
    : WindowPartition(inputMapping, sortKeyInfo, partial) {
  blockPrefixSums_.push_back(0);
}

VectorBasedWindowPartition::VectorBasedWindowPartition(
    const std::vector<RowBlock>& blocks,
    const std::vector<column_index_t>& inputMapping,
    const std::vector<std::pair<column_index_t, core::SortOrder>>& sortKeyInfo)
    : WindowPartition(inputMapping, sortKeyInfo, false) {
  // Initialize with blocks directly, complete_=true from partial=false.
  blockPrefixSums_.push_back(0);
  for (const auto& block : blocks) {
    blocks_.push_back(block);
    blockPrefixSums_.push_back(blockPrefixSums_.back() + block.size());
    totalRows_ += block.size();
  }
}

void VectorBasedWindowPartition::extractColumn(
    int32_t columnIndex,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    vector_size_t resultOffset,
    const VectorPtr& result) const {
  // columnIndex is the original input column index (0, 1, 2, ...).
  // VectorBasedWindowPartition stores original input vectors.
  // So we directly access block.input->childAt(columnIndex).
  VELOX_CHECK_GE(partitionOffset, startRow_);

  if (numRows == 0) {
    return;
  }

  vector_size_t globalRow = partitionOffset - startRow_;
  auto [blockIdx, localRow] = findBlock(globalRow);

  vector_size_t remaining = numRows;
  vector_size_t destOffset = resultOffset;

  while (remaining > 0) {
    const auto& block = blocks_[blockIdx];
    auto available = block.endRow - localRow;
    auto toCopy = std::min(available, remaining);

    // Use BaseVector::copy() for batch copy within a block
    result->copy(
        block.input->childAt(columnIndex).get(), destOffset, localRow, toCopy);

    destOffset += toCopy;
    remaining -= toCopy;

    if (remaining > 0) {
      // Move to next block
      ++blockIdx;
      localRow = blocks_[blockIdx].startRow;
    }
  }
}

void VectorBasedWindowPartition::extractColumn(
    int32_t columnIndex,
    folly::Range<const vector_size_t*> rowNumbers,
    vector_size_t resultOffset,
    const VectorPtr& result) const {
  // columnIndex is the original input column index.
  // VectorBasedWindowPartition stores original input vectors.
  // rowNumbers contains absolute partition row numbers.
  // A rowNumber of kNullRow (-1) indicates null result for that position.

  if (rowNumbers.empty()) {
    return;
  }

  // Track current block to avoid repeated findBlock calls for adjacent rows
  size_t currentBlockIdx = 0;
  vector_size_t blockEndRow =
      0; // Exclusive end row for current block (relative)
  bool blockInitialized = false;

  for (size_t i = 0; i < rowNumbers.size(); ++i) {
    auto rowNumber = rowNumbers[i];

    // Handle kNullRow (-1): set result to null
    if (rowNumber < 0) {
      result->setNull(resultOffset + i, true);
      continue;
    }

    auto relativeRow = rowNumber - startRow_;

    // Check if we need to find a new block
    if (!blockInitialized || relativeRow >= blockEndRow ||
        relativeRow < blockPrefixSums_[currentBlockIdx]) {
      auto [blockIdx, localRow] = findBlock(relativeRow);
      currentBlockIdx = blockIdx;
      blockEndRow = blockPrefixSums_[currentBlockIdx + 1];
      blockInitialized = true;
    }

    // Calculate local row within the current block
    vector_size_t offsetInPartition =
        relativeRow - blockPrefixSums_[currentBlockIdx];
    vector_size_t localRow =
        blocks_[currentBlockIdx].startRow + offsetInPartition;

    result->copy(
        blocks_[currentBlockIdx].input->childAt(columnIndex).get(),
        resultOffset + i,
        localRow,
        1);
  }
}

void VectorBasedWindowPartition::extractNulls(
    int32_t columnIndex,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    const BufferPtr& nullsBuffer) const {
  // columnIndex is the original input column index.
  // VectorBasedWindowPartition stores original input vectors.
  VELOX_CHECK_GE(partitionOffset, startRow_);

  if (numRows == 0) {
    return;
  }

  auto* rawNulls = nullsBuffer->asMutable<uint64_t>();
  vector_size_t globalRow = partitionOffset - startRow_;
  auto [blockIdx, localRow] = findBlock(globalRow);

  vector_size_t processed = 0;
  while (processed < numRows) {
    const auto& block = blocks_[blockIdx];
    auto child = block.input->childAt(columnIndex);
    auto available = block.endRow - localRow;
    auto toProcess = std::min(available, numRows - processed);

    // Process rows in the current block.
    // Use bits::setNull/clearNull for correct null semantics:
    // - setNull(bits, idx) = clearBit = 0 = null
    // - clearNull(bits, idx) = setBit = 1 = not null
    for (vector_size_t j = 0; j < toProcess; ++j) {
      if (child->isNullAt(localRow + j)) {
        bits::setNull(rawNulls, processed + j);
      } else {
        bits::clearNull(rawNulls, processed + j);
      }
    }

    processed += toProcess;
    if (processed < numRows) {
      ++blockIdx;
      localRow = blocks_[blockIdx].startRow;
    }
  }
}

bool VectorBasedWindowPartition::arePeers(
    vector_size_t lhsRow,
    vector_size_t rhsRow) const {
  // lhsRow and rhsRow are absolute partition row numbers.
  // Convert to relative row numbers within current blocks.
  auto lhsRelative = lhsRow - startRow_;
  auto rhsRelative = rhsRow - startRow_;

  auto [lhsBlockIdx, lhsLocal] = findBlock(lhsRelative);
  auto [rhsBlockIdx, rhsLocal] = findBlock(rhsRelative);

  const auto& lhsBlock = blocks_[lhsBlockIdx];
  const auto& rhsBlock = blocks_[rhsBlockIdx];

  for (const auto& [column, order] : sortKeyInfo_) {
    // sortKeyInfo_ uses reordered column index, convert to original input index
    auto originalColumn = inputMapping_[column];
    auto lhsChild = lhsBlock.input->childAt(originalColumn);
    auto rhsChild = rhsBlock.input->childAt(originalColumn);

    // Use BaseVector::equalValueAt()
    if (!lhsChild->equalValueAt(rhsChild.get(), lhsLocal, rhsLocal)) {
      return false;
    }
  }
  return true;
}

std::pair<vector_size_t, vector_size_t>
VectorBasedWindowPartition::computePeerBuffers(
    vector_size_t start,
    vector_size_t end,
    vector_size_t prevPeerStart,
    vector_size_t prevPeerEnd,
    vector_size_t* rawPeerStarts,
    vector_size_t* rawPeerEnds) {
  // start and end are absolute partition row numbers.
  // peerStart/peerEnd are also absolute row numbers.
  // The last row in current partition (absolute) is startRow_ + totalRows_ - 1.
  auto lastPartitionRow = startRow_ + totalRows_;

  vector_size_t peerStart = prevPeerStart;
  vector_size_t peerEnd = prevPeerEnd;
  size_t index = 0;

  for (vector_size_t i = start; i < end; ++i, ++index) {
    // Check if we're entering a new peer group
    if (i == 0 || i >= peerEnd) {
      peerStart = i;
      // Find the end of the peer group (exclusive)
      peerEnd = i + 1;
      while (peerEnd < lastPartitionRow && arePeers(i, peerEnd)) {
        ++peerEnd;
      }
    }

    rawPeerStarts[index] = peerStart;
    rawPeerEnds[index] = peerEnd - 1; // Store inclusive end index
  }

  return {peerStart, peerEnd};
}

vector_size_t VectorBasedWindowPartition::searchFrameValue(
    bool firstMatch,
    vector_size_t start,
    vector_size_t end,
    vector_size_t currentRow,
    column_index_t orderByColumn,
    const VectorPtr& frameValue,
    vector_size_t frameValueIndex,
    core::SortOrder sortOrder) const {
  // Binary search to find an approximate starting point, then linear search
  // to find the exact boundary.
  // start and end are absolute partition row numbers.
  auto left = start;
  auto right = end;

  CompareFlags flags;
  flags.ascending = sortOrder.isAscending();
  flags.nullsFirst = sortOrder.isNullsFirst();

  // Binary search for approximate position
  while (right - left >= 2) {
    auto mid = (left + right) / 2;
    // Convert to relative row number for findBlock
    auto [blockIdx, localRow] = findBlock(mid - startRow_);
    const auto& block = blocks_[blockIdx];

    auto orderByChild = block.input->childAt(orderByColumn);

    auto cmp = orderByChild->compare(
        frameValue.get(), localRow, frameValueIndex, flags);

    if (!cmp.has_value() || cmp.value() >= 0) {
      // NULL or orderBy >= frame: search in first half
      right = mid;
    } else {
      // orderBy < frame: search in second half
      left = mid;
    }
  }

  // Linear search from 'left' to 'end' for exact boundary
  return linearSearchFrameValue(
      firstMatch, left, end, orderByColumn, frameValue, frameValueIndex, flags);
}

vector_size_t VectorBasedWindowPartition::linearSearchFrameValue(
    bool firstMatch,
    vector_size_t start,
    vector_size_t end,
    column_index_t orderByColumn,
    const VectorPtr& frameValue,
    vector_size_t frameValueIndex,
    const CompareFlags& flags) const {
  for (vector_size_t i = start; i < end; ++i) {
    auto [blockIdx, localRow] = findBlock(i - startRow_);
    const auto& block = blocks_[blockIdx];
    auto orderByChild = block.input->childAt(orderByColumn);

    auto cmp = orderByChild->compare(
        frameValue.get(), localRow, frameValueIndex, flags);

    // The bound value was found. Return if firstMatch required.
    if (cmp.has_value() && cmp.value() == 0) {
      if (firstMatch) {
        return i;
      }
    }

    // Bound is crossed.
    if (cmp.has_value() && cmp.value() > 0) {
      if (firstMatch) {
        // First row that crosses the bound
        return i;
      } else {
        // Last match needs the previous row
        return i - 1;
      }
    }
  }

  // Return a row beyond the partition boundary.
  return end == numRows() ? numRows() + 1 : -1;
}

void VectorBasedWindowPartition::computeKRangeFrameBounds(
    bool isStartBound,
    bool isPreceding,
    column_index_t frameColumn,
    vector_size_t startRow,
    vector_size_t numRows,
    const vector_size_t* rawPeerStarts,
    vector_size_t* rawFrameBounds,
    SelectivityVector& validFrames) const {
  // startRow is the absolute partition row number.
  // lastPartitionRow is the last absolute row number in current partition.
  auto lastPartitionRow = startRow_ + totalRows_;

  // The ORDER BY column is used for searching, not the frame column.
  // sortKeyInfo_[0].first is the reordered column index.
  // inputMapping_ maps reordered position -> original column index.
  column_index_t orderByColumn = inputMapping_[sortKeyInfo_[0].first];

  for (vector_size_t i = 0; i < numRows; ++i) {
    auto currentRow = startRow + i;
    // Convert to relative row number for findBlock
    auto [blockIdx, localRow] = findBlock(currentRow - startRow_);
    const auto& block = blocks_[blockIdx];

    auto frameValueVector = block.input->childAt(frameColumn);

    // Check if frame value is NULL
    if (frameValueVector->isNullAt(localRow)) {
      // NULL frame value: use peer row as boundary
      rawFrameBounds[i] = rawPeerStarts[i];
      continue;
    }

    // Use binary search to find the frame boundary
    // searchStart/searchEnd are absolute row numbers
    vector_size_t searchStart = isPreceding ? startRow_ : currentRow;
    vector_size_t searchEnd = isPreceding ? currentRow + 1 : lastPartitionRow;

    rawFrameBounds[i] = searchFrameValue(
        isStartBound,
        searchStart,
        searchEnd,
        currentRow,
        orderByColumn, // Use ORDER BY column for comparison
        frameValueVector, // Frame bound value to compare against
        localRow,
        sortKeyInfo_[0].second); // Sort order from first sort key
  }
}

void VectorBasedWindowPartition::addRows(const RowBlock& block) {
  VELOX_CHECK(partial_, "addRows only valid in partial mode");
  blocks_.push_back(block);
  blockPrefixSums_.push_back(blockPrefixSums_.back() + block.size());
  totalRows_ += block.size();
}

void VectorBasedWindowPartition::removeProcessedRows(vector_size_t numRows) {
  VELOX_CHECK(partial_, "removeProcessedRows only valid in partial mode");

  vector_size_t remaining = numRows;
  while (remaining > 0 && !blocks_.empty()) {
    auto& frontBlock = blocks_.front();
    auto blockSize = frontBlock.size();

    if (remaining >= blockSize) {
      // Remove the entire block, shared_ptr will automatically release memory
      blocks_.erase(blocks_.begin());
      remaining -= blockSize;
    } else {
      // Partial removal: adjust the start position
      frontBlock.startRow += remaining;
      remaining = 0;
    }
  }

  // Update startRow_ to track total removed rows.
  // This is used to convert absolute partitionOffset to relative offset.
  startRow_ += numRows;

  // Rebuild prefix sums
  rebuildPrefixSums();
}

std::pair<size_t, vector_size_t> VectorBasedWindowPartition::findBlock(
    vector_size_t globalRow) const {
  VELOX_DCHECK_LT(globalRow, totalRows_, "Row index out of range");

  // Binary search: find the first prefixSum > globalRow
  auto it = std::upper_bound(
      blockPrefixSums_.begin(), blockPrefixSums_.end(), globalRow);

  size_t blockIdx = std::distance(blockPrefixSums_.begin(), it) - 1;
  vector_size_t offsetInPartition = globalRow - blockPrefixSums_[blockIdx];
  vector_size_t localRow = blocks_[blockIdx].startRow + offsetInPartition;

  return {blockIdx, localRow};
}

void VectorBasedWindowPartition::rebuildPrefixSums() {
  blockPrefixSums_.clear();
  blockPrefixSums_.push_back(0);
  for (const auto& block : blocks_) {
    blockPrefixSums_.push_back(blockPrefixSums_.back() + block.size());
  }
  totalRows_ = blockPrefixSums_.back();
}

} // namespace facebook::velox::exec
