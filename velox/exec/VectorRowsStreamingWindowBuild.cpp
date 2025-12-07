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

#include "velox/exec/VectorRowsStreamingWindowBuild.h"
#include "velox/common/testutil/TestValue.h"

namespace facebook::velox::exec {

VectorRowsStreamingWindowBuild::VectorRowsStreamingWindowBuild(
    const std::shared_ptr<const core::WindowNode>& windowNode,
    velox::memory::MemoryPool* pool,
    const common::SpillConfig* spillConfig,
    tsan_atomic<bool>* nonReclaimableSection)
    : WindowBuild(windowNode, pool, spillConfig, nonReclaimableSection) {
  velox::common::testutil::TestValue::adjust(
      "facebook::velox::exec::VectorRowsStreamingWindowBuild::"
      "VectorRowsStreamingWindowBuild",
      this);
}

void VectorRowsStreamingWindowBuild::addInput(RowVectorPtr input) {
  // Handle non-FlatVector cases:
  // 1. Lazy Vector: call loadedVector() to ensure data is loaded
  // 2. Dictionary/Constant Encoding: BaseVector's compare/equalValueAt
  //    methods transparently handle various encodings

  // Ensure lazy loading is complete for all child vectors
  for (auto& child : input->children()) {
    child->loadedVector();
  }

  vector_size_t blockStart = 0;

  for (auto row = 0; row < input->size(); ++row) {
    // Check partition boundary (compare with previous row)
    if (isNewPartition(input, row)) {
      flushBlock(input, blockStart, row);
      addPartitionInputs(true /* finished */);
      blockStart = row;
    }
    // Check if output is needed (reached numRowsPerOutput_)
    else if (pendingRowCount_ >= numRowsPerOutput_) {
      // For streaming window, we need to wait for peer group to end
      // to ensure correct peer group boundaries for functions like rank().
      if (isNewPeerGroup(input, row)) {
        flushBlock(input, blockStart, row);
        addPartitionInputs(false /* finished */);
        blockStart = row;
      }
    }

    ++pendingRowCount_;
    // Update previousRef_ after each row (consistent with original
    // implementation)
    previousRef_ = {input, row};
  }

  // Save remaining rows to current partition (don't flush, wait for more input
  // or noMoreInput)
  if (blockStart < input->size()) {
    flushBlock(input, blockStart, input->size());
  }
}

void VectorRowsStreamingWindowBuild::noMoreInput() {
  // Complete the last partition
  if (!currentBlocks_.empty()) {
    addPartitionInputs(true /* finished */);
  } else if (
      !windowPartitions_.empty() && !windowPartitions_.back()->complete()) {
    // If there are no pending blocks but we have an incomplete partition,
    // mark it as complete
    windowPartitions_.back()->setComplete();
  }
}

bool VectorRowsStreamingWindowBuild::hasNextPartition() {
  for (const auto& partition : windowPartitions_) {
    if (!partition->complete() || partition->numRows() > 0) {
      return true;
    }
  }
  return false;
}

std::shared_ptr<WindowPartition>
VectorRowsStreamingWindowBuild::nextPartition() {
  // Remove completed partitions with no remaining rows
  while (!windowPartitions_.empty() && windowPartitions_.front()->complete() &&
         windowPartitions_.front()->numRows() == 0) {
    windowPartitions_.pop_front();
  }

  VELOX_CHECK(!windowPartitions_.empty());
  return windowPartitions_.front();
}

void VectorRowsStreamingWindowBuild::flushBlock(
    const RowVectorPtr& input,
    vector_size_t start,
    vector_size_t end) {
  if (start >= end) {
    return;
  }

  currentBlocks_.push_back({input, start, end});
  // Note: pendingRowCount_ is not reset here
  // It's handled by addPartitionInputs() at the appropriate time
}

void VectorRowsStreamingWindowBuild::addPartitionInputs(bool finished) {
  if (currentBlocks_.empty()) {
    return;
  }

  ensureInputPartition();

  // Add accumulated blocks to the current partition
  for (const auto& block : currentBlocks_) {
    windowPartitions_.back()->addRows(block);
  }

  if (finished) {
    windowPartitions_.back()->setComplete();
  }

  currentBlocks_.clear();
  pendingRowCount_ = 0; // Reset pending count here
}

void VectorRowsStreamingWindowBuild::ensureInputPartition() {
  if (windowPartitions_.empty() || windowPartitions_.back()->complete()) {
    // Note: Use inputChannels_ (forward mapping) instead of
    // inversedInputChannels_ because VectorBasedWindowPartition stores original
    // input vectors, and extractColumn(columnIndex) needs to map from reordered
    // column index to original input column index.
    windowPartitions_.emplace_back(
        std::make_shared<VectorBasedWindowPartition>(
            inputChannels_, sortKeyInfo_, true /* partial */));
  }
}

bool VectorRowsStreamingWindowBuild::isNewPartition(
    const RowVectorPtr& input,
    vector_size_t row) {
  // First row is not a new partition boundary
  if (row == 0 && !previousRef_.isValid()) {
    return false;
  }

  // Get the previous row to compare with
  const RowVectorPtr& prevInput = (row == 0) ? previousRef_.input : input;
  vector_size_t prevRow = (row == 0) ? previousRef_.row : row - 1;

  // Use partitionKeyInfo_ from base class (contains reordered column index)
  // Convert to original input column index using inputChannels_
  for (const auto& [keyCol, sortOrder] : partitionKeyInfo_) {
    auto originalCol = inputChannels_[keyCol];
    auto prevChild = prevInput->childAt(originalCol);
    auto currChild = input->childAt(originalCol);

    // Use BaseVector::equalValueAt
    if (!prevChild->equalValueAt(currChild.get(), prevRow, row)) {
      return true;
    }
  }
  return false;
}

bool VectorRowsStreamingWindowBuild::isNewPeerGroup(
    const RowVectorPtr& input,
    vector_size_t row) {
  // First row is not a new peer group boundary
  if (row == 0 && !previousRef_.isValid()) {
    return false;
  }

  // Get the previous row to compare with
  const RowVectorPtr& prevInput = (row == 0) ? previousRef_.input : input;
  vector_size_t prevRow = (row == 0) ? previousRef_.row : row - 1;

  // Use sortKeyInfo_ from base class (contains reordered column index)
  // Convert to original input column index using inputChannels_
  for (const auto& [keyCol, sortOrder] : sortKeyInfo_) {
    auto originalCol = inputChannels_[keyCol];
    auto prevChild = prevInput->childAt(originalCol);
    auto currChild = input->childAt(originalCol);

    if (!prevChild->equalValueAt(currChild.get(), prevRow, row)) {
      return true;
    }
  }
  return false;
}

} // namespace facebook::velox::exec
