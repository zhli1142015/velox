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

#include "velox/exec/VectorPartitionStreamingWindowBuild.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/Spill.h"

namespace facebook::velox::exec {

VectorPartitionStreamingWindowBuild::VectorPartitionStreamingWindowBuild(
    const std::shared_ptr<const core::WindowNode>& windowNode,
    velox::memory::MemoryPool* pool,
    const common::SpillConfig* spillConfig,
    tsan_atomic<bool>* nonReclaimableSection,
    folly::Synchronized<common::SpillStats>* spillStats)
    : WindowBuild(windowNode, pool, spillConfig, nonReclaimableSection),
      spillStats_(spillStats),
      pool_(pool),
      originalInputType_(windowNode->inputType()) {
  // Initialize with an empty partition state (the "building" partition).
  partitionStates_.emplace_back();

  velox::common::testutil::TestValue::adjust(
      "facebook::velox::exec::VectorPartitionStreamingWindowBuild::"
      "VectorPartitionStreamingWindowBuild",
      this);
}

void VectorPartitionStreamingWindowBuild::addInput(RowVectorPtr input) {
  ensureInputFits(input);

  // Ensure lazy vectors are loaded.
  for (auto& child : input->children()) {
    child->loadedVector();
  }

  vector_size_t blockStart = 0;

  for (auto row = 0; row < input->size(); ++row) {
    if (isNewPartition(input, row)) {
      // Finalize current partition and start new one.
      if (!isNewPartitionStart_) {
        // Add rows [blockStart, row) to currentBlocks_ before building
        // partition.
        if (row > blockStart) {
          currentBlocks_.push_back({input, blockStart, row});
        }
        buildNextPartition();
        blockStart = row;
      }
      isNewPartitionStart_ = false;
    }

    // Update previousRef_ for next comparison.
    previousRef_ = {input, row};
  }

  // Add remaining rows to currentBlocks_.
  if (input->size() > blockStart) {
    currentBlocks_.push_back({input, blockStart, input->size()});
  }
}

bool VectorPartitionStreamingWindowBuild::isNewPartition(
    const RowVectorPtr& input,
    vector_size_t row) {
  if (!previousRef_.isValid()) {
    return true;
  }

  // Compare partition keys with previous row.
  for (auto i = 0; i < partitionKeyInfo_.size(); ++i) {
    const auto& [channel, sortOrder] = partitionKeyInfo_[i];
    // channel is the reordered position, we need to map to original column
    // index using inputChannels_.
    auto originalChannel = inputChannels_[channel];
    auto* previousChild = previousRef_.input->childAt(originalChannel).get();
    auto* currentChild = input->childAt(originalChannel).get();

    if (!previousChild->equalValueAt(currentChild, previousRef_.row, row)) {
      return true;
    }
  }

  return false;
}

void VectorPartitionStreamingWindowBuild::buildNextPartition() {
  auto& state = partitionStates_.back();

  // Nothing to build if no data.
  if (currentBlocks_.empty()) {
    // In spill mode with no new blocks, still need to start a new partition
    // if the current one has data (totalRows > 0).
    if (!state.spilled || state.totalRows == 0) {
      return;
    }
  }

  if (state.spilled) {
    // Spill mode: write remaining blocks to spill files.
    if (!currentBlocks_.empty()) {
      auto files = spillBlocksToFiles(currentBlocks_);
      state.spillFiles.insert(
          state.spillFiles.end(), files.begin(), files.end());
      for (const auto& block : currentBlocks_) {
        state.totalRows += block.size();
      }
      currentBlocks_.clear();
    }
  } else {
    // Memory mode: move blocks to the partition state.
    state.blocks = std::move(currentBlocks_);
    for (const auto& block : state.blocks) {
      state.totalRows += block.size();
    }
    currentBlocks_.clear();
  }

  // Start a new partition.
  partitionStates_.emplace_back();
  isNewPartitionStart_ = true;
}

void VectorPartitionStreamingWindowBuild::noMoreInput() {
  // Finalize the last partition by calling buildNextPartition().
  // This moves remaining data to the current partition state and
  // adds a new empty "building" partition, making the last data partition
  // processable via hasNextPartition().
  buildNextPartition();
}

bool VectorPartitionStreamingWindowBuild::hasNextPartition() {
  // partitionStates_[size-1] is always the partition being built.
  return currentPartition_ <
      static_cast<vector_size_t>(partitionStates_.size()) - 2;
}

std::shared_ptr<WindowPartition>
VectorPartitionStreamingWindowBuild::nextPartition() {
  VELOX_CHECK(hasNextPartition());
  ++currentPartition_;

  if (currentPartition_ > 0) {
    cleanupPreviousPartition();
  }

  auto& state = partitionStates_[currentPartition_];

  auto partition = state.spilled ? createSpillablePartition(state)
                                 : createInMemoryPartition(state);

  state.windowPartition = partition;
  return partition;
}

bool VectorPartitionStreamingWindowBuild::needsInput() {
  // Need input if:
  // 1. Only have the building partition (size <= 1), or
  // 2. Currently processing the last completed partition (size - 2).
  return partitionStates_.size() <= 1 ||
      currentPartition_ ==
      static_cast<vector_size_t>(partitionStates_.size()) - 2;
}

void VectorPartitionStreamingWindowBuild::spill() {
  if (!canSpill()) {
    return;
  }

  // 1. Mark already processed partitions as spilled (their data is released).
  for (auto i = 0; i <= currentPartition_; ++i) {
    if (i < partitionStates_.size()) {
      partitionStates_[i].spilled = true;
    }
  }

  // 2. Spill the active partition if it exists and is in memory.
  if (currentPartition_ >= 0 &&
      currentPartition_ < static_cast<vector_size_t>(partitionStates_.size()) &&
      !partitionStates_[currentPartition_].spilled) {
    if (auto partition =
            partitionStates_[currentPartition_].windowPartition.lock()) {
      partition->spill();
    }
    partitionStates_[currentPartition_].spilled = true;
  }

  // 3. Spill completed partitions waiting to be processed.
  spillCompletedPartitions();

  // 4. Spill the partition being built (currentBlocks_).
  if (!currentBlocks_.empty()) {
    spillCurrentBlocks();
  } else {
    // Even if currentBlocks_ is empty, preserve previousRef_.
    preservePreviousRef();
  }
}

void VectorPartitionStreamingWindowBuild::spillCompletedPartitions() {
  // Partition layout: [processed | active | waiting | building]
  // Only spill waiting partitions [currentPartition_+1 .. size-2].
  const auto firstWaiting =
      std::max(0, static_cast<int>(currentPartition_) + 1);
  const auto lastCompleted = static_cast<int>(partitionStates_.size()) - 2;

  for (auto i = firstWaiting; i <= lastCompleted; ++i) {
    auto& state = partitionStates_[i];
    if (!state.spilled && state.totalRows > 0) {
      spillPartitionBlocks(state);
    }
  }
}

void VectorPartitionStreamingWindowBuild::spillPartitionBlocks(
    PartitionState& state) {
  if (state.blocks.empty()) {
    return;
  }

  auto files = spillBlocksToFiles(state.blocks);
  state.spillFiles.insert(state.spillFiles.end(), files.begin(), files.end());
  state.blocks.clear();
  state.spilled = true;
}

void VectorPartitionStreamingWindowBuild::spillCurrentBlocks() {
  if (currentBlocks_.empty()) {
    return;
  }

  // The building partition is always partitionStates_.back().
  auto& state = partitionStates_.back();

  // Calculate rows being spilled.
  vector_size_t rowsToSpill = 0;
  for (const auto& block : currentBlocks_) {
    rowsToSpill += block.size();
  }

  // Spill blocks and get file list.
  auto files = spillBlocksToFiles(currentBlocks_);
  state.spillFiles.insert(state.spillFiles.end(), files.begin(), files.end());
  currentBlocks_.clear();

  // Preserve previousRef_ for partition boundary detection.
  preservePreviousRef();

  if (!state.spilled) {
    state.spilled = true;
    state.totalRows = rowsToSpill;
  } else {
    state.totalRows += rowsToSpill;
  }
}

SpillFiles VectorPartitionStreamingWindowBuild::spillBlocksToFiles(
    const std::vector<RowBlock>& blocks) {
  ensureSpiller();

  for (const auto& block : blocks) {
    auto spillVector = copyBlockToRowVector(block);
    spiller_->spill(SpillPartitionId(0), spillVector);
  }

  SpillPartitionSet spillPartitionSet;
  spiller_->finishSpill(spillPartitionSet);
  spiller_.reset();

  SpillFiles result;
  for (auto& entry : spillPartitionSet) {
    auto files = entry.second->files();
    result.insert(result.end(), files.begin(), files.end());
  }
  return result;
}

void VectorPartitionStreamingWindowBuild::preservePreviousRef() {
  if (previousRef_.isValid()) {
    preservedPreviousRow_ = extractSingleRow(previousRef_);
    previousRef_ = {preservedPreviousRow_, 0};
  }
}

RowVectorPtr VectorPartitionStreamingWindowBuild::copyBlockToRowVector(
    const RowBlock& block) {
  auto numRows = block.size();
  auto result = BaseVector::create<RowVector>(inputType_, numRows, pool_);

  // inputType_ is in reordered format (partition keys, sort keys, rest).
  // block.input stores original input vectors (user's column order).
  // inputChannels_[reorderedPos] = originalColumnIndex
  for (auto i = 0; i < inputType_->size(); ++i) {
    auto originalCol = inputChannels_[i];
    result->childAt(i)->copy(
        block.input->childAt(originalCol).get(),
        0, // targetIndex
        block.startRow, // sourceIndex
        numRows); // count
  }

  return result;
}

RowVectorPtr VectorPartitionStreamingWindowBuild::extractSingleRow(
    const RowReference& ref) {
  // Keep original column order for isNewPartition() comparison.
  // ref.input is in original format, and isNewPartition() expects original
  // format.
  auto result = BaseVector::create<RowVector>(originalInputType_, 1, pool_);
  for (auto i = 0; i < originalInputType_->size(); ++i) {
    result->childAt(i)->copy(ref.input->childAt(i).get(), 0, ref.row, 1);
  }
  return result;
}

void VectorPartitionStreamingWindowBuild::ensureSpiller() {
  if (spiller_ == nullptr) {
    spiller_ = std::make_unique<NoRowContainerSpiller>(
        inputType_,
        std::nullopt, // parentId
        HashBitRange{},
        spillConfig_,
        spillStats_);
  }
}

void VectorPartitionStreamingWindowBuild::ensureInputFits(
    const RowVectorPtr& input) {
  if (spillConfig_ == nullptr) {
    return;
  }

  // Nothing to spill if no blocks and no data in partition states.
  // Note: partitionStates_ always has at least one "building" partition,
  // so we need to check if there's actually any data to spill.
  if (currentBlocks_.empty()) {
    bool hasData = false;
    for (const auto& state : partitionStates_) {
      if (state.totalRows > 0 || !state.blocks.empty()) {
        hasData = true;
        break;
      }
    }
    if (!hasData) {
      return;
    }
  }

  // Test-only spill path.
  if (testingTriggerSpill(pool_->name())) {
    spill();
    return;
  }

  const auto currentUsage = pool_->usedBytes();
  const auto availableReservation = pool_->availableReservation();
  const auto minReservation =
      currentUsage * spillConfig_->minSpillableReservationPct / 100;

  if (availableReservation < minReservation) {
    const auto targetIncrement = std::max<int64_t>(
        estimateInputMemory(input) * 2,
        currentUsage * spillConfig_->spillableReservationGrowthPct / 100);

    memory::ReclaimableSectionGuard guard(nonReclaimableSection_);
    if (!pool_->maybeReserve(targetIncrement)) {
      spill();
    }
  }
}

void VectorPartitionStreamingWindowBuild::ensureOutputFits(
    vector_size_t numOutputRows) {
  // Similar to PartitionStreamingWindowBuild.
  if (spillConfig_ == nullptr || numOutputRows == 0) {
    return;
  }

  // Test-only spill path.
  if (testingTriggerSpill(pool_->name())) {
    spill();
    return;
  }

  // Check if we need to spill to make room for output.
  const auto currentUsage = pool_->usedBytes();
  const auto availableReservation = pool_->availableReservation();

  // Estimate output size (rough estimate).
  const auto estimatedOutputSize = numOutputRows * 100; // Conservative

  if (availableReservation < estimatedOutputSize) {
    memory::ReclaimableSectionGuard guard(nonReclaimableSection_);
    if (!pool_->maybeReserve(estimatedOutputSize)) {
      // Try to spill the active partition.
      if (currentPartition_ >= 0 &&
          currentPartition_ <
              static_cast<vector_size_t>(partitionStates_.size())) {
        if (auto partition =
                partitionStates_[currentPartition_].windowPartition.lock()) {
          partition->spill();
        }
      }
    }
  }
}

std::optional<common::SpillStats>
VectorPartitionStreamingWindowBuild::spilledStats() const {
  if (spillStats_ == nullptr) {
    return std::nullopt;
  }
  return spillStats_->copy();
}

std::shared_ptr<SpillableVectorBasedWindowPartition>
VectorPartitionStreamingWindowBuild::createInMemoryPartition(
    const PartitionState& state) {
  VELOX_CHECK(!state.spilled);

  // Pass inputChannels_ as the column mapping.
  // VectorBasedWindowPartition stores original input vectors.
  // SpillableVectorBasedWindowPartition uses this mapping:
  // - In memory mode: no mapping needed (directly access original columns)
  // - In spill mode: needs reorderedToOriginal_ to create reordered vectors,
  //   and originalToReordered_ to extract columns from cache.
  return std::make_shared<SpillableVectorBasedWindowPartition>(
      state.blocks,
      inputChannels_,
      sortKeyInfo_,
      inputType_,
      spillConfig_,
      spillStats_,
      pool_);
}

std::shared_ptr<SpillableVectorBasedWindowPartition>
VectorPartitionStreamingWindowBuild::createSpillablePartition(
    PartitionState& state) {
  VELOX_CHECK(state.spilled);

  auto spillFiles = std::move(state.spillFiles);
  state.spillFiles.clear();

  // Pass inputChannels_ as the column mapping.
  // Spill files contain reordered vectors (created by copyBlockToRowVector).
  // SpillableVectorBasedWindowPartition will use originalToReordered_ to
  // extract columns from cached data.
  return std::make_shared<SpillableVectorBasedWindowPartition>(
      std::move(spillFiles),
      state.totalRows,
      inputChannels_,
      sortKeyInfo_,
      inputType_,
      spillConfig_->readBufferSize,
      spillConfig_->windowSpillCacheMaxBytes,
      spillConfig_->windowSpillCacheMaxRows,
      pool_,
      spillStats_);
}

void VectorPartitionStreamingWindowBuild::cleanupPreviousPartition() {
  auto prevPartitionIdx = currentPartition_ - 1;
  if (prevPartitionIdx < 0 || prevPartitionIdx >= partitionStates_.size()) {
    return;
  }

  auto& prevState = partitionStates_[prevPartitionIdx];

  if (prevState.spilled) {
    // Spill mode data is managed by SpillableVectorBasedWindowPartition.
    return;
  }

  // Release previous partition's memory.
  prevState.blocks.clear();
  prevState.blocks.shrink_to_fit();
}

uint64_t VectorPartitionStreamingWindowBuild::estimateInputMemory(
    const RowVectorPtr& input) const {
  uint64_t bytes = 0;
  for (const auto& child : input->children()) {
    bytes += child->retainedSize();
  }
  return bytes;
}

} // namespace facebook::velox::exec
