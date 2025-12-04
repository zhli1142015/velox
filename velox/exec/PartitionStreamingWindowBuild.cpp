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

#include "velox/exec/PartitionStreamingWindowBuild.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/exec/MemoryReclaimer.h"
#include "velox/exec/SpillFile.h"
#include "velox/exec/SpillableWindowPartition.h"

namespace facebook::velox::exec {

PartitionStreamingWindowBuild::PartitionStreamingWindowBuild(
    const std::shared_ptr<const core::WindowNode>& windowNode,
    velox::memory::MemoryPool* pool,
    const common::SpillConfig* spillConfig,
    tsan_atomic<bool>* nonReclaimableSection,
    folly::Synchronized<common::SpillStats>* spillStats)
    : WindowBuild(windowNode, pool, spillConfig, nonReclaimableSection),
      spillStats_(spillStats) {
  partitionStates_.emplace_back();
}

void PartitionStreamingWindowBuild::buildNextPartition() {
  auto& state = partitionStates_.back();

  // Nothing to build if no rows.
  if (inputRows_.empty()) {
    // In spill mode with no new rows, still need to start a new partition
    // if the current one has data (numRows > 0).
    if (!state.spilled || state.numRows == 0) {
      return;
    }
  }

  if (state.spilled) {
    // Spill mode: write remaining inputRows_ to spill files.
    if (!inputRows_.empty()) {
      auto files = spillRowsToFiles(
          folly::Range<char**>(inputRows_.data(), inputRows_.size()));
      state.spillFiles.insert(
          state.spillFiles.end(), files.begin(), files.end());
      state.numRows += inputRows_.size();
      inputRows_.clear();
      data_->clear();
      // Release unused memory reservation after clearing data.
      data_->pool()->release();
    }
  } else {
    // Memory mode: move rows to sortedRows_.
    state.numRows = inputRows_.size();
    state.startRow = sortedRows_.size();
    sortedRows_.insert(sortedRows_.end(), inputRows_.begin(), inputRows_.end());
    inputRows_.clear();
  }

  // Start a new partition.
  partitionStates_.emplace_back();
  previousRow_ = nullptr;
  isNewPartitionStart_ = true;
}

void PartitionStreamingWindowBuild::addInput(RowVectorPtr input) {
  ensureInputFits(input);

  for (auto i = 0; i < inputChannels_.size(); ++i) {
    decodedInputVectors_[i].decode(*input->childAt(inputChannels_[i]));
  }

  for (auto row = 0; row < input->size(); ++row) {
    char* newRow = data_->newRow();

    for (auto col = 0; col < input->childrenSize(); ++col) {
      data_->store(decodedInputVectors_[col], row, newRow, col);
    }

    // Detect partition boundary.
    bool shouldBuildNext = !isNewPartitionStart_ && previousRow_ != nullptr &&
        compareRowsWithKeys(previousRow_, newRow, partitionKeyInfo_);

    if (shouldBuildNext) {
      buildNextPartition();
    }

    inputRows_.push_back(newRow);
    previousRow_ = newRow;
    isNewPartitionStart_ = false;
  }
}

void PartitionStreamingWindowBuild::updateEstimatedOutputRowSize() {
  const auto optionalRowSize = data_->estimateRowSize();
  if (!optionalRowSize.has_value() || optionalRowSize.value() == 0) {
    return;
  }

  const auto rowSize = optionalRowSize.value();
  if (!estimatedOutputRowSize_.has_value()) {
    estimatedOutputRowSize_ = rowSize;
  } else if (rowSize > estimatedOutputRowSize_.value()) {
    estimatedOutputRowSize_ = rowSize;
  }
}

void PartitionStreamingWindowBuild::ensureInputFits(const RowVectorPtr& input) {
  if (spillConfig_ == nullptr) {
    return;
  }

  // Update estimated row size for ensureOutputFits() before potential spill.
  updateEstimatedOutputRowSize();

  if (data_->numRows() == 0) {
    // Nothing to spill.
    return;
  }

  // Test-only spill path.
  if (testingTriggerSpill(data_->pool()->name())) {
    spill();
    return;
  }

  // Memory-based spill trigger logic similar to SortWindowBuild.
  auto [freeRows, outOfLineFreeBytes] = data_->freeSpace();
  const auto outOfLineBytes =
      data_->stringAllocator().retainedSize() - outOfLineFreeBytes;
  const auto outOfLineBytesPerRow =
      data_->numRows() > 0 ? outOfLineBytes / data_->numRows() : 0;

  const auto currentUsage = data_->pool()->usedBytes();
  const auto minReservationBytes =
      currentUsage * spillConfig_->minSpillableReservationPct / 100;
  const auto availableReservationBytes = data_->pool()->availableReservation();
  const auto incrementBytes =
      data_->sizeIncrement(input->size(), outOfLineBytesPerRow * input->size());

  // Check if sufficient memory reservation exists.
  if (availableReservationBytes >= minReservationBytes) {
    if ((freeRows > input->size()) &&
        (outOfLineBytes == 0 ||
         outOfLineFreeBytes >= outOfLineBytesPerRow * input->size())) {
      // Enough free rows and variable length space for input.
      return;
    }
  }

  // Check if we can increase reservation. The increment is the larger of twice
  // the maximum increment from this input and a percentage of current usage.
  const auto targetIncrementBytes = std::max<int64_t>(
      incrementBytes * 2,
      currentUsage * spillConfig_->spillableReservationGrowthPct / 100);
  {
    memory::ReclaimableSectionGuard guard(nonReclaimableSection_);
    if (data_->pool()->maybeReserve(targetIncrementBytes)) {
      return;
    }
  }

  LOG(WARNING) << "Failed to reserve " << succinctBytes(targetIncrementBytes)
               << " for memory pool " << data_->pool()->name()
               << ", usage: " << succinctBytes(data_->pool()->usedBytes())
               << ", reservation: "
               << succinctBytes(data_->pool()->reservedBytes());
}

void PartitionStreamingWindowBuild::spillInputRowsCompletely() {
  if (inputRows_.empty()) {
    return;
  }

  auto& state = partitionStates_.back();
  auto rowsToSpill = inputRows_.size();

  auto files = spillRowsToFiles(
      folly::Range<char**>(inputRows_.data(), inputRows_.size()));
  state.spillFiles.insert(state.spillFiles.end(), files.begin(), files.end());

  inputRows_.clear();
  clearDataPreservingPreviousRow();

  if (!state.spilled) {
    state.spilled = true;
    state.numRows = rowsToSpill;
  } else {
    state.numRows += rowsToSpill;
  }
}

SpillFiles PartitionStreamingWindowBuild::spillRowsToFiles(
    folly::Range<char**> rows) {
  VELOX_CHECK_NULL(spiller_);
  spiller_ = std::make_unique<SortOutputSpiller>(
      data_.get(), inputType_, spillConfig_, spillStats_);

  SpillerBase::SpillRows spillRows(
      rows.begin(),
      rows.end(),
      SpillerBase::SpillRows::allocator_type(*memory::spillMemoryPool()));
  spiller_->spill(spillRows);

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

void PartitionStreamingWindowBuild::clearDataPreservingPreviousRow() {
  if (previousRow_ == nullptr) {
    data_->clear();
    // Release unused memory reservation after clearing data.
    data_->pool()->release();
    return;
  }

  // Serialize previousRow_ before clearing.
  auto serialized =
      BaseVector::create<FlatVector<StringView>>(VARBINARY(), 1, data_->pool());
  data_->extractSerializedRows(
      folly::Range<char**>(&previousRow_, 1), serialized);
  auto value = serialized->valueAt(0);
  std::string savedRow(value.data(), value.size());

  data_->clear();
  // Release unused memory reservation after clearing data.
  // This must be done before restoring previousRow_ to ensure the memory
  // for the restored row is not released.
  data_->pool()->release();

  // Restore previousRow_ after clearing.
  serialized->set(0, StringView(savedRow));
  char* newRow = data_->newRow();
  data_->storeSerializedRow(*serialized, 0, newRow);
  previousRow_ = newRow;
}

void PartitionStreamingWindowBuild::noMoreInput() {
  buildNextPartition();
}

void PartitionStreamingWindowBuild::spill() {
  // Mark all previously processed partitions as spilled.
  for (auto i = 0; i < currentPartition_; ++i) {
    partitionStates_[i].spilled = true;
  }

  // Spill the active partition if it exists and is in memory.
  if (currentPartition_ >= 0 &&
      currentPartition_ < static_cast<vector_size_t>(partitionStates_.size()) &&
      !partitionStates_[currentPartition_].spilled) {
    if (auto partition =
            partitionStates_[currentPartition_].windowPartition.lock()) {
      partition->spill();
    }
    partitionStates_[currentPartition_].spilled = true;
  }

  // Spill completed but unprocessed in-memory partitions.
  spillCompletedPartitions();

  // Clear all in-memory data structures. After spill, all data is on disk.
  sortedRows_.clear();

  if (!inputRows_.empty()) {
    spillInputRowsCompletely();
  } else {
    clearDataPreservingPreviousRow();
  }
}

void PartitionStreamingWindowBuild::spillCompletedPartitions() {
  // Partition layout: [processed | active | waiting | building]
  //                    0..curr-1   curr    curr+1..n-2   n-1
  // Only spill waiting in-memory partitions [curr+1..n-2].
  const auto firstWaiting =
      std::max(0, static_cast<int>(currentPartition_) + 1);
  const auto lastCompleted = static_cast<int>(partitionStates_.size()) - 2;

  for (auto i = firstWaiting; i <= lastCompleted; ++i) {
    auto& state = partitionStates_[i];
    if (!state.spilled) {
      if (state.numRows > 0) {
        auto files = spillRowsToFiles(
            folly::Range<char**>(
                sortedRows_.data() + state.startRow, state.numRows));
        state.spillFiles.insert(
            state.spillFiles.end(), files.begin(), files.end());
      }
      state.spilled = true;
    }
  }
}

std::optional<common::SpillStats> PartitionStreamingWindowBuild::spilledStats()
    const {
  auto stats = spillStats_->rlock();
  if (stats->spilledBytes == 0) {
    return std::nullopt;
  }
  return *stats;
}

void PartitionStreamingWindowBuild::ensureOutputFits(
    vector_size_t numOutputRows) {
  if (spillConfig_ == nullptr || numOutputRows == 0) {
    return;
  }

  // Test-only spill path.
  if (testingTriggerSpill(data_->pool()->name())) {
    spill();
    return;
  }

  // Estimate the memory needed for output.
  uint64_t rowSize;
  if (estimatedOutputRowSize_.has_value()) {
    rowSize = estimatedOutputRowSize_.value();
  } else {
    const auto currentRowSize = data_->estimateRowSize();
    if (!currentRowSize.has_value() || currentRowSize.value() == 0) {
      // No row size available - cannot estimate output memory needs.
      return;
    }
    rowSize = currentRowSize.value();
  }

  const uint64_t outputBufferSizeToReserve = rowSize * numOutputRows * 1.2;
  {
    memory::ReclaimableSectionGuard guard(nonReclaimableSection_);
    if (data_->pool()->maybeReserve(outputBufferSizeToReserve)) {
      return;
    }
  }

  LOG(WARNING) << "Failed to reserve "
               << succinctBytes(outputBufferSizeToReserve)
               << " for memory pool " << data_->pool()->name()
               << ", usage: " << succinctBytes(data_->pool()->usedBytes())
               << ", reservation: "
               << succinctBytes(data_->pool()->reservedBytes())
               << ". Triggering spill for output.";
}

bool PartitionStreamingWindowBuild::needsInput() {
  return partitionStates_.size() <= 1 ||
      currentPartition_ ==
      static_cast<vector_size_t>(partitionStates_.size()) - 2;
}

bool PartitionStreamingWindowBuild::hasNextPartition() {
  // partitionStates_[size-1] is always the partition being built.
  return currentPartition_ <
      static_cast<vector_size_t>(partitionStates_.size()) - 2;
}

std::shared_ptr<WindowPartition>
PartitionStreamingWindowBuild::nextPartition() {
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

void PartitionStreamingWindowBuild::cleanupPreviousPartition() {
  auto prevPartitionIdx = currentPartition_ - 1;
  auto& prevState = partitionStates_[prevPartitionIdx];

  if (prevState.spilled) {
    // Spill mode data is managed by SpillableWindowPartition.
    return;
  }

  // If sortedRows_ is empty, spill() has already cleared it. The previous
  // partition's data was freed when data_->clear() was called during spill.
  // No cleanup needed in this case.
  if (sortedRows_.empty()) {
    return;
  }

  auto rowsToRemove = prevState.numRows;
  if (rowsToRemove == 0) {
    return;
  }

  // Validate that we have enough rows to remove.
  VELOX_CHECK_LE(
      rowsToRemove,
      sortedRows_.size(),
      "rowsToRemove ({}) > sortedRows_.size() ({})",
      rowsToRemove,
      sortedRows_.size());

  data_->eraseRows(folly::Range<char**>(sortedRows_.data(), rowsToRemove));
  sortedRows_.erase(sortedRows_.begin(), sortedRows_.begin() + rowsToRemove);
  sortedRows_.shrink_to_fit();

  // Adjust startRow for subsequent in-memory partitions.
  for (size_t i = currentPartition_; i < partitionStates_.size(); ++i) {
    if (!partitionStates_[i].spilled) {
      partitionStates_[i].startRow -= rowsToRemove;
    }
  }
}

std::shared_ptr<SpillableWindowPartition>
PartitionStreamingWindowBuild::createInMemoryPartition(
    const PartitionState& state) {
  VELOX_CHECK(!state.spilled);
  return std::make_shared<SpillableWindowPartition>(
      data_.get(),
      folly::Range(sortedRows_.data() + state.startRow, state.numRows),
      inversedInputChannels_,
      sortKeyInfo_,
      inputType_,
      spillConfig_,
      spillStats_);
}

std::shared_ptr<SpillableWindowPartition>
PartitionStreamingWindowBuild::createSpillablePartition(PartitionState& state) {
  VELOX_CHECK(state.spilled);
  auto spillFiles = std::move(state.spillFiles);
  state.spillFiles.clear();
  return std::make_shared<SpillableWindowPartition>(
      data_.get(),
      std::move(spillFiles),
      state.numRows,
      inversedInputChannels_,
      sortKeyInfo_,
      spillConfig_->readBufferSize,
      spillConfig_->windowSpillCacheMaxBytes,
      spillConfig_->windowSpillCacheMaxRows,
      spillStats_);
}

} // namespace facebook::velox::exec
