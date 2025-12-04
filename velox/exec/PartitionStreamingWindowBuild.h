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

#include "velox/exec/SpillableWindowPartition.h"
#include "velox/exec/Spiller.h"
#include "velox/exec/WindowBuild.h"

namespace facebook::velox::exec {

/// A WindowBuild implementation for pre-sorted input.
///
/// This class processes input data that is already sorted by partition keys
/// and order keys. It detects partition boundaries during addInput() and
/// emits partitions as they become complete.
///
/// Spill support:
/// - spillInputRowsCompletely(): Spills all inputRows_ for memory reclaim.
/// - spillCompletedPartitions(): Spills waiting partitions stored in
///   sortedRows_.
/// - spill(): Called by memory reclaim to spill everything possible.
///
/// Each partition can independently be in memory mode or spill mode.
class PartitionStreamingWindowBuild : public WindowBuild {
 public:
  PartitionStreamingWindowBuild(
      const std::shared_ptr<const core::WindowNode>& windowNode,
      velox::memory::MemoryPool* pool,
      const common::SpillConfig* spillConfig,
      tsan_atomic<bool>* nonReclaimableSection,
      folly::Synchronized<common::SpillStats>* spillStats);

  void addInput(RowVectorPtr input) override;

  void spill() override;

  bool canSpillAfterNoMoreInput() const override {
    return canSpill();
  }

  void ensureOutputFits(vector_size_t numOutputRows) override;

  std::optional<common::SpillStats> spilledStats() const override;

  void noMoreInput() override;

  bool hasNextPartition() override;

  std::shared_ptr<WindowPartition> nextPartition() override;

  bool needsInput() override;

 private:
  /// Tracks the state of a single partition.
  struct PartitionState {
    /// True if this partition's data has been spilled to disk.
    bool spilled{false};

    /// Number of rows in this partition.
    vector_size_t numRows{0};

    /// Offset into sortedRows_ for memory mode partitions.
    vector_size_t startRow{0};

    /// Spill files containing this partition's data when spilled.
    SpillFiles spillFiles;

    /// Weak reference to the WindowPartition, used to spill active partitions.
    std::weak_ptr<SpillableWindowPartition> windowPartition;
  };

  // Finalizes the current partition and prepares for the next one.
  void buildNextPartition();

  // Checks if there is enough memory for the input and triggers spill if not.
  void ensureInputFits(const RowVectorPtr& input);

  // Updates the cached estimated output row size from data_.
  void updateEstimatedOutputRowSize();

  // Spills all inputRows_ for memory reclaim.
  void spillInputRowsCompletely();

  // Spills completed partitions that are waiting to be processed.
  void spillCompletedPartitions();

  // Creates a memory-mode WindowPartition from the given state.
  std::shared_ptr<SpillableWindowPartition> createInMemoryPartition(
      const PartitionState& state);

  // Creates a spill-mode WindowPartition from the given state.
  std::shared_ptr<SpillableWindowPartition> createSpillablePartition(
      PartitionState& state);

  // Cleans up resources from the previously processed partition.
  void cleanupPreviousPartition();

  // Spills a range of rows and returns the resulting spill files.
  // This is the core spill operation used by all spill methods.
  SpillFiles spillRowsToFiles(folly::Range<char**> rows);

  // Clears data_ while preserving previousRow_ for partition boundary
  // detection. This saves previousRow_ before clear and restores it after.
  void clearDataPreservingPreviousRow();

  // Row pointers for completed partitions in memory mode. Indexed by
  // PartitionState.startRow.
  std::vector<char*> sortedRows_;

  // Row pointers for the partition currently being built. Moved to
  // sortedRows_ when a partition boundary is detected.
  std::vector<char*> inputRows_;

  // State for each partition.
  // Layout: [processed | active | waiting | building]
  //   - processed: partitions that have been fully processed
  //   - active: the partition currently being processed by Window
  //   - waiting: completed partitions waiting to be processed
  //   - building: the partition currently receiving input (last element)
  std::vector<PartitionState> partitionStates_;

  // The last row added, used for partition boundary detection.
  char* previousRow_ = nullptr;

  // Index of the current partition being processed. -1 indicates not started.
  vector_size_t currentPartition_ = -1;

  // True if the next row starts a new partition.
  bool isNewPartitionStart_{true};

  // Spiller instance, created on demand when spilling is triggered.
  std::unique_ptr<SortOutputSpiller> spiller_;

  // Shared statistics for spill operations.
  folly::Synchronized<common::SpillStats>* spillStats_;

  // Cached estimate of output row size. Used to estimate memory needs even
  // after data_ has been cleared by spill.
  std::optional<uint64_t> estimatedOutputRowSize_;
};

} // namespace facebook::velox::exec
