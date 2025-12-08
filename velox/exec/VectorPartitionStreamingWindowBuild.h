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

#include "velox/exec/SpillableVectorBasedWindowPartition.h"
#include "velox/exec/Spiller.h"
#include "velox/exec/WindowBuild.h"

namespace facebook::velox::exec {

/// A WindowBuild implementation for pre-sorted input using Vector-based
/// storage.
///
/// This class is the Vector-based equivalent of PartitionStreamingWindowBuild.
/// It processes input data that is already sorted by partition keys and order
/// keys, detecting partition boundaries during addInput() and emitting
/// partitions as they become complete.
///
/// Key differences from PartitionStreamingWindowBuild:
/// - Uses VectorBasedWindowPartition instead of RowContainer for storage
/// - Uses NoRowContainerSpiller instead of SortOutputSpiller for spilling
/// - Zero-copy storage of input vectors (no decode + store per row)
///
/// Spill support:
/// - spillInputBlocksCompletely(): Spills all currentBlocks_ for memory
/// reclaim.
/// - spillCompletedPartitions(): Spills waiting partitions.
/// - spill(): Called by memory reclaim to spill everything possible.
class VectorPartitionStreamingWindowBuild : public WindowBuild {
 public:
  VectorPartitionStreamingWindowBuild(
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
    /// Blocks for this partition (memory mode).
    std::vector<RowBlock> blocks;

    /// Total number of rows in this partition.
    vector_size_t totalRows{0};

    /// True if this partition's data has been spilled to disk.
    bool spilled{false};

    /// Spill files containing this partition's data when spilled.
    SpillFiles spillFiles;

    /// Weak reference to the WindowPartition, used to spill active partitions.
    std::weak_ptr<SpillableVectorBasedWindowPartition> windowPartition;
  };

  /// Checks if the current row starts a new partition.
  bool isNewPartition(const RowVectorPtr& input, vector_size_t row);

  /// Finalizes the current partition and prepares for the next one.
  void buildNextPartition();

  /// Checks if there is enough memory for the input and triggers spill if not.
  void ensureInputFits(const RowVectorPtr& input);

  /// Spills all currentBlocks_ for memory reclaim.
  void spillInputBlocksCompletely();

  /// Spills completed partitions that are waiting to be processed.
  void spillCompletedPartitions();

  /// Spills a single partition's blocks to files.
  void spillPartitionBlocks(PartitionState& state);

  /// Spills the current building blocks.
  void spillCurrentBlocks();

  /// Spills blocks to files and returns the file list.
  SpillFiles spillBlocksToFiles(const std::vector<RowBlock>& blocks);

  /// Preserves previousRef_ for partition boundary detection after spill.
  void preservePreviousRef();

  /// Deep copies a RowBlock to a standalone RowVector.
  RowVectorPtr copyBlockToRowVector(const RowBlock& block);

  /// Extracts a single row for previousRef_ preservation.
  RowVectorPtr extractSingleRow(const RowReference& ref);

  /// Creates a memory-mode WindowPartition from the given state.
  std::shared_ptr<SpillableVectorBasedWindowPartition> createInMemoryPartition(
      const PartitionState& state);

  /// Creates a spill-mode WindowPartition from the given state.
  std::shared_ptr<SpillableVectorBasedWindowPartition> createSpillablePartition(
      PartitionState& state);

  /// Cleans up resources from the previously processed partition.
  void cleanupPreviousPartition();

  /// Ensures the spiller is initialized.
  void ensureSpiller();

  /// Estimates memory usage of an input vector.
  uint64_t estimateInputMemory(const RowVectorPtr& input) const;

  /// Blocks being accumulated for the current partition.
  std::vector<RowBlock> currentBlocks_;

  /// State for each partition.
  /// Layout: [processed | active | waiting | building]
  std::vector<PartitionState> partitionStates_;

  /// Previous row reference for partition boundary detection.
  RowReference previousRef_;

  /// Preserved previous row after spill (deep copied).
  RowVectorPtr preservedPreviousRow_;

  /// Index of the current partition being processed. -1 indicates not started.
  vector_size_t currentPartition_{static_cast<vector_size_t>(-1)};

  /// True if the next row starts a new partition.
  bool isNewPartitionStart_{true};

  /// Spiller instance, created on demand when spilling is triggered.
  std::unique_ptr<NoRowContainerSpiller> spiller_;

  /// Shared statistics for spill operations.
  folly::Synchronized<common::SpillStats>* spillStats_;

  /// Memory pool for allocations.
  memory::MemoryPool* pool_;

  /// Original input type (before reordering). Used for extractSingleRow to
  /// preserve the original column order for partition key comparison.
  RowTypePtr originalInputType_;
};

} // namespace facebook::velox::exec
