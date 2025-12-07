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

#include "velox/exec/VectorBasedWindowPartition.h"
#include "velox/exec/WindowBuild.h"

namespace facebook::velox::exec {

/// VectorRowsStreamingWindowBuild is an alternative implementation of
/// RowsStreamingWindowBuild that uses VectorBasedWindowPartition instead of
/// RowContainer. This eliminates the overhead of copying data into RowContainer
/// format and provides zero-copy storage of input vectors.
///
/// This implementation is selected when:
/// 1. Input is already sorted by partition and order by keys
/// 2. All window functions support kRows processing mode
/// 3. Aggregate functions only use the default frame (UNBOUNDED PRECEDING to
/// CURRENT ROW)
class VectorRowsStreamingWindowBuild : public WindowBuild {
 public:
  VectorRowsStreamingWindowBuild(
      const std::shared_ptr<const core::WindowNode>& windowNode,
      velox::memory::MemoryPool* pool,
      const common::SpillConfig* spillConfig,
      tsan_atomic<bool>* nonReclaimableSection);

  void addInput(RowVectorPtr input) override;

  bool needsInput() override {
    // Hold at most 2 partitions: one being output, one being built
    return windowPartitions_.size() < 2;
  }

  void spill() override {
    // VectorRowsStreamingWindowBuild does not support spilling
    VELOX_UNREACHABLE();
  }

  std::optional<common::SpillStats> spilledStats() const override {
    return std::nullopt;
  }

  void noMoreInput() override;

  bool hasNextPartition() override;

  std::shared_ptr<WindowPartition> nextPartition() override;

 private:
  /// Flushes accumulated rows as a block.
  void
  flushBlock(const RowVectorPtr& input, vector_size_t start, vector_size_t end);

  /// Adds accumulated blocks to the current partition.
  void addPartitionInputs(bool finished);

  /// Ensures a partition exists for receiving input.
  void ensureInputPartition();

  /// Checks if the current row starts a new partition.
  bool isNewPartition(const RowVectorPtr& input, vector_size_t row);

  /// Checks if the current row starts a new peer group.
  bool isNewPeerGroup(const RowVectorPtr& input, vector_size_t row);

  // Blocks being accumulated for the current partition
  std::vector<RowBlock> currentBlocks_;

  // Queue of built partitions
  std::deque<std::shared_ptr<VectorBasedWindowPartition>> windowPartitions_;

  // Previous row reference (for cross-batch partition/peer group detection)
  RowReference previousRef_;

  // Number of pending rows since last flush
  vector_size_t pendingRowCount_ = 0;
};

} // namespace facebook::velox::exec
