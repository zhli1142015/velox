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

#include "velox/dwio/common/PrefetchBufferedInput.h"

#include <fmt/format.h>
#include <algorithm>

namespace facebook::velox::dwio::common {

PrefetchBufferedInput::PrefetchBufferedInput(
    std::shared_ptr<ReadFileInputStream> input,
    memory::MemoryPool& pool,
    folly::Executor* executor,
    const AsyncChunkedReader::Config& chunkConfig)
    : BufferedInput(std::move(input), pool),
      executor_(executor),
      chunkConfig_(chunkConfig) {}

PrefetchBufferedInput::~PrefetchBufferedInput() = default;

std::unique_ptr<SeekableInputStream> PrefetchBufferedInput::enqueue(
    velox::common::Region region,
    const StreamIdentifier* sid) {
  if (region.length == 0) {
    return std::make_unique<SeekableArrayInputStream>(
        static_cast<const char*>(nullptr), 0);
  }

  // Create a lazy stream for this region.
  auto stream = std::make_unique<ChunkedPageInputStream>(*pool_, region);

  // Use column id from StreamIdentifier if available, otherwise use
  // offset-based grouping.
  uint64_t columnId = sid ? sid->getColumnId() : 0;

  // Store the pending region info for later connection.
  pendingRegions_.push_back({region, stream.get(), columnId});

  return stream;
}

void PrefetchBufferedInput::load(const LogType /*logType*/) {
  if (loaded_ || pendingRegions_.empty()) {
    loaded_ = true;
    return;
  }

  // Group regions by column for cross-column parallel IO.
  groupRegionsByColumn();

  // Create AsyncChunkedReader for each column group in parallel.
  for (auto& group : columnGroups_) {
    // Collect relative regions for this group.
    std::vector<velox::common::Region> relativeRegions;
    relativeRegions.reserve(group.regions.size());
    for (const auto& pending : group.regions) {
      relativeRegions.push_back(
          {pending.region.offset - group.baseOffset,
           pending.region.length,
           pending.region.label});
    }

    // Create AsyncChunkedReader for this column group.
    group.reader = std::make_shared<AsyncChunkedReader>(
        input_->getReadFile(),
        *pool_,
        executor_,
        group.baseOffset,
        group.size,
        chunkConfig_);

    // Set read regions for optimized chunk computation.
    group.reader->setReadRegions(std::move(relativeRegions));

    // Connect streams in this group to the reader.
    for (const auto& pending : group.regions) {
      if (pending.stream) {
        pending.stream->connectToReader(group.reader, group.baseOffset);
      }
    }
  }

  // Trigger initial prefetch for ALL column groups in parallel.
  // This is where cross-column parallel IO happens - all groups submit
  // their first prefetch requests to the shared executor simultaneously.
  for (auto& group : columnGroups_) {
    group.reader->triggerPrefetch(0);
  }

  loaded_ = true;
  pendingRegions_.clear();
}

void PrefetchBufferedInput::groupRegionsByColumn() {
  if (pendingRegions_.empty()) {
    return;
  }

  // Group regions by columnId for cross-column parallel IO.
  // Each unique columnId gets its own AsyncChunkedReader.
  std::unordered_map<uint64_t, std::vector<PendingRegion*>> columnMap;
  for (auto& pending : pendingRegions_) {
    columnMap[pending.columnId].push_back(&pending);
  }

  columnGroups_.clear();
  columnGroups_.reserve(columnMap.size());

  for (auto& [columnId, regions] : columnMap) {
    // Sort regions within column by offset.
    std::sort(regions.begin(), regions.end(), [](auto* a, auto* b) {
      return a->region.offset < b->region.offset;
    });

    // Create a group for this column.
    ColumnGroup group;
    group.baseOffset = regions.front()->region.offset;
    uint64_t maxEnd = 0;
    for (auto* pending : regions) {
      maxEnd =
          std::max(maxEnd, pending->region.offset + pending->region.length);
      group.regions.push_back(*pending);
    }
    group.size = maxEnd - group.baseOffset;
    columnGroups_.push_back(std::move(group));
  }
}

bool PrefetchBufferedInput::isBuffered(uint64_t offset, uint64_t length) const {
  auto* group =
      const_cast<PrefetchBufferedInput*>(this)->findGroupForOffset(offset);
  if (!group || !group->reader) {
    return false;
  }
  if (offset < group->baseOffset) {
    return false;
  }
  return group->reader->isLoaded(offset - group->baseOffset, length);
}

PrefetchBufferedInput::ColumnGroup* PrefetchBufferedInput::findGroupForOffset(
    uint64_t offset) {
  for (auto& group : columnGroups_) {
    if (offset >= group.baseOffset && offset < group.baseOffset + group.size) {
      return &group;
    }
  }
  return nullptr;
}

std::unique_ptr<BufferedInput> PrefetchBufferedInput::clone() const {
  // Create a fresh instance with same config but no pending regions.
  return std::make_unique<PrefetchBufferedInput>(
      input_, *pool_, executor_, chunkConfig_);
}

// ChunkedPageInputStream implementation.

ChunkedPageInputStream::ChunkedPageInputStream(
    memory::MemoryPool& pool,
    velox::common::Region absoluteRegion)
    : pool_(pool), absoluteRegion_(absoluteRegion), relativeRegion_({0, 0}) {}

void ChunkedPageInputStream::connectToReader(
    std::shared_ptr<AsyncChunkedReader> reader,
    uint64_t baseOffset) {
  reader_ = std::move(reader);
  relativeRegion_ = {
      absoluteRegion_.offset - baseOffset,
      absoluteRegion_.length,
      absoluteRegion_.label};
}

bool ChunkedPageInputStream::Next(const void** data, int* size) {
  if (position_ >= absoluteRegion_.length) {
    return false; // EOF.
  }

  // Ensure we're connected to a reader.
  VELOX_CHECK_NOT_NULL(
      reader_,
      "ChunkedPageInputStream::Next called before load(). "
      "Ensure PrefetchBufferedInput::load() is called after enqueue().");

  // relativeRegion_.offset is relative to the base offset.
  uint64_t offsetInRange = relativeRegion_.offset + position_;
  uint64_t remaining = absoluteRegion_.length - position_;

  // Ensure data is loaded and get pointer.
  const uint8_t* ptr = reader_->ensureLoaded(offsetInRange, remaining);

  // Calculate how much contiguous data is available from current position.
  uint64_t bytesAvailable = reader_->availableContiguous(offsetInRange);
  bytesAvailable = std::min(bytesAvailable, remaining);

  *data = ptr;
  *size = static_cast<int>(bytesAvailable);

  position_ += bytesAvailable;

  // Update cursor after consuming data to trigger prefetch.
  reader_->updateCursor(offsetInRange + bytesAvailable);

  return true;
}

void ChunkedPageInputStream::BackUp(int count) {
  VELOX_CHECK_GE(position_, static_cast<uint64_t>(count));
  position_ -= count;
}

bool ChunkedPageInputStream::SkipInt64(int64_t count) {
  if (count < 0 ||
      static_cast<uint64_t>(count) > absoluteRegion_.length - position_) {
    return false;
  }
  position_ += count;
  return true;
}

google::protobuf::int64 ChunkedPageInputStream::ByteCount() const {
  return static_cast<google::protobuf::int64>(position_);
}

void ChunkedPageInputStream::seekToPosition(PositionProvider& position) {
  position_ = position.next();
}

std::string ChunkedPageInputStream::getName() const {
  return fmt::format(
      "ChunkedPageInputStream(offset={}, length={})",
      absoluteRegion_.offset,
      absoluteRegion_.length);
}

size_t ChunkedPageInputStream::positionSize() const {
  return 1;
}

} // namespace facebook::velox::dwio::common
