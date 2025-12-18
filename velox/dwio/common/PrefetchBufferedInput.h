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

#include <unordered_map>
#include "velox/dwio/common/AsyncChunkedReader.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/InputStream.h"

namespace facebook::velox::dwio::common {

class ChunkedPageInputStream;

/// BufferedInput implementation that uses AsyncChunkedReader for IO.
class PrefetchBufferedInput : public BufferedInput {
 public:
  PrefetchBufferedInput(
      std::shared_ptr<ReadFile> readFile,
      memory::MemoryPool& pool,
      folly::Executor* executor,
      const AsyncChunkedReader::Config& chunkConfig)
      : PrefetchBufferedInput(
            std::make_shared<ReadFileInputStream>(std::move(readFile)),
            pool,
            executor,
            chunkConfig) {}

  PrefetchBufferedInput(
      std::shared_ptr<ReadFile> readFile,
      memory::MemoryPool& pool,
      folly::Executor* executor)
      : PrefetchBufferedInput(
            std::move(readFile),
            pool,
            executor,
            AsyncChunkedReader::defaultConfig()) {}

  PrefetchBufferedInput(
      std::shared_ptr<ReadFileInputStream> input,
      memory::MemoryPool& pool,
      folly::Executor* executor)
      : PrefetchBufferedInput(
            std::move(input),
            pool,
            executor,
            AsyncChunkedReader::defaultConfig()) {}

  PrefetchBufferedInput(
      std::shared_ptr<ReadFileInputStream> input,
      memory::MemoryPool& pool,
      folly::Executor* executor,
      const AsyncChunkedReader::Config& chunkConfig);

  ~PrefetchBufferedInput() override;

  std::unique_ptr<SeekableInputStream> enqueue(
      velox::common::Region region,
      const StreamIdentifier* sid = nullptr) override;

  void load(const LogType logType) override;

  bool supportSyncLoad() const override {
    return false;
  }

  bool isBuffered(uint64_t offset, uint64_t length) const override;

  std::unique_ptr<BufferedInput> clone() const override;

  folly::Executor* executor() const override {
    return executor_;
  }

  bool shouldPreload(int32_t numPages = 0) override {
    return executor_ != nullptr;
  }

  bool shouldPrefetchStripes() const override {
    return executor_ != nullptr;
  }

  AsyncChunkedReader::Stats ioStats() const {
    AsyncChunkedReader::Stats total;
    for (const auto& group : columnGroups_) {
      if (group.reader) {
        auto s = group.reader->stats();
        total.syncLoads += s.syncLoads;
        total.asyncLoads += s.asyncLoads;
        total.cacheHits += s.cacheHits;
        total.cacheMisses += s.cacheMisses;
        total.bytesRead += s.bytesRead;
      }
    }
    return total;
  }

  size_t numColumnGroups() const {
    return columnGroups_.size();
  }

  bool isLoaded() const {
    return loaded_;
  }

 private:
  struct PendingRegion {
    velox::common::Region region;
    ChunkedPageInputStream* stream;
    uint64_t columnId{0}; // From StreamIdentifier, used for column grouping.
  };

  // Group pending regions by column (non-contiguous ranges).
  // Each group gets its own AsyncChunkedReader for parallel IO.
  struct ColumnGroup {
    uint64_t baseOffset{0};
    uint64_t size{0};
    std::vector<PendingRegion> regions;
    std::shared_ptr<AsyncChunkedReader> reader;
  };

  void groupRegionsByColumn();
  ColumnGroup* findGroupForOffset(uint64_t offset);

  folly::Executor* executor_;
  AsyncChunkedReader::Config chunkConfig_;
  std::vector<PendingRegion> pendingRegions_;
  bool loaded_{false};
  // Multiple readers for cross-column parallel IO.
  std::vector<ColumnGroup> columnGroups_;
};

/// Lazy input stream that reads from AsyncChunkedReader.
class ChunkedPageInputStream : public SeekableInputStream {
 public:
  ChunkedPageInputStream(
      memory::MemoryPool& pool,
      velox::common::Region absoluteRegion);

  ~ChunkedPageInputStream() override = default;

  void connectToReader(
      std::shared_ptr<AsyncChunkedReader> reader,
      uint64_t baseOffset);

  bool Next(const void** data, int* size) override;
  void BackUp(int count) override;
  bool SkipInt64(int64_t count) override;
  google::protobuf::int64 ByteCount() const override;
  void seekToPosition(PositionProvider& position) override;
  std::string getName() const override;
  size_t positionSize() const override;

 private:
  memory::MemoryPool& pool_;
  velox::common::Region absoluteRegion_;
  velox::common::Region relativeRegion_;
  std::shared_ptr<AsyncChunkedReader> reader_;
  uint64_t position_{0};
};

} // namespace facebook::velox::dwio::common
