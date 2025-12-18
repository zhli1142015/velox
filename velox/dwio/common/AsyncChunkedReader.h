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

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

#include <folly/Executor.h>
#include <folly/futures/SharedPromise.h>

#include "velox/buffer/Buffer.h"
#include "velox/common/file/File.h"
#include "velox/common/file/Region.h"
#include "velox/common/memory/Memory.h"

namespace facebook::velox::dwio::common {

/// Async chunk-based file reader with cursor-based prefetching.
class AsyncChunkedReader {
 public:
  struct Config {
    uint64_t chunkSize{4 * 1024 * 1024};
    int32_t prefetchCount{2};
    int32_t maxCachedChunks{8};
    double prefetchThreshold{0.75};
  };

  struct AdaptiveConfig {
    bool enabled{true};
    int32_t evaluationWindow{4};
    int32_t minPrefetchCount{1};
    int32_t maxPrefetchCount{8};
    double minPrefetchThreshold{0.5};
    double maxPrefetchThreshold{0.9};
  };

  static Config defaultConfig() {
    return Config{};
  }

  AsyncChunkedReader(
      std::shared_ptr<ReadFile> readFile,
      memory::MemoryPool& pool,
      folly::Executor* executor,
      uint64_t columnChunkOffset,
      uint64_t columnChunkSize)
      : AsyncChunkedReader(
            std::move(readFile),
            pool,
            executor,
            columnChunkOffset,
            columnChunkSize,
            defaultConfig()) {}

  AsyncChunkedReader(
      std::shared_ptr<ReadFile> readFile,
      memory::MemoryPool& pool,
      folly::Executor* executor,
      uint64_t columnChunkOffset,
      uint64_t columnChunkSize,
      const Config& config);

  ~AsyncChunkedReader();

  void setReadRegions(std::vector<velox::common::Region> regions);

  /// Ensures data at [offset, offset+length) is loaded and returns pointer.
  /// Blocks if data is not yet available. Triggers prefetch as needed.
  const uint8_t* ensureLoaded(uint64_t offset, uint64_t length);

  void updateCursor(uint64_t cursor);

  bool isLoaded(uint64_t offset, uint64_t length) const;

  uint64_t availableContiguous(uint64_t offset) const;

  void triggerPrefetch(size_t fromIndex);

  struct Stats {
    int64_t syncLoads{0};
    int64_t asyncLoads{0};
    int64_t cacheHits{0};
    int64_t cacheMisses{0};
    int64_t bytesRead{0};
  };

  Stats stats() const {
    Stats snapshot;
    snapshot.syncLoads = stats_.syncLoads.load();
    snapshot.asyncLoads = stats_.asyncLoads.load();
    snapshot.cacheHits = stats_.cacheHits.load();
    snapshot.cacheMisses = stats_.cacheMisses.load();
    snapshot.bytesRead = stats_.bytesRead.load();
    return snapshot;
  }

  uint64_t columnChunkSize() const {
    return columnChunkSize_;
  }

 private:
  struct AtomicStats {
    std::atomic<int64_t> syncLoads{0};
    std::atomic<int64_t> asyncLoads{0};
    std::atomic<int64_t> cacheHits{0};
    std::atomic<int64_t> cacheMisses{0};
    std::atomic<int64_t> bytesRead{0};
  };

  enum class ChunkState { kNotLoaded, kLoading, kLoaded };

  struct Chunk {
    uint64_t offset{0};
    uint64_t length{0};
    std::vector<velox::common::Region> regions;
    BufferPtr data;
    std::atomic<ChunkState> state{ChunkState::kNotLoaded};
    std::shared_ptr<folly::SharedPromise<folly::Unit>> promise;
    std::atomic<uint64_t> lastAccessTime{0};
  };

  struct WindowStats {
    int32_t chunksProcessed{0};
    int32_t hitCount{0};
    int32_t waitCount{0};
    int32_t missCount{0};
    int64_t totalWaitNanos{0};

    void reset() {
      chunksProcessed = 0;
      hitCount = 0;
      waitCount = 0;
      missCount = 0;
      totalWaitNanos = 0;
    }
  };

  void computeChunks();
  void computeFixedSizeChunks();
  void computeRegionBasedChunks();
  size_t findChunkIndex(uint64_t offset) const;
  Chunk* getChunk(size_t chunkIndex);
  void executeChunkLoad(Chunk* chunk);
  void loadChunk(size_t chunkIndex, bool async);
  void maybeTriggerPrefetch();
  void maybeEvictChunks();
  uint64_t getLoadedDataBoundary() const;
  void recordAccess(ChunkState stateOnAccess, int64_t waitNanos);
  void adaptPrefetchStrategy();

  std::shared_ptr<ReadFile> readFile_;
  memory::MemoryPool& pool_;
  folly::Executor* executor_;
  Config config_;
  AdaptiveConfig adaptiveConfig_;
  uint64_t columnChunkOffset_;
  uint64_t columnChunkSize_;
  uint64_t cursor_{0};
  std::vector<velox::common::Region> readRegions_;
  bool hasReadRegions_{false};
  bool chunksComputed_{false};
  mutable std::mutex mutex_;
  std::vector<std::unique_ptr<Chunk>> chunks_;
  std::atomic<uint64_t> accessCounter_{0};
  AtomicStats stats_;
  WindowStats windowStats_;
};

} // namespace facebook::velox::dwio::common
