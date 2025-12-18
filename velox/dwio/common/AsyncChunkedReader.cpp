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

#include "velox/dwio/common/AsyncChunkedReader.h"

#include <folly/ExceptionWrapper.h>

#include <algorithm>
#include <chrono>

namespace facebook::velox::dwio::common {

AsyncChunkedReader::AsyncChunkedReader(
    std::shared_ptr<ReadFile> readFile,
    memory::MemoryPool& pool,
    folly::Executor* executor,
    uint64_t columnChunkOffset,
    uint64_t columnChunkSize,
    const Config& config)
    : readFile_(std::move(readFile)),
      pool_(pool),
      executor_(executor),
      config_(config),
      columnChunkOffset_(columnChunkOffset),
      columnChunkSize_(columnChunkSize) {}

AsyncChunkedReader::~AsyncChunkedReader() {
  // Wait for all pending loads.
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto& chunk : chunks_) {
    if (chunk && chunk->state.load() == ChunkState::kLoading) {
      chunk->promise->getFuture().wait();
    }
  }
}

void AsyncChunkedReader::setReadRegions(
    std::vector<velox::common::Region> regions) {
  std::lock_guard<std::mutex> lock(mutex_);
  readRegions_ = std::move(regions);
  hasReadRegions_ = !readRegions_.empty();
  chunksComputed_ = false;
}

void AsyncChunkedReader::computeChunks() {
  if (chunksComputed_) {
    return;
  }

  chunks_.clear();

  if (!hasReadRegions_) {
    computeFixedSizeChunks();
  } else {
    computeRegionBasedChunks();
  }

  chunksComputed_ = true;
}

void AsyncChunkedReader::computeFixedSizeChunks() {
  // No read regions: fixed-size chunks covering entire Column Chunk.
  for (uint64_t offset = 0; offset < columnChunkSize_;
       offset += config_.chunkSize) {
    auto chunk = std::make_unique<Chunk>();
    chunk->offset = offset;
    chunk->length = std::min(config_.chunkSize, columnChunkSize_ - offset);
    chunk->regions.push_back({offset, chunk->length});
    chunk->promise = std::make_shared<folly::SharedPromise<folly::Unit>>();
    chunks_.push_back(std::move(chunk));
  }
}

void AsyncChunkedReader::computeRegionBasedChunks() {
  // With read regions: merge regions into chunks until >= chunkSize.
  uint64_t currentSize = 0;
  std::vector<velox::common::Region> currentRegions;
  uint64_t firstOffset = 0;

  auto flushChunk = [&]() {
    if (currentRegions.empty()) {
      return;
    }

    auto chunk = std::make_unique<Chunk>();
    chunk->offset = firstOffset;
    chunk->length = currentSize;
    chunk->regions = std::move(currentRegions);
    chunk->promise = std::make_shared<folly::SharedPromise<folly::Unit>>();
    chunks_.push_back(std::move(chunk));

    currentRegions.clear();
    currentSize = 0;
  };

  for (const auto& region : readRegions_) {
    if (currentRegions.empty()) {
      firstOffset = region.offset;
    }

    // Check if this region is contiguous with the last one.
    if (!currentRegions.empty()) {
      auto& lastRegion = currentRegions.back();
      if (lastRegion.offset + lastRegion.length == region.offset) {
        // Contiguous: merge into last region.
        lastRegion.length += region.length;
      } else {
        // Non-contiguous: add as new region.
        currentRegions.push_back(region);
      }
    } else {
      currentRegions.push_back(region);
    }
    currentSize += region.length;

    if (currentSize >= config_.chunkSize) {
      flushChunk();
    }
  }
  flushChunk();
}

size_t AsyncChunkedReader::findChunkIndex(uint64_t offset) const {
  // Binary search for chunk containing offset.
  if (chunks_.empty()) {
    return 0;
  }

  auto it = std::upper_bound(
      chunks_.begin(),
      chunks_.end(),
      offset,
      [](uint64_t off, const std::unique_ptr<Chunk>& chunk) {
        return off < chunk->offset;
      });

  if (it == chunks_.begin()) {
    return 0;
  }
  return std::distance(chunks_.begin(), it) - 1;
}

const uint8_t* AsyncChunkedReader::ensureLoaded(
    uint64_t offset,
    uint64_t length) {
  VELOX_CHECK_LE(
      offset + length, columnChunkSize_, "Read beyond Column Chunk size");

  // Compute chunks if not done yet.
  {
    std::lock_guard<std::mutex> lock(mutex_);
    computeChunks();
  }

  size_t chunkIndex = findChunkIndex(offset);
  Chunk* chunk = getChunk(chunkIndex);

  // Record state before potentially blocking.
  ChunkState stateOnAccess = chunk->state.load(std::memory_order_acquire);
  int64_t waitNanos = 0;

  switch (stateOnAccess) {
    case ChunkState::kLoaded:
      stats_.cacheHits++;
      break;

    case ChunkState::kLoading: {
      // Wait for prefetch to complete.
      stats_.cacheMisses++;
      auto startWait = std::chrono::steady_clock::now();
      chunk->promise->getFuture().wait();
      waitNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
                      std::chrono::steady_clock::now() - startWait)
                      .count();
      break;
    }

    case ChunkState::kNotLoaded:
      // Load synchronously.
      stats_.cacheMisses++;
      stats_.syncLoads++;
      loadChunk(chunkIndex, /*async=*/false);
      break;
  }

  // Record access for adaptive tuning.
  recordAccess(stateOnAccess, waitNanos);

  // Update access time for LRU.
  chunk->lastAccessTime.store(accessCounter_++, std::memory_order_relaxed);

  VELOX_CHECK_LE(
      chunk->offset,
      offset,
      "Chunk offset {} is greater than requested offset {}",
      chunk->offset,
      offset);
  uint64_t offsetInChunk = offset - chunk->offset;
  return reinterpret_cast<const uint8_t*>(chunk->data->as<char>()) +
      offsetInChunk;
}

void AsyncChunkedReader::updateCursor(uint64_t cursor) {
  cursor_ = cursor;
  maybeTriggerPrefetch();
}

bool AsyncChunkedReader::isLoaded(uint64_t offset, uint64_t length) const {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!chunksComputed_ || chunks_.empty()) {
    return false;
  }

  size_t chunkIndex = findChunkIndex(offset);
  if (chunkIndex >= chunks_.size()) {
    return false;
  }

  const auto& chunk = chunks_[chunkIndex];
  return chunk->state.load() == ChunkState::kLoaded &&
      chunk->offset <= offset &&
      chunk->offset + chunk->length >= offset + length;
}

uint64_t AsyncChunkedReader::availableContiguous(uint64_t offset) const {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!chunksComputed_ || chunks_.empty()) {
    return 0;
  }

  size_t chunkIndex = findChunkIndex(offset);
  uint64_t available = 0;

  // Sum up contiguous loaded chunks from offset.
  while (chunkIndex < chunks_.size()) {
    const auto& chunk = chunks_[chunkIndex];
    if (chunk->state.load() != ChunkState::kLoaded) {
      break;
    }
    if (chunkIndex == findChunkIndex(offset)) {
      // First chunk: only count from offset to end.
      available += (chunk->offset + chunk->length) - offset;
    } else {
      // Check if contiguous with previous chunk.
      if (chunkIndex > 0) {
        const auto& prevChunk = chunks_[chunkIndex - 1];
        if (prevChunk->offset + prevChunk->length != chunk->offset) {
          break;
        }
      }
      available += chunk->length;
    }
    ++chunkIndex;
  }

  return available;
}

AsyncChunkedReader::Chunk* AsyncChunkedReader::getChunk(size_t chunkIndex) {
  std::lock_guard<std::mutex> lock(mutex_);

  VELOX_CHECK_LT(chunkIndex, chunks_.size(), "Invalid chunk index");
  auto& chunk = chunks_[chunkIndex];

  // Allocate buffer if not done yet.
  if (!chunk->data) {
    maybeEvictChunks();
    chunk->data = AlignedBuffer::allocate<char>(chunk->length, &pool_);
  }

  return chunk.get();
}

void AsyncChunkedReader::executeChunkLoad(Chunk* chunk) {
  try {
    uint64_t absoluteOffset = columnChunkOffset_ + chunk->offset;
    readFile_->pread(
        absoluteOffset, chunk->length, chunk->data->asMutable<char>());
    stats_.bytesRead += chunk->length;
    chunk->state.store(ChunkState::kLoaded, std::memory_order_release);
    chunk->promise->setValue(folly::Unit{});
  } catch (const std::exception& e) {
    chunk->state.store(ChunkState::kNotLoaded, std::memory_order_release);
    chunk->promise->setException(folly::exception_wrapper(e));
  }
}

void AsyncChunkedReader::loadChunk(size_t chunkIndex, bool async) {
  Chunk* chunk = nullptr;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    chunk = chunks_[chunkIndex].get();

    ChunkState expected = ChunkState::kNotLoaded;
    if (!chunk->state.compare_exchange_strong(expected, ChunkState::kLoading)) {
      return; // Already loading or loaded.
    }
  }

  if (async && executor_) {
    stats_.asyncLoads++;
    executor_->add([this, chunk]() { executeChunkLoad(chunk); });
  } else {
    executeChunkLoad(chunk);
  }
}

void AsyncChunkedReader::triggerPrefetch(size_t fromIndex) {
  if (!executor_) {
    return;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  computeChunks();

  size_t endIndex = std::min(
      chunks_.size(), fromIndex + static_cast<size_t>(config_.prefetchCount));

  for (size_t i = fromIndex; i < endIndex; ++i) {
    auto* chunk = chunks_[i].get();

    // Ensure buffer is allocated.
    if (!chunk->data) {
      maybeEvictChunks();
      chunk->data = AlignedBuffer::allocate<char>(chunk->length, &pool_);
    }

    ChunkState expected = ChunkState::kNotLoaded;
    if (chunk->state.compare_exchange_strong(expected, ChunkState::kLoading)) {
      stats_.asyncLoads++;
      executor_->add([this, chunk]() { executeChunkLoad(chunk); });
    }
  }
}

uint64_t AsyncChunkedReader::getLoadedDataBoundary() const {
  uint64_t boundary = 0;
  for (const auto& chunk : chunks_) {
    if (chunk->state.load() == ChunkState::kLoaded) {
      boundary = std::max(boundary, chunk->offset + chunk->length);
    }
  }
  return boundary;
}

void AsyncChunkedReader::maybeTriggerPrefetch() {
  if (!executor_) {
    return;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  computeChunks();

  if (chunks_.empty()) {
    return;
  }

  // Find the chunk containing current cursor.
  size_t currentChunkIndex = findChunkIndex(cursor_);
  if (currentChunkIndex >= chunks_.size()) {
    return;
  }

  // Calculate how much loaded data remains after cursor.
  uint64_t loadedBoundary = getLoadedDataBoundary();
  if (loadedBoundary <= cursor_) {
    // No loaded data after cursor, need to load current chunk.
    // Release lock to call triggerPrefetch.
    mutex_.unlock();
    triggerPrefetch(currentChunkIndex);
    mutex_.lock();
    return;
  }

  uint64_t remainingLoaded = loadedBoundary - cursor_;

  // Trigger prefetch when remaining loaded data is below threshold.
  uint64_t prefetchTriggerPoint = static_cast<uint64_t>(
      config_.chunkSize * (1.0 - config_.prefetchThreshold));

  if (remainingLoaded < prefetchTriggerPoint) {
    // Find next unloaded chunk after current loaded boundary.
    size_t nextUnloadedChunk = findChunkIndex(loadedBoundary);
    if (nextUnloadedChunk < chunks_.size() &&
        chunks_[nextUnloadedChunk]->state.load() == ChunkState::kLoaded) {
      nextUnloadedChunk++;
    }
    if (nextUnloadedChunk < chunks_.size()) {
      // Release lock to call triggerPrefetch.
      mutex_.unlock();
      triggerPrefetch(nextUnloadedChunk);
      mutex_.lock();
    }
  }
}

void AsyncChunkedReader::maybeEvictChunks() {
  // Already holding mutex_.
  // Count allocated chunks.
  size_t allocatedCount = 0;
  for (const auto& chunk : chunks_) {
    if (chunk->data) {
      allocatedCount++;
    }
  }

  if (allocatedCount < static_cast<size_t>(config_.maxCachedChunks)) {
    return;
  }

  // Find LRU chunk that is LOADED (not currently being accessed).
  size_t lruIndex = SIZE_MAX;
  uint64_t lruTime = UINT64_MAX;

  for (size_t i = 0; i < chunks_.size(); ++i) {
    const auto& chunk = chunks_[i];
    if (chunk->data && chunk->state.load() == ChunkState::kLoaded) {
      uint64_t accessTime = chunk->lastAccessTime.load();
      if (accessTime < lruTime) {
        lruTime = accessTime;
        lruIndex = i;
      }
    }
  }

  if (lruIndex != SIZE_MAX) {
    // Release allocation but keep chunk structure.
    chunks_[lruIndex]->data.reset();
    chunks_[lruIndex]->state.store(ChunkState::kNotLoaded);
    chunks_[lruIndex]->promise =
        std::make_shared<folly::SharedPromise<folly::Unit>>();
  }
}

void AsyncChunkedReader::recordAccess(
    ChunkState stateOnAccess,
    int64_t waitNanos) {
  if (!adaptiveConfig_.enabled) {
    return;
  }

  windowStats_.chunksProcessed++;
  switch (stateOnAccess) {
    case ChunkState::kLoaded:
      windowStats_.hitCount++;
      break;
    case ChunkState::kLoading:
      windowStats_.waitCount++;
      windowStats_.totalWaitNanos += waitNanos;
      break;
    case ChunkState::kNotLoaded:
      windowStats_.missCount++;
      break;
  }

  if (windowStats_.chunksProcessed >= adaptiveConfig_.evaluationWindow) {
    adaptPrefetchStrategy();
    windowStats_.reset();
  }
}

void AsyncChunkedReader::adaptPrefetchStrategy() {
  if (windowStats_.chunksProcessed == 0) {
    return;
  }

  double hitRate =
      static_cast<double>(windowStats_.hitCount) / windowStats_.chunksProcessed;
  double missRate = static_cast<double>(windowStats_.missCount) /
      windowStats_.chunksProcessed;

  // Strategy 1: Adjust prefetch count.
  if (missRate > 0.1) {
    // >10% sync loads: prefetch not aggressive enough.
    config_.prefetchCount =
        std::min(config_.prefetchCount + 1, adaptiveConfig_.maxPrefetchCount);
  } else if (
      hitRate > 0.95 &&
      config_.prefetchCount > adaptiveConfig_.minPrefetchCount) {
    // >95% hit rate and can reduce: save memory.
    config_.prefetchCount--;
  }

  // Strategy 2: Adjust trigger threshold.
  if (windowStats_.waitCount > 0 && windowStats_.totalWaitNanos > 0) {
    // Had to wait: trigger prefetch earlier.
    config_.prefetchThreshold = std::min(
        config_.prefetchThreshold + 0.05, adaptiveConfig_.maxPrefetchThreshold);
  } else if (
      hitRate > 0.9 &&
      config_.prefetchThreshold > adaptiveConfig_.minPrefetchThreshold) {
    // Almost no waiting: can delay trigger slightly.
    config_.prefetchThreshold = std::max(
        config_.prefetchThreshold - 0.02, adaptiveConfig_.minPrefetchThreshold);
  }
}

} // namespace facebook::velox::dwio::common
