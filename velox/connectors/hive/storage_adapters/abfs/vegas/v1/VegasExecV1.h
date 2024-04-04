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

#include "VegasJournalV1.h"
#include "velox/connectors/hive/storage_adapters/abfs/vegas/VegasExecBase.h"
#include "velox/connectors/hive/storage_adapters/abfs/vegas/client/VegasCacheConfig.h"

namespace facebook::velox::filesystems::abfs::vegas {
class VegasExecV1 final : public VegasExecBase {
 public:
  explicit VegasExecV1(
      const std::string& path,
      const std::shared_ptr<VegasCacheConfig> vegasConfig);

  ~VegasExecV1() = default;

  bool initialize(
      const std::string remoteEtag,
      const uint64_t size,
      const uint64_t splitOffset,
      const uint64_t splitLength);

  template <typename ReadFunc>
  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buf,
      ReadFunc preadInternal,
      uint64_t& cacheReadBytes) {
    std::vector<Chunk> chunks;
    uint64_t bytesRead = blockJournal_->cachedBlocks(offset, length, chunks);

    // Complete cache hit
    if (bytesRead == length) {
      for (auto& chunk : chunks) {
        blockJournal_->cacheRead(
            chunk.fileOffset_,
            chunk.length_,
            static_cast<uint8_t*>(buf) + chunk.outputOffset_);
        cacheReadBytes += chunk.length_;
      }
    } else if (bytesRead > 0) { // Partial read
      for (auto& chunk : chunks) {
        blockJournal_->cacheRead(
            chunk.fileOffset_,
            chunk.length_,
            static_cast<uint8_t*>(buf) + chunk.outputOffset_);
        cacheReadBytes += chunk.length_;
      }
      getFromABFS(
          offset + bytesRead,
          length - bytesRead,
          static_cast<char*>(buf) + bytesRead,
          preadInternal);
    } else {
      getFromABFS(offset, length, static_cast<char*>(buf), preadInternal);
    }
    return {static_cast<char*>(buf), length};
  }

  template <typename ReadFunc>
  std::string pread(
      uint64_t offset,
      uint64_t length,
      ReadFunc preadInternal,
      uint64_t& cacheReadBytes) {
    std::string result(length, 0);
    std::vector<Chunk> chunks;
    uint64_t bytesRead = blockJournal_->cachedBlocks(offset, length, chunks);

    // Complete cache hit
    if (bytesRead == length) {
      for (auto& chunk : chunks) {
        blockJournal_->cacheRead(
            chunk.fileOffset_,
            chunk.length_,
            reinterpret_cast<uint8_t*>(result.data() + chunk.outputOffset_));
        cacheReadBytes += chunk.length_;
      }
    } else if (bytesRead > 0) { // Partial read
      for (auto& chunk : chunks) {
        blockJournal_->cacheRead(
            chunk.fileOffset_,
            chunk.length_,
            reinterpret_cast<uint8_t*>(result.data() + chunk.outputOffset_));
        cacheReadBytes += chunk.length_;
      }
      getFromABFS(
          offset + bytesRead,
          length - bytesRead,
          result.data() + bytesRead,
          preadInternal);
    } else {
      getFromABFS(offset, length, result.data(), preadInternal);
    }
    return result;
  }

  template <typename ReadFunc, typename ReadVFunc>
  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers,
      ReadFunc preadInternal,
      ReadVFunc preadVInternal,
      uint64_t& cacheReadBytes) {
    std::vector<folly::Range<char*>> buffers_t;
    uint64_t offset_t = offset;
    bool vReadNeed = false;
    size_t length = 0;
    for (auto& range : buffers) {
      length += range.size();
      if (range.data()) {
        std::vector<Chunk> chunks;
        uint64_t bytesRead =
            blockJournal_->cachedBlocks(offset_t, range.size(), chunks);

        // Complete cache hit
        if (bytesRead == range.size()) {
          for (auto& chunk : chunks) {
            blockJournal_->cacheRead(
                chunk.fileOffset_,
                chunk.length_,
                reinterpret_cast<uint8_t*>(range.data() + chunk.outputOffset_));
            cacheReadBytes += chunk.length_;
          }
          buffers_t.push_back(folly::Range<char*>(nullptr, range.size()));
        } else if (bytesRead > 0) { // Partial read
          for (auto& chunk : chunks) {
            blockJournal_->cacheRead(
                chunk.fileOffset_,
                chunk.length_,
                reinterpret_cast<uint8_t*>(range.data() + chunk.outputOffset_));
            cacheReadBytes += chunk.length_;
          }
          buffers_t.push_back(folly::Range<char*>(nullptr, bytesRead));
          vReadNeed = true;
          buffers_t.push_back(folly::Range<char*>(
              reinterpret_cast<char*>(range.data() + bytesRead),
              range.size() - bytesRead));
        } else {
          vReadNeed = true;
          buffers_t.push_back(range);
        }
      } else {
        buffers_t.push_back(range);
      }

      offset_t += range.size();
    }
    if (vReadNeed) {
      getFromABFSV(offset, buffers_t, preadVInternal);
    }
    return length;
  }

  void tryClose();

  static void cleanup(const std::string& uri);

 private:
  template <typename ReadFunc>
  bool getFromABFS(
      uint64_t offset,
      uint64_t length,
      char* pos,
      ReadFunc preadInternal) {
    preadInternal(offset, length, pos);

    std::vector<uint64_t> offsetVec({offset});
    std::vector<uint64_t> lenVec({length});
    std::vector<char*> bufVec({pos});
    writeCacheAsync(offsetVec, lenVec, bufVec);
    return true;
  }

  template <typename ReadVFunc>
  bool getFromABFSV(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers,
      ReadVFunc preadVInternal) {
    preadVInternal(offset, buffers);

    std::vector<uint64_t> offsetVec;
    std::vector<uint64_t> lenVec;
    std::vector<char*> bufVec;
    auto offset_t = offset;
    for (auto& range : buffers) {
      if (range.data()) {
        offsetVec.push_back(offset_t);
        lenVec.push_back(range.size());
        bufVec.push_back(range.data());
      }
      offset_t += range.size();
    }
    writeCacheAsync(offsetVec, lenVec, bufVec);

    return true;
  }

  void writeCacheAsync(
      std::vector<uint64_t>& logicalOffset,
      std::vector<uint64_t>& lenVec,
      std::vector<char*>& bufVec);

  std::unique_ptr<VegasJournalV1> blockJournal_{nullptr};
};
} // namespace facebook::velox::filesystems::abfs::vegas