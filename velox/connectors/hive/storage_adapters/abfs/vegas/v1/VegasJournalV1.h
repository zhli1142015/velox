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

#include "folly/concurrency/ConcurrentHashMap.h"
#include "velox/connectors/hive/storage_adapters/abfs/vegas/VegasJournalBase.h"

namespace facebook::velox::filesystems::abfs::vegas {

struct BlockDef {
  uint64_t logicalOffset_;
  uint64_t length_;
  uint64_t fileOffset_;
  explicit BlockDef(
      uint64_t logicalOffset,
      uint64_t length,
      uint64_t fileOffset)
      : logicalOffset_(logicalOffset),
        length_(length),
        fileOffset_(fileOffset) {}
};

struct Chunk : public BlockDef {
  bool isCached_ = false;
  uint64_t outputOffset_ = 0;

  explicit Chunk(
      uint64_t logicalOffset,
      uint64_t length,
      uint64_t fileOffset,
      bool isCached,
      uint64_t outputOffset)
      : BlockDef(logicalOffset, length, fileOffset),
        isCached_(isCached),
        outputOffset_(outputOffset) {}
};

class VegasJournalV1 : public VegasJournalBase {
 public:
  ~VegasJournalV1() = default;

  explicit VegasJournalV1(
      const std::shared_ptr<vegas::VegasCacheConfig> vegasConfig,
      const std::string& uri,
      const std::string& path);

  void cacheRead(uint64_t offset, uint64_t length, uint8_t* pos) const;

  uint64_t cachedBlocks(
      const uint64_t startOffset,
      const uint64_t length,
      std::vector<Chunk>& chunks);
  uint64_t
  appendBlock(uint64_t logicalOffset, uint64_t length, uint64_t fileOffset);
  bool initialize(
      const std::string& eTag,
      const uint64_t fileLength,
      const uint64_t splitOffset,
      const uint64_t splitLength);
  void tryClose();
  bool getWriteLock();
  uint64_t cacheWrite(std::vector<uint64_t>& lenVec, std::vector<char*>& bufVec)
      const;
  uint64_t saveMap();

  static void cleanup(const std::string& uri);

 private:
  bool loadMap();
  uint64_t getTotalFileOpens() const;
  bool verifyMap();
  void computeSequentialness();
  std::pair<uint64_t, uint64_t> locateUsableContent(
      const BlockDef& block,
      const uint64_t& startOffset,
      const uint64_t& length);
  bool publishCatalog(uint64_t fileLengthMap, uint64_t fileLengthJournal);

  uint64_t fileLengthMap_ = 0;
  uint64_t fileLengthJournal_ = 0;

  bool isDirty_ = false;
  FILE* fpLock_ = nullptr;
  std::vector<BlockDef> blocks_;

  static bool kShouldCleanup;
  static uint64_t kWriteLocksLimit;

  static folly::ConcurrentHashMap<std::string, uint64_t> kWriteLocks;
  static std::atomic_uint64_t kWriteLocksCount;
};
} // namespace facebook::velox::filesystems::abfs::vegas
