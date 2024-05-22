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

#include <common/file/FileSystems.h>
#include <fstream>
#include <string>
#include <vector>
#include "velox/connectors/hive/storage_adapters/abfs/vegas/client/VegasCacheClient.h"
#include "velox/connectors/hive/storage_adapters/abfs/vegas/client/VegasCacheConfig.h"

namespace facebook::velox::filesystems::abfs::vegas {
class VegasJournalBase {
 public:
  ~VegasJournalBase() = default;

  explicit VegasJournalBase(
      const std::shared_ptr<vegas::VegasCacheConfig> vegasConfig,
      const std::string& uri,
      const std::string& path);

  inline std::string journalFileName() const {
    return path_ + VegasCacheConfig::VFSJournalExt;
  }

  inline std::string uri() const {
    return uri_;
  }

 protected:
  std::string eTagRemote_;
  std::string eTagFromCache_ = "";
  uint64_t fileLengthRemote_ = -1;
  uint64_t totalFileOpens_ = 0;
  bool isSequentiallyComplete_ = false;

  uint64_t splitOffset_ = 0;
  uint64_t splitLength_ = 0;

  const std::string uri_;
  const std::string path_;
  const std::shared_ptr<vegas::VegasCacheConfig> vegasConfig_;
  const std::unique_ptr<vegas::VegasCacheClient> vegasClient_;

  // TODO: Statistics
  int64_t cacheMisses_ = 0;
  int64_t accesses_ = 0;
  int64_t cachePartialHits_ = 0;
  int64_t cachePartialHitBytes_ = 0;
  int64_t cacheHits_ = 0;
  int64_t cacheHitBytes_ = 0;
  int64_t cacheMissBytes_ = 0;
  int64_t openTime_ = 0;

  // inline
  inline std::string mapFileName() const {
    return path_ + VegasCacheConfig::VFSMapExt;
  }

  // static
  static bool readString(
      const uint8_t* buffer,
      uint32_t& offset,
      uint64_t szMax,
      std::string& result);
  static uint8_t readOneByte(const uint8_t* buffer, uint32_t offset);
  static uint16_t readTwoBytes(const uint8_t* buffer, uint32_t offset);
  static uint32_t readFourBytes(const uint8_t* buffer, uint32_t offset);
  static uint64_t readEightBytes(const uint8_t* buffer, uint32_t offset);
  static void saveOneByteInMap(std::ofstream& mapFile, uint8_t n);
  static void saveTwoBytesInMap(std::ofstream& mapFile, uint16_t n);
  static void saveFourBytesInMap(std::ofstream& mapFile, uint32_t n);
  static void saveEightBytesInMap(std::ofstream& mapFile, uint64_t n);
};
} // namespace facebook::velox::filesystems::abfs::vegas
