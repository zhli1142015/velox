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

#include "velox/core/Config.h"

namespace facebook::velox::filesystems::abfs::vegas {
class VegasCacheConfig {
 public:
  virtual ~VegasCacheConfig() = default;

  explicit VegasCacheConfig(const Config* config);

  int64_t getVfsAccumulationBufferSizeBytes() const;
  int64_t getVfsFlushThresholdBytes() const;
  int64_t getVfsAccumulateReadThresholdBytes() const;
  int64_t getVfsCatalogPublishTimeoutMs() const;
  bool getVfsPublishCatalogEntry() const;
  bool getVfsLimitTotalWriteLocks() const;
  uint64_t getVfsWriteLocksLimit() const;

  std::string getVfsCacheDir() const;

  const static inline std::string VFSJournalExt = ".block.journal";
  const static inline std::string VFSLockExt = ".block.journal.lock";
  const static inline std::string VFSMapExt = ".block.map";

  const static inline std::string mapFileSignature = "BlockJournal_v101_Begin";
  const static inline std::string mapFileTerminator = "BlockJournal_v101_End";

  const static inline std::string CATALOG_ITEM_ID = "$ID";
  const static inline std::string CATALOG_ACCESS_COUNT = "AccessCount";
  const static inline std::string CATALOG_FILE_LENGTH = "FileLength";
  const static inline std::string CATALOG_JOURNAL_LENGTH = "JournalLength";
  const static inline std::string CATALOG_MAP_FILE_LENGTH = "MapFileLength";
  const static inline std::string CATALOG_FULL_FILE_LENGTH = "FullFileLength";
  const static inline std::string CATALOG_ITEM_TYPE = "ItemType";
  const static inline std::string CATALOG_ITEM_TYPE_VFS = "VFS";
  const static inline std::string CATALOG_ITEM_LOCAL_PATH = "LocalPath";
  const static inline std::string CATALOG_CACHE_HIT_BYTES = "CacheHitBytes";
  const static inline std::string CATALOG_CACHE_MISS_BYTES = "CacheMissBytes";
  const static inline std::string CATALOG_ITEM_BLOCKS = "BlockCount";
  const static inline std::string CATALOG_IS_SEQUENTIAL = "IsSequential";
  const static inline std::string CATALOG_UPDATE_TIME = "LastUpdate";
  const static inline std::string CATALOG_AZURE_URL = "AzureUrl";
  const static inline std::string CATALOG_PARTITION_OFFSET = "Partition.Offset";
  const static inline std::string CATALOG_PARTITION_LENGTH = "Partition.Length";
  const static inline std::string CATALOG_HOST = "Host";
  const static inline std::string CATALOG_IPV4 = "IpV4";
  const static inline std::string CATALOG_BLOCK_MAP = "BlockMap";

  const static inline int UX_SOCKET_VERSION_102 = 102;
  const static inline int UX_SOCKET_OPCODE_PUBLISH_CATALOG_ITEM = 1003;
  const static inline int UX_SOCKET_RESPONSE_SUCCESS = 1006;

  static constexpr const char* kVfsCacheDir = "Vfs.CacheDir";
  static constexpr const char* kVfsAccumulationBufferSizeBytes =
      "Vfs.AccumulationBufferSizeBytes";
  static constexpr const char* kVfsFlushThresholdBytes =
      "Vfs.FlushThresholdBytes";
  static constexpr const char* kVfsAccumulateReadThresholdBytes =
      "Vfs.AccumulateReadThresholdBytes";
  static constexpr const char* kVfsCatalogPublishTimeoutMs =
      "Vfs.BlockJournal.CatalogPublishTimeoutMs";
  static constexpr const char* kVfsPublishCatalogEntry =
      "Vfs.BlockJournal.PublishCatalogEntry";
  static constexpr const char* kVfsLimitTotalWriteLocks =
      "Vfs.LimitTotalWriteLocks";
  static constexpr const char* kVfsWriteLocksLimit = "Vfs.WriteLocksLimit";
  std::string getVfsCachePath(const std::string& path) const;

 private:
  const Config* FOLLY_NONNULL config_;

  std::string vfsCacheDir_ = "/mnt2/vegas/vfs";

  int64_t vfsAccumulationBufferSizeBytes_ = 4096;
};
} // namespace facebook::velox::filesystems::abfs::vegas
