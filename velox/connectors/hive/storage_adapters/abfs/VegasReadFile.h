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

#include "AbfsReadFile.h"
#include "velox/common/file/File.h"
#include "velox/common/file/Region.h"
#include "velox/connectors/hive/storage_adapters/abfs/vegas/VegasExecBase.h"
#include "velox/connectors/hive/storage_adapters/abfs/vegas/client/VegasCacheConfig.h"

namespace facebook::velox::filesystems::abfs {
class VegasReadFile final : public ReadFile {
 public:
  explicit VegasReadFile(
      const std::string& path,
      const int32_t loadQuantum,
      const std::shared_ptr<folly::Executor> ioExecutor,
      const std::string abfsEndpoint,
      std::shared_ptr<vegas::VegasCacheConfig> vegasConfig);

  ~VegasReadFile();

  void initialize();

  std::string_view pread(uint64_t offset, uint64_t length, void* buf)
      const final {
    VELOX_UNSUPPORTED("pread of VegasReadFile should not be called");
  }

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buf,
      uint64_t& cacheReadBytes,
      const uint64_t splitOffset,
      const uint64_t splitLength) const final;

  std::string pread(
      uint64_t offset,
      uint64_t length,
      uint64_t& cacheReadBytes,
      const uint64_t splitOffset,
      const uint64_t splitLength) const final;

  void preadv(
      folly::Range<const common::Region*> regions,
      folly::Range<folly::IOBuf*> iobufs,
      uint64_t& cacheReadBytes,
      const uint64_t splitOffset,
      const uint64_t splitLength) const final;

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers,
      uint64_t& cacheReadBytes,
      const uint64_t splitOffset,
      const uint64_t splitLength) const final;

  uint64_t size() const final {
    return readFile_->size();
  }

  uint64_t memoryUsage() const final {
    return readFile_->memoryUsage();
  }

  bool shouldCoalesce() const final {
    return readFile_->shouldCoalesce();
  }

  std::string getName() const final {
    return readFile_->getName();
  }

  uint64_t getNaturalReadSize() const final {
    return readFile_->getNaturalReadSize();
  }

 private:
  const std::unique_ptr<AbfsReadFile> readFile_;
  const std::shared_ptr<vegas::VegasCacheConfig> vegasConfig_;
};
} // namespace facebook::velox::filesystems::abfs