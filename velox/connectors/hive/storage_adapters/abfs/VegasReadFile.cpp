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

#include "VegasReadFile.h"
#include "velox/connectors/hive/storage_adapters/abfs/vegas/v1/VegasExecV1.h"

namespace facebook::velox::filesystems::abfs {
VegasReadFile::VegasReadFile(
    const std::string& path,
    const int32_t loadQuantum,
    const std::shared_ptr<folly::Executor> ioExecutor,
    const std::string abfsEndpoint,
    const bool passEtagLength,
    std::shared_ptr<vegas::VegasCacheConfig> vegasConfig)
    : readFile_(std::make_unique<AbfsReadFile>(
          path,
          loadQuantum,
          ioExecutor,
          abfsEndpoint,
          passEtagLength)),
      vegasConfig_(vegasConfig) {}

VegasReadFile::~VegasReadFile() {
  // this would be triggered when it's removed from LRU file handle cache.
  if (readFile_) {
    vegas::VegasExecV1::cleanup(readFile_->getPath());
  }
}

void VegasReadFile::initialize() {
  readFile_->initialize();
}

std::string_view VegasReadFile::pread(
    uint64_t offset,
    uint64_t length,
    void* buffer,
    uint64_t& cacheReadBytes,
    const uint64_t splitOffset,
    const uint64_t splitLength) const {
  std::string_view result = {static_cast<char*>(buffer), length};
  auto vegasFile =
      std::make_unique<vegas::VegasExecV1>(readFile_->getPath(), vegasConfig_);
  auto vegasInitialized = vegasFile->initialize(
      readFile_->getEtag(), readFile_->size(), splitOffset, splitLength);
  if (vegasInitialized) {
    result = vegasFile->pread(
        offset,
        length,
        buffer,
        [&](uint64_t o, uint64_t l, char* p) {
          readFile_->preadInternal(o, l, p);
        },
        cacheReadBytes);
  } else {
    result = readFile_->pread(offset, length, buffer);
  }
  vegasFile->tryClose();
  return result;
}

std::string VegasReadFile::pread(
    uint64_t offset,
    uint64_t length,
    uint64_t& cacheReadBytes,
    const uint64_t splitOffset,
    const uint64_t splitLength) const {
  bool success = false;
  std::string result;
  auto vegasFile =
      std::make_unique<vegas::VegasExecV1>(readFile_->getPath(), vegasConfig_);
  auto vegasInitialized = vegasFile->initialize(
      readFile_->getEtag(), readFile_->size(), splitOffset, splitLength);
  if (vegasInitialized) {
    result = vegasFile->pread(
        offset,
        length,
        [&](uint64_t o, uint64_t l, char* p) {
          readFile_->preadInternal(o, l, p);
        },
        cacheReadBytes);
  } else {
    result = readFile_->pread(offset, length);
  }
  vegasFile->tryClose();
  return result;
}

void VegasReadFile::preadv(
    folly::Range<const common::Region*> regions,
    folly::Range<folly::IOBuf*> iobufs,
    uint64_t& cacheReadBytes,
    const uint64_t splitOffset,
    const uint64_t splitLength) const {
  auto vegasFile =
      std::make_unique<vegas::VegasExecV1>(readFile_->getPath(), vegasConfig_);
  auto vegasInitialized = vegasFile->initialize(
      readFile_->getEtag(), readFile_->size(), splitOffset, splitLength);
  if (vegasInitialized) {
    uint64_t offset = AbfsReadFile::getOffset(regions);
    std::vector<folly::Range<char*>> ranges;
    AbfsReadFile::convertRegionsToRanges(regions, iobufs, ranges);
    vegasFile->preadv(
        offset,
        ranges,
        [&](uint64_t o, uint64_t l, char* p) {
          readFile_->preadInternal(o, l, p);
        },
        [&](uint64_t o, const std::vector<folly::Range<char*>>& b) {
          return readFile_->preadv(o, b);
        },
        cacheReadBytes);
  } else {
    readFile_->preadv(regions, iobufs);
  }
  vegasFile->tryClose();
}

uint64_t VegasReadFile::preadv(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers,
    uint64_t& cacheReadBytes,
    const uint64_t splitOffset,
    const uint64_t splitLength) const {
  uint64_t result = 0;
  auto vegasFile =
      std::make_unique<vegas::VegasExecV1>(readFile_->getPath(), vegasConfig_);
  auto vegasInitialized = vegasFile->initialize(
      readFile_->getEtag(), readFile_->size(), splitOffset, splitLength);
  if (vegasInitialized) {
    result = vegasFile->preadv(
        offset,
        buffers,
        [&](uint64_t o, uint64_t l, char* p) {
          readFile_->preadInternal(o, l, p);
        },
        [&](uint64_t o, const std::vector<folly::Range<char*>>& b) {
          return readFile_->preadv(o, b);
        },
        cacheReadBytes);
  } else {
    result = readFile_->preadv(offset, buffers);
  }
  vegasFile->tryClose();
  return result;
}
} // namespace facebook::velox::filesystems::abfs
