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

#include "velox/connectors/hive/storage_adapters/abfs/AbfsFileSystem.h"

#include <azure/storage/blobs/blob_client.hpp>
#include <fmt/format.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <glog/logging.h>

#include "velox/common/file/File.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsReadFile.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsUtil.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsWriteFile.h"
#include "velox/connectors/hive/storage_adapters/abfs/VegasReadFile.h"
#include "velox/core/Config.h"

namespace facebook::velox::filesystems::abfs {
using namespace Azure::Storage::Blobs;

class AbfsConfig {
 public:
  AbfsConfig(const Config* config) : config_(config) {}

  std::string connectionString(const std::string& path) const {
    auto abfsAccount = AbfsAccount(path);
    auto key = abfsAccount.credKey();
    VELOX_USER_CHECK(
        config_->isValueExists(key),
        "Failed to find storage connectionStr credentials");

    return abfsAccount.connectionString(config_->get(key).value());
  }

  int32_t loadQuantum() const {
    return config_->get<int32_t>(
        AbfsFileSystem::kReaderAbfsLoadQuantum, AbfsReadFile::kNaturalReadSize);
  }

  int32_t ioThreadPoolSize() const {
    return config_->get<int32_t>(AbfsFileSystem::kReaderAbfsIoThreads, 0);
  }

  bool isVegasEnabled() const {
    return config_->get<bool>(AbfsFileSystem::kVegasEnabled, false);
  }

  int32_t vegasCacheSize() const {
    return config_->get<int32_t>(AbfsFileSystem::kVegasCacheSize, 0);
  }

  std::string abfsEndpoint() const {
    return config_->get<std::string>(AbfsFileSystem::kAbfsEndpoint, "");
  }

  bool passEtagLength() const {
    return config_->get<bool>(AbfsFileSystem::kPassEtagLength, true);
  }

 private:
  const Config* config_;
};

class AbfsReadFile::Impl {
 public:
  explicit Impl(
      const std::string& path,
      const int32_t loadQuantum,
      const std::shared_ptr<folly::Executor> ioExecutor,
      const std::string& abfsEndpoint,
      bool passEtagLength)
      : loadQuantum_(loadQuantum),
        ioExecutor_(ioExecutor),
        passEtagLength_(passEtagLength) {
    auto abfsAccount = AbfsAccount(path, passEtagLength, abfsEndpoint);
    fileName_ = abfsAccount.filePath();
    eTag_ = abfsAccount.etag();
    path_ = abfsAccount.url();
    length_ = abfsAccount.length();
    fileClient_ = BlobClientProviderFactory::getBlobClient(path_, abfsAccount);
  }

  void initialize() {
    if (eTag_.empty() || length_ == -1) {
      try {
        VLOG(1) << "Fetching properties from remote file system for " << path_;
        auto properties = fileClient_->GetProperties();
        length_ = properties.Value.BlobSize;
        auto eTagFull = properties.Value.ETag.ToString();
        if (eTagFull.length() > 2) {
          // Remove Quotes
          eTag_ = eTagFull.substr(1, eTagFull.length() - 2);
        } else {
          eTag_ = "invalid";
        }
      } catch (Azure::Storage::StorageException& e) {
        throwStorageExceptionWithOperationDetails(
            "GetProperties", fileName_, e);
      }
    }
    VELOX_CHECK_GE(length_, 0);
  }

  std::string_view pread(uint64_t offset, uint64_t length, void* buffer) const {
    preadInternal(offset, length, static_cast<char*>(buffer));
    return {static_cast<char*>(buffer), length};
  }

  std::string pread(uint64_t offset, uint64_t length) const {
    std::string result(length, 0);
    preadInternal(offset, length, result.data());
    return result;
  }

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const {
    size_t length = 0;
    auto size = buffers.size();
    for (auto& range : buffers) {
      length += range.size();
    }
    if (ioExecutor_) {
      size_t requestOffset = offset;
      std::vector<folly::Future<folly::Unit>> futures;
      for (auto region : buffers) {
        const auto& buffer = region.data();
        if (buffer) {
          auto loadQuantum = calculateSplitQuantum(region.size(), loadQuantum_);
          if (region.size() > loadQuantum) {
            std::vector<std::tuple<uint64_t, uint64_t>> ranges;
            splitRegion(region.size(), loadQuantum, ranges);
            for (size_t idx = 0; idx < ranges.size(); idx++) {
              auto cursor = std::get<0>(ranges[idx]);
              auto length = std::get<1>(ranges[idx]);
              auto future = folly::via(
                  ioExecutor_.get(),
                  [this, buffer, region, requestOffset, cursor, length]() {
                    char* b = reinterpret_cast<char*>(buffer);
                    preadInternal(requestOffset + cursor, length, b + cursor);
                  });
              futures.push_back(std::move(future));
            }
          } else {
            auto future = folly::via(
                ioExecutor_.get(), [this, requestOffset, buffer, region]() {
                  preadInternal(requestOffset, region.size(), buffer);
                });
            futures.push_back(std::move(future));
          }
        }
        requestOffset += region.size();
      }
      for (int64_t i = futures.size() - 1; i >= 0; --i) {
        futures[i].wait();
      }
    } else {
      std::string result(length, 0);
      preadInternal(offset, length, static_cast<char*>(result.data()));
      size_t resultOffset = 0;
      for (auto range : buffers) {
        if (range.data()) {
          memcpy(range.data(), &(result.data()[resultOffset]), range.size());
        }
        resultOffset += range.size();
      }
    }
    return length;
  }

  void preadv(
      folly::Range<const common::Region*> regions,
      folly::Range<folly::IOBuf*> iobufs) const {
    VELOX_CHECK_EQ(regions.size(), iobufs.size());
    if (ioExecutor_) {
      std::vector<folly::Future<folly::Unit>> futures;
      for (size_t i = 0; i < regions.size(); ++i) {
        const auto& region = regions[i];
        auto& output = iobufs[i];
        output = folly::IOBuf(folly::IOBuf::CREATE, region.length);
        auto loadQuantum = calculateSplitQuantum(region.length, loadQuantum_);
        if (region.length > loadQuantum) {
          std::vector<std::tuple<uint64_t, uint64_t>> ranges;
          splitRegion(region.length, loadQuantum, ranges);
          for (size_t idx = 0; idx < ranges.size(); idx++) {
            auto cursor = std::get<0>(ranges[idx]);
            auto length = std::get<1>(ranges[idx]);
            auto future = folly::via(
                ioExecutor_.get(), [this, region, &output, cursor, length]() {
                  char* b = reinterpret_cast<char*>(output.writableData());
                  preadInternal(region.offset + cursor, length, b + cursor);
                });
            futures.push_back(std::move(future));
          }
        } else {
          auto future =
              folly::via(ioExecutor_.get(), [this, region, &output]() {
                char* b = reinterpret_cast<char*>(output.writableData());
                preadInternal(region.offset, region.length, b);
              });
          futures.push_back(std::move(future));
        }
      }
      for (int64_t i = futures.size() - 1; i >= 0; --i) {
        futures[i].wait();
      }
      for (size_t i = 0; i < regions.size(); ++i) {
        iobufs[i].append(regions[i].length);
      }
    } else {
      for (size_t i = 0; i < regions.size(); ++i) {
        const auto& region = regions[i];
        auto& output = iobufs[i];
        output = folly::IOBuf(folly::IOBuf::CREATE, region.length);
        pread(region.offset, region.length, output.writableData());
        output.append(region.length);
      }
    }
  }

  uint64_t size() const {
    return length_;
  }

  uint64_t memoryUsage() const {
    return 3 * sizeof(std::string) + sizeof(int64_t);
  }

  bool shouldCoalesce() const {
    return false;
  }

  std::string getName() const {
    return fileName_;
  }

  uint64_t getNaturalReadSize() const {
    return kNaturalReadSize;
  }

  std::string getEtag() const {
    return eTag_;
  }

  std::string getPath() const {
    return path_;
  }

  void preadInternal(uint64_t offset, uint64_t length, char* position) const {
    // Read the desired range of bytes.
    Azure::Core::Http::HttpRange range;
    range.Offset = offset;
    range.Length = length;

    Azure::Storage::Blobs::DownloadBlobOptions blob;
    blob.Range = range;

    auto response = fileClient_->Download(blob);
    response.Value.BodyStream->ReadToCount(
        reinterpret_cast<uint8_t*>(position), length);
  }

 private:
  std::string path_;
  const int32_t loadQuantum_;
  const std::shared_ptr<folly::Executor> ioExecutor_;
  std::string fileName_;
  std::string eTag_;
  bool passEtagLength_;
  std::shared_ptr<BlobClient> fileClient_;

  int64_t length_ = -1;
};

AbfsReadFile::AbfsReadFile(
    const std::string& path,
    const int32_t loadQuantum,
    const std::shared_ptr<folly::Executor> ioExecutor,
    const std::string abfsEndpoint,
    const bool passEtagLength) {
  impl_ = std::make_shared<Impl>(
      path, loadQuantum, ioExecutor, abfsEndpoint, passEtagLength);
}

void AbfsReadFile::initialize() {
  return impl_->initialize();
}

std::string_view
AbfsReadFile::pread(uint64_t offset, uint64_t length, void* buffer) const {
  return impl_->pread(offset, length, buffer);
}

std::string AbfsReadFile::pread(uint64_t offset, uint64_t length) const {
  return impl_->pread(offset, length);
}

uint64_t AbfsReadFile::preadv(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers) const {
  return impl_->preadv(offset, buffers);
}

void AbfsReadFile::preadv(
    folly::Range<const common::Region*> regions,
    folly::Range<folly::IOBuf*> iobufs) const {
  return impl_->preadv(regions, iobufs);
}

uint64_t AbfsReadFile::size() const {
  return impl_->size();
}

uint64_t AbfsReadFile::memoryUsage() const {
  return impl_->memoryUsage();
}

bool AbfsReadFile::shouldCoalesce() const {
  return false;
}

std::string AbfsReadFile::getName() const {
  return impl_->getName();
}

uint64_t AbfsReadFile::getNaturalReadSize() const {
  return impl_->getNaturalReadSize();
}

std::string AbfsReadFile::getEtag() const {
  return impl_->getEtag();
}

std::string AbfsReadFile::getPath() const {
  return impl_->getPath();
}

void AbfsReadFile::preadInternal(uint64_t offset, uint64_t length, char* pos)
    const {
  return impl_->preadInternal(offset, length, pos);
}

// static
uint64_t AbfsReadFile::calculateSplitQuantum(
    const uint64_t length,
    const uint64_t loadQuantum) {
  if (length <= loadQuantum * kReadConcurrency) {
    return loadQuantum;
  } else {
    return length / kReadConcurrency;
  }
}

// static
void AbfsReadFile::splitRegion(
    const uint64_t length,
    const uint64_t loadQuantum,
    std::vector<std::tuple<uint64_t, uint64_t>>& range) {
  uint64_t cursor = 0;
  while (cursor + loadQuantum < length) {
    range.emplace_back(cursor, loadQuantum);
    cursor += loadQuantum;
  }

  if ((length - cursor) > (loadQuantum / 2)) {
    range.emplace_back(cursor, (length - cursor));
  } else {
    auto last = range.back();
    range.pop_back();
    range.emplace_back(
        std::get<0>(last), std::get<1>(last) + (length - cursor));
  }
}

// static
uint64_t AbfsReadFile::getOffset(folly::Range<const common::Region*> regions) {
  uint64_t offset = regions[0].offset;
  for (auto& r : regions) {
    if (r.offset < offset) {
      offset = r.offset;
    }
  }
  return offset;
}

// static
void AbfsReadFile::convertRegionsToRanges(
    folly::Range<const common::Region*> regions,
    folly::Range<folly::IOBuf*> iobufs,
    std::vector<folly::Range<char*>>& ranges) {
  uint64_t offset = AbfsReadFile::getOffset(regions);
  uint64_t lastEnd = offset;
  uint64_t curOffset = offset;
  for (size_t i = 0; i < regions.size(); ++i) {
    const auto& region = regions[i];
    auto& output = iobufs[i];
    // fill each buffer
    curOffset = region.offset;
    output = folly::IOBuf(folly::IOBuf::CREATE, region.length);
    const auto& buffer = output.writableData();
    if (lastEnd != curOffset) {
      ranges.push_back(folly::Range<char*>(nullptr, curOffset - lastEnd));
    }
    ranges.push_back(
        folly::Range<char*>(reinterpret_cast<char*>(buffer), region.length));
    lastEnd = curOffset + region.length;
    output.append(region.length);
  }
}

class AbfsFileSystem::Impl {
 public:
  explicit Impl(const Config* config) : abfsConfig_(config) {
    if (abfsConfig_.ioThreadPoolSize() > 0) {
      ioExecutor_ = std::make_shared<folly::IOThreadPoolExecutor>(
          abfsConfig_.ioThreadPoolSize());
    }
    LOG(INFO) << "Init Azure Blob file system"
              << ", thread pool size:"
              << std::to_string(abfsConfig_.ioThreadPoolSize())
              << ", load quantum:" << abfsConfig_.loadQuantum()
              << ", ABFS endpoint:" << abfsConfig_.abfsEndpoint()
              << ", Vegas enabled:" << abfsConfig_.isVegasEnabled()
              << ", Vegas cache size:" << abfsConfig_.vegasCacheSize()
              << ", pass ETag and length:" << abfsConfig_.passEtagLength();
  }

  ~Impl() {
    LOG(INFO) << "Dispose Azure Blob file system";
  }

  const std::string connectionString(const std::string& path) const {
    // Extract account name
    return abfsConfig_.connectionString(path);
  }

  const int32_t getLoadQuantum() const {
    return abfsConfig_.loadQuantum();
  }

  const std::shared_ptr<folly::Executor>& getIOExecutor() const {
    return ioExecutor_;
  }

  const bool isVegasEnabled() const {
    return abfsConfig_.isVegasEnabled();
  }

  const int32_t vegasCacheSize() const {
    return abfsConfig_.vegasCacheSize();
  }

  const std::string getAbfsEndpoint() const {
    return abfsConfig_.abfsEndpoint();
  }

  const bool passEtagLength() const {
    return abfsConfig_.passEtagLength();
  }

 private:
  const AbfsConfig abfsConfig_;
  std::shared_ptr<folly::Executor> ioExecutor_;
};

AbfsFileSystem::AbfsFileSystem(const std::shared_ptr<const Config>& config)
    : FileSystem(config) {
  impl_ = std::make_shared<Impl>(config.get());
  isVegasEnabled_ = impl_->isVegasEnabled() && impl_->vegasCacheSize() > 0;
  if (isVegasEnabled_) {
    vegasConfig_ = std::make_shared<vegas::VegasCacheConfig>(config.get());
  }
}

std::string AbfsFileSystem::name() const {
  return "ABFS";
}

std::unique_ptr<ReadFile> AbfsFileSystem::openFileForRead(
    std::string_view path,
    const FileOptions& /*unused*/) {
  if (isVegasEnabled_) {
    auto vegasfile = std::make_unique<VegasReadFile>(
        std::string(path),
        impl_->getLoadQuantum(),
        impl_->getIOExecutor(),
        impl_->getAbfsEndpoint(),
        impl_->passEtagLength(),
        vegasConfig_);
    vegasfile->initialize();
    return vegasfile;
  }
  auto abfsfile = std::make_unique<AbfsReadFile>(
      std::string(path),
      impl_->getLoadQuantum(),
      impl_->getIOExecutor(),
      impl_->getAbfsEndpoint(),
      impl_->passEtagLength());
  abfsfile->initialize();
  return abfsfile;
}

std::unique_ptr<WriteFile> AbfsFileSystem::openFileForWrite(
    std::string_view path,
    const FileOptions& /*unused*/) {
  auto abfsfile = std::make_unique<AbfsWriteFile>(
      std::string(path), impl_->connectionString(std::string(path)));
  abfsfile->initialize();
  return abfsfile;
}
} // namespace facebook::velox::filesystems::abfs
