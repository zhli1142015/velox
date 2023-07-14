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

  std::string abfsEndpoint() const {
    return config_->get<std::string>(AbfsFileSystem::kAbfsEndpoint, "");
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
      const std::string abfsEndpoint)
      : path_(path), loadQuantum_(loadQuantum), ioExecutor_(ioExecutor) {
    auto abfsAccount = AbfsAccount(path_, abfsEndpoint);
    fileName_ = abfsAccount.filePath();
    fileClient_ = BlobClientProviderFactory::getBlobClient(path, abfsAccount);
  }

  void initialize(const FileOptions& options) {
    if (options.fileSize.has_value()) {
      VELOX_CHECK_GE(
          options.fileSize.value(), 0, "File size must be non-negative");
      length_ = options.fileSize.value();
    }

    if (length_ != -1) {
      return;
    }

    try {
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
      throwStorageExceptionWithOperationDetails("GetProperties", fileName_, e);
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
  const std::string path_;
  const int32_t loadQuantum_;
  const std::shared_ptr<folly::Executor> ioExecutor_;
  std::string fileName_;
  std::string eTag_;
  std::shared_ptr<BlobClient> fileClient_;

  int64_t length_ = -1;
};

AbfsReadFile::AbfsReadFile(
    const std::string& path,
    const int32_t loadQuantum,
    const std::shared_ptr<folly::Executor> ioExecutor,
    const std::string abfsEndpoint) {
  impl_ = std::make_shared<Impl>(path, loadQuantum, ioExecutor, abfsEndpoint);
}

void AbfsReadFile::initialize(const FileOptions& options) {
  return impl_->initialize(options);
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
              << ", ABFS endpoint " << abfsConfig_.abfsEndpoint();
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

  const std::string getAbfsEndpoint() const {
    return abfsConfig_.abfsEndpoint();
  }

 private:
  const AbfsConfig abfsConfig_;
  std::shared_ptr<folly::Executor> ioExecutor_;
};

AbfsFileSystem::AbfsFileSystem(const std::shared_ptr<const Config>& config)
    : FileSystem(config) {
  impl_ = std::make_shared<Impl>(config.get());
}

std::string AbfsFileSystem::name() const {
  return "ABFS";
}

std::unique_ptr<ReadFile> AbfsFileSystem::openFileForRead(
    std::string_view path,
    const FileOptions& options) {
  auto abfsfile = std::make_unique<AbfsReadFile>(
      std::string(path),
      impl_->getLoadQuantum(),
      impl_->getIOExecutor(),
      impl_->getAbfsEndpoint());
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
