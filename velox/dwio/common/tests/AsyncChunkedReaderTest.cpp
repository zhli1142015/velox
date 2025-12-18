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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <folly/executors/CPUThreadPoolExecutor.h>

#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/AsyncChunkedReader.h"

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;
using facebook::velox::common::Region;
using namespace ::testing;

namespace {

// Mock ReadFile for testing.
class MockReadFile : public ReadFile {
 public:
  explicit MockReadFile(std::string_view content) : content_(content) {}

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buf,
      const FileStorageContext& /* unused */) const override {
    EXPECT_LE(offset + length, content_.size());
    memcpy(buf, content_.data() + offset, length);
    preadCount_.fetch_add(1);
    return {content_.data() + offset, length};
  }

  uint64_t size() const override {
    return content_.size();
  }

  uint64_t memoryUsage() const override {
    return 0;
  }

  std::string getName() const override {
    return "MockReadFile";
  }

  uint64_t getNaturalReadSize() const override {
    return 4 * 1024 * 1024; // 4MB
  }

  bool shouldCoalesce() const override {
    return false;
  }

  int preadCount() const {
    return preadCount_.load();
  }

 private:
  std::string content_;
  mutable std::atomic<int> preadCount_{0};
};

class AsyncChunkedReaderTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool();
    executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(4);
  }

  void TearDown() override {
    executor_->join();
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  std::unique_ptr<folly::CPUThreadPoolExecutor> executor_;
};

} // namespace

TEST_F(AsyncChunkedReaderTest, basicRead) {
  std::string content(16 * 1024 * 1024, 'A');
  for (size_t i = 0; i < content.size(); ++i) {
    content[i] = static_cast<char>('A' + (i % 26));
  }
  auto readFile = std::make_shared<MockReadFile>(content);

  AsyncChunkedReader::Config config;
  config.chunkSize = 4 * 1024 * 1024;
  config.prefetchCount = 2;
  config.maxCachedChunks = 4;

  AsyncChunkedReader reader(
      readFile, *pool_, executor_.get(), 0, content.size(), config);

  const uint8_t* data = reader.ensureLoaded(0, 1024 * 1024);
  ASSERT_NE(data, nullptr);
  EXPECT_EQ(
      std::string(reinterpret_cast<const char*>(data), 26),
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ");

  data = reader.ensureLoaded(4 * 1024 * 1024, 1024);
  ASSERT_NE(data, nullptr);

  auto stats = reader.stats();
  EXPECT_GT(stats.bytesRead, 0);
}

TEST_F(AsyncChunkedReaderTest, prefetchTrigger) {
  std::string content(32 * 1024 * 1024, 'X');
  auto readFile = std::make_shared<MockReadFile>(content);

  AsyncChunkedReader::Config config;
  config.chunkSize = 4 * 1024 * 1024;
  config.prefetchCount = 2;
  config.maxCachedChunks = 8;
  config.prefetchThreshold = 0.75;

  AsyncChunkedReader reader(
      readFile, *pool_, executor_.get(), 0, content.size(), config);

  const uint8_t* data = reader.ensureLoaded(0, 1024);
  ASSERT_NE(data, nullptr);

  reader.updateCursor(3 * 1024 * 1024);

  // Poll for prefetch completion.
  bool prefetched = false;
  for (int i = 0; i < 50 && !prefetched; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    prefetched = reader.isLoaded(4 * 1024 * 1024, 1024);
  }

  auto stats = reader.stats();
  EXPECT_GT(stats.bytesRead, 0);
}

TEST_F(AsyncChunkedReaderTest, pageAlignedChunkComputation) {
  std::string content(5 * 1024 * 1024, 'P');
  auto readFile = std::make_shared<MockReadFile>(content);

  AsyncChunkedReader::Config config;
  config.chunkSize = 4 * 1024 * 1024;
  config.prefetchCount = 1;

  AsyncChunkedReader reader(
      readFile, *pool_, executor_.get(), 0, content.size(), config);

  std::vector<Region> regions;
  for (int i = 0; i < 5; ++i) {
    regions.push_back(
        {static_cast<uint64_t>(i * 1024 * 1024), 1024 * 1024, ""});
  }
  reader.setReadRegions(regions);

  const uint8_t* data = reader.ensureLoaded(0, 1024);
  ASSERT_NE(data, nullptr);

  EXPECT_TRUE(reader.isLoaded(0, 1024 * 1024));
  EXPECT_TRUE(reader.isLoaded(3 * 1024 * 1024, 1024 * 1024));
}

TEST_F(AsyncChunkedReaderTest, fixedChunkWithoutPageIndex) {
  std::string content(10 * 1024 * 1024, 'F');
  auto readFile = std::make_shared<MockReadFile>(content);

  AsyncChunkedReader::Config config;
  config.chunkSize = 4 * 1024 * 1024;
  config.prefetchCount = 0;

  AsyncChunkedReader reader(
      readFile, *pool_, executor_.get(), 0, content.size(), config);

  reader.ensureLoaded(0, 1024);
  EXPECT_TRUE(reader.isLoaded(0, 4 * 1024 * 1024));

  reader.ensureLoaded(4 * 1024 * 1024, 1024);
  EXPECT_TRUE(reader.isLoaded(4 * 1024 * 1024, 4 * 1024 * 1024));

  reader.ensureLoaded(8 * 1024 * 1024, 1024);
  EXPECT_TRUE(reader.isLoaded(8 * 1024 * 1024, 2 * 1024 * 1024));
}

TEST_F(AsyncChunkedReaderTest, pageNeverSplitAcrossChunksWithPageIndex) {
  std::string content(20 * 1024 * 1024, 'S');
  auto readFile = std::make_shared<MockReadFile>(content);

  AsyncChunkedReader::Config config;
  config.chunkSize = 4 * 1024 * 1024;

  AsyncChunkedReader reader(
      readFile, *pool_, executor_.get(), 0, content.size(), config);

  std::vector<Region> regions;
  uint64_t offset = 0;
  std::vector<uint64_t> pageSizes = {
      1 * 1024 * 1024,
      2 * 1024 * 1024,
      3 * 1024 * 1024,
      1 * 1024 * 1024, // 1MB
      5 * 1024 * 1024, // 5MB - larger than chunkSize
  };

  for (auto size : pageSizes) {
    regions.push_back({offset, size, ""});
    offset += size;
  }
  reader.setReadRegions(regions);

  for (const auto& region : regions) {
    reader.ensureLoaded(region.offset, 1);
    uint64_t available = reader.availableContiguous(region.offset);
    EXPECT_GE(available, region.length)
        << "Page at offset " << region.offset << " with length "
        << region.length << " should be fully available, but only " << available
        << " bytes";
  }
}

TEST_F(AsyncChunkedReaderTest, lruEviction) {
  std::string content(32 * 1024 * 1024, 'L');
  auto readFile = std::make_shared<MockReadFile>(content);

  AsyncChunkedReader::Config config;
  config.chunkSize = 4 * 1024 * 1024;
  config.prefetchCount = 0;
  config.maxCachedChunks = 2;

  AsyncChunkedReader reader(
      readFile, *pool_, executor_.get(), 0, content.size(), config);

  reader.ensureLoaded(0, 1024);
  EXPECT_TRUE(reader.isLoaded(0, 1024));

  reader.ensureLoaded(4 * 1024 * 1024, 1024);
  EXPECT_TRUE(reader.isLoaded(4 * 1024 * 1024, 1024));

  reader.ensureLoaded(8 * 1024 * 1024, 1024);
  EXPECT_TRUE(reader.isLoaded(8 * 1024 * 1024, 1024));
}

TEST_F(AsyncChunkedReaderTest, concurrentAccess) {
  std::string content(16 * 1024 * 1024, 'C');
  auto readFile = std::make_shared<MockReadFile>(content);

  AsyncChunkedReader::Config config;
  config.chunkSize = 4 * 1024 * 1024;
  config.prefetchCount = 2;

  AsyncChunkedReader reader(
      readFile, *pool_, executor_.get(), 0, content.size(), config);

  std::vector<std::thread> threads;
  std::atomic<int> successCount{0};

  for (int i = 0; i < 8; ++i) {
    threads.emplace_back([&reader, &successCount, i]() {
      try {
        uint64_t offset = (i % 4) * 4 * 1024 * 1024;
        const uint8_t* data = reader.ensureLoaded(offset, 1024);
        if (data != nullptr) {
          successCount++;
        }
      } catch (...) {
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(successCount, 8);
}

TEST_F(AsyncChunkedReaderTest, runBasedChunksWithPageSkipping) {
  std::string content(8 * 1024 * 1024, 'R');
  auto readFile = std::make_shared<MockReadFile>(content);

  AsyncChunkedReader::Config config;
  config.chunkSize = 4 * 1024 * 1024;

  AsyncChunkedReader reader(
      readFile, *pool_, executor_.get(), 0, content.size(), config);

  std::vector<Region> regions;
  regions.push_back({0, 1 * 1024 * 1024, ""});
  regions.push_back({3 * 1024 * 1024, 1 * 1024 * 1024, ""});
  regions.push_back({4 * 1024 * 1024, 1 * 1024 * 1024, ""});
  regions.push_back({7 * 1024 * 1024, 1 * 1024 * 1024, ""});

  reader.setReadRegions(regions);

  const uint8_t* data = reader.ensureLoaded(0, 1024);
  ASSERT_NE(data, nullptr);

  data = reader.ensureLoaded(3 * 1024 * 1024, 1024);
  ASSERT_NE(data, nullptr);
}

TEST_F(AsyncChunkedReaderTest, pageSkippingCursorTracking) {
  std::string content(16 * 1024 * 1024, 'T');
  auto readFile = std::make_shared<MockReadFile>(content);

  AsyncChunkedReader::Config config;
  config.chunkSize = 4 * 1024 * 1024;

  AsyncChunkedReader reader(
      readFile, *pool_, executor_.get(), 0, content.size(), config);

  reader.ensureLoaded(0, 1024);
  reader.updateCursor(2 * 1024 * 1024);

  auto stats = reader.stats();
  EXPECT_GT(stats.bytesRead, 0);
}

TEST_F(AsyncChunkedReaderTest, pageSkippingPrefetchTrigger) {
  std::string content(32 * 1024 * 1024, 'P');
  auto readFile = std::make_shared<MockReadFile>(content);

  AsyncChunkedReader::Config config;
  config.chunkSize = 4 * 1024 * 1024;
  config.prefetchCount = 2;
  config.prefetchThreshold = 0.75;

  AsyncChunkedReader reader(
      readFile, *pool_, executor_.get(), 0, content.size(), config);

  std::vector<Region> regions;
  regions.push_back({0, 4 * 1024 * 1024, ""});
  regions.push_back({8 * 1024 * 1024, 4 * 1024 * 1024, ""});
  regions.push_back({16 * 1024 * 1024, 4 * 1024 * 1024, ""});
  reader.setReadRegions(regions);

  reader.ensureLoaded(0, 1024);
  reader.updateCursor(3 * 1024 * 1024);

  bool prefetched = false;
  for (int i = 0; i < 50 && !prefetched; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    prefetched = reader.isLoaded(8 * 1024 * 1024, 1024);
  }

  auto stats = reader.stats();
  EXPECT_GT(stats.bytesRead, 0);
}

TEST_F(AsyncChunkedReaderTest, adaptiveIncreasePrefetchCount) {
  std::string content(64 * 1024 * 1024, 'A');
  auto readFile = std::make_shared<MockReadFile>(content);

  AsyncChunkedReader::Config config;
  config.chunkSize = 4 * 1024 * 1024;
  config.prefetchCount = 1;

  AsyncChunkedReader reader(
      readFile, *pool_, executor_.get(), 0, content.size(), config);

  for (int i = 0; i < 8; ++i) {
    reader.ensureLoaded(i * 4 * 1024 * 1024, 1024);
    reader.updateCursor((i + 1) * 4 * 1024 * 1024);
  }

  auto stats = reader.stats();
  EXPECT_GT(stats.bytesRead, 0);
}

TEST_F(AsyncChunkedReaderTest, adaptiveDecreasePrefetchCount) {
  std::string content(32 * 1024 * 1024, 'D');
  auto readFile = std::make_shared<MockReadFile>(content);

  AsyncChunkedReader::Config config;
  config.chunkSize = 4 * 1024 * 1024;
  config.prefetchCount = 4;

  AsyncChunkedReader reader(
      readFile, *pool_, executor_.get(), 0, content.size(), config);

  for (int i = 0; i < 4; ++i) {
    reader.ensureLoaded(i * 4 * 1024 * 1024, 1024);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    reader.updateCursor((i + 1) * 4 * 1024 * 1024);
  }

  auto stats = reader.stats();
  EXPECT_GT(stats.cacheHits + stats.asyncLoads, 0);
}

TEST_F(AsyncChunkedReaderTest, adaptiveBoundsRespected) {
  std::string content(32 * 1024 * 1024, 'B');
  auto readFile = std::make_shared<MockReadFile>(content);

  AsyncChunkedReader::Config config;
  config.chunkSize = 4 * 1024 * 1024;
  config.prefetchCount = 2;

  AsyncChunkedReader reader(
      readFile, *pool_, executor_.get(), 0, content.size(), config);

  for (int i = 0; i < 8; ++i) {
    reader.ensureLoaded(i * 4 * 1024 * 1024, 1024);
  }

  auto stats = reader.stats();
  EXPECT_GE(stats.bytesRead, content.size());
}

TEST_F(AsyncChunkedReaderTest, adaptiveDisabled) {
  std::string content(32 * 1024 * 1024, 'N');
  auto readFile = std::make_shared<MockReadFile>(content);

  AsyncChunkedReader::Config config;
  config.chunkSize = 4 * 1024 * 1024;
  config.prefetchCount = 2;

  AsyncChunkedReader reader(
      readFile, *pool_, executor_.get(), 0, content.size(), config);

  for (int i = 0; i < 8; ++i) {
    reader.ensureLoaded(i * 4 * 1024 * 1024, 1024);
  }

  auto stats = reader.stats();
  EXPECT_GT(stats.bytesRead, 0);
}

TEST_F(AsyncChunkedReaderTest, columnChunkOffset) {
  std::string content(20 * 1024 * 1024, 'O');
  auto readFile = std::make_shared<MockReadFile>(content);

  uint64_t chunkOffset = 4 * 1024 * 1024;
  uint64_t chunkSize = 8 * 1024 * 1024;

  AsyncChunkedReader::Config config;
  config.chunkSize = 4 * 1024 * 1024;

  AsyncChunkedReader reader(
      readFile, *pool_, executor_.get(), chunkOffset, chunkSize, config);

  const uint8_t* data = reader.ensureLoaded(0, 1024);
  ASSERT_NE(data, nullptr);
  EXPECT_EQ(*data, 'O');
  EXPECT_EQ(reader.columnChunkSize(), chunkSize);
}
