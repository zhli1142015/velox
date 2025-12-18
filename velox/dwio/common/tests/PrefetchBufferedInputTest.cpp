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
#include "velox/dwio/common/PrefetchBufferedInput.h"

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;
using namespace ::testing;

namespace {

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

std::string readStream(SeekableInputStream& stream, int maxSize = 4096) {
  std::string result;
  const void* data;
  int size;
  while (stream.Next(&data, &size)) {
    result.append(static_cast<const char*>(data), size);
    if (result.size() >= static_cast<size_t>(maxSize)) {
      break;
    }
  }
  return result;
}

class PrefetchBufferedInputTest : public testing::Test {
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

TEST_F(PrefetchBufferedInputTest, pageToChunkMapping) {
  std::string content(16 * 1024 * 1024, 'X');
  for (size_t i = 0; i < content.size(); ++i) {
    content[i] = static_cast<char>('A' + (i % 26));
  }
  auto readFile = std::make_shared<MockReadFile>(content);

  AsyncChunkedReader::Config config;
  config.chunkSize = 4 * 1024 * 1024;
  config.prefetchCount = 1;

  PrefetchBufferedInput input(readFile, *pool_, executor_.get(), config);

  auto stream1 = input.enqueue({0, 1024 * 1024});
  auto stream2 = input.enqueue({4 * 1024 * 1024, 1024 * 1024});
  auto stream3 = input.enqueue({8 * 1024 * 1024, 1024 * 1024});

  ASSERT_NE(stream1, nullptr);
  ASSERT_NE(stream2, nullptr);
  ASSERT_NE(stream3, nullptr);

  input.load(LogType::TEST);

  std::string data1 = readStream(*stream1);
  EXPECT_EQ(data1.size(), 1024 * 1024);
  EXPECT_EQ(data1.substr(0, 26), "ABCDEFGHIJKLMNOPQRSTUVWXYZ");

  std::string data2 = readStream(*stream2);
  EXPECT_EQ(data2.size(), 1024 * 1024);
}

TEST_F(PrefetchBufferedInputTest, lazyStreamBehavior) {
  std::string content(8 * 1024 * 1024, 'L');
  auto readFile = std::make_shared<MockReadFile>(content);

  PrefetchBufferedInput input(readFile, *pool_, executor_.get());

  auto stream = input.enqueue({0, 1024});
  ASSERT_NE(stream, nullptr);
  EXPECT_EQ(readFile->preadCount(), 0);

  input.load(LogType::TEST);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  const void* data;
  int size;
  EXPECT_TRUE(stream->Next(&data, &size));
  EXPECT_GT(size, 0);
}

TEST_F(PrefetchBufferedInputTest, singleChunkPerPage) {
  std::string content(8 * 1024 * 1024, 'S');
  auto readFile = std::make_shared<MockReadFile>(content);

  AsyncChunkedReader::Config config;
  config.chunkSize = 4 * 1024 * 1024;

  PrefetchBufferedInput input(readFile, *pool_, executor_.get(), config);

  auto stream = input.enqueue({0, 1000});
  input.load(LogType::TEST);

  const void* data;
  int size;
  EXPECT_TRUE(stream->Next(&data, &size));
  EXPECT_EQ(size, 1000);
  EXPECT_FALSE(stream->Next(&data, &size));
}

TEST_F(PrefetchBufferedInputTest, statistics) {
  std::string content(16 * 1024 * 1024, 'T');
  auto readFile = std::make_shared<MockReadFile>(content);

  AsyncChunkedReader::Config config;
  config.chunkSize = 4 * 1024 * 1024;
  config.prefetchCount = 1;

  PrefetchBufferedInput input(readFile, *pool_, executor_.get(), config);

  auto stream1 = input.enqueue({0, 1024 * 1024});
  auto stream2 = input.enqueue({4 * 1024 * 1024, 1024 * 1024});

  input.load(LogType::TEST);

  readStream(*stream1);
  readStream(*stream2);

  auto stats = input.ioStats();
  EXPECT_GT(stats.bytesRead, 0);
}

TEST_F(PrefetchBufferedInputTest, cursorBasedPrefetch) {
  // Verify cursor-based prefetch triggering
  std::string content(32 * 1024 * 1024, 'C');
  auto readFile = std::make_shared<MockReadFile>(content);

  AsyncChunkedReader::Config config;
  config.chunkSize = 4 * 1024 * 1024;
  config.prefetchCount = 2;
  config.prefetchThreshold = 0.75;

  PrefetchBufferedInput input(readFile, *pool_, executor_.get(), config);

  // Enqueue regions across multiple chunks
  auto stream1 = input.enqueue({0, 4 * 1024 * 1024});
  auto stream2 = input.enqueue({4 * 1024 * 1024, 4 * 1024 * 1024});
  auto stream3 = input.enqueue({8 * 1024 * 1024, 4 * 1024 * 1024});

  input.load(LogType::TEST);

  // Read first stream (should trigger prefetch for subsequent)
  readStream(*stream1);

  // Wait for prefetch
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Next chunks should be prefetched
  EXPECT_TRUE(input.isBuffered(4 * 1024 * 1024, 1024));
}

TEST_F(PrefetchBufferedInputTest, cloneBehavior) {
  // Verify clone creates independent instance
  std::string content(8 * 1024 * 1024, 'K');
  auto readFile = std::make_shared<MockReadFile>(content);

  PrefetchBufferedInput input(readFile, *pool_, executor_.get());

  // Enqueue a region
  input.enqueue({0, 1024});
  input.load(LogType::TEST);

  // Clone
  auto cloned = input.clone();
  ASSERT_NE(cloned, nullptr);

  // Cloned should be a PrefetchBufferedInput
  auto* prefetchClone = dynamic_cast<PrefetchBufferedInput*>(cloned.get());
  ASSERT_NE(prefetchClone, nullptr);

  // Cloned should not be loaded yet
  EXPECT_FALSE(prefetchClone->isLoaded());

  // Should share executor
  EXPECT_EQ(prefetchClone->executor(), executor_.get());
}

TEST_F(PrefetchBufferedInputTest, streamSeekAndSkip) {
  // Test stream seek and skip operations
  std::string content(8 * 1024 * 1024, 'Z');
  for (size_t i = 0; i < content.size(); ++i) {
    content[i] = static_cast<char>('A' + (i % 26));
  }
  auto readFile = std::make_shared<MockReadFile>(content);

  PrefetchBufferedInput input(readFile, *pool_, executor_.get());

  auto stream = input.enqueue({0, 1024});
  input.load(LogType::TEST);

  // Read some data
  const void* data;
  int size;
  EXPECT_TRUE(stream->Next(&data, &size));
  EXPECT_GT(size, 0);

  // Back up
  stream->BackUp(10);
  EXPECT_EQ(stream->ByteCount(), size - 10);

  // Skip
  EXPECT_TRUE(stream->SkipInt64(5));
  EXPECT_EQ(stream->ByteCount(), size - 5);
}

TEST_F(PrefetchBufferedInputTest, multipleEnqueuesSameChunk) {
  // Multiple enqueues in the same chunk should share IO
  std::string content(4 * 1024 * 1024, 'M');
  auto readFile = std::make_shared<MockReadFile>(content);

  AsyncChunkedReader::Config config;
  config.chunkSize = 4 * 1024 * 1024; // One big chunk

  PrefetchBufferedInput input(readFile, *pool_, executor_.get(), config);

  // Enqueue multiple small regions in same chunk
  auto stream1 = input.enqueue({0, 1000});
  auto stream2 = input.enqueue({1000, 1000});
  auto stream3 = input.enqueue({2000, 1000});

  input.load(LogType::TEST);

  // All should be readable with minimal IO
  EXPECT_TRUE(readStream(*stream1).size() == 1000);
  EXPECT_TRUE(readStream(*stream2).size() == 1000);
  EXPECT_TRUE(readStream(*stream3).size() == 1000);

  // Only one chunk read should have happened
  auto stats = input.ioStats();
  EXPECT_EQ(stats.syncLoads + stats.asyncLoads, 1);
}

TEST_F(PrefetchBufferedInputTest, isBufferedCheck) {
  std::string content(16 * 1024 * 1024, 'B');
  auto readFile = std::make_shared<MockReadFile>(content);

  AsyncChunkedReader::Config config;
  config.chunkSize = 4 * 1024 * 1024;
  config.prefetchCount = 0; // Disable prefetch for this test

  PrefetchBufferedInput input(readFile, *pool_, executor_.get(), config);

  auto stream = input.enqueue({0, 1024});
  input.load(LogType::TEST);

  // Before reading, nothing should be buffered
  // (depending on implementation, initial prefetch might load chunk 0)

  // Read the stream to load chunk 0
  readStream(*stream);

  // Now chunk 0 should be buffered
  EXPECT_TRUE(input.isBuffered(0, 1024));

  // Chunk 1 (at offset 4MB) should not be buffered yet
  EXPECT_FALSE(input.isBuffered(4 * 1024 * 1024, 1024));
}

TEST_F(PrefetchBufferedInputTest, streamPositionProvider) {
  std::string content(4 * 1024 * 1024, 'P');
  auto readFile = std::make_shared<MockReadFile>(content);

  PrefetchBufferedInput input(readFile, *pool_, executor_.get());

  auto stream = input.enqueue({0, 1024});
  input.load(LogType::TEST);

  // Get position size
  EXPECT_GT(stream->positionSize(), 0);

  // Get name
  EXPECT_FALSE(stream->getName().empty());
}

TEST_F(PrefetchBufferedInputTest, shouldPreloadAndPrefetchStripes) {
  std::string content(4 * 1024 * 1024, 'R');
  auto readFile = std::make_shared<MockReadFile>(content);

  // With executor
  PrefetchBufferedInput withExecutor(readFile, *pool_, executor_.get());
  EXPECT_TRUE(withExecutor.shouldPreload());
  EXPECT_TRUE(withExecutor.shouldPrefetchStripes());

  // Without executor
  PrefetchBufferedInput withoutExecutor(readFile, *pool_, nullptr);
  EXPECT_FALSE(withoutExecutor.shouldPreload());
  EXPECT_FALSE(withoutExecutor.shouldPrefetchStripes());
}

TEST_F(PrefetchBufferedInputTest, supportSyncLoad) {
  std::string content(4 * 1024 * 1024, 'Y');
  auto readFile = std::make_shared<MockReadFile>(content);

  PrefetchBufferedInput input(readFile, *pool_, executor_.get());

  // PrefetchBufferedInput uses async loading
  EXPECT_FALSE(input.supportSyncLoad());
}

TEST_F(PrefetchBufferedInputTest, emptyEnqueue) {
  std::string content(4 * 1024 * 1024, 'E');
  auto readFile = std::make_shared<MockReadFile>(content);

  PrefetchBufferedInput input(readFile, *pool_, executor_.get());

  // Don't enqueue anything
  input.load(LogType::TEST);

  // Should not crash, but chunkedReader might be null or empty
  auto stats = input.ioStats();
  EXPECT_EQ(stats.bytesRead, 0);
}

TEST_F(PrefetchBufferedInputTest, largeRegion) {
  // Test with a region larger than chunk size
  std::string content(20 * 1024 * 1024, 'G');
  auto readFile = std::make_shared<MockReadFile>(content);

  AsyncChunkedReader::Config config;
  config.chunkSize = 4 * 1024 * 1024;
  config.prefetchCount = 2;

  PrefetchBufferedInput input(readFile, *pool_, executor_.get(), config);

  // Enqueue a region larger than one chunk (8MB)
  auto stream = input.enqueue({0, 8 * 1024 * 1024});

  input.load(LogType::TEST);

  // Should be able to read all data
  std::string data = readStream(*stream, 8 * 1024 * 1024);
  EXPECT_EQ(data.size(), 8 * 1024 * 1024);
}
