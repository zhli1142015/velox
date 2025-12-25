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

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "velox/common/file/AsyncLocalFile.h"
#include "velox/common/file/FileInputStream.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

namespace facebook::velox {
namespace {

class AsyncLocalFileTest : public testing::Test {
 protected:
  void SetUp() override {
    tempDir_ = exec::test::TempDirectoryPath::create();
  }

  std::string createTestFile(const std::string& content) {
    std::string path = tempDir_->getPath() + "/test_file";
    std::ofstream file(path, std::ios::binary);
    file.write(content.data(), content.size());
    file.close();
    return path;
  }

  std::string generateRandomData(size_t size) {
    std::string data;
    data.resize(size);
    for (size_t i = 0; i < size; ++i) {
      data[i] = static_cast<char>(folly::Random::rand32() % 256);
    }
    return data;
  }

  std::shared_ptr<exec::test::TempDirectoryPath> tempDir_;
};

#ifdef VELOX_ENABLE_IO_URING

TEST_F(AsyncLocalFileTest, isIoUringAvailable) {
  // Just test that this doesn't crash
  bool available = isIoUringAvailable();
  LOG(INFO) << "io_uring available: " << (available ? "yes" : "no");
}

TEST_F(AsyncLocalFileTest, basicRead) {
  std::string content = "Hello, io_uring World!";
  std::string path = createTestFile(content);

  AsyncLocalReadFile file(path);

  // Check basic properties
  EXPECT_EQ(file.size(), content.size());
  EXPECT_FALSE(file.getName().empty());

  // Test pread
  std::vector<char> buffer(content.size());
  auto result = file.pread(0, content.size(), buffer.data());
  EXPECT_EQ(std::string(result), content);
}

TEST_F(AsyncLocalFileTest, readWithSubmitAndWait) {
  std::string content = generateRandomData(1024 * 1024); // 1MB
  std::string path = createTestFile(content);

  AsyncLocalReadFile file(path);

  if (!file.uringEnabled()) {
    GTEST_SKIP() << "io_uring not available";
  }

  // Use Bolt-style API
  std::vector<char> buffer(content.size());
  EXPECT_TRUE(file.submitRead(0, content.size(), buffer.data()));
  uint64_t bytesRead = file.waitForComplete();

  EXPECT_EQ(bytesRead, content.size());
  EXPECT_EQ(std::string(buffer.data(), buffer.size()), content);
}

TEST_F(AsyncLocalFileTest, chunkedLargeRead) {
  // Test that large reads are properly split into 128KB chunks
  size_t fileSize = 5 * kDefaultUringReadBlockSize; // 5 * 128KB = 640KB
  std::string content = generateRandomData(fileSize);
  std::string path = createTestFile(content);

  AsyncLocalReadFile file(path);

  if (!file.uringEnabled()) {
    GTEST_SKIP() << "io_uring not available";
  }

  std::vector<char> buffer(fileSize);
  EXPECT_TRUE(file.submitRead(0, fileSize, buffer.data()));
  uint64_t bytesRead = file.waitForComplete();

  EXPECT_EQ(bytesRead, fileSize);
  EXPECT_EQ(std::string(buffer.data(), buffer.size()), content);
}

TEST_F(AsyncLocalFileTest, preadvAsync) {
  std::string content = generateRandomData(256 * 1024); // 256KB
  std::string path = createTestFile(content);

  AsyncLocalReadFile file(path);

  // Test async preadv
  std::vector<char> buffer(content.size());
  std::vector<folly::Range<char*>> ranges;
  ranges.emplace_back(buffer.data(), buffer.size());

  auto future = file.preadvAsync(0, ranges);

  // Do some "work" while I/O is in flight
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // Wait for completion
  uint64_t bytesRead = std::move(future).get();
  EXPECT_EQ(bytesRead, content.size());
  EXPECT_EQ(std::string(buffer.data(), buffer.size()), content);
}

TEST_F(AsyncLocalFileTest, dualBufferPrefetch) {
  // Simulate Bolt's dual-buffer prefetch pattern
  size_t bufferSize = kDefaultUringReadBlockSize; // 128KB per buffer
  size_t fileSize = 10 * bufferSize; // 10 buffers worth
  std::string content = generateRandomData(fileSize);
  std::string path = createTestFile(content);

  AsyncLocalReadFile file(path);

  if (!file.uringEnabled()) {
    GTEST_SKIP() << "io_uring not available";
  }

  // Allocate dual buffers like Bolt's SpillInputStream
  std::vector<char> buffer0(bufferSize);
  std::vector<char> buffer1(bufferSize);

  uint64_t offset = 0;
  uint64_t completed = 0;
  int idx = 0;

  // Phase 1: init() - sync read buffer[0], async prefetch buffer[1]
  file.pread(offset, bufferSize, buffer0.data());
  offset += bufferSize;
  completed += bufferSize;

  // Verify first buffer
  EXPECT_EQ(
      std::string(buffer0.data(), bufferSize), content.substr(0, bufferSize));

  // Start prefetch for buffer[1]
  file.submitRead(offset, bufferSize, buffer1.data());
  offset += bufferSize;
  idx = 1;

  // Phase 2: next() loop - wait, process, prefetch
  while (completed < fileSize) {
    // Wait for current prefetch
    file.waitForComplete();

    // Get current buffer
    char* currentBuffer = (idx % 2 == 1) ? buffer1.data() : buffer0.data();
    size_t expectedOffset = completed;

    // Verify data
    EXPECT_EQ(
        std::string(currentBuffer, bufferSize),
        content.substr(expectedOffset, bufferSize));

    completed += bufferSize;
    idx++;

    // Start next prefetch if not at end
    if (offset < fileSize) {
      char* nextBuffer = (idx % 2 == 1) ? buffer1.data() : buffer0.data();
      file.submitRead(offset, bufferSize, nextBuffer);
      offset += bufferSize;
    }
  }

  EXPECT_EQ(completed, fileSize);
}

TEST_F(AsyncLocalFileTest, basicWrite) {
  std::string path = tempDir_->getPath() + "/write_test";
  std::string content = "Hello, io_uring Write!";

  {
    AsyncLocalWriteFile file(path, true, false);
    file.append(content);
    file.close();
  }

  // Verify by reading back
  std::ifstream inFile(path, std::ios::binary);
  std::string readBack(
      (std::istreambuf_iterator<char>(inFile)),
      std::istreambuf_iterator<char>());
  EXPECT_EQ(readBack, content);
}

TEST_F(AsyncLocalFileTest, writeWithSubmitAndWait) {
  std::string path = tempDir_->getPath() + "/async_write_test";
  std::string content = generateRandomData(512 * 1024); // 512KB

  {
    std::unique_ptr<WriteFile> file =
        std::make_unique<AsyncLocalWriteFile>(path, true, false);

    if (!file->uringEnabled()) {
      GTEST_SKIP() << "io_uring not available";
    }

    // Create WriteBuffers to manage IOBuf lifetime (Bolt-style)
    WriteBuffers writeBuffers;

    // Create IOBuf from content
    auto iobuf = folly::IOBuf::copyBuffer(content);
    int taskId = 0;

    // Add to WriteBuffers and get pointer
    folly::IOBuf* buf = writeBuffers.add(iobuf, taskId, file);

    // Submit async write
    file->submitWrite(buf, taskId);

    // Wait for completion
    EXPECT_TRUE(file->waitForComplete(taskId, writeBuffers.buffers()));
    file->close();
  }

  // Verify
  std::ifstream inFile(path, std::ios::binary);
  std::string readBack(
      (std::istreambuf_iterator<char>(inFile)),
      std::istreambuf_iterator<char>());
  EXPECT_EQ(readBack, content);
}

TEST_F(AsyncLocalFileTest, multipleAsyncWrites) {
  std::string path = tempDir_->getPath() + "/multi_async_write_test";
  std::vector<std::string> chunks;
  size_t chunkSize = 128 * 1024; // 128KB chunks
  int numChunks = 10;

  for (int i = 0; i < numChunks; ++i) {
    chunks.push_back(generateRandomData(chunkSize));
  }

  {
    std::unique_ptr<WriteFile> file =
        std::make_unique<AsyncLocalWriteFile>(path, true, false);

    if (!file->uringEnabled()) {
      GTEST_SKIP() << "io_uring not available";
    }

    // Create WriteBuffers to manage IOBuf lifetime (Bolt-style)
    WriteBuffers writeBuffers;

    // Submit multiple async writes
    for (int i = 0; i < numChunks; ++i) {
      auto iobuf = folly::IOBuf::copyBuffer(chunks[i]);
      folly::IOBuf* buf = writeBuffers.add(iobuf, i, file);
      file->submitWrite(buf, i);
    }

    // Wait for all and clear buffers
    writeBuffers.clear(file);
    file->close();
  }

  // Verify
  std::ifstream inFile(path, std::ios::binary);
  std::string readBack(
      (std::istreambuf_iterator<char>(inFile)),
      std::istreambuf_iterator<char>());

  std::string expected;
  for (const auto& chunk : chunks) {
    expected += chunk;
  }
  EXPECT_EQ(readBack, expected);
}

TEST_F(AsyncLocalFileTest, pendingReadsTracking) {
  std::string content = generateRandomData(256 * 1024);
  std::string path = createTestFile(content);

  AsyncLocalReadFile file(path);

  if (!file.uringEnabled()) {
    GTEST_SKIP() << "io_uring not available";
  }

  EXPECT_EQ(file.pendingReads(), 0);

  std::vector<char> buffer(content.size());
  file.submitRead(0, content.size(), buffer.data());

  // After submit, should have pending reads
  EXPECT_GT(file.pendingReads(), 0);

  file.waitForComplete();

  // After wait, should be back to 0
  EXPECT_EQ(file.pendingReads(), 0);
}

TEST_F(AsyncLocalFileTest, fileInputStreamWithUring) {
  // Test FileInputStream using io_uring dual-buffer prefetch.
  // This simulates Bolt's SpillInputStream behavior.
  size_t fileSize = 5 * 1024 * 1024; // 5MB
  std::string content = generateRandomData(fileSize);
  std::string path = createTestFile(content);

  // Create async file
  auto file = std::make_unique<AsyncLocalReadFile>(path);
  if (!file->uringEnabled()) {
    GTEST_SKIP() << "io_uring not available";
  }

  // Create memory pool for FileInputStream
  memory::MemoryManager::testingSetInstance({});
  auto pool = memory::memoryManager()->addLeafPool("test");

  // Use a buffer size smaller than file size to trigger dual-buffer mode
  uint64_t bufferSize = 1024 * 1024; // 1MB buffer

  // Create FileInputStream with io_uring file
  common::FileInputStream stream(std::move(file), bufferSize, pool.get());

  // Verify the stream can read all data correctly
  EXPECT_EQ(stream.size(), fileSize);
  EXPECT_FALSE(stream.atEnd());

  std::vector<uint8_t> readBuffer(fileSize);
  size_t totalRead = 0;

  while (!stream.atEnd()) {
    size_t chunkSize = std::min<size_t>(128 * 1024, fileSize - totalRead);
    stream.readBytes(readBuffer.data() + totalRead, chunkSize);
    totalRead += chunkSize;
  }

  EXPECT_EQ(totalRead, fileSize);
  EXPECT_TRUE(stream.atEnd());

  // Verify data integrity
  EXPECT_EQ(
      std::string(
          reinterpret_cast<char*>(readBuffer.data()), readBuffer.size()),
      content);

  // Check stats
  auto stats = stream.stats();
  LOG(INFO) << "FileInputStream io_uring stats: " << stats.toString();
  EXPECT_GT(stats.numReads, 0);
  EXPECT_EQ(stats.readBytes, fileSize);
}

#else // !VELOX_ENABLE_IO_URING

TEST_F(AsyncLocalFileTest, ioUringDisabled) {
  EXPECT_FALSE(isIoUringAvailable());
  LOG(INFO) << "io_uring is disabled at compile time";
}

#endif // VELOX_ENABLE_IO_URING

} // namespace
} // namespace facebook::velox
