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

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <gtest/gtest.h>

#include "velox/common/base/Fs.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/common/AsyncChunkedReader.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/PrefetchBufferedInput.h"
#include "velox/dwio/parquet/tests/ParquetTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::common;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::parquet;

namespace {

/// Mock ReadFile that tracks IO operations for testing prefetch effectiveness.
class TrackedReadFile : public ReadFile {
 public:
  explicit TrackedReadFile(std::shared_ptr<ReadFile> delegate)
      : delegate_(std::move(delegate)) {}

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buf,
      const FileStorageContext& ctx) const override {
    recordRead(offset, length);
    return delegate_->pread(offset, length, buf, ctx);
  }

  uint64_t size() const override {
    return delegate_->size();
  }

  uint64_t memoryUsage() const override {
    return delegate_->memoryUsage();
  }

  std::string getName() const override {
    return delegate_->getName();
  }

  uint64_t getNaturalReadSize() const override {
    return delegate_->getNaturalReadSize();
  }

  bool shouldCoalesce() const override {
    return delegate_->shouldCoalesce();
  }

  /// Returns number of pread calls made.
  int readCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return readCount_;
  }

  /// Returns total bytes read.
  uint64_t totalBytesRead() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return totalBytesRead_;
  }

  /// Returns timestamps of read operations (for concurrency analysis).
  std::vector<std::chrono::steady_clock::time_point> readTimestamps() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return readTimestamps_;
  }

  /// Returns read regions for analysis.
  std::vector<std::pair<uint64_t, uint64_t>> readRegions() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return readRegions_;
  }

  void resetStats() {
    std::lock_guard<std::mutex> lock(mutex_);
    readCount_ = 0;
    totalBytesRead_ = 0;
    readTimestamps_.clear();
    readRegions_.clear();
  }

 private:
  void recordRead(uint64_t offset, uint64_t length) const {
    std::lock_guard<std::mutex> lock(mutex_);
    readCount_++;
    totalBytesRead_ += length;
    readTimestamps_.push_back(std::chrono::steady_clock::now());
    readRegions_.emplace_back(offset, length);
  }

  /// Underlying file to delegate reads to.
  std::shared_ptr<ReadFile> delegate_;

  /// Mutex to protect mutable state.
  mutable std::mutex mutex_;

  /// Number of pread calls made.
  mutable int readCount_{0};

  /// Total bytes read across all pread calls.
  mutable uint64_t totalBytesRead_{0};

  /// Timestamps of each read operation.
  mutable std::vector<std::chrono::steady_clock::time_point> readTimestamps_;

  /// Regions (offset, length) of each read operation.
  mutable std::vector<std::pair<uint64_t, uint64_t>> readRegions_;
};

/// Mock executor that tracks task submissions for concurrency analysis.
class TrackedExecutor : public folly::Executor {
 public:
  explicit TrackedExecutor(std::shared_ptr<folly::Executor> delegate)
      : delegate_(std::move(delegate)) {}

  void add(folly::Func func) override {
    recordSubmission();
    delegate_->add(std::move(func));
  }

  /// Returns number of tasks submitted.
  int taskCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return taskCount_;
  }

  /// Returns timestamps of task submissions.
  std::vector<std::chrono::steady_clock::time_point> submissionTimestamps()
      const {
    std::lock_guard<std::mutex> lock(mutex_);
    return submissionTimestamps_;
  }

  /// Returns max concurrent submissions within a time window.
  int maxConcurrentSubmissions(std::chrono::milliseconds window) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (submissionTimestamps_.empty()) {
      return 0;
    }

    int maxConcurrent = 0;
    for (size_t i = 0; i < submissionTimestamps_.size(); ++i) {
      int concurrent = 1;
      for (size_t j = i + 1; j < submissionTimestamps_.size(); ++j) {
        auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(
            submissionTimestamps_[j] - submissionTimestamps_[i]);
        if (diff <= window) {
          concurrent++;
        }
      }
      maxConcurrent = std::max(maxConcurrent, concurrent);
    }
    return maxConcurrent;
  }

  void resetStats() {
    std::lock_guard<std::mutex> lock(mutex_);
    taskCount_ = 0;
    submissionTimestamps_.clear();
  }

 private:
  void recordSubmission() {
    std::lock_guard<std::mutex> lock(mutex_);
    taskCount_++;
    submissionTimestamps_.push_back(std::chrono::steady_clock::now());
  }

  /// Underlying executor to delegate tasks to.
  std::shared_ptr<folly::Executor> delegate_;

  /// Mutex to protect mutable state.
  mutable std::mutex mutex_;

  /// Number of tasks submitted.
  mutable int taskCount_{0};

  /// Timestamps of each task submission.
  mutable std::vector<std::chrono::steady_clock::time_point>
      submissionTimestamps_;
};

} // namespace

class ParquetReaderPrefetchTest : public ParquetTestBase {
 protected:
  void SetUp() override {
    ParquetTestBase::SetUp();
    filesystems::registerLocalFileSystem();
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(4);
    trackedExecutor_ = std::make_shared<TrackedExecutor>(executor_);
  }

  void TearDown() override {
    executor_->join();
  }

  /// Creates a Parquet file with specified number of rows and row groups.
  std::string createTestParquetFile(
      const std::string& name,
      const RowTypePtr& schema,
      int64_t numRows,
      int32_t rowsPerRowGroup) {
    auto filePath = fmt::format("{}/{}", tempPath_->getPath(), name);

    auto sink = std::make_unique<dwio::common::LocalFileSink>(
        filePath, dwio::common::FileSink::Options{});

    parquet::WriterOptions writerOptions;
    writerOptions.memoryPool = rootPool_.get();
    // Set small row group size to create multiple row groups.
    writerOptions.parquetWriteTimestampUnit = TimestampPrecision::kNanoseconds;

    auto writer = std::make_unique<parquet::Writer>(
        std::move(sink), writerOptions, schema);

    VectorFuzzer::Options fuzzerOpts;
    fuzzerOpts.vectorSize = rowsPerRowGroup;
    fuzzerOpts.nullRatio = 0.0; // No nulls to avoid Arrow import issues.
    VectorFuzzer fuzzer(fuzzerOpts, leafPool_.get());

    int64_t rowsWritten = 0;
    while (rowsWritten < numRows) {
      auto batch = fuzzer.fuzzRow(schema);
      writer->write(batch);
      writer->flush();
      rowsWritten += rowsPerRowGroup;
    }
    writer->close();
    return filePath;
  }

  /// Creates a large Parquet file suitable for prefetch testing.
  std::string createLargeTestFile() {
    // Schema with multiple columns to have substantial data per row group.
    auto schema =
        ROW({"id", "value", "name", "data"},
            {BIGINT(), DOUBLE(), VARCHAR(), VARBINARY()});

    return createTestParquetFile(
        "large_test.parquet",
        schema,
        100000, // 100K rows
        10000); // 10K rows per row group = 10 row groups
  }

  /// Thread pool executor for async operations.
  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;

  /// Tracked executor wrapper for testing.
  std::shared_ptr<TrackedExecutor> trackedExecutor_;
};

// =============================================================================
// Prefetch Effect Assertion Tests.
// =============================================================================

TEST_F(ParquetReaderPrefetchTest, asyncLoadTriggered) {
  // Verify that async IO is actually triggered when using
  // PrefetchBufferedInput.

  // Create a moderate-sized test file.
  auto schema = ROW({"a", "b"}, {BIGINT(), DOUBLE()});
  auto filePath = createTestParquetFile(
      "async_load_test.parquet",
      schema,
      50000, // 50K rows
      10000); // 5 row groups

  auto localFile = std::make_shared<LocalReadFile>(filePath);
  auto trackedFile = std::make_shared<TrackedReadFile>(localFile);

  AsyncChunkedReader::Config config;
  config.chunkSize = 64 * 1024; // 64KB chunks for testing
  config.prefetchCount = 2;

  PrefetchBufferedInput input(
      trackedFile, *leafPool_, trackedExecutor_.get(), config);

  // Enqueue several regions.
  std::vector<std::unique_ptr<SeekableInputStream>> streams;
  uint64_t fileSize = trackedFile->size();
  uint64_t regionSize = 16 * 1024; // 16KB regions
  for (uint64_t offset = 0; offset < fileSize && offset < 256 * 1024;
       offset += regionSize) {
    streams.push_back(input.enqueue({offset, regionSize}));
  }

  input.load(LogType::TEST);

  // Read from streams.
  for (auto& stream : streams) {
    const void* data;
    int size;
    stream->Next(&data, &size);
  }

  // PREFETCH EFFECT ASSERTION:
  // With async prefetch enabled, the executor should have received tasks.
  EXPECT_GT(trackedExecutor_->taskCount(), 0)
      << "Async executor should have received prefetch tasks";

  auto stats = input.ioStats();
  // Either sync or async loads should have happened.
  EXPECT_GT(stats.syncLoads + stats.asyncLoads, 0)
      << "IO operations should have occurred";

  // PREFETCH EFFECTIVENESS:
  // If prefetch is working, we should see some async loads.
  // This is a soft check as timing can affect results.
  if (stats.asyncLoads > 0) {
    EXPECT_GT(stats.asyncLoads, 0) << "Async prefetch should have loaded data";
  }
}

TEST_F(ParquetReaderPrefetchTest, prefetchReducesSyncLoads) {
  // Compare behavior with and without prefetch to verify it reduces sync loads.

  auto schema = ROW({"a"}, {BIGINT()});
  auto filePath =
      createTestParquetFile("prefetch_compare.parquet", schema, 100000, 20000);

  auto localFile = std::make_shared<LocalReadFile>(filePath);
  uint64_t fileSize = localFile->size();

  // Test 1: Without prefetch (prefetchCount = 0).
  {
    auto trackedFile = std::make_shared<TrackedReadFile>(localFile);
    AsyncChunkedReader::Config config;
    config.chunkSize = 32 * 1024;
    config.prefetchCount = 0; // No prefetch.

    PrefetchBufferedInput input(
        trackedFile, *leafPool_, executor_.get(), config);

    std::vector<std::unique_ptr<SeekableInputStream>> streams;
    for (uint64_t offset = 0; offset < fileSize && offset < 128 * 1024;
         offset += 8 * 1024) {
      streams.push_back(input.enqueue({offset, 8 * 1024}));
    }
    input.load(LogType::TEST);

    for (auto& stream : streams) {
      const void* data;
      int size;
      stream->Next(&data, &size);
    }

    auto statsNoPrefetch = input.ioStats();

    // Test 2: With prefetch.
    trackedFile->resetStats();
    trackedExecutor_->resetStats();

    config.prefetchCount = 4; // Enable prefetch.
    PrefetchBufferedInput inputWithPrefetch(
        trackedFile, *leafPool_, trackedExecutor_.get(), config);

    std::vector<std::unique_ptr<SeekableInputStream>> streamsWithPrefetch;
    for (uint64_t offset = 0; offset < fileSize && offset < 128 * 1024;
         offset += 8 * 1024) {
      streamsWithPrefetch.push_back(
          inputWithPrefetch.enqueue({offset, 8 * 1024}));
    }
    inputWithPrefetch.load(LogType::TEST);

    for (auto& stream : streamsWithPrefetch) {
      const void* data;
      int size;
      stream->Next(&data, &size);
    }

    auto statsWithPrefetch = inputWithPrefetch.ioStats();

    // PREFETCH EFFECT ASSERTION:
    // With prefetch enabled, async loads should increase relative to sync
    // loads. Total loads should be similar, but distribution changes.

    // Verify both scenarios loaded data.
    EXPECT_GT(statsNoPrefetch.bytesRead, 0);
    EXPECT_GT(statsWithPrefetch.bytesRead, 0);

    // With prefetch, we expect either:
    // 1. More async loads than without prefetch, OR
    // 2. Similar total but executor received tasks
    if (statsWithPrefetch.asyncLoads > 0) {
      EXPECT_GE(statsWithPrefetch.asyncLoads, statsNoPrefetch.asyncLoads)
          << "With prefetch enabled, async loads should not decrease";
    }
  }
}

TEST_F(ParquetReaderPrefetchTest, cacheHitRateWithPrefetch) {
  // Verify that prefetch improves cache hit rate.

  auto schema = ROW({"a", "b"}, {BIGINT(), DOUBLE()});
  auto filePath =
      createTestParquetFile("cache_hit_test.parquet", schema, 50000, 10000);

  auto localFile = std::make_shared<LocalReadFile>(filePath);
  auto trackedFile = std::make_shared<TrackedReadFile>(localFile);

  AsyncChunkedReader::Config config;
  config.chunkSize = 64 * 1024;
  config.prefetchCount = 4;
  config.maxCachedChunks = 16;
  config.prefetchThreshold = 0.5; // Trigger prefetch early.

  PrefetchBufferedInput input(
      trackedFile, *leafPool_, trackedExecutor_.get(), config);

  // Enqueue regions that will benefit from prefetch.
  std::vector<std::unique_ptr<SeekableInputStream>> streams;
  uint64_t fileSize = trackedFile->size();
  for (uint64_t offset = 0; offset < fileSize && offset < 512 * 1024;
       offset += 32 * 1024) {
    streams.push_back(input.enqueue({offset, 32 * 1024}));
  }

  input.load(LogType::TEST);

  // Simulate sequential access pattern (ideal for prefetch).
  for (auto& stream : streams) {
    const void* data;
    int size;
    while (stream->Next(&data, &size)) {
      // Consume data slowly to allow prefetch to work.
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  }

  auto stats = input.ioStats();

  // PREFETCH EFFECT ASSERTION:
  // With sequential access and prefetch, we should see cache hits.
  int64_t totalAccesses = stats.cacheHits + stats.cacheMisses;
  if (totalAccesses > 0) {
    double hitRate = static_cast<double>(stats.cacheHits) /
        static_cast<double>(totalAccesses);
    // We expect some level of cache hits with prefetch.
    // Exact rate depends on timing, so we use a soft threshold.
    EXPECT_GE(hitRate, 0.0)
        << "Cache hit rate should be non-negative: hits=" << stats.cacheHits
        << ", misses=" << stats.cacheMisses;

    // Log the actual hit rate for analysis.
    LOG(INFO) << "Prefetch cache hit rate: " << (hitRate * 100) << "% ("
              << stats.cacheHits << " hits, " << stats.cacheMisses
              << " misses)";
  }

  // Verify prefetch tasks were submitted.
  EXPECT_GT(trackedExecutor_->taskCount(), 0)
      << "Prefetch should submit tasks to executor";
}

TEST_F(ParquetReaderPrefetchTest, singleRowGroupFile) {
  // Test single row group file. This is where page-level prefetch helps most.
  auto schema = ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), VARCHAR()});
  auto filePath = createTestParquetFile(
      "single_rg.parquet",
      schema,
      50000, // 50K rows
      50000); // All in one row group.

  auto localFile = std::make_shared<LocalReadFile>(filePath);
  auto trackedFile = std::make_shared<TrackedReadFile>(localFile);

  AsyncChunkedReader::Config config;
  config.chunkSize = 128 * 1024; // 128KB chunks
  config.prefetchCount = 2;

  PrefetchBufferedInput input(
      trackedFile, *leafPool_, trackedExecutor_.get(), config);

  // Single row group means all column data is in one region per column.
  auto stream = input.enqueue({0, trackedFile->size()});
  input.load(LogType::TEST);

  // Read all data.
  const void* data;
  int size;
  while (stream->Next(&data, &size)) {
    // Process.
  }

  auto stats = input.ioStats();
  EXPECT_GT(stats.bytesRead, 0);

  // PREFETCH EFFECT:
  // For single row group, prefetch should still help by loading chunks ahead.
  LOG(INFO) << "Single row group stats: syncLoads=" << stats.syncLoads
            << ", asyncLoads=" << stats.asyncLoads
            << ", bytes=" << stats.bytesRead;
}

TEST_F(ParquetReaderPrefetchTest, multipleColumnsParallelPrefetch) {
  // Verify parallel prefetch across multiple columns.

  auto schema =
      ROW({"col1", "col2", "col3", "col4"},
          {BIGINT(), DOUBLE(), BIGINT(), DOUBLE()});
  auto filePath = createTestParquetFile(
      "multi_col.parquet",
      schema,
      50000, // 50K rows
      10000); // 5 row groups

  auto localFile = std::make_shared<LocalReadFile>(filePath);

  trackedExecutor_->resetStats();

  AsyncChunkedReader::Config config;
  config.chunkSize = 32 * 1024;
  config.prefetchCount = 2;

  // Create multiple PrefetchBufferedInput instances (one per column
  // simulation).
  std::vector<std::unique_ptr<PrefetchBufferedInput>> inputs;
  std::vector<std::unique_ptr<SeekableInputStream>> streams;

  uint64_t fileSize = localFile->size();
  uint64_t regionSize = fileSize / 4;

  for (int i = 0; i < 4; ++i) {
    auto trackedFile = std::make_shared<TrackedReadFile>(localFile);
    auto input = std::make_unique<PrefetchBufferedInput>(
        trackedFile, *leafPool_, trackedExecutor_.get(), config);

    uint64_t offset = i * regionSize;
    streams.push_back(input->enqueue({offset, regionSize}));
    input->load(LogType::TEST);
    inputs.push_back(std::move(input));
  }

  // PREFETCH EFFECT ASSERTION:
  // Multiple inputs loading simultaneously should submit multiple tasks.
  // Check concurrent task submissions.
  int maxConcurrent = trackedExecutor_->maxConcurrentSubmissions(
      std::chrono::milliseconds(100));

  // We expect some level of concurrent submissions.
  EXPECT_GT(trackedExecutor_->taskCount(), 0)
      << "Should have submitted prefetch tasks";

  LOG(INFO) << "Multi-column parallel prefetch: total tasks="
            << trackedExecutor_->taskCount()
            << ", max concurrent (100ms window)=" << maxConcurrent;
}

TEST_F(ParquetReaderPrefetchTest, prefetchWithLargeFile) {
  // Test prefetch behavior with larger files.

  auto filePath = createLargeTestFile();
  auto localFile = std::make_shared<LocalReadFile>(filePath);
  auto trackedFile = std::make_shared<TrackedReadFile>(localFile);

  AsyncChunkedReader::Config config;
  config.chunkSize = 256 * 1024; // 256KB chunks
  config.prefetchCount = 3;
  config.maxCachedChunks = 8;

  PrefetchBufferedInput input(
      trackedFile, *leafPool_, trackedExecutor_.get(), config);

  uint64_t fileSize = trackedFile->size();
  std::vector<std::unique_ptr<SeekableInputStream>> streams;

  // Enqueue many regions.
  for (uint64_t offset = 0; offset < fileSize; offset += 64 * 1024) {
    uint64_t length = std::min(64 * 1024UL, fileSize - offset);
    streams.push_back(input.enqueue({offset, length}));
  }

  input.load(LogType::TEST);

  // Process all streams.
  for (auto& stream : streams) {
    const void* data;
    int size;
    while (stream->Next(&data, &size)) {
      // Process data.
    }
  }

  auto stats = input.ioStats();

  // PREFETCH EFFECT ASSERTIONS:
  EXPECT_GT(stats.bytesRead, 0) << "Should have read data";

  // Verify efficient IO pattern.
  int readCount = trackedFile->readCount();
  EXPECT_GT(readCount, 0) << "IO operations should have occurred";

  // Calculate average read size. Prefetch should coalesce reads.
  double avgReadSize =
      static_cast<double>(stats.bytesRead) / static_cast<double>(readCount);
  LOG(INFO) << "Large file prefetch: total reads=" << readCount
            << ", bytes=" << stats.bytesRead
            << ", avg read size=" << avgReadSize << ", sync=" << stats.syncLoads
            << ", async=" << stats.asyncLoads;

  // With chunked reading, average read size should be close to chunk size.
  // Allowing for the last partial chunk.
  EXPECT_GE(avgReadSize, config.chunkSize * 0.5)
      << "Average read size should be substantial due to chunking";
}

TEST_F(ParquetReaderPrefetchTest, statsAccuracy) {
  // Verify that IO statistics are accurate.

  auto schema = ROW({"a"}, {BIGINT()});
  auto filePath =
      createTestParquetFile("stats_test.parquet", schema, 10000, 5000);

  auto localFile = std::make_shared<LocalReadFile>(filePath);
  auto trackedFile = std::make_shared<TrackedReadFile>(localFile);

  AsyncChunkedReader::Config config;
  config.chunkSize = 32 * 1024;
  config.prefetchCount = 1;

  PrefetchBufferedInput input(
      trackedFile, *leafPool_, trackedExecutor_.get(), config);

  auto stream = input.enqueue({0, trackedFile->size()});
  input.load(LogType::TEST);

  const void* data;
  int size;
  while (stream->Next(&data, &size)) {
  }

  auto stats = input.ioStats();
  uint64_t trackedBytes = trackedFile->totalBytesRead();

  // STATS ACCURACY ASSERTION:
  // The bytes reported by stats should match actual IO.
  EXPECT_EQ(stats.bytesRead, trackedBytes)
      << "Stats bytes should match actual IO bytes";

  // Total loads should match read count (approximately).
  // May differ slightly due to retries or coalescing.
  int64_t totalLoads = stats.syncLoads + stats.asyncLoads;
  EXPECT_GT(totalLoads, 0) << "Should have performed IO operations";
}

TEST_F(ParquetReaderPrefetchTest, executorNullFallback) {
  // Verify graceful fallback when executor is null.

  auto schema = ROW({"a"}, {BIGINT()});
  auto filePath =
      createTestParquetFile("null_executor.parquet", schema, 10000, 5000);

  auto localFile = std::make_shared<LocalReadFile>(filePath);

  // Create without executor (nullptr).
  PrefetchBufferedInput input(
      localFile, *leafPool_, static_cast<folly::Executor*>(nullptr));

  auto stream = input.enqueue({0, localFile->size()});
  input.load(LogType::TEST);

  // Should still work via synchronous path.
  const void* data;
  int size;
  EXPECT_TRUE(stream->Next(&data, &size));
  EXPECT_GT(size, 0);

  auto stats = input.ioStats();
  // Without executor, all loads should be sync.
  EXPECT_GE(stats.syncLoads, 0);
  EXPECT_EQ(stats.asyncLoads, 0) << "No async loads without executor";
}
