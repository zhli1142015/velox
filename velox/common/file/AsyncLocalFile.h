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

#include "velox/common/file/File.h"

#include <deque>
#include <memory>
#include <mutex>
#include <vector>

#ifdef VELOX_ENABLE_IO_URING
#include <liburing.h>
#endif

namespace facebook::velox {

/// Default ring depth for io_uring operations (matches Bolt's design).
constexpr int32_t kDefaultUringRingDepth = 64;

/// Default block size for read operations (128KB, matches Bolt's design).
/// Large reads are split into multiple 128KB chunks for better NVMe
/// parallelism.
constexpr size_t kDefaultUringReadBlockSize = 128 * 1024;

#ifdef VELOX_ENABLE_IO_URING

/// Manages a pool of IOBuf buffers for async write operations.
/// Following Bolt's WriteBuffers design pattern.
///
/// Key design points:
/// - Capacity matches io_uring ring depth (64 slots)
/// - Each slot holds an IOBuf (no data copying, stores entire IOBuf)
/// - When slot is reused before completion, waits for previous write
/// - IOBuf lifetime managed until I/O completion
class WriteBuffers {
 public:
  /// Creates a buffer pool with the specified capacity.
  /// @param capacity Maximum concurrent buffers (default:
  /// kDefaultUringRingDepth)
  explicit WriteBuffers(int32_t capacity = kDefaultUringRingDepth);

  ~WriteBuffers();

  /// Adds an IOBuf to a slot, waiting if slot is occupied.
  /// Following Bolt's add() pattern - stores IOBuf until write completes.
  /// @param buf The IOBuf to store (ownership transferred)
  /// @param taskId The task ID for this write
  /// @param file The file to wait on if slot is occupied
  /// @return Pointer to the stored IOBuf's data
  folly::IOBuf* add(
      std::unique_ptr<folly::IOBuf>& buf,
      int taskId,
      const std::unique_ptr<WriteFile>& file);

  /// Waits for all pending writes to complete and releases all buffers.
  /// @param file The file to wait on
  void clear(std::unique_ptr<WriteFile>& file);

  /// Returns capacity (number of slots).
  int32_t capacity() const {
    return capacity_;
  }

  /// Returns the buffers vector (for waitForComplete to release).
  std::vector<std::unique_ptr<folly::IOBuf>>& buffers() {
    return buffers_;
  }

 private:
  const int32_t capacity_;
  std::vector<std::unique_ptr<folly::IOBuf>> buffers_;
  std::vector<std::mutex> mutexes_;
};

/// Async local file reader using io_uring for non-blocking reads.
/// Following Bolt's AsyncLocalReadFile design pattern.
///
/// Key features:
/// - submitRead() for async read submission (non-blocking)
/// - waitForComplete() to wait for pending reads (blocking)
/// - 128KB chunking for large reads
/// - Supports dual-buffer prefetch pattern
class AsyncLocalReadFile final : public ReadFile {
 public:
  /// Opens a file for async reading.
  /// @param path File path to open
  /// @param ringDepth io_uring ring depth (default: kDefaultUringRingDepth)
  explicit AsyncLocalReadFile(
      std::string_view path,
      int32_t ringDepth = kDefaultUringRingDepth);

  ~AsyncLocalReadFile() override;

  // ReadFile interface
  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buf,
      const FileStorageContext& fileStorageContext = {}) const override;

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers,
      const FileStorageContext& fileStorageContext = {}) const override;

  folly::SemiFuture<uint64_t> preadvAsync(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers,
      const FileStorageContext& fileStorageContext = {}) const override;

  bool hasPreadvAsync() const override {
    return uringEnabled_;
  }

  uint64_t size() const override {
    return size_;
  }

  uint64_t memoryUsage() const override;

  bool shouldCoalesce() const override {
    return false;
  }

  std::string getName() const override {
    return path_.empty() ? "<AsyncLocalReadFile>" : path_;
  }

  uint64_t getNaturalReadSize() const override {
    return kDefaultUringReadBlockSize;
  }

  // Bolt-style direct async API
  bool uringEnabled() const override {
    return uringEnabled_;
  }

  bool submitRead(uint64_t offset, uint64_t length, void* buf) override;

  uint64_t waitForComplete() override;

  int32_t pendingReads() const override {
    return pendingReads_;
  }

 private:
  std::string path_;
  int32_t fd_{-1};
  uint64_t size_{0};
  bool uringEnabled_{false};
  mutable struct io_uring ring_;
  mutable int32_t pendingReads_{0};
  mutable std::mutex mutex_;
};

/// Async local file writer using io_uring for non-blocking writes.
/// Following Bolt's AsyncLocalWriteFile design pattern.
///
/// Key features:
/// - submitWrite() for async write submission (non-blocking)
/// - waitForComplete()/waitForCompleteAll() for completion
/// - WriteBuffers managed externally by caller (following Bolt design)
class AsyncLocalWriteFile final : public WriteFile {
 public:
  /// Opens a file for async writing.
  /// @param path File path to create/open
  /// @param shouldCreateParentDirectories Create parent dirs if needed
  /// @param shouldThrowOnFileAlreadyExists Throw if file exists
  /// @param ringDepth io_uring ring depth
  explicit AsyncLocalWriteFile(
      std::string_view path,
      bool shouldCreateParentDirectories = false,
      bool shouldThrowOnFileAlreadyExists = true,
      int32_t ringDepth = kDefaultUringRingDepth);

  ~AsyncLocalWriteFile() override;

  // WriteFile interface
  void append(std::string_view data) override;
  void append(std::unique_ptr<folly::IOBuf> data) override;
  void flush() override;
  void close() override;

  uint64_t size() const override {
    return size_;
  }

  const std::string getName() const override {
    return path_;
  }

  // Bolt-style direct async API
  bool uringEnabled() const override {
    return uringEnabled_;
  }

  /// Submits an async write using io_uring.
  /// Following Bolt's submitWrite pattern - receives IOBuf pointer.
  /// @param data The IOBuf to write (caller manages lifetime via WriteBuffers)
  /// @param taskId The task ID for tracking completion
  void submitWrite(folly::IOBuf* data, int taskId) override;

  /// Wait for a specific task to complete.
  /// Following Bolt's design - receives buffers to release completed ones.
  bool waitForComplete(
      int taskId,
      std::vector<std::unique_ptr<folly::IOBuf>>& buffers) override;

  bool waitForCompleteAll() override;

  int32_t pendingWrites() const override {
    return pendingWrites_;
  }

 private:
  std::string path_;
  int32_t fd_{-1};
  uint64_t size_{0};
  uint64_t writeOffset_{0};
  bool closed_{false};
  bool uringEnabled_{false};
  struct io_uring ring_;
  int32_t pendingWrites_{0};
  mutable std::mutex mutex_;
};

#endif // VELOX_ENABLE_IO_URING

/// Returns true if io_uring is available at runtime.
bool isIoUringAvailable();

/// Creates a ReadFile - uses AsyncLocalReadFile if io_uring enabled.
/// @param path File path to open
/// @param useIoUring Whether to use io_uring if available
std::unique_ptr<ReadFile> openLocalReadFile(
    std::string_view path,
    bool useIoUring = false);

/// Creates a WriteFile - uses AsyncLocalWriteFile if io_uring enabled.
/// @param path File path to create/open
/// @param shouldCreateParentDirectories Create parent dirs if needed
/// @param shouldThrowOnFileAlreadyExists Throw if file exists
/// @param useIoUring Whether to use io_uring if available
std::unique_ptr<WriteFile> openLocalWriteFile(
    std::string_view path,
    bool shouldCreateParentDirectories = false,
    bool shouldThrowOnFileAlreadyExists = true,
    bool useIoUring = false);

} // namespace facebook::velox
