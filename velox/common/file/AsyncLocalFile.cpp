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

#include "velox/common/file/AsyncLocalFile.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Fs.h"

#include <fcntl.h>
#include <folly/String.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <unistd.h>

namespace facebook::velox {

// ============================================================================
// Utility Functions
// ============================================================================

bool isIoUringAvailable() {
#ifdef VELOX_ENABLE_IO_URING
  // Try to initialize a minimal io_uring to test availability
  struct io_uring ring;
  int ret = io_uring_queue_init(1, &ring, 0);
  if (ret == 0) {
    io_uring_queue_exit(&ring);
    return true;
  }
  return false;
#else
  return false;
#endif
}

std::unique_ptr<ReadFile> openLocalReadFile(
    std::string_view path,
    bool useIoUring) {
#ifdef VELOX_ENABLE_IO_URING
  if (useIoUring && isIoUringAvailable()) {
    return std::make_unique<AsyncLocalReadFile>(path);
  }
#endif
  // Fallback to LocalReadFile (defined in File.cpp)
  // For now, we create AsyncLocalReadFile which falls back internally
  VELOX_NYI("LocalReadFile fallback - please use filesystems API");
}

std::unique_ptr<WriteFile> openLocalWriteFile(
    std::string_view path,
    bool shouldCreateParentDirectories,
    bool shouldThrowOnFileAlreadyExists,
    bool useIoUring) {
#ifdef VELOX_ENABLE_IO_URING
  if (useIoUring && isIoUringAvailable()) {
    return std::make_unique<AsyncLocalWriteFile>(
        path, shouldCreateParentDirectories, shouldThrowOnFileAlreadyExists);
  }
#endif
  VELOX_NYI("LocalWriteFile fallback - please use filesystems API");
}

#ifdef VELOX_ENABLE_IO_URING

// ============================================================================
// WriteBuffers Implementation - Following Bolt's Design
// ============================================================================

WriteBuffers::WriteBuffers(int32_t capacity)
    : capacity_(capacity), buffers_(capacity), mutexes_(capacity) {
  LOG(INFO) << "WriteBuffers initialized with " << capacity_ << " slots";
}

WriteBuffers::~WriteBuffers() {
  // Buffers are automatically freed by unique_ptr
}

folly::IOBuf* WriteBuffers::add(
    std::unique_ptr<folly::IOBuf>& buf,
    int taskId,
    const std::unique_ptr<WriteFile>& file) {
  int idx = taskId % capacity_;
  std::lock_guard<std::mutex> lock(mutexes_[idx]);

  // If slot is occupied, wait for the previous task to complete.
  // Following Bolt's logic: we use round-robin, so the taskId to wait is
  // taskId - capacity_
  if (buffers_[idx] != nullptr) {
    VELOX_CHECK_GE(taskId, capacity_, "WriteBuffers: taskId out of index");

    // Wait for the previous write on this slot to complete
    auto ret = file->waitForComplete(taskId - capacity_, buffers_);
    VELOX_CHECK(
        ret, "Error occurred when waiting for io_uring write to complete");
  }

  buffers_[idx] = std::move(buf);
  return buffers_[idx].get();
}

void WriteBuffers::clear(std::unique_ptr<WriteFile>& file) {
  // Wait for all pending writes to complete
  auto ret = file->waitForCompleteAll();
  VELOX_CHECK(
      ret, "Error occurred when waiting for io_uring write to complete");

  for (int i = 0; i < capacity_; ++i) {
    std::lock_guard<std::mutex> lock(mutexes_[i]);
    if (buffers_[i] != nullptr) {
      buffers_[i].reset();
    }
  }
}

// ============================================================================
// AsyncLocalReadFile Implementation - Following Bolt's Design
// ============================================================================

AsyncLocalReadFile::AsyncLocalReadFile(std::string_view path, int32_t ringDepth)
    : path_(path) {
  // Open file
  fd_ = open(path_.c_str(), O_RDONLY);
  if (fd_ < 0) {
    if (errno == ENOENT) {
      VELOX_FILE_NOT_FOUND_ERROR("No such file or directory: {}", path);
    } else {
      VELOX_FAIL(
          "open failure in AsyncLocalReadFile constructor, {} {} {}.",
          fd_,
          path,
          folly::errnoStr(errno));
    }
  }

  // Get file size
  const off_t ret = lseek(fd_, 0, SEEK_END);
  VELOX_CHECK_GE(
      ret,
      0,
      "lseek failure in AsyncLocalReadFile constructor, {} {} {}.",
      ret,
      path,
      folly::errnoStr(errno));
  size_ = ret;

  // Initialize io_uring
  int initRet = io_uring_queue_init(ringDepth, &ring_, 0);
  if (initRet == 0) {
    uringEnabled_ = true;
    LOG(INFO) << "AsyncLocalReadFile: io_uring enabled for " << path
              << ", ring depth=" << ringDepth;
  } else {
    LOG(WARNING) << "AsyncLocalReadFile: io_uring_queue_init failed: "
                 << folly::errnoStr(-initRet) << ". Using sync fallback.";
    uringEnabled_ = false;
  }
}

AsyncLocalReadFile::~AsyncLocalReadFile() {
  // Wait for any pending reads
  if (pendingReads_ > 0) {
    try {
      const_cast<AsyncLocalReadFile*>(this)->waitForComplete();
    } catch (const std::exception& ex) {
      LOG(WARNING) << "Error waiting for pending reads in destructor: "
                   << ex.what();
    }
  }

  if (uringEnabled_) {
    io_uring_queue_exit(&ring_);
  }

  if (fd_ >= 0) {
    const int ret = close(fd_);
    if (ret < 0) {
      LOG(WARNING) << "close failure in AsyncLocalReadFile destructor: " << ret
                   << ", " << folly::errnoStr(errno);
    }
  }
}

std::string_view AsyncLocalReadFile::pread(
    uint64_t offset,
    uint64_t length,
    void* buf,
    const FileStorageContext& /*fileStorageContext*/) const {
  bytesRead_ += length;

  if (uringEnabled_) {
    // Use io_uring for the read
    auto* self = const_cast<AsyncLocalReadFile*>(this);
    if (self->submitRead(offset, length, buf)) {
      self->waitForComplete();
    } else {
      // Fallback to sync read if submission fails
      auto bytesRead = ::pread(fd_, buf, length, offset);
      VELOX_CHECK_EQ(
          bytesRead,
          length,
          "pread failure in AsyncLocalReadFile::pread: {} vs {}",
          bytesRead,
          length);
    }
  } else {
    // Sync fallback
    auto bytesRead = ::pread(fd_, buf, length, offset);
    VELOX_CHECK_EQ(
        bytesRead,
        length,
        "pread failure in AsyncLocalReadFile::pread: {} vs {}",
        bytesRead,
        length);
  }

  return {static_cast<char*>(buf), length};
}

uint64_t AsyncLocalReadFile::preadv(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers,
    const FileStorageContext& /*fileStorageContext*/) const {
  uint64_t totalRead = 0;

  if (!uringEnabled_) {
    // Sync fallback
    for (const auto& buffer : buffers) {
      if (buffer.data() != nullptr) {
        auto bytesRead = ::pread(fd_, buffer.data(), buffer.size(), offset);
        if (bytesRead > 0) {
          totalRead += bytesRead;
          bytesRead_ += bytesRead;
        }
      }
      offset += buffer.size();
    }
    return totalRead;
  }

  // Submit all reads using io_uring
  auto* self = const_cast<AsyncLocalReadFile*>(this);
  for (const auto& buffer : buffers) {
    if (buffer.data() != nullptr) {
      self->submitRead(offset, buffer.size(), buffer.data());
    }
    offset += buffer.size();
  }

  return self->waitForComplete();
}

folly::SemiFuture<uint64_t> AsyncLocalReadFile::preadvAsync(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers,
    const FileStorageContext& /*fileStorageContext*/) const {
  if (!uringEnabled_) {
    // Sync fallback wrapped in SemiFuture
    try {
      uint64_t totalRead = 0;
      for (const auto& buffer : buffers) {
        if (buffer.data() != nullptr) {
          auto bytesRead = ::pread(fd_, buffer.data(), buffer.size(), offset);
          if (bytesRead > 0) {
            totalRead += bytesRead;
            bytesRead_ += bytesRead;
          }
        }
        offset += buffer.size();
      }
      return folly::SemiFuture<uint64_t>(totalRead);
    } catch (const std::exception& e) {
      return folly::makeSemiFuture<uint64_t>(e);
    }
  }

  // Submit reads to io_uring (non-blocking)
  auto* self = const_cast<AsyncLocalReadFile*>(this);
  uint64_t totalSize = 0;
  for (const auto& buffer : buffers) {
    if (buffer.data() != nullptr) {
      if (self->submitRead(offset, buffer.size(), buffer.data())) {
        totalSize += buffer.size();
      } else {
        // Submission failed, do sync read
        auto bytesRead = ::pread(fd_, buffer.data(), buffer.size(), offset);
        if (bytesRead > 0) {
          bytesRead_ += bytesRead;
          totalSize += bytesRead;
        }
      }
    }
    offset += buffer.size();
  }

  // Return a deferred SemiFuture - true async behavior
  // The wait happens when the Future is consumed
  return folly::makeSemiFuture().deferValue(
      [self, totalSize](folly::Unit) -> uint64_t {
        self->waitForComplete();
        return totalSize;
      });
}

uint64_t AsyncLocalReadFile::memoryUsage() const {
  return sizeof(*this);
}

bool AsyncLocalReadFile::submitRead(
    uint64_t offset,
    uint64_t length,
    void* buf) {
  if (!uringEnabled_) {
    return false;
  }

  std::lock_guard<std::mutex> lock(mutex_);

  // Following Bolt's design: split large reads into 128KB chunks
  // This enables better parallelism on NVMe drives
  const size_t readBlockSize = kDefaultUringReadBlockSize;
  size_t buffOffset = 0;
  size_t remainingBytes = length;

  while (remainingBytes > 0) {
    size_t bytesToRead = std::min(readBlockSize, remainingBytes);

    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      LOG(WARNING) << "io_uring submission queue full";
      if (buffOffset == 0) {
        return false; // No requests submitted
      }
      break; // Some submitted, continue with what we have
    }

    io_uring_prep_read(
        sqe,
        fd_,
        static_cast<char*>(buf) + buffOffset,
        bytesToRead,
        offset + buffOffset);
    io_uring_sqe_set_data(sqe, nullptr); // We don't track individual chunks

    buffOffset += bytesToRead;
    remainingBytes -= bytesToRead;
    ++pendingReads_;
  }

  // Batch submit all requests at once (single syscall)
  int ret = io_uring_submit(&ring_);
  if (ret < 0) {
    LOG(ERROR) << "io_uring_submit failed: " << folly::errnoStr(-ret);
    return false;
  }

  bytesRead_ += length;
  return true;
}

uint64_t AsyncLocalReadFile::waitForComplete() {
  if (!uringEnabled_ || pendingReads_ == 0) {
    return 0;
  }

  std::lock_guard<std::mutex> lock(mutex_);

  uint64_t totalBytesRead = 0;
  while (pendingReads_ > 0) {
    struct io_uring_cqe* cqe;
    int ret = io_uring_wait_cqe(&ring_, &cqe);
    if (ret < 0) {
      VELOX_FAIL("io_uring_wait_cqe failed: {}", folly::errnoStr(-ret));
    }

    if (cqe->res < 0) {
      io_uring_cqe_seen(&ring_, cqe);
      VELOX_FAIL("io_uring read failed: {}", folly::errnoStr(-cqe->res));
    }

    totalBytesRead += cqe->res;
    io_uring_cqe_seen(&ring_, cqe);
    --pendingReads_;
  }

  return totalBytesRead;
}

// ============================================================================
// AsyncLocalWriteFile Implementation - Following Bolt's Design
// ============================================================================

AsyncLocalWriteFile::AsyncLocalWriteFile(
    std::string_view path,
    bool shouldCreateParentDirectories,
    bool shouldThrowOnFileAlreadyExists,
    int32_t ringDepth)
    : path_(path) {
  // Create parent directories if needed
  const auto dir = fs::path(path_).parent_path();
  if (shouldCreateParentDirectories && !dir.empty() && !fs::exists(dir)) {
    VELOX_CHECK(
        common::generateFileDirectory(dir.c_str()),
        "Failed to generate file directory");
  }

  // Check if file exists
  if (shouldThrowOnFileAlreadyExists && fs::exists(path_)) {
    VELOX_FAIL("File already exists: {}", path);
  }

  // Open file for writing
  int flags = O_WRONLY | O_CREAT | O_TRUNC;
  fd_ = open(path_.c_str(), flags, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  if (fd_ < 0) {
    VELOX_FAIL(
        "open failure in AsyncLocalWriteFile constructor: {} {}",
        path,
        folly::errnoStr(errno));
  }

  // Initialize io_uring
  int initRet = io_uring_queue_init(ringDepth, &ring_, 0);
  if (initRet == 0) {
    uringEnabled_ = true;
    LOG(INFO) << "AsyncLocalWriteFile: io_uring enabled for " << path
              << ", ring depth=" << ringDepth;
  } else {
    LOG(WARNING) << "AsyncLocalWriteFile: io_uring_queue_init failed: "
                 << folly::errnoStr(-initRet) << ". Using sync fallback.";
    uringEnabled_ = false;
  }
}

AsyncLocalWriteFile::~AsyncLocalWriteFile() {
  if (!closed_) {
    try {
      close();
    } catch (const std::exception& ex) {
      LOG(WARNING) << "Error closing file in destructor: " << ex.what();
    }
  }
}

void AsyncLocalWriteFile::append(std::string_view data) {
  if (closed_) {
    VELOX_FAIL("Cannot write to closed file");
  }

  if (uringEnabled_) {
    // Use async path - but for simplicity, we do sync here
    // In Bolt's full implementation, this would use WriteBuffers
    auto bytesWritten = ::pwrite(fd_, data.data(), data.size(), writeOffset_);
    VELOX_CHECK_EQ(
        bytesWritten,
        data.size(),
        "pwrite failure in AsyncLocalWriteFile::append");
    writeOffset_ += data.size();
    size_ += data.size();
  } else {
    // Sync write
    auto bytesWritten = ::pwrite(fd_, data.data(), data.size(), writeOffset_);
    VELOX_CHECK_EQ(
        bytesWritten,
        data.size(),
        "pwrite failure in AsyncLocalWriteFile::append");
    writeOffset_ += data.size();
    size_ += data.size();
  }
}

void AsyncLocalWriteFile::append(std::unique_ptr<folly::IOBuf> data) {
  if (closed_) {
    VELOX_FAIL("Cannot write to closed file");
  }

  // Handle IOBuf chain
  for (auto& buf : *data) {
    append(
        std::string_view(
            reinterpret_cast<const char*>(buf.data()), buf.size()));
  }
}

void AsyncLocalWriteFile::flush() {
  if (uringEnabled_ && pendingWrites_ > 0) {
    waitForCompleteAll();
  }
  if (fd_ >= 0) {
    fsync(fd_);
  }
}

void AsyncLocalWriteFile::close() {
  if (closed_) {
    return;
  }

  // Wait for pending writes
  if (uringEnabled_) {
    waitForCompleteAll();
    io_uring_queue_exit(&ring_);
  }

  if (fd_ >= 0) {
    fsync(fd_);
    ::close(fd_);
    fd_ = -1;
  }

  closed_ = true;
}

void AsyncLocalWriteFile::submitWrite(folly::IOBuf* data, int taskId) {
  if (!uringEnabled_ || closed_) {
    VELOX_FAIL("submitWrite called on closed or non-uring file");
  }

  std::lock_guard<std::mutex> lock(mutex_);

  const auto length = data->length();

  // Get SQE and prepare write
  struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  if (sqe == nullptr) {
    VELOX_FAIL("io_uring submission queue full");
  }

  sqe->user_data = taskId;
  io_uring_prep_write(sqe, fd_, data->data(), length, writeOffset_);

  // Submit
  int ret = io_uring_submit(&ring_);
  if (ret < 0) {
    VELOX_FAIL("io_uring_submit failed: {}", folly::errnoStr(-ret));
  }

  writeOffset_ += length;
  size_ += length;
  ++pendingWrites_;
}

bool AsyncLocalWriteFile::waitForComplete(
    int targetTaskId,
    std::vector<std::unique_ptr<folly::IOBuf>>& buffers) {
  if (!uringEnabled_ || pendingWrites_ == 0) {
    return true;
  }

  std::lock_guard<std::mutex> lock(mutex_);

  int completed = -1;
  while (completed != targetTaskId) {
    struct io_uring_cqe* cqe;
    int ret = io_uring_wait_cqe(&ring_, &cqe);
    if (ret < 0) {
      LOG(ERROR) << "io_uring_wait_cqe failed: " << folly::errnoStr(-ret);
      return false;
    }

    if (cqe->res < 0) {
      LOG(ERROR) << "io_uring write failed for task " << cqe->user_data << ": "
                 << folly::errnoStr(-cqe->res);
      io_uring_cqe_seen(&ring_, cqe);
      return false;
    }

    --pendingWrites_;
    completed = static_cast<int>(cqe->user_data);
    int idx = completed % buffers.size();
    buffers[idx].reset();
    io_uring_cqe_seen(&ring_, cqe);
  }

  return true;
}

bool AsyncLocalWriteFile::waitForCompleteAll() {
  if (!uringEnabled_ || pendingWrites_ == 0) {
    return true;
  }

  std::lock_guard<std::mutex> lock(mutex_);

  while (pendingWrites_ > 0) {
    struct io_uring_cqe* cqe;
    int ret = io_uring_wait_cqe(&ring_, &cqe);
    if (ret < 0) {
      LOG(ERROR) << "io_uring_wait_cqe failed: " << folly::errnoStr(-ret);
      return false;
    }

    if (cqe->res < 0) {
      LOG(ERROR) << "io_uring write failed for task " << cqe->user_data << ": "
                 << folly::errnoStr(-cqe->res);
    }

    --pendingWrites_;
    io_uring_cqe_seen(&ring_, cqe);
  }

  return true;
}

#endif // VELOX_ENABLE_IO_URING

} // namespace facebook::velox
