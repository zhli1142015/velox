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

#include "velox/exec/SpillFile.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/time/Timer.h"
#include "velox/exec/SpillRowSerde.h"
#include "velox/serializers/SerializedPageFile.h"

#ifdef __linux__
#include <lz4.h>
#include <zstd.h>
#endif

namespace facebook::velox::exec {
namespace {
// Spilling currently uses the default PrestoSerializer which by default
// serializes timestamp with millisecond precision to maintain compatibility
// with presto. Since velox's native timestamp implementation supports
// nanosecond precision, we use this serde option to ensure the serializer
// preserves precision.
static const bool kDefaultUseLosslessTimestamp = true;
} // namespace

SpillWriter::SpillWriter(
    const RowTypePtr& type,
    const std::vector<SpillSortKey>& sortingKeys,
    common::CompressionKind compressionKind,
    const std::string& pathPrefix,
    uint64_t targetFileSize,
    uint64_t writeBufferSize,
    const std::string& fileCreateConfig,
    const common::UpdateAndCheckSpillLimitCB& updateAndCheckSpillLimitCb,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* stats,
    bool spillUringEnabled)
    : serializer::SerializedPageFileWriter(
          pathPrefix,
          targetFileSize,
          writeBufferSize,
          fileCreateConfig,
          std::make_unique<
              serializer::presto::PrestoVectorSerde::PrestoOptions>(
              kDefaultUseLosslessTimestamp,
              compressionKind,
              0.8,
              /*_nullsFirst=*/true),
          getNamedVectorSerde(VectorSerde::Kind::kPresto),
          pool,
          spillUringEnabled),
      type_(type),
      sortingKeys_(sortingKeys),
      stats_(stats),
      updateAndCheckLimitCb_(updateAndCheckSpillLimitCb) {}

void SpillWriter::updateAppendStats(
    uint64_t numRows,
    uint64_t serializationTimeNs) {
  auto statsLocked = stats_->wlock();
  statsLocked->spilledRows += numRows;
  statsLocked->spillSerializationTimeNanos += serializationTimeNs;
  common::updateGlobalSpillAppendStats(numRows, serializationTimeNs);
}

void SpillWriter::updateWriteStats(
    uint64_t spilledBytes,
    uint64_t flushTimeNs,
    uint64_t fileWriteTimeNs) {
  auto statsLocked = stats_->wlock();
  statsLocked->spilledBytes += spilledBytes;
  statsLocked->spillFlushTimeNanos += flushTimeNs;
  statsLocked->spillWriteTimeNanos += fileWriteTimeNs;
  ++statsLocked->spillWrites;
  common::updateGlobalSpillWriteStats(
      spilledBytes, flushTimeNs, fileWriteTimeNs);
  updateAndCheckLimitCb_(spilledBytes);
}

void SpillWriter::updateFileStats(
    const serializer::SerializedPageFile::FileInfo& file) {
  ++stats_->wlock()->spilledFiles;
  addThreadLocalRuntimeStat(
      "spillFileSize", RuntimeCounter(file.size, RuntimeCounter::Unit::kBytes));
  common::incrementGlobalSpilledFiles();
}

SpillFiles SpillWriter::finish() {
  const auto serializedPageFiles =
      serializer::SerializedPageFileWriter::finish();
  SpillFiles spillFiles;
  spillFiles.reserve(serializedPageFiles.size());
  for (const auto& fileInfo : serializedPageFiles) {
    spillFiles.push_back(
        SpillFileInfo{
            .id = fileInfo.id,
            .type = type_,
            .path = fileInfo.path,
            .size = fileInfo.size,
            .sortingKeys = sortingKeys_,
            .compressionKind = serdeOptions_->compressionKind});
  }
  return spillFiles;
}

std::vector<std::string> SpillWriter::testingSpilledFilePaths() const {
  checkNotFinished();

  std::vector<std::string> spilledFilePaths;
  spilledFilePaths.reserve(
      finishedFiles_.size() + (currentFile_ != nullptr ? 1 : 0));
  for (auto& file : finishedFiles_) {
    spilledFilePaths.push_back(file.path);
  }
  if (currentFile_ != nullptr) {
    spilledFilePaths.push_back(currentFile_->path());
  }
  return spilledFilePaths;
}

std::vector<uint32_t> SpillWriter::testingSpilledFileIds() const {
  checkNotFinished();

  std::vector<uint32_t> fileIds;
  for (auto& file : finishedFiles_) {
    fileIds.push_back(file.id);
  }
  if (currentFile_ != nullptr) {
    fileIds.push_back(currentFile_->id());
  }
  return fileIds;
}

std::unique_ptr<SpillReadFile> SpillReadFile::create(
    const SpillFileInfo& fileInfo,
    uint64_t bufferSize,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* stats,
    bool spillUringEnabled) {
  return std::unique_ptr<SpillReadFile>(new SpillReadFile(
      fileInfo.id,
      fileInfo.path,
      fileInfo.size,
      bufferSize,
      fileInfo.type,
      fileInfo.sortingKeys,
      fileInfo.compressionKind,
      pool,
      stats,
      spillUringEnabled));
}

SpillReadFile::SpillReadFile(
    uint32_t id,
    const std::string& path,
    uint64_t size,
    uint64_t bufferSize,
    const RowTypePtr& type,
    const std::vector<SpillSortKey>& sortingKeys,
    common::CompressionKind compressionKind,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* stats,
    bool spillUringEnabled)
    : serializer::SerializedPageFileReader(
          path,
          bufferSize,
          type,
          getNamedVectorSerde(VectorSerde::Kind::kPresto),
          std::make_unique<
              serializer::presto::PrestoVectorSerde::PrestoOptions>(
              kDefaultUseLosslessTimestamp,
              compressionKind,
              0.8,
              /*_nullsFirst=*/true),
          pool,
          spillUringEnabled),
      id_(id),
      path_(path),
      size_(size),
      sortingKeys_(sortingKeys),
      stats_(stats) {}

void SpillReadFile::updateFinalStats() {
  VELOX_CHECK(input_->atEnd());
  const auto readStats = input_->stats();
  common::updateGlobalSpillReadStats(
      readStats.numReads, readStats.readBytes, readStats.readTimeNs);
  auto lockedSpillStats = stats_->wlock();
  lockedSpillStats->spillReads += readStats.numReads;
  lockedSpillStats->spillReadTimeNanos += readStats.readTimeNs;
  lockedSpillStats->spillReadBytes += readStats.readBytes;
};

void SpillReadFile::updateSerializationTimeStats(uint64_t timeNs) {
  stats_->wlock()->spillDeserializationTimeNanos += timeNs;
  common::updateGlobalSpillDeserializationTimeNs(timeNs);
};

// RowBasedSpillWriter implementation
namespace {
constexpr size_t kRowBasedBlockHeaderSize = sizeof(uint32_t) * 2;
constexpr size_t kDefaultRowBasedBufferSize = (1ULL << 20); // 1MB
} // namespace

RowBasedSpillWriter::RowBasedSpillWriter(
    const std::string& pathPrefix,
    uint64_t targetFileSize,
    uint64_t writeBufferSize,
    const std::string& fileCreateConfig,
    common::CompressionKind compressionKind,
    const RowFormatInfo& rowInfo,
    const common::UpdateAndCheckSpillLimitCB& updateAndCheckSpillLimitCb,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* stats,
    bool spillUringEnabled)
    : pathPrefix_(pathPrefix),
      targetFileSize_(targetFileSize),
      writeBufferSize_(
          writeBufferSize > 0 ? writeBufferSize : kDefaultRowBasedBufferSize),
      fileCreateConfig_(fileCreateConfig),
      compressionKind_(compressionKind),
      rowInfo_(rowInfo),
      updateAndCheckLimitCb_(updateAndCheckSpillLimitCb),
      pool_(pool),
      stats_(stats),
      spillUringEnabled_(spillUringEnabled) {}

RowBasedSpillWriter::~RowBasedSpillWriter() {
  if (!finished_ && currentFile_ != nullptr) {
    try {
      closeFile();
    } catch (...) {
      // Ignore errors during destruction.
    }
  }
}

WriteFile* RowBasedSpillWriter::ensureFile() {
  if (currentFile_ == nullptr) {
    const auto path =
        fmt::format("{}-{}.{}", pathPrefix_, currentFileId_, "rowspill");
    auto fs = filesystems::getFileSystem(path, nullptr);
    filesystems::FileOptions options;
    // Don't throw if file already exists - overwrite it
    options.shouldThrowOnFileAlreadyExists = false;
    options.values[filesystems::FileOptions::kFileCreateConfig.str()] =
        fileCreateConfig_;
    currentFile_ = fs->openFileForWrite(path, options);
    currentFileSize_ = 0;
    rowsInCurrentFile_ = 0;
  }
  return currentFile_.get();
}

void RowBasedSpillWriter::closeFile() {
  if (currentFile_ == nullptr) {
    return;
  }
  currentFile_->close();
  finishedFiles_.push_back(
      SpillFileInfo{
          .id = currentFileId_,
          .type = nullptr, // Row format doesn't need type for reading
          .path = currentFile_->getName(),
          .size = currentFileSize_,
          .sortingKeys = {}, // Will be set by caller if needed
          .compressionKind = compressionKind_});

  ++currentFileId_;
  currentFile_.reset();

  ++stats_->wlock()->spilledFiles;
  common::incrementGlobalSpilledFiles();
  addThreadLocalRuntimeStat(
      "spillFileSize",
      RuntimeCounter(currentFileSize_, RuntimeCounter::Unit::kBytes));
}

uint64_t RowBasedSpillWriter::write(
    const std::vector<char*, memory::StlAllocator<char*>>& rows) {
  VELOX_CHECK(!finished_, "Cannot write to finished RowBasedSpillWriter");

  if (rows.empty()) {
    return 0;
  }

  BufferPtr buffer = AlignedBuffer::allocate<char>(writeBufferSize_, pool_, 0);
  size_t writeBufferLimit =
      std::min(writeBufferSize_, static_cast<uint64_t>(buffer->capacity()));
  BufferPtr compressBuffer = nullptr;

  char* current = nullptr;
  const char *bufferStart = nullptr, *bufferEnd = nullptr;
  uint32_t* headerPtr = nullptr;
  uint64_t totalSize = 0;

  auto resetNewBufferState = [&]() {
    bufferStart = buffer->as<char>();
    headerPtr = reinterpret_cast<uint32_t*>(buffer->asMutable<char>());
    current = buffer->asMutable<char>() + kRowBasedBlockHeaderSize;
    bufferEnd = buffer->as<char>() + buffer->capacity();
  };

  resetNewBufferState();

  auto writeBuffer = [&]() {
    size_t bufferSize = current - bufferStart;
    size_t dataSize = bufferSize - kRowBasedBlockHeaderSize;
    totalSize += bufferSize;
    VELOX_CHECK_LE(bufferSize, static_cast<size_t>(bufferEnd - bufferStart));

    auto* file = ensureFile();
    VELOX_CHECK_NOT_NULL(file);

    const char* writeBuffer = nullptr;
    size_t writeBufferSize = 0;
    uint64_t writtenBytes{0}, writeTimeUs{0}, compressTimeUs{0};

#ifdef __linux__
    if (rowInfo_.enableCompression &&
        compressionKind_ != common::CompressionKind::CompressionKind_NONE) {
      // Compression mode
      size_t needSize = kRowBasedBlockHeaderSize +
          ((compressionKind_ == common::CompressionKind::CompressionKind_ZSTD)
               ? ZSTD_compressBound(dataSize)
               : LZ4_compressBound(dataSize));
      if (!compressBuffer || compressBuffer->capacity() < needSize) {
        compressBuffer = AlignedBuffer::allocate<char>(needSize, pool_);
      }

      {
        MicrosecondTimer timer(&compressTimeUs);
        uint32_t* compressedHeaderPtr =
            reinterpret_cast<uint32_t*>(compressBuffer->asMutable<char>());
        char* compressedStart =
            compressBuffer->asMutable<char>() + kRowBasedBlockHeaderSize;

        int ret = 0;
        if (compressionKind_ == common::CompressionKind::CompressionKind_ZSTD) {
          ret = ZSTD_compress(
              compressedStart,
              needSize - kRowBasedBlockHeaderSize,
              bufferStart + kRowBasedBlockHeaderSize,
              dataSize,
              3); // Compression level 3
        } else {
          ret = LZ4_compress_default(
              bufferStart + kRowBasedBlockHeaderSize,
              compressedStart,
              dataSize,
              needSize - kRowBasedBlockHeaderSize);
        }
        VELOX_CHECK_GT(ret, 0, "Compression failed");

        compressedHeaderPtr[0] = dataSize; // Original size
        compressedHeaderPtr[1] = ret; // Compressed size
        writeBufferSize = kRowBasedBlockHeaderSize + ret;
        writeBuffer = compressBuffer->as<char>();
      }
    } else
#endif
    {
      // No compression mode
      headerPtr[0] = dataSize;
      headerPtr[1] = dataSize; // compressedSize == dataSize
      writeBufferSize = bufferSize;
      writeBuffer = bufferStart;
    }

    {
      MicrosecondTimer timer(&writeTimeUs);
      file->append(std::string_view(writeBuffer, writeBufferSize));
      writtenBytes = writeBufferSize;
    }

    currentFileSize_ += writtenBytes;
    updateWriteStats(writtenBytes, compressTimeUs, writeTimeUs);
  };

  // Serialize all rows
  vector_size_t rowIndex = 0;
  vector_size_t needWriteRowCount = 0;

  while (rowIndex < rows.size()) {
    char* row = rows[rowIndex];
    int32_t rowSize = 0;

    if (rowInfo_.serialized) {
      rowSize = SpillRowSerde::rowSize(row, rowInfo_);
    } else {
      rowSize = rowInfo_.getRowSize(row);
    }

    if (current + rowSize - bufferStart > writeBufferLimit) {
      if (needWriteRowCount > 0) {
        writeBuffer();
      }
      auto sizeWithHeader = rowSize + kRowBasedBlockHeaderSize;
      if (sizeWithHeader > (bufferEnd - bufferStart)) {
        // Single row exceeds buffer size
        LOG(INFO) << "large row in row based spill, header + row size: "
                  << sizeWithHeader
                  << ", buffer capacity: " << buffer->capacity();
        buffer = AlignedBuffer::allocate<char>(sizeWithHeader, pool_, 0);
        writeBufferLimit = sizeWithHeader;
      }
      needWriteRowCount = 0;
      resetNewBufferState();
    }

    if (rowInfo_.serialized) {
      // Already serialized data - direct copy
      int currentRowSize = SpillRowSerde::rowSize(row, rowInfo_);
      VELOX_CHECK_LE(current + currentRowSize, bufferEnd);
      simd::memcpy(current, row, currentRowSize);
      current += currentRowSize;
    } else {
      // Serialize row data
      SpillRowSerde::serialize(row, current, bufferEnd, rowInfo_);
    }

    rowIndex++;
    needWriteRowCount++;
  }

  if (needWriteRowCount > 0) {
    writeBuffer();
  }

  rowsInCurrentFile_ += rows.size();

  // Check if we should close current file
  if (currentFileSize_ >= targetFileSize_) {
    closeFile();
  }

  // Update row stats
  {
    auto statsLocked = stats_->wlock();
    statsLocked->spilledRows += rows.size();
    common::updateGlobalSpillAppendStats(rows.size(), 0);
  }

  return totalSize;
}

void RowBasedSpillWriter::updateWriteStats(
    uint64_t spilledBytes,
    uint64_t compressTimeUs,
    uint64_t writeTimeUs) {
  auto statsLocked = stats_->wlock();
  statsLocked->spilledBytes += spilledBytes;
  // Convert microseconds to nanoseconds
  statsLocked->spillFlushTimeNanos += compressTimeUs * 1000;
  statsLocked->spillWriteTimeNanos += writeTimeUs * 1000;
  ++statsLocked->spillWrites;
  common::updateGlobalSpillWriteStats(
      spilledBytes, compressTimeUs * 1000, writeTimeUs * 1000);
  updateAndCheckLimitCb_(spilledBytes);
}

SpillFiles RowBasedSpillWriter::finish() {
  VELOX_CHECK(!finished_, "Already finished");
  if (currentFile_ != nullptr) {
    closeFile();
  }
  finished_ = true;
  return std::move(finishedFiles_);
}

std::vector<std::string> RowBasedSpillWriter::testingSpilledFilePaths() const {
  VELOX_CHECK(!finished_);

  std::vector<std::string> spilledFilePaths;
  spilledFilePaths.reserve(
      finishedFiles_.size() + (currentFile_ != nullptr ? 1 : 0));
  for (auto& file : finishedFiles_) {
    spilledFilePaths.push_back(file.path);
  }
  if (currentFile_ != nullptr) {
    spilledFilePaths.push_back(currentFile_->getName());
  }
  return spilledFilePaths;
}

std::vector<uint32_t> RowBasedSpillWriter::testingSpilledFileIds() const {
  VELOX_CHECK(!finished_);

  std::vector<uint32_t> fileIds;
  for (auto& file : finishedFiles_) {
    fileIds.push_back(file.id);
  }
  if (currentFile_ != nullptr) {
    fileIds.push_back(currentFileId_);
  }
  return fileIds;
}

// RowBasedSpillReadFile implementation
std::unique_ptr<RowBasedSpillReadFile> RowBasedSpillReadFile::create(
    const SpillFileInfo& fileInfo,
    const RowFormatInfo& rowInfo,
    uint64_t bufferSize,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* stats,
    bool spillUringEnabled) {
  return std::unique_ptr<RowBasedSpillReadFile>(new RowBasedSpillReadFile(
      fileInfo.id,
      fileInfo.path,
      fileInfo.size,
      fileInfo.type,
      fileInfo.sortingKeys,
      fileInfo.compressionKind,
      rowInfo,
      bufferSize,
      pool,
      stats,
      spillUringEnabled));
}

RowBasedSpillReadFile::RowBasedSpillReadFile(
    uint32_t id,
    const std::string& path,
    uint64_t size,
    const RowTypePtr& type,
    const std::vector<SpillSortKey>& sortingKeys,
    common::CompressionKind compressionKind,
    const RowFormatInfo& rowInfo,
    uint64_t bufferSize,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* stats,
    bool spillUringEnabled)
    : id_(id),
      path_(path),
      size_(size),
      type_(type),
      sortingKeys_(sortingKeys),
      compressionKind_(compressionKind),
      rowInfo_(rowInfo),
      pool_(pool),
      stats_(stats) {
  auto fs = filesystems::getFileSystem(path_, nullptr);
  auto readFile = fs->openFileForRead(path_);
  input_ = std::make_unique<common::FileInputStream>(
      std::move(readFile), bufferSize, pool_);
  rowBuffer_ = AlignedBuffer::allocate<char>(kDefaultRowBasedBufferSize, pool_);
}

uint32_t RowBasedSpillReadFile::nextBatch(std::vector<char*>& rows) {
  rows.clear();
  if (input_->atEnd()) {
    // Record final stats
    const auto readStats = input_->stats();
    common::updateGlobalSpillReadStats(
        readStats.numReads, readStats.readBytes, readStats.readTimeNs);
    auto lockedSpillStats = stats_->wlock();
    lockedSpillStats->spillReads += readStats.numReads;
    lockedSpillStats->spillReadTimeNanos += readStats.readTimeNs;
    lockedSpillStats->spillReadBytes += readStats.readBytes;
    return 0;
  }

  // Read header
  uint32_t bufferSize = input_->read<uint32_t>(); // Original data size
  uint32_t compressedSize = input_->read<uint32_t>(); // Compressed size

  // Ensure buffer has correct alignment
  uint32_t alignedBufferSize = bufferSize + rowInfo_.alignment;
  if (rowBuffer_->capacity() < alignedBufferSize) {
    rowBuffer_ = AlignedBuffer::allocate<char>(alignedBufferSize, pool_);
  }

  // Align start address
  char* rawBuffer = rowBuffer_->asMutable<char>();
  uintptr_t alignedPtr =
      (reinterpret_cast<uintptr_t>(rawBuffer) + rowInfo_.alignment - 1) &
      ~static_cast<uintptr_t>(rowInfo_.alignment - 1);
  char* current = reinterpret_cast<char*>(alignedPtr);

#ifdef __linux__
  if (rowInfo_.enableCompression &&
      compressionKind_ != common::CompressionKind::CompressionKind_NONE) {
    // Compressed mode: read compressed data then decompress
    if (!compressedBuffer_ || compressedBuffer_->capacity() < compressedSize) {
      compressedBuffer_ = AlignedBuffer::allocate<char>(compressedSize, pool_);
    }
    input_->readBytes(
        reinterpret_cast<uint8_t*>(compressedBuffer_->asMutable<char>()),
        compressedSize);

    MicrosecondTimer timer(&spillDecompressTimeUs_);
    if (compressionKind_ == common::CompressionKind::CompressionKind_ZSTD) {
      auto ret = ZSTD_decompress(
          current,
          rowBuffer_->capacity(),
          compressedBuffer_->as<char>(),
          compressedSize);
      VELOX_CHECK_EQ(ret, bufferSize, "ZSTD decompression failed");
    } else {
      int ret = LZ4_decompress_safe(
          compressedBuffer_->as<char>(),
          current,
          compressedSize,
          rowBuffer_->capacity());
      VELOX_CHECK_EQ(ret, bufferSize, "LZ4 decompression failed");
    }
  } else
#endif
  {
    // No compression mode: read directly
    VELOX_CHECK_EQ(bufferSize, compressedSize);
    input_->readBytes(reinterpret_cast<uint8_t*>(current), bufferSize);
  }

  // Parse rows
  char* bufferEnd = current + bufferSize;
  while (current < bufferEnd) {
    rows.push_back(current);
    VELOX_DCHECK_EQ(
        reinterpret_cast<std::uintptr_t>(current) % rowInfo_.alignment, 0);
    SpillRowSerde::deserialize(current, rowInfo_);
  }
  VELOX_CHECK_EQ(current - bufferEnd, 0);

  // For aggregation bypass hash table, clear nextEqual flag for last row
  if (input_->atEnd() && rowInfo_.nextEqualOffset > 0 &&
      bits::isBitSet(rows.back(), rowInfo_.nextEqualOffset)) {
    bits::setBit(rows.back(), rowInfo_.nextEqualOffset, false);
  }

  return bufferSize;
}

bool RowBasedSpillReadFile::nextBatch(
    std::vector<char*>& rows,
    std::vector<size_t>& rowLengths) {
  auto bytesRead = nextBatch(rows);
  if (bytesRead == 0) {
    return false;
  }

  rowLengths.clear();
  rowLengths.reserve(rows.size());
  for (size_t i = 0; i < rows.size(); ++i) {
    if (i + 1 < rows.size()) {
      rowLengths.push_back(rows[i + 1] - rows[i]);
    } else {
      // Last row - calculate from row size
      rowLengths.push_back(SpillRowSerde::rowSize(rows[i], rowInfo_));
    }
  }
  return true;
}

bool RowBasedSpillReadFile::atEnd() const {
  return input_->atEnd();
}

const std::vector<CompareFlags>& RowBasedSpillReadFile::sortCompareFlags()
    const {
  if (sortCompareFlags_.empty() && !sortingKeys_.empty()) {
    sortCompareFlags_.reserve(sortingKeys_.size());
    for (const auto& key : sortingKeys_) {
      sortCompareFlags_.push_back(key.second);
    }
  }
  return sortCompareFlags_;
}

uint64_t RowBasedSpillReadFile::getSpillReadIOTime() const {
  return input_->stats().readTimeNs / 1000; // Convert to microseconds
}

void RowBasedSpillReadFile::prefetch() {
  // TODO: Implement prefetch for io_uring support
}

} // namespace facebook::velox::exec
