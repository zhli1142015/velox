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
#include "velox/common/file/FileInputStream.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/exec/RowContainer.h"
#include "velox/exec/SpillFile.h"

namespace facebook::velox::exec {

/// Simplified serializer for writing RowContainer rows directly to spill files.
/// This bypasses the Vector conversion step, directly serializing the
/// native row format to reduce CPU overhead.
///
/// Simplified file format (similar to Bolt):
/// - Batch 0..N: [batchHeader][data]
///
/// Batch format:
/// - Batch Header: dataSize(4B) + numRows(4B)
/// - Data: [Row0_fixed][Row1_fixed]...[RowN_fixed]
///         [Row0_var][Row1_var]...
///
/// No file header, magic number, version, or checksum for simplicity.
/// The file format is identified by SpillFileInfo.rowFormatInfo being set.
class RowContainerSpillSerializer {
 public:
  /// Constructs a serializer using RowFormatInfo.
  /// @param info The row format metadata from the RowContainer.
  /// @param pool Memory pool for temporary allocations.
  explicit RowContainerSpillSerializer(
      const RowFormatInfo& info,
      memory::MemoryPool* pool);

  /// Legacy constructor from RowContainer (creates RowFormatInfo internally).
  explicit RowContainerSpillSerializer(
      const RowContainer* container,
      memory::MemoryPool* pool);

  /// Serializes a batch of rows to the output stream.
  /// Writes batch header followed by serialized row data.
  /// @param rows The rows to serialize.
  /// @param output The output stream to write to.
  void serialize(folly::Range<char**> rows, OutputStream* output);

  /// Returns the serialized size of the given rows without actually
  /// serializing them. Useful for buffer pre-allocation.
  size_t serializedSize(folly::Range<char**> rows) const;

  /// Writes a batch header before the batch data.
  /// @param output The output stream.
  /// @param dataSize Size of the row data in bytes.
  /// @param numRows Number of rows in the batch.
  static void
  writeBatchHeader(OutputStream* output, uint32_t dataSize, uint32_t numRows);

  /// Serializes rows and returns the data buffer.
  BufferPtr serializeToBuffer(folly::Range<char**> rows);

 private:
  /// Serializes variable-width data for a single column.
  /// Returns bytes written.
  size_t serializeVariableColumn(
      const char* row,
      column_index_t column,
      char* output) const;

  /// Calculates the size needed for variable-width data in a row.
  size_t variableDataSize(const char* row) const;

  const RowFormatInfo info_;
  const RowContainer* container_;
  memory::MemoryPool* pool_;
};

/// Deserializer for reading RowContainer format spill files.
/// Reads the native row format and restores rows into a RowContainer.
class RowContainerSpillDeserializer {
 public:
  /// Constructs a deserializer for the given RowContainer.
  /// @param container The RowContainer to restore rows into.
  /// @param pool Memory pool for temporary allocations.
  explicit RowContainerSpillDeserializer(
      RowContainer* container,
      memory::MemoryPool* pool);

  /// Deserializes rows from the input stream into the RowContainer.
  /// @param input The input stream to read from.
  /// @param maxRows Maximum number of rows to deserialize.
  /// @param rows Output vector to store the deserialized row pointers.
  /// @return The number of rows actually deserialized.
  int32_t deserialize(
      ByteInputStream* input,
      int32_t maxRows,
      std::vector<char*>& rows);

  /// Reads a batch header.
  /// @return A pair of (dataSize, numRows).
  static std::pair<uint32_t, uint32_t> readBatchHeader(ByteInputStream* input);

  /// Restores all rows from a spill file directly into the target container.
  /// This is the most efficient way to restore spilled data as it avoids
  /// any intermediate Vector conversion.
  /// @param filePath Path to the spill file.
  /// @param container Target RowContainer to restore rows into.
  /// @param pool Memory pool for allocations.
  /// @return Number of rows restored.
  static int64_t restoreRows(
      const std::string& filePath,
      RowContainer* container,
      memory::MemoryPool* pool);

 private:
  /// Deserializes variable-width data for a row.
  void deserializeVariableData(
      ByteInputStream* input,
      char* row,
      column_index_t column);

  RowContainer* container_;
  memory::MemoryPool* pool_;

  // Cached container properties
  int32_t fixedRowSize_;
  int32_t rowSizeOffset_;
  std::vector<column_index_t> variableWidthColumns_;
  std::vector<TypeKind> typeKinds_;
};

/// Reader for RowContainer format spill files.
/// Reads row format data and converts to RowVector for interface compatibility.
class RowContainerSpillReader {
 public:
  /// Constructs a reader for the given spill file.
  /// @param path Path to the spill file.
  /// @param type Row type for the output RowVector.
  /// @param pool Memory pool for allocations.
  RowContainerSpillReader(
      const std::string& path,
      const RowTypePtr& type,
      memory::MemoryPool* pool);

  ~RowContainerSpillReader();

  /// Reads the next batch of rows and returns them as a RowVector.
  /// @param batch Output RowVector to store the result.
  /// @return true if a batch was read, false if end of file.
  bool nextBatch(RowVectorPtr& batch);

  /// Returns true if the file has been fully read.
  bool atEnd() const;

 private:
  /// Reads data from the input stream into a batch.
  void readBatch(RowVectorPtr& batch);

  const std::string path_;
  RowTypePtr type_;
  memory::MemoryPool* pool_;

  std::unique_ptr<common::FileInputStream> input_;
  std::unique_ptr<RowContainer> container_;
  std::unique_ptr<RowContainerSpillDeserializer> deserializer_;
};

/// Writer for RowContainer format spill files.
/// Manages writing row data directly to spill files without Vector conversion.
class RowContainerSpillWriter {
 public:
  /// Constructs a writer for the given RowContainer.
  /// @param container The RowContainer whose rows will be written.
  /// @param pathPrefix File path prefix for spill files.
  /// @param targetFileSize Target size for individual spill files.
  /// @param pool Memory pool for allocations.
  RowContainerSpillWriter(
      const RowContainer* container,
      const std::string& pathPrefix,
      uint64_t targetFileSize,
      memory::MemoryPool* pool);

  ~RowContainerSpillWriter();

  /// Writes a batch of rows to the current spill file.
  /// @param rows The rows to write.
  /// @return The number of bytes written.
  uint64_t write(folly::Range<char**> rows);

  /// Finishes the current file and starts a new one if needed.
  void finishFile();

  /// Returns the paths of all finished spill files.
  std::vector<std::string> finishedFilePaths() const;

  /// Returns the total number of bytes written.
  uint64_t totalBytesWritten() const {
    return totalBytesWritten_;
  }

 private:
  void ensureFile();
  void closeFile();

  const RowContainer* container_;
  RowFormatInfo info_;
  std::string pathPrefix_;
  uint64_t targetFileSize_;
  memory::MemoryPool* pool_;

  std::unique_ptr<RowContainerSpillSerializer> serializer_;
  std::unique_ptr<WriteFile> currentFile_;
  std::string currentFilePath_;
  uint64_t currentFileSize_{0};
  uint32_t fileIndex_{0};
  uint64_t totalBytesWritten_{0};

  std::vector<std::string> finishedFiles_;
};

} // namespace facebook::velox::exec
