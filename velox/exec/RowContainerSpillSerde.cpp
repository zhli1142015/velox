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

#include "velox/exec/RowContainerSpillSerde.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/HashStringAllocator.h"

namespace facebook::velox::exec {

namespace {
/// Helper to get the size of variable-width data stored in a StringView.
int32_t getStringViewDataSize(const StringView& value) {
  return value.size();
}

/// Copies data from a StringView that may be stored in multiple segments
/// in HashStringAllocator.
void copyStringViewData(const StringView& value, char* output) {
  if (value.isInline() ||
      reinterpret_cast<const HashStringAllocator::Header*>(value.data())[-1]
              .size() >= value.size()) {
    // Data is contiguous
    ::memcpy(output, value.data(), value.size());
  } else {
    // Data is in multiple segments, use InputStream to read
    HashStringAllocator::InputStream stream(
        HashStringAllocator::headerOf(value.data()));
    stream.ByteInputStream::readBytes(output, value.size());
  }
}

/// Simple OutputStream wrapper for WriteFile.
class FileOutputStream : public OutputStream {
 public:
  explicit FileOutputStream(WriteFile* file) : file_(file), position_(0) {}

  void write(const char* s, std::streamsize count) override {
    file_->append(std::string_view(s, count));
    position_ += count;
  }

  std::streampos tellp() const override {
    return position_;
  }

  void seekp(std::streampos pos) override {
    VELOX_CHECK(
        pos == position_, "FileOutputStream doesn't support random seeking");
  }

 private:
  WriteFile* file_;
  std::streampos position_;
};
} // namespace

// ============================================================================
// RowFormatInfo Implementation
// ============================================================================

RowFormatInfo RowFormatInfo::fromContainer(const RowContainer* container) {
  VELOX_CHECK_NOT_NULL(container);

  RowFormatInfo info;
  info.container = container;
  info.fixedRowSize = container->fixedRowSize();
  info.nextOffset = container->nextOffset();

  const auto& types = container->columnTypes();
  info.typeKinds.reserve(types.size());
  for (column_index_t i = 0; i < types.size(); ++i) {
    info.typeKinds.push_back(types[i]->kind());
    if (!types[i]->isFixedWidth()) {
      info.variableWidthColumns.push_back(i);
    }
  }

  return info;
}

int32_t RowFormatInfo::rowSize(const char* row) const {
  // Fixed part size (excluding nextOffset_)
  int32_t size = nextOffset > 0 ? nextOffset : fixedRowSize;

  // Add variable-width data sizes
  for (auto col : variableWidthColumns) {
    // 4 bytes for size prefix + actual data
    size += sizeof(int32_t);
    const auto& rowColumn = container->columnAt(col);
    if (!RowContainer::isNullAt(row, rowColumn)) {
      size += container->variableSizeAt(row, col);
    }
  }

  return size;
}

// ============================================================================
// RowContainerSpillSerializer Implementation
// ============================================================================

RowContainerSpillSerializer::RowContainerSpillSerializer(
    const RowFormatInfo& info,
    memory::MemoryPool* pool)
    : info_(info), container_(info.container), pool_(pool) {
  VELOX_CHECK_NOT_NULL(container_);
  VELOX_CHECK_NOT_NULL(pool_);
}

RowContainerSpillSerializer::RowContainerSpillSerializer(
    const RowContainer* container,
    memory::MemoryPool* pool)
    : RowContainerSpillSerializer(
          RowFormatInfo::fromContainer(container),
          pool) {}

size_t RowContainerSpillSerializer::serializedSize(
    folly::Range<char**> rows) const {
  size_t totalSize = 0;
  for (const char* row : rows) {
    totalSize += info_.rowSize(row);
  }
  return totalSize;
}

size_t RowContainerSpillSerializer::variableDataSize(const char* row) const {
  size_t size = 0;
  for (auto col : info_.variableWidthColumns) {
    // 4 bytes for size prefix + actual data
    size += sizeof(int32_t);
    const auto& rowColumn = container_->columnAt(col);
    if (!RowContainer::isNullAt(row, rowColumn)) {
      size += container_->variableSizeAt(row, col);
    }
  }
  return size;
}

void RowContainerSpillSerializer::writeBatchHeader(
    OutputStream* output,
    uint32_t dataSize,
    uint32_t numRows) {
  // Simple batch header: [dataSize:4B][numRows:4B]
  output->write(reinterpret_cast<const char*>(&dataSize), sizeof(dataSize));
  output->write(reinterpret_cast<const char*>(&numRows), sizeof(numRows));
}

void RowContainerSpillSerializer::serialize(
    folly::Range<char**> rows,
    OutputStream* output) {
  if (rows.empty()) {
    return;
  }

  // Serialize to buffer
  auto buffer = serializeToBuffer(rows);

  // Write batch header
  writeBatchHeader(output, static_cast<uint32_t>(buffer->size()), rows.size());

  // Write serialized data
  output->write(buffer->as<char>(), buffer->size());
}

BufferPtr RowContainerSpillSerializer::serializeToBuffer(
    folly::Range<char**> rows) {
  if (rows.empty()) {
    return nullptr;
  }

  // Calculate total size needed
  size_t totalSize = serializedSize(rows);

  // Allocate temporary buffer
  auto buffer = AlignedBuffer::allocate<char>(totalSize, pool_);
  char* rawBuffer = buffer->asMutable<char>();

  // Size of fixed part per row (excluding nextOffset_)
  int32_t fixedPartSize =
      info_.nextOffset > 0 ? info_.nextOffset : info_.fixedRowSize;

  // Serialize fixed parts
  char* currentPos = rawBuffer;
  for (const char* row : rows) {
    ::memcpy(currentPos, row, fixedPartSize);
    currentPos += fixedPartSize;
  }

  // Serialize variable-width data
  for (const char* row : rows) {
    for (auto col : info_.variableWidthColumns) {
      currentPos += serializeVariableColumn(row, col, currentPos);
    }
  }

  VELOX_CHECK_EQ(
      currentPos - rawBuffer,
      totalSize,
      "Serialized size mismatch: expected {}, actual {}",
      totalSize,
      currentPos - rawBuffer);

  return buffer;
}

size_t RowContainerSpillSerializer::serializeVariableColumn(
    const char* row,
    column_index_t column,
    char* output) const {
  const auto& rowColumn = container_->columnAt(column);

  // Check if null
  if (RowContainer::isNullAt(row, rowColumn)) {
    int32_t size = 0;
    ::memcpy(output, &size, sizeof(int32_t));
    return sizeof(int32_t);
  }

  const auto typeKind = info_.typeKinds[column];
  if (typeKind == TypeKind::VARCHAR || typeKind == TypeKind::VARBINARY) {
    // Handle StringView
    const auto value =
        RowContainer::valueAt<StringView>(row, rowColumn.offset());
    const auto size = static_cast<int32_t>(value.size());
    ::memcpy(output, &size, sizeof(int32_t));

    if (size > 0) {
      copyStringViewData(value, output + sizeof(int32_t));
    }
    return sizeof(int32_t) + size;
  }

  // Handle complex types (ROW, ARRAY, MAP) stored as std::string_view
  const auto value =
      RowContainer::valueAt<std::string_view>(row, rowColumn.offset());
  const auto size = static_cast<int32_t>(value.size());
  ::memcpy(output, &size, sizeof(int32_t));

  if (size > 0) {
    // Complex types may also be stored in multiple segments
    auto stream = RowContainer::prepareRead(row, rowColumn.offset());
    stream.ByteInputStream::readBytes(output + sizeof(int32_t), size);
  }

  return sizeof(int32_t) + size;
}

// ============================================================================
// RowContainerSpillDeserializer Implementation
// ============================================================================

RowContainerSpillDeserializer::RowContainerSpillDeserializer(
    RowContainer* container,
    memory::MemoryPool* pool)
    : container_(container), pool_(pool) {
  VELOX_CHECK_NOT_NULL(container_);
  VELOX_CHECK_NOT_NULL(pool_);

  // Cache container properties
  fixedRowSize_ = container_->fixedRowSize();
  rowSizeOffset_ = container_->rowSizeOffset();

  // Identify variable-width columns
  const auto& types = container_->columnTypes();
  for (column_index_t i = 0; i < types.size(); ++i) {
    typeKinds_.push_back(types[i]->kind());
    if (!types[i]->isFixedWidth()) {
      variableWidthColumns_.push_back(i);
    }
  }
}

std::pair<uint32_t, uint32_t> RowContainerSpillDeserializer::readBatchHeader(
    ByteInputStream* input) {
  // Simple batch header: [dataSize:4B][numRows:4B]
  auto dataSize = input->read<uint32_t>();
  auto numRows = input->read<uint32_t>();
  return {dataSize, numRows};
}

int32_t RowContainerSpillDeserializer::deserialize(
    ByteInputStream* input,
    int32_t maxRows,
    std::vector<char*>& rows) {
  int32_t rowsRead = 0;

  // Size of fixed part per row (based on container's layout)
  int32_t fixedPartSize = fixedRowSize_;
  const int32_t nextOffset = container_->nextOffset();
  if (nextOffset > 0) {
    fixedPartSize = nextOffset;
  }

  rows.reserve(rows.size() + maxRows);

  // Phase 1: Read all fixed parts
  std::vector<char*> newRows;
  newRows.reserve(maxRows);

  while (rowsRead < maxRows && input->remainingSize() >= fixedPartSize) {
    char* row = container_->newRow();
    input->readBytes(row, fixedPartSize);
    newRows.push_back(row);
    ++rowsRead;
  }

  // Phase 2: Read all variable parts
  if (!variableWidthColumns_.empty()) {
    for (char* row : newRows) {
      if (rowSizeOffset_ > 0) {
        // Track variable data size for rowSizeOffset_
        RowSizeTracker<char> tracker(
            row[rowSizeOffset_], container_->stringAllocator());
        for (auto col : variableWidthColumns_) {
          deserializeVariableData(input, row, col);
        }
      } else {
        for (auto col : variableWidthColumns_) {
          deserializeVariableData(input, row, col);
        }
      }
    }
  }

  // Add all rows to output
  for (char* row : newRows) {
    rows.push_back(row);
  }

  return rowsRead;
}

void RowContainerSpillDeserializer::deserializeVariableData(
    ByteInputStream* input,
    char* row,
    column_index_t column) {
  const auto& rowColumn = container_->columnAt(column);
  const auto typeKind = typeKinds_[column];

  // Read size
  auto size = input->read<int32_t>();

  if (size == 0) {
    // Null or empty - set to empty value
    if (typeKind == TypeKind::VARCHAR || typeKind == TypeKind::VARBINARY) {
      RowContainer::valueAt<StringView>(row, rowColumn.offset()) = StringView();
    } else {
      RowContainer::valueAt<std::string_view>(row, rowColumn.offset()) =
          std::string_view();
    }
    return;
  }

  // Read data into temporary buffer
  std::vector<char> tempBuffer(size);
  input->readBytes(tempBuffer.data(), size);

  // Store in HashStringAllocator
  if (typeKind == TypeKind::VARCHAR || typeKind == TypeKind::VARBINARY) {
    ByteOutputStream stream(&container_->stringAllocator(), false, false);
    auto position = container_->stringAllocator().newWrite(stream, size);
    stream.appendStringView(std::string_view(tempBuffer.data(), size));
    container_->stringAllocator().finishWrite(stream, 0);
    RowContainer::valueAt<StringView>(row, rowColumn.offset()) =
        StringView(reinterpret_cast<char*>(position.position), size);
  } else {
    // Complex types
    ByteOutputStream stream(&container_->stringAllocator(), false, false);
    auto position = container_->stringAllocator().newWrite(stream, size);
    stream.appendStringView(std::string_view(tempBuffer.data(), size));
    container_->stringAllocator().finishWrite(stream, 0);
    RowContainer::valueAt<std::string_view>(row, rowColumn.offset()) =
        std::string_view(reinterpret_cast<char*>(position.position), size);
  }
}

int64_t RowContainerSpillDeserializer::restoreRows(
    const std::string& filePath,
    RowContainer* container,
    memory::MemoryPool* pool) {
  auto fs = filesystems::getFileSystem(filePath, nullptr);
  auto readFile = fs->openFileForRead(filePath);
  common::FileInputStream input(std::move(readFile), 1 << 20, pool);

  RowContainerSpillDeserializer deserializer(container, pool);
  std::vector<char*> rows;
  int64_t totalRows = 0;

  while (!input.atEnd()) {
    auto [dataSize, numRows] = readBatchHeader(&input);
    if (numRows == 0) {
      continue;
    }
    rows.clear();
    deserializer.deserialize(&input, numRows, rows);
    totalRows += rows.size();
  }

  return totalRows;
}

// ============================================================================
// RowContainerSpillWriter Implementation
// ============================================================================

RowContainerSpillWriter::RowContainerSpillWriter(
    const RowContainer* container,
    const std::string& pathPrefix,
    uint64_t targetFileSize,
    memory::MemoryPool* pool)
    : container_(container),
      info_(RowFormatInfo::fromContainer(container)),
      pathPrefix_(pathPrefix),
      targetFileSize_(targetFileSize),
      pool_(pool) {
  VELOX_CHECK_NOT_NULL(container_);
  VELOX_CHECK_NOT_NULL(pool_);
  serializer_ = std::make_unique<RowContainerSpillSerializer>(info_, pool_);
}

RowContainerSpillWriter::~RowContainerSpillWriter() {
  closeFile();
}

void RowContainerSpillWriter::ensureFile() {
  if (currentFile_ != nullptr) {
    return;
  }

  currentFilePath_ = fmt::format("{}-{}.rowspill", pathPrefix_, fileIndex_);
  auto fs = filesystems::getFileSystem(currentFilePath_, nullptr);
  currentFile_ = fs->openFileForWrite(currentFilePath_);
  currentFileSize_ = 0;
}

void RowContainerSpillWriter::closeFile() {
  if (currentFile_ == nullptr) {
    return;
  }

  currentFile_->close();
  finishedFiles_.push_back(currentFilePath_);
  currentFile_.reset();
  currentFilePath_.clear();
  ++fileIndex_;
}

uint64_t RowContainerSpillWriter::write(folly::Range<char**> rows) {
  if (rows.empty()) {
    return 0;
  }

  ensureFile();

  FileOutputStream outputStream(currentFile_.get());

  // Serialize and write (includes batch header)
  auto startPos = currentFileSize_;
  serializer_->serialize(rows, &outputStream);

  auto newPosition = static_cast<uint64_t>(outputStream.tellp());
  uint64_t bytesWritten = newPosition - startPos;
  currentFileSize_ = newPosition;
  totalBytesWritten_ += bytesWritten;

  // Check if we should start a new file
  if (currentFileSize_ >= targetFileSize_) {
    finishFile();
  }

  return bytesWritten;
}

void RowContainerSpillWriter::finishFile() {
  closeFile();
}

std::vector<std::string> RowContainerSpillWriter::finishedFilePaths() const {
  std::vector<std::string> result = finishedFiles_;
  if (currentFile_ != nullptr) {
    result.push_back(currentFilePath_);
  }
  return result;
}

// ============================================================================
// RowContainerSpillReader Implementation
// ============================================================================

RowContainerSpillReader::RowContainerSpillReader(
    const std::string& path,
    const RowTypePtr& type,
    memory::MemoryPool* pool)
    : path_(path), type_(type), pool_(pool) {
  auto fs = filesystems::getFileSystem(path, nullptr);
  auto readFile = fs->openFileForRead(path);
  input_ = std::make_unique<common::FileInputStream>(
      std::move(readFile), 1 << 20, pool_); // 1MB buffer

  // Create a temporary RowContainer for deserialization.
  std::vector<TypePtr> types;
  types.reserve(type_->size());
  for (size_t i = 0; i < type_->size(); ++i) {
    types.push_back(type_->childAt(i));
  }
  container_ = std::make_unique<RowContainer>(types, pool_);
  deserializer_ =
      std::make_unique<RowContainerSpillDeserializer>(container_.get(), pool_);
}

RowContainerSpillReader::~RowContainerSpillReader() = default;

bool RowContainerSpillReader::nextBatch(RowVectorPtr& batch) {
  if (input_->atEnd()) {
    return false;
  }

  readBatch(batch);
  return true;
}

void RowContainerSpillReader::readBatch(RowVectorPtr& batch) {
  // Read batch header to get dataSize and numRows
  auto [dataSize, numRows] =
      RowContainerSpillDeserializer::readBatchHeader(input_.get());

  if (numRows == 0) {
    // Empty batch, create an empty result
    batch = BaseVector::create<RowVector>(type_, 0, pool_);
    return;
  }

  // Clear the container for this batch
  container_->clear();

  // Deserialize rows into container
  std::vector<char*> rows;
  rows.reserve(numRows);
  deserializer_->deserialize(input_.get(), numRows, rows);

  // Convert container rows to RowVector using extractColumn
  batch = BaseVector::create<RowVector>(type_, rows.size(), pool_);
  const auto& types = container_->columnTypes();
  for (size_t i = 0; i < types.size(); ++i) {
    container_->extractColumn(rows.data(), rows.size(), i, batch->childAt(i));
  }
}

bool RowContainerSpillReader::atEnd() const {
  return input_->atEnd();
}

} // namespace facebook::velox::exec
