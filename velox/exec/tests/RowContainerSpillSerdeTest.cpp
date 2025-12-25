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

#include <gtest/gtest.h>

#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/RowContainer.h"
#include "velox/exec/RowContainerSpillSerde.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::exec::test {

class RowContainerSpillSerdeTest : public testing::Test,
                                   public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    filesystems::registerLocalFileSystem();
    tempDir_ = exec::test::TempDirectoryPath::create();
  }

  /// Creates a RowContainer with the given type and populates it with data.
  std::unique_ptr<RowContainer> createRowContainer(
      const RowTypePtr& type,
      int numRows) {
    std::vector<TypePtr> keyTypes;
    std::vector<TypePtr> dependentTypes;

    // Use first column as key, rest as dependents
    keyTypes.push_back(type->childAt(0));
    for (int i = 1; i < type->size(); ++i) {
      dependentTypes.push_back(type->childAt(i));
    }

    auto container = std::make_unique<RowContainer>(
        keyTypes,
        false, // nullableKeys
        std::vector<Accumulator>{},
        dependentTypes,
        false, // hasNext
        false, // isJoinBuild
        false, // hasProbedFlag
        false, // hasNormalizedKey
        false, // useListRowIndex
        pool_.get());

    // Generate test data
    VectorFuzzer::Options options;
    options.vectorSize = numRows;
    options.nullRatio = 0.1;
    VectorFuzzer fuzzer(options, pool_.get());

    auto rowVector = fuzzer.fuzzInputFlatRow(type);

    // Store data in container
    std::vector<DecodedVector> decoders(type->size());
    for (int i = 0; i < type->size(); ++i) {
      decoders[i].decode(*rowVector->childAt(i));
    }

    for (int row = 0; row < numRows; ++row) {
      char* newRow = container->newRow();
      for (int col = 0; col < type->size(); ++col) {
        container->store(decoders[col], row, newRow, col);
      }
    }

    return container;
  }

  /// Extracts all rows from a RowContainer.
  std::vector<char*> listRows(RowContainer* container) {
    RowContainerIterator iter;
    std::vector<char*> rows(container->numRows());
    int32_t numRows =
        container->listRows(&iter, container->numRows(), rows.data());
    EXPECT_EQ(numRows, container->numRows());
    return rows;
  }

  std::shared_ptr<exec::test::TempDirectoryPath> tempDir_;
};

TEST_F(RowContainerSpillSerdeTest, serializerBasic) {
  // Create a simple row container with fixed-width columns
  auto type = ROW({"a", "b", "c"}, {BIGINT(), INTEGER(), DOUBLE()});
  auto container = createRowContainer(type, 100);

  auto rows = listRows(container.get());
  ASSERT_EQ(rows.size(), 100);

  // Create serializer
  RowContainerSpillSerializer serializer(container.get(), pool_.get());

  // Check serialized size calculation
  size_t serializedSize =
      serializer.serializedSize(folly::Range(rows.data(), rows.size()));
  EXPECT_GT(serializedSize, 0);
}

TEST_F(RowContainerSpillSerdeTest, serializerWithStrings) {
  // Create a row container with variable-width columns
  auto type = ROW({"id", "name", "value"}, {BIGINT(), VARCHAR(), DOUBLE()});
  auto container = createRowContainer(type, 50);

  auto rows = listRows(container.get());
  ASSERT_EQ(rows.size(), 50);

  // Create serializer
  RowContainerSpillSerializer serializer(container.get(), pool_.get());

  // Check serialized size calculation
  size_t serializedSize =
      serializer.serializedSize(folly::Range(rows.data(), rows.size()));
  EXPECT_GT(serializedSize, 0);
}

TEST_F(RowContainerSpillSerdeTest, writerBasic) {
  auto type = ROW({"a", "b"}, {BIGINT(), DOUBLE()});
  auto container = createRowContainer(type, 100);

  auto rows = listRows(container.get());
  ASSERT_EQ(rows.size(), 100);

  // Create writer
  auto pathPrefix = tempDir_->getPath() + "/test_spill";
  RowContainerSpillWriter writer(
      container.get(),
      pathPrefix,
      1 << 20, // 1MB target file size
      pool_.get());

  // Write rows
  uint64_t bytesWritten = writer.write(folly::Range(rows.data(), rows.size()));
  EXPECT_GT(bytesWritten, 0);

  // Finish writing
  writer.finishFile();

  // Check files were created
  auto filePaths = writer.finishedFilePaths();
  EXPECT_GE(filePaths.size(), 1);

  // Verify file exists
  auto fs = filesystems::getFileSystem(filePaths[0], nullptr);
  EXPECT_TRUE(fs->exists(filePaths[0]));
}

TEST_F(RowContainerSpillSerdeTest, roundTrip) {
  // Test serialization -> deserialization round trip
  auto type = ROW({"a", "b", "c"}, {BIGINT(), INTEGER(), DOUBLE()});

  // Create source container and populate
  auto sourceContainer = createRowContainer(type, 5);
  auto sourceRows = listRows(sourceContainer.get());
  ASSERT_EQ(sourceRows.size(), 5);

  // Extract original values for comparison
  std::vector<int64_t> originalValues(5);
  auto col0Vector = BaseVector::create(BIGINT(), 5, pool_.get());
  sourceContainer->extractColumn(
      sourceRows.data(), sourceRows.size(), 0, col0Vector);
  auto flatVector = col0Vector->asFlatVector<int64_t>();
  for (int i = 0; i < 5; ++i) {
    originalValues[i] = flatVector->valueAt(i);
  }

  // Serialize
  RowContainerSpillSerializer serializer(sourceContainer.get(), pool_.get());
  auto buffer = serializer.serializeToBuffer(
      folly::Range(sourceRows.data(), sourceRows.size()));
  ASSERT_NE(buffer, nullptr);

  // Create destination container with same structure
  auto destContainer = std::make_unique<RowContainer>(
      type->children(), // keyTypes = all columns
      std::vector<TypePtr>{}, // dependentTypes
      pool_.get());

  // Deserialize - create ByteRange from buffer
  std::vector<ByteRange> ranges;
  ranges.push_back(
      ByteRange{
          const_cast<uint8_t*>(buffer->as<uint8_t>()),
          (int32_t)buffer->size(),
          0});
  BufferInputStream inputStream(std::move(ranges));
  RowContainerSpillDeserializer deserializer(destContainer.get(), pool_.get());

  std::vector<char*> destRows;
  int32_t numDeserialized =
      deserializer.deserialize(&inputStream, 100, destRows);
  EXPECT_EQ(numDeserialized, 5);
  EXPECT_EQ(destRows.size(), 5);

  // Extract deserialized values
  auto destCol0Vector = BaseVector::create(BIGINT(), 5, pool_.get());
  destContainer->extractColumn(
      destRows.data(), destRows.size(), 0, destCol0Vector);
  auto destFlatVector = destCol0Vector->asFlatVector<int64_t>();

  // Verify data integrity by comparing values
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(originalValues[i], destFlatVector->valueAt(i))
        << "Mismatch at row " << i << " column 0";
  }
}

TEST_F(RowContainerSpillSerdeTest, roundTripWithStrings) {
  // Test round trip with variable-width string columns
  // For now, test only fixed-width columns since string storage requires
  // proper DecodedVector handling
  auto type = ROW({"id", "value"}, {BIGINT(), DOUBLE()});

  // Create source container
  std::vector<TypePtr> keyTypes = {BIGINT()};
  std::vector<TypePtr> dependentTypes = {DOUBLE()};

  auto sourceContainer = std::make_unique<RowContainer>(
      keyTypes,
      false, // nullableKeys
      std::vector<Accumulator>{},
      dependentTypes,
      false, // hasNext
      false, // isJoinBuild
      false, // hasProbedFlag
      false, // hasNormalizedKey
      false, // useListRowIndex
      pool_.get());

  // Manually add rows
  std::vector<int64_t> ids = {1, 2, 3, 4, 5};
  std::vector<double> values = {1.1, 2.2, 3.3, 4.4, 5.5};

  for (int i = 0; i < 5; ++i) {
    char* row = sourceContainer->newRow();
    // Store id (column 0)
    *reinterpret_cast<int64_t*>(row + sourceContainer->columnAt(0).offset()) =
        ids[i];
    // Store value (column 1)
    *reinterpret_cast<double*>(row + sourceContainer->columnAt(1).offset()) =
        values[i];
  }

  auto sourceRows = listRows(sourceContainer.get());
  ASSERT_EQ(sourceRows.size(), 5);

  // Serialize
  RowContainerSpillSerializer serializer(sourceContainer.get(), pool_.get());
  auto buffer = serializer.serializeToBuffer(
      folly::Range(sourceRows.data(), sourceRows.size()));
  ASSERT_NE(buffer, nullptr);

  // Create destination container
  auto destContainer = std::make_unique<RowContainer>(
      keyTypes,
      false,
      std::vector<Accumulator>{},
      dependentTypes,
      false,
      false,
      false,
      false,
      false,
      pool_.get());

  // Deserialize
  std::vector<ByteRange> ranges;
  ranges.push_back(
      ByteRange{
          const_cast<uint8_t*>(buffer->as<uint8_t>()),
          (int32_t)buffer->size(),
          0});
  BufferInputStream inputStream(std::move(ranges));
  RowContainerSpillDeserializer deserializer(destContainer.get(), pool_.get());

  std::vector<char*> destRows;
  int32_t numDeserialized =
      deserializer.deserialize(&inputStream, 100, destRows);
  EXPECT_EQ(numDeserialized, 5);

  // Verify id values
  for (int i = 0; i < 5; ++i) {
    int64_t destId = *reinterpret_cast<int64_t*>(
        destRows[i] + destContainer->columnAt(0).offset());
    EXPECT_EQ(ids[i], destId) << "Mismatch at row " << i << " for id";
  }

  // Verify double values
  for (int i = 0; i < 5; ++i) {
    double destValue = *reinterpret_cast<double*>(
        destRows[i] + destContainer->columnAt(1).offset());
    EXPECT_DOUBLE_EQ(values[i], destValue)
        << "Mismatch at row " << i << " for value";
  }
}

TEST_F(RowContainerSpillSerdeTest, emptyRows) {
  // Test with empty row set
  auto type = ROW({"a"}, {BIGINT()});
  auto container = std::make_unique<RowContainer>(
      type->children(), std::vector<TypePtr>{}, pool_.get());

  std::vector<char*> rows;
  RowContainerSpillSerializer serializer(container.get(), pool_.get());

  // Empty serialize should succeed
  auto buffer =
      serializer.serializeToBuffer(folly::Range<char**>(nullptr, (size_t)0));
  EXPECT_EQ(buffer, nullptr);

  size_t size =
      serializer.serializedSize(folly::Range<char**>(nullptr, (size_t)0));
  EXPECT_EQ(size, 0);
}

TEST_F(RowContainerSpillSerdeTest, roundTripWithVarchar) {
  // Test round trip with VARCHAR columns using proper store() method
  // This test uses the full Writer/Deserializer flow with batch header
  auto type = ROW({"id", "name"}, {BIGINT(), VARCHAR()});

  // Create source container using the helper which stores via DecodedVector
  auto sourceContainer = createRowContainer(type, 10);
  auto sourceRows = listRows(sourceContainer.get());
  ASSERT_EQ(sourceRows.size(), 10);

  // Extract original values for comparison
  std::vector<int64_t> originalIds(10);
  std::vector<std::string> originalNames(10);
  std::vector<bool> idNulls(10), nameNulls(10);

  auto idVector = BaseVector::create(BIGINT(), 10, pool_.get());
  sourceContainer->extractColumn(
      sourceRows.data(), sourceRows.size(), 0, idVector);
  auto idFlatVector = idVector->asFlatVector<int64_t>();
  for (int i = 0; i < 10; ++i) {
    idNulls[i] = idFlatVector->isNullAt(i);
    if (!idNulls[i]) {
      originalIds[i] = idFlatVector->valueAt(i);
    }
  }

  auto nameVector = BaseVector::create(VARCHAR(), 10, pool_.get());
  sourceContainer->extractColumn(
      sourceRows.data(), sourceRows.size(), 1, nameVector);
  auto nameFlatVector = nameVector->asFlatVector<StringView>();
  for (int i = 0; i < 10; ++i) {
    nameNulls[i] = nameFlatVector->isNullAt(i);
    if (!nameNulls[i]) {
      originalNames[i] = nameFlatVector->valueAt(i).str();
    }
  }

  // Write to file using the full writer flow
  auto pathPrefix = tempDir_->getPath() + "/varchar_test";
  RowContainerSpillWriter writer(
      sourceContainer.get(), pathPrefix, 1 << 20, pool_.get());

  writer.write(folly::Range(sourceRows.data(), sourceRows.size()));
  writer.finishFile();

  auto filePaths = writer.finishedFilePaths();
  ASSERT_EQ(filePaths.size(), 1);

  // Read back from file
  auto fs = filesystems::getFileSystem(filePaths[0], nullptr);
  auto readFile = fs->openFileForRead(filePaths[0]);
  auto fileSize = readFile->size();

  // Read entire file into buffer
  std::string fileContent(fileSize, '\0');
  readFile->pread(0, fileSize, fileContent.data());

  // Create destination container with same structure as source
  std::vector<TypePtr> keyTypes = {BIGINT()};
  std::vector<TypePtr> dependentTypes = {VARCHAR()};
  auto destContainer = std::make_unique<RowContainer>(
      keyTypes,
      false,
      std::vector<Accumulator>{},
      dependentTypes,
      false,
      false,
      false,
      false,
      false,
      pool_.get());

  // Verify layouts match
  ASSERT_EQ(sourceContainer->fixedRowSize(), destContainer->fixedRowSize());
  ASSERT_EQ(sourceContainer->nextOffset(), destContainer->nextOffset());

  // Read batch with simplified format
  std::vector<ByteRange> ranges;
  // Simplified format: no file header
  // Batch header: dataSize(4) + numRows(4) = 8
  const int batchHeaderSize = 8;

  // Read batch header to get size info
  uint32_t dataSize = *reinterpret_cast<const uint32_t*>(fileContent.data());
  uint32_t numRows = *reinterpret_cast<const uint32_t*>(fileContent.data() + 4);

  // Create input stream for batch data only
  ranges.push_back(
      ByteRange{
          reinterpret_cast<uint8_t*>(
              const_cast<char*>(fileContent.data() + batchHeaderSize)),
          (int32_t)dataSize,
          0});
  BufferInputStream inputStream(std::move(ranges));

  RowContainerSpillDeserializer deserializer(destContainer.get(), pool_.get());
  std::vector<char*> destRows;
  int32_t numDeserialized =
      deserializer.deserialize(&inputStream, numRows, destRows);
  EXPECT_EQ(numDeserialized, 10);

  // Verify raw null bits are correctly deserialized
  const auto& destNameColumn = destContainer->columnAt(1);
  for (size_t i = 0; i < destRows.size(); ++i) {
    bool srcIsNull = nameNulls[i];
    bool destIsNull = RowContainer::isNullAt(destRows[i], destNameColumn);
    EXPECT_EQ(srcIsNull, destIsNull) << "Raw null mismatch at row " << i;
  }

  // Extract and verify.
  // Note: For deserialized data, we must use extractColumn with
  // columnHasNulls=true because column statistics are not updated during raw
  // byte deserialization.
  auto destIdVector = BaseVector::create(BIGINT(), 10, pool_.get());
  RowContainer::extractColumn(
      destRows.data(),
      destRows.size(),
      destContainer->columnAt(0),
      true /* columnHasNulls */,
      destIdVector);
  auto destIdFlatVector = destIdVector->asFlatVector<int64_t>();

  auto destNameVector = BaseVector::create(VARCHAR(), 10, pool_.get());
  RowContainer::extractColumn(
      destRows.data(),
      destRows.size(),
      destContainer->columnAt(1),
      true /* columnHasNulls */,
      destNameVector);
  auto destNameFlatVector = destNameVector->asFlatVector<StringView>();

  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(idNulls[i], destIdFlatVector->isNullAt(i))
        << "Null mismatch at row " << i << " for id";
    if (!idNulls[i]) {
      EXPECT_EQ(originalIds[i], destIdFlatVector->valueAt(i))
          << "Value mismatch at row " << i << " for id";
    }

    EXPECT_EQ(nameNulls[i], destNameFlatVector->isNullAt(i))
        << "Null mismatch at row " << i << " for name";
    if (!nameNulls[i]) {
      EXPECT_EQ(originalNames[i], destNameFlatVector->valueAt(i).str())
          << "Value mismatch at row " << i << " for name: expected '"
          << originalNames[i] << "' got '"
          << destNameFlatVector->valueAt(i).str() << "'";
    }
  }
}

TEST_F(RowContainerSpillSerdeTest, restoreRows) {
  // Test direct restoration using restoreRows() static method
  auto type = ROW({"a", "b", "c"}, {BIGINT(), INTEGER(), DOUBLE()});

  // Create source container and populate
  auto sourceContainer = createRowContainer(type, 100);
  auto sourceRows = listRows(sourceContainer.get());
  ASSERT_EQ(sourceRows.size(), 100);

  // Extract original values for comparison
  std::vector<int64_t> originalCol0(100);
  auto col0Vector = BaseVector::create(BIGINT(), 100, pool_.get());
  sourceContainer->extractColumn(
      sourceRows.data(), sourceRows.size(), 0, col0Vector);
  auto flatVector = col0Vector->asFlatVector<int64_t>();
  for (int i = 0; i < 100; ++i) {
    originalCol0[i] = flatVector->valueAt(i);
  }

  // Write to file
  auto pathPrefix = tempDir_->getPath() + "/restore_test";
  RowContainerSpillWriter writer(
      sourceContainer.get(), pathPrefix, 1 << 20, pool_.get());

  writer.write(folly::Range(sourceRows.data(), sourceRows.size()));
  writer.finishFile();

  auto filePaths = writer.finishedFilePaths();
  ASSERT_EQ(filePaths.size(), 1);

  // Create destination container with same structure
  std::vector<TypePtr> keyTypes = {BIGINT()};
  std::vector<TypePtr> dependentTypes = {INTEGER(), DOUBLE()};
  auto destContainer = std::make_unique<RowContainer>(
      keyTypes,
      false,
      std::vector<Accumulator>{},
      dependentTypes,
      false,
      false,
      false,
      false,
      false,
      pool_.get());

  // Directly restore rows
  auto numRestored = RowContainerSpillDeserializer::restoreRows(
      filePaths[0], destContainer.get(), pool_.get());
  EXPECT_EQ(numRestored, 100);

  // Verify restored data
  auto destRows = listRows(destContainer.get());
  ASSERT_EQ(destRows.size(), 100);

  auto destCol0Vector = BaseVector::create(BIGINT(), 100, pool_.get());
  RowContainer::extractColumn(
      destRows.data(),
      destRows.size(),
      destContainer->columnAt(0),
      true,
      destCol0Vector);
  auto destFlatVector = destCol0Vector->asFlatVector<int64_t>();

  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(originalCol0[i], destFlatVector->valueAt(i))
        << "Value mismatch at row " << i;
  }
}

} // namespace facebook::velox::exec::test
