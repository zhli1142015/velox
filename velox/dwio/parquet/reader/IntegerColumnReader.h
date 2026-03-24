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

#include "velox/dwio/common/SelectiveIntegerColumnReader.h"
#include "velox/vector/DictionaryVector.h"

namespace facebook::velox::parquet {

class IntegerColumnReader : public dwio::common::SelectiveIntegerColumnReader {
 public:
  IntegerColumnReader(
      const TypePtr& requestedType,
      std::shared_ptr<const dwio::common::TypeWithId> fileType,
      ParquetParams& params,
      common::ScanSpec& scanSpec)
      : SelectiveIntegerColumnReader(
            requestedType,
            params,
            scanSpec,
            std::move(fileType)) {}

  bool hasBulkPath() const override {
    return !formatData_->as<ParquetData>().isDeltaBinaryPacked() &&
        !this->fileType().type()->isLongDecimal() &&
        ((this->fileType().type()->isShortDecimal())
             ? formatData_->as<ParquetData>().hasDictionary()
             : true);
  }

  void seekToRowGroup(int64_t index) override {
    SelectiveIntegerColumnReader::seekToRowGroup(index);
    scanState().clear();
    readOffset_ = 0;
    formatData_->as<ParquetData>().seekToRowGroup(index);
  }

  uint64_t skip(uint64_t numValues) override {
    formatData_->as<ParquetData>().skip(numValues);
    return numValues;
  }

  void getValues(const RowSet& rows, VectorPtr* result) override {
    auto& parquetData = formatData_->as<ParquetData>();
    auto& fileType = static_cast<const ParquetTypeWithId&>(*fileType_);
    auto logicalType = fileType.logicalType_;
    bool isUnsigned = logicalType.has_value() &&
        logicalType.value().__isset.INTEGER &&
        !logicalType.value().INTEGER.isSigned;

    // Check if we should output DictionaryVector.
    if (scanState_.dictionary.values && isDictOutputEnabled_) {
      switch (requestedType_->kind()) {
        case TypeKind::INTEGER:
          getDictValues<int32_t>(rows, result);
          return;
        case TypeKind::BIGINT:
          getDictValues<int64_t>(rows, result);
          return;
        default:
          break;
      }
    }

    if (isUnsigned) {
      getUnsignedIntValues(rows, requestedType_, result);
    } else {
      getIntValues(rows, requestedType_, result);
    }
  }

  void dedictionarize() override {
    if (!scanSpec_->keepValues() || !isDictOutputEnabled_) {
      scanState_.clear();
      return;
    }
    // Check if the index-preserving visitor was actually used for the
    // previous dict pages. If not (cardinality exceeded threshold), the
    // buffer contains resolved values, not indices — just clear state.
    if (!formatData_->as<ParquetData>().indexPreservingDictUsed()) {
      isDictOutputEnabled_ = false;
      formatData_->as<ParquetData>().setUseIndexPreservingDict(false);
      scanState_.clear();
      return;
    }
    // Expand int32 indices back to typed values in-place.
    auto* indices = values_->asMutable<int32_t>();
    auto numVals = numValues_;
    // Use raw dictionary pointer directly — avoids allocating a temporary
    // FlatVector wrapper just to read values.
    const auto* rawDict = scanState_.rawState.dictionary.values;
    switch (requestedType_->kind()) {
      case TypeKind::INTEGER: {
        auto* dict = reinterpret_cast<const int32_t*>(rawDict);
        // int32 indices → int32 values: same size, safe forward.
        for (auto i = 0; i < numVals; ++i) {
          if (anyNulls_ && bits::isBitNull(rawResultNulls_, i)) {
            continue;
          }
          reinterpret_cast<int32_t*>(rawValues_)[i] = dict[indices[i]];
        }
        break;
      }
      case TypeKind::BIGINT: {
        auto* dict = reinterpret_cast<const int64_t*>(rawDict);
        // int32 indices → int64 values: expand from end to avoid overwrite.
        for (auto i = numVals - 1; i >= 0; --i) {
          if (anyNulls_ && bits::isBitNull(rawResultNulls_, i)) {
            reinterpret_cast<int64_t*>(rawValues_)[i] = 0;
            continue;
          }
          reinterpret_cast<int64_t*>(rawValues_)[i] = dict[indices[i]];
        }
        valueSize_ = sizeof(int64_t);
        break;
      }
      default:
        break;
    }
    isDictOutputEnabled_ = false;
    formatData_->as<ParquetData>().setUseIndexPreservingDict(false);
    scanState_.clear();
    formatData_->as<ParquetData>().clearDictionary();
  }

  void read(
      int64_t offset,
      const RowSet& rows,
      const uint64_t* /*incomingNulls*/) override {
    auto& parquetData = formatData_->as<ParquetData>();
    auto typeKind = requestedType_->kind();
    auto fileKind = fileType_->type()->kind();
    auto* fileTypePtr = fileType_->type().get();

    isDictOutputEnabled_ = false;
    if (fileKind == typeKind && !fileTypePtr->isShortDecimal() &&
        !fileTypePtr->isLongDecimal() &&
        (typeKind == TypeKind::INTEGER || typeKind == TypeKind::BIGINT) &&
        parquetData.outputDictVector()) {
      isDictOutputEnabled_ = true;
      parquetData.setUseIndexPreservingDict(true);
    }

    VELOX_WIDTH_DISPATCH(
        parquetSizeOfIntKind(fileType_->type()->kind()),
        prepareRead,
        offset,
        rows,
        nullptr);
    readCommon<IntegerColumnReader, true>(rows);
    readOffset_ += rows.back() + 1;

    // After readCommon, check if the index-preserving visitor was actually
    // used. If not (e.g., plain page, or cardinality exceeded threshold in
    // callDecoder), the buffer contains resolved values, not indices.
    if (isDictOutputEnabled_) {
      if (!scanState_.dictionary.values ||
          !parquetData.indexPreservingDictUsed()) {
        isDictOutputEnabled_ = false;
      }
    }
    parquetData.setUseIndexPreservingDict(false);
  }

  template <typename ColumnVisitor>
  void readWithVisitor(const RowSet& rows, ColumnVisitor visitor) {
    formatData_->as<ParquetData>().readWithVisitor(visitor);
  }

 private:
  template <typename T>
  void getDictValues(const RowSet& rows, VectorPtr* result) {
    auto& parquetData = formatData_->as<ParquetData>();
    auto dictionaryValues =
        parquetData.template typedDictionaryValues<T>(requestedType_);
    compactScalarValues<int32_t, int32_t>(rows, false);
    *result = std::make_shared<DictionaryVector<T>>(
        memoryPool_, resultNulls(), numValues_, dictionaryValues, values_);
  }

  bool isDictOutputEnabled_{false};
};

} // namespace facebook::velox::parquet
