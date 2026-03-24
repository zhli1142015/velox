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

#include "velox/dwio/common/SelectiveFloatingPointColumnReader.h"
#include "velox/vector/DictionaryVector.h"

namespace facebook::velox::parquet {

template <typename TData, typename TRequested>
class FloatingPointColumnReader
    : public dwio::common::
          SelectiveFloatingPointColumnReader<TData, TRequested> {
 public:
  using ValueType = TRequested;

  using base =
      dwio::common::SelectiveFloatingPointColumnReader<TData, TRequested>;

  FloatingPointColumnReader(
      const TypePtr& requestedType,
      std::shared_ptr<const dwio::common::TypeWithId> fileType,
      ParquetParams& params,
      common::ScanSpec& scanSpec);

  // Parquet floating point reader always supports a bulk path
  static constexpr bool kHasBulkPath = true;

  bool hasBulkPath() const override {
    return kHasBulkPath;
  }

  void seekToRowGroup(int64_t index) override {
    base::seekToRowGroup(index);
    this->scanState().clear();
    this->readOffset_ = 0;
    this->formatData_->template as<ParquetData>().seekToRowGroup(index);
  }

  uint64_t skip(uint64_t numValues) override;

  void getValues(const RowSet& rows, VectorPtr* result) override {
    auto& parquetData = this->formatData_->template as<ParquetData>();
    // Check if we should output DictionaryVector.
    if (this->scanState_.dictionary.values && isDictOutputEnabled_) {
      auto dictionaryValues =
          parquetData.template typedDictionaryValues<TRequested>(
              this->requestedType_);
      this->template compactScalarValues<int32_t, int32_t>(rows, false);
      *result = std::make_shared<DictionaryVector<TRequested>>(
          this->memoryPool_,
          this->resultNulls(),
          this->numValues_,
          dictionaryValues,
          this->values_);
      return;
    }
    this->template getFlatValues<TData, TRequested>(
        rows, result, this->requestedType_);
  }

  void dedictionarize() override {
    if (!this->scanSpec_->keepValues() || !isDictOutputEnabled_) {
      this->scanState_.clear();
      return;
    }
    auto& parquetData = this->formatData_->template as<ParquetData>();
    // Check if the index-preserving visitor was actually used for the
    // previous dict pages. If not (cardinality exceeded threshold), the
    // buffer contains resolved values, not indices — just clear state.
    if (!parquetData.indexPreservingDictUsed()) {
      isDictOutputEnabled_ = false;
      parquetData.setUseIndexPreservingDict(false);
      this->scanState_.clear();
      return;
    }
    // Use raw dictionary pointer directly — avoids allocating a temporary
    // FlatVector wrapper just to read values.
    auto* dict = reinterpret_cast<const TRequested*>(
        this->scanState_.rawState.dictionary.values);
    auto* indices = this->values_->template asMutable<int32_t>();
    auto numVals = this->numValues_;
    // int32 indices → TRequested values: expand from end to avoid overwrite
    // since sizeof(TRequested) >= sizeof(int32_t).
    for (auto i = numVals - 1; i >= 0; --i) {
      if (this->anyNulls_ && bits::isBitNull(this->rawResultNulls_, i)) {
        reinterpret_cast<TRequested*>(this->rawValues_)[i] = TRequested();
        continue;
      }
      reinterpret_cast<TRequested*>(this->rawValues_)[i] = dict[indices[i]];
    }
    this->valueSize_ = sizeof(TRequested);
    isDictOutputEnabled_ = false;
    parquetData.setUseIndexPreservingDict(false);
    this->scanState_.clear();
    parquetData.clearDictionary();
  }

  void read(int64_t offset, const RowSet& rows, const uint64_t* incomingNulls)
      override {
    using T = FloatingPointColumnReader<TData, TRequested>;
    auto& parquetData = this->formatData_->template as<ParquetData>();

    // Enable index-preserving dict mode speculatively based on config.
    // Only for sizeof(TRequested) >= 4 (float and double both qualify).
    // Disable for type evolution cases where TData != TRequested (e.g.,
    // float→double) since the dict buffer holds TData values, not TRequested.
    isDictOutputEnabled_ =
        std::is_same_v<TData, TRequested> && parquetData.outputDictVector();
    parquetData.setUseIndexPreservingDict(isDictOutputEnabled_);

    this->template readCommon<T, true>(offset, rows, incomingNulls);
    this->readOffset_ += rows.back() + 1;

    // After readCommon, check if the index-preserving visitor was actually
    // used. If not (e.g., plain page, or cardinality exceeded threshold in
    // callDecoder), the buffer contains resolved values, not indices.
    if (isDictOutputEnabled_) {
      if (!this->scanState_.dictionary.values ||
          !parquetData.indexPreservingDictUsed()) {
        isDictOutputEnabled_ = false;
      }
    }
    parquetData.setUseIndexPreservingDict(false);
  }

  template <typename TVisitor>
  void readWithVisitor(const RowSet& rows, TVisitor visitor);

 private:
  bool isDictOutputEnabled_{false};
};

template <typename TData, typename TRequested>
FloatingPointColumnReader<TData, TRequested>::FloatingPointColumnReader(
    const TypePtr& requestedType,
    std::shared_ptr<const dwio::common::TypeWithId> fileType,
    ParquetParams& params,
    common::ScanSpec& scanSpec)
    : dwio::common::SelectiveFloatingPointColumnReader<TData, TRequested>(
          requestedType,
          std::move(fileType),
          params,
          scanSpec) {
  VELOX_DCHECK(
      (this->requestedType_->kind() == TypeKind::REAL &&
       std::is_same_v<TRequested, float>) ||
          (this->requestedType_->kind() == TypeKind::DOUBLE &&
           std::is_same_v<TRequested, double>),
      "TRequested type mismatch: template parameter is {}, but requestedType is {}",
      folly::demangle(typeid(TRequested)),
      this->requestedType_->toString());
}

template <typename TData, typename TRequested>
uint64_t FloatingPointColumnReader<TData, TRequested>::skip(
    uint64_t numValues) {
  return this->formatData_->skip(numValues);
}

template <typename TData, typename TRequested>
template <typename TVisitor>
void FloatingPointColumnReader<TData, TRequested>::readWithVisitor(
    const RowSet& rows,
    TVisitor visitor) {
  this->formatData_->template as<ParquetData>().readWithVisitor(visitor);
}

} // namespace facebook::velox::parquet
