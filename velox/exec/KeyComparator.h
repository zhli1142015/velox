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

#include "velox/common/base/BitUtil.h"
#include "velox/exec/VectorHasher.h"

#include <cstring>
#include <vector>

namespace facebook::velox::exec {

/// Per-column cached pointers for batch-level key comparison.
struct KeyCompareColumn {
  const DecodedVector* decoded{nullptr};
  const void* rawData{nullptr};
  TypeKind typeKind{TypeKind::INVALID};
  int32_t typeSize{0}; // Sizeof(element) for fixed-width, 0 for VARCHAR/BOOL.
  const vector_size_t* indices{nullptr}; // Null if flat (identity mapping).
  /// True if the column is a ConstantVector.
  bool isConstant{false};
  /// Index into the base vector for the constant value.
  vector_size_t constantIndex{0};
};

/// Compares keys at two row positions using cached decoded vector pointers.
/// Used by SwissDedup's keysEqual callback in both HashProbe and GroupingSet.
class KeyComparator {
 public:
  /// Prepare the comparator from a set of VectorHashers (already decoded
  /// for the current batch).
  void prepare(const std::vector<std::unique_ptr<VectorHasher>>& hashers) {
    cols_.resize(hashers.size());
    for (size_t i = 0; i < hashers.size(); ++i) {
      auto& dv = hashers[i]->decodedVector();
      auto& col = cols_[i];
      col.decoded = &dv;
      col.typeKind = hashers[i]->typeKind();
      col.isConstant = dv.isConstantMapping();
      col.constantIndex = col.isConstant ? dv.index(0) : 0;
      col.indices =
          (!dv.isIdentityMapping() && !col.isConstant) ? dv.indices() : nullptr;
      if (col.typeKind == TypeKind::VARCHAR ||
          col.typeKind == TypeKind::VARBINARY) {
        col.rawData = dv.data<StringView>();
        col.typeSize = 0;
      } else if (col.typeKind == TypeKind::BOOLEAN) {
        // BOOLEAN is bit-packed; handled specially in operator().
        col.rawData = dv.data<char>();
        col.typeSize = 0;
      } else {
        col.rawData = dv.data<char>();
        col.typeSize =
            static_cast<int32_t>(dv.base()->type()->cppSizeInBytes());
      }
    }
  }

  /// Compare keys at two row indices. Returns true if all key columns match.
  bool operator()(vector_size_t rowA, vector_size_t rowB) const {
    for (const auto& col : cols_) {
      // Constant column: all rows have the same value, always equal.
      if (col.isConstant) {
        continue;
      }

      if (col.decoded->mayHaveNulls()) {
        bool nullA = col.decoded->isNullAt(rowA);
        bool nullB = col.decoded->isNullAt(rowB);
        if (nullA != nullB)
          return false;
        if (nullA)
          continue; // Both null, equal for grouping.
      }

      auto idxA = col.indices ? col.indices[rowA] : rowA;
      auto idxB = col.indices ? col.indices[rowB] : rowB;

      if (col.typeKind == TypeKind::BOOLEAN) {
        auto* base = reinterpret_cast<const uint64_t*>(col.rawData);
        if (bits::isBitSet(base, idxA) != bits::isBitSet(base, idxB)) {
          return false;
        }
        continue;
      }

      if (col.typeSize > 0) {
        auto* base = static_cast<const char*>(col.rawData);
        if (memcmp(
                base + idxA * col.typeSize,
                base + idxB * col.typeSize,
                col.typeSize) != 0) {
          return false;
        }
        continue;
      }

      auto* strings = static_cast<const StringView*>(col.rawData);
      if (strings[idxA] != strings[idxB])
        return false;
    }
    return true;
  }

 private:
  std::vector<KeyCompareColumn> cols_;
};

} // namespace facebook::velox::exec
