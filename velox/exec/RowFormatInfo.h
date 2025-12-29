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

#include <cstdint>
#include <utility>
#include <vector>

#include <folly/Range.h>
#include "velox/common/base/BitUtil.h"

namespace facebook::velox::exec {

class RowContainer;
class RowColumn;
class Accumulator;

/// Stores row format information for row-based spill/restore.
/// This struct captures the memory layout of rows in RowContainer,
/// enabling direct serialization/deserialization without intermediate
/// columnar conversion.
struct RowFormatInfo {
  /// Fixed-size portion of each row (header + fixed-width columns)
  /// Note: If rowSizeOffset > 0, fixRowSize is set to rowSizeOffset
  /// to avoid serializing the rowSize field itself.
  int32_t fixRowSize{0};

  /// Offset to nextEqual flag bit (for aggregation bypass hash table)
  /// Corresponds to RowContainer::probedFlagOffset()
  int32_t nextEqualOffset{0};

  /// Offset to row size field (if present, 0 means no variable width)
  int32_t rowSizeOffset{0};

  /// Memory alignment requirement
  int alignment{8};

  /// Variable-width columns information: (isStringView, (offset, nullOffset))
  /// isStringView=true: StringView type (VARCHAR, VARBINARY)
  /// isStringView=false: std::string_view type (complex types serialized)
  /// The pair<int32_t, int32_t> represents (offset, nullOffset) like RowColumn
  std::vector<std::tuple<bool, int32_t, int32_t>> variableColumns;

  /// Column metadata array for all columns: (offset, nullOffset) pairs
  std::vector<std::pair<int32_t, int32_t>> rowColumns;

  /// Serializable accumulators that support direct serialization.
  /// This vector contains accumulators that are not fixed size but support
  /// accumulator serde. For row-based spill, these accumulators can be
  /// serialized directly without extracting to vectors.
  std::vector<const Accumulator*> serializableAccumulators;

  /// Whether compression is enabled
  bool enableCompression{false};

  /// Whether the row data is already in serialized format
  bool serialized{false};

  /// Default constructor
  RowFormatInfo() = default;

  /// Constructor from RowContainer
  RowFormatInfo(RowContainer* container, bool enableCompression);

  /// Calculate total row size including variable-length parts
  /// Note: This version can only be used when there are no serializable
  /// accumulators, as it cannot calculate accumulator serialize sizes.
  /// @param row The row to calculate size for
  /// @return Total row size in bytes (aligned)
  uint32_t getRowSize(const char* row) const;

  /// Calculate total row size including variable-length parts and
  /// serializable accumulator sizes.
  /// @param row The row to calculate size for
  /// @return Total row size in bytes (aligned)
  uint32_t getRowSize(char* row) const;

  /// Calculate total size for multiple rows
  /// @param rows Range of row pointers
  /// @return Total size in bytes for all rows
  uint32_t getRowSize(folly::Range<char**> rows) const;

  /// Helper to check if value is null at given offset/mask
  static bool isNullAt(const char* row, int32_t offset, int32_t nullOffset);

  /// Helper to get column offset from variableColumns entry
  static int32_t getOffset(const std::tuple<bool, int32_t, int32_t>& col) {
    return std::get<1>(col);
  }

  /// Helper to get null offset from variableColumns entry
  static int32_t getNullOffset(const std::tuple<bool, int32_t, int32_t>& col) {
    return std::get<2>(col);
  }

  /// Helper to check if column is StringView type
  static bool isStringView(const std::tuple<bool, int32_t, int32_t>& col) {
    return std::get<0>(col);
  }
};

} // namespace facebook::velox::exec
