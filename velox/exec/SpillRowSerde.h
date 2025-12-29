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

#include "velox/exec/RowFormatInfo.h"

namespace facebook::velox::exec {

/// Serializes and deserializes RowContainer rows for direct disk spill.
/// This avoids the overhead of converting to/from columnar format.
class SpillRowSerde {
 public:
  /// Serialize a row to buffer.
  /// @param row Source row from RowContainer
  /// @param current Current position in output buffer (updated after write)
  /// @param bufferEnd End of output buffer (for bounds checking)
  /// @param info Row format metadata
  static void serialize(
      char* row,
      char*& current,
      const char* bufferEnd,
      const RowFormatInfo& info);

  /// Deserialize a row from buffer.
  /// Updates pointers in the row to point to the variable-length data
  /// that follows the fixed-size portion.
  /// @param current Current position in input buffer (updated if advance=true)
  /// @param info Row format metadata
  /// @param advance Whether to advance current pointer after deserialization
  /// @return Size of the deserialized row
  static int32_t
  deserialize(char*& current, const RowFormatInfo& info, bool advance = true);

  /// Calculate serialized size of a row.
  /// @param row The row to calculate size for
  /// @param info Row format metadata
  /// @return Serialized size in bytes
  static int32_t rowSize(char* row, const RowFormatInfo& info);

  /// Copy row with pointer fixup for variable-length data.
  /// @param dest Destination buffer (updated after copy)
  /// @param src Source row
  /// @param length Number of bytes to copy
  /// @param info Row format metadata
  static void copyRow(
      char*& dest,
      const char* src,
      size_t length,
      const RowFormatInfo& info);
};

} // namespace facebook::velox::exec
