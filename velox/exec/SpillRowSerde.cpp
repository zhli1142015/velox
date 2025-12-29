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

#include "velox/exec/SpillRowSerde.h"

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/exec/RowContainer.h"

namespace facebook::velox::exec {

// static
void SpillRowSerde::serialize(
    char* row,
    char*& current,
    const char* bufferEnd,
    const RowFormatInfo& info) {
  // 1. Copy fixed-length portion
  simd::memcpy(current, row, info.fixRowSize);
  char* currentHead = current;
  current += info.fixRowSize;

  // 2. Process variable-width columns
  // variableColumns is vector of tuple<isStr, offset, nullOffset>
  for (const auto& varCol : info.variableColumns) {
    bool isStr = std::get<0>(varCol);
    int32_t offset = std::get<1>(varCol);
    int32_t nullOffset = std::get<2>(varCol);

    if (RowFormatInfo::isNullAt(row, offset, nullOffset)) {
      continue;
    }

    if (isStr) {
      // StringView type (VARCHAR, VARBINARY)
      const StringView& sv = *reinterpret_cast<StringView*>(row + offset);
      if (!sv.isInline()) {
        // Non-inline string needs data copy
        const auto* header =
            reinterpret_cast<const HashStringAllocator::Header*>(sv.data()) - 1;
        if (header->size() >= sv.size()) {
          // Contiguous storage
          simd::memcpy(current, sv.data(), sv.size());
        } else {
          // Fragmented storage (linked list)
          HashStringAllocator::InputStream stream(
              HashStringAllocator::headerOf(sv.data()));
          stream.readBytes(reinterpret_cast<uint8_t*>(current), sv.size());
        }

        // Key optimization: Clear StringView's data pointer
        // This improves compression ratio as pointer values are typically
        // random
        *(char**)(currentHead + offset + sizeof(uint64_t)) = nullptr;

        current += sv.size();
        VELOX_CHECK_LE(current, bufferEnd);
      }
    } else {
      // std::string_view type (serialized complex types)
      const auto value = *reinterpret_cast<std::string_view*>(row + offset);
      HashStringAllocator::InputStream stream(
          HashStringAllocator::headerOf(value.data()));
      stream.readBytes(reinterpret_cast<uint8_t*>(current), value.size());
      current += value.size();
      VELOX_CHECK_LE(current, bufferEnd);
    }
  }

  // 3. Serialize accumulator data for non-fixed size accumulators that support
  // serde
  for (const auto* accumulator : info.serializableAccumulators) {
    current = accumulator->serializeAccumulator(row, current);
    VELOX_CHECK_LE(current, bufferEnd);
  }

  // 4. Align to alignment boundary
  auto actualSize = current - currentHead;
  auto rowSize = bits::roundUp(actualSize, info.alignment);
  current += rowSize - actualSize;
}

// static
int32_t SpillRowSerde::deserialize(
    char*& current,
    const RowFormatInfo& info,
    bool advance) {
  // Check alignment
  VELOX_DCHECK_EQ(
      reinterpret_cast<std::uintptr_t>(current) % info.alignment, 0);

  // Variable-length data follows immediately after fixed portion
  char* varBufferStart = current + info.fixRowSize;

  // Fix up variable-width column pointers
  // variableColumns is vector of tuple<isStr, offset, nullOffset>
  for (const auto& varCol : info.variableColumns) {
    bool isStr = std::get<0>(varCol);
    int32_t offset = std::get<1>(varCol);
    int32_t nullOffset = std::get<2>(varCol);

    if (RowFormatInfo::isNullAt(current, offset, nullOffset)) {
      continue;
    }

    if (isStr) {
      StringView& sv = *reinterpret_cast<StringView*>(current + offset);
      if (!sv.isInline()) {
        // Update StringView to point to correct data location
        sv = StringView(varBufferStart, sv.size());
        varBufferStart += sv.size();
      }
    } else {
      auto& value = *reinterpret_cast<std::string_view*>(current + offset);
      value = std::string_view(varBufferStart, value.size());
      varBufferStart += value.size();
    }
  }

  // Deserialize accumulator data for non-fixed size accumulators that support
  // serde
  for (const auto* accumulator : info.serializableAccumulators) {
    varBufferStart =
        accumulator->deserializeAccumulator(current, varBufferStart);
  }

  // Calculate row size (including alignment)
  auto rowSize = varBufferStart - current;
  auto delta = bits::roundUp(rowSize, info.alignment) - rowSize;
  rowSize += delta;

  // Advance pointer based on parameter
  if (advance) {
    current = varBufferStart + delta;
  }

  return rowSize;
}

// static
int32_t SpillRowSerde::rowSize(char* row, const RowFormatInfo& info) {
  int32_t size = info.fixRowSize;

  // Add variable-width column sizes
  // variableColumns is vector of tuple<isStr, offset, nullOffset>
  for (const auto& varCol : info.variableColumns) {
    bool isStr = std::get<0>(varCol);
    int32_t offset = std::get<1>(varCol);
    int32_t nullOffset = std::get<2>(varCol);

    if (RowFormatInfo::isNullAt(row, offset, nullOffset)) {
      continue;
    }

    if (isStr) {
      const StringView& sv = *reinterpret_cast<const StringView*>(row + offset);
      if (!sv.isInline()) {
        size += sv.size();
      }
    } else {
      const auto& value =
          *reinterpret_cast<const std::string_view*>(row + offset);
      size += value.size();
    }
  }

  // Add serializable accumulator sizes
  for (const auto* accumulator : info.serializableAccumulators) {
    size += accumulator->getSerializeSize(row);
  }

  return bits::roundUp(size, info.alignment);
}

// static
void SpillRowSerde::copyRow(
    char*& dest,
    const char* src,
    size_t length,
    const RowFormatInfo& info) {
  simd::memcpy(dest, src, length);
  dest += length;
}

} // namespace facebook::velox::exec
