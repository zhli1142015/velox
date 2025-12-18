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

#include <fmt/core.h>
#include <cstdint>
#include <limits>
#include <string>

namespace facebook::velox::dwio::common {

constexpr uint32_t MAX_UINT32 = std::numeric_limits<uint32_t>::max();

class StreamIdentifier {
 public:
  StreamIdentifier() : id_(MAX_UINT32), columnId_(MAX_UINT32) {}

  explicit StreamIdentifier(int32_t id) : id_(id), columnId_(id) {}

  /// Constructor with separate column id for grouping (used by page skipping).
  StreamIdentifier(int32_t id, int32_t columnId)
      : id_(id), columnId_(columnId) {}

  virtual ~StreamIdentifier() = default;

  virtual int32_t getId() const {
    return id_;
  }

  /// Returns the column id for grouping purposes.
  /// For normal streams this equals id_, for page skipping streams this is
  /// the actual column id while id_ is an encoded unique identifier.
  virtual int32_t getColumnId() const {
    return columnId_;
  }

  virtual bool operator==(const StreamIdentifier& other) const {
    return id_ == other.id_;
  }

  virtual std::size_t hash() const {
    return std::hash<uint32_t>()(id_);
  }

  virtual std::string toString() const {
    return fmt::format("[id={}]", id_);
  }

  /// Returns a special value indicating a stream to be read load quantum by
  /// load quantum.
  static StreamIdentifier sequentialFile() {
    constexpr int32_t kSequentialFile = std::numeric_limits<int32_t>::max() - 1;
    return StreamIdentifier(kSequentialFile);
  }

  int32_t id_;
  int32_t columnId_; // For column grouping in PrefetchBufferedInput
};

struct StreamIdentifierHash {
  std::size_t operator()(const StreamIdentifier& si) const {
    return si.hash();
  }
};

} // namespace facebook::velox::dwio::common
