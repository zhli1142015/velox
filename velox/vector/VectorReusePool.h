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

#include "velox/common/base/RuntimeMetrics.h"
#include "velox/type/CppToType.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox {

/// Auto-growing pool for reusing VectorPtr instances across batches.
/// Solves the pipeline-depth problem: in A→B→C, downstream B/C hold refs
/// to A's previous output, making single-slot caches always fail.
/// Pool grows to pipeline depth (typically 2-3), capped at maxSize.
class VectorReusePool {
 public:
  static constexpr size_t kDefaultMaxSize = 8;

  explicit VectorReusePool(size_t maxSize = kDefaultMaxSize)
      : maxSize_(maxSize) {}

  /// Returns a reference to a reusable slot (null or use_count==1).
  /// Grows pool if all slots are held. Returns overflow slot if at maxSize.
  VectorPtr& checkout() {
    for (auto& entry : pool_) {
      if (!entry || entry.use_count() == 1) {
        ++hits_;
        return entry;
      }
    }
    ++misses_;
    if (pool_.size() >= maxSize_) {
      overflow_ = nullptr;
      return overflow_;
    }
    pool_.emplace_back(nullptr);
    highWater_ = std::max(highWater_, pool_.size());
    return pool_.back();
  }

  /// High-level helper: checkout a RowVector, reuse or create.
  /// Clears children to break cross-pool reference chains.
  RowVectorPtr checkoutRowVector(
      const RowTypePtr& type,
      vector_size_t size,
      velox::memory::MemoryPool* pool) {
    auto& entry = checkout();
    if (entry && entry.use_count() == 1) {
      VectorPtr vec = std::move(entry);
      BaseVector::prepareForReuse(vec, size);
      entry = std::move(vec);
    } else {
      entry = BaseVector::create<RowVector>(type, size, pool);
    }
    return std::static_pointer_cast<RowVector>(entry);
  }

  /// Detach values/nulls buffers from exclusively-owned FlatVector<T> entries
  /// so that the reader's values_ buffer becomes uniquely owned.
  /// Only modifies entries with use_count==1. Skips unsupported types.
  template <typename T>
  void detachFlatBuffers() {
    if constexpr (requires { CppToType<T>::typeKind; }) {
      constexpr auto targetKind = CppToType<T>::typeKind;
      for (auto& entry : pool_) {
        if (!entry || entry.use_count() != 1 || !entry->isFlatEncoding()) {
          continue;
        }
        if (entry->typeKind() != targetKind) {
          continue;
        }
        auto* flat = entry->template asUnchecked<FlatVector<T>>();
        flat->unsafeSetValues(BufferPtr(nullptr));
        flat->setNulls(BufferPtr(nullptr));
        // Do NOT clear stringBuffers_ — they are std::move'd into each new
        // FlatVector, so they don't cause the refcount sharing problem.
      }
    }
  }

  void maybeShrink(size_t minSize = 2) {
    if (pool_.size() <= minSize) {
      return;
    }
    if (std::all_of(pool_.begin(), pool_.end(), [](auto& e) {
          return !e || e.use_count() == 1;
        })) {
      pool_.resize(minSize);
    }
  }

  void clear() {
    pool_.clear();
    overflow_ = nullptr;
  }

  size_t size() const {
    return pool_.size();
  }
  size_t hits() const {
    return hits_;
  }
  size_t misses() const {
    return misses_;
  }
  size_t highWater() const {
    return highWater_;
  }

  /// Report pool metrics. prefix distinguishes multiple pools per operator.
  /// Example: addStat("outputPoolHits", RuntimeCounter(hits_));
  template <typename AddStatFn>
  void reportMetrics(const std::string& prefix, AddStatFn&& addStat) const {
    addStat(prefix + "PoolHits", RuntimeCounter(static_cast<int64_t>(hits_)));
    addStat(
        prefix + "PoolMisses", RuntimeCounter(static_cast<int64_t>(misses_)));
    addStat(
        prefix + "PoolHighWater",
        RuntimeCounter(static_cast<int64_t>(highWater_)));
  }

 private:
  std::vector<VectorPtr> pool_;
  VectorPtr overflow_;
  size_t maxSize_;
  size_t hits_{0};
  size_t misses_{0};
  size_t highWater_{0};
};

} // namespace facebook::velox
