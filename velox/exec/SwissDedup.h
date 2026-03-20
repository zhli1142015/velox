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

#ifdef __SSE2__
#include <immintrin.h>
#endif
#include "velox/common/base/BitUtil.h"
#include "velox/common/memory/RawVector.h"
#include "velox/exec/HashTable.h"

namespace facebook::velox::exec {

/// Batch-level dedup: finds duplicate keys within a probe/agg batch using
/// already-computed hashes. Dispatches to one of three algorithms:
///   kArray small range: DirectIndex (0.3ns/row)
///   kArray large / kNK: PersistentSlot (1.0ns/row, no memset between batches)
///   kHash:              SwissTable with SIMD tags (2.7ns/row)
class SwissDedup {
 public:
  static constexpr int32_t kGroupWidth = 16;
  static constexpr int32_t kMinRows = 64;
  static constexpr int32_t kEarlyStopRowAgg = 63;
  static constexpr int32_t kEarlyStopRowProbe = 127;
  static constexpr int64_t kDirectIndexMaxRange = 32768;
  /// EMA decay factor for success rate tracking.
  static constexpr double kDecayFactor = 0.95;
  /// Minimum success rate (EMA) to keep dedup active.
  static constexpr double kMinSuccessRate = 0.10;
  /// Minimum batches before auto-disable can trigger.
  static constexpr int32_t kMinBatchesBeforeDisable = 20;

  enum class State { kActive, kDisabled };

  static bool shouldAttempt(int32_t numRows) {
    return numRows >= kMinRows;
  }

  /// Estimates probe cost in nanoseconds based on hash table size.
  static double estimateProbeCostNs(uint64_t htDistinct) {
    if (htDistinct <= 4096) {
      return 5.0; // L1.
    } else if (htDistinct <= 65536) {
      return 15.0; // L2.
    } else if (htDistinct <= 2000000) {
      return 30.0; // L3.
    } else {
      return 60.0; // Main memory.
    }
  }

  State state() const {
    return state_;
  }

  /// Called after each batch where dedup was attempted. Updates the
  /// exponential moving average of the success rate. Disables dedup when
  /// the rate drops below kMinSuccessRate after kMinBatchesBeforeDisable.
  void trackDedupOutcome(bool used) {
    if (state_ != State::kActive) {
      return;
    }
    successRate_ =
        successRate_ * kDecayFactor + (used ? (1.0 - kDecayFactor) : 0.0);
    if (++totalBatches_ >= kMinBatchesBeforeDisable &&
        successRate_ < kMinSuccessRate) {
      state_ = State::kDisabled;
    }
  }

  /// Main entry point. Template on KeysEqual for zero-overhead comparison.
  /// KeysEqual: bool(vector_size_t rowA, vector_size_t rowB) — true if same
  /// key. Pass nullptr_t for kArray/kNK modes (no comparison needed).
  ///
  /// @param mode Hash table mode.
  /// @param hashes Pre-computed hashes (indexed by row number).
  /// @param rows Array of active row indices.
  /// @param numRows Number of active rows.
  /// @param uniqueRows Output: unique row indices (first occurrence of each
  /// key).
  /// @param result Output: result[row] = first row with same key.
  /// @param arrayRangeSize For kArray mode: the range size (0 if unknown).
  /// @param earlyStopRow Row index at which to check early-stop condition.
  /// @param keysEqual Comparator for kHash mode.
  /// @return Number of unique rows written to uniqueRows.
  template <typename KeysEqual = std::nullptr_t>
  int32_t compute(
      BaseHashTable::HashMode mode,
      const uint64_t* __restrict__ hashes,
      const vector_size_t* rows,
      int32_t numRows,
      vector_size_t* __restrict__ uniqueRows,
      vector_size_t* __restrict__ result,
      int64_t arrayRangeSize = 0,
      int32_t earlyStopRow = kEarlyStopRowAgg,
      const KeysEqual& keysEqual = KeysEqual{}) {
    switch (mode) {
      case BaseHashTable::HashMode::kArray:
        if (arrayRangeSize > 0 && arrayRangeSize <= kDirectIndexMaxRange) {
          return computeDirectIndex(
              hashes,
              rows,
              numRows,
              uniqueRows,
              result,
              static_cast<int32_t>(arrayRangeSize));
        }
        [[fallthrough]];

      case BaseHashTable::HashMode::kNormalizedKey:
        return computePerfect(
            hashes, rows, numRows, uniqueRows, result, earlyStopRow);

      case BaseHashTable::HashMode::kHash:
        if constexpr (std::is_null_pointer_v<KeysEqual>) {
          // No comparator — cannot safely dedup kHash.
          for (int32_t i = 0; i < numRows; ++i) {
            uniqueRows[i] = rows[i];
            result[rows[i]] = rows[i];
          }
          return numRows;
        } else {
          return computeSwissTable(
              hashes,
              rows,
              numRows,
              uniqueRows,
              result,
              earlyStopRow,
              keysEqual);
        }
    }
    return numRows;
  }

  /// Fast path for dictionary-encoded single-column keys.
  /// Uses dictionary indices as direct lookup keys (~0.3ns/row).
  /// No hash computation or key comparison needed: same dict index
  /// guarantees same key value.
  ///
  /// @param dictIndices DecodedVector::indices() — maps row → dict entry.
  /// @param rows Array of active row indices.
  /// @param numRows Number of active rows.
  /// @param uniqueRows Output: unique row indices (first occurrence).
  /// @param result Output: result[row] = first row with same dict entry.
  /// @param dictSize Number of entries in the dictionary (base vector size).
  /// @return Number of unique rows.
  int32_t computeWithDictionary(
      const vector_size_t* dictIndices,
      const vector_size_t* rows,
      int32_t numRows,
      vector_size_t* uniqueRows,
      vector_size_t* result,
      int32_t dictSize) {
    // Reuse DirectIndex infrastructure (directSeq_ / directFirstRow_).
    ensureDirectIndexCapacity(dictSize);
    directBatch_ = nextBatch(directBatch_, directSeq_.data(), directCapacity_);

    int32_t numUnique = 0;
    for (int32_t i = 0; i < numRows; ++i) {
      if (i + kPrefetchAhead < numRows) {
        auto futureIdx = dictIndices[rows[i + kPrefetchAhead]];
        __builtin_prefetch(directSeq_.data() + futureIdx, 0, 1);
      }
      auto row = rows[i];
      auto dictIdx = dictIndices[row];
      if (directSeq_[dictIdx] != directBatch_) {
        directSeq_[dictIdx] = directBatch_;
        directFirstRow_[dictIdx] = row;
        uniqueRows[numUnique++] = row;
        result[row] = row;
      } else {
        result[row] = directFirstRow_[dictIdx];
      }
    }
    return numUnique;
  }

  /// Result of computeAutoDetect().
  struct DedupResult {
    int32_t numUnique;
    /// True if the dictionary fast path was used.
    bool usedDictPath;
  };

  /// Unified entry point: tries dictionary path first (if dictIndices given
  /// and dictSize is small enough), falls back to hash-based compute().
  /// Caller should call trackDedupOutcome() after checking profitability.
  template <typename KeysEqual = std::nullptr_t>
  DedupResult computeAutoDetect(
      BaseHashTable::HashMode mode,
      const uint64_t* __restrict__ hashes,
      const vector_size_t* rows,
      int32_t numRows,
      vector_size_t* __restrict__ uniqueRows,
      vector_size_t* __restrict__ result,
      const vector_size_t* dictIndices,
      int32_t dictSize,
      int64_t arrayRangeSize = 0,
      int32_t earlyStopRow = kEarlyStopRowAgg,
      const KeysEqual& keysEqual = KeysEqual{}) {
    // Dictionary fast path: ~0.3ns/row, no hash/compare needed.
    // Require dictSize <= 90% of numRows (at least 10% duplicates) to justify
    // the overhead of buffer allocation and full-batch traversal.
    if (dictIndices && dictSize > 0 && dictSize <= numRows * 9 / 10 &&
        dictSize <= kDirectIndexMaxRange) {
      auto numUnique = computeWithDictionary(
          dictIndices, rows, numRows, uniqueRows, result, dictSize);
      return {numUnique, true};
    }

    // Hash-based path.
    auto numUnique = compute(
        mode,
        hashes,
        rows,
        numRows,
        uniqueRows,
        result,
        arrayRangeSize,
        earlyStopRow,
        keysEqual);
    return {numUnique, false};
  }

 private:
  // ═══════════════════════════════════════════════════
  // Path 1: DirectIndex — kArray small range (≤32K)
  // 0.3ns/row. Direct array: table[valueId] = firstRow.
  // ═══════════════════════════════════════════════════
  int32_t computeDirectIndex(
      const uint64_t* hashes,
      const vector_size_t* rows,
      int32_t numRows,
      vector_size_t* uniqueRows,
      vector_size_t* result,
      int32_t rangeSize) {
    ensureDirectIndexCapacity(rangeSize);
    directBatch_ = nextBatch(directBatch_, directSeq_.data(), directCapacity_);

    int32_t numUnique = 0;
    for (int32_t i = 0; i < numRows; ++i) {
      if (i + kPrefetchAhead < numRows) {
        auto futureId = static_cast<uint32_t>(hashes[rows[i + kPrefetchAhead]]);
        __builtin_prefetch(directSeq_.data() + futureId, 0, 1);
      }
      auto row = rows[i];
      auto id = static_cast<uint32_t>(hashes[row]);
      if (directSeq_[id] != directBatch_) {
        directSeq_[id] = directBatch_;
        directFirstRow_[id] = row;
        uniqueRows[numUnique++] = row;
        result[row] = row;
      } else {
        result[row] = directFirstRow_[id];
      }
    }
    return numUnique;
  }

  // ═══════════════════════════════════════════════════
  // Path 2: PersistentSlot — kArray large / kNK
  // 1.0ns/row. No memset between batches (batchSeq trick).
  // NK = perfect mapping → hash match = key match.
  // ═══════════════════════════════════════════════════
  int32_t computePerfect(
      const uint64_t* hashes,
      const vector_size_t* rows,
      int32_t numRows,
      vector_size_t* uniqueRows,
      vector_size_t* result,
      int32_t earlyStopRow) {
    ensureSlotCapacity(numRows);
    if (FOLLY_UNLIKELY(currentBatch_ == std::numeric_limits<uint32_t>::max())) {
      for (int32_t i = 0; i < slotCapacity_; ++i) {
        slots_[i].batchSeq = 0;
      }
      currentBatch_ = 0;
    }
    currentBatch_++;

    int32_t numUnique = 0;
    auto* slots = slots_.data();
    auto batch = currentBatch_;
    auto limit = (earlyStopRow > 0 && earlyStopRow < numRows) ? earlyStopRow + 1
                                                              : numRows;

    for (int32_t i = 0; i < limit; ++i) {
      if (i + kPrefetchAhead < numRows) {
        auto futureIdx =
            static_cast<int32_t>(hashes[rows[i + kPrefetchAhead]]) & slotMask_;
        __builtin_prefetch(slots + futureIdx, 1, 1);
      }
      auto row = rows[i];
      auto h = hashes[row];
      auto idx = static_cast<int32_t>(h) & slotMask_;

      bool resolved = false;
      for (int p = 0; p < kMaxProbe; ++p) {
        auto& s = slots[idx];
        if (s.batchSeq != batch) {
          s.key = h;
          s.firstRow = row;
          s.batchSeq = batch;
          uniqueRows[numUnique++] = row;
          result[row] = row;
          resolved = true;
          break;
        }
        if (s.key == h) {
          result[row] = s.firstRow;
          resolved = true;
          break;
        }
        idx = (idx + 1) & slotMask_;
      }
      if (!resolved) {
        uniqueRows[numUnique++] = row;
        result[row] = row;
      }
    }

    if (limit < numRows && numUnique == limit) {
      for (int32_t j = limit; j < numRows; ++j) {
        uniqueRows[numUnique++] = rows[j];
        result[rows[j]] = rows[j];
      }
      return numUnique;
    }

    for (int32_t i = limit; i < numRows; ++i) {
      if (i + kPrefetchAhead < numRows) {
        auto futureIdx =
            static_cast<int32_t>(hashes[rows[i + kPrefetchAhead]]) & slotMask_;
        __builtin_prefetch(slots + futureIdx, 1, 1);
      }
      auto row = rows[i];
      auto h = hashes[row];
      auto idx = static_cast<int32_t>(h) & slotMask_;

      bool resolved = false;
      for (int p = 0; p < kMaxProbe; ++p) {
        auto& s = slots[idx];
        if (s.batchSeq != batch) {
          s.key = h;
          s.firstRow = row;
          s.batchSeq = batch;
          uniqueRows[numUnique++] = row;
          result[row] = row;
          resolved = true;
          break;
        }
        if (s.key == h) {
          result[row] = s.firstRow;
          resolved = true;
          break;
        }
        idx = (idx + 1) & slotMask_;
      }
      if (!resolved) {
        uniqueRows[numUnique++] = row;
        result[row] = row;
      }
    }
    return numUnique;
  }

  // ═══════════════════════════════════════════════════
  // Path 3: SwissTable — kHash mode
  // 2.7ns/row. SSE2 16-wide tag compare. keysEqual on tag match.
  // Tags memset every batch. No full hash stored (uses input array).
  // ═══════════════════════════════════════════════════
  template <typename KeysEqual>
  int32_t computeSwissTable(
      const uint64_t* __restrict__ hashes,
      const vector_size_t* rows,
      int32_t numRows,
      vector_size_t* uniqueRows,
      vector_size_t* result,
      int32_t earlyStopRow,
      const KeysEqual& keysEqual) {
    ensureTagCapacity(numRows);
    tagBatch_ = nextBatch(
        tagBatch_,
        tagSeq_.data(),
        (tagCapacity_ + kGroupWidth - 1) / kGroupWidth);
    auto batch = tagBatch_;

    int32_t numUnique = 0;
    auto limit = (earlyStopRow > 0 && earlyStopRow < numRows) ? earlyStopRow + 1
                                                              : numRows;

    for (int32_t i = 0; i < numRows; ++i) {
      if (i + kPrefetchAhead < numRows) {
        auto futureHash = hashes[rows[i + kPrefetchAhead]];
        auto futureGroup = makeGroup(futureHash);
        __builtin_prefetch(tags_.data() + futureGroup * kGroupWidth, 1, 1);
        __builtin_prefetch(tagSeq_.data() + futureGroup, 0, 1);
      }
      auto row = rows[i];
      auto hash = hashes[row];
      auto tag = makeTag(hash);
      auto group = makeGroup(hash);

      bool resolved = false;
      for (;;) {
        auto base = group * kGroupWidth;

        if (tagSeq_[group] != batch) {
          memset(tags_.data() + base, 0, kGroupWidth);
          tagSeq_[group] = batch;
          tags_[base] = tag;
          tagFirstRow_[base] = row;
          uniqueRows[numUnique++] = row;
          result[row] = row;
          break;
        }

#ifdef __SSE2__
        auto tagGroup = _mm_loadu_si128(
            reinterpret_cast<const __m128i*>(tags_.data() + base));
        auto targetVec = _mm_set1_epi8(static_cast<char>(tag));

        auto matchMask = static_cast<uint32_t>(
            _mm_movemask_epi8(_mm_cmpeq_epi8(tagGroup, targetVec)));
        while (matchMask) {
          auto bit = __builtin_ctz(matchMask);
          auto slot = base + bit;
          auto storedRow = tagFirstRow_[slot];
          if (hashes[storedRow] == hash && keysEqual(storedRow, row)) {
            result[row] = storedRow;
            resolved = true;
            break;
          }
          matchMask &= matchMask - 1;
        }
        if (resolved)
          break;

        auto emptyMask = static_cast<uint32_t>(
            _mm_movemask_epi8(_mm_cmpeq_epi8(tagGroup, _mm_setzero_si128())));
        if (emptyMask) {
          auto bit = __builtin_ctz(emptyMask);
          auto slot = base + bit;
          tags_[slot] = tag;
          tagFirstRow_[slot] = row;
          uniqueRows[numUnique++] = row;
          result[row] = row;
          resolved = true;
          break;
        }
#else
        int32_t emptySlot = -1;
        for (int32_t s = 0; s < kGroupWidth; ++s) {
          auto slot = base + s;
          if (tags_[slot] == tag) {
            auto storedRow = tagFirstRow_[slot];
            if (hashes[storedRow] == hash && keysEqual(storedRow, row)) {
              result[row] = storedRow;
              resolved = true;
              break;
            }
          } else if (tags_[slot] == 0 && emptySlot < 0) {
            emptySlot = slot;
          }
        }
        if (resolved)
          break;
        if (emptySlot >= 0) {
          tags_[emptySlot] = tag;
          tagFirstRow_[emptySlot] = row;
          uniqueRows[numUnique++] = row;
          result[row] = row;
          resolved = true;
          break;
        }
#endif
        group = (group + 1) & tagGroupMask_;
      }
      if (i + 1 == limit && limit < numRows && numUnique == limit) {
        for (int32_t j = limit; j < numRows; ++j) {
          uniqueRows[numUnique++] = rows[j];
          result[rows[j]] = rows[j];
        }
        return numUnique;
      }
    }
    return numUnique;
  }

  // ═══════════════════════════════════════════════════
  // Helpers
  // ═══════════════════════════════════════════════════

  /// Increments a batch counter, resetting the sequence array on wrap-around.
  /// Wrap-around at UINT32_MAX happens once every ~4B batches (~50 days at
  /// 1000 batches/sec). The cost is one memset for the affected path.
  static uint32_t
  nextBatch(uint32_t current, uint32_t* seqArray, int32_t seqCount) {
    if (FOLLY_UNLIKELY(current == std::numeric_limits<uint32_t>::max())) {
      memset(seqArray, 0, seqCount * sizeof(uint32_t));
      return 1;
    }
    return current + 1;
  }

  static uint8_t makeTag(uint64_t hash) {
    return static_cast<uint8_t>(hash >> 57) | 0x80;
  }

  int32_t makeGroup(uint64_t hash) const {
    return static_cast<int32_t>(hash) & tagGroupMask_;
  }

  void ensureSlotCapacity(int32_t numRows) {
    auto needed = static_cast<int32_t>(
        bits::nextPowerOfTwo(static_cast<uint64_t>(numRows) * 2));
    if (needed > slotCapacity_) {
      slotCapacity_ = needed;
      slotMask_ = needed - 1;
      slots_.resize(needed);
      for (int32_t i = 0; i < needed; ++i) {
        slots_[i].batchSeq = 0;
      }
    }
  }

  void ensureDirectIndexCapacity(int32_t rangeSize) {
    if (rangeSize > directCapacity_) {
      directCapacity_ = rangeSize;
      directFirstRow_.resize(rangeSize);
      directSeq_.resize(rangeSize);
      memset(directSeq_.data(), 0, rangeSize * sizeof(uint32_t));
    }
  }

  void ensureTagCapacity(int32_t numRows) {
    auto needed = static_cast<int32_t>(
        bits::nextPowerOfTwo(static_cast<uint64_t>(numRows) * 2));
    if (needed < kGroupWidth) {
      needed = kGroupWidth;
    }
    if (needed > tagCapacity_) {
      tagCapacity_ = needed;
      auto numGroups = needed / kGroupWidth;
      tagGroupMask_ = numGroups - 1;
      tags_.resize(needed);
      tagFirstRow_.resize(needed);
      tagSeq_.resize(numGroups);
      memset(tagSeq_.data(), 0, numGroups * sizeof(uint32_t));
    }
  }

  // PersistentSlot AoS layout (kArray large + kNK).
  // 16 bytes per slot — 2048 slots = 32KB fits L1 cache.
  struct Slot {
    uint64_t key;
    vector_size_t firstRow;
    uint32_t batchSeq;
  };
  int32_t slotCapacity_ = 0;
  int32_t slotMask_ = 0;
  uint32_t currentBatch_ = 0;
  raw_vector<Slot> slots_;

  // Swiss Table members (kHash).
  int32_t tagCapacity_ = 0;
  int32_t tagGroupMask_ = 0;
  uint32_t tagBatch_ = 0;
  raw_vector<uint8_t> tags_;
  raw_vector<vector_size_t> tagFirstRow_;
  raw_vector<uint32_t> tagSeq_; // Per-group batch sequence.

  // DirectIndex members (kArray small range).
  int32_t directCapacity_{0};
  uint32_t directBatch_{0};
  raw_vector<vector_size_t> directFirstRow_;
  raw_vector<uint32_t> directSeq_;

  // Adaptive state.
  State state_{State::kActive};
  double successRate_{1.0};
  int32_t totalBatches_{0};

  static constexpr int32_t kMaxProbe = 8;
  static constexpr int32_t kPrefetchAhead = 4;
};

} // namespace facebook::velox::exec
