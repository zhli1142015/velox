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
#include "velox/exec/SwissDedup.h"
#include <gtest/gtest.h>
#include "velox/exec/KeyComparator.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

class SwissDedupTest : public testing::Test {};

TEST_F(SwissDedupTest, shouldAttempt) {
  EXPECT_FALSE(SwissDedup::shouldAttempt(10));
  EXPECT_FALSE(SwissDedup::shouldAttempt(63));
  EXPECT_TRUE(SwissDedup::shouldAttempt(64));
  EXPECT_TRUE(SwissDedup::shouldAttempt(1000));
}

TEST_F(SwissDedupTest, directIndexAllUnique) {
  SwissDedup dedup;
  // 100 rows, all unique hash values 0..99.
  const int32_t N = 100;
  std::vector<uint64_t> hashes(N);
  std::vector<vector_size_t> rows(N);
  for (int i = 0; i < N; ++i) {
    hashes[i] = i;
    rows[i] = i;
  }
  std::vector<vector_size_t> uniqueRows(N);
  std::vector<vector_size_t> result(N);

  auto numUnique = dedup.compute(
      BaseHashTable::HashMode::kArray,
      hashes.data(),
      rows.data(),
      N,
      uniqueRows.data(),
      result.data(),
      /*arrayRangeSize=*/100);

  EXPECT_EQ(numUnique, N);
  for (int i = 0; i < N; ++i) {
    EXPECT_EQ(result[i], i);
  }
}

TEST_F(SwissDedupTest, directIndexWithDups) {
  SwissDedup dedup;
  // 8 rows, keys cycle through 0,1,2 → 3 unique.
  const int32_t N = 8;
  std::vector<uint64_t> hashes = {0, 1, 2, 0, 1, 2, 0, 1};
  std::vector<vector_size_t> rows = {0, 1, 2, 3, 4, 5, 6, 7};
  std::vector<vector_size_t> uniqueRows(N);
  std::vector<vector_size_t> result(N);

  auto numUnique = dedup.compute(
      BaseHashTable::HashMode::kArray,
      hashes.data(),
      rows.data(),
      N,
      uniqueRows.data(),
      result.data(),
      /*arrayRangeSize=*/3);

  EXPECT_EQ(numUnique, 3);
  // uniqueRows should be [0, 1, 2].
  EXPECT_EQ(uniqueRows[0], 0);
  EXPECT_EQ(uniqueRows[1], 1);
  EXPECT_EQ(uniqueRows[2], 2);
  // result: each row maps to the first occurrence.
  EXPECT_EQ(result[0], 0);
  EXPECT_EQ(result[1], 1);
  EXPECT_EQ(result[2], 2);
  EXPECT_EQ(result[3], 0); // dup of row 0
  EXPECT_EQ(result[4], 1); // dup of row 1
  EXPECT_EQ(result[5], 2); // dup of row 2
  EXPECT_EQ(result[6], 0); // dup of row 0
  EXPECT_EQ(result[7], 1); // dup of row 1
}

TEST_F(SwissDedupTest, persistentSlotNK) {
  SwissDedup dedup;
  // Normalized key mode with duplicates.
  const int32_t N = 100;
  std::vector<uint64_t> hashes(N);
  std::vector<vector_size_t> rows(N);
  for (int i = 0; i < N; ++i) {
    hashes[i] = i % 10; // 10 unique keys
    rows[i] = i;
  }
  std::vector<vector_size_t> uniqueRows(N);
  std::vector<vector_size_t> result(N);

  auto numUnique = dedup.compute(
      BaseHashTable::HashMode::kNormalizedKey,
      hashes.data(),
      rows.data(),
      N,
      uniqueRows.data(),
      result.data(),
      /*arrayRangeSize=*/0,
      /*earlyStopRow=*/-1); // disable early stop

  EXPECT_EQ(numUnique, 10);
  // Each duplicate maps to the first occurrence with the same hash.
  for (int i = 0; i < N; ++i) {
    EXPECT_EQ(result[i], i % 10);
  }
}

TEST_F(SwissDedupTest, persistentSlotEarlyStop) {
  SwissDedup dedup;
  // All unique keys → early stop should trigger.
  const int32_t N = 200;
  std::vector<uint64_t> hashes(N);
  std::vector<vector_size_t> rows(N);
  for (int i = 0; i < N; ++i) {
    hashes[i] = i + 1; // all unique (non-zero)
    rows[i] = i;
  }
  std::vector<vector_size_t> uniqueRows(N);
  std::vector<vector_size_t> result(N);

  auto numUnique = dedup.compute(
      BaseHashTable::HashMode::kNormalizedKey,
      hashes.data(),
      rows.data(),
      N,
      uniqueRows.data(),
      result.data(),
      /*arrayRangeSize=*/0,
      /*earlyStopRow=*/63);

  // Early stop at row 63: all 200 rows should be returned as unique.
  EXPECT_EQ(numUnique, N);
  for (int i = 0; i < N; ++i) {
    EXPECT_EQ(result[i], i);
  }
}

TEST_F(SwissDedupTest, swissTableKHash) {
  SwissDedup dedup;
  // kHash mode with duplicates. Need keysEqual comparator.
  const int32_t N = 100;
  std::vector<uint64_t> hashes(N);
  std::vector<vector_size_t> rows(N);
  std::vector<int64_t> keys(N);
  for (int i = 0; i < N; ++i) {
    keys[i] = i % 5; // 5 unique keys
    hashes[i] = std::hash<int64_t>{}(keys[i]);
    rows[i] = i;
  }
  std::vector<vector_size_t> uniqueRows(N);
  std::vector<vector_size_t> result(N);

  auto keysEqual = [&](vector_size_t a, vector_size_t b) {
    return keys[a] == keys[b];
  };

  auto numUnique = dedup.compute(
      BaseHashTable::HashMode::kHash,
      hashes.data(),
      rows.data(),
      N,
      uniqueRows.data(),
      result.data(),
      /*arrayRangeSize=*/0,
      /*earlyStopRow=*/-1,
      keysEqual);

  EXPECT_EQ(numUnique, 5);
  // Each duplicate maps to the first row with the same key.
  for (int i = 0; i < N; ++i) {
    EXPECT_EQ(keys[result[i]], keys[i]);
    EXPECT_LE(result[i], i);
  }
}

TEST_F(SwissDedupTest, swissTableHashCollision) {
  SwissDedup dedup;
  // kHash: same hash but different keys → NOT deduped.
  const int32_t N = 100;
  std::vector<uint64_t> hashes(N, 42); // ALL same hash
  std::vector<vector_size_t> rows(N);
  std::vector<int64_t> keys(N);
  for (int i = 0; i < N; ++i) {
    keys[i] = i; // all different keys
    rows[i] = i;
  }
  std::vector<vector_size_t> uniqueRows(N);
  std::vector<vector_size_t> result(N);

  auto keysEqual = [&](vector_size_t a, vector_size_t b) {
    return keys[a] == keys[b];
  };

  auto numUnique = dedup.compute(
      BaseHashTable::HashMode::kHash,
      hashes.data(),
      rows.data(),
      N,
      uniqueRows.data(),
      result.data(),
      /*arrayRangeSize=*/0,
      /*earlyStopRow=*/-1,
      keysEqual);

  // All keys are different despite same hash → all unique.
  EXPECT_EQ(numUnique, N);
  for (int i = 0; i < N; ++i) {
    EXPECT_EQ(result[i], i);
  }
}

TEST_F(SwissDedupTest, persistentSlotReuse) {
  SwissDedup dedup;
  // Call compute twice — batchSeq_ should differentiate batches.
  const int32_t N = 100;
  std::vector<uint64_t> hashes(N);
  std::vector<vector_size_t> rows(N);
  for (int i = 0; i < N; ++i) {
    hashes[i] = i % 10;
    rows[i] = i;
  }
  std::vector<vector_size_t> uniqueRows(N);
  std::vector<vector_size_t> result(N);

  // First batch.
  auto n1 = dedup.compute(
      BaseHashTable::HashMode::kNormalizedKey,
      hashes.data(),
      rows.data(),
      N,
      uniqueRows.data(),
      result.data(),
      0,
      -1);
  EXPECT_EQ(n1, 10);

  // Second batch with different data but same dedup instance.
  for (int i = 0; i < N; ++i) {
    hashes[i] = i % 20; // 20 unique keys now
  }
  auto n2 = dedup.compute(
      BaseHashTable::HashMode::kNormalizedKey,
      hashes.data(),
      rows.data(),
      N,
      uniqueRows.data(),
      result.data(),
      0,
      -1);
  EXPECT_EQ(n2, 20);
}

// ═══════════════════════════════════════════════════
// Dictionary path tests
// ═══════════════════════════════════════════════════

TEST_F(SwissDedupTest, dictionaryAllUnique) {
  SwissDedup dedup;
  const int32_t N = 100;
  // Each row maps to a different dictionary entry.
  std::vector<vector_size_t> dictIndices(N);
  std::vector<vector_size_t> rows(N);
  for (int i = 0; i < N; ++i) {
    dictIndices[i] = i;
    rows[i] = i;
  }
  std::vector<vector_size_t> uniqueRows(N);
  std::vector<vector_size_t> result(N);

  auto numUnique = dedup.computeWithDictionary(
      dictIndices.data(), rows.data(), N, uniqueRows.data(), result.data(), N);

  EXPECT_EQ(numUnique, N);
  for (int i = 0; i < N; ++i) {
    EXPECT_EQ(result[i], i);
  }
}

TEST_F(SwissDedupTest, dictionaryWithDups) {
  SwissDedup dedup;
  const int32_t N = 200;
  // 5 dictionary entries, cycling through them.
  std::vector<vector_size_t> dictIndices(N);
  std::vector<vector_size_t> rows(N);
  for (int i = 0; i < N; ++i) {
    dictIndices[i] = i % 5;
    rows[i] = i;
  }
  std::vector<vector_size_t> uniqueRows(N);
  std::vector<vector_size_t> result(N);

  auto numUnique = dedup.computeWithDictionary(
      dictIndices.data(), rows.data(), N, uniqueRows.data(), result.data(), 5);

  EXPECT_EQ(numUnique, 5);
  // First 5 rows are unique (first occurrence of each dict entry).
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(uniqueRows[i], i);
    EXPECT_EQ(result[i], i);
  }
  // Subsequent rows map to the first occurrence.
  for (int i = 5; i < N; ++i) {
    EXPECT_EQ(result[i], i % 5);
  }
}

TEST_F(SwissDedupTest, dictionaryAllSameEntry) {
  SwissDedup dedup;
  const int32_t N = 100;
  // All rows map to dictionary entry 0.
  std::vector<vector_size_t> dictIndices(N, 0);
  std::vector<vector_size_t> rows(N);
  for (int i = 0; i < N; ++i) {
    rows[i] = i;
  }
  std::vector<vector_size_t> uniqueRows(N);
  std::vector<vector_size_t> result(N);

  auto numUnique = dedup.computeWithDictionary(
      dictIndices.data(), rows.data(), N, uniqueRows.data(), result.data(), 1);

  EXPECT_EQ(numUnique, 1);
  EXPECT_EQ(uniqueRows[0], 0);
  for (int i = 0; i < N; ++i) {
    EXPECT_EQ(result[i], 0);
  }
}

TEST_F(SwissDedupTest, dictionaryWithSubsetRows) {
  SwissDedup dedup;
  // Only a subset of rows are active (e.g., after null filtering).
  std::vector<vector_size_t> dictIndices = {0, 1, 0, 1, 2, 0, 2, 1};
  // Active rows: 1, 3, 4, 6 (skipping 0, 2, 5, 7).
  std::vector<vector_size_t> rows = {1, 3, 4, 6};
  const int32_t numRows = 4;
  std::vector<vector_size_t> uniqueRows(numRows);
  std::vector<vector_size_t> result(8); // indexed by row number

  auto numUnique = dedup.computeWithDictionary(
      dictIndices.data(),
      rows.data(),
      numRows,
      uniqueRows.data(),
      result.data(),
      3);

  EXPECT_EQ(numUnique, 2); // dict entries 1 and 2
  EXPECT_EQ(uniqueRows[0], 1); // first row with dict entry 1
  EXPECT_EQ(uniqueRows[1], 4); // first row with dict entry 2
  EXPECT_EQ(result[1], 1); // row 1: dict 1 → first
  EXPECT_EQ(result[3], 1); // row 3: dict 1 → maps to row 1
  EXPECT_EQ(result[4], 4); // row 4: dict 2 → first
  EXPECT_EQ(result[6], 4); // row 6: dict 2 → maps to row 4
}

TEST_F(SwissDedupTest, dictionaryBatchReuse) {
  SwissDedup dedup;
  // Two consecutive batches should not interfere (batchSeq trick).
  const int32_t N = 50;
  std::vector<vector_size_t> rows(N);
  for (int i = 0; i < N; ++i) {
    rows[i] = i;
  }
  std::vector<vector_size_t> uniqueRows(N);
  std::vector<vector_size_t> result(N);

  // Batch 1: 10 unique entries.
  std::vector<vector_size_t> dict1(N);
  for (int i = 0; i < N; ++i) {
    dict1[i] = i % 10;
  }
  auto n1 = dedup.computeWithDictionary(
      dict1.data(), rows.data(), N, uniqueRows.data(), result.data(), 10);
  EXPECT_EQ(n1, 10);

  // Batch 2: 25 unique entries (different dict).
  std::vector<vector_size_t> dict2(N);
  for (int i = 0; i < N; ++i) {
    dict2[i] = i % 25;
  }
  auto n2 = dedup.computeWithDictionary(
      dict2.data(), rows.data(), N, uniqueRows.data(), result.data(), 25);
  EXPECT_EQ(n2, 25);
}

// ═══════════════════════════════════════════════════
// computeAutoDetect tests
// ═══════════════════════════════════════════════════

TEST_F(SwissDedupTest, autoDetectDictPath) {
  SwissDedup dedup;
  const int32_t N = 200;
  // Dict indices: 5 unique entries cycling over 200 rows.
  std::vector<vector_size_t> dictIndices(N);
  std::vector<uint64_t> hashes(N);
  std::vector<vector_size_t> rows(N);
  for (int i = 0; i < N; ++i) {
    dictIndices[i] = i % 5;
    hashes[i] = i % 5; // unused in dict path
    rows[i] = i;
  }
  std::vector<vector_size_t> uniqueRows(N);
  std::vector<vector_size_t> result(N);

  auto [numUnique, usedDict] = dedup.computeAutoDetect(
      BaseHashTable::HashMode::kNormalizedKey,
      hashes.data(),
      rows.data(),
      N,
      uniqueRows.data(),
      result.data(),
      dictIndices.data(),
      5, // dictSize < N * 9/10 = 180
      0,
      -1);

  EXPECT_TRUE(usedDict);
  EXPECT_EQ(numUnique, 5);
  // Verify dedup mapping.
  for (int i = 0; i < N; ++i) {
    EXPECT_EQ(dictIndices[result[i]], dictIndices[i]);
  }
}

TEST_F(SwissDedupTest, autoDetectHashFallback) {
  SwissDedup dedup;
  const int32_t N = 100;
  std::vector<uint64_t> hashes(N);
  std::vector<vector_size_t> rows(N);
  for (int i = 0; i < N; ++i) {
    hashes[i] = i % 10;
    rows[i] = i;
  }
  std::vector<vector_size_t> uniqueRows(N);
  std::vector<vector_size_t> result(N);

  // No dict indices → hash fallback.
  auto [numUnique, usedDict] = dedup.computeAutoDetect(
      BaseHashTable::HashMode::kNormalizedKey,
      hashes.data(),
      rows.data(),
      N,
      uniqueRows.data(),
      result.data(),
      nullptr, // no dict
      0,
      0,
      -1);

  EXPECT_FALSE(usedDict);
  EXPECT_EQ(numUnique, 10);
}

TEST_F(SwissDedupTest, autoDetectDictTooLarge) {
  SwissDedup dedup;
  const int32_t N = 100;
  std::vector<vector_size_t> dictIndices(N);
  std::vector<uint64_t> hashes(N);
  std::vector<vector_size_t> rows(N);
  for (int i = 0; i < N; ++i) {
    dictIndices[i] = i % 95; // 95 unique out of 100 → dictSize > N * 9/10
    hashes[i] = i % 95;
    rows[i] = i;
  }
  std::vector<vector_size_t> uniqueRows(N);
  std::vector<vector_size_t> result(N);

  auto [numUnique, usedDict] = dedup.computeAutoDetect(
      BaseHashTable::HashMode::kNormalizedKey,
      hashes.data(),
      rows.data(),
      N,
      uniqueRows.data(),
      result.data(),
      dictIndices.data(),
      95, // dictSize = 95 > 100 * 9/10 = 90 → fallback
      0,
      -1);

  // Dict too large relative to batch → should fall back to hash.
  EXPECT_FALSE(usedDict);
}

TEST_F(SwissDedupTest, autoDetectUpdatesSampling) {
  SwissDedup dedup;
  EXPECT_EQ(dedup.state(), SwissDedup::State::kSampling);

  const int32_t N = 100;
  std::vector<uint64_t> hashes(N);
  std::vector<vector_size_t> rows(N);
  for (int i = 0; i < N; ++i) {
    hashes[i] = i % 5;
    rows[i] = i;
  }
  std::vector<vector_size_t> uniqueRows(N);
  std::vector<vector_size_t> result(N);

  // Run 10 batches via computeAutoDetect.
  for (int b = 0; b < SwissDedup::kSamplingBatches; ++b) {
    dedup.computeAutoDetect(
        BaseHashTable::HashMode::kNormalizedKey,
        hashes.data(),
        rows.data(),
        N,
        uniqueRows.data(),
        result.data(),
        nullptr,
        0,
        0,
        -1);
  }
  // 95% dup → should be kActive.
  EXPECT_EQ(dedup.state(), SwissDedup::State::kActive);
}

// ═══════════════════════════════════════════════════
// Sampling state transition tests
// ═══════════════════════════════════════════════════

TEST_F(SwissDedupTest, samplingInitialState) {
  SwissDedup dedup;
  EXPECT_EQ(dedup.state(), SwissDedup::State::kSampling);
}

TEST_F(SwissDedupTest, samplingTransitionToActive) {
  SwissDedup dedup;
  // Feed 10 batches with high duplication (90% dup → 10% unique).
  for (int b = 0; b < SwissDedup::kSamplingBatches; ++b) {
    auto state = dedup.updateSampling(/*numRows=*/1000, /*numUnique=*/100);
    if (b < SwissDedup::kSamplingBatches - 1) {
      EXPECT_EQ(state, SwissDedup::State::kSampling);
    }
  }
  EXPECT_EQ(dedup.state(), SwissDedup::State::kActive);
}

TEST_F(SwissDedupTest, samplingTransitionToDisabled) {
  SwissDedup dedup;
  // Feed 10 batches with almost no duplication (1% dup).
  for (int b = 0; b < SwissDedup::kSamplingBatches; ++b) {
    dedup.updateSampling(/*numRows=*/1000, /*numUnique=*/990);
  }
  EXPECT_EQ(dedup.state(), SwissDedup::State::kDisabled);
}

TEST_F(SwissDedupTest, samplingBoundaryAtMinDupFraction) {
  SwissDedup dedup;
  // Feed just above the kMinDupFraction boundary.
  // Use one fewer unique than the exact boundary to avoid floating-point
  // precision issues with >= comparison.
  int32_t numRows = 1000;
  int32_t numUnique =
      static_cast<int32_t>(numRows * (1.0 - SwissDedup::kMinDupFraction)) - 1;
  for (int b = 0; b < SwissDedup::kSamplingBatches; ++b) {
    dedup.updateSampling(numRows, numUnique);
  }
  // Above boundary should activate.
  EXPECT_EQ(dedup.state(), SwissDedup::State::kActive);

  // Clearly below boundary (only 1% dup).
  SwissDedup dedup2;
  int32_t numUniqueHigh = numRows * 99 / 100;
  for (int b = 0; b < SwissDedup::kSamplingBatches; ++b) {
    dedup2.updateSampling(numRows, numUniqueHigh);
  }
  EXPECT_EQ(dedup2.state(), SwissDedup::State::kDisabled);
}

TEST_F(SwissDedupTest, samplingNoUpdateAfterTransition) {
  SwissDedup dedup;
  // Transition to kActive.
  for (int b = 0; b < SwissDedup::kSamplingBatches; ++b) {
    dedup.updateSampling(1000, 100);
  }
  EXPECT_EQ(dedup.state(), SwissDedup::State::kActive);

  // Further calls should not change state.
  dedup.updateSampling(1000, 1000); // all unique
  EXPECT_EQ(dedup.state(), SwissDedup::State::kActive);

  // Same for kDisabled.
  SwissDedup dedup2;
  for (int b = 0; b < SwissDedup::kSamplingBatches; ++b) {
    dedup2.updateSampling(1000, 999);
  }
  EXPECT_EQ(dedup2.state(), SwissDedup::State::kDisabled);
  dedup2.updateSampling(1000, 100); // high dup
  EXPECT_EQ(dedup2.state(), SwissDedup::State::kDisabled);
}

TEST_F(SwissDedupTest, samplingMixedBatches) {
  SwissDedup dedup;
  // 5 batches with high dup, 5 batches with no dup.
  // Total: 5000 rows with 500 unique + 5000 rows with 5000 unique = 5500/10000
  // = 55% unique → 45% dup → should activate.
  for (int b = 0; b < 5; ++b) {
    dedup.updateSampling(1000, 100);
  }
  for (int b = 5; b < SwissDedup::kSamplingBatches; ++b) {
    dedup.updateSampling(1000, 1000);
  }
  EXPECT_EQ(dedup.state(), SwissDedup::State::kActive);
}

// ═══════════════════════════════════════════════════
// Early stop tests
// ═══════════════════════════════════════════════════

TEST_F(SwissDedupTest, earlyStopAllUnique) {
  SwissDedup dedup;
  const int32_t N = 200;
  std::vector<uint64_t> hashes(N);
  std::vector<vector_size_t> rows(N);
  for (int i = 0; i < N; ++i) {
    hashes[i] = i + 1; // all unique
    rows[i] = i;
  }
  std::vector<vector_size_t> uniqueRows(N);
  std::vector<vector_size_t> result(N);

  // With earlyStopRow = 63 (kEarlyStopRowAgg default).
  auto numUnique = dedup.compute(
      BaseHashTable::HashMode::kNormalizedKey,
      hashes.data(),
      rows.data(),
      N,
      uniqueRows.data(),
      result.data(),
      0,
      SwissDedup::kEarlyStopRowAgg);

  // All rows unique → early stop triggers → returns N.
  EXPECT_EQ(numUnique, N);
}

TEST_F(SwissDedupTest, earlyStopWithDupsBeforeCutoff) {
  SwissDedup dedup;
  const int32_t N = 200;
  std::vector<uint64_t> hashes(N);
  std::vector<vector_size_t> rows(N);
  // First 64 rows have duplicates, rest are unique.
  for (int i = 0; i < N; ++i) {
    hashes[i] = (i < 64) ? (i % 10) : (i + 1000);
    rows[i] = i;
  }
  std::vector<vector_size_t> uniqueRows(N);
  std::vector<vector_size_t> result(N);

  auto numUnique = dedup.compute(
      BaseHashTable::HashMode::kNormalizedKey,
      hashes.data(),
      rows.data(),
      N,
      uniqueRows.data(),
      result.data(),
      0,
      SwissDedup::kEarlyStopRowAgg);

  // Early stop NOT triggered (dups before row 63) → processes all rows.
  EXPECT_LT(numUnique, N);
  // 10 unique from first 64 + 136 unique from rest = 146.
  EXPECT_EQ(numUnique, 10 + (N - 64));
}

TEST_F(SwissDedupTest, earlyStopProbeVsAgg) {
  // kEarlyStopRowProbe (127) is higher than kEarlyStopRowAgg (63).
  // With 100 unique rows out of 200, agg should NOT early-stop but probe
  // early stop depends on whether first 128 rows are all unique.
  SwissDedup dedup;
  const int32_t N = 200;
  std::vector<uint64_t> hashes(N);
  std::vector<vector_size_t> rows(N);
  // All unique.
  for (int i = 0; i < N; ++i) {
    hashes[i] = i + 1;
    rows[i] = i;
  }
  std::vector<vector_size_t> uniqueRows(N);
  std::vector<vector_size_t> result(N);

  // Agg early stop at 63.
  auto n1 = dedup.compute(
      BaseHashTable::HashMode::kNormalizedKey,
      hashes.data(),
      rows.data(),
      N,
      uniqueRows.data(),
      result.data(),
      0,
      SwissDedup::kEarlyStopRowAgg);
  EXPECT_EQ(n1, N); // early stop → all returned as unique

  // Probe early stop at 127.
  auto n2 = dedup.compute(
      BaseHashTable::HashMode::kNormalizedKey,
      hashes.data(),
      rows.data(),
      N,
      uniqueRows.data(),
      result.data(),
      0,
      SwissDedup::kEarlyStopRowProbe);
  EXPECT_EQ(n2, N); // early stop → all returned as unique
}

// ═══════════════════════════════════════════════════
// Interaction: sampling + early stop + compute
// ═══════════════════════════════════════════════════

TEST_F(SwissDedupTest, samplingWithEarlyStopBatches) {
  SwissDedup dedup;
  const int32_t N = 200;
  std::vector<uint64_t> hashes(N);
  std::vector<vector_size_t> rows(N);
  std::vector<vector_size_t> uniqueRows(N);
  std::vector<vector_size_t> result(N);

  // 10 batches of all-unique data. Early stop triggers each time.
  // updateSampling gets (N, N) each time → 0% dup → should disable.
  for (int b = 0; b < SwissDedup::kSamplingBatches; ++b) {
    for (int i = 0; i < N; ++i) {
      hashes[i] = b * 1000 + i + 1;
      rows[i] = i;
    }
    auto numUnique = dedup.compute(
        BaseHashTable::HashMode::kNormalizedKey,
        hashes.data(),
        rows.data(),
        N,
        uniqueRows.data(),
        result.data(),
        0,
        SwissDedup::kEarlyStopRowAgg);
    EXPECT_EQ(numUnique, N);
    dedup.updateSampling(N, numUnique);
  }
  EXPECT_EQ(dedup.state(), SwissDedup::State::kDisabled);
}

TEST_F(SwissDedupTest, samplingWithHighDupBatches) {
  SwissDedup dedup;
  const int32_t N = 200;
  std::vector<uint64_t> hashes(N);
  std::vector<vector_size_t> rows(N);
  std::vector<vector_size_t> uniqueRows(N);
  std::vector<vector_size_t> result(N);

  // 10 batches with 5 unique keys → 97.5% dup.
  for (int b = 0; b < SwissDedup::kSamplingBatches; ++b) {
    for (int i = 0; i < N; ++i) {
      hashes[i] = i % 5;
      rows[i] = i;
    }
    auto numUnique = dedup.compute(
        BaseHashTable::HashMode::kNormalizedKey,
        hashes.data(),
        rows.data(),
        N,
        uniqueRows.data(),
        result.data(),
        0,
        SwissDedup::kEarlyStopRowAgg);
    EXPECT_EQ(numUnique, 5);
    dedup.updateSampling(N, numUnique);
  }
  EXPECT_EQ(dedup.state(), SwissDedup::State::kActive);
}

// ── KeyComparator tests ──

class KeyComparatorTest : public testing::Test,
                          public facebook::velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  // Helper: create a VectorHasher, decode a vector, return the hasher.
  std::unique_ptr<VectorHasher> makeHasher(
      const VectorPtr& vector,
      column_index_t channel = 0) {
    auto hasher = VectorHasher::create(vector->type(), channel);
    SelectivityVector rows(vector->size());
    hasher->decode(*vector, rows);
    return hasher;
  }
};

TEST_F(KeyComparatorTest, booleanBitPacked) {
  // BOOLEAN values are bit-packed. Old code used memcmp with typeSize=1,
  // which compared a full byte (8 booleans).
  auto boolVec = makeFlatVector<bool>({true, false, true, false, true, true});
  std::vector<std::unique_ptr<VectorHasher>> hashers;
  hashers.push_back(makeHasher(boolVec));

  KeyComparator cmp;
  cmp.prepare(hashers);

  EXPECT_TRUE(cmp(0, 2)); // true == true
  EXPECT_TRUE(cmp(1, 3)); // false == false
  EXPECT_TRUE(cmp(0, 4)); // true == true
  EXPECT_FALSE(cmp(0, 1)); // true != false
  EXPECT_FALSE(cmp(1, 2)); // false != true
  EXPECT_FALSE(cmp(3, 4)); // false != true
}

TEST_F(KeyComparatorTest, booleanWithNulls) {
  auto boolVec =
      makeNullableFlatVector<bool>({true, std::nullopt, false, std::nullopt});
  std::vector<std::unique_ptr<VectorHasher>> hashers;
  hashers.push_back(makeHasher(boolVec));

  KeyComparator cmp;
  cmp.prepare(hashers);

  EXPECT_TRUE(cmp(1, 3)); // both null → equal for grouping
  EXPECT_FALSE(cmp(0, 1)); // non-null vs null
  EXPECT_FALSE(cmp(1, 2)); // null vs non-null
  EXPECT_FALSE(cmp(0, 2)); // true != false
}

TEST_F(KeyComparatorTest, constantVector) {
  // ConstantVector: isConstantMapping()=true, indices()=null.
  // Old code dereferenced null indices, causing crash.
  auto constVec = makeConstant<int32_t>(42, 10);
  std::vector<std::unique_ptr<VectorHasher>> hashers;
  hashers.push_back(makeHasher(constVec));

  KeyComparator cmp;
  cmp.prepare(hashers);

  EXPECT_TRUE(cmp(0, 5));
  EXPECT_TRUE(cmp(3, 9));
  EXPECT_TRUE(cmp(0, 9));
}

TEST_F(KeyComparatorTest, constantNullVector) {
  auto constNull = BaseVector::createNullConstant(INTEGER(), 10, pool());
  std::vector<std::unique_ptr<VectorHasher>> hashers;
  hashers.push_back(makeHasher(constNull));

  KeyComparator cmp;
  cmp.prepare(hashers);

  EXPECT_TRUE(cmp(0, 5));
  EXPECT_TRUE(cmp(3, 9));
}

TEST_F(KeyComparatorTest, constantBooleanVector) {
  // Exercises both constant and bit-packed paths.
  auto constBool = makeConstant<bool>(true, 10);
  std::vector<std::unique_ptr<VectorHasher>> hashers;
  hashers.push_back(makeHasher(constBool));

  KeyComparator cmp;
  cmp.prepare(hashers);

  EXPECT_TRUE(cmp(0, 5));
  EXPECT_TRUE(cmp(3, 9));
}

TEST_F(KeyComparatorTest, multiColumnWithBooleanAndConstant) {
  // Multi-column: INT32 (flat) + BOOLEAN (flat) + VARCHAR (constant).
  auto intVec = makeFlatVector<int32_t>({1, 1, 2, 1, 2});
  auto boolVec = makeFlatVector<bool>({true, true, false, false, false});
  auto strConst = makeConstant<StringView>("abc"_sv, 5);

  std::vector<std::unique_ptr<VectorHasher>> hashers;
  hashers.push_back(makeHasher(intVec, 0));
  hashers.push_back(makeHasher(boolVec, 1));
  hashers.push_back(makeHasher(strConst, 2));

  KeyComparator cmp;
  cmp.prepare(hashers);

  EXPECT_TRUE(cmp(0, 1)); // (1, true, "abc") == (1, true, "abc")
  EXPECT_TRUE(cmp(2, 4)); // (2, false, "abc") == (2, false, "abc")
  EXPECT_FALSE(cmp(0, 3)); // bool differs
  EXPECT_FALSE(cmp(0, 2)); // int differs
}

TEST_F(KeyComparatorTest, dictionaryVector) {
  // Dictionary-wrapped: isIdentityMapping()=false, has non-null indices.
  auto baseVec = makeFlatVector<int64_t>({100, 200, 300});
  auto indices = makeIndices({0, 1, 2, 0, 1, 0});
  auto dictVec = wrapInDictionary(indices, 6, baseVec);

  std::vector<std::unique_ptr<VectorHasher>> hashers;
  hashers.push_back(makeHasher(dictVec));

  KeyComparator cmp;
  cmp.prepare(hashers);

  EXPECT_TRUE(cmp(0, 3)); // both → 100
  EXPECT_TRUE(cmp(0, 5)); // both → 100
  EXPECT_TRUE(cmp(1, 4)); // both → 200
  EXPECT_FALSE(cmp(0, 1)); // 100 != 200
  EXPECT_FALSE(cmp(1, 2)); // 200 != 300
}
