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

// Standalone benchmark: SwissDedup vs F14FastSet for batch dedup.
//
// Build:
//   cd _build && make -j$(nproc) velox_swiss_dedup_benchmark
//
// Run:
//   ./velox/exec/tests/velox_swiss_dedup_benchmark

#include <folly/Benchmark.h>
#include <folly/container/F14Set.h>
#include <folly/init/Init.h>

#include "velox/exec/SwissDedup.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

// Generate hashes with a given number of unique keys.
struct BenchData {
  int32_t numRows;
  int32_t numUnique;
  std::vector<uint64_t> hashes;
  std::vector<vector_size_t> rows;
  std::vector<vector_size_t> uniqueRows;
  std::vector<vector_size_t> result;

  BenchData(int32_t n, int32_t u) : numRows(n), numUnique(u) {
    hashes.resize(n);
    rows.resize(n);
    uniqueRows.resize(n);
    result.resize(n);
    for (int32_t i = 0; i < n; ++i) {
      // Use a good hash-like distribution.
      hashes[i] = folly::hash::twang_mix64(static_cast<uint64_t>(i % u));
      rows[i] = i;
    }
  }
};

// Trivial keysEqual that always agrees (for kNK/kArray where hash=key).
auto trivialEqual = [](vector_size_t, vector_size_t) { return true; };

// ── SwissDedup: PersistentSlot (kNormalizedKey mode) ──

void benchSwissDedupPerfect(int32_t numRows, int32_t numUnique, int iters) {
  BenchData d(numRows, numUnique);
  SwissDedup dedup;

  for (int iter = 0; iter < iters; ++iter) {
    dedup.compute(
        BaseHashTable::HashMode::kNormalizedKey,
        d.hashes.data(),
        d.rows.data(),
        d.numRows,
        d.uniqueRows.data(),
        d.result.data(),
        0,
        -1);
  }
  folly::doNotOptimizeAway(d.result[0]);
}

// ── SwissDedup: SwissTable (kHash mode with keysEqual) ──

void benchSwissDedupSwissTable(int32_t numRows, int32_t numUnique, int iters) {
  BenchData d(numRows, numUnique);
  SwissDedup dedup;
  // Use hash equality as proxy for key equality (same as perfect hash).
  auto keysEqual = [&](vector_size_t a, vector_size_t b) {
    return d.hashes[a] == d.hashes[b];
  };

  for (int iter = 0; iter < iters; ++iter) {
    dedup.compute(
        BaseHashTable::HashMode::kHash,
        d.hashes.data(),
        d.rows.data(),
        d.numRows,
        d.uniqueRows.data(),
        d.result.data(),
        0,
        -1,
        keysEqual);
  }
  folly::doNotOptimizeAway(d.result[0]);
}

// ── SwissDedup: DirectIndex (kArray mode, small range) ──

void benchSwissDedupDirectIndex(int32_t numRows, int32_t numUnique, int iters) {
  BenchData d(numRows, numUnique);
  // DirectIndex needs raw value IDs as hashes (0..rangeSize-1).
  for (int32_t i = 0; i < numRows; ++i) {
    d.hashes[i] = i % numUnique;
  }
  SwissDedup dedup;

  for (int iter = 0; iter < iters; ++iter) {
    dedup.compute(
        BaseHashTable::HashMode::kArray,
        d.hashes.data(),
        d.rows.data(),
        d.numRows,
        d.uniqueRows.data(),
        d.result.data(),
        /*arrayRangeSize=*/numUnique,
        -1);
  }
  folly::doNotOptimizeAway(d.result[0]);
}

// ── F14FastSet baseline ──

void benchF14Set(int32_t numRows, int32_t numUnique, int iters) {
  BenchData d(numRows, numUnique);

  for (int iter = 0; iter < iters; ++iter) {
    folly::F14FastSet<uint64_t> seen;
    seen.reserve(numUnique);
    int32_t nu = 0;
    for (int32_t i = 0; i < numRows; ++i) {
      auto h = d.hashes[d.rows[i]];
      if (seen.insert(h).second) {
        d.uniqueRows[nu++] = d.rows[i];
        d.result[d.rows[i]] = d.rows[i];
      } else {
        // F14 doesn't track first occurrence natively; skip for fairness.
        d.result[d.rows[i]] = d.rows[i]; // placeholder
      }
    }
    folly::doNotOptimizeAway(nu);
  }
}

// ── F14FastMap baseline (tracks first occurrence like SwissDedup) ──

void benchF14Map(int32_t numRows, int32_t numUnique, int iters) {
  BenchData d(numRows, numUnique);

  for (int iter = 0; iter < iters; ++iter) {
    folly::F14FastMap<uint64_t, vector_size_t> seen;
    seen.reserve(numUnique);
    int32_t nu = 0;
    for (int32_t i = 0; i < numRows; ++i) {
      auto row = d.rows[i];
      auto h = d.hashes[row];
      auto [it, inserted] = seen.emplace(h, row);
      if (inserted) {
        d.uniqueRows[nu++] = row;
        d.result[row] = row;
      } else {
        d.result[row] = it->second;
      }
    }
    folly::doNotOptimizeAway(nu);
  }
}

// ── Benchmark matrix ──
// Dimensions: {batch size} × {dup ratio} × {algorithm}

// 1024 rows, 10 unique (97% dups) — high duplication
BENCHMARK_NAMED_PARAM(benchSwissDedupDirectIndex, 1024r_10u, 1024, 10)
BENCHMARK_NAMED_PARAM(benchSwissDedupPerfect, 1024r_10u, 1024, 10)
BENCHMARK_NAMED_PARAM(benchSwissDedupSwissTable, 1024r_10u, 1024, 10)
BENCHMARK_NAMED_PARAM(benchF14Set, 1024r_10u, 1024, 10)
BENCHMARK_NAMED_PARAM(benchF14Map, 1024r_10u, 1024, 10)
BENCHMARK_DRAW_LINE();

// 1024 rows, 100 unique (90% dups) — moderate duplication
BENCHMARK_NAMED_PARAM(benchSwissDedupDirectIndex, 1024r_100u, 1024, 100)
BENCHMARK_NAMED_PARAM(benchSwissDedupPerfect, 1024r_100u, 1024, 100)
BENCHMARK_NAMED_PARAM(benchSwissDedupSwissTable, 1024r_100u, 1024, 100)
BENCHMARK_NAMED_PARAM(benchF14Set, 1024r_100u, 1024, 100)
BENCHMARK_NAMED_PARAM(benchF14Map, 1024r_100u, 1024, 100)
BENCHMARK_DRAW_LINE();

// 1024 rows, 512 unique (50% dups) — boundary
BENCHMARK_NAMED_PARAM(benchSwissDedupDirectIndex, 1024r_512u, 1024, 512)
BENCHMARK_NAMED_PARAM(benchSwissDedupPerfect, 1024r_512u, 1024, 512)
BENCHMARK_NAMED_PARAM(benchSwissDedupSwissTable, 1024r_512u, 1024, 512)
BENCHMARK_NAMED_PARAM(benchF14Set, 1024r_512u, 1024, 512)
BENCHMARK_NAMED_PARAM(benchF14Map, 1024r_512u, 1024, 512)
BENCHMARK_DRAW_LINE();

// 1024 rows, 1024 unique (0% dups) — worst case for dedup
BENCHMARK_NAMED_PARAM(benchSwissDedupDirectIndex, 1024r_1024u, 1024, 1024)
BENCHMARK_NAMED_PARAM(benchSwissDedupPerfect, 1024r_1024u, 1024, 1024)
BENCHMARK_NAMED_PARAM(benchSwissDedupSwissTable, 1024r_1024u, 1024, 1024)
BENCHMARK_NAMED_PARAM(benchF14Set, 1024r_1024u, 1024, 1024)
BENCHMARK_NAMED_PARAM(benchF14Map, 1024r_1024u, 1024, 1024)
BENCHMARK_DRAW_LINE();

// 4096 rows, 50 unique (98.8% dups) — large batch, high dups
BENCHMARK_NAMED_PARAM(benchSwissDedupDirectIndex, 4096r_50u, 4096, 50)
BENCHMARK_NAMED_PARAM(benchSwissDedupPerfect, 4096r_50u, 4096, 50)
BENCHMARK_NAMED_PARAM(benchSwissDedupSwissTable, 4096r_50u, 4096, 50)
BENCHMARK_NAMED_PARAM(benchF14Set, 4096r_50u, 4096, 50)
BENCHMARK_NAMED_PARAM(benchF14Map, 4096r_50u, 4096, 50)
BENCHMARK_DRAW_LINE();

// 256 rows, 5 unique — small batch
BENCHMARK_NAMED_PARAM(benchSwissDedupDirectIndex, 256r_5u, 256, 5)
BENCHMARK_NAMED_PARAM(benchSwissDedupPerfect, 256r_5u, 256, 5)
BENCHMARK_NAMED_PARAM(benchSwissDedupSwissTable, 256r_5u, 256, 5)
BENCHMARK_NAMED_PARAM(benchF14Set, 256r_5u, 256, 5)
BENCHMARK_NAMED_PARAM(benchF14Map, 256r_5u, 256, 5)

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
