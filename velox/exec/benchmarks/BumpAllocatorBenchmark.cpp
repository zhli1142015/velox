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

/**
 * Comprehensive benchmark for bump allocator optimization in
 * HashStringAllocator.
 *
 * This benchmark covers three main scenarios:
 * 1. SORT-like: Pure allocation, no free until clear (RowContainer bulk insert)
 * 2. AGG-like: Mixed allocate/free with accumulator updates
 * 3. JOIN-like: Probe-side allocation with varying patterns
 *
 * For each scenario, we test combinations of:
 * - Key types: BIGINT only, VARCHAR only, mixed
 * - Dependent columns: none, integers only, strings, mixed
 * - String lengths: short (16), medium (64), long (256)
 * - Cardinality: 10K, 100K, 1M rows
 *
 * Run with:
 *   ./velox_bump_allocator_benchmark --velox_enable_bump_allocator=true
 *   ./velox_bump_allocator_benchmark --velox_enable_bump_allocator=false
 */

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <iostream>
#include <random>

#include "velox/common/memory/HashStringAllocator.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/RowContainer.h"

DECLARE_bool(velox_enable_bump_allocator);

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

// Thread-local pool to avoid cleanup issues
thread_local std::shared_ptr<memory::MemoryPool> tlRootPool;
thread_local std::shared_ptr<memory::MemoryPool> tlPool;

memory::MemoryPool* getPool() {
  if (!tlPool) {
    tlRootPool = memory::memoryManager()->addRootPool(
        "benchmark_" + std::to_string(folly::getCurrentThreadID()));
    tlPool = tlRootPool->addLeafChild("leaf");
  }
  return tlPool.get();
}

// =============================================================================
// SECTION 1: SORT-like Benchmarks (Pure allocation, bulk insert)
// =============================================================================

// Sort with BIGINT key only, no dependent columns
void sortBigintKeyOnly(int numRows) {
  auto container = std::make_unique<RowContainer>(
      std::vector<TypePtr>{BIGINT()},
      std::vector<TypePtr>{},
      false,
      getPool(),
      FLAGS_velox_enable_bump_allocator);

  for (int row = 0; row < numRows; ++row) {
    char* rowPtr = container->newRow();
    *reinterpret_cast<int64_t*>(rowPtr) = row;
  }
}

// Sort with VARCHAR key only
void sortVarcharKeyOnly(int numRows, int keyLength) {
  auto container = std::make_unique<RowContainer>(
      std::vector<TypePtr>{VARCHAR()},
      std::vector<TypePtr>{},
      false,
      getPool(),
      FLAGS_velox_enable_bump_allocator);

  std::string keyStr(keyLength, 'k');

  for (int row = 0; row < numRows; ++row) {
    char* rowPtr = container->newRow();
    StringView view(keyStr);
    container->stringAllocator().copyMultipart(
        view, rowPtr, container->columnAt(0).offset());
  }
}

// Sort with BIGINT key + VARCHAR dependents
void sortBigintKeyVarcharDep(int numRows, int numVarcharDeps, int strLength) {
  auto container = std::make_unique<RowContainer>(
      std::vector<TypePtr>{BIGINT()},
      std::vector<TypePtr>(numVarcharDeps, VARCHAR()),
      false,
      getPool(),
      FLAGS_velox_enable_bump_allocator);

  std::string str(strLength, 'v');

  for (int row = 0; row < numRows; ++row) {
    char* rowPtr = container->newRow();
    *reinterpret_cast<int64_t*>(rowPtr) = row;
    for (int col = 0; col < numVarcharDeps; ++col) {
      StringView view(str);
      container->stringAllocator().copyMultipart(
          view, rowPtr, container->columnAt(1 + col).offset());
    }
  }
}

// Sort with VARCHAR key + VARCHAR dependents
void sortVarcharKeyVarcharDep(
    int numRows,
    int keyLength,
    int numVarcharDeps,
    int depLength) {
  auto container = std::make_unique<RowContainer>(
      std::vector<TypePtr>{VARCHAR()},
      std::vector<TypePtr>(numVarcharDeps, VARCHAR()),
      false,
      getPool(),
      FLAGS_velox_enable_bump_allocator);

  std::string keyStr(keyLength, 'k');
  std::string depStr(depLength, 'd');

  for (int row = 0; row < numRows; ++row) {
    char* rowPtr = container->newRow();
    StringView keyView(keyStr);
    container->stringAllocator().copyMultipart(
        keyView, rowPtr, container->columnAt(0).offset());
    for (int col = 0; col < numVarcharDeps; ++col) {
      StringView depView(depStr);
      container->stringAllocator().copyMultipart(
          depView, rowPtr, container->columnAt(1 + col).offset());
    }
  }
}

// Sort with mixed key (BIGINT + VARCHAR) + mixed dependents
void sortMixedKeyMixedDep(
    int numRows,
    int keyStrLength,
    int numIntDeps,
    int numVarcharDeps,
    int depStrLength) {
  std::vector<TypePtr> keyTypes = {BIGINT(), VARCHAR()};
  std::vector<TypePtr> depTypes;
  for (int i = 0; i < numIntDeps; ++i) {
    depTypes.push_back(BIGINT());
  }
  for (int i = 0; i < numVarcharDeps; ++i) {
    depTypes.push_back(VARCHAR());
  }

  auto container = std::make_unique<RowContainer>(
      keyTypes, depTypes, false, getPool(), FLAGS_velox_enable_bump_allocator);

  std::string keyStr(keyStrLength, 'k');
  std::string depStr(depStrLength, 'd');

  for (int row = 0; row < numRows; ++row) {
    char* rowPtr = container->newRow();
    // Key: BIGINT + VARCHAR
    *reinterpret_cast<int64_t*>(rowPtr) = row;
    StringView keyView(keyStr);
    container->stringAllocator().copyMultipart(
        keyView, rowPtr, container->columnAt(1).offset());
    // Deps: BIGINTs then VARCHARs
    int depOffset = 2;
    for (int i = 0; i < numIntDeps; ++i) {
      *reinterpret_cast<int64_t*>(
          rowPtr + container->columnAt(depOffset + i).offset()) = row + i;
    }
    depOffset += numIntDeps;
    for (int i = 0; i < numVarcharDeps; ++i) {
      StringView depView(depStr);
      container->stringAllocator().copyMultipart(
          depView, rowPtr, container->columnAt(depOffset + i).offset());
    }
  }
}

// =============================================================================
// SECTION 2: AGG-like Benchmarks (Mixed allocate/free with updates)
// NOTE: These benchmarks test OVERWRITE patterns (replacing accumulator
// values), which is NOT ideal for bump mode. This is intentional to show edge
// cases. For realistic aggregation patterns, see SECTION 5.
// =============================================================================

// Aggregation with BIGINT key, VARCHAR accumulator (like string_agg)
// WARNING: This benchmark OVERWRITES accumulator values repeatedly,
// causing memory waste in bump mode. Real aggregations typically append.
void aggBigintKeyVarcharAcc(int numGroups, int numUpdates, int strLength) {
  auto container = std::make_unique<RowContainer>(
      std::vector<TypePtr>{BIGINT()},
      std::vector<TypePtr>{VARCHAR()},
      false,
      getPool(),
      FLAGS_velox_enable_bump_allocator);

  std::mt19937 gen(42);
  std::uniform_int_distribution<> groupDist(0, numGroups - 1);

  // Pre-create groups
  std::vector<char*> groups(numGroups);
  for (int i = 0; i < numGroups; ++i) {
    groups[i] = container->newRow();
    *reinterpret_cast<int64_t*>(groups[i]) = i;
  }

  std::string str(strLength, 'a');

  // Simulate updates (append to accumulator)
  for (int u = 0; u < numUpdates; ++u) {
    int groupIdx = groupDist(gen);
    char* rowPtr = groups[groupIdx];
    StringView view(str);
    container->stringAllocator().copyMultipart(
        view, rowPtr, container->columnAt(1).offset());
  }
}

// Aggregation with VARCHAR key (like GROUP BY name)
void aggVarcharKeyIntAcc(int numGroups, int numUpdates, int keyLength) {
  auto container = std::make_unique<RowContainer>(
      std::vector<TypePtr>{VARCHAR()},
      std::vector<TypePtr>{BIGINT(), BIGINT()}, // sum, count
      false,
      getPool(),
      FLAGS_velox_enable_bump_allocator);

  std::mt19937 gen(42);
  std::uniform_int_distribution<> groupDist(0, numGroups - 1);

  // Pre-create groups
  std::vector<char*> groups(numGroups);
  for (int i = 0; i < numGroups; ++i) {
    groups[i] = container->newRow();
    std::string keyStr = std::to_string(i) + std::string(keyLength - 5, 'k');
    StringView keyView(keyStr);
    container->stringAllocator().copyMultipart(
        keyView, groups[i], container->columnAt(0).offset());
  }

  // Simulate updates
  for (int u = 0; u < numUpdates; ++u) {
    int groupIdx = groupDist(gen);
    char* rowPtr = groups[groupIdx];
    // Update integer accumulators (in-place, no alloc)
    *reinterpret_cast<int64_t*>(rowPtr + container->columnAt(1).offset()) += 1;
  }
}

// Aggregation with VARCHAR key + VARCHAR accumulator (worst case)
void aggVarcharKeyVarcharAcc(
    int numGroups,
    int numUpdates,
    int keyLength,
    int accLength) {
  auto container = std::make_unique<RowContainer>(
      std::vector<TypePtr>{VARCHAR()},
      std::vector<TypePtr>{VARCHAR()},
      false,
      getPool(),
      FLAGS_velox_enable_bump_allocator);

  std::mt19937 gen(42);
  std::uniform_int_distribution<> groupDist(0, numGroups - 1);

  std::vector<char*> groups(numGroups);
  for (int i = 0; i < numGroups; ++i) {
    groups[i] = container->newRow();
    std::string keyStr = std::to_string(i) + std::string(keyLength - 5, 'k');
    StringView keyView(keyStr);
    container->stringAllocator().copyMultipart(
        keyView, groups[i], container->columnAt(0).offset());
  }

  std::string accStr(accLength, 'a');
  for (int u = 0; u < numUpdates; ++u) {
    int groupIdx = groupDist(gen);
    char* rowPtr = groups[groupIdx];
    StringView view(accStr);
    container->stringAllocator().copyMultipart(
        view, rowPtr, container->columnAt(1).offset());
  }
}

// =============================================================================
// SECTION 3: JOIN-like Benchmarks (Build side allocation)
// NOTE: For BIGINT-only scenarios without VARCHAR payload, bump mode overhead
// may outweigh benefits since no string allocation occurs. This is expected.
// Bump mode shines when there are VARCHAR key or payload columns.
// =============================================================================

// Join build with BIGINT key, no payload
void joinBigintKeyNoPayload(int numRows) {
  auto container = std::make_unique<RowContainer>(
      std::vector<TypePtr>{BIGINT()},
      std::vector<TypePtr>{},
      true, // useListRowIndex for join
      getPool(),
      FLAGS_velox_enable_bump_allocator);

  for (int row = 0; row < numRows; ++row) {
    char* rowPtr = container->newRow();
    *reinterpret_cast<int64_t*>(rowPtr) = row;
  }
}

// Join build with VARCHAR key
void joinVarcharKey(int numRows, int keyLength) {
  auto container = std::make_unique<RowContainer>(
      std::vector<TypePtr>{VARCHAR()},
      std::vector<TypePtr>{},
      true,
      getPool(),
      FLAGS_velox_enable_bump_allocator);

  std::string keyStr(keyLength, 'j');

  for (int row = 0; row < numRows; ++row) {
    char* rowPtr = container->newRow();
    StringView view(keyStr);
    container->stringAllocator().copyMultipart(
        view, rowPtr, container->columnAt(0).offset());
  }
}

// Join build with BIGINT key + VARCHAR payload (common case)
void joinBigintKeyVarcharPayload(
    int numRows,
    int numPayloadCols,
    int strLength) {
  auto container = std::make_unique<RowContainer>(
      std::vector<TypePtr>{BIGINT()},
      std::vector<TypePtr>(numPayloadCols, VARCHAR()),
      true,
      getPool(),
      FLAGS_velox_enable_bump_allocator);

  std::string str(strLength, 'p');

  for (int row = 0; row < numRows; ++row) {
    char* rowPtr = container->newRow();
    *reinterpret_cast<int64_t*>(rowPtr) = row;
    for (int col = 0; col < numPayloadCols; ++col) {
      StringView view(str);
      container->stringAllocator().copyMultipart(
          view, rowPtr, container->columnAt(1 + col).offset());
    }
  }
}

// Join build with multiple VARCHAR keys + mixed payload
void joinMultiVarcharKeyMixedPayload(
    int numRows,
    int numKeyVarchars,
    int keyLength,
    int numPayloadInts,
    int numPayloadVarchars,
    int payloadStrLength) {
  std::vector<TypePtr> keyTypes(numKeyVarchars, VARCHAR());
  std::vector<TypePtr> payloadTypes;
  for (int i = 0; i < numPayloadInts; ++i) {
    payloadTypes.push_back(BIGINT());
  }
  for (int i = 0; i < numPayloadVarchars; ++i) {
    payloadTypes.push_back(VARCHAR());
  }

  auto container = std::make_unique<RowContainer>(
      keyTypes,
      payloadTypes,
      true,
      getPool(),
      FLAGS_velox_enable_bump_allocator);

  std::string keyStr(keyLength, 'k');
  std::string payloadStr(payloadStrLength, 'p');

  for (int row = 0; row < numRows; ++row) {
    char* rowPtr = container->newRow();
    // Keys
    for (int i = 0; i < numKeyVarchars; ++i) {
      StringView view(keyStr);
      container->stringAllocator().copyMultipart(
          view, rowPtr, container->columnAt(i).offset());
    }
    // Payload ints
    int payloadOffset = numKeyVarchars;
    for (int i = 0; i < numPayloadInts; ++i) {
      *reinterpret_cast<int64_t*>(
          rowPtr + container->columnAt(payloadOffset + i).offset()) = row;
    }
    payloadOffset += numPayloadInts;
    // Payload varchars
    for (int i = 0; i < numPayloadVarchars; ++i) {
      StringView view(payloadStr);
      container->stringAllocator().copyMultipart(
          view, rowPtr, container->columnAt(payloadOffset + i).offset());
    }
  }
}

// =============================================================================
// SECTION 4: Direct Allocator Benchmarks (Isolate allocator performance)
// =============================================================================

void directAllocOnly(int numAllocs, int allocSize) {
  auto allocator = std::make_unique<HashStringAllocator>(
      getPool(), FLAGS_velox_enable_bump_allocator);
  for (int i = 0; i < numAllocs; ++i) {
    allocator->allocate(allocSize);
  }
}

void directAllocFree(int numOps, int allocSize) {
  auto allocator = std::make_unique<HashStringAllocator>(
      getPool(), FLAGS_velox_enable_bump_allocator);

  std::mt19937 gen(42);
  std::uniform_real_distribution<> opDist(0.0, 1.0);
  std::vector<HashStringAllocator::Header*> headers;
  headers.reserve(1000);

  for (int i = 0; i < numOps; ++i) {
    if (headers.empty() || opDist(gen) < 0.7) {
      headers.push_back(allocator->allocate(allocSize));
    } else {
      size_t idx = gen() % headers.size();
      allocator->free(headers[idx]);
      headers[idx] = headers.back();
      headers.pop_back();
    }
  }
}

// =============================================================================
// SECTION 5: Real Aggregation Scenarios (Group creation + integer updates)
// =============================================================================

// Realistic aggregation: VARCHAR key creation only, integer accumulator updates
// This is the typical SUM/COUNT pattern where only the key requires string
// alloc
void aggRealSumCount(int numGroups, int numUpdates, int keyLength) {
  auto container = std::make_unique<RowContainer>(
      std::vector<TypePtr>{VARCHAR()},
      std::vector<TypePtr>{BIGINT(), BIGINT()}, // sum, count
      false,
      getPool(),
      FLAGS_velox_enable_bump_allocator);

  std::mt19937 gen(42);
  std::uniform_int_distribution<> groupDist(0, numGroups - 1);

  // Phase 1: Create all groups (this is where string allocation happens)
  std::vector<char*> groups(numGroups);
  for (int i = 0; i < numGroups; ++i) {
    groups[i] = container->newRow();
    std::string keyStr = std::to_string(i) + std::string(keyLength - 5, 'k');
    StringView keyView(keyStr);
    container->stringAllocator().copyMultipart(
        keyView, groups[i], container->columnAt(0).offset());
    // Initialize accumulators to 0
    *reinterpret_cast<int64_t*>(groups[i] + container->columnAt(1).offset()) =
        0;
    *reinterpret_cast<int64_t*>(groups[i] + container->columnAt(2).offset()) =
        0;
  }

  // Phase 2: Update accumulators (no memory allocation, just integer updates)
  for (int u = 0; u < numUpdates; ++u) {
    int groupIdx = groupDist(gen);
    char* rowPtr = groups[groupIdx];
    // Increment sum and count - no memory allocation
    *reinterpret_cast<int64_t*>(rowPtr + container->columnAt(1).offset()) += u;
    *reinterpret_cast<int64_t*>(rowPtr + container->columnAt(2).offset()) += 1;
  }
}

// Realistic MIN/MAX aggregation: VARCHAR key + VARCHAR accumulator
// But accumulator is set once and potentially replaced (not repeatedly
// overwritten)
void aggRealMinMax(
    int numGroups,
    int numUpdates,
    int keyLength,
    int valueLength) {
  auto container = std::make_unique<RowContainer>(
      std::vector<TypePtr>{VARCHAR()},
      std::vector<TypePtr>{VARCHAR()}, // min/max value
      false,
      getPool(),
      FLAGS_velox_enable_bump_allocator);

  std::mt19937 gen(42);
  std::uniform_int_distribution<> groupDist(0, numGroups - 1);

  // Phase 1: Create all groups with initial values
  std::vector<char*> groups(numGroups);
  std::vector<std::string> currentValues(numGroups);
  for (int i = 0; i < numGroups; ++i) {
    groups[i] = container->newRow();
    std::string keyStr = std::to_string(i) + std::string(keyLength - 5, 'k');
    StringView keyView(keyStr);
    container->stringAllocator().copyMultipart(
        keyView, groups[i], container->columnAt(0).offset());

    // Initial accumulator value
    currentValues[i] = std::string(valueLength, 'z');
    StringView accView(currentValues[i]);
    container->stringAllocator().copyMultipart(
        accView, groups[i], container->columnAt(1).offset());
  }

  // Phase 2: Simulate MIN updates - only update when new value is smaller
  // In practice, only ~log(N) updates per group, not N updates
  int updateCount = 0;
  for (int u = 0; u < numUpdates && updateCount < numGroups * 3; ++u) {
    int groupIdx = groupDist(gen);
    char currentChar = currentValues[groupIdx][0];
    // Simulate finding a smaller value (decreasing probability)
    if (gen() % 100 < 10) { // 10% chance of actually updating
      char newFirstChar = 'a' + (gen() % (currentChar - 'a' + 1));
      if (newFirstChar < currentChar) {
        currentValues[groupIdx][0] = newFirstChar;
        StringView newView(currentValues[groupIdx]);
        container->stringAllocator().copyMultipart(
            newView, groups[groupIdx], container->columnAt(1).offset());
        updateCount++;
      }
    }
  }
}

// =============================================================================
// BENCHMARK DEFINITIONS
// =============================================================================

// --- SORT: Key-only scenarios ---
BENCHMARK(Sort_BigintKey_100k) {
  sortBigintKeyOnly(100000);
}
BENCHMARK(Sort_BigintKey_1M) {
  sortBigintKeyOnly(1000000);
}
BENCHMARK(Sort_VarcharKey16_100k) {
  sortVarcharKeyOnly(100000, 16);
}
BENCHMARK(Sort_VarcharKey64_100k) {
  sortVarcharKeyOnly(100000, 64);
}
BENCHMARK(Sort_VarcharKey256_100k) {
  sortVarcharKeyOnly(100000, 256);
}
BENCHMARK(Sort_VarcharKey16_1M) {
  sortVarcharKeyOnly(1000000, 16);
}

// --- SORT: BIGINT key + VARCHAR dependents ---
BENCHMARK(Sort_BigintKey_1Varchar16_100k) {
  sortBigintKeyVarcharDep(100000, 1, 16);
}
BENCHMARK(Sort_BigintKey_1Varchar64_100k) {
  sortBigintKeyVarcharDep(100000, 1, 64);
}
BENCHMARK(Sort_BigintKey_1Varchar256_100k) {
  sortBigintKeyVarcharDep(100000, 1, 256);
}
BENCHMARK(Sort_BigintKey_4Varchar16_100k) {
  sortBigintKeyVarcharDep(100000, 4, 16);
}
BENCHMARK(Sort_BigintKey_4Varchar64_100k) {
  sortBigintKeyVarcharDep(100000, 4, 64);
}
BENCHMARK(Sort_BigintKey_4Varchar256_100k) {
  sortBigintKeyVarcharDep(100000, 4, 256);
}
BENCHMARK(Sort_BigintKey_1Varchar64_1M) {
  sortBigintKeyVarcharDep(1000000, 1, 64);
}
BENCHMARK(Sort_BigintKey_4Varchar64_1M) {
  sortBigintKeyVarcharDep(1000000, 4, 64);
}

// --- SORT: VARCHAR key + VARCHAR dependents ---
BENCHMARK(Sort_Varchar16Key_1Varchar64_100k) {
  sortVarcharKeyVarcharDep(100000, 16, 1, 64);
}
BENCHMARK(Sort_Varchar64Key_2Varchar64_100k) {
  sortVarcharKeyVarcharDep(100000, 64, 2, 64);
}
BENCHMARK(Sort_Varchar64Key_4Varchar64_100k) {
  sortVarcharKeyVarcharDep(100000, 64, 4, 64);
}

// --- SORT: Mixed key + mixed dependents ---
BENCHMARK(Sort_MixedKey_2Int2Varchar_100k) {
  sortMixedKeyMixedDep(100000, 32, 2, 2, 64);
}
BENCHMARK(Sort_MixedKey_0Int4Varchar_100k) {
  sortMixedKeyMixedDep(100000, 32, 0, 4, 64);
}
BENCHMARK(Sort_MixedKey_4Int0Varchar_100k) {
  sortMixedKeyMixedDep(100000, 32, 4, 0, 64);
}

// --- AGG: BIGINT key + VARCHAR accumulator ---
BENCHMARK(Agg_BigintKey_VarcharAcc16_10kG_100kU) {
  aggBigintKeyVarcharAcc(10000, 100000, 16);
}
BENCHMARK(Agg_BigintKey_VarcharAcc64_10kG_100kU) {
  aggBigintKeyVarcharAcc(10000, 100000, 64);
}
BENCHMARK(Agg_BigintKey_VarcharAcc256_10kG_100kU) {
  aggBigintKeyVarcharAcc(10000, 100000, 256);
}
BENCHMARK(Agg_BigintKey_VarcharAcc64_100kG_1MU) {
  aggBigintKeyVarcharAcc(100000, 1000000, 64);
}

// --- AGG: VARCHAR key + INT accumulator ---
BENCHMARK(Agg_Varchar16Key_IntAcc_10kG_100kU) {
  aggVarcharKeyIntAcc(10000, 100000, 16);
}
BENCHMARK(Agg_Varchar64Key_IntAcc_10kG_100kU) {
  aggVarcharKeyIntAcc(10000, 100000, 64);
}
BENCHMARK(Agg_Varchar64Key_IntAcc_100kG_1MU) {
  aggVarcharKeyIntAcc(100000, 1000000, 64);
}

// --- AGG: VARCHAR key + VARCHAR accumulator (worst case) ---
BENCHMARK(Agg_Varchar16Key_Varchar16Acc_10kG_100kU) {
  aggVarcharKeyVarcharAcc(10000, 100000, 16, 16);
}
BENCHMARK(Agg_Varchar64Key_Varchar64Acc_10kG_100kU) {
  aggVarcharKeyVarcharAcc(10000, 100000, 64, 64);
}
BENCHMARK(Agg_Varchar64Key_Varchar256Acc_10kG_100kU) {
  aggVarcharKeyVarcharAcc(10000, 100000, 64, 256);
}

// --- JOIN: BIGINT key ---
BENCHMARK(Join_BigintKey_NoPayload_100k) {
  joinBigintKeyNoPayload(100000);
}
BENCHMARK(Join_BigintKey_NoPayload_1M) {
  joinBigintKeyNoPayload(1000000);
}

// --- JOIN: VARCHAR key ---
BENCHMARK(Join_Varchar16Key_100k) {
  joinVarcharKey(100000, 16);
}
BENCHMARK(Join_Varchar64Key_100k) {
  joinVarcharKey(100000, 64);
}
BENCHMARK(Join_Varchar256Key_100k) {
  joinVarcharKey(100000, 256);
}
BENCHMARK(Join_Varchar64Key_1M) {
  joinVarcharKey(1000000, 64);
}

// --- JOIN: BIGINT key + VARCHAR payload ---
BENCHMARK(Join_BigintKey_1Varchar16_100k) {
  joinBigintKeyVarcharPayload(100000, 1, 16);
}
BENCHMARK(Join_BigintKey_1Varchar64_100k) {
  joinBigintKeyVarcharPayload(100000, 1, 64);
}
BENCHMARK(Join_BigintKey_1Varchar256_100k) {
  joinBigintKeyVarcharPayload(100000, 1, 256);
}
BENCHMARK(Join_BigintKey_4Varchar64_100k) {
  joinBigintKeyVarcharPayload(100000, 4, 64);
}
BENCHMARK(Join_BigintKey_4Varchar64_1M) {
  joinBigintKeyVarcharPayload(1000000, 4, 64);
}

// --- JOIN: Multi-VARCHAR key + mixed payload ---
BENCHMARK(Join_2Varchar32Key_2Int2Varchar_100k) {
  joinMultiVarcharKeyMixedPayload(100000, 2, 32, 2, 2, 64);
}
BENCHMARK(Join_3Varchar32Key_0Int4Varchar_100k) {
  joinMultiVarcharKeyMixedPayload(100000, 3, 32, 0, 4, 64);
}

// --- Direct allocator benchmarks ---
BENCHMARK(Direct_Alloc_1M_16bytes) {
  directAllocOnly(1000000, 16);
}
BENCHMARK(Direct_Alloc_1M_32bytes) {
  directAllocOnly(1000000, 32);
}
BENCHMARK(Direct_Alloc_1M_64bytes) {
  directAllocOnly(1000000, 64);
}
BENCHMARK(Direct_Alloc_1M_128bytes) {
  directAllocOnly(1000000, 128);
}
BENCHMARK(Direct_Alloc_1M_256bytes) {
  directAllocOnly(1000000, 256);
}
BENCHMARK(Direct_AllocFree_1M_32bytes) {
  directAllocFree(1000000, 32);
}
BENCHMARK(Direct_AllocFree_1M_64bytes) {
  directAllocFree(1000000, 64);
}

// --- REAL AGG: SUM/COUNT with VARCHAR key (key alloc only, int updates) ---
BENCHMARK(AggReal_SumCount_Varchar16Key_10kG_100kU) {
  aggRealSumCount(10000, 100000, 16);
}
BENCHMARK(AggReal_SumCount_Varchar64Key_10kG_100kU) {
  aggRealSumCount(10000, 100000, 64);
}
BENCHMARK(AggReal_SumCount_Varchar64Key_100kG_1MU) {
  aggRealSumCount(100000, 1000000, 64);
}

// --- REAL AGG: MIN/MAX with VARCHAR key + VARCHAR accumulator (sparse updates)
// ---
BENCHMARK(AggReal_MinMax_Varchar64Key_Varchar64Val_10kG_100kU) {
  aggRealMinMax(10000, 100000, 64, 64);
}
BENCHMARK(AggReal_MinMax_Varchar64Key_Varchar256Val_10kG_100kU) {
  aggRealMinMax(10000, 100000, 64, 256);
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);

  memory::MemoryManager::initialize(memory::MemoryManager::Options{});

  std::cout << "========================================================"
            << std::endl;
  std::cout << "Bump Allocator Comprehensive Benchmark" << std::endl;
  std::cout << "Mode: " << (FLAGS_velox_enable_bump_allocator ? "ON" : "OFF")
            << std::endl;
  std::cout << "========================================================"
            << std::endl;

  folly::runBenchmarks();

  // Clean up thread-local pools
  tlPool.reset();
  tlRootPool.reset();

  return 0;
}
