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
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include "velox/exec/VectorHasher.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

using namespace facebook;
using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {
class BenchmarkBase {
 public:
  VectorMaker vectorMaker() const {
    return vectorMaker_;
  }

  BufferPtr makeIndices(
      vector_size_t size,
      std::function<vector_size_t(vector_size_t)> indexAt) {
    BufferPtr indices =
        AlignedBuffer::allocate<vector_size_t>(size, pool_.get());
    auto rawIndices = indices->asMutable<vector_size_t>();
    for (auto i = 0; i < size; i++) {
      rawIndices[i] = indexAt(i);
    }
    return indices;
  }

  VectorPtr makeDictionary(vector_size_t size, const VectorPtr& base) {
    auto baseSize = base->size();
    return BaseVector::wrapInDictionary(
        BufferPtr(nullptr),
        makeIndices(
            size, [baseSize](vector_size_t row) { return row % baseSize; }),
        size,
        base);
  }

  memory::MemoryPool* pool() {
    return pool_.get();
  }

 private:
  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  VectorMaker vectorMaker_{pool_.get()};
};

template <typename T>
void benchmarkComputeValueIds(bool withNulls) {
  folly::BenchmarkSuspender suspender;
  vector_size_t size = 1'000;
  BenchmarkBase base;
  VectorHasher hasher(CppToType<T>::create(), 0);
  auto values = base.vectorMaker().flatVector<T>(
      size,
      [](vector_size_t row) { return row % 17; },
      withNulls ? velox::test::VectorMaker::nullEvery(7) : nullptr);

  raw_vector<uint64_t> hashes(size, base.pool());
  SelectivityVector rows(size);
  hasher.decode(*values, rows);
  hasher.computeValueIds(rows, hashes);
  hasher.enableValueRange(1, 0);
  suspender.dismiss();

  for (int i = 0; i < 10'000; i++) {
    hasher.decode(*values, rows);
    bool ok = hasher.computeValueIds(rows, hashes);
    folly::doNotOptimizeAway(ok);
  }
}
} // namespace

// Uses SIMD acceleration
BENCHMARK(computeValueIdsBigintNoNulls) {
  benchmarkComputeValueIds<int64_t>(false);
}

// Doesn't use SIMD acceleration
BENCHMARK_RELATIVE(computeValueIdsBigintWithNulls) {
  benchmarkComputeValueIds<int64_t>(true);
}

BENCHMARK(computeValueIdsIntegerNoNulls) {
  benchmarkComputeValueIds<int32_t>(false);
}

BENCHMARK_RELATIVE(computeValueIdsIntegerWithNulls) {
  benchmarkComputeValueIds<int32_t>(true);
}

BENCHMARK(computeValueIdsSmallintNoNulls) {
  benchmarkComputeValueIds<int16_t>(false);
}

BENCHMARK_RELATIVE(computeValueIdsSmallintWithNulls) {
  benchmarkComputeValueIds<int16_t>(true);
}

// Benchmark for integer types using UniqueValues mode (not range mode)
// This tests the caching optimization for flat integer vectors
template <typename T>
void benchmarkComputeValueIdsUniqueMode(bool flatten) {
  folly::BenchmarkSuspender suspender;
  vector_size_t size = 1'000;
  BenchmarkBase base;

  // Create a dictionary vector with low cardinality (10 unique values)
  auto baseVector = base.vectorMaker().flatVector<T>(
      10, [](vector_size_t row) { return row * 100; });

  auto dictVector = base.makeDictionary(size, baseVector);

  VectorPtr vector;
  if (flatten) {
    vector = VectorMaker::flatten(dictVector);
  } else {
    vector = dictVector;
  }

  std::unique_ptr<exec::VectorHasher> hasher =
      exec::VectorHasher::create(vector->type(), 0);

  raw_vector<uint64_t> result(size, base.pool());
  SelectivityVector rows(size);

  // First pass to populate uniqueValues_
  hasher->decode(*vector, rows);
  auto ok = hasher->computeValueIds(rows, result);
  folly::doNotOptimizeAway(ok);

  // Enable value IDs mode (not range mode)
  hasher->enableValueIds(1, 0);

  suspender.dismiss();

  for (int i = 0; i < 10'000; i++) {
    hasher->decode(*vector, rows);
    ok = hasher->computeValueIds(rows, result);
    folly::doNotOptimizeAway(ok);
  }
}

BENCHMARK(computeValueIdsBigintDictUnique) {
  benchmarkComputeValueIdsUniqueMode<int64_t>(false);
}

BENCHMARK_RELATIVE(computeValueIdsBigintFlatUnique) {
  benchmarkComputeValueIdsUniqueMode<int64_t>(true);
}

// Benchmark for multi-column integer keys (tests SIMD with multiplier != 1)
// Uses Range mode where SIMD optimization applies
void benchmarkComputeValueIdsMultiColumnInt(bool flatten) {
  folly::BenchmarkSuspender suspender;
  BenchmarkBase base;
  vector_size_t size = 1'000;

  // Create 4 integer columns with low cardinality
  std::vector<VectorPtr> baseVectors;
  baseVectors.push_back(
      base.vectorMaker().flatVector<int64_t>(10, [](auto row) { return row; }));
  baseVectors.push_back(base.vectorMaker().flatVector<int64_t>(
      20, [](auto row) { return row * 100; }));
  baseVectors.push_back(base.vectorMaker().flatVector<int64_t>(
      15, [](auto row) { return row * 1000; }));
  baseVectors.push_back(base.vectorMaker().flatVector<int64_t>(
      25, [](auto row) { return row * 10000; }));

  std::vector<VectorPtr> vectors;
  vectors.reserve(4);
  for (auto& baseVector : baseVectors) {
    auto dict = base.makeDictionary(size, baseVector);
    if (flatten) {
      vectors.emplace_back(VectorMaker::flatten(dict));
    } else {
      vectors.emplace_back(dict);
    }
  }

  std::vector<std::unique_ptr<exec::VectorHasher>> hashers;
  hashers.reserve(4);
  for (int i = 0; i < 4; i++) {
    hashers.emplace_back(exec::VectorHasher::create(vectors[i]->type(), i));
  }

  SelectivityVector allRows(size);
  uint64_t multiplier = 1;
  for (int i = 0; i < 4; i++) {
    auto hasher = hashers[i].get();
    raw_vector<uint64_t> result(size, base.pool());
    hasher->decode(*vectors[i], allRows);
    auto ok = hasher->computeValueIds(allRows, result);
    folly::doNotOptimizeAway(ok);
    multiplier = hasher->enableValueRange(multiplier, 0);
  }
  suspender.dismiss();

  raw_vector<uint64_t> result(size, base.pool());
  for (int i = 0; i < 10'000; i++) {
    for (int j = 0; j < 4; j++) {
      auto hasher = hashers[j].get();
      hasher->decode(*vectors[j], allRows);
      bool ok = hasher->computeValueIds(allRows, result);
      folly::doNotOptimizeAway(ok);
    }
  }
}

// This test compares:
// - Dict: Dictionary vector, no SIMD (uses cachedHashes_)
// - Flat: Flat vector, uses SIMD with multiplier > 1 (new optimization)
BENCHMARK(computeValueIdsMultiColIntDict) {
  benchmarkComputeValueIdsMultiColumnInt(false);
}

BENCHMARK_RELATIVE(computeValueIdsMultiColIntFlat) {
  benchmarkComputeValueIdsMultiColumnInt(true);
}

void benchmarkComputeValueIdsForStrings(bool flattenDictionaries) {
  folly::BenchmarkSuspender suspender;
  BenchmarkBase base;
  auto b0 = base.vectorMaker().flatVector({"2021-02-02", "2021-02-01"});
  auto b1 = base.vectorMaker().flatVector({"red", "green"});
  auto b2 = base.vectorMaker().flatVector(
      {"apple", "orange", "grapefruit", "banana", "star fruit", "potato"});
  auto b3 = base.vectorMaker().flatVector(
      {"pine", "birch", "elm", "maple", "chestnut"});

  std::vector<VectorPtr> baseVectors = {b0, b1, b2, b3};

  std::vector<VectorPtr> dictionaryVectors;
  dictionaryVectors.reserve(baseVectors.size());

  vector_size_t size = 1'000;
  for (auto& baseVector : baseVectors) {
    dictionaryVectors.emplace_back(base.makeDictionary(size, baseVector));
  }

  std::vector<VectorPtr> vectors;
  if (flattenDictionaries) {
    vectors.reserve(dictionaryVectors.size());
    for (auto& dictionaryVector : dictionaryVectors) {
      vectors.emplace_back(VectorMaker::flatten(dictionaryVector));
    }
  } else {
    vectors = dictionaryVectors;
  }

  std::vector<std::unique_ptr<exec::VectorHasher>> hashers;
  hashers.reserve(4);
  for (int i = 0; i < 4; i++) {
    hashers.emplace_back(exec::VectorHasher::create(vectors[i]->type(), i));
  }

  SelectivityVector allRows(size);
  uint64_t multiplier = 1;
  for (int i = 0; i < 4; i++) {
    auto hasher = hashers[i].get();
    raw_vector<uint64_t> result(size, base.pool());
    hasher->decode(*vectors[i], allRows);
    auto ok = hasher->computeValueIds(allRows, result);
    folly::doNotOptimizeAway(ok);

    multiplier = hasher->enableValueIds(multiplier, 0);
  }
  suspender.dismiss();

  raw_vector<uint64_t> result(size, base.pool());
  for (int i = 0; i < 10'000; i++) {
    for (int j = 0; j < 4; j++) {
      auto hasher = hashers[j].get();
      auto vector = vectors[j];
      hasher->decode(*vector, allRows);
      bool ok = hasher->computeValueIds(allRows, result);
      folly::doNotOptimizeAway(ok);
    }
  }
}

BENCHMARK(computeValueIdsDictionaryStrings) {
  benchmarkComputeValueIdsForStrings(false);
}

BENCHMARK_RELATIVE(computeValueIdsFlatStrings) {
  benchmarkComputeValueIdsForStrings(true);
}

BENCHMARK(computeValueIdsLowCardinalityLargeBatchSize) {
  folly::BenchmarkSuspender suspender;

  vector_size_t cardinality = 300;
  vector_size_t batchSize = 30'000'000;
  BenchmarkBase base;

  std::vector<std::optional<int64_t>> data(batchSize);
  for (int i = 0; i < batchSize; i++) {
    data[i] = i % cardinality;
  }
  auto values = base.vectorMaker().dictionaryVector<int64_t>(data);

  for (int i = 0; i < 10; i++) {
    raw_vector<uint64_t> hashes(batchSize, base.pool());
    SelectivityVector rows(batchSize);
    VectorHasher hasher(BIGINT(), 0);
    hasher.decode(*values, rows);
    suspender.dismiss();

    bool ok = hasher.computeValueIds(rows, hashes);
    folly::doNotOptimizeAway(ok);
    suspender.rehire();
  }
}

BENCHMARK(computeValueIdsLowCardinalityNotAllUsed) {
  folly::BenchmarkSuspender suspender;

  vector_size_t cardinality = 300;
  vector_size_t batchSize = 30'000'000;
  BenchmarkBase base;

  auto data = base.vectorMaker().flatVector<int64_t>(
      cardinality, [](vector_size_t row) { return row; });
  BufferPtr indices = allocateIndices(batchSize, base.pool());
  auto rawIndices = indices->asMutable<vector_size_t>();
  // Assign indices such that array is reversed.
  for (size_t i = 0; i < batchSize; ++i) {
    rawIndices[i] = i % (cardinality - 1);
  }
  auto values = BaseVector::wrapInDictionary(nullptr, indices, batchSize, data);

  for (int i = 0; i < 10; i++) {
    raw_vector<uint64_t> hashes(batchSize, base.pool());
    SelectivityVector rows(batchSize);
    VectorHasher hasher(BIGINT(), 0);
    hasher.decode(*values, rows);
    suspender.dismiss();

    bool ok = hasher.computeValueIds(rows, hashes);
    folly::doNotOptimizeAway(ok);
    suspender.rehire();
  }
}

BENCHMARK(computeValueIdsDictionaryForFiltering) {
  folly::BenchmarkSuspender suspender;

  vector_size_t cardinality = 30'000'000;
  vector_size_t batchSize = 300;
  BenchmarkBase base;

  auto data = base.vectorMaker().flatVector<int64_t>(
      cardinality, [](vector_size_t row) { return row; });
  BufferPtr indices = allocateIndices(batchSize, base.pool());
  auto rawIndices = indices->asMutable<vector_size_t>();
  // Assign indices such that array is reversed.
  for (size_t i = 0; i < batchSize; ++i) {
    rawIndices[i] = i * 1000;
  }
  auto values = BaseVector::wrapInDictionary(nullptr, indices, batchSize, data);

  for (int i = 0; i < 10; i++) {
    raw_vector<uint64_t> hashes(batchSize, base.pool());
    SelectivityVector rows(batchSize);
    VectorHasher hasher(BIGINT(), 0);
    hasher.decode(*values, rows);
    suspender.dismiss();

    bool ok = hasher.computeValueIds(rows, hashes);
    folly::doNotOptimizeAway(ok);
    suspender.rehire();
  }
}

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  folly::runBenchmarks();
  return 0;
}
