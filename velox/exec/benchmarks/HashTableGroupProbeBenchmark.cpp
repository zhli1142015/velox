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

#include "velox/exec/HashTable.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <folly/Benchmark.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/init/Init.h>
#include <iostream>

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {
struct HashTableBenchmarkParams {
  HashTableBenchmarkParams() = default;

  // Benchmark params, we need to provide:
  //  -the expect hash mode,
  //  -the key schema,
  //  -the expected hash table size,
  //  -number of rows.
  HashTableBenchmarkParams(
      BaseHashTable::HashMode mode,
      const TypePtr& keyType,
      int64_t hashTableSize,
      int64_t size)
      : mode{mode}, keyType{keyType}, hashTableSize{hashTableSize}, size{size} {
    VELOX_CHECK_LE(hashTableSize, size);

    if (hashTableSize > BaseHashTable::kArrayHashMaxSize &&
        mode == BaseHashTable::HashMode::kArray) {
      VELOX_FAIL("Bad hash mode.");
    }

    numFields = keyType->size();

    title = fmt::format(
        "Size:{},{},{}",
        size,
        BaseHashTable::modeString(mode),
        keyType->toString());
  }

  // Expected mode.
  BaseHashTable::HashMode mode;

  // Type of key row.
  TypePtr keyType;

  // Distinct rows in the table.
  int64_t hashTableSize;

  // Number of rows.
  int64_t size;

  // Title for reporting
  std::string title;

  // Number of fields.
  int32_t numFields;

  // Number of batches.
  int32_t numBatches = 100;

  std::string toString() const {
    return fmt::format(
        "HashTableSize:{}, InputSize:{}, ExpectHashMode:{}",
        hashTableSize,
        size,
        BaseHashTable::modeString(mode));
  }
};

class HashTableGropeProbeBenchmark : public VectorTestBase {
 public:
  HashTableGropeProbeBenchmark() : randomEngine_((std::random_device{}())) {}

  // Create table and generate data.
  void prepare(HashTableBenchmarkParams params) {
    params_ = params;
    table_.reset();
    batches_.clear();
    createTable();
    makeBatches(batches_);
  }

  void insertGroups(int32_t batch) {
    const SelectivityVector rows(batches_[batch]->size());
    insertGroups(*batches_[batch], rows, *lookup_, *table_);
  }

  // Run 'groupProbe'.
  void groupProbe() {
    table_->groupProbe(*lookup_);
    // std::cout << table_->hashMode() << " " << params_.mode << std::endl;
    // VELOX_CHECK_EQ(topTable_->hashMode(), params_.mode);
  }

 private:
  RowVectorPtr makeRows(int64_t numKeys, int64_t maxKey, int64_t& seed) {
    std::vector<int64_t> data;
    auto makeData = [&]() {
      data.clear();
      for (auto i = 0; i < numKeys; ++i) {
        data.emplace_back((seed++) % maxKey);
      }
      data[0] = maxKey;
      std::shuffle(data.begin(), data.end(), randomEngine_);
    };

    std::vector<VectorPtr> children;
    for (int32_t i = 0; i < params_.numFields; ++i) {
      makeData();
      switch (params_.keyType->childAt(i)->kind()) {
        case TypeKind::BIGINT: {
          children.push_back(makeFlatVector<int64_t>(data));
          break;
        }
        case TypeKind::VARCHAR: {
          auto strings = BaseVector::create<FlatVector<StringView>>(
              VARCHAR(), data.size(), pool_.get());
          for (auto row = 0; row < data.size(); ++row) {
            auto string = fmt::format("---------------{}", data[row]);
            strings->set(row, StringView(string));
          }
          children.push_back(strings);
          break;
        }
        default:
          VELOX_FAIL(
              "Unsupported kind for makeVector {}",
              params_.keyType->childAt(i)->kind());
      }
    }
    return makeRowVector(children);
  }

  void makeBatches(std::vector<RowVectorPtr>& batches) {
    int64_t seed = 0;
    for (auto i = 0; i < params_.numBatches; ++i) {
      batches.push_back(makeRows(
          params_.size / params_.numBatches, params_.hashTableSize, seed));
    }
  }

  void createTable() {
    std::vector<std::unique_ptr<VectorHasher>> keyHashers;
    for (int j = 0; j < params_.numFields; ++j) {
      keyHashers.emplace_back(
          std::make_unique<VectorHasher>(params_.keyType->childAt(j), j));
    }

    table_ = HashTable<false>::createForAggregation(
        std::move(keyHashers), std::vector<Accumulator>{}, pool_.get());
    lookup_ = std::make_unique<HashLookup>(table_->hashers());
  }

  void insertGroups(
      const RowVector& input,
      const SelectivityVector& rows,
      HashLookup& lookup,
      HashTable<false>& table) {
    lookup.reset(rows.end());
    lookup.rows.clear();
    rows.applyToSelected([&](auto row) { lookup.rows.push_back(row); });

    auto& hashers = table.hashers();
    auto mode = table.hashMode();
    bool rehash = false;
    for (int32_t i = 0; i < hashers.size(); ++i) {
      auto key = input.childAt(hashers[i]->channel());
      hashers[i]->decode(*key, rows);
      if (mode != BaseHashTable::HashMode::kHash) {
        if (!hashers[i]->computeValueIds(rows, lookup_->hashes)) {
          rehash = true;
        }
      } else {
        hashers[i]->hash(rows, i > 0, lookup_->hashes);
      }
    }

    if (rehash) {
      if (table.hashMode() != BaseHashTable::HashMode::kHash) {
        table.decideHashMode(input.size());
      }
      insertGroups(input, rows, lookup, table);
      return;
    }
  }

  std::default_random_engine randomEngine_;
  std::unique_ptr<HashTable<false>> table_;
  std::unique_ptr<HashLookup> lookup_;
  std::vector<RowVectorPtr> batches_;
  HashTableBenchmarkParams params_;
};

void initNormalizedKeyModeBenchmarkParams(
    std::vector<HashTableBenchmarkParams>& params) {
  int64_t size = 4096 * 1000;
  params.push_back(HashTableBenchmarkParams(
      BaseHashTable::HashMode::kNormalizedKey,
      ROW({"k1"}, {BIGINT()}),
      2L << 20,
      size));
  params.push_back(HashTableBenchmarkParams(
      BaseHashTable::HashMode::kNormalizedKey,
      ROW({"k1"}, {BIGINT()}),
      size,
      size));
  params.push_back(HashTableBenchmarkParams(
      BaseHashTable::HashMode::kNormalizedKey,
      ROW({"k1", "k2"}, {VARCHAR(), BIGINT()}),
      10000,
      size));
  params.push_back(HashTableBenchmarkParams(
      BaseHashTable::HashMode::kNormalizedKey,
      ROW({"k1", "k2"}, {BIGINT(), BIGINT()}),
      (2L << 20) - 3,
      size));
  params.push_back(HashTableBenchmarkParams(
      BaseHashTable::HashMode::kNormalizedKey,
      ROW({"k1", "k2", "k3"}, {VARCHAR(), BIGINT(), BIGINT()}),
      10000,
      size));
  params.push_back(HashTableBenchmarkParams(
      BaseHashTable::HashMode::kNormalizedKey,
      ROW({"k1", "k2", "k3"}, {BIGINT(), BIGINT(), BIGINT()}),
      2L << 18,
      size));
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManagerOptions options;
  options.useMmapAllocator = true;
  options.allocatorCapacity = 10UL << 30;
  options.useMmapArena = true;
  options.mmapArenaCapacityRatio = 1;
  memory::MemoryManager::initialize(options);

  auto bm = std::make_unique<HashTableGropeProbeBenchmark>();
  std::vector<HashTableBenchmarkParams> params;
  // initArrayModeBenchmarkParams(params);
  initNormalizedKeyModeBenchmarkParams(params);
  // initHashModeBenchmarkParams(params);

  for (auto& param : params) {
    folly::addBenchmark(__FILE__, param.title, [param, &bm]() {
      folly::BenchmarkSuspender suspender;
      bm->prepare(param);
      suspender.dismiss();
      for (auto i = 0; i < param.numBatches; ++i) {
        folly::BenchmarkSuspender suspender;
        bm->insertGroups(i);
        suspender.dismiss();
        bm->groupProbe();
      }
      return 1;
    });
  }
  folly::runBenchmarks();
  return 0;
}
