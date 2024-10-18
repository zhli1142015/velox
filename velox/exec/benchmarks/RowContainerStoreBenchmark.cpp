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

#include "glog/logging.h"
#include "velox/exec/PrefixSort.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

class TestCase {
 public:
  TestCase(
      memory::MemoryPool* pool,
      const std::string& testName,
      size_t numRows,
      const RowTypePtr& rowType,
      int numKeys,
      int stringLength)
      : testName_(testName),
        numRows_(numRows),
        pool_(pool),
        rowType_(rowType),
        stringLength_(stringLength) {
    // Initialize a RowContainer that holds fuzzed rows to be sorted.
    std::vector<TypePtr> keyTypes;
    std::vector<TypePtr> dependentTypes;
    for (auto i = 0; i < rowType->size(); ++i) {
      if (i < numKeys) {
        keyTypes.push_back(rowType->childAt(i));
      } else {
        dependentTypes.push_back(rowType->childAt(i));
      }
    }
    container_ = std::make_unique<RowContainer>(keyTypes, dependentTypes, pool);
    data_ = fuzzRows(numRows, numKeys);
  };

  const std::string& testName() const {
    return testName_;
  }

  size_t numRows() const {
    return numRows_;
  }

  const std::vector<char*>& rows() const {
    return rows_;
  }

  RowContainer* rowContainer() const {
    return container_.get();
  }

  void storeRows() {
    rows_.resize(numRows_);
    for (auto row = 0; row < numRows_; ++row) {
      rows_[row] = container_->newRow();
    }
    for (auto column = 0; column < data_->childrenSize(); ++column) {
      DecodedVector decoded(*data_->childAt(column));
      container_->store(decoded, folly::Range(rows_.data(), numRows_), column);
    }
  }

 private:
  RowVectorPtr fuzzRows(size_t numRows, int numKeys) {
    VectorFuzzer fuzzer({.vectorSize = numRows}, pool_);
    VectorFuzzer fuzzerWithNulls(
        {.vectorSize = numRows, .nullRatio = 0.1, .stringLength = 12}, pool_);
    std::vector<VectorPtr> children;

    // Fuzz keys: for front keys (column 0 to numKeys -2) use high
    // nullRatio to enforce all columns to be compared.
    {
      for (auto i = 0; i < numKeys - 1; ++i) {
        children.push_back(fuzzerWithNulls.fuzz(rowType_->childAt(i)));
      }
      children.push_back(fuzzer.fuzz(rowType_->childAt(numKeys - 1)));
    }
    // Fuzz payload
    {
      for (auto i = numKeys; i < rowType_->size(); ++i) {
        children.push_back(fuzzer.fuzz(rowType_->childAt(i)));
      }
    }
    return std::make_shared<RowVector>(
        pool_, rowType_, nullptr, numRows, std::move(children));
  }

  const std::string testName_;
  const size_t numRows_;
  // Rows address stored in RowContainer
  std::vector<char*> rows_;
  std::unique_ptr<RowContainer> container_;
  memory::MemoryPool* const pool_;
  const RowTypePtr rowType_;
  RowVectorPtr data_;
  const int32_t stringLength_;
};

class RowContainerStoreBenchmark {
 public:
  RowContainerStoreBenchmark(memory::MemoryPool* pool) : pool_(pool) {}

  void addBenchmark(
      const std::string& testName,
      size_t numRows,
      const RowTypePtr& rowType,
      int iterations,
      int numKeys,
      int stringLength) {
    auto testCase = std::make_unique<TestCase>(
        pool_, testName, numRows, rowType, numKeys, stringLength);
    {
      folly::addBenchmark(__FILE__, testCase->testName(), [&]() {
        for (auto i = 0; i < iterations; ++i) {
          testCase->storeRows();
        }
        return 1;
      });
    }
    testCases_.push_back(std::move(testCase));
  }

  void benchmark(
      const std::string& prefix,
      const std::string& keyName,
      const std::vector<vector_size_t>& batchSizes,
      const std::vector<RowTypePtr>& rowTypes,
      const std::vector<int>& numKeys,
      int32_t iterations) {
    for (auto len : {12, 50}) {
      for (auto batchSize : batchSizes) {
        for (auto i = 0; i < rowTypes.size(); ++i) {
          const auto name = fmt::format(
              "{}_{}_{}_{}k_{}",
              prefix,
              numKeys[i],
              keyName,
              batchSize / 1000.0,
              len);
          addBenchmark(
              name, batchSize, rowTypes[i], iterations, numKeys[i], len);
        }
      }
    }
  }

  void largeVarchar() {
    const auto iterations = 1;
    const std::vector<vector_size_t> batchSizes = {
        1'000, 10'000, 100'000, 1'000'000};
    std::vector<RowTypePtr> rowTypes = {
        ROW({VARCHAR()}),
        ROW({VARCHAR(), VARCHAR()}),
        ROW({VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR()}),
        ROW(
            {VARCHAR(),
             VARCHAR(),
             VARCHAR(),
             VARCHAR(),
             VARCHAR(),
             VARCHAR(),
             VARCHAR(),
             VARCHAR()}),
    };
    std::vector<int> numKeys = {1, 2, 4, 8};
    benchmark(
        "no-payloads", "varchar", batchSizes, rowTypes, numKeys, iterations);
  }

 private:
  std::vector<std::unique_ptr<TestCase>> testCases_;
  memory::MemoryPool* pool_;
};
} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);

  memory::MemoryManager::initialize({});
  auto rootPool = memory::memoryManager()->addRootPool();
  auto leafPool = rootPool->addLeafChild("leaf");

  RowContainerStoreBenchmark bm(leafPool.get());

  bm.largeVarchar();

  folly::runBenchmarks();
  return 0;
}
