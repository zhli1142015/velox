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

// Standalone Q67-like profiling benchmark for callgrind / cachegrind.
// Usage:
//   valgrind --tool=callgrind ./velox_q67_profile baseline
//   valgrind --tool=callgrind ./velox_q67_profile partitioned
//   valgrind --tool=cachegrind ./velox_q67_profile baseline
//   valgrind --tool=cachegrind ./velox_q67_profile partitioned

#include <folly/init/Init.h>
#include <iostream>

#include "velox/common/memory/Memory.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::core;

namespace {
using namespace facebook::velox::test;

class Q67Profiler : public VectorTestBase {
 public:
  Q67Profiler() {
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
  }

  void run(bool enablePartitioning, int64_t numRows, int64_t numDistinct) {
    auto type =
        ROW({"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "v1"},
            {BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT()});

    std::cerr << "Generating input: " << numRows << " rows, " << numDistinct
              << " distinct..." << std::endl;

    auto input = generateInput(type, numRows, numDistinct);

    std::cerr << "Running " << (enablePartitioning ? "PARTITIONED" : "BASELINE")
              << " Q67 agg..." << std::endl;

    auto plan =
        PlanBuilder()
            .values(input)
            .singleAggregation(
                {"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"}, {"sum(v1)"})
            .planNode();

    auto start = std::chrono::steady_clock::now();

    AssertQueryBuilder builder(plan);
    builder.serialExecution(true);
    if (!enablePartitioning) {
      builder.config(
          QueryConfig::kHashTableDirectoryPartitionThreshold,
          std::to_string(INT64_MAX));
    }

    auto rows = builder.countResults();
    auto end = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
                  .count();

    std::cerr << "Done: " << rows << " result rows in " << ms << "ms"
              << std::endl;
  }

  /// Benchmark-style run: warmup + N baseline + N partitioned, same input.
  void runBenchmarkStyle(int64_t numRows, int64_t numDistinct, int iterations) {
    auto type =
        ROW({"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "v1"},
            {BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT()});

    std::cerr << "Generating input: " << numRows << " rows, " << numDistinct
              << " distinct..." << std::endl;
    auto input = generateInput(type, numRows, numDistinct);

    auto runOnce = [&](bool enablePartitioning) -> int64_t {
      auto plan =
          PlanBuilder()
              .values(input)
              .singleAggregation(
                  {"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"}, {"sum(v1)"})
              .planNode();
      auto start = std::chrono::steady_clock::now();
      AssertQueryBuilder builder(plan);
      builder.serialExecution(true);
      if (!enablePartitioning) {
        builder.config(
            QueryConfig::kHashTableDirectoryPartitionThreshold,
            std::to_string(INT64_MAX));
      }
      auto rows = builder.countResults();
      auto end = std::chrono::steady_clock::now();
      return std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
          .count();
    };

    // Warmup
    std::cerr << "Warmup (baseline)..." << std::endl;
    runOnce(false);

    // Baseline
    int64_t totalBaseline = 0;
    for (int i = 0; i < iterations; ++i) {
      auto ms = runOnce(false);
      std::cerr << "  Baseline " << (i + 1) << ": " << ms << "ms" << std::endl;
      totalBaseline += ms;
    }

    // Partitioned
    int64_t totalPartitioned = 0;
    for (int i = 0; i < iterations; ++i) {
      auto ms = runOnce(true);
      std::cerr << "  Partitioned " << (i + 1) << ": " << ms << "ms"
                << std::endl;
      totalPartitioned += ms;
    }

    auto avgBase = totalBaseline / iterations;
    auto avgPart = totalPartitioned / iterations;
    double speedup = static_cast<double>(avgBase) / avgPart;
    std::cerr << "\nResult: baseline=" << avgBase
              << "ms, partitioned=" << avgPart << "ms, speedup=" << speedup
              << "x" << std::endl;
  }

 private:
  std::vector<RowVectorPtr>
  generateInput(const RowTypePtr& type, int64_t numRows, int64_t numDistinct) {
    const int64_t batchSize = std::min<int64_t>(numRows, 100000);
    const int64_t numBatches = (numRows + batchSize - 1) / batchSize;
    std::vector<RowVectorPtr> batches;
    batches.reserve(numBatches);

    for (int64_t b = 0; b < numBatches; ++b) {
      auto rowsInBatch = static_cast<vector_size_t>(
          std::min<int64_t>(batchSize, numRows - b * batchSize));
      std::vector<VectorPtr> children;
      for (int c = 0; c < type->size(); ++c) {
        children.push_back(
            makeFlatVector<int64_t>(
                rowsInBatch, [&](vector_size_t row) -> int64_t {
                  return (b * batchSize + row) % numDistinct;
                }));
      }
      batches.push_back(makeRowVector(type->names(), children));
    }
    return batches;
  }
};

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv, false);
  memory::MemoryManager::Options options;
  options.useMmapAllocator = false;
  options.allocatorCapacity = 10UL << 30;
  memory::MemoryManager::initialize(options);

  if (argc < 2) {
    std::cerr << "Usage: " << argv[0]
              << " <baseline|partitioned|benchmark> [numRows] [numDistinct]"
              << std::endl;
    return 1;
  }

  std::string mode = argv[1];

  // Use smaller sizes for valgrind (10x smaller).
  int64_t numRows = (argc > 2) ? std::stoll(argv[2]) : 2'000'000;
  int64_t numDistinct = (argc > 3) ? std::stoll(argv[3]) : 1'800'000;

  Q67Profiler profiler;

  if (mode == "benchmark") {
    profiler.runBenchmarkStyle(numRows, numDistinct, 3);
  } else {
    bool partitioned = (mode == "partitioned");
    profiler.run(partitioned, numRows, numDistinct);
  }

  return 0;
}
