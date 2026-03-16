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
#include <functional>
#include <gflags/gflags.h>

#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

DEFINE_int32(num_iterations, 3, "Number of iterations per benchmark case");
DEFINE_string(
    case_filter,
    "",
    "If set, run only cases whose name exactly matches this string.");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::velox::exec::test {
using namespace facebook::velox::test;

struct BenchmarkResult {
  std::string name;
  int64_t avgNs{0};
  int64_t outputRows{0};
};

class DirectoryPartitionBenchmark : public VectorTestBase {
 public:
  DirectoryPartitionBenchmark() {
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
  }

  /// Run an aggregation benchmark case (single mode, no comparison).
  BenchmarkResult runAgg(
      const std::string& name,
      int64_t numRows,
      int64_t numDistinct,
      const RowTypePtr& inputType,
      const std::vector<std::string>& groupKeys,
      const std::vector<std::string>& aggregates,
      double nullRatio = 0.0) {
    auto input = generateAggInput(inputType, numRows, numDistinct, nullRatio);

    BenchmarkResult result;
    result.name = name;

    // Warmup.
    runAggPlan(input, inputType, groupKeys, aggregates);

    // Measure.
    int64_t totalNs = 0;
    for (int i = 0; i < FLAGS_num_iterations; ++i) {
      auto [ns, rows] = runAggPlan(input, inputType, groupKeys, aggregates);
      totalNs += ns;
      result.outputRows = rows;
    }
    result.avgNs = totalNs / FLAGS_num_iterations;
    return result;
  }

  /// Run a hash join benchmark case.
  BenchmarkResult runJoin(
      const std::string& name,
      const std::vector<RowVectorPtr>& probeInput,
      const std::vector<RowVectorPtr>& buildInput,
      const std::vector<std::string>& probeKeys,
      const std::vector<std::string>& buildKeys,
      const std::vector<std::string>& outputLayout,
      core::JoinType joinType) {
    BenchmarkResult result;
    result.name = name;

    // Warmup.
    runJoinPlan(
        probeInput, buildInput, probeKeys, buildKeys, outputLayout, joinType);

    // Measure.
    int64_t totalNs = 0;
    for (int i = 0; i < FLAGS_num_iterations; ++i) {
      auto [ns, rows] = runJoinPlan(
          probeInput, buildInput, probeKeys, buildKeys, outputLayout, joinType);
      totalNs += ns;
      result.outputRows = rows;
    }
    result.avgNs = totalNs / FLAGS_num_iterations;
    return result;
  }

  /// Generate batched data for one side of a join.
  /// @param valueGen (columnIndex, globalRowIndex) -> int64_t value.
  std::vector<RowVectorPtr> generateJoinSide(
      const RowTypePtr& type,
      int64_t numRows,
      std::function<int64_t(int, int64_t)> valueGen) {
    const int64_t batchSize = std::min<int64_t>(numRows, 4096);
    const int64_t numBatches = (numRows + batchSize - 1) / batchSize;
    std::vector<RowVectorPtr> batches;
    batches.reserve(numBatches);

    for (int64_t b = 0; b < numBatches; ++b) {
      auto rowsInBatch = static_cast<vector_size_t>(
          std::min<int64_t>(batchSize, numRows - b * batchSize));
      std::vector<VectorPtr> children;
      for (int c = 0; c < type->size(); ++c) {
        auto vec = makeFlatVector<int64_t>(
            rowsInBatch, [&](vector_size_t row) -> int64_t {
              return valueGen(c, b * batchSize + row);
            });
        children.push_back(std::move(vec));
      }
      batches.push_back(makeRowVector(type->names(), children));
    }
    return batches;
  }

  /// Generate batched data for one side of a join with mixed
  /// BIGINT/VARCHAR columns. BIGINT columns use @p valueGen; VARCHAR columns
  /// are filled with ~30-char formatted strings.
  std::vector<RowVectorPtr> generateJoinSideMixed(
      const RowTypePtr& type,
      int64_t numRows,
      std::function<int64_t(int, int64_t)> valueGen) {
    const int64_t batchSize = std::min<int64_t>(numRows, 4096);
    const int64_t numBatches = (numRows + batchSize - 1) / batchSize;
    std::vector<RowVectorPtr> batches;
    batches.reserve(numBatches);

    for (int64_t b = 0; b < numBatches; ++b) {
      auto rowsInBatch = static_cast<vector_size_t>(
          std::min<int64_t>(batchSize, numRows - b * batchSize));
      std::vector<VectorPtr> children;
      for (int c = 0; c < type->size(); ++c) {
        if (type->childAt(c)->isVarchar()) {
          auto vec = BaseVector::create(VARCHAR(), rowsInBatch, pool());
          auto flat = vec->as<FlatVector<StringView>>();
          for (vector_size_t r = 0; r < rowsInBatch; ++r) {
            auto str =
                fmt::format("value_{:020d}", valueGen(c, b * batchSize + r));
            flat->set(r, StringView(str));
          }
          children.push_back(std::move(vec));
        } else {
          auto vec = makeFlatVector<int64_t>(
              rowsInBatch, [&](vector_size_t row) -> int64_t {
                return valueGen(c, b * batchSize + row);
              });
          children.push_back(std::move(vec));
        }
      }
      batches.push_back(makeRowVector(type->names(), children));
    }
    return batches;
  }

 private:
  std::vector<RowVectorPtr> generateAggInput(
      const RowTypePtr& type,
      int64_t numRows,
      int64_t numDistinct,
      double nullRatio = 0.0) {
    const int64_t batchSize = std::min<int64_t>(numRows, 4096);
    const int64_t numBatches = (numRows + batchSize - 1) / batchSize;
    std::vector<RowVectorPtr> batches;
    batches.reserve(numBatches);

    for (int64_t b = 0; b < numBatches; ++b) {
      auto rowsInBatch = static_cast<vector_size_t>(
          std::min<int64_t>(batchSize, numRows - b * batchSize));
      std::vector<VectorPtr> children;
      for (int c = 0; c < type->size(); ++c) {
        if (type->childAt(c)->isVarchar()) {
          auto vec = BaseVector::create(VARCHAR(), rowsInBatch, pool());
          auto flat = vec->as<FlatVector<StringView>>();
          for (vector_size_t r = 0; r < rowsInBatch; ++r) {
            auto val = (b * batchSize + r) % numDistinct;
            auto str = fmt::format("{:016d}", val);
            flat->set(r, StringView(str));
          }
          if (nullRatio > 0) {
            auto period =
                std::max<int64_t>(1, static_cast<int64_t>(1.0 / nullRatio));
            for (vector_size_t r = 0; r < rowsInBatch; ++r) {
              if ((b * batchSize + r) % period == 0) {
                flat->setNull(r, true);
              }
            }
          }
          children.push_back(std::move(vec));
        } else if (type->childAt(c)->isDouble()) {
          auto vec = makeFlatVector<double>(
              rowsInBatch, [&](vector_size_t row) -> double {
                return static_cast<double>((b * batchSize + row) % numDistinct);
              });
          if (nullRatio > 0) {
            auto flat = vec->as<FlatVector<double>>();
            auto period =
                std::max<int64_t>(1, static_cast<int64_t>(1.0 / nullRatio));
            for (vector_size_t r = 0; r < rowsInBatch; ++r) {
              if ((b * batchSize + r) % period == 0) {
                flat->setNull(r, true);
              }
            }
          }
          children.push_back(std::move(vec));
        } else {
          auto vec = makeFlatVector<int64_t>(
              rowsInBatch, [&](vector_size_t row) -> int64_t {
                return (b * batchSize + row) % numDistinct;
              });
          if (nullRatio > 0) {
            auto flat = vec->as<FlatVector<int64_t>>();
            auto period =
                std::max<int64_t>(1, static_cast<int64_t>(1.0 / nullRatio));
            for (vector_size_t r = 0; r < rowsInBatch; ++r) {
              if ((b * batchSize + r) % period == 0) {
                flat->setNull(r, true);
              }
            }
          }
          children.push_back(std::move(vec));
        }
      }
      batches.push_back(makeRowVector(type->names(), children));
    }
    return batches;
  }

  std::pair<int64_t, int64_t> runAggPlan(
      const std::vector<RowVectorPtr>& input,
      const RowTypePtr& inputType,
      const std::vector<std::string>& groupKeys,
      const std::vector<std::string>& aggregates) {
    auto plan = PlanBuilder()
                    .values(input)
                    .singleAggregation(groupKeys, aggregates)
                    .planNode();

    auto start = std::chrono::steady_clock::now();

    AssertQueryBuilder builder(plan);
    builder.serialExecution(true);

    auto rows = builder.countResults();
    auto end = std::chrono::steady_clock::now();
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                  .count();
    return {ns, static_cast<int64_t>(rows)};
  }

  std::pair<int64_t, int64_t> runJoinPlan(
      const std::vector<RowVectorPtr>& probeInput,
      const std::vector<RowVectorPtr>& buildInput,
      const std::vector<std::string>& probeKeys,
      const std::vector<std::string>& buildKeys,
      const std::vector<std::string>& outputLayout,
      core::JoinType joinType) {
    auto planNodeIdGenerator =
        std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(probeInput)
                    .hashJoin(
                        probeKeys,
                        buildKeys,
                        PlanBuilder(planNodeIdGenerator)
                            .values(buildInput)
                            .planNode(),
                        "",
                        outputLayout,
                        joinType)
                    .planNode();

    auto start = std::chrono::steady_clock::now();
    AssertQueryBuilder builder(plan);
    builder.serialExecution(true);
    auto rows = builder.countResults();
    auto end = std::chrono::steady_clock::now();
    auto ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
    return {ns, static_cast<int64_t>(rows)};
  }
};

void printResults(const std::vector<BenchmarkResult>& results) {
  fmt::print("\n{:<25} {:>12} {:>10}\n", "Case", "AvgTime", "Rows");
  fmt::print("{:-<50}\n", "");
  for (const auto& r : results) {
    auto ms = r.avgNs / 1e6;
    fmt::print("{:<25} {:>9.1f}ms {:>10}\n", r.name, ms, r.outputRows);
    // CSV output for script consumption.
    fmt::print("CSV,{},{},{}\n", r.name, r.avgNs, r.outputRows);
  }
  fmt::print("\n");
}

} // namespace facebook::velox::exec::test

using namespace facebook::velox::exec::test;

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::Options options;
  options.useMmapAllocator = false;
  options.allocatorCapacity = 10UL << 30;
  memory::MemoryManager::initialize(options);

  DirectoryPartitionBenchmark bm;
  std::vector<BenchmarkResult> results;

  auto shouldRun = [](const std::string& name) {
    return FLAGS_case_filter.empty() || FLAGS_case_filter == name;
  };

  // ────────────────────────────────────────────────────────────
  // Original Aggregation Cases (from TPCDS profiles)
  // ────────────────────────────────────────────────────────────

  // AGG-S1: Small agg, kArray mode. 100K rows, 100 distinct.
  if (shouldRun("AGG-S1")) {
    auto type = ROW({"k1", "v1", "v2"}, {BIGINT(), BIGINT(), BIGINT()});
    results.push_back(bm.runAgg(
        "AGG-S1", 100'000, 100, type, {"k1"}, {"sum(v1)", "count(v2)"}));
  }

  // AGG-M2: Medium agg, kNK mode. 10M rows, 5M distinct.
  if (shouldRun("AGG-M2")) {
    auto type =
        ROW({"k1", "k2", "v1", "v2"}, {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
    results.push_back(bm.runAgg(
        "AGG-M2",
        10'000'000,
        5'000'000,
        type,
        {"k1", "k2"},
        {"sum(v1)", "count(v2)"}));
  }

  // AGG-L1: Large agg, kNK mode. 50M rows, 40M distinct.
  if (shouldRun("AGG-L1")) {
    auto type = ROW({"k1", "v1", "v2"}, {BIGINT(), BIGINT(), BIGINT()});
    results.push_back(bm.runAgg(
        "AGG-L1",
        50'000'000,
        40'000'000,
        type,
        {"k1"},
        {"sum(v1)", "count(v2)"}));
  }

  // AGG-L2: Large agg, kNK mode. 20M rows, 18M distinct.
  if (shouldRun("AGG-L2")) {
    auto type = ROW({"k1", "v1", "v2"}, {BIGINT(), BIGINT(), BIGINT()});
    results.push_back(bm.runAgg(
        "AGG-L2",
        20'000'000,
        18'000'000,
        type,
        {"k1"},
        {"sum(v1)", "count(v2)"}));
  }

  // TPCDS-Q97: kNK, 7.6M rows, 7.5M distinct.
  if (shouldRun("TPCDS-Q97")) {
    auto type =
        ROW({"ss_customer_sk", "ss_item_sk", "ss_sales_price"},
            {BIGINT(), BIGINT(), BIGINT()});
    results.push_back(bm.runAgg(
        "TPCDS-Q97",
        7'597'000,
        7'522'000,
        type,
        {"ss_customer_sk", "ss_item_sk"},
        {"sum(ss_sales_price)"}));
  }

  // TPCDS-Q65: kNK, 7.4M rows, 7.1M distinct.
  if (shouldRun("TPCDS-Q65")) {
    auto type =
        ROW({"ss_store_sk", "ss_item_sk", "ss_sales_price"},
            {BIGINT(), BIGINT(), BIGINT()});
    results.push_back(bm.runAgg(
        "TPCDS-Q65",
        7'418'000,
        7'059'000,
        type,
        {"ss_store_sk", "ss_item_sk"},
        {"sum(ss_sales_price)"}));
  }

  // TPCDS-Q67: kHash (VARCHAR keys), 2.7M rows, 1.9M distinct.
  if (shouldRun("TPCDS-Q67")) {
    auto type =
        ROW({"i_category",
             "i_class",
             "i_brand",
             "i_product_name",
             "d_year",
             "d_qoy",
             "d_moy",
             "s_store_id",
             "v1"},
            {VARCHAR(),
             VARCHAR(),
             VARCHAR(),
             VARCHAR(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             VARCHAR(),
             BIGINT()});
    results.push_back(bm.runAgg(
        "TPCDS-Q67",
        2'671'000,
        1'922'000,
        type,
        {"i_category",
         "i_class",
         "i_brand",
         "i_product_name",
         "d_year",
         "d_qoy",
         "d_moy",
         "s_store_id"},
        {"sum(v1)"}));
  }

  // TPCDS-Q23a: kNK, 9.7M rows, 962K distinct.
  if (shouldRun("TPCDS-Q23a")) {
    auto type =
        ROW({"d_date", "ss_item_sk", "v1"}, {BIGINT(), BIGINT(), BIGINT()});
    results.push_back(bm.runAgg(
        "TPCDS-Q23a",
        9'693'000,
        961'749,
        type,
        {"d_date", "ss_item_sk"},
        {"count(v1)"}));
  }

  // TPCDS-Q4: kNK, 7.6M rows, 657K distinct.
  if (shouldRun("TPCDS-Q4")) {
    auto type =
        ROW({"d_year", "ss_customer_sk", "ss_ext_list_price"},
            {BIGINT(), BIGINT(), BIGINT()});
    results.push_back(bm.runAgg(
        "TPCDS-Q4",
        7'564'000,
        657'000,
        type,
        {"d_year", "ss_customer_sk"},
        {"sum(ss_ext_list_price)"}));
  }

  // TPCDS-Q38: kNK, 7.4M rows, 658K distinct.
  if (shouldRun("TPCDS-Q38")) {
    auto type =
        ROW({"d_date", "ss_customer_sk", "v1"}, {BIGINT(), BIGINT(), BIGINT()});
    results.push_back(bm.runAgg(
        "TPCDS-Q38",
        7'418'000,
        658'000,
        type,
        {"d_date", "ss_customer_sk"},
        {"count(v1)"}));
  }

  // TPCDS-Q47: kHash (VARCHAR), 9.1M rows, 31K distinct. High compression.
  if (shouldRun("TPCDS-Q47")) {
    auto type =
        ROW({"i_category",
             "i_brand",
             "s_store_name",
             "s_company_name",
             "d_year",
             "d_moy",
             "v1"},
            {VARCHAR(),
             VARCHAR(),
             VARCHAR(),
             VARCHAR(),
             BIGINT(),
             BIGINT(),
             BIGINT()});
    results.push_back(bm.runAgg(
        "TPCDS-Q47",
        9'106'000,
        31'100,
        type,
        {"i_category",
         "i_brand",
         "s_store_name",
         "s_company_name",
         "d_year",
         "d_moy"},
        {"sum(v1)"}));
  }

  // TPCDS-Q1: kNK, 272K rows, 270K distinct.
  if (shouldRun("TPCDS-Q1")) {
    auto type =
        ROW({"sr_customer_sk", "sr_store_sk", "sr_fee"},
            {BIGINT(), BIGINT(), BIGINT()});
    results.push_back(bm.runAgg(
        "TPCDS-Q1",
        271'768,
        269'620,
        type,
        {"sr_customer_sk", "sr_store_sk"},
        {"sum(sr_fee)"}));
  }

  // ────────────────────────────────────────────────────────────
  // Extended Cases: High Compression, Nulls, Various Aggregates
  // ────────────────────────────────────────────────────────────

  // AGG-HC1: High compression, kNK. 10M → 1K distinct.
  if (shouldRun("AGG-HC1")) {
    auto type = ROW({"k1", "v1", "v2"}, {BIGINT(), BIGINT(), BIGINT()});
    results.push_back(bm.runAgg(
        "AGG-HC1", 10'000'000, 1'000, type, {"k1"}, {"sum(v1)", "count(v2)"}));
  }

  // AGG-HC2: High compression, kHash (VARCHAR). 5M → 500.
  if (shouldRun("AGG-HC2")) {
    auto type = ROW({"k1", "k2", "v1"}, {VARCHAR(), VARCHAR(), BIGINT()});
    results.push_back(
        bm.runAgg("AGG-HC2", 5'000'000, 500, type, {"k1", "k2"}, {"sum(v1)"}));
  }

  // AGG-NULL1: Nullable values, kNK. 10M → 5M, 10% nulls.
  if (shouldRun("AGG-NULL1")) {
    auto type = ROW({"k1", "v1", "v2"}, {BIGINT(), BIGINT(), BIGINT()});
    results.push_back(bm.runAgg(
        "AGG-NULL1",
        10'000'000,
        5'000'000,
        type,
        {"k1"},
        {"sum(v1)", "count(v2)"},
        0.1));
  }

  // AGG-NULL2: Nullable values, kHash (VARCHAR). 5M → 2M, 20% nulls.
  if (shouldRun("AGG-NULL2")) {
    auto type = ROW({"k1", "k2", "v1"}, {VARCHAR(), VARCHAR(), BIGINT()});
    results.push_back(bm.runAgg(
        "AGG-NULL2",
        5'000'000,
        2'000'000,
        type,
        {"k1", "k2"},
        {"sum(v1)"},
        0.2));
  }

  // AGG-MINMAX: min/max aggregates, kNK. 10M → 5M.
  if (shouldRun("AGG-MINMAX")) {
    auto type = ROW({"k1", "v1", "v2"}, {BIGINT(), BIGINT(), BIGINT()});
    results.push_back(bm.runAgg(
        "AGG-MINMAX",
        10'000'000,
        5'000'000,
        type,
        {"k1"},
        {"min(v1)", "max(v2)"}));
  }

  // AGG-AVG: avg aggregate, kNK. 10M → 5M.
  if (shouldRun("AGG-AVG")) {
    auto type = ROW({"k1", "v1"}, {BIGINT(), BIGINT()});
    results.push_back(
        bm.runAgg("AGG-AVG", 10'000'000, 5'000'000, type, {"k1"}, {"avg(v1)"}));
  }

  // AGG-MULTI: Multiple aggregates, kNK. 10M → 5M.
  if (shouldRun("AGG-MULTI")) {
    auto type =
        ROW({"k1", "v1", "v2", "v3"}, {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
    results.push_back(bm.runAgg(
        "AGG-MULTI",
        10'000'000,
        5'000'000,
        type,
        {"k1"},
        {"sum(v1)", "count(v2)", "min(v3)", "max(v3)"}));
  }

  // AGG-DBL: Double-precision sum, kNK. 10M → 5M.
  if (shouldRun("AGG-DBL")) {
    auto type = ROW({"k1", "v1"}, {BIGINT(), DOUBLE()});
    results.push_back(
        bm.runAgg("AGG-DBL", 10'000'000, 5'000'000, type, {"k1"}, {"sum(v1)"}));
  }

  // AGG-HC-NK: High compression kNK, 2 keys. 20M → 10K.
  if (shouldRun("AGG-HC-NK")) {
    auto type =
        ROW({"k1", "k2", "v1", "v2"}, {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
    results.push_back(bm.runAgg(
        "AGG-HC-NK",
        20'000'000,
        10'000,
        type,
        {"k1", "k2"},
        {"sum(v1)", "avg(v2)"}));
  }

  // HASH-L1: kHash mode, high cardinality. 10M → 8M distinct.
  // 2 VARCHAR keys → table ~128MB → triggers AMAC for kHash.
  // Tests whether AMAC helps with variable-cost string compareKeys.
  if (shouldRun("HASH-L1")) {
    auto type =
        ROW({"k1", "k2", "v1"}, {VARCHAR(), VARCHAR(), BIGINT()});
    results.push_back(bm.runAgg(
        "HASH-L1", 10'000'000, 8'000'000, type, {"k1", "k2"}, {"sum(v1)"}));
  }

  // HASH-L2: kHash mode, medium cardinality. 10M → 2M distinct.
  // 3 VARCHAR keys → table ~32MB → uses lock-step (NOT AMAC).
  // Control case to compare against HASH-L1.
  if (shouldRun("HASH-L2")) {
    auto type =
        ROW({"k1", "k2", "k3", "v1"}, {VARCHAR(), VARCHAR(), VARCHAR(), BIGINT()});
    results.push_back(bm.runAgg(
        "HASH-L2",
        10'000'000,
        2'000'000,
        type,
        {"k1", "k2", "k3"},
        {"sum(v1)"}));
  }

  // ────────────────────────────────────────────────────────────
  // Hash Join Cases (from TPCDS workload analysis)
  // ────────────────────────────────────────────────────────────

  // JOIN-Q97: FullOuter, balanced, 2 BIGINT keys with ~30% overlap.
  // Build 1.4M × Probe 2.7M. Build key1 in [0, 1M), probe key1 in
  // [500K, 2M). Overlap in key1: [500K, 1M) exercises hash matching.
  if (shouldRun("JOIN-Q97")) {
    auto probeType =
        ROW({"p_key1", "p_key2", "p_val1"}, {BIGINT(), BIGINT(), BIGINT()});
    auto buildType =
        ROW({"b_key1", "b_key2", "b_val1"}, {BIGINT(), BIGINT(), BIGINT()});
    auto probeData = bm.generateJoinSide(
        probeType, 2'671'267, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return (row % 1'500'000) + 500'000;
          if (c == 1)
            return row / 100;
          return row;
        });
    auto buildData = bm.generateJoinSide(
        buildType, 1'422'144, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row % 1'000'000;
          if (c == 1)
            return row / 100;
          return row;
        });
    results.push_back(bm.runJoin(
        "JOIN-Q97",
        probeData,
        buildData,
        {"p_key1", "p_key2"},
        {"b_key1", "b_key2"},
        {"p_key1", "p_key2", "p_val1", "b_key1", "b_key2", "b_val1"},
        core::JoinType::kFull));
  }

  // JOIN-Q24a: Inner, high fan-out (build-side duplication).
  // Build 63K rows with 12 unique keys (~5305 rows/key) × Probe 1.7K rows.
  // Each probe row matches ~5305 build rows → ~9.1M output.
  if (shouldRun("JOIN-Q24a")) {
    auto probeType =
        ROW({"p_key", "p_val1"}, {BIGINT(), BIGINT()});
    auto buildType =
        ROW({"b_key", "b_val1"}, {BIGINT(), BIGINT()});
    auto probeData = bm.generateJoinSide(
        probeType, 1'719, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row % 12;
          return row;
        });
    auto buildData = bm.generateJoinSide(
        buildType, 63'662, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row % 12;
          return row;
        });
    results.push_back(bm.runJoin(
        "JOIN-Q24a",
        probeData,
        buildData,
        {"p_key"},
        {"b_key"},
        {"p_key", "p_val1", "b_val1"},
        core::JoinType::kInner));
  }

  // JOIN-Q64: Inner, large build+probe, selective (10:1 reduction).
  // Build 720K (unique keys) × Probe 7.2M → 720K output.
  if (shouldRun("JOIN-Q64")) {
    auto probeType =
        ROW({"p_key", "p_val1", "p_val2"}, {BIGINT(), BIGINT(), BIGINT()});
    auto buildType =
        ROW({"b_key", "b_val1", "b_val2"}, {BIGINT(), BIGINT(), BIGINT()});
    auto probeData = bm.generateJoinSide(
        probeType, 7'199'881, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row; // unique keys 0..7199880
          return row;
        });
    auto buildData = bm.generateJoinSide(
        buildType, 720'023, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row; // unique keys 0..720022
          return row;
        });
    results.push_back(bm.runJoin(
        "JOIN-Q64",
        probeData,
        buildData,
        {"p_key"},
        {"b_key"},
        {"p_key", "p_val1", "p_val2", "b_val1", "b_val2"},
        core::JoinType::kInner));
  }

  // JOIN-Q16: Inner, huge build (7.2M), tiny probe (706) — build-heavy.
  // All probe keys within build range → ~706 output.
  if (shouldRun("JOIN-Q16")) {
    auto probeType =
        ROW({"p_key", "p_val1", "p_val2"}, {BIGINT(), BIGINT(), BIGINT()});
    auto buildType =
        ROW({"b_key", "b_val1", "b_val2"}, {BIGINT(), BIGINT(), BIGINT()});
    auto probeData = bm.generateJoinSide(
        probeType, 706, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row; // keys 0..705, all in build range
          return row;
        });
    auto buildData = bm.generateJoinSide(
        buildType, 7'199'881, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row; // unique keys 0..7199880
          return row;
        });
    results.push_back(bm.runJoin(
        "JOIN-Q16",
        probeData,
        buildData,
        {"p_key"},
        {"b_key"},
        {"p_key", "p_val1", "p_val2", "b_val1", "b_val2"},
        core::JoinType::kInner));
  }

  // JOIN-Q67: Inner, tiny build (1500), large probe (2.7M) — probe passthrough.
  // All probe keys within build range → 2.7M output.
  if (shouldRun("JOIN-Q67")) {
    auto probeType =
        ROW({"p_key", "p_val1", "p_val2"}, {BIGINT(), BIGINT(), BIGINT()});
    auto buildType =
        ROW({"b_key", "b_val1", "b_val2"}, {BIGINT(), BIGINT(), BIGINT()});
    auto probeData = bm.generateJoinSide(
        probeType, 2'670'552, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return (row % 1500) + 1; // keys 1..1500
          return row;
        });
    auto buildData = bm.generateJoinSide(
        buildType, 1'500, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row + 1; // keys 1..1500
          return row;
        });
    results.push_back(bm.runJoin(
        "JOIN-Q67",
        probeData,
        buildData,
        {"p_key"},
        {"b_key"},
        {"p_key", "p_val1", "p_val2", "b_val1", "b_val2"},
        core::JoinType::kInner));
  }

  // JOIN-Q4: Inner, tiny build (365), huge probe (7.4M) — passthrough.
  // All probe keys within build range → 7.4M output.
  if (shouldRun("JOIN-Q4")) {
    auto probeType =
        ROW({"p_key", "p_val1", "p_val2"}, {BIGINT(), BIGINT(), BIGINT()});
    auto buildType =
        ROW({"b_key", "b_val1", "b_val2"}, {BIGINT(), BIGINT(), BIGINT()});
    auto probeData = bm.generateJoinSide(
        probeType, 7'418'105, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return (row % 365) + 1; // keys 1..365
          return row;
        });
    auto buildData = bm.generateJoinSide(
        buildType, 365, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row + 1; // keys 1..365
          return row;
        });
    results.push_back(bm.runJoin(
        "JOIN-Q4",
        probeData,
        buildData,
        {"p_key"},
        {"b_key"},
        {"p_key", "p_val1", "p_val2", "b_val1", "b_val2"},
        core::JoinType::kInner));
  }

  // JOIN-SEMI: LeftSemiFilter, highly selective (EXISTS pattern).
  // Build 138K unique keys × Probe 4.3M sparse keys → ~1381 output.
  // Probe key = row * 100, so only rows where row*100 < 138093 match.
  if (shouldRun("JOIN-SEMI")) {
    auto probeType =
        ROW({"p_key", "p_val1", "p_val2"}, {BIGINT(), BIGINT(), BIGINT()});
    auto buildType =
        ROW({"b_key", "b_val1"}, {BIGINT(), BIGINT()});
    auto probeData = bm.generateJoinSide(
        probeType, 4'336'023, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row * 100;
          return row;
        });
    auto buildData = bm.generateJoinSide(
        buildType, 138'093, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row;
          return row;
        });
    results.push_back(bm.runJoin(
        "JOIN-SEMI",
        probeData,
        buildData,
        {"p_key"},
        {"b_key"},
        {"p_key", "p_val1", "p_val2"},
        core::JoinType::kLeftSemiFilter));
  }

  // JOIN-ANTI: LeftAnti, selective (NOT EXISTS pattern).
  // Build 100K unique keys × Probe 1M keys → ~900K output.
  // Probe keys [0, 1M), build keys [0, 100K) → 900K probe rows unmatched.
  if (shouldRun("JOIN-ANTI")) {
    auto probeType =
        ROW({"p_key", "p_val1", "p_val2"}, {BIGINT(), BIGINT(), BIGINT()});
    auto buildType =
        ROW({"b_key", "b_val1"}, {BIGINT(), BIGINT()});
    auto probeData = bm.generateJoinSide(
        probeType, 1'000'000, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row;
          return row;
        });
    auto buildData = bm.generateJoinSide(
        buildType, 100'000, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row;
          return row;
        });
    results.push_back(bm.runJoin(
        "JOIN-ANTI",
        probeData,
        buildData,
        {"p_key"},
        {"b_key"},
        {"p_key", "p_val1", "p_val2"},
        core::JoinType::kAnti));
  }

  // JOIN-C2R: Build-side C2R heavy — 2M wide build rows (8 VARCHAR cols)
  // inserted into RowContainer, small probe (100K).
  if (shouldRun("JOIN-C2R")) {
    auto buildType = ROW(
        {"b_key",
         "b_v0",
         "b_v1",
         "b_v2",
         "b_v3",
         "b_v4",
         "b_v5",
         "b_v6",
         "b_v7"},
        {BIGINT(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR()});
    auto probeType = ROW({"p_key", "p_val"}, {BIGINT(), BIGINT()});

    auto buildData = bm.generateJoinSideMixed(
        buildType, 2'000'000, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row; // unique key
          return row; // varchar content seed
        });
    auto probeData = bm.generateJoinSide(
        probeType, 100'000, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row; // matches build keys [0, 100K)
          return row;
        });
    results.push_back(bm.runJoin(
        "JOIN-C2R",
        probeData,
        buildData,
        {"p_key"},
        {"b_key"},
        {"p_key", "p_val"},
        core::JoinType::kInner));
  }

  // JOIN-R2C: Probe-output R2C heavy — build has duplicate keys (10 per key)
  // causing output expansion. Probe 1M → Output 10M (10x fan-out).
  // Output includes 6 wide VARCHAR columns from build → massive R2C cost.
  if (shouldRun("JOIN-R2C")) {
    auto buildType = ROW(
        {"b_key", "b_v0", "b_v1", "b_v2", "b_v3", "b_v4", "b_v5"},
        {BIGINT(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR(),
         VARCHAR()});
    auto probeType = ROW({"p_key", "p_val"}, {BIGINT(), BIGINT()});

    // Build: 50K rows, key = row % 5000 → 10 duplicates per key.
    // Each probe match produces 10 output rows.
    auto buildData = bm.generateJoinSideMixed(
        buildType, 50'000, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row % 5'000; // 5000 unique keys, 10 rows each
          return row;
        });
    // Probe: 1M rows, key = row % 5000 → all match.
    // Output = 1M × 10 = 10M rows (massive expansion).
    auto probeData = bm.generateJoinSide(
        probeType, 1'000'000, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row % 5'000; // all keys match build
          return row;
        });
    results.push_back(bm.runJoin(
        "JOIN-R2C",
        probeData,
        buildData,
        {"p_key"},
        {"b_key"},
        {"p_key", "p_val", "b_v0", "b_v1", "b_v2", "b_v3", "b_v4", "b_v5"},
        core::JoinType::kInner));
  }

  // JOIN-NK-L: Large kNK mode join — 2 BIGINT keys (composite), DRAM-bound.
  // Build 5M unique composite keys × Probe 10M → 10M output (1:1 match).
  if (shouldRun("JOIN-NK-L")) {
    auto buildType =
        ROW({"b_k1", "b_k2", "b_val"}, {BIGINT(), BIGINT(), BIGINT()});
    auto probeType =
        ROW({"p_k1", "p_k2", "p_val"}, {BIGINT(), BIGINT(), BIGINT()});
    auto buildData = bm.generateJoinSide(
        buildType, 5'000'000, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row; // k1 = row
          if (c == 1)
            return row / 100; // k2 = row / 100
          return row;
        });
    auto probeData = bm.generateJoinSide(
        probeType, 10'000'000, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row % 5'000'000; // k1 matches build
          if (c == 1)
            return (row % 5'000'000) / 100; // k2 matches build
          return row;
        });
    results.push_back(bm.runJoin(
        "JOIN-NK-L",
        probeData,
        buildData,
        {"p_k1", "p_k2"},
        {"b_k1", "b_k2"},
        {"p_k1", "p_k2", "p_val", "b_val"},
        core::JoinType::kInner));
  }

  // JOIN-HASH-L: Large kHash mode join — VARCHAR key, DRAM-bound.
  // Build 2M unique VARCHAR keys × Probe 5M → 5M output.
  if (shouldRun("JOIN-HASH-L")) {
    auto buildType =
        ROW({"b_key", "b_val1", "b_val2"}, {VARCHAR(), BIGINT(), BIGINT()});
    auto probeType = ROW({"p_key", "p_val"}, {VARCHAR(), BIGINT()});
    auto buildData = bm.generateJoinSideMixed(
        buildType, 2'000'000, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row; // unique key seed
          return row;
        });
    auto probeData = bm.generateJoinSideMixed(
        probeType, 5'000'000, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row % 2'000'000; // all match build
          return row;
        });
    results.push_back(bm.runJoin(
        "JOIN-HASH-L",
        probeData,
        buildData,
        {"p_key"},
        {"b_key"},
        {"p_key", "p_val", "b_val1", "b_val2"},
        core::JoinType::kInner));
  }

  // JOIN-ARR-L: Large kArray mode join — high cardinality within array range.
  // Build 500K unique keys × Probe 10M → 10M output (20x fan-out).
  // 500K < 2M (kArrayHashMaxSize) → should use kArray mode.
  if (shouldRun("JOIN-ARR-L")) {
    auto buildType =
        ROW({"b_key", "b_val1", "b_val2"}, {BIGINT(), BIGINT(), BIGINT()});
    auto probeType = ROW({"p_key", "p_val"}, {BIGINT(), BIGINT()});
    auto buildData = bm.generateJoinSide(
        buildType, 500'000, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row; // unique keys [0, 500K)
          return row;
        });
    auto probeData = bm.generateJoinSide(
        probeType, 10'000'000, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row % 500'000; // all match, ~20x fan-out
          return row;
        });
    results.push_back(bm.runJoin(
        "JOIN-ARR-L",
        probeData,
        buildData,
        {"p_key"},
        {"b_key"},
        {"p_key", "p_val", "b_val1", "b_val2"},
        core::JoinType::kInner));
  }

  // JOIN-HASH-SK: kHash with selective VARCHAR key (dimension join pattern).
  // Build 100K unique VARCHAR keys × Probe 20M → 20M output (passthrough).
  if (shouldRun("JOIN-HASH-SK")) {
    auto buildType = ROW(
        {"b_key", "b_val1", "b_val2", "b_val3"},
        {VARCHAR(), BIGINT(), BIGINT(), BIGINT()});
    auto probeType = ROW({"p_key", "p_val"}, {VARCHAR(), BIGINT()});
    auto buildData = bm.generateJoinSideMixed(
        buildType, 100'000, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row; // unique key seed
          return row;
        });
    auto probeData = bm.generateJoinSideMixed(
        probeType, 20'000'000, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row % 100'000; // all match build
          return row;
        });
    results.push_back(bm.runJoin(
        "JOIN-HASH-SK",
        probeData,
        buildData,
        {"p_key"},
        {"b_key"},
        {"p_key", "p_val", "b_val1", "b_val2", "b_val3"},
        core::JoinType::kInner));
  }

  // JOIN-Q97-L: Scaled-up Q97 — FullOuter, 2 BIGINT keys, DRAM-bound.
  // Build 5M × Probe 10M → ~15M output. Table ~57MB → exceeds L3.
  if (shouldRun("JOIN-Q97-L")) {
    auto probeType =
        ROW({"p_key1", "p_key2", "p_val1"}, {BIGINT(), BIGINT(), BIGINT()});
    auto buildType =
        ROW({"b_key1", "b_key2", "b_val1"}, {BIGINT(), BIGINT(), BIGINT()});
    // Build keys [0, 5M), probe keys [2.5M, 12.5M) → 50% overlap
    auto probeData = bm.generateJoinSide(
        probeType, 10'000'000, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row + 2'500'000;
          if (c == 1)
            return (row + 2'500'000) % 50000;
          return row;
        });
    auto buildData = bm.generateJoinSide(
        buildType, 5'000'000, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row;
          if (c == 1)
            return row % 50000;
          return row;
        });
    results.push_back(bm.runJoin(
        "JOIN-Q97-L",
        probeData,
        buildData,
        {"p_key1", "p_key2"},
        {"b_key1", "b_key2"},
        {"p_key1", "p_key2", "p_val1", "b_key1", "b_key2", "b_val1"},
        core::JoinType::kFull));
  }

  // JOIN-Q94: Build-heavy medium — 3.6M build, 27K probe (TPCDS Q94 pattern).
  // Build dominates. Table ~41MB. Tests build at medium scale.
  if (shouldRun("JOIN-Q94")) {
    auto probeType =
        ROW({"p_key", "p_val1"}, {BIGINT(), BIGINT()});
    auto buildType =
        ROW({"b_key", "b_val1", "b_val2"}, {BIGINT(), BIGINT(), BIGINT()});
    auto probeData = bm.generateJoinSide(
        probeType, 26'582, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row;
          return row * 7;
        });
    auto buildData = bm.generateJoinSide(
        buildType, 3'599'799, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row;
          return row * (c + 3);
        });
    results.push_back(bm.runJoin(
        "JOIN-Q94",
        probeData,
        buildData,
        {"p_key"},
        {"b_key"},
        {"p_key", "p_val1", "b_val1", "b_val2"},
        core::JoinType::kInner));
  }

  // JOIN-HASH-XL: Very large kHash VARCHAR join — 5M build, 20M probe.
  // Tests string hashing + comparison at production scale.
  // Table ~57MB + string storage → heavily DRAM-bound.
  if (shouldRun("JOIN-HASH-XL")) {
    auto buildType = ROW(
        {"b_key", "b_val1", "b_val2"},
        {VARCHAR(), BIGINT(), BIGINT()});
    auto probeType = ROW(
        {"p_key", "p_val1"},
        {VARCHAR(), BIGINT()});
    auto buildData = bm.generateJoinSideMixed(
        buildType, 5'000'000, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row; // unique VARCHAR key
          return row * 13;
        });
    auto probeData = bm.generateJoinSideMixed(
        probeType, 20'000'000, [](int c, int64_t row) -> int64_t {
          if (c == 0)
            return row % 5'000'000; // all match build
          return row * 7;
        });
    results.push_back(bm.runJoin(
        "JOIN-HASH-XL",
        probeData,
        buildData,
        {"p_key"},
        {"b_key"},
        {"p_key", "p_val1", "b_val1", "b_val2"},
        core::JoinType::kInner));
  }

  printResults(results);
  return 0;
}
