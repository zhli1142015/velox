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
    auto type = ROW({"k1", "k2", "v1"}, {VARCHAR(), VARCHAR(), BIGINT()});
    results.push_back(bm.runAgg(
        "HASH-L1", 10'000'000, 8'000'000, type, {"k1", "k2"}, {"sum(v1)"}));
  }

  // HASH-L2: kHash mode, medium cardinality. 10M → 2M distinct.
  // 3 VARCHAR keys → table ~32MB → uses lock-step (NOT AMAC).
  // Control case to compare against HASH-L1.
  if (shouldRun("HASH-L2")) {
    auto type = ROW(
        {"k1", "k2", "k3", "v1"}, {VARCHAR(), VARCHAR(), VARCHAR(), BIGINT()});
    results.push_back(bm.runAgg(
        "HASH-L2",
        10'000'000,
        2'000'000,
        type,
        {"k1", "k2", "k3"},
        {"sum(v1)"}));
  }

  printResults(results);
  return 0;
}
