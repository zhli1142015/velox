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

#include "velox/core/QueryConfig.h"
#include "velox/exec/PlanNodeStats.h"
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
DEFINE_string(
    run_mode,
    "",
    "If 'baseline' or 'partitioned', run only that mode and print CSV. "
    "Empty = run both modes (normal).");
DEFINE_string(
    profile_mode,
    "",
    "If set to 'baseline' or 'partitioned', run only JOIN-LL in that mode "
    "for profiling. Empty = normal benchmark.");
DEFINE_int64(
    profile_build_rows,
    20'000'000,
    "Build rows for profile mode (reduce for valgrind)");
DEFINE_int64(
    profile_probe_rows,
    40'000'000,
    "Probe rows for profile mode (reduce for valgrind)");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::core;

namespace facebook::velox::exec::test {
using namespace facebook::velox::test;

struct BenchmarkResult {
  std::string name;
  int64_t baselineNs{0};
  int64_t partitionedNs{0};
  int64_t baselineRows{0};
  int64_t partitionedRows{0};

  double speedup() const {
    return partitionedNs > 0 ? static_cast<double>(baselineNs) / partitionedNs
                             : 0.0;
  }
};

class DirectoryPartitionBenchmark : public VectorTestBase {
 public:
  DirectoryPartitionBenchmark() {
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
  }

  // Public wrappers for profile mode.
  std::vector<RowVectorPtr> generateJoinInputPublic(
      const RowTypePtr& type,
      int64_t numRows,
      int64_t numDistinct) {
    return generateAggInput(type, numRows, numDistinct);
  }

  std::pair<int64_t, int64_t> runJoinPlanPublic(
      const std::vector<RowVectorPtr>& buildInput,
      const std::vector<RowVectorPtr>& probeInput,
      const RowTypePtr& buildType,
      const RowTypePtr& probeType,
      const std::vector<std::string>& joinKeys,
      const std::vector<std::string>& outputCols,
      JoinType joinType,
      bool enablePartitioning,
      bool printStats = false) {
    return runJoinPlan(
        buildInput,
        probeInput,
        buildType,
        probeType,
        joinKeys,
        outputCols,
        joinType,
        enablePartitioning,
        printStats);
  }

  /// Run an aggregation benchmark case.
  BenchmarkResult runAgg(
      const std::string& name,
      int64_t numRows,
      int64_t numDistinct,
      const RowTypePtr& inputType,
      const std::vector<std::string>& groupKeys,
      const std::vector<std::string>& aggregates) {
    auto input = generateAggInput(inputType, numRows, numDistinct);

    BenchmarkResult result;
    result.name = name;

    const bool runBaseline =
        FLAGS_run_mode.empty() || FLAGS_run_mode == "baseline";
    const bool runPartitioned =
        FLAGS_run_mode.empty() || FLAGS_run_mode == "partitioned";

    // Warmup whichever modes we will run.
    if (runBaseline) {
      runAggPlan(input, inputType, groupKeys, aggregates, false);
    }
    if (runPartitioned) {
      runAggPlan(input, inputType, groupKeys, aggregates, true);
    }

    // Measure.
    int64_t totalBaseline = 0;
    int64_t totalPartitioned = 0;
    for (int i = 0; i < FLAGS_num_iterations; ++i) {
      if (runBaseline) {
        auto [ns, rows] =
            runAggPlan(input, inputType, groupKeys, aggregates, false);
        totalBaseline += ns;
        result.baselineRows = rows;
      }
      if (runPartitioned) {
        auto [ns, rows] =
            runAggPlan(input, inputType, groupKeys, aggregates, true);
        totalPartitioned += ns;
        result.partitionedRows = rows;
      }
    }
    if (runBaseline) {
      result.baselineNs = totalBaseline / FLAGS_num_iterations;
    }
    if (runPartitioned) {
      result.partitionedNs = totalPartitioned / FLAGS_num_iterations;
    }

    return result;
  }

  /// Run a hash join benchmark case.
  BenchmarkResult runJoin(
      const std::string& name,
      int64_t buildRows,
      int64_t probeRows,
      int64_t buildDistinct,
      const RowTypePtr& buildType,
      const RowTypePtr& probeType,
      const std::vector<std::string>& joinKeys,
      const std::vector<std::string>& outputCols,
      JoinType joinType = JoinType::kInner) {
    auto buildInput = generateJoinInput(buildType, buildRows, buildDistinct);
    auto probeInput = generateJoinInput(probeType, probeRows, buildDistinct);

    BenchmarkResult result;
    result.name = name;

    const bool runBaseline =
        FLAGS_run_mode.empty() || FLAGS_run_mode == "baseline";
    const bool runPartitioned =
        FLAGS_run_mode.empty() || FLAGS_run_mode == "partitioned";

    // Warmup whichever modes we will run.
    if (runBaseline) {
      runJoinPlan(
          buildInput,
          probeInput,
          buildType,
          probeType,
          joinKeys,
          outputCols,
          joinType,
          false);
    }
    if (runPartitioned) {
      runJoinPlan(
          buildInput,
          probeInput,
          buildType,
          probeType,
          joinKeys,
          outputCols,
          joinType,
          true);
    }

    // Measure.
    int64_t totalBaseline = 0;
    int64_t totalPartitioned = 0;
    for (int i = 0; i < FLAGS_num_iterations; ++i) {
      if (runBaseline) {
        auto [ns, rows] = runJoinPlan(
            buildInput,
            probeInput,
            buildType,
            probeType,
            joinKeys,
            outputCols,
            joinType,
            false,
            false);
        totalBaseline += ns;
        result.baselineRows = rows;
      }
      if (runPartitioned) {
        auto [ns, rows] = runJoinPlan(
            buildInput,
            probeInput,
            buildType,
            probeType,
            joinKeys,
            outputCols,
            joinType,
            true,
            false);
        totalPartitioned += ns;
        result.partitionedRows = rows;
      }
    }
    if (runBaseline) {
      result.baselineNs = totalBaseline / FLAGS_num_iterations;
    }
    if (runPartitioned) {
      result.partitionedNs = totalPartitioned / FLAGS_num_iterations;
    }

    return result;
  }

 private:
  std::vector<RowVectorPtr> generateAggInput(
      const RowTypePtr& type,
      int64_t numRows,
      int64_t numDistinct) {
    const int64_t batchSize = std::min<int64_t>(numRows, 100000);
    const int64_t numBatches = (numRows + batchSize - 1) / batchSize;
    std::vector<RowVectorPtr> batches;
    batches.reserve(numBatches);

    for (int64_t b = 0; b < numBatches; ++b) {
      auto rowsInBatch = static_cast<vector_size_t>(
          std::min<int64_t>(batchSize, numRows - b * batchSize));
      std::vector<VectorPtr> children;
      for (int c = 0; c < type->size(); ++c) {
        if (type->childAt(c)->isVarchar()) {
          // Generate fixed-length 16-byte padded strings for VARCHAR columns.
          auto vec = BaseVector::create(VARCHAR(), rowsInBatch, pool());
          auto flat = vec->as<FlatVector<StringView>>();
          for (vector_size_t r = 0; r < rowsInBatch; ++r) {
            auto val = (b * batchSize + r) % numDistinct;
            auto str = fmt::format("{:016d}", val);
            flat->set(r, StringView(str));
          }
          children.push_back(std::move(vec));
        } else {
          children.push_back(
              makeFlatVector<int64_t>(
                  rowsInBatch, [&](vector_size_t row) -> int64_t {
                    return (b * batchSize + row) % numDistinct;
                  }));
        }
      }
      batches.push_back(makeRowVector(type->names(), children));
    }
    return batches;
  }

  std::vector<RowVectorPtr> generateJoinInput(
      const RowTypePtr& type,
      int64_t numRows,
      int64_t numDistinct) {
    return generateAggInput(type, numRows, numDistinct);
  }

  std::pair<int64_t, int64_t> runAggPlan(
      const std::vector<RowVectorPtr>& input,
      const RowTypePtr& inputType,
      const std::vector<std::string>& groupKeys,
      const std::vector<std::string>& aggregates,
      bool enablePartitioning) {
    auto plan = PlanBuilder()
                    .values(input)
                    .singleAggregation(groupKeys, aggregates)
                    .planNode();

    auto start = std::chrono::steady_clock::now();

    AssertQueryBuilder builder(plan);
    builder.serialExecution(true);
    if (!enablePartitioning) {
      builder.config(
          QueryConfig::kHashTableDirectoryPartitionThreshold,
          std::to_string(INT64_MAX));
    } else {
      builder.config(
          QueryConfig::kHashTableDirectoryPartitionThreshold,
          std::to_string(8L * 1024 * 1024));
    }

    auto rows = builder.countResults();
    auto end = std::chrono::steady_clock::now();
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                  .count();
    return {ns, static_cast<int64_t>(rows)};
  }

  std::pair<int64_t, int64_t> runJoinPlan(
      const std::vector<RowVectorPtr>& buildInput,
      const std::vector<RowVectorPtr>& probeInput,
      const RowTypePtr& buildType,
      const RowTypePtr& probeType,
      const std::vector<std::string>& joinKeys,
      const std::vector<std::string>& outputCols,
      JoinType joinType,
      bool enablePartitioning,
      bool printStats = false) {
    auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();

    auto plan = PlanBuilder(planNodeIdGenerator, pool_.get())
                    .values(probeInput)
                    .hashJoin(
                        joinKeys,
                        joinKeys,
                        PlanBuilder(planNodeIdGenerator, pool_.get())
                            .values(buildInput)
                            .planNode(),
                        /*filter=*/"",
                        outputCols,
                        joinType)
                    .planNode();

    auto start = std::chrono::steady_clock::now();

    AssertQueryBuilder builder(plan);
    builder.serialExecution(true);
    if (!enablePartitioning) {
      builder.config(
          QueryConfig::kHashTableDirectoryPartitionThreshold,
          std::to_string(INT64_MAX));
    } else {
      builder.config(
          QueryConfig::kHashTableDirectoryPartitionThreshold,
          std::to_string(8L * 1024 * 1024));
    }

    int64_t rows = 0;
    if (printStats) {
      std::shared_ptr<Task> task;
      rows = builder.copyResults(pool_.get(), task)->size();
      auto stats = task->taskStats();
      for (const auto& pipeStats : stats.pipelineStats) {
        for (const auto& opStats : pipeStats.operatorStats) {
          fmt::print(
              "  [{}] {} wall={:.1f}ms cpu={:.1f}ms rows={}\n",
              enablePartitioning ? "PART" : "BASE",
              opStats.operatorType,
              opStats.addInputTiming.wallNanos / 1e6 +
                  opStats.getOutputTiming.wallNanos / 1e6 +
                  opStats.finishTiming.wallNanos / 1e6,
              opStats.addInputTiming.cpuNanos / 1e6 +
                  opStats.getOutputTiming.cpuNanos / 1e6 +
                  opStats.finishTiming.cpuNanos / 1e6,
              opStats.outputPositions);
        }
      }
    } else {
      rows = builder.countResults();
    }

    auto end = std::chrono::steady_clock::now();
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                  .count();
    return {ns, rows};
  }
};

void printResults(const std::vector<BenchmarkResult>& results) {
  fmt::print(
      "\n{:<25} {:>12} {:>12} {:>8} {:>10}\n",
      "Case",
      "Baseline",
      "Partitioned",
      "Speedup",
      "Rows");
  fmt::print("{:-<71}\n", "");
  for (const auto& r : results) {
    auto baseMs = r.baselineNs / 1e6;
    auto partMs = r.partitionedNs / 1e6;
    auto speedup = r.speedup();
    auto indicator = speedup >= 1.5 ? " ★★★"
        : speedup >= 1.2            ? " ★★"
        : speedup >= 1.05           ? " ★"
        : speedup < 0.97            ? " ⚠"
                                    : "";
    fmt::print(
        "{:<25} {:>9.1f}ms {:>9.1f}ms {:>7.2f}x{} {:>10}\n",
        r.name,
        baseMs,
        partMs,
        speedup,
        indicator,
        r.baselineRows);
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

  // Profile mode: run only JOIN-LL for perf profiling.
  if (!FLAGS_profile_mode.empty()) {
    bool enablePart = (FLAGS_profile_mode == "partitioned");
    fmt::print(
        "PROFILE MODE: JOIN-LL {} ({}x iterations)\n",
        FLAGS_profile_mode,
        FLAGS_num_iterations);
    auto buildType =
        ROW({"k1", "k2", "b1", "b2", "b3", "b4"},
            {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT()});
    auto probeType =
        ROW({"k1", "k2", "p1", "p2"}, {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
    auto buildInput = bm.generateJoinInputPublic(
        buildType, FLAGS_profile_build_rows, FLAGS_profile_build_rows);
    auto probeInput = bm.generateJoinInputPublic(
        probeType, FLAGS_profile_probe_rows, FLAGS_profile_build_rows);

    // Warmup.
    bm.runJoinPlanPublic(
        buildInput,
        probeInput,
        buildType,
        probeType,
        {"k1", "k2"},
        {"p1", "b1", "b2"},
        core::JoinType::kInner,
        enablePart,
        true);

    // Timed iterations.
    int64_t totalNs = 0;
    for (int i = 0; i < FLAGS_num_iterations; ++i) {
      auto [ns, rows] = bm.runJoinPlanPublic(
          buildInput,
          probeInput,
          buildType,
          probeType,
          {"k1", "k2"},
          {"p1", "b1", "b2"},
          core::JoinType::kInner,
          enablePart,
          false);
      totalNs += ns;
      fmt::print("  iter {}: {:.1f}ms  rows={}\n", i, ns / 1e6, rows);
    }
    fmt::print("Average: {:.1f}ms\n", totalNs / 1e6 / FLAGS_num_iterations);
    return 0;
  }

  // ────────────────────────────────────────────────────────────
  // Aggregation Cases
  // ────────────────────────────────────────────────────────────

  auto shouldRun = [](const std::string& name) {
    return FLAGS_case_filter.empty() || FLAGS_case_filter == name;
  };

  // AGG-S1: Small agg, low cardinality (should NOT trigger partitioning).
  if (shouldRun("AGG-S1")) {
    auto type = ROW({"k1", "v1", "v2"}, {BIGINT(), BIGINT(), BIGINT()});
    results.push_back(bm.runAgg(
        "AGG-S1", 100'000, 100, type, {"k1"}, {"sum(v1)", "count(v2)"}));
  }

  // AGG-M2: Medium agg, high cardinality (triggers partitioning).
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

  // AGG-L1: Very large agg, single key (triggers heavy partitioning).
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

  // AGG-L2: Large agg, very high cardinality.
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

  // ────────────────────────────────────────────────────────────
  // TPCDS-representative Aggregation Cases
  //
  // Derived from TPCDS SF-1000 query profiles. Each case matches
  // the key/value schema, cardinality, and aggregate functions
  // from real production queries.
  // ────────────────────────────────────────────────────────────

  // ── Per-task metrics from SQL operator events in /mnt/d/flamegraph/ ──
  // input = child operator output rows / stage tasks
  // distinct = agg operator output rows / stage tasks
  // All values verified from SparkListenerSQLAdaptiveExecutionUpdate

  // TPCDS-Q97: GROUP BY (customer_sk, item_sk), partial_sum
  // 72 tasks, kNK mode, per-task: input=7,597,157 output=7,521,999 peak=248MB
  // Dir ~67MB → triggers partitioning at 4MB
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

  // TPCDS-Q65: GROUP BY (store_sk, item_sk), partial_sum
  // 72 tasks, kNK mode, per-task: input=7,418,200 output=7,058,541 peak=274MB
  // Dir ~67MB → triggers partitioning at 4MB
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

  // TPCDS-Q67: GROUP BY 9 keys (5 VARCHAR + 3 BIGINT + grouping_id), sum
  // 200 tasks, kArray→kNK (partial) / kHash (final), per-task: input=2,670,552
  // output=1,921,910 peak=507MB. Dir ~17MB → triggers at 4MB
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

  // TPCDS-Q23a: GROUP BY (d_date, ss_item_sk), count
  // 227 tasks, kNK mode, per-task: input=9,692,918 output=961,749 peak=36MB
  // Dir ~8.4MB → triggers at 4MB
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

  // TPCDS-Q4: GROUP BY (d_year, ss_customer_sk), sum
  // 71 tasks, kNK mode, per-task: input=7,564,150 output=657,072 peak=52MB
  // Dir ~8.4MB → triggers at 4MB
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

  // TPCDS-Q38: GROUP BY (d_date, ss_customer_sk), count
  // 72 tasks, kNK mode, per-task: input=7,418,105 output=658,287 peak=21MB
  // Dir ~8.4MB → triggers at 4MB
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

  // TPCDS-Q47: GROUP BY 6 keys (2 VARCHAR + 4 BIGINT-ish), partial_sum
  // 72 tasks, kArray→kNK, per-task: input=9,105,958 output=31,099 peak=19MB
  // Dir ~0.5MB but large input → good reduction-heavy case
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

  // TPCDS-Q1: GROUP BY (sr_customer_sk, sr_store_sk), sum
  // 200 tasks, kNK mode, per-task: input=271,768 output=269,620 peak=13MB
  // Dir ~4.2MB → borderline trigger at 4MB
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
  // Hash Join Cases
  // ────────────────────────────────────────────────────────────

  // JOIN-SS: Small build + small probe (no partition).
  if (shouldRun("JOIN-SS")) {
    auto buildType =
        ROW({"k1", "b1", "b2", "b3"}, {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
    auto probeType = ROW({"k1", "p1"}, {BIGINT(), BIGINT()});
    results.push_back(bm.runJoin(
        "JOIN-SS",
        10'000,
        100'000,
        10'000,
        buildType,
        probeType,
        {"k1"},
        {"p1", "b1", "b2"}));
  }

  // JOIN-LL: Large build + large probe (2 keys → kHash → triggers
  // partitioning). 20M build rows → 256MB directory (doesn't fit L3) →
  // partitioning critical.
  if (shouldRun("JOIN-LL")) {
    auto buildType =
        ROW({"k1", "k2", "b1", "b2", "b3", "b4"},
            {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT()});
    auto probeType =
        ROW({"k1", "k2", "p1", "p2"}, {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
    results.push_back(bm.runJoin(
        "JOIN-LL",
        20'000'000,
        40'000'000,
        20'000'000,
        buildType,
        probeType,
        {"k1", "k2"},
        {"p1", "b1", "b2"}));
  }

  // JOIN-LS: Large build + small probe (2 keys → kHash → triggers
  // partitioning).
  if (shouldRun("JOIN-LS")) {
    auto buildType =
        ROW({"k1", "k2", "b1", "b2", "b3", "b4"},
            {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT()});
    auto probeType = ROW({"k1", "k2", "p1"}, {BIGINT(), BIGINT(), BIGINT()});
    results.push_back(bm.runJoin(
        "JOIN-LS",
        20'000'000,
        100'000,
        20'000'000,
        buildType,
        probeType,
        {"k1", "k2"},
        {"p1", "b1"}));
  }

  // In single-mode, also emit a CSV line for script consumption.
  if (!FLAGS_run_mode.empty()) {
    for (const auto& r : results) {
      auto ns = FLAGS_run_mode == "baseline" ? r.baselineNs : r.partitionedNs;
      auto rows =
          FLAGS_run_mode == "baseline" ? r.baselineRows : r.partitionedRows;
      // CSV: name,mode,avg_ns,rows
      fmt::print("CSV,{},{},{},{}\n", r.name, FLAGS_run_mode, ns, rows);
    }
  }

  printResults(results);
  return 0;
}
