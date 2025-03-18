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
#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <string>

#include "velox/exec/Cursor.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/functions/sparksql/aggregates/Register.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");

using namespace facebook::velox;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::exec::test;

static constexpr int32_t kNumVectors = 1'000;
static constexpr int32_t kRowsPerVector = 10'000;

namespace {

class DecimalSumAggregateBenchmark : public HiveConnectorTestBase {
 public:
 DecimalSumAggregateBenchmark() {
    HiveConnectorTestBase::SetUp();
    facebook::velox::functions::aggregate::sparksql::registerAggregateFunctions("");

    inputType_ = ROW({
        {"k_int_s", INTEGER()},
        {"k_int_m", INTEGER()},
        {"k_int_l", INTEGER()},
        {"v_decimal_s", INTEGER()},
        {"v_decimal_m", BIGINT()},
        {"v_decimal_l", BIGINT()},
    });

    VectorFuzzer::Options opts;
    opts.vectorSize = kRowsPerVector;
    opts.nullRatio = 0;
    VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);

    std::vector<RowVectorPtr> vectors;
    for (auto i = 0; i < kNumVectors; ++i) {
      std::vector<VectorPtr> children;

      // Generate key with a small number of unique values from a small range
      // (0-16).
      children.emplace_back(makeFlatVector<int32_t>(
          kRowsPerVector, [](auto row) { return row % 17; }));

      // Generate key with a small number of unique values from a large range
      // (3000 total values).
      children.emplace_back(
          makeFlatVector<int32_t>(kRowsPerVector, [](auto row) {
            if (row % 3 == 0) {
              return std::numeric_limits<int32_t>::max() - row % 1000;
            } else if (row % 3 == 1) {
              return row % 1000;
            } else {
              return std::numeric_limits<int32_t>::min() + row % 1000;
            }
          }));

      // Generate key with many unique values from a large range (500K total
      // values).
      children.emplace_back(fuzzer.fuzzFlat(INTEGER()));

      opts.nullRatio = 0.01; // 1%
      fuzzer.setOptions(opts);

      children.emplace_back(fuzzer.fuzzFlat(INTEGER()));
      children.emplace_back(fuzzer.fuzzFlat(BIGINT()));
      children.emplace_back(fuzzer.fuzzFlat(BIGINT()));

      vectors.emplace_back(makeRowVector(inputType_->names(), children));
    }

    filePath_ = TempFilePath::create();
    writeToFile(filePath_->getPath(), vectors);
  }

  ~DecimalSumAggregateBenchmark() override {
    HiveConnectorTestBase::TearDown();
  }

  void TestBody() override {}

  template <typename T, typename U>
  VectorPtr copy(const VectorPtr& source, const TypePtr& targetType) {
    auto flatSource = source->asFlatVector<T>();
    auto flatTarget = BaseVector::create<FlatVector<U>>(
        targetType, flatSource->size(), pool_.get());
    for (auto i = 0; i < flatSource->size(); ++i) {
      if (flatSource->isNullAt(i)) {
        flatTarget->setNull(i, true);
      } else {
        flatTarget->set(i, flatSource->valueAt(i));
      }
    }
    return flatTarget;
  }

  void run(const std::string& key, const std::string& value, const std::string& size) {
    folly::BenchmarkSuspender suspender;
    auto cast = fmt::format("cast(({} % 10000000) as decimal(12, 5)) as c", value);
    if (size == "19") {
        cast = fmt::format("cast(({} % 1000000000) as decimal(19, 5)) as c", value);
    } else if (size == 38) {
        cast = fmt::format("cast({} as decimal(38, 19)) as c", value);
    }
    auto plan = PlanBuilder()
                    .tableScan(inputType_)
                    .project({key, cast})
                    .partialAggregation({key}, {"sum(c)"})
                    .finalAggregation()
                    .planFragment();

    vector_size_t numResultRows = 0;
    auto task = makeTask(plan);

    task->addSplit(
        "0", exec::Split(makeHiveConnectorSplit(filePath_->getPath())));
    task->noMoreSplits("0");

    suspender.dismiss();

    while (auto result = task->next()) {
      numResultRows += result->size();
    }

    folly::doNotOptimizeAway(numResultRows);
  }

  std::shared_ptr<exec::Task> makeTask(core::PlanFragment plan) {
    return exec::Task::create(
        "t",
        std::move(plan),
        0,
        core::QueryCtx::create(executor_.get()),
        exec::Task::ExecutionMode::kSerial);
  }

 private:
  RowTypePtr inputType_;
  std::shared_ptr<TempFilePath> filePath_;
};

std::unique_ptr<DecimalSumAggregateBenchmark> benchmark;

void doRun(uint32_t, const std::string& key, const std::string& value, const std::string& size) {
  benchmark->run(key, value, size);
}

BENCHMARK_NAMED_PARAM(doRun, ss, "k_int_s", "v_decimal_s", "12");
BENCHMARK_NAMED_PARAM(doRun, ms, "k_int_m", "v_decimal_s", "12");
BENCHMARK_NAMED_PARAM(doRun, ls, "k_int_l", "v_decimal_s", "12");
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(doRun, sm, "k_int_s", "v_decimal_m", "19");
BENCHMARK_NAMED_PARAM(doRun, mm, "k_int_m", "v_decimal_m", "19");
BENCHMARK_NAMED_PARAM(doRun, lm, "k_int_l", "v_decimal_m", "19");
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(doRun, sl, "k_int_s", "v_decimal_l", "38");
BENCHMARK_NAMED_PARAM(doRun, ml, "k_int_m", "v_decimal_l", "38");
BENCHMARK_NAMED_PARAM(doRun, ll, "k_int_l", "v_decimal_l", "38");
BENCHMARK_DRAW_LINE();

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  OperatorTestBase::SetUpTestCase();
  benchmark = std::make_unique<DecimalSumAggregateBenchmark>();
  folly::runBenchmarks();
  benchmark.reset();
  OperatorTestBase::TearDownTestCase();
  return 0;
}
