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

#include "velox/functions/lib/aggregates/MinMaxAggregateBase.h"

namespace facebook::velox::functions::aggregate::sparksql {

namespace {
class TimestampMaxAggregate : public MaxAggregate<Timestamp> {
 public:
  explicit TimestampMaxAggregate(TypePtr resultType)
      : MaxAggregate<Timestamp>(resultType) {}

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    using BaseAggregate =
        SimpleNumericAggregate<Timestamp, Timestamp, Timestamp>;
    BaseAggregate::template doExtractValues<Timestamp>(
        groups, numGroups, result, [&](char* group) {
          auto ts = *BaseAggregate::Aggregate::template value<Timestamp>(group);
          return Timestamp::fromMicros(ts.toMicros());
        });
  }
};

class TimestampMinAggregate : public MinAggregate<Timestamp> {
 public:
  explicit TimestampMinAggregate(TypePtr resultType)
      : MinAggregate<Timestamp>(resultType) {}

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    using BaseAggregate =
        SimpleNumericAggregate<Timestamp, Timestamp, Timestamp>;
    BaseAggregate::template doExtractValues<Timestamp>(
        groups, numGroups, result, [&](char* group) {
          auto ts = *BaseAggregate::Aggregate::template value<Timestamp>(group);
          return Timestamp::fromMicros(ts.toMicros());
        });
  }
};

template <
    template <typename T>
    class TNumeric,
    typename TNonNumeric,
    typename TTimestamp>
exec::AggregateRegistrationResult registerMinMax(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .orderableTypeVariable("T")
                           .returnType("T")
                           .intermediateType("T")
                           .argumentType("T")
                           .build());

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          std::vector<TypePtr> argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        auto inputType = argTypes[0];
        switch (inputType->kind()) {
          case TypeKind::BOOLEAN:
            return std::make_unique<TNumeric<bool>>(resultType);
          case TypeKind::TINYINT:
            return std::make_unique<TNumeric<int8_t>>(resultType);
          case TypeKind::SMALLINT:
            return std::make_unique<TNumeric<int16_t>>(resultType);
          case TypeKind::INTEGER:
            return std::make_unique<TNumeric<int32_t>>(resultType);
          case TypeKind::BIGINT:
            return std::make_unique<TNumeric<int64_t>>(resultType);
          case TypeKind::REAL:
            return std::make_unique<TNumeric<float>>(resultType);
          case TypeKind::DOUBLE:
            return std::make_unique<TNumeric<double>>(resultType);
          case TypeKind::TIMESTAMP:
            return std::make_unique<TTimestamp>(resultType);
          case TypeKind::HUGEINT:
            return std::make_unique<TNumeric<int128_t>>(resultType);
          case TypeKind::VARBINARY:
            [[fallthrough]];
          case TypeKind::VARCHAR:
            return std::make_unique<TNonNumeric>(inputType, false);
          case TypeKind::ARRAY:
            [[fallthrough]];
          case TypeKind::ROW:
            return std::make_unique<TNonNumeric>(inputType, false);
          default:
            VELOX_CHECK(
                false,
                "Unknown input type for {} aggregation {}",
                name,
                inputType->kindName());
        }
      },
      {false /*orderSensitive*/},
      withCompanionFunctions,
      overwrite);
}

} // namespace

void registerMinMaxAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerMinMax<
      MinAggregate,
      NonNumericMinAggregate<false>,
      TimestampMinAggregate>(prefix + "min", withCompanionFunctions, overwrite);
  registerMinMax<
      MaxAggregate,
      NonNumericMaxAggregate<false>,
      TimestampMaxAggregate>(prefix + "max", withCompanionFunctions, overwrite);
}
} // namespace facebook::velox::functions::aggregate::sparksql
