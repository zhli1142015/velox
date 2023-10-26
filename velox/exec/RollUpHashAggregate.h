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
#pragma once

#include "velox/core/Expressions.h"
#include "velox/exec/GroupingSet.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

/**
 * An Operator which act as partial Aggregate operator and does aggregate in
 * rollup manner without a child Expand operator. The processing happen in
 * multiple stages. Each stage produce output of one grouping set. In each stage
 * we maintain a primary and seoncdary grouping set. Primary grouping set is
 * used to produce output for current set and the secondary one will act as
 * buffer for next set. For rollup (a, b, c), the child operator should have an
 * extra grouping id (0) constant column. This operator will then do grouping on
 * (a, b, c, id) columns. For rollup (a, b, c) These grouping set will be used:
 * (a, b, c, 0)
 * (a, b, null, 1)
 * (a, null, null, 3)
 * (null, null, null, 7)
 *
 * Note::
 * This implementation is similar to HashAggregate.h/cpp which specifics changes
 * required for rollup. Have remove few code-section/feature like abondoning
 * aggregate, partial output which are not needed in this. This operator can
 * spill both primary and secondary table even though partial aggregate cannot.
 */
class RollUpHashAggregate : public Operator {
 public:
  RollUpHashAggregate(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::AggregationNode>& rollUpAggregateNode);

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool needsInput() const override {
    return !noMoreInput_;
  }

  void noMoreInput() override;

  BlockingReason isBlocked(ContinueFuture* /* unused */) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

  void reclaim(uint64_t targetBytes, memory::MemoryReclaimer::Stats& stats)
      override;

  void close() override;

 private:
  AggregateInfo createAggregateInfoForRawInput(
      int idx,
      RowTypePtr inputType,
      const core::AggregationNode::Aggregate& aggregate,
      size_t numHashers);
  AggregateInfo createAggregateInfoForIntermediateInput(
      int idx,
      RowTypePtr inputType,
      const core::AggregationNode::Aggregate& aggregate,
      size_t numHashers);

  void prepareOutput(vector_size_t size);

  void recordHashMode();

  // Creates the secondary hash table with modified Aggregate functions and
  // spill config
  std::unique_ptr<GroupingSet> createGroupingSet(
      bool secondary,
      RowTypePtr inputType);

  void createPrimaryGroupingSet();

  void createSecondaryGroupingSet();

  void updateGroupingSetStats(const GroupingSet& gs);

  void switchGroupingSet();

  // Calls when output is generated from this operator.
  // Before sending output it will add the output data to secondary table.
  void insertIntoSecondaryTable();

  const bool isPartialOutput_;
  const bool isIntermediate_;

  std::unique_ptr<GroupingSet> groupingSet_;
  std::unique_ptr<GroupingSet> secondaryGroupingSet_;

  bool finished_ = false;
  int rollup_ = 0;
  int groupingKeySize_;

  // The pointer to corresponding plan node. Needed for creating secondary
  // tables repeatedly
  const std::shared_ptr<const core::AggregationNode> rollUpAggregationNode_;

  RowContainerIterator resultIterator_;
  bool pushdownChecked_ = false;
  bool mayPushdown_ = false;

  // Contains count for each type of hashMode used across all grouping set
  // created Index 0 -> BaseHashTable::HashMode::kHash Index 1 ->
  // BaseHashTable::HashMode::kArray Index 2 ->
  // BaseHashTable::HashMode::kNormalizedKey
  std::array<int, 3> hashModeStats_ = {0, 0, 0};

  // Count the number of input rows. It is reset on partial aggregation output
  // flush.
  int64_t numInputRows_ = 0;
  /// Count the number of input vectors. It is reset on partial aggregation
  /// output flush.
  int64_t numInputVectors_ = 0;
  // Count the number of output rows. It is reset on partial aggregation output
  // flush.
  int64_t numOutputRows_ = 0;

  // Possibly reusable output vector.
  RowVectorPtr output_;
};

} // namespace facebook::velox::exec
