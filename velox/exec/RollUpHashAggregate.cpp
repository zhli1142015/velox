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
#include "velox/exec/RollUpHashAggregate.h"
#include <optional>
#include "velox/exec/Aggregate.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"
#include "velox/expression/Expr.h"

namespace facebook::velox::exec {

RollUpHashAggregate::RollUpHashAggregate(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::AggregationNode>& rollUpAggregationNode)
    : Operator(
          driverCtx,
          rollUpAggregationNode->outputType(),
          operatorId,
          rollUpAggregationNode->id(),
          rollUpAggregationNode->step() == core::AggregationNode::Step::kPartial
              ? "PartialAggregation"
              : "Aggregation",
          rollUpAggregationNode->canSpill(driverCtx->queryConfig())
              ? driverCtx->makeSpillConfig(operatorId)
              : std::nullopt),
      isPartialOutput_(isPartialOutput(rollUpAggregationNode->step())),
      isIntermediate_(
          rollUpAggregationNode->step() ==
          core::AggregationNode::Step::kIntermediate),
      groupingKeySize_(rollUpAggregationNode->groupingKeys().size()),
      rollUpAggregationNode_(rollUpAggregationNode) {
  // This initial logic is same as HashAggregation.cpp operator
  VELOX_CHECK(pool()->trackUsage());

  createPrimaryGroupingSet();

  createSecondaryGroupingSet();
}

AggregateInfo RollUpHashAggregate::createAggregateInfoForRawInput(
    int aggIdx,
    RowTypePtr inputType,
    const core::AggregationNode::Aggregate& aggregate,
    size_t numHashers) {
  AggregateInfo info;

  auto& channels = info.inputs;
  auto& constants = info.constantInputs;
  std::vector<TypePtr> argTypes;
  for (auto& arg : aggregate.call->inputs()) {
    argTypes.push_back(arg->type());
    channels.push_back(exprToChannel(arg.get(), inputType));
    if (channels.back() == kConstantChannel) {
      auto constant = dynamic_cast<const core::ConstantTypedExpr*>(arg.get());
      constants.push_back(constant->toConstantVector(pool()));
    } else {
      constants.push_back(nullptr);
    }
  }
  if (isRawInput(rollUpAggregationNode_->step())) {
    if ((aggregate.call->name() == "avg_partial" ||
         aggregate.call->name() == "sum_partial") &&
        argTypes.back()->isDecimal()) {
      // Spiller looks into intermediate type when it spill the aggregate
      // accumulator data. DecimalAvgAggregate and DecimalSumAggregate agg.
      // function expect output sum of LargeDecimal type (128 bits). So we
      // need to updated intermediate type as now spill can also happen in
      // partial aggregate which was not happening till now. outputType_
      // contains the correct LargeDecimal which we can use - this is set in
      // Gluten.
      info.intermediateType = outputType_->childAt(numHashers + aggIdx);
    } else {
      info.intermediateType =
          Aggregate::intermediateType(aggregate.call->name(), argTypes);
    }
  } else {
    VELOX_CHECK_EQ(
        argTypes.size(),
        1,
        "Intermediate aggregates must have a single argument");
    info.intermediateType = argTypes[0];
  }
  // Setup aggregation mask: convert the Variable Reference name to the
  // channel (projection) index, if there is a mask.
  if (const auto& mask = aggregate.mask) {
    if (mask != nullptr) {
      info.mask = inputType->asRow().getChildIdx(mask->name());
    }
  }

  const auto& resultType = outputType_->childAt(numHashers + aggIdx);
  info.function = Aggregate::create(
      aggregate.call->name(),
      rollUpAggregationNode_->step(),
      argTypes,
      resultType,
      operatorCtx_->driverCtx()->queryConfig());
  info.output = numHashers + aggIdx;

  return info;
}

AggregateInfo RollUpHashAggregate::createAggregateInfoForIntermediateInput(
    int aggIdx,
    RowTypePtr inputType,
    const core::AggregationNode::Aggregate& aggregate,
    size_t numHashers) {
  AggregateInfo info;

  auto& channels = info.inputs;
  auto& constants = info.constantInputs;
  // No need to update constants for secondary hash table. This will be empty as
  // we cannot know if intermediate data will be constant

  std::vector<TypePtr> argTypes;

  channels.push_back(numHashers + aggIdx);
  const auto& resultType = outputType_->childAt(numHashers + aggIdx);
  argTypes.push_back(resultType);

  info.intermediateType = resultType;

  // We don't neet to set any mask here as output from primary key is already
  // processed through mask. Setting nullopt is needed else will cause crash
  info.mask = std::nullopt;

  auto secondaryAggregationStep = rollUpAggregationNode_->step();
  if (rollUpAggregationNode_->step() == core::AggregationNode::Step::kPartial) {
    secondaryAggregationStep = core::AggregationNode::Step::kIntermediate;
  } else if (
      rollUpAggregationNode_->step() == core::AggregationNode::Step::kSingle) {
    secondaryAggregationStep = core::AggregationNode::Step::kFinal;
  }

  info.function = Aggregate::create(
      aggregate.call->name(),
      secondaryAggregationStep,
      argTypes,
      resultType,
      operatorCtx_->driverCtx()->queryConfig());
  info.output = numHashers + aggIdx;

  return info;
}

void RollUpHashAggregate::createPrimaryGroupingSet() {
  auto inputType = rollUpAggregationNode_->sources()[0]->outputType();
  groupingSet_ = createGroupingSet(false, inputType);
}

void RollUpHashAggregate::createSecondaryGroupingSet() {
  secondaryGroupingSet_ = createGroupingSet(true, outputType_);
}

std::unique_ptr<GroupingSet> RollUpHashAggregate::createGroupingSet(
    bool secondary,
    RowTypePtr inputType) {
  auto hashers =
      createVectorHashers(inputType, rollUpAggregationNode_->groupingKeys());

  auto numHashers = hashers.size();

  // Assuming the output of a grouping set is not clustered preGroupedChannels
  // will be empty for secondary table
  std::vector<column_index_t> preGroupedChannels = {};
  if (!secondary) {
    preGroupedChannels.reserve(rollUpAggregationNode_->preGroupedKeys().size());
    for (const auto& key : rollUpAggregationNode_->preGroupedKeys()) {
      auto channel = exprToChannel(key.get(), inputType);
      preGroupedChannels.push_back(channel);
    }
  }

  auto numAggregates = rollUpAggregationNode_->aggregates().size();
  std::vector<AggregateInfo> aggregateInfos;
  aggregateInfos.reserve(numAggregates);

  for (auto i = 0; i < numAggregates; i++) {
    const auto& aggregate = rollUpAggregationNode_->aggregates()[i];
    AggregateInfo info = (secondary)
        ? createAggregateInfoForIntermediateInput(
              i, inputType, aggregate, numHashers)
        : createAggregateInfoForRawInput(i, inputType, aggregate, numHashers);

    aggregateInfos.emplace_back(std::move(info));
  }

  // Check that aggregate result type match the output type
  for (auto i = 0; i < aggregateInfos.size(); i++) {
    const auto& aggResultType = aggregateInfos[i].function->resultType();
    const auto& expectedType = outputType_->childAt(numHashers + i);
    VELOX_CHECK(
        aggResultType->kindEquals(expectedType),
        "Unexpected result type for an aggregation: {}, expected {}, step {}",
        aggResultType->toString(),
        expectedType->toString(),
        core::AggregationNode::stepName(rollUpAggregationNode_->step()));
  }

  std::optional<column_index_t> groupIdChannel;
  if (rollUpAggregationNode_->groupId().has_value()) {
    groupIdChannel = outputType_->getChildIdxIfExists(
        rollUpAggregationNode_->groupId().value()->name());
    VELOX_CHECK(groupIdChannel.has_value());
  }

  return std::make_unique<GroupingSet>(
      inputType,
      std::move(hashers),
      std::move(preGroupedChannels),
      std::move(aggregateInfos),
      rollUpAggregationNode_->ignoreNullKeys(),
      isPartialOutput_,
      true, // isRollUp
      !secondary, // isRawInput
      rollUpAggregationNode_->globalGroupingSets(),
      groupIdChannel,
      spillConfig_.has_value() ? &spillConfig_.value() : nullptr,
      &nonReclaimableSection_,
      operatorCtx_.get(),
      &spillStats_);
}

void RollUpHashAggregate::updateGroupingSetStats(const GroupingSet& gs) {
  const auto mode = gs.hashMode();
  hashModeStats_[static_cast<int>(mode)]++;
  numRehashes += gs.hashTableStats().numRehashes;
  rehashTime += gs.rehashTime();
}

void RollUpHashAggregate::switchGroupingSet() {
  secondaryGroupingSet_->noMoreInput();
  // Accumulate stats for each grouping sets
  updateGroupingSetStats(*secondaryGroupingSet_);
  groupingSet_->resetTable();
  std::swap(groupingSet_, secondaryGroupingSet_);
  createSecondaryGroupingSet();
  rollup_++;
}

void RollUpHashAggregate::addInput(RowVectorPtr input) {
  if (!pushdownChecked_) {
    mayPushdown_ = operatorCtx_->driver()->mayPushdownAggregation(this);
    pushdownChecked_ = true;
  }

  groupingSet_->addInput(input, mayPushdown_);

  numInputRows_ += input->size();
  numInputVectors_ += 1;
}

void RollUpHashAggregate::recordHashMode() {
  auto lockedStats = stats_.wlock();
  lockedStats->runtimeStats["hashtable.numHashMode"] =
      RuntimeMetric(hashModeStats_[0]);
  lockedStats->runtimeStats["hashtable.numArrayMode"] =
      RuntimeMetric(hashModeStats_[1]);
  lockedStats->runtimeStats["hashtable.numNormalizedKeyMode"] =
      RuntimeMetric(hashModeStats_[2]);
}

void RollUpHashAggregate::recordRehashStats() {
  auto lockedStats = stats_.wlock();
  lockedStats->runtimeStats["hashtable.numRehashes"] =
      RuntimeMetric(numRehashes);
  lockedStats->runtimeStats["hashtable.rehashTime"] =
      RuntimeMetric(rehashTime, RuntimeCounter::Unit::kNanos);
}

void RollUpHashAggregate::prepareOutput(vector_size_t size) {
  if (output_) {
    VectorPtr output = std::move(output_);
    BaseVector::prepareForReuse(output, size);
    output_ = std::static_pointer_cast<RowVector>(output);
  } else {
    output_ = std::static_pointer_cast<RowVector>(
        BaseVector::create(outputType_, size, pool()));
  }
}

RowVectorPtr RollUpHashAggregate::getOutput() {
  if (finished_) {
    input_ = nullptr;
    return nullptr;
  }

  // Produce results if one of the following is true:
  // - received no-more-input message;
  // - running in partial streaming mode and have some output ready.
  if (!noMoreInput_ && !groupingSet_->hasOutput()) {
    input_ = nullptr;
    return nullptr;
  }

  const auto batchSize = outputBatchRows(groupingSet_->estimateOutputRowSize());

  // Reuse output vectors if possible.
  prepareOutput(batchSize);
  const auto& queryConfig = operatorCtx_->driverCtx()->queryConfig();
  bool hasData = groupingSet_->getOutput(
      batchSize,
      queryConfig.preferredOutputBatchBytes(),
      resultIterator_,
      output_);
  if (!hasData) {
    resultIterator_.reset();
    if (noMoreInput_ && rollup_ == groupingKeySize_ - 1) {
      finished_ = true;
      recordHashMode();
      recordRehashStats();
    } else if (noMoreInput_) {
      switchGroupingSet();

      // Call getOutput again so that the current call does not return empty
      // output
      return getOutput();
    }
    return nullptr;
  }
  numOutputRows_ += output_->size();

  insertIntoSecondaryTable();

  return output_;
}

void RollUpHashAggregate::insertIntoSecondaryTable() {
  if (rollup_ == groupingKeySize_ - 1)
    return;

  int nextRollup = rollup_ + 1;

  // We need to create intermediate data which will have the new rows with
  // required null columns and updated grouping id Columns will be in this order
  // | non-null grouping columns | null grouping columns | grouping id |
  // aggregates |
  RowVectorPtr intermediate_ = std::static_pointer_cast<RowVector>(
      BaseVector::create(outputType_, output_->size(), pool()));

  for (int i = 0; i < outputType_->size(); ++i) {
    if (i < groupingKeySize_ - 1 - nextRollup) {
      intermediate_->childAt(i) = output_->childAt(i);
    } else if (i < groupingKeySize_ - 1) {
      intermediate_->childAt(i) = BaseVector::createNullConstant(
          outputType_->childAt(i), output_->size(), pool());
    } else if (i == groupingKeySize_ - 1) {
      intermediate_->childAt(i) = BaseVector::createConstant(
          outputType_->childAt(i),
          (1LL << nextRollup) - 1,
          output_->size(),
          pool());
    } else {
      intermediate_->childAt(i) = output_->childAt(i);
    }
  }

  secondaryGroupingSet_->addInput(intermediate_, mayPushdown_);
}

void RollUpHashAggregate::noMoreInput() {
  groupingSet_->noMoreInput();
  updateGroupingSetStats(*groupingSet_);
  Operator::noMoreInput();
}

bool RollUpHashAggregate::isFinished() {
  return finished_;
}

void RollUpHashAggregate::reclaim(
    uint64_t targetBytes,
    memory::MemoryReclaimer::Stats& stats) {
  VELOX_CHECK(canReclaim());
  auto* driver = operatorCtx_->driver();

  /// NOTE: an aggregation operator is reclaimable if it hasn't started output
  /// processing and is not under non-reclaimable execution section.
  if (noMoreInput_ || nonReclaimableSection_) {
    // TODO: add stats to record the non-reclaimable case and reduce the log
    // frequency if it is too verbose.
    LOG(WARNING) << "Can't reclaim from aggregation operator, noMoreInput_["
                 << noMoreInput_ << "], nonReclaimableSection_["
                 << nonReclaimableSection_ << "], " << toString();
    return;
  }

  // TODO: support fine-grain disk spilling based on 'targetBytes' after having
  // row container memory compaction support later.
  groupingSet_->spill();
  VELOX_CHECK_EQ(groupingSet_->numRows(), 0);
  VELOX_CHECK_EQ(groupingSet_->numDistinct(), 0);
  // Release the minimum reserved memory.
  pool()->release();
}

void RollUpHashAggregate::close() {
  Operator::close();

  output_ = nullptr;
  groupingSet_.reset();
  secondaryGroupingSet_.reset();
}
} // namespace facebook::velox::exec
