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

#include "velox/functions/sparksql/specialforms/FromJson.h"
#include <iostream>
#include "velox/expression/CastExpr.h"
#include "velox/expression/EvalCtx.h"
#include "velox/expression/PeeledEncoding.h"
#include "velox/expression/ScopedVarSetter.h"
#include "velox/expression/SpecialForm.h"
#include "velox/functions/prestosql/types/JsonType.h"

using namespace facebook::velox::exec;

namespace facebook::velox::functions::sparksql {
namespace {
class FromJsonExpr : public SpecialForm {
 public:
  /// @param type The target type of the cast expression
  /// @param expr The expression to cast
  /// @param trackCpuUsage Whether to track CPU usage
  FromJsonExpr(TypePtr type, ExprPtr&& expr, bool trackCpuUsage)
      : SpecialForm(
            type,
            std::vector<ExprPtr>({expr}),
            FromJsonCallToSpecialForm::kFromJson,
            false /* supportsFlatNoNullsFastPath */,
            trackCpuUsage) {
    castOp_ = getCustomTypeCastOperator("json");
    if (!castOp_->isSupportedToType(type)) {
      VELOX_UNSUPPORTED("Unsupported type {}.", type->toString());
    }
  }

  void evalSpecialForm(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override {
    VectorPtr input;
    inputs_[0]->eval(rows, context, input);
    auto toType = std::const_pointer_cast<const Type>(type_);
    {
      ScopedVarSetter holder{context.mutableThrowOnError(), false};
      ScopedVarSetter captureErrorDetails(
          context.mutableCaptureErrorDetails(), false);

      ScopedThreadSkipErrorDetails skipErrorDetails(true);
      apply(rows, input, context, toType, result);
    }
    // Return 'input' back to the vector pool in 'context' so it can be reused.
    context.releaseVector(input);
  }

 private:
  void computePropagatesNulls() override {
    propagatesNulls_ = false;
  }
  void apply(
      const SelectivityVector& rows,
      const VectorPtr& input,
      exec::EvalCtx& context,
      const TypePtr& toType,
      VectorPtr& result) {
    LocalSelectivityVector remainingRows(context, rows);

    context.deselectErrors(*remainingRows);

    LocalDecodedVector decoded(context, *input, *remainingRows);
    auto* rawNulls = decoded->nulls(remainingRows.get());

    if (rawNulls) {
      remainingRows->deselectNulls(
          rawNulls, remainingRows->begin(), remainingRows->end());
    }

    VectorPtr localResult;
    if (!remainingRows->hasSelections()) {
      localResult =
          BaseVector::createNullConstant(toType, rows.end(), context.pool());
    } else if (decoded->isIdentityMapping()) {
      applyPeeled(
          *remainingRows, *decoded->base(), context, toType, localResult);
    } else {
      withContextSaver([&](ContextSaver& saver) {
        LocalSelectivityVector newRowsHolder(*context.execCtx());

        LocalDecodedVector localDecoded(context);
        std::vector<VectorPtr> peeledVectors;
        auto peeledEncoding = PeeledEncoding::peel(
            {input}, *remainingRows, localDecoded, true, peeledVectors);
        VELOX_CHECK_EQ(peeledVectors.size(), 1);
        if (peeledVectors[0]->isLazy()) {
          peeledVectors[0] =
              peeledVectors[0]->as<LazyVector>()->loadedVectorShared();
        }
        auto newRows =
            peeledEncoding->translateToInnerRows(*remainingRows, newRowsHolder);
        // Save context and set the peel.
        context.saveAndReset(saver, *remainingRows);
        context.setPeeledEncoding(peeledEncoding);
        applyPeeled(*newRows, *peeledVectors[0], context, toType, localResult);

        localResult = context.getPeeledEncoding()->wrap(
            toType, context.pool(), localResult, *remainingRows);
      });
    }
    context.moveOrCopyResult(localResult, *remainingRows, result);
    context.releaseVector(localResult);

    // If there are nulls or rows that encountered errors in the input, add
    // nulls to the result at the same rows.
    VELOX_CHECK_NOT_NULL(result);
    if (rawNulls || context.errors()) {
      EvalCtx::addNulls(
          rows, remainingRows->asRange().bits(), context, toType, result);
    }
  }

  void applyPeeled(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& toType,
      VectorPtr& result) {
    EvalErrorsPtr oldErrors;
    context.swapErrors(oldErrors);

    castOp_->castFrom(input, context, rows, toType, result);

    if (context.errors()) {
      auto errors = context.errors();
      auto rawNulls = result->mutableRawNulls();

      rows.applyToSelected([&](auto row) {
        if (errors->hasErrorAt(row)) {
          bits::setNull(rawNulls, row, true);
          std::cout << "should set null in top level" << std::endl;
        }
      });
    };
    // Restore original state.
    context.swapErrors(oldErrors);
  }

  exec::CastOperatorPtr castOp_;
};

} // namespace

TypePtr FromJsonCallToSpecialForm::resolveType(
    const std::vector<TypePtr>& /*argTypes*/) {
  VELOX_FAIL("from_json function does not support type resolution.");
}

exec::ExprPtr FromJsonCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<exec::ExprPtr>&& args,
    bool trackCpuUsage,
    const core::QueryConfig& /*config*/) {
  VELOX_USER_CHECK_EQ(args.size(), 1, "from_json expects one argument.");
  VELOX_USER_CHECK_EQ(
      args[0]->type()->kind(),
      TypeKind::VARCHAR,
      "The first argument of from_json should be of varchar type.");

  return std::make_shared<FromJsonExpr>(
      type, std::move(args[0]), trackCpuUsage);
}
} // namespace facebook::velox::functions::sparksql
