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

#include "velox/functions/prestosql/Comparisons.h"
#include <velox/common/base/Exceptions.h>
#include "velox/functions/Udf.h"
#include "velox/vector/BaseVector.h"
#include <iostream>

namespace facebook::velox::functions {

namespace {

/// This class implements comparison for vectors of primitive types using SIMD.
/// Currently this only supports fixed length primitive types (except Boolean).
/// It also requires the vectors to have a flat encoding.
/// If the vector encoding is not flat, we revert to non simd approach.
template <typename ComparisonOp, typename Arch = xsimd::default_arch>
struct SimdComparator {
  template <
      TypeKind kind,
      typename std::enable_if_t<
          (xsimd::has_simd_register<
               typename TypeTraits<kind>::NativeType>::value &&
           kind != TypeKind::BOOLEAN) ||
              kind == TypeKind::HUGEINT,
          int> = 0>
  void applyComparison(
      const SelectivityVector& rows,
      BaseVector& lhs,
      BaseVector& rhs,
      exec::EvalCtx& context,
      VectorPtr& result) {
    using T = typename TypeTraits<kind>::NativeType;
    if constexpr (!std::is_same_v<T, int128_t>) {
      bool evaluated = false;
      if (lhs.isFlatEncoding() && rhs.isFlatEncoding()) {
        evaluated =
            applySimdComparison<T, false, false>(rows, lhs, rhs, result);
      } else if (lhs.isConstantEncoding() && rhs.isFlatEncoding()) {
        evaluated = applySimdComparison<T, true, false>(rows, lhs, rhs, result);
      } else if (lhs.isFlatEncoding() && rhs.isConstantEncoding()) {
        evaluated = applySimdComparison<T, false, true>(rows, lhs, rhs, result);
      }
      if (evaluated) {
        return;
      }
    }
    auto resultVector = result->asUnchecked<FlatVector<bool>>();
    auto rawResult = resultVector->mutableRawValues<uint8_t>();

    exec::LocalDecodedVector lhsDecoded(context, lhs, rows);
    exec::LocalDecodedVector rhsDecoded(context, rhs, rows);

    context.template applyToSelectedNoThrow(rows, [&](auto row) {
      auto l = lhsDecoded->template valueAt<T>(row);
      auto r = rhsDecoded->template valueAt<T>(row);
      auto filtered = ComparisonOp()(l, r);
      resultVector->set(row, filtered);
    });
    return;
  }

  template <
      TypeKind kind,
      typename std::enable_if_t<
          (!xsimd::has_simd_register<
               typename TypeTraits<kind>::NativeType>::value ||
           kind == TypeKind::BOOLEAN) &&
              kind != TypeKind::HUGEINT,
          int> = 0>
  void applyComparison(
      const SelectivityVector& /* rows */,
      BaseVector& /* lhs */,
      BaseVector& /* rhs */,
      exec::EvalCtx& /* context */,
      VectorPtr& /* result */) {
    VELOX_UNSUPPORTED("Unsupported type for SIMD comparison");
  }

 private:
  template <typename T, bool constA, bool constB>
  bool applySimdComparison(
      const SelectivityVector& rows,
      BaseVector& lhs,
      BaseVector& rhs,
      VectorPtr& result) const {
    vector_size_t begin = rows.begin();
    vector_size_t end = rows.end();
    vector_size_t size = end - begin;
    if (rows.isAllSelected()) {
      // std::cout << "use simd" << std::endl;
      tempRes_->reserve(rows.size());
      auto __restrict tempRes = tempRes_->data();
      auto resultVector = result->asUnchecked<FlatVector<bool>>();
      auto rawResult = resultVector->mutableRawValues<uint8_t>();
      if constexpr (!constA && !constB) {
        T* __restrict rawA =
            lhs.asUnchecked<FlatVector<T>>()->mutableRawValues();
        T* __restrict rawB =
            rhs.asUnchecked<FlatVector<T>>()->mutableRawValues();
        for (auto i = begin; i < end; i++) {
          tempRes[i] = ComparisonOp()(rawA[i], rawB[i]) ? -1 : 0;
        }
      } else if constexpr (constA) {
        auto constant = lhs.asUnchecked<ConstantVector<T>>()->valueAt(0);
        T* __restrict rawB =
            rhs.asUnchecked<FlatVector<T>>()->mutableRawValues();
        for (auto i = begin; i < end; i++) {
          tempRes[i] = ComparisonOp()(constant, rawB[i]) ? -1 : 0;
        }
      } else if constexpr (constB) {
        T* __restrict rawA =
            lhs.asUnchecked<FlatVector<T>>()->mutableRawValues();
        auto constant = rhs.asUnchecked<ConstantVector<T>>()->valueAt(0);
        for (auto i = begin; i < end; i++) {
          tempRes[i] = ComparisonOp()(rawA[i], constant) ? -1 : 0;
        }
      } else {
        VELOX_UNREACHABLE();
      }

      const auto vectorEnd = size - size % 32;
      for (auto i = begin; i < vectorEnd; i += 32) {
        auto res = simd::toBitMask(
            xsimd::batch_bool<int8_t>(xsimd::load_unaligned(tempRes + i)));
        uint32_t* addr = reinterpret_cast<uint32_t*>(rawResult + i / 8);
        *addr = res;
      }
      for (auto i = vectorEnd; i < end; i++) {
        bits::setBit(rawResult, i, tempRes[i]);
      }
      result->clearNulls(rows);
      return true;
    }
    return false;
  }

  std::unique_ptr<std::vector<int8_t>> tempRes_ =
      std::make_unique<std::vector<int8_t>>();
};

template <typename ComparisonOp, typename Arch = xsimd::default_arch>
class ComparisonSimdFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 2, "Comparison requires two arguments");
    VELOX_CHECK_EQ(args[0]->typeKind(), args[1]->typeKind());
    VELOX_USER_CHECK_EQ(outputType->kind(), TypeKind::BOOLEAN);

    context.ensureWritable(rows, outputType, result);
    auto comparator = SimdComparator<ComparisonOp>{};

    if (args[0]->type()->isLongDecimal()) {
      comparator.template applyComparison<TypeKind::HUGEINT>(
          rows, *args[0], *args[1], context, result);
      return;
    }

    VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        comparator.template applyComparison,
        args[0]->typeKind(),
        rows,
        *args[0],
        *args[1],
        context,
        result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;

    for (const auto& inputType : {
             "tinyint",
             "smallint",
             "integer",
             "bigint",
             "real",
             "double",
             "date",
             "interval day to second",
             "interval year to month",
         }) {
      signatures.push_back(exec::FunctionSignatureBuilder()
                               .returnType("boolean")
                               .argumentType(inputType)
                               .argumentType(inputType)
                               .build());
    }
    signatures.push_back(exec::FunctionSignatureBuilder()
                             .integerVariable("a_precision")
                             .integerVariable("a_scale")
                             .returnType("boolean")
                             .argumentType("DECIMAL(a_precision, a_scale)")
                             .argumentType("DECIMAL(a_precision, a_scale)")
                             .build());
    return signatures;
  }

  bool supportsFlatNoNullsFastPath() const override {
    return true;
  }

  exec::FunctionCanonicalName getCanonicalName() const override {
    return std::is_same_v<ComparisonOp, std::less<>>
        ? exec::FunctionCanonicalName::kLt
        : exec::FunctionCanonicalName::kUnknown;
  }
};

} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_eq,
    (ComparisonSimdFunction<std::equal_to<>>::signatures()),
    (std::make_unique<ComparisonSimdFunction<std::equal_to<>>>()));

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_neq,
    (ComparisonSimdFunction<std::not_equal_to<>>::signatures()),
    (std::make_unique<ComparisonSimdFunction<std::not_equal_to<>>>()));

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_lt,
    (ComparisonSimdFunction<std::less<>>::signatures()),
    (std::make_unique<ComparisonSimdFunction<std::less<>>>()));

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_gt,
    (ComparisonSimdFunction<std::greater<>>::signatures()),
    (std::make_unique<ComparisonSimdFunction<std::greater<>>>()));

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_lte,
    (ComparisonSimdFunction<std::less_equal<>>::signatures()),
    (std::make_unique<ComparisonSimdFunction<std::less_equal<>>>()));

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_gte,
    (ComparisonSimdFunction<std::greater_equal<>>::signatures()),
    (std::make_unique<ComparisonSimdFunction<std::greater_equal<>>>()));

} // namespace facebook::velox::functions
