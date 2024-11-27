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
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace facebook::velox::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

class FromJsonTest : public SparkFunctionBaseTest {
 protected:
  core::CallTypedExprPtr createFromJson(const TypePtr& outputType) {
    std::vector<core::TypedExprPtr> inputs = {
        std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c0")};

    return std::make_shared<const core::CallTypedExpr>(
        outputType, std::move(inputs), "from_json");
  }

  void testFromJson(const VectorPtr& input, const VectorPtr& expected) {
    auto expr = createFromJson(expected->type());
    testEncodings(expr, {input}, expected);
  }
};

TEST_F(FromJsonTest, basic) {
  auto expected = makeFlatVector<int64_t>({1, 2, 3});
  auto input = makeFlatVector<std::string>(
      {R"({"a": 1})", R"({"a": 2})", R"({"a": 3})"});
  testFromJson(input, makeRowVector({"a"}, {expected}));
}

TEST_F(FromJsonTest, decimal) {
  auto expected = makeFlatVector<int64_t>({1, 2, 3}, DECIMAL(3, 2));
  auto input = makeFlatVector<std::string>(
      {R"({"a": 1.00})", R"({"a": 2.00})", R"({"a": 3.00})"});
  testFromJson(input, makeRowVector({"a"}, {expected}));
}

TEST_F(FromJsonTest, nullOnFailure) {
  auto expected =
      makeNullableFlatVector<int64_t>({std::nullopt, std::nullopt, 3});
  auto input =
      makeFlatVector<std::string>({R"({"a": 1)", R"({"a" 2})", R"({"a": 3})"});
  testFromJson(input, makeRowVector({"a"}, {expected}));
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
