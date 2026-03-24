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
#include <gtest/gtest.h>
#include "velox/vector/tests/VectorTestUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook;
using namespace facebook::velox;

class VectorPrepareForReuseTest : public testing::Test,
                                  public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  VectorPrepareForReuseTest() = default;
};

class MemoryAllocationChecker {
 public:
  explicit MemoryAllocationChecker(memory::MemoryPool* pool)
      : pool_{pool}, numAllocations_{pool_->stats().numAllocs} {}

  bool assertOne() {
    bool ok = numAllocations_ + 1 == pool_->stats().numAllocs;
    numAllocations_ = pool_->stats().numAllocs;
    return ok;
  }

  bool assertAtLeastOne() {
    bool ok = numAllocations_ < pool_->stats().numAllocs;
    numAllocations_ = pool_->stats().numAllocs;
    return ok;
  }

  ~MemoryAllocationChecker() {
    EXPECT_EQ(numAllocations_, pool_->stats().numAllocs);
  }

 private:
  memory::MemoryPool* const pool_;
  uint64_t numAllocations_;
};

TEST_F(VectorPrepareForReuseTest, strings) {
  std::vector<std::string> largeStrings = {
      std::string(20, '.'),
      std::string(30, '-'),
      std::string(40, '='),
  };

  auto stringAt = [&](auto row) {
    return row % 3 == 0 ? StringView(largeStrings[(row / 3) % 3]) : ""_sv;
  };

  VectorPtr vector = makeFlatVector<StringView>(1'000, stringAt);
  auto originalBytes = vector->retainedSize();
  BaseVector* originalVector = vector.get();

  // Verify that string buffers get reused rather than appended to.
  {
    MemoryAllocationChecker allocationChecker(pool());

    BaseVector::prepareForReuse(vector, vector->size());
    ASSERT_EQ(originalVector, vector.get());
    ASSERT_EQ(originalBytes, vector->retainedSize());

    // Verify that StringViews are reset to empty strings.
    for (auto i = 0; i < vector->size(); i++) {
      ASSERT_EQ("", vector->asFlatVector<StringView>()->valueAt(i).str());
    }

    for (auto i = 0; i < vector->size(); i++) {
      vector->asFlatVector<StringView>()->set(i, stringAt(i));
    }
    ASSERT_EQ(originalBytes, vector->retainedSize());
  }

  // Verify that string buffers get dropped if not singly referenced.
  auto getStringBuffers = [](const VectorPtr& vector) {
    return vector->asFlatVector<StringView>()->stringBuffers();
  };

  {
    MemoryAllocationChecker allocationChecker(pool());

    auto stringBuffers = getStringBuffers(vector);
    ASSERT_FALSE(stringBuffers.empty());

    BaseVector::prepareForReuse(vector, vector->size());
    ASSERT_EQ(originalVector, vector.get());
    ASSERT_GT(originalBytes, vector->retainedSize());
    ASSERT_TRUE(getStringBuffers(vector).empty());

    for (auto i = 0; i < vector->size(); i++) {
      vector->asFlatVector<StringView>()->set(i, stringAt(i));
    }
    ASSERT_EQ(originalBytes, vector->retainedSize());

    ASSERT_TRUE(allocationChecker.assertAtLeastOne());
  }

  // Verify that only one string buffer is kept for re-use.
  {
    std::vector<std::string> extraLargeStrings = {
        std::string(200, '.'),
        std::string(300, '-'),
        std::string(400, '='),
    };

    VectorPtr extraLargeVector = makeFlatVector<StringView>(
        1'000,
        [&](auto row) { return StringView(extraLargeStrings[row % 3]); });
    ASSERT_LT(1, getStringBuffers(extraLargeVector).size());

    auto originalExtraLargeBytes = extraLargeVector->retainedSize();
    BaseVector* originalExtraLargeVector = extraLargeVector.get();

    MemoryAllocationChecker allocationChecker(pool());

    BaseVector::prepareForReuse(extraLargeVector, extraLargeVector->size());
    ASSERT_EQ(originalExtraLargeVector, extraLargeVector.get());
    ASSERT_GT(originalExtraLargeBytes, extraLargeVector->retainedSize());
    ASSERT_EQ(1, getStringBuffers(extraLargeVector).size());

    for (auto i = 0; i < extraLargeVector->size(); i++) {
      extraLargeVector->asFlatVector<StringView>()->set(i, stringAt(i));
    }
    ASSERT_EQ(originalBytes, extraLargeVector->retainedSize());
  }
}

TEST_F(VectorPrepareForReuseTest, nulls) {
  VectorPtr vector = makeFlatVector<int32_t>(
      1'000, [](auto row) { return row; }, nullEvery(7));
  auto originalBytes = vector->retainedSize();
  BaseVector* originalVector = vector.get();

  // Verify that nulls buffer is reused.
  {
    MemoryAllocationChecker allocationChecker(pool());

    ASSERT_TRUE(vector->nulls() != nullptr);

    BaseVector::prepareForReuse(vector, vector->size());
    ASSERT_EQ(originalVector, vector.get());
    ASSERT_EQ(originalBytes, vector->retainedSize());
  }

  // Verify that nulls buffer is freed if there are no nulls.
  {
    MemoryAllocationChecker allocationChecker(pool());

    for (auto i = 0; i < vector->size(); i++) {
      vector->setNull(i, false);
    }
    ASSERT_TRUE(vector->nulls() != nullptr);
    ASSERT_EQ(originalBytes, vector->retainedSize());

    BaseVector::prepareForReuse(vector, vector->size());
    ASSERT_EQ(originalVector, vector.get());
    ASSERT_TRUE(vector->nulls() == nullptr);
    ASSERT_GT(originalBytes, vector->retainedSize());

    vector->setNull(12, true);
    ASSERT_EQ(originalBytes, vector->retainedSize());

    ASSERT_TRUE(allocationChecker.assertOne());
  }

  // Verify that nulls buffer is dropped if not singly-referenced.
  {
    MemoryAllocationChecker allocationChecker(pool());

    ASSERT_TRUE(vector->nulls() != nullptr);
    ASSERT_EQ(originalBytes, vector->retainedSize());

    auto nulls = vector->nulls();
    BaseVector::prepareForReuse(vector, vector->size());
    ASSERT_EQ(originalVector, vector.get());
    ASSERT_TRUE(vector->nulls() == nullptr);
    ASSERT_GT(originalBytes, vector->retainedSize());

    vector->setNull(12, true);
    ASSERT_EQ(originalBytes, vector->retainedSize());

    ASSERT_TRUE(allocationChecker.assertOne());
  }
}

TEST_F(VectorPrepareForReuseTest, arrays) {
  VectorPtr vector = makeArrayVector<int32_t>(
      1'000,
      [](auto row) { return 1; },
      [](auto row, auto index) { return row + index; });
  auto originalSize = vector->retainedSize();
  BaseVector* originalVector = vector.get();

  auto otherVector = makeArrayVector<int32_t>(
      1'000,
      [](auto row) { return 1; },
      [](auto row, auto index) { return 2 * row + index; });

  MemoryAllocationChecker allocationChecker(pool());
  BaseVector::prepareForReuse(vector, vector->size());
  ASSERT_EQ(originalVector, vector.get());
  ASSERT_EQ(originalSize, vector->retainedSize());

  for (auto i = 0; i < 1'000; i++) {
    ASSERT_EQ(0, vector->as<ArrayVector>()->sizeAt(i));
    ASSERT_EQ(0, vector->as<ArrayVector>()->offsetAt(i));
  }

  vector->copy(otherVector.get(), 0, 0, 1'000);
  ASSERT_EQ(originalSize, vector->retainedSize());
}

TEST_F(VectorPrepareForReuseTest, arrayOfStrings) {
  VectorPtr vector = makeArrayVector<std::string>(
      1'000,
      [](auto /*row*/) { return 1; },
      [](auto row, auto index) {
        return std::string(20 + index, 'a' + row % 5);
      });
  auto originalSize = vector->retainedSize();
  BaseVector* originalVector = vector.get();

  MemoryAllocationChecker allocationChecker(pool());
  BaseVector::prepareForReuse(vector, vector->size());
  ASSERT_EQ(originalVector, vector.get());
  ASSERT_EQ(originalSize, vector->retainedSize());

  auto* arrayVector = vector->as<ArrayVector>();
  for (auto i = 0; i < 1'000; i++) {
    ASSERT_EQ(0, arrayVector->sizeAt(i));
    ASSERT_EQ(0, arrayVector->offsetAt(i));
  }

  // Cannot use BaseVector::copy because it is too smart and acquired string
  // buffers instead of copying the strings.
  auto* elementsVector = arrayVector->elements()->as<FlatVector<StringView>>();
  elementsVector->resize(1'000);
  for (auto i = 0; i < 1'000; i++) {
    arrayVector->setOffsetAndSize(i, i, 1);
    std::string newValue(21, 'b' + i % 7);
    elementsVector->set(i, StringView(newValue));
  }

  ASSERT_EQ(originalSize, vector->retainedSize());
}

TEST_F(VectorPrepareForReuseTest, dataDependentFlags) {
  auto size = 10;

  auto prepareForReuseStatic = [](VectorPtr& vector) {
    BaseVector::prepareForReuse(vector, vector->size());
  };
  auto prepareForReuseInstance = [](VectorPtr& vector) {
    vector->prepareForReuse();
  };

  // Primitive flat vector.
  {
    SCOPED_TRACE("Flat");
    auto createVector = [&]() {
      return test::makeFlatVectorWithFlags<TypeKind::VARCHAR>(size, pool());
    };

    test::checkVectorFlagsReset(
        createVector, prepareForReuseInstance, SelectivityVector{size});
    test::checkVectorFlagsReset(
        createVector, prepareForReuseStatic, SelectivityVector{size});
  }

  // Constant vector.
  {
    SCOPED_TRACE("Constant");
    auto createVector = [&]() {
      return test::makeConstantVectorWithFlags<TypeKind::VARCHAR>(size, pool());
    };

    test::checkVectorFlagsReset(
        createVector, prepareForReuseStatic, SelectivityVector{size});
  }

  // Dictionary vector.
  {
    SCOPED_TRACE("Dictionary");
    auto createVector = [&]() {
      return test::makeDictionaryVectorWithFlags<TypeKind::VARCHAR>(
          size, pool());
    };

    test::checkVectorFlagsReset(
        createVector, prepareForReuseStatic, SelectivityVector{size});
  }

  // Map vector.
  {
    SCOPED_TRACE("Map");
    auto createVector = [&]() {
      return test::makeMapVectorWithFlags<TypeKind::VARCHAR, TypeKind::VARCHAR>(
          size, pool());
    };

    test::checkVectorFlagsReset(
        createVector, prepareForReuseInstance, SelectivityVector{size});
    test::checkVectorFlagsReset(
        createVector, prepareForReuseStatic, SelectivityVector{size});
  }
}

TEST_F(VectorPrepareForReuseTest, recursivelyReusableFlatVector) {
  // Single reference flat vector should be reusable.
  VectorPtr vector = makeFlatVector<int32_t>(100, [](auto row) { return row; });
  ASSERT_TRUE(BaseVector::recursivelyReusable(vector));

  // Multiple references make it non-reusable.
  VectorPtr copy = vector;
  ASSERT_FALSE(BaseVector::recursivelyReusable(vector));
  ASSERT_FALSE(BaseVector::recursivelyReusable(copy));

  // Release the extra reference.
  copy.reset();
  ASSERT_TRUE(BaseVector::recursivelyReusable(vector));
}

TEST_F(VectorPrepareForReuseTest, recursivelyReusableNullVector) {
  // Null vector should return false (not reusable because there's nothing to
  // reuse).
  VectorPtr nullVector = nullptr;
  ASSERT_TRUE(BaseVector::recursivelyReusable(nullVector));
}

TEST_F(VectorPrepareForReuseTest, recursivelyReusableArrayVector) {
  // Single reference array vector with single reference elements.
  VectorPtr vector = makeArrayVector<int32_t>(
      100,
      [](auto row) { return 1; },
      [](auto row, auto index) { return row + index; });
  ASSERT_TRUE(BaseVector::recursivelyReusable(vector));

  // Share the elements - should make it non-reusable.
  auto* arrayVector = vector->as<ArrayVector>();
  VectorPtr elementsCopy = arrayVector->elements();
  ASSERT_FALSE(BaseVector::recursivelyReusable(vector));

  // Release the elements copy.
  elementsCopy.reset();
  ASSERT_TRUE(BaseVector::recursivelyReusable(vector));
}

TEST_F(VectorPrepareForReuseTest, recursivelyReusableRowVector) {
  // Create children vectors first
  auto child0 = makeFlatVector<int32_t>(100, [](auto row) { return row; });
  auto child1 = makeFlatVector<int64_t>(100, [](auto row) { return row * 2; });

  // Create row vector - children are moved in, so row vector owns them
  VectorPtr vector = std::make_shared<RowVector>(
      pool(),
      ROW({{"a", INTEGER()}, {"b", BIGINT()}}),
      nullptr,
      100,
      std::vector<VectorPtr>{child0, child1});

  // At this point, child0 and child1 still hold references
  // so the row vector is NOT reusable
  ASSERT_FALSE(BaseVector::recursivelyReusable(vector));

  // Release the external references to children
  child0.reset();
  child1.reset();
  ASSERT_TRUE(BaseVector::recursivelyReusable(vector));

  // Share a child - should make it non-reusable.
  VectorPtr childCopy = vector->as<RowVector>()->childAt(0);
  ASSERT_FALSE(BaseVector::recursivelyReusable(vector));

  // Release the child copy.
  childCopy.reset();
  ASSERT_TRUE(BaseVector::recursivelyReusable(vector));
}

TEST_F(VectorPrepareForReuseTest, recursivelyReusableMapVector) {
  // Use makeMapVector helper which creates a fully owned structure
  VectorPtr vector =
      makeMapVector<int32_t, int32_t>({{{{1, 10}, {2, 20}}}, {{{3, 30}}}});
  ASSERT_TRUE(BaseVector::recursivelyReusable(vector));

  // Share the keys - should make it non-reusable.
  auto* mapVector = vector->as<MapVector>();
  VectorPtr keysCopy = mapVector->mapKeys();
  ASSERT_FALSE(BaseVector::recursivelyReusable(vector));

  // Release the keys copy.
  keysCopy.reset();
  ASSERT_TRUE(BaseVector::recursivelyReusable(vector));

  // Share values instead.
  VectorPtr valuesCopy = mapVector->mapValues();
  ASSERT_FALSE(BaseVector::recursivelyReusable(vector));

  valuesCopy.reset();
  ASSERT_TRUE(BaseVector::recursivelyReusable(vector));
}

TEST_F(VectorPrepareForReuseTest, recursivelyReusableNestedArrayOfRow) {
  // Test nested structure: Array<Row<int32, int64>>
  // Sharing deeply nested children makes the whole structure non-reusable.
  const auto rowType = ROW({{"a", INTEGER()}, {"b", BIGINT()}});
  constexpr int kNumArrays = 10;
  constexpr int kElementsPerArray = 5;
  constexpr int kTotalElements = kNumArrays * kElementsPerArray;

  // Create the rows RowVector as elements for the ArrayVector.
  auto child0 = makeFlatVector<int32_t>(kTotalElements, [](auto idx) {
    return (idx / kElementsPerArray) * 10 + (idx % kElementsPerArray);
  });
  auto child1 = makeFlatVector<int64_t>(kTotalElements, [](auto idx) {
    return (idx / kElementsPerArray) * 100 + (idx % kElementsPerArray);
  });
  auto rows =
      makeRowVector(rowType->names(), {std::move(child0), std::move(child1)});

  // Build the ArrayVector on top of rows.
  VectorPtr vector =
      makeArrayVector({0, 5, 10, 15, 20, 25, 30, 35, 40, 45}, rows);
  ASSERT_FALSE(BaseVector::recursivelyReusable(vector));

  rows.reset();
  ASSERT_TRUE(BaseVector::recursivelyReusable(vector));

  // Share the nested row's child - should make the whole array non-reusable.
  auto* arrayVector = vector->as<ArrayVector>();
  auto* rowElements = arrayVector->elements()->as<RowVector>();
  VectorPtr nestedChildCopy = rowElements->childAt(0);
  ASSERT_FALSE(BaseVector::recursivelyReusable(vector));

  nestedChildCopy.reset();
  ASSERT_TRUE(BaseVector::recursivelyReusable(vector));
}

TEST_F(VectorPrepareForReuseTest, recursivelyReusableDictionaryVector) {
  // Dictionary vectors are not considered reusable encoding.
  auto flat = makeFlatVector<int32_t>(100, [](auto row) { return row; });
  auto indices = makeIndices(100, [](auto row) { return row; });
  auto dictionary = BaseVector::wrapInDictionary(nullptr, indices, 100, flat);

  ASSERT_FALSE(BaseVector::recursivelyReusable(dictionary));

  flat.reset();
  ASSERT_FALSE(BaseVector::recursivelyReusable(dictionary));
  indices.reset();
  ASSERT_FALSE(BaseVector::recursivelyReusable(dictionary));
}

TEST_F(VectorPrepareForReuseTest, prepareForReuseDictionaryUnwrap) {
  // prepareForReuse on a uniquely-owned DictionaryVector should unwrap to
  // the inner FlatVector instead of allocating a brand new one.
  auto flat = makeFlatVector<int64_t>(100, [](auto row) { return row * 10; });
  auto indices = makeIndices(100, [](auto row) { return row % 50; });
  VectorPtr dict = BaseVector::wrapInDictionary(nullptr, indices, 100, flat);

  // Drop external refs so only dict holds inner flat.
  auto* innerRawPtr = flat.get();
  flat.reset();
  indices.reset();

  // dict is the sole owner.
  ASSERT_EQ(dict.use_count(), 1);
  ASSERT_EQ(dict->encoding(), VectorEncoding::Simple::DICTIONARY);

  // prepareForReuse should unwrap to the inner flat vector.
  BaseVector::prepareForReuse(dict, 200);
  ASSERT_EQ(dict->encoding(), VectorEncoding::Simple::FLAT);
  ASSERT_EQ(dict->size(), 200);
  ASSERT_EQ(dict.get(), innerRawPtr);
}

TEST_F(VectorPrepareForReuseTest, prepareForReuseDictionarySharedInner) {
  // When the inner FlatVector of a DictionaryVector has extra references,
  // prepareForReuse should fall back to creating a new FlatVector.
  auto flat = makeFlatVector<int32_t>(100, [](auto row) { return row; });
  auto indices = makeIndices(100, [](auto row) { return row; });
  VectorPtr dict = BaseVector::wrapInDictionary(nullptr, indices, 100, flat);
  indices.reset();

  // flat still holds an external reference to the inner vector.
  ASSERT_EQ(dict.use_count(), 1);

  BaseVector::prepareForReuse(dict, 50);
  ASSERT_EQ(dict->encoding(), VectorEncoding::Simple::FLAT);
  ASSERT_EQ(dict->size(), 50);
  // Should be a brand new vector, not the original flat.
  ASSERT_NE(dict.get(), flat.get());
}

TEST_F(VectorPrepareForReuseTest, prepareForReuseDictionaryWithNulls) {
  // DictionaryVector with nulls — unwrap should still work.
  auto flat = makeFlatVector<int32_t>(50, [](auto row) { return row; });
  auto indices = makeIndices(100, [](auto row) { return row % 50; });
  auto nulls = AlignedBuffer::allocate<bool>(100, pool(), bits::kNotNull);
  // Set every 10th row as null.
  auto* rawNulls = nulls->asMutable<uint64_t>();
  for (int i = 0; i < 100; i += 10) {
    bits::setNull(rawNulls, i, true);
  }
  VectorPtr dict = BaseVector::wrapInDictionary(nulls, indices, 100, flat);

  auto* innerRawPtr = flat.get();
  flat.reset();
  indices.reset();
  nulls.reset();

  ASSERT_EQ(dict.use_count(), 1);
  BaseVector::prepareForReuse(dict, 80);
  ASSERT_EQ(dict->encoding(), VectorEncoding::Simple::FLAT);
  ASSERT_EQ(dict->size(), 80);
  ASSERT_EQ(dict.get(), innerRawPtr);
}

TEST_F(VectorPrepareForReuseTest, prepareForReuseRowWithDictionaryChild) {
  // A RowVector whose child is a DictionaryVector — the child should be
  // unwrapped to FlatVector, not recreated from scratch.
  auto flat = makeFlatVector<int64_t>(100, [](auto row) { return row; });
  auto indices = makeIndices(100, [](auto row) { return row % 50; });
  VectorPtr dictChild =
      BaseVector::wrapInDictionary(nullptr, indices, 100, flat);
  auto* innerFlatPtr = flat.get();
  flat.reset();
  indices.reset();

  auto rowType = ROW({"c0"}, {BIGINT()});
  // Use std::move to ensure the RowVector has use_count == 1.
  auto row = std::make_shared<RowVector>(
      pool(), rowType, nullptr, 100, std::vector<VectorPtr>{dictChild});
  dictChild.reset();
  VectorPtr rowVec = std::move(row);

  ASSERT_EQ(rowVec.use_count(), 1);
  ASSERT_EQ(
      rowVec->asUnchecked<RowVector>()->childAt(0)->encoding(),
      VectorEncoding::Simple::DICTIONARY);

  BaseVector::prepareForReuse(rowVec, 200);

  // Row should be reused, and its child should be unwrapped from dict to flat.
  ASSERT_EQ(rowVec->encoding(), VectorEncoding::Simple::ROW);
  auto* resultRow = rowVec->asUnchecked<RowVector>();
  ASSERT_EQ(resultRow->childAt(0)->encoding(), VectorEncoding::Simple::FLAT);
  ASSERT_EQ(resultRow->childAt(0).get(), innerFlatPtr);
}

TEST_F(VectorPrepareForReuseTest, prepareForReuseDictionaryOfDictionary) {
  // DICTIONARY(DICTIONARY(FLAT)) — inner is also dict, not reusable.
  // Should fall back to BaseVector::create().
  auto flat = makeFlatVector<int32_t>(50, [](auto row) { return row; });
  auto innerIndices = makeIndices(100, [](auto row) { return row % 50; });
  auto innerDict =
      BaseVector::wrapInDictionary(nullptr, innerIndices, 100, flat);
  auto outerIndices = makeIndices(100, [](auto row) { return row; });
  VectorPtr outerDict =
      BaseVector::wrapInDictionary(nullptr, outerIndices, 100, innerDict);
  flat.reset();
  innerIndices.reset();
  innerDict.reset();
  outerIndices.reset();

  ASSERT_EQ(outerDict.use_count(), 1);
  ASSERT_EQ(outerDict->encoding(), VectorEncoding::Simple::DICTIONARY);

  BaseVector::prepareForReuse(outerDict, 80);
  // Inner is DICTIONARY, which is not reusable — should create a new flat.
  ASSERT_EQ(outerDict->encoding(), VectorEncoding::Simple::FLAT);
  ASSERT_EQ(outerDict->size(), 80);
}

TEST_F(VectorPrepareForReuseTest, prepareForReuseDictionaryOfConstant) {
  // DICTIONARY wrapping CONSTANT — inner is not reusable encoding.
  auto constant = BaseVector::createConstant(INTEGER(), 42, 50, pool());
  auto indices = makeIndices(100, [](auto row) { return row % 50; });
  VectorPtr dict =
      BaseVector::wrapInDictionary(nullptr, indices, 100, constant);
  constant.reset();
  indices.reset();

  ASSERT_EQ(dict.use_count(), 1);
  BaseVector::prepareForReuse(dict, 60);
  // CONSTANT is not reusable — should create a brand new flat.
  ASSERT_EQ(dict->encoding(), VectorEncoding::Simple::FLAT);
  ASSERT_EQ(dict->size(), 60);
}

TEST_F(VectorPrepareForReuseTest, prepareForReuseDictionaryUnwrapIsUsable) {
  // After unwrapping a dict to its inner flat, the vector should be fully
  // usable: we can write data to it and read it back.
  auto flat = makeFlatVector<int64_t>(100, [](auto row) { return row * 3; });
  auto indices = makeIndices(80, [](auto row) { return row % 100; });
  VectorPtr dict = BaseVector::wrapInDictionary(nullptr, indices, 80, flat);
  flat.reset();
  indices.reset();

  ASSERT_EQ(dict.use_count(), 1);
  BaseVector::prepareForReuse(dict, 50);
  ASSERT_EQ(dict->encoding(), VectorEncoding::Simple::FLAT);
  ASSERT_EQ(dict->size(), 50);

  // Write new data into the reused vector.
  auto* flatResult = dict->asFlatVector<int64_t>();
  ASSERT_NE(flatResult, nullptr);
  for (int i = 0; i < 50; ++i) {
    flatResult->set(i, i * 7);
  }
  // Read it back.
  for (int i = 0; i < 50; ++i) {
    ASSERT_EQ(flatResult->valueAt(i), i * 7);
  }
}

TEST_F(VectorPrepareForReuseTest, prepareForReuseRowMultipleDictChildren) {
  // RowVector with multiple dict children: one with uniquely-owned inner
  // (should unwrap) and one with shared inner (should create new).
  auto flat1 = makeFlatVector<int32_t>(50, [](auto row) { return row; });
  auto flat2 = makeFlatVector<int32_t>(50, [](auto row) { return row * 2; });
  auto indices = makeIndices(100, [](auto row) { return row % 50; });

  VectorPtr dict1 = BaseVector::wrapInDictionary(nullptr, indices, 100, flat1);
  VectorPtr dict2 = BaseVector::wrapInDictionary(nullptr, indices, 100, flat2);

  auto* flat1Ptr = flat1.get();
  flat1.reset();
  // flat2 retains an extra reference — dict2's inner will have use_count > 1.
  indices.reset();

  auto rowType = ROW({"c0", "c1"}, {INTEGER(), INTEGER()});
  auto row = std::make_shared<RowVector>(
      pool(), rowType, nullptr, 100, std::vector<VectorPtr>{dict1, dict2});
  dict1.reset();
  dict2.reset();
  VectorPtr rowVec = std::move(row);

  BaseVector::prepareForReuse(rowVec, 80);

  auto* resultRow = rowVec->asUnchecked<RowVector>();
  // First child: inner was uniquely owned → unwrapped to original flat.
  ASSERT_EQ(resultRow->childAt(0)->encoding(), VectorEncoding::Simple::FLAT);
  ASSERT_EQ(resultRow->childAt(0).get(), flat1Ptr);
  // Second child: inner had extra ref (flat2) → created new flat.
  ASSERT_EQ(resultRow->childAt(1)->encoding(), VectorEncoding::Simple::FLAT);
  ASSERT_NE(resultRow->childAt(1).get(), flat2.get());
}
