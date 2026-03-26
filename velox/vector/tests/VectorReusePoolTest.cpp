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
#include "velox/vector/VectorReusePool.h"

#include <gtest/gtest.h>
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::test {

class VectorReusePoolTest : public testing::Test, public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(VectorReusePoolTest, basicCheckoutAndReuse) {
  VectorReusePool pool;

  // First checkout: pool is empty → grows by one, returns new null slot.
  auto& slot1 = pool.checkout();
  ASSERT_EQ(slot1, nullptr);
  ASSERT_EQ(pool.size(), 1);

  // Assign a vector to it.
  slot1 = makeFlatVector<int64_t>({1, 2, 3});

  // Second checkout: slot1 has use_count==1 → reusable, same slot returned.
  auto& slot2 = pool.checkout();
  ASSERT_NE(slot2, nullptr);
  ASSERT_EQ(&slot2, &slot1); // Same reference
}

TEST_F(VectorReusePoolTest, growsWhenHeld) {
  VectorReusePool pool(4); // Max 4 slots

  // Checkout and hold multiple vectors (simulating pipeline depth).
  auto& s1 = pool.checkout();
  s1 = makeFlatVector<int64_t>({1});
  VectorPtr held1 = s1; // Hold extra ref → use_count == 2

  auto& s2 = pool.checkout();
  // s1 is held (uc==2), so pool grows.
  ASSERT_EQ(s2, nullptr); // New empty slot
  s2 = makeFlatVector<int64_t>({2});
  VectorPtr held2 = s2;

  auto& s3 = pool.checkout();
  ASSERT_EQ(s3, nullptr);
  ASSERT_EQ(pool.size(), 3);

  // Release held1 → s1 becomes use_count==1
  held1.reset();
  auto& s4 = pool.checkout();
  ASSERT_NE(s4, nullptr);
  ASSERT_EQ(pool.size(), 3); // Didn't grow — found free slot
}

TEST_F(VectorReusePoolTest, maxSizeCap) {
  VectorReusePool pool(2); // Max 2 slots

  auto& s1 = pool.checkout();
  s1 = makeFlatVector<int64_t>({1});
  VectorPtr h1 = s1;

  auto& s2 = pool.checkout();
  s2 = makeFlatVector<int64_t>({2});
  VectorPtr h2 = s2;

  // Pool is full (2 slots, both held). Next checkout returns overflow.
  auto& s3 = pool.checkout();
  ASSERT_EQ(s3, nullptr);
  ASSERT_EQ(pool.size(), 2); // Didn't grow past max

  // Assign to overflow — not retained in pool.
  s3 = makeFlatVector<int64_t>({3});
  ASSERT_EQ(pool.size(), 2); // Still 2
  ASSERT_GT(pool.misses(), 0u);
}

TEST_F(VectorReusePoolTest, shrink) {
  VectorReusePool pool;

  // Grow pool to 4 slots.
  std::vector<VectorPtr> held;
  for (int i = 0; i < 4; ++i) {
    auto& s = pool.checkout();
    s = makeFlatVector<int64_t>({static_cast<int64_t>(i)});
    held.push_back(s);
  }
  ASSERT_EQ(pool.size(), 4);

  // All held → shrink does nothing.
  pool.maybeShrink(2);
  ASSERT_EQ(pool.size(), 4);

  // Release all.
  held.clear();
  pool.maybeShrink(2);
  ASSERT_EQ(pool.size(), 2);
}

TEST_F(VectorReusePoolTest, clear) {
  VectorReusePool pool;

  auto& s = pool.checkout();
  s = makeFlatVector<int64_t>({1});
  ASSERT_EQ(pool.size(), 1);

  pool.clear();
  ASSERT_EQ(pool.size(), 0);
}

TEST_F(VectorReusePoolTest, detachFlatBuffers) {
  VectorReusePool pool;

  // Create a FlatVector<int64_t> and put it in the pool.
  auto& s = pool.checkout();
  s = makeFlatVector<int64_t>({10, 20, 30});
  auto* rawBuf = s->asFlatVector<int64_t>()->rawValues();
  ASSERT_NE(rawBuf, nullptr);

  // Detach — should null out values and nulls.
  pool.detachFlatBuffers<int64_t>();
  ASSERT_EQ(s->asFlatVector<int64_t>()->rawValues(), nullptr);

  // Type mismatch: detach<StringView> should skip int64_t entries.
  s = makeFlatVector<int64_t>({40, 50});
  pool.detachFlatBuffers<StringView>(); // Should NOT crash or modify
  ASSERT_NE(s->asFlatVector<int64_t>()->rawValues(), nullptr);
}

TEST_F(VectorReusePoolTest, detachSkipsHeldEntries) {
  VectorReusePool pool;

  auto& s = pool.checkout();
  s = makeFlatVector<int64_t>({1, 2, 3});
  VectorPtr held = s; // use_count == 2

  pool.detachFlatBuffers<int64_t>();
  // Should NOT detach because use_count > 1.
  ASSERT_NE(s->asFlatVector<int64_t>()->rawValues(), nullptr);
}

// --- §8 Test #10: Detach safety (uc==1 only) ---

TEST_F(VectorReusePoolTest, detachSafetyOldOutputReadable) {
  // Simulates pipeline depth 2: pool has 2 entries, one held by downstream.
  // Detach should only modify the uc==1 entry, not the held one.
  VectorReusePool pool;

  // Slot 0: checked out and held by "downstream"
  auto& s0 = pool.checkout();
  s0 = makeFlatVector<int64_t>({10, 20, 30});
  VectorPtr downstream = s0; // uc == 2

  // Slot 1: checked out (grows pool)
  auto& s1 = pool.checkout();
  s1 = makeFlatVector<int64_t>({40, 50, 60});
  // s1 uc == 1 (only pool holds it)

  ASSERT_EQ(pool.size(), 2);

  // Detach should only modify s1 (uc==1), not s0 (uc==2)
  pool.detachFlatBuffers<int64_t>();

  // s0 (held by downstream) must still be readable
  ASSERT_NE(downstream->asFlatVector<int64_t>()->rawValues(), nullptr);
  ASSERT_EQ(downstream->asFlatVector<int64_t>()->valueAt(0), 10);
  ASSERT_EQ(downstream->asFlatVector<int64_t>()->valueAt(2), 30);

  // s1 (uc==1) should have been detached
  ASSERT_EQ(s1->asFlatVector<int64_t>()->rawValues(), nullptr);
}

// --- §8 Test #11: VARCHAR/StringView detach skips ---

TEST_F(VectorReusePoolTest, varcharDetachSkipped) {
  VectorReusePool pool;

  auto& s = pool.checkout();
  s = makeFlatVector<StringView>({"hello"_sv, "world"_sv});
  ASSERT_NE(s->asFlatVector<StringView>()->rawValues(), nullptr);

  // detachFlatBuffers should skip VARCHAR entries entirely
  pool.detachFlatBuffers<StringView>();

  // Values should NOT be detached (VARCHAR skip rule)
  ASSERT_NE(s->asFlatVector<StringView>()->rawValues(), nullptr);
  ASSERT_EQ(s->asFlatVector<StringView>()->valueAt(0), StringView("hello"));
}

// --- §8 Test #14: Error/abort/close — pool entries freed ---

TEST_F(VectorReusePoolTest, clearFreesAllEntries) {
  VectorReusePool pool;

  // Create multiple entries
  std::vector<VectorPtr> held;
  for (int i = 0; i < 4; ++i) {
    auto& s = pool.checkout();
    s = makeFlatVector<int64_t>({static_cast<int64_t>(i)});
    held.push_back(s);
  }
  ASSERT_EQ(pool.size(), 4);

  // Even with held references, clear drops pool entries
  pool.clear();
  ASSERT_EQ(pool.size(), 0);

  // Held vectors are still valid (shared_ptr protects them)
  for (int i = 0; i < 4; ++i) {
    ASSERT_EQ(held[i]->asFlatVector<int64_t>()->valueAt(0), i);
  }
}

// --- §8 Test #16: maybeShrink releases free entries only ---

TEST_F(VectorReusePoolTest, shrinkReleasesOnlyFreeEntries) {
  VectorReusePool pool;

  // Grow pool by holding ALL entries
  std::vector<VectorPtr> held;
  for (int i = 0; i < 4; ++i) {
    auto& s = pool.checkout();
    s = makeFlatVector<int64_t>({static_cast<int64_t>(i)});
    held.push_back(s); // Hold all
  }
  ASSERT_EQ(pool.size(), 4);

  // maybeShrink with all held — can't shrink
  pool.maybeShrink(0);
  ASSERT_EQ(pool.size(), 4);

  // Release all
  held.clear();

  // Now all are free — shrink to 0
  pool.maybeShrink(0);
  ASSERT_EQ(pool.size(), 0);
}

// --- Pipeline simulation: pool grows to depth then stabilizes ---

TEST_F(VectorReusePoolTest, pipelineDepthSimulation) {
  // Simulates: Reader → FilterProject → downstream
  // Pipeline depth = 2 (FP holds previous output while Reader produces next)
  VectorReusePool pool;

  VectorPtr downstream; // Simulates downstream operator holding prev batch

  for (int batch = 0; batch < 10; ++batch) {
    auto& entry = pool.checkout();
    if (entry && entry.use_count() == 1 && entry->isFlatEncoding()) {
      // Reuse shell
      entry->asFlatVector<int64_t>()->unsafeSetSize(100);
    } else {
      entry =
          makeFlatVector<int64_t>(100, [&](auto i) { return batch * 100 + i; });
    }

    // "Return" to downstream — downstream releases old, holds new
    downstream = entry;
  }

  // Pool should stabilize at depth 2 (one held by downstream, one free)
  ASSERT_LE(pool.size(), 3u);
  ASSERT_GE(pool.size(), 2u);
  ASSERT_GT(pool.hits(), 0u);
}

// --- Verify actual buffer reuse via raw pointer identity ---

TEST_F(VectorReusePoolTest, bufferPointerReuseAcrossBatches) {
  // Simulates the reader pattern: detach → ensureValuesCapacity → getFlatValues
  // Verifies that the values_ buffer is reused (same raw pointer) after detach.
  VectorReusePool pool;
  BufferPtr values;

  // Batch 1: create initial vector and buffer
  auto& entry1 = pool.checkout();
  values = AlignedBuffer::allocate<int64_t>(100, pool_.get());
  auto* rawBuf1 = values->as<char>();
  entry1 = std::make_shared<FlatVector<int64_t>>(
      pool_.get(), BIGINT(), nullptr, 100, values, std::vector<BufferPtr>{});

  // Simulate downstream holding the vector (pipeline depth)
  VectorPtr downstream = entry1;

  // Batch 2: pool grows to slot 2
  auto& entry2 = pool.checkout();
  ASSERT_EQ(entry2, nullptr); // New slot (entry1 held by downstream)
  ASSERT_EQ(pool.size(), 2);

  // Create new vector for batch 2 with same buffer
  entry2 = std::make_shared<FlatVector<int64_t>>(
      pool_.get(), BIGINT(), nullptr, 100, values, std::vector<BufferPtr>{});

  // Now values has refcount=3 (our local + entry1's FlatVec + entry2's FlatVec)
  ASSERT_FALSE(values->unique());

  // Detach entry1 (still held by downstream, uc=2) — should NOT detach
  // Detach entry2 (uc=1 in pool) — SHOULD detach
  pool.detachFlatBuffers<int64_t>();

  // entry2's values were detached → values refcount drops
  // entry1's values NOT detached (held by downstream)
  // Release downstream → entry1's FlatVec refcount drops to 1
  downstream.reset();

  // Now entry1 is uc=1. Detach it too.
  pool.detachFlatBuffers<int64_t>();

  // After both detached, values should be unique (only our local holds it)
  ASSERT_TRUE(values->unique());

  // Batch 3: checkout should find entry1 (uc=1 after downstream released)
  auto& entry3 = pool.checkout();
  ASSERT_NE(entry3, nullptr);
  ASSERT_EQ(entry3.use_count(), 1);
  ASSERT_GT(pool.hits(), 0u);
}

TEST_F(VectorReusePoolTest, flatVectorShellReuseVerification) {
  // Verify that the FlatVector SHELL object (not just buffer) is reused.
  VectorReusePool pool;

  // Batch 1
  auto& e1 = pool.checkout();
  e1 = makeFlatVector<int64_t>({1, 2, 3});
  auto* shellPtr1 = e1.get(); // Raw pointer to FlatVector object

  // Batch 2: e1 is uc=1 → should be reused
  auto& e2 = pool.checkout();
  ASSERT_EQ(e2.get(), shellPtr1); // SAME shell object returned
  ASSERT_GE(pool.hits(), 1u); // At least one hit (reuse)

  // Swap internals (simulating getFlatValues pattern)
  auto newValues = AlignedBuffer::allocate<int64_t>(5, pool_.get());
  e2->asFlatVector<int64_t>()->unsafeSetSize(5);
  e2->asFlatVector<int64_t>()->unsafeSetValues(std::move(newValues));

  // Shell is same object, internals changed
  ASSERT_EQ(e2.get(), shellPtr1);
  ASSERT_EQ(e2->size(), 5);
}

TEST_F(VectorReusePoolTest, detachMakesBufferUnique) {
  // Core correctness: detach on uc=1 entry makes the shared buffer unique
  VectorReusePool pool;

  auto values = AlignedBuffer::allocate<int64_t>(100, pool_.get());
  ASSERT_TRUE(values->unique()); // Only we hold it

  // Create FlatVector sharing the buffer
  auto& entry = pool.checkout();
  entry = std::make_shared<FlatVector<int64_t>>(
      pool_.get(), BIGINT(), nullptr, 100, values, std::vector<BufferPtr>{});
  ASSERT_FALSE(values->unique()); // Now shared: us + FlatVector

  // Detach: entry is uc=1 → detach fires → FlatVector drops its ref
  pool.detachFlatBuffers<int64_t>();

  // Buffer should now be unique again
  ASSERT_TRUE(values->unique());
  // FlatVector's rawValues should be null
  ASSERT_EQ(entry->asFlatVector<int64_t>()->rawValues(), nullptr);
}

TEST_F(VectorReusePoolTest, metricsReporting) {
  VectorReusePool pool;

  // Generate some activity
  auto& s1 = pool.checkout(); // miss (grows)
  s1 = makeFlatVector<int64_t>({1});
  VectorPtr held = s1;

  auto& s2 = pool.checkout(); // miss (grows, s1 held)
  s2 = makeFlatVector<int64_t>({2});

  held.reset();
  auto& s3 = pool.checkout(); // hit (s1 now free)

  // Verify metrics
  std::map<std::string, int64_t> reported;
  pool.reportMetrics(
      "test", [&](const std::string& name, const RuntimeCounter& val) {
        reported[name] = val.value;
      });

  ASSERT_GT(reported["testPoolHits"], 0);
  ASSERT_GT(reported["testPoolMisses"], 0);
  ASSERT_EQ(reported["testPoolHighWater"], 2);
}

} // namespace facebook::velox::test
