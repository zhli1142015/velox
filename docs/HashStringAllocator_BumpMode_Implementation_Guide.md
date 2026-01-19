# HashStringAllocator Bump Mode 实现指南

> **目标**: 为 Sort/HashJoin/Aggregation 场景添加快速的 bump-pointer 分配模式
>
> **预期收益**: 分配速度 3-5x 提升，整体性能 +3-8%

---

## 快速开始

```bash
# 前置条件：确保环境就绪
cd /var/git/velox
ninja -C _build/release velox_common_memory_test
```

## 实现进度检查表

| Step | 任务 | 文件 | 验证命令 | 状态 |
|------|------|------|----------|------|
| 1 | State 类添加 bump 字段 | HashStringAllocator.h | `ninja && test HashStringAllocator` | ☐ |
| 2 | 构造函数添加 bumpMode | HashStringAllocator.h | `ninja && test HashStringAllocator` | ☐ |
| 3 | 实现 allocateFromBump | HashStringAllocator.cpp | `ninja && test HashStringAllocator` | ☐ |
| 4 | 修改 free() | HashStringAllocator.cpp | `ninja && test HashStringAllocator` | ☐ |
| 5 | 修改 freeSpace/clear | HashStringAllocator.h/cpp | `ninja && test HashStringAllocator` | ☐ |
| 6 | 添加单元测试 | HashStringAllocatorTest.cpp | `ninja && test bumpMode` | ☐ |
| 7 | RowContainer 集成 | RowContainer.h/cpp | `ninja && test RowContainer` | ☐ |
| 8 | Sort 场景启用 | SortBuffer.cpp | `ninja && test Sort` | ☐ |
| 9 | HashJoin 场景启用 | HashTable.cpp | `ninja && test HashJoin` | ☐ |
| 10 | 性能验证 | benchmark | benchmark 命令 | ☐ |

---

## Step 1: State 类添加 bump 字段

**文件**: `velox/common/memory/HashStringAllocator.h`

### 1.1 修改 State 构造函数 (L416)

找到:
```cpp
explicit State(memory::MemoryPool* pool) : pool_(pool) {}
```

替换为:
```cpp
explicit State(memory::MemoryPool* pool, bool bumpMode = false)
    : pool_(pool), bumpMode_(bumpMode) {}
```

### 1.2 添加 isBumpMode 方法 (在 `unfreeze()` 方法后)

添加:
```cpp
/// Returns true if bump allocation mode is enabled.
bool isBumpMode() const {
  return bumpMode_;
}
```

### 1.3 添加 bump 字段 (在 `DECLARE_FIELD_WITH_INIT_VALUE(int64_t, sizeFromPool, 0);` 后，约 L495)

添加:
```cpp
// Bump allocation mode fields (only used when bumpMode_ is true)
DECLARE_FIELD_WITH_INIT_VALUE(char*, bumpHead, nullptr);
DECLARE_FIELD_WITH_INIT_VALUE(char*, bumpEnd, nullptr);
```

### 1.4 添加 bumpMode_ 私有成员 (在 `assertMutability()` 函数前，约 L500)

添加:
```cpp
// Whether bump allocation mode is enabled. Must be const since it's set
// at construction time and never changes.
const bool bumpMode_{false};
```

### 验证 Step 1

```bash
cd /var/git/velox
ninja -C _build/release velox_common_memory_test
_build/release/velox/common/memory/tests/velox_common_memory_test \
    --gtest_filter="*HashStringAllocator*"
```

**预期**: 编译成功，所有测试通过

---

## Step 2: HashStringAllocator 构造函数

**文件**: `velox/common/memory/HashStringAllocator.h`

### 2.1 修改构造函数 (L183)

找到:
```cpp
explicit HashStringAllocator(memory::MemoryPool* pool)
    : StreamArena(pool), state_(pool) {}
```

替换为:
```cpp
/// Creates a HashStringAllocator.
/// @param pool The memory pool to allocate from.
/// @param bumpMode If true, enables bump-pointer allocation mode which is
///        faster but does not support individual object deallocation.
///        In bump mode, free() becomes a no-op and memory is only released
///        via clear(). This is ideal for Sort, HashJoin, and Aggregation
///        scenarios where all data is cleared at once.
explicit HashStringAllocator(
    memory::MemoryPool* pool,
    bool bumpMode = false)
    : StreamArena(pool), state_(pool, bumpMode) {}
```

### 2.2 添加 isBumpMode 公共方法 (在 `toString()` 方法后)

添加:
```cpp
/// Returns true if this allocator is in bump mode.
/// In bump mode, free() is a no-op and memory is only released via clear().
bool isBumpMode() const {
  return state_.isBumpMode();
}
```

### 验证 Step 2

```bash
ninja -C _build/release velox_common_memory_test
_build/release/velox/common/memory/tests/velox_common_memory_test \
    --gtest_filter="*HashStringAllocator*"
```

---

## Step 3: 实现 bump 分配逻辑

**文件**: `velox/common/memory/HashStringAllocator.h` 和 `.cpp`

### 3.1 头文件添加私有方法声明 (在 `int32_t freeListIndex(int size);` 后)

添加:
```cpp
// Bump allocation mode methods
Header* allocateFromBump(int64_t size);
void newBumpSlab(int64_t minSize);
```

### 3.2 cpp 文件添加实现 (在文件末尾)

添加:
```cpp
HashStringAllocator::Header* HashStringAllocator::allocateFromBump(
    int64_t size) {
  VELOX_DCHECK(state_.isBumpMode());
  size = std::max(size, static_cast<int64_t>(kMinAlloc));
  const int64_t totalSize = size + kHeaderSize;
  const int64_t alignedSize = bits::roundUp(totalSize, 8);

  char* bumpHead = state_.bumpHead();
  char* bumpEnd = state_.bumpEnd();

  // Check if we need a new slab
  if (bumpHead == nullptr || bumpHead + alignedSize > bumpEnd) {
    newBumpSlab(alignedSize);
    bumpHead = state_.bumpHead();
  }

  // Bump pointer allocation
  auto* header = reinterpret_cast<Header*>(bumpHead);
  new (header) Header(size);
  state_.bumpHead() = bumpHead + alignedSize;
  state_.currentBytes() += alignedSize;

  return header;
}

void HashStringAllocator::newBumpSlab(int64_t minSize) {
  VELOX_DCHECK(state_.isBumpMode());
  // Use kUnitSize as default slab size, same as newSlab()
  const int64_t slabSize = std::max(minSize, static_cast<int64_t>(kUnitSize));
  // Allocate through AllocationPool to maintain consistent memory tracking
  char* run = state_.pool().allocateFixed(slabSize);
  VELOX_CHECK_NOT_NULL(run, "Failed to allocate bump slab of size {}", slabSize);
  state_.bumpHead() = run;
  state_.bumpEnd() = run + slabSize;
}
```

### 3.3 修改 allocate() 方法 (L374)

在 `Header* HashStringAllocator::allocate(int64_t size, bool exactSize)` 开头添加:
```cpp
Header* HashStringAllocator::allocate(int64_t size, bool exactSize) {
  // Bump mode: use fast bump pointer allocation
  if (state_.isBumpMode()) {
    return allocateFromBump(size);
  }
  // ... 原有逻辑不变
```

### 验证 Step 3

```bash
ninja -C _build/release velox_common_memory_test
_build/release/velox/common/memory/tests/velox_common_memory_test \
    --gtest_filter="*HashStringAllocator*"
```

---

## Step 4: 修改 free() 方法

**文件**: `velox/common/memory/HashStringAllocator.cpp`

### 4.1 在 free() 开头添加 bump mode 处理 (L442)

找到 `void HashStringAllocator::free(Header* header)` 在开头添加:
```cpp
void HashStringAllocator::free(Header* header) {
  // Bump mode: free() is a no-op, only update statistics
  if (state_.isBumpMode()) {
    Header* h = header;
    while (h) {
      state_.currentBytes() -= blockBytes(h);
      h = h->isContinued() ? h->nextContinued() : nullptr;
    }
    return;
  }
  // ... 原有逻辑不变 (从 "Header* headerToFree = header;" 开始)
```

### 验证 Step 4

```bash
ninja -C _build/release velox_common_memory_test
_build/release/velox/common/memory/tests/velox_common_memory_test \
    --gtest_filter="*HashStringAllocator*"
```

---

## Step 5: 修改 freeSpace() 和 clear()

### 5.1 修改 freeSpace() (HashStringAllocator.h L307)

找到:
```cpp
uint64_t freeSpace() const {
  const int64_t minFree = state_.freeBytes() -
      state_.numFree() * (kHeaderSize + Header::kContinuedPtrSize);
  VELOX_CHECK_GE(minFree, 0, "Guaranteed free space cannot be negative");
  return minFree;
}
```

替换为:
```cpp
uint64_t freeSpace() const {
  if (state_.isBumpMode()) {
    // In bump mode, return remaining space in current slab
    if (state_.bumpHead() == nullptr) {
      return 0;
    }
    return state_.bumpEnd() - state_.bumpHead();
  }
  const int64_t minFree = state_.freeBytes() -
      state_.numFree() * (kHeaderSize + Header::kContinuedPtrSize);
  VELOX_CHECK_GE(minFree, 0, "Guaranteed free space cannot be negative");
  return minFree;
}
```

### 5.2 修改 clear() (HashStringAllocator.cpp L88)

在 `void HashStringAllocator::clear()` 开头添加:
```cpp
void HashStringAllocator::clear() {
  // Reset bump mode state
  if (state_.isBumpMode()) {
    state_.bumpHead() = nullptr;
    state_.bumpEnd() = nullptr;
  }
  // ... 原有逻辑不变 (从 "state_.numFree() = 0;" 开始)
```

### 验证 Step 5

```bash
ninja -C _build/release velox_common_memory_test
_build/release/velox/common/memory/tests/velox_common_memory_test \
    --gtest_filter="*HashStringAllocator*"
```

---

## Step 6: 添加单元测试

**文件**: `velox/common/memory/tests/HashStringAllocatorTest.cpp`

在文件末尾添加:
```cpp
TEST_F(HashStringAllocatorTest, bumpModeBasic) {
  // Create bump mode allocator
  HashStringAllocator bumpAllocator(pool_.get(), /*bumpMode=*/true);
  EXPECT_TRUE(bumpAllocator.isBumpMode());

  // Test allocation
  auto* header1 = bumpAllocator.allocate(100);
  ASSERT_NE(header1, nullptr);
  EXPECT_GE(header1->size(), 100);

  auto* header2 = bumpAllocator.allocate(200);
  ASSERT_NE(header2, nullptr);
  EXPECT_GE(header2->size(), 200);

  // Verify currentBytes is tracked
  EXPECT_GT(bumpAllocator.currentBytes(), 0);

  // Test that free() is a no-op (should not crash)
  bumpAllocator.free(header1);
  bumpAllocator.free(header2);

  // Test clear()
  bumpAllocator.clear();
  EXPECT_EQ(bumpAllocator.currentBytes(), 0);
}

TEST_F(HashStringAllocatorTest, bumpModeMultipleSlab) {
  HashStringAllocator bumpAllocator(pool_.get(), /*bumpMode=*/true);

  // Allocate enough to trigger multiple slabs
  constexpr int kIterations = 10000;
  std::vector<HashStringAllocator::Header*> headers;
  headers.reserve(kIterations);

  for (int i = 0; i < kIterations; ++i) {
    auto* header = bumpAllocator.allocate(100);
    ASSERT_NE(header, nullptr);
    headers.push_back(header);
  }

  // Free all (should be no-op)
  for (auto* header : headers) {
    bumpAllocator.free(header);
  }

  // Clear should release all memory
  bumpAllocator.clear();
  EXPECT_EQ(bumpAllocator.currentBytes(), 0);
}

TEST_F(HashStringAllocatorTest, bumpModePerformance) {
  constexpr int kIterations = 100000;

  // Normal mode
  HashStringAllocator normalAllocator(pool_.get(), /*bumpMode=*/false);
  auto normalStart = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < kIterations; ++i) {
    normalAllocator.allocate(64);
  }
  auto normalEnd = std::chrono::high_resolution_clock::now();
  normalAllocator.clear();

  // Bump mode
  HashStringAllocator bumpAllocator(pool_.get(), /*bumpMode=*/true);
  auto bumpStart = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < kIterations; ++i) {
    bumpAllocator.allocate(64);
  }
  auto bumpEnd = std::chrono::high_resolution_clock::now();
  bumpAllocator.clear();

  auto normalDuration =
      std::chrono::duration_cast<std::chrono::microseconds>(normalEnd - normalStart)
          .count();
  auto bumpDuration =
      std::chrono::duration_cast<std::chrono::microseconds>(bumpEnd - bumpStart)
          .count();

  LOG(INFO) << "Normal mode: " << normalDuration << " us";
  LOG(INFO) << "Bump mode: " << bumpDuration << " us";
  LOG(INFO) << "Speedup: " << static_cast<double>(normalDuration) / bumpDuration
            << "x";

  // Bump mode should be at least 2x faster
  EXPECT_LT(bumpDuration * 2, normalDuration);
}

TEST_F(HashStringAllocatorTest, normalModeUnchanged) {
  // Verify normal mode behavior is unchanged
  HashStringAllocator normalAllocator(pool_.get()); // bumpMode defaults to false
  EXPECT_FALSE(normalAllocator.isBumpMode());

  auto* header = normalAllocator.allocate(100);
  ASSERT_NE(header, nullptr);

  // Free should work normally
  normalAllocator.free(header);

  // Should be able to allocate again from free list
  auto* header2 = normalAllocator.allocate(100);
  ASSERT_NE(header2, nullptr);

  normalAllocator.clear();
}
```

### 验证 Step 6

```bash
ninja -C _build/release velox_common_memory_test
_build/release/velox/common/memory/tests/velox_common_memory_test \
    --gtest_filter="*bumpMode*"
```

**预期输出**:
```
[  PASSED  ] 4 tests.
Normal mode: XXXX us
Bump mode: XXXX us
Speedup: 3.XX x  (至少 2x)
```

---

## Step 7: RowContainer 集成

**文件**: `velox/exec/RowContainer.h` 和 `velox/exec/RowContainer.cpp`

### 7.1 修改 4 参数构造函数声明 (RowContainer.h L292-308)

找到:
```cpp
RowContainer(
    const std::vector<TypePtr>& keyTypes,
    const std::vector<TypePtr>& dependentTypes,
    bool useListRowIndex,
    memory::MemoryPool* pool)
    : RowContainer(
          keyTypes,
          true, // nullableKeys
          std::vector<Accumulator>{},
          dependentTypes,
          false, // hasNext
          false, // isJoinBuild
          false, // hasProbedFlag
          false, // hasNormalizedKey
          useListRowIndex,
          pool) {}
```

替换为:
```cpp
RowContainer(
    const std::vector<TypePtr>& keyTypes,
    const std::vector<TypePtr>& dependentTypes,
    bool useListRowIndex,
    memory::MemoryPool* pool,
    bool useBumpAllocator = false)
    : RowContainer(
          keyTypes,
          true, // nullableKeys
          std::vector<Accumulator>{},
          dependentTypes,
          false, // hasNext
          false, // isJoinBuild
          false, // hasProbedFlag
          false, // hasNormalizedKey
          useListRowIndex,
          pool,
          useBumpAllocator) {}
```

### 7.2 修改 10 参数主构造函数声明 (RowContainer.h L327-340)

找到主构造函数声明，在参数列表末尾添加 `bool useBumpAllocator = false`:
```cpp
RowContainer(
    const std::vector<TypePtr>& keyTypes,
    bool nullableKeys,
    const std::vector<Accumulator>& accumulators,
    const std::vector<TypePtr>& dependentTypes,
    bool hasNext,
    bool isJoinBuild,
    bool hasProbedFlag,
    bool hasNormalizedKey,
    bool useListRowIndex,
    memory::MemoryPool* pool,
    bool useBumpAllocator = false);  // 新增参数
```

### 7.3 修改构造函数实现 (RowContainer.cpp L126-145)

找到构造函数实现，修改参数列表和 stringAllocator_ 初始化:
```cpp
RowContainer::RowContainer(
    const std::vector<TypePtr>& keyTypes,
    bool nullableKeys,
    const std::vector<Accumulator>& accumulators,
    const std::vector<TypePtr>& dependentTypes,
    bool hasNext,
    bool isJoinBuild,
    bool hasProbedFlag,
    bool hasNormalizedKeys,
    bool useListRowIndex,
    memory::MemoryPool* pool,
    bool useBumpAllocator)  // 新增参数
    : keyTypes_(keyTypes),
      nullableKeys_(nullableKeys),
      isJoinBuild_(isJoinBuild),
      hasNormalizedKeys_(hasNormalizedKeys),
      useListRowIndex_(useListRowIndex),
      stringAllocator_(std::make_unique<HashStringAllocator>(
          pool, useBumpAllocator)),  // 传递 bumpMode 参数
      accumulators_(accumulators),
      rows_(pool),
      rowPointers_(StlAllocator<char*>(stringAllocator_.get())) {
```

### 验证 Step 7

```bash
ninja -C _build/release velox_exec_test
_build/release/velox/exec/tests/velox_exec_test \
    --gtest_filter="*RowContainer*"
```

---

## Step 8: Sort 场景启用

**文件**: `velox/exec/SortBuffer.cpp`

### 8.1 修改 RowContainer 创建 (L76-77)

找到:
```cpp
data_ = std::make_unique<RowContainer>(
    sortedColumnTypes, nonSortedColumnTypes, /*useListRowIndex=*/true, pool_);
```

替换为:
```cpp
data_ = std::make_unique<RowContainer>(
    sortedColumnTypes,
    nonSortedColumnTypes,
    /*useListRowIndex=*/true,
    pool_,
    /*useBumpAllocator=*/true);  // 启用 bump mode
```

### 验证 Step 8

```bash
ninja -C _build/release velox_exec_test
_build/release/velox/exec/tests/velox_exec_test \
    --gtest_filter="*Sort*"
```

---

## Step 9: HashJoin 场景启用

**文件**: `velox/exec/HashTable.cpp`

### 9.1 修改 RowContainer 创建 (构造函数中)

找到:
```cpp
rows_ = std::make_unique<RowContainer>(
    keys,
    !ignoreNullKeys,
    accumulators,
    dependentTypes,
    allowDuplicates,
    isJoinBuild,
    hasProbedFlag,
    hashMode_ != HashMode::kHash,
    /*useListRowIndex=*/false,
    pool);
```

替换为:
```cpp
rows_ = std::make_unique<RowContainer>(
    keys,
    !ignoreNullKeys,
    accumulators,
    dependentTypes,
    allowDuplicates,
    isJoinBuild,
    hasProbedFlag,
    hashMode_ != HashMode::kHash,
    /*useListRowIndex=*/false,
    pool,
    /*useBumpAllocator=*/isJoinBuild);  // 只有 join build 启用 bump mode
```

### 验证 Step 9

```bash
ninja -C _build/release velox_exec_test
_build/release/velox/exec/tests/velox_exec_test \
    --gtest_filter="*HashJoin*:*HashBuild*"
```

---

## Step 10: 性能基准测试

### 10.1 运行 benchmark

```bash
# 构建 benchmarks
ninja -C _build/release row_container_benchmark aggregation_benchmark

# 运行 RowContainer benchmark
_build/release/velox/exec/benchmarks/row_container_benchmark \
    --bm_min_iters=10

# 运行 Aggregation benchmark  
_build/release/velox/exec/benchmarks/aggregation_benchmark \
    --bm_min_iters=10
```

### 10.2 预期性能提升

| 指标 | 预期提升 |
|------|----------|
| Bump mode 分配速度 | 3-5x |
| Sort 整体性能 | +3-8% |
| HashJoin Build | +3-8% |

---

## 完整验证脚本

```bash
#!/bin/bash
# 完整验证脚本 - 保存为 verify_bump_mode.sh
set -e

cd /var/git/velox

echo "=== Building tests ==="
ninja -C _build/release velox_common_memory_test velox_exec_test

echo ""
echo "=== Step 1-5: HashStringAllocator tests ==="
_build/release/velox/common/memory/tests/velox_common_memory_test \
    --gtest_filter="*HashStringAllocator*"

echo ""
echo "=== Step 6: Bump mode tests ==="
_build/release/velox/common/memory/tests/velox_common_memory_test \
    --gtest_filter="*bumpMode*"

echo ""
echo "=== Step 7: RowContainer tests ==="
_build/release/velox/exec/tests/velox_exec_test \
    --gtest_filter="*RowContainer*"

echo ""
echo "=== Step 8: Sort tests ==="
_build/release/velox/exec/tests/velox_exec_test \
    --gtest_filter="*Sort*"

echo ""
echo "=== Step 9: HashJoin tests ==="
_build/release/velox/exec/tests/velox_exec_test \
    --gtest_filter="*HashJoin*:*HashBuild*"

echo ""
echo "=== Regression: Window tests (should not be affected) ==="
_build/release/velox/exec/tests/velox_exec_test \
    --gtest_filter="*Window*"

echo ""
echo "=========================================="
echo "=== All tests passed! ==="
echo "=========================================="
```

---

## 附录: 代码位置速查表

| 功能 | 文件 | 行号 |
|------|------|------|
| State 类 | HashStringAllocator.h | L413-510 |
| 构造函数 | HashStringAllocator.h | L183 |
| allocate() | HashStringAllocator.cpp | L374 |
| free() | HashStringAllocator.cpp | L442 |
| clear() | HashStringAllocator.cpp | L88 |
| freeSpace() | HashStringAllocator.h | L307 |
| RowContainer 4参数构造 | RowContainer.h | L292-308 |
| RowContainer 10参数构造 | RowContainer.cpp | L126-145 |
| SortBuffer 创建 | SortBuffer.cpp | L76-77 |
| HashTable 创建 | HashTable.cpp | 构造函数 |

---

## 故障排除

### 编译错误: bumpMode_ 未定义

**原因**: Step 1.4 遗漏或位置错误

**解决**: 确保在 State 类的 `assertMutability()` 函数前添加:
```cpp
const bool bumpMode_{false};
```

### 测试失败: bumpModePerformance

**原因**: 可能是测试环境不稳定

**解决**: 增加迭代次数或放宽断言条件

### 运行时崩溃: VELOX_CHECK_NOT_NULL

**原因**: 内存分配失败

**解决**: 检查 MemoryPool 配额设置
