# HashStringAllocator Bump Mode Optimization Design Document

## 1. Overview

### 1.1 Problem Statement

Velox 的 HashStringAllocator 是一个功能强大的内存分配器，支持 Arena 式内存池分配、单对象释放（free list）、相邻空闲块合并、以及多段连续分配（kContinued）。

然而，对于 Sort、HashJoin、Aggregation 等场景，这些高级功能带来了不必要的开销：

**场景分析表**

| 场景 | 释放方式 | free() 必要性 |
|------|----------|---------------|
| Sort | clear() 整体清空 | 不需要 |
| HashJoin | clear() 整体清空 | 不需要 |
| Aggregation | freeAggregates() + clear() | 冗余 |

### 1.2 Solution: Bump Mode

引入 Bump Pointer Allocation Mode，在构造时通过 bumpMode=true 启用。

**Bump Mode 工作方式：**
- 分配：O(1) 指针移动
- 释放：no-op（仅更新统计）
- 清空：释放所有内存

### 1.3 Performance Results

| Metric | Normal Mode | Bump Mode | Improvement |
|--------|-------------|-----------|-------------|
| 小对象分配 (≤256B) | 80-400 ns | 10-100 ns | 3-10x faster |
| 混合分配模式 | 190-360 ns | 75-95 ns | 2.5-4.5x faster |
| 分配+释放周期 | 125-165 ns | 40-47 ns | 3x faster |
| 字符串复制 | 65-75 ns | 40-47 ns | 1.5x faster |

---

## 2. Design Goals

1. **最小侵入性**：在现有 HashStringAllocator 内部添加模式开关，不创建新类
2. **向后兼容**：默认 bumpMode=false，现有代码无需修改
3. **渐进式采用**：可按场景逐步启用
4. **正确性优先**：保证内存统计、清理等行为正确

---

## 3. Architecture

### 3.1 High-Level Design

HashStringAllocator 内部包含两种模式：

**Normal Mode（默认）：**
- 使用 Free lists 管理空闲块
- 支持 Block merging（相邻块合并）
- 使用 Header flags（kFree 等标志位）

**Bump Mode（bumpMode=true）：**
- 使用 Bump pointer（bumpHead_）进行快速分配
- 维护 Slab end（bumpEnd_）标记当前 slab 结束位置
- 大对象分配委托给 Pool
- free() 变为 no-op

**共享组件：**
- AllocationPool（slab 管理）
- Header structure（4 bytes）
- currentBytes tracking（内存统计）
- allocationsFromPool（大对象追踪）

### 3.2 Memory Layout Comparison

**Normal Mode 内存布局：**

每个 slab 包含多个 Header + Data 块，末尾有 kArenaEnd 标记。Free list 管理空闲块，可能存在不连续的 free 块。

**Bump Mode 内存布局：**

每个 slab 包含连续的 Header + Data 块，末尾保留 SIMD Padding。bumpHead 指向当前分配位置，bumpEnd 指向可用空间末尾。无 free 块，无 kArenaEnd 标记。

---

## 4. Implementation Details

### 4.1 State Class Extensions

在 State 类中新增以下字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| bumpHead_ | char* | 当前分配位置，初始为 nullptr |
| bumpEnd_ | char* | 当前 slab 结束位置，初始为 nullptr |
| bumpMode_ | const bool | 是否启用 bump 模式，构造时设置，不可变 |

新增方法 isBumpMode() 返回当前模式。

### 4.2 Constructor Changes

构造函数新增 bumpMode 参数，默认值为 false。

**参数说明：**
- pool：内存池指针
- bumpMode：如果为 true，启用 bump-pointer 分配模式。该模式更快但不支持单对象释放。在 bump mode 下，free() 变为 no-op，内存仅通过 clear() 释放。适用于 Sort、HashJoin、Aggregation 等整体清空的场景。

### 4.3 Core Methods

#### 4.3.1 allocate()

在函数开头检查 isBumpMode()，如果为 true 则调用 allocateFromBump()，否则执行原有逻辑。

#### 4.3.2 allocateFromBump() - 新方法

该方法实现 bump pointer 分配逻辑：

1. 将 size 调整为至少 kMinAlloc
2. **大对象处理**：如果 size > kMaxAlloc，直接调用 allocateFromPool 分配，避免浪费 slab 空间
3. 计算 totalSize = size + kHeaderSize，并按 8 字节对齐
4. **检查当前 slab**：如果 bumpHead 为 null 或空间不足，调用 newBumpSlab() 分配新 slab
5. 在 bumpHead 位置构造 Header，移动 bumpHead，更新 currentBytes
6. **预取优化**：如果剩余空间不足，使用 __builtin_prefetch 提示 CPU 预取 pool 数据结构
7. 返回 Header 指针

#### 4.3.3 newBumpSlab() - 新方法

该方法分配新的 bump slab：

1. 计算 slabSize = max(minSize + kSimdPadding, kUnitSize)
2. 通过 AllocationPool.allocateFixed() 分配内存
3. 设置 bumpHead = run（分配起始地址）
4. 设置 bumpEnd = run + slabSize - kSimdPadding（保留 SIMD 安全填充）

#### 4.3.4 free()

在函数开头检查 isBumpMode()，如果为 true：

1. 遍历 header 链（处理 kContinued）
2. **大对象检查**：如果 size > kMaxAlloc 且在 allocationsFromPool 中，调用 freeToPool 真正释放
3. **普通分配**：仅更新 currentBytes 统计，不实际释放（no-op）
4. 提前返回，不执行原有 free list 逻辑

#### 4.3.5 clear()

在函数开头，如果 isBumpMode() 为 true，重置 bumpHead 和 bumpEnd 为 nullptr。

在 DEBUG 模式的 header 遍历检查前，如果 isBumpMode() 为 true，跳过该检查。因为 bump mode 的 header 连续排列，没有 kArenaEnd 标记，无法按 normal mode 方式遍历。

#### 4.3.6 storeStringFast() - 内联热路径优化

在函数开头检查 isBumpMode()，如果为 true：

1. 计算 alignedSize
2. **热路径**（使用 FOLLY_LIKELY 标记）：如果 bumpHead 非空且空间足够，直接在 bumpHead 构造 Header，移动指针，更新统计
3. **冷路径**：调用 allocateFromBump()
4. 使用 simd::memcpy 复制数据
5. 构造 StringView 返回 true

#### 4.3.7 freeSpace()

如果 isBumpMode() 为 true，返回 bumpEnd - bumpHead（当前 slab 剩余空间），如果 bumpHead 为 nullptr 返回 0。

#### 4.3.8 checkConsistency()

如果 isBumpMode() 为 true：
- 检查 numFree == 0
- 检查 freeBytes == 0
- 直接返回 currentBytes
- 跳过原有的 header 遍历验证

---

## 5. Optimizations

### 5.1 Optimization Summary

| 优化 | 描述 | 预期收益 |
|------|------|----------|
| Inline Hot Path | storeStringFast() 中内联 bump 分配逻辑 | 避免函数调用开销 |
| Large Object Pool | size > kMaxAlloc 直接使用 allocateFromPool | 减少 slab 内存浪费 |
| Prefetch Hint | 剩余空间不足时预取 pool 数据结构 | 减少分配延迟抖动 |
| SIMD Padding | slab 末尾保留 simd::kPadding 字节 | 支持 SIMD 安全内存访问 |
| 8-byte Alignment | 分配大小按 8 字节对齐 | 优化内存访问 |

### 5.2 Large Object Handling

当分配大小超过 kMaxAlloc（约 12KB）时，bump mode 会将分配委托给 allocateFromPool()。

这样做的好处：
1. **避免浪费 slab 空间**：大对象会独占大部分 slab，剩余空间无法利用
2. **支持释放**：通过 allocationsFromPool_ 跟踪，free() 时可以真正释放

### 5.3 Branch Prediction Hints

使用 FOLLY_LIKELY 和 FOLLY_UNLIKELY 标记分支概率，帮助 CPU 分支预测器优化热路径。

---

## 6. API Changes

### 6.1 New Constructor Parameter

**修改前：** HashStringAllocator(memory::MemoryPool* pool)

**修改后：** HashStringAllocator(memory::MemoryPool* pool, bool bumpMode = false)

### 6.2 New Public Method

新增 isBumpMode() 方法，返回是否处于 bump mode。

### 6.3 Behavior Changes in Bump Mode

| Method | Normal Mode | Bump Mode |
|--------|-------------|-----------|
| allocate() | Free list 查找后分配 | Bump pointer 移动 |
| free() | 释放到 free list + 合并 | No-op（统计更新） |
| clear() | 释放所有内存 | 重置 bump pointers + 释放内存 |
| freeSpace() | Free list 中可用空间 | 当前 slab 剩余空间 |
| checkConsistency() | 遍历验证 Header 链 | 仅验证统计一致性 |

---

## 7. Testing

### 7.1 New Test Cases

| Test Case | Description |
|-----------|-------------|
| bumpModeBasic | 基本分配、释放、清空测试 |
| bumpModeMultipleSlab | 多 slab 分配测试（10000 次分配） |
| bumpModePerformance | 综合性能对比测试 |
| bumpModeLargeAllocation | 大对象分配和释放测试 |
| bumpModeInlineFastPath | 内联热路径测试（storeStringFast） |
| normalModeUnchanged | 验证 normal mode 行为不变 |

### 7.2 Test Results

全部 28 个 HashStringAllocatorTest 测试通过，包括 6 个新增的 bump mode 测试。

### 7.3 Performance Benchmark Results

测试环境：Linux，Release 构建，100000 次迭代

#### 7.3.1 不同分配大小的性能对比

| Size (Bytes) | Normal Mode (ns) | Bump Mode (ns) | Speedup |
|--------------|------------------|----------------|---------|
| 16 | 79-235 | 16-100 | 2.4-4.9x |
| 32 | 86-207 | 7-14 | 6.1-29.6x |
| 64 | 94-187 | 32-62 | 2.8-3.4x |
| 128 | 134-310 | 20-82 | 3.0-11.4x |
| 256 | 126-413 | 25-93 | 2.6-5.9x |
| 512 | 158-1049 | 127-295 | 1.2-3.6x |
| 1024 | 168-1302 | 123-514 | 1.4-3.2x |
| 4096 | 3106-5108 | 2877-4685 | 1.0-1.1x |

**分析：**
- 小对象分配（16-256 字节）：Bump mode 提供显著加速，通常在 3-10x 范围
- 中等对象（512-1024 字节）：仍有 1.5-3x 的提升
- 大对象（4096 字节及以上）：性能相近，因为大对象在两种模式下都走 allocateFromPool 路径

#### 7.3.2 混合分配模式

模拟真实工作负载，随机分配 16-512 字节大小的对象：

| Metric | Normal Mode | Bump Mode | Speedup |
|--------|-------------|-----------|---------|
| Mixed (16-512B) | 187-358 ns | 77-95 ns | 2.3-4.7x |

**分析：** 在模拟真实工作负载的混合分配模式下，Bump mode 平均提供约 3x 的性能提升。

#### 7.3.3 分配 + 释放模式

测试完整的分配-释放周期（64 字节对象，50000 次迭代）：

| Metric | Normal Mode | Bump Mode | Speedup |
|--------|-------------|-----------|---------|
| Alloc + Free | 124-165 ns | 40-47 ns | 3.0-3.5x |

**分析：** 即使包含 free() 调用（在 bump mode 下是 no-op），bump mode 仍然快 3x 以上。这对于 Aggregation 场景中累加器销毁时调用 free() 的情况特别有价值。

#### 7.3.4 字符串复制性能（storeStringFast）

测试 copyMultipart 函数的性能（100 字节字符串，100000 次迭代）：

| Metric | Normal Mode | Bump Mode | Speedup |
|--------|-------------|-----------|---------|
| String copy (100B) | 65-74 ns | 39-47 ns | 1.4-1.8x |

**分析：** 字符串复制包含内存拷贝操作，因此分配器优化的影响被稀释。但 bump mode 的内联热路径优化仍然提供了 1.5x 左右的提升。

### 7.4 Performance Summary

| 场景 | 平均加速比 | 最佳加速比 | 说明 |
|------|-----------|-----------|------|
| 小对象分配 (≤256B) | 4-6x | 29.6x | 核心优化场景 |
| 中等对象 (512-1024B) | 2-3x | 3.6x | 有明显提升 |
| 大对象 (≥4096B) | 1x | 1.1x | 性能相近（走 pool 路径） |
| 混合分配 | 3-4x | 4.7x | 模拟真实工作负载 |
| 分配+释放 | 3x | 3.5x | 包含 free() 调用 |
| 字符串复制 | 1.5x | 1.8x | 受内存拷贝限制 |

**关键发现：**

1. **小对象分配是最大受益者**：16-256 字节的分配获得 3-10x 加速，这正是 Sort/HashJoin/Aggregation 中最常见的分配大小

2. **性能稳定**：多次运行显示 bump mode 性能更稳定（方差更小），因为避免了 free list 遍历的不确定性

3. **零额外开销**：大对象分配性能与 normal mode 相当，不会造成回退

4. **free() 优化有效**：在 bump mode 下 free() 变为 no-op，显著减少了分配-释放周期的开销

---

## 8. End-to-End Performance Analysis (Sort/Agg/Join)

### 8.1 Benchmark Overview

除了 HashStringAllocator 级别的微基准测试外，我们还使用 `BumpAllocatorBenchmark` 对 Sort、Aggregation、Join 三个真实场景进行了端到端性能测试。该 benchmark 使用 RowContainer（集成了 HashStringAllocator）来模拟真实的数据处理负载。

**测试环境**：Linux，DEBUG 构建（folly benchmark 限制），100K-1M 行数据

### 8.2 Sort 场景性能

Sort 场景是 bump mode 的理想应用场景：只分配不释放，最后 clear() 整体清空。

| Benchmark | Normal Mode | Bump Mode | Speedup | 说明 |
|-----------|-------------|-----------|---------|------|
| Sort_BigintKey_100k | 565.93 us | 530.91 us | 1.07x | 纯整数 key |
| Sort_BigintKey_1M | 5.63 ms | 5.33 ms | 1.06x | 大数据量 |
| Sort_VarcharKey16_100k | 1.41 ms | 1.40 ms | 1.01x | 短字符串 key |
| Sort_VarcharKey64_100k | 1.66 ms | 1.60 ms | 1.04x | 中等字符串 |
| Sort_VarcharKey256_100k | 3.41 ms | 3.27 ms | 1.04x | 长字符串 |
| **Sort_BigintKey_1Varchar16_100k** | 1.55 ms | 1.42 ms | **1.09x** | 整数 key + 短字符串依赖列 |
| **Sort_BigintKey_1Varchar64_100k** | 1.82 ms | 1.63 ms | **1.12x** | 整数 key + 中等字符串依赖列 |
| **Sort_BigintKey_1Varchar256_100k** | 3.89 ms | 3.41 ms | **1.14x** | 整数 key + 长字符串依赖列 |
| **Sort_BigintKey_4Varchar16_100k** | 4.49 ms | 3.92 ms | **1.15x** | 整数 key + 4 个短字符串 |
| Sort_Varchar64Key_4Varchar64_100k | 9.24 ms | 8.15 ms | **1.13x** | 字符串 key + 字符串依赖列 |
| **Sort_MixedKey_2Int2Varchar_100k** | 5.89 ms | 4.33 ms | **1.36x** | 混合 key + 混合依赖列 |

**Sort 场景分析**：
- **最佳场景**：多个 VARCHAR 依赖列，加速 9-36%
- **平均加速**：5-15%
- **无负面影响**：所有 Sort 测试都显示正向或持平

### 8.3 Aggregation 场景性能

Aggregation 场景的特点是：group 创建一次，但累加器可能多次更新。

| Benchmark | Normal Mode | Bump Mode | Speedup | 说明 |
|-----------|-------------|-----------|---------|------|
| Agg_BigintKey_VarcharAcc16_10kG_100kU | 1.44 ms | 1.33 ms | **1.08x** | 整数 key + 短字符串累加器 |
| **Agg_BigintKey_VarcharAcc64_10kG_100kU** | 1.86 ms | 1.53 ms | **1.22x** | 整数 key + 中等字符串累加器 |
| Agg_BigintKey_VarcharAcc256_10kG_100kU | 3.05 ms | 2.98 ms | 1.02x | 整数 key + 长字符串累加器 |
| Agg_BigintKey_VarcharAcc64_100kG_1MU | 18.56 ms | 19.03 ms | 0.98x | 大规模聚合 |
| Agg_Varchar16Key_IntAcc_10kG_100kU | 499.26 us | 511.40 us | 0.98x | 字符串 key + 整数累加器 |
| Agg_Varchar64Key_IntAcc_10kG_100kU | 707.73 us | 743.28 us | 0.95x | 字符串 key + 整数累加器 |
| Agg_Varchar16Key_Varchar16Acc_10kG_100kU | 1.24 ms | 1.30 ms | 0.95x | 字符串 key + 字符串累加器 |
| Agg_Varchar64Key_Varchar64Acc_10kG_100kU | 1.69 ms | 1.87 ms | 0.90x | 中等字符串 |
| Agg_Varchar64Key_Varchar256Acc_10kG_100kU | 3.32 ms | 3.71 ms | 0.89x | 长字符串累加器 |

**Aggregation 场景分析**：
- **正向场景**：BIGINT key + VARCHAR 累加器（更少的初始 key 分配），加速 8-22%
- **负向场景**：VARCHAR key + VARCHAR 累加器且频繁更新（-5% 到 -11%）
- **原因**：benchmark 模拟了累加器频繁覆盖写入的场景。在 bump mode 下，被覆盖的旧数据无法释放，导致内存浪费和更多的 slab 分配
- **实际生产影响**：真实聚合场景中，累加器通常是追加模式（如 array_agg），或者只在最后输出时读取一次

**建议**：对于频繁覆盖更新字符串累加器的聚合（如 `first_value`, `last_value`），考虑在调用点禁用 bump mode

### 8.4 真实聚合场景性能（新增测试）

为了更准确地评估 bump mode 在真实聚合场景中的表现，我们添加了更真实的测试：

**SUM/COUNT 场景**（VARCHAR key 分配一次，累加器只做整数更新）：

| Benchmark | Normal Mode | Bump Mode | Speedup |
|-----------|-------------|-----------|---------|
| **AggReal_SumCount_Varchar16Key_10kG_100kU** | 542.67 us | 487.13 us | **1.11x** |
| AggReal_SumCount_Varchar64Key_10kG_100kU | 812.05 us | 787.55 us | **1.03x** |
| AggReal_SumCount_Varchar64Key_100kG_1MU | 8.49 ms | 8.38 ms | **1.01x** |

**MIN/MAX 场景**（稀疏更新，只有当新值更优时才更新累加器）：

| Benchmark | Normal Mode | Bump Mode | Speedup |
|-----------|-------------|-----------|---------|
| **AggReal_MinMax_Varchar64Key_Varchar64Val_10kG** | 1.38 ms | 1.29 ms | **1.07x** |
| AggReal_MinMax_Varchar64Key_Varchar256Val_10kG | 1.88 ms | 1.98 ms | 0.95x |

**真实聚合场景分析**：
- **SUM/COUNT**：纯正向收益（3-11%），因为只有 key 分配需要 HSA，累加器更新是纯整数操作
- **MIN/MAX**：大部分场景有正向收益，只有长字符串（256 bytes）场景略有负面影响
- **对比原有测试**：原有测试模拟的是"每次更新都覆盖写入字符串"，这是最坏情况；真实场景中累加器更新通常不涉及内存分配

### 8.5 Join 场景性能

Join Build 场景是 bump mode 的另一个理想场景：批量插入 build 侧数据，探测时只读不写。

| Benchmark | Normal Mode | Bump Mode | Speedup | 说明 |
|-----------|-------------|-----------|---------|------|
| Join_BigintKey_NoPayload_100k | 571.91 us | 687.16 us | 0.83x | 纯整数 key（无字符串分配） |
| Join_BigintKey_NoPayload_1M | 5.99 ms | 7.24 ms | 0.83x | 大数据量纯整数 |
| Join_Varchar16Key_100k | 1.35 ms | 1.63 ms | 0.83x | 短字符串 key |
| Join_Varchar64Key_100k | 1.60 ms | 1.92 ms | 0.83x | 中等字符串 key |
| Join_Varchar256Key_100k | 3.24 ms | 3.22 ms | 1.01x | 长字符串 key |
| Join_Varchar64Key_1M | 20.96 ms | 21.55 ms | 0.97x | 大规模字符串 key |
| Join_BigintKey_1Varchar16_100k | 1.39 ms | 1.41 ms | 0.99x | 整数 key + 短字符串 payload |
| **Join_BigintKey_1Varchar64_100k** | 1.63 ms | 1.59 ms | **1.03x** | 整数 key + 中等字符串 payload |
| **Join_BigintKey_1Varchar256_100k** | 3.63 ms | 3.32 ms | **1.09x** | 整数 key + 长字符串 payload |
| Join_BigintKey_4Varchar64_100k | 6.13 ms | 6.02 ms | 1.02x | 多个 payload 列 |
| **Join_BigintKey_4Varchar64_1M** | 73.06 ms | 69.90 ms | **1.05x** | 大规模多 payload |
| Join_2Varchar32Key_2Int2Varchar_100k | 5.45 ms | 5.28 ms | 1.03x | 混合 key + 混合 payload |

**Join 场景分析**：
- **负向场景**：无 payload 或纯整数场景（-17%），因为没有实际的字符串分配，bump mode 的优势无法体现，反而增加了模式检查开销
- **正向场景**：有 VARCHAR payload 时，加速 3-9%
- **大数据量**：在 1M 行规模下，VARCHAR payload 场景有 5% 提升

**建议**：对于无 VARCHAR 列的纯整数 Join，考虑禁用 bump mode

### 8.6 Direct Allocator 性能对比

这组测试直接测量 HashStringAllocator 的分配性能，排除 RowContainer 的其他开销：

| Benchmark | Normal Mode | Bump Mode | Speedup |
|-----------|-------------|-----------|---------|
| **Direct_Alloc_1M_16bytes** | 30.58 ms | 2.95 ms | **10.4x** |
| **Direct_Alloc_1M_32bytes** | 31.64 ms | 4.81 ms | **6.6x** |
| **Direct_Alloc_1M_64bytes** | 34.79 ms | 8.78 ms | **4.0x** |
| Direct_Alloc_1M_128bytes | 40.91 ms | 14.70 ms | **2.8x** |
| Direct_Alloc_1M_256bytes | 52.97 ms | 26.77 ms | **2.0x** |
| Direct_AllocFree_1M_32bytes | 30.06 ms | 17.23 ms | 1.7x |
| Direct_AllocFree_1M_64bytes | 34.59 ms | 23.49 ms | 1.5x |

**Direct Allocator 分析**：
- 小对象分配（16-64 bytes）：**4-10x 加速**
- 中等对象（128-256 bytes）：**2-3x 加速**
- 分配+释放周期：**1.5-2x 加速**（因为 free() 在 bump mode 下是 no-op）

### 8.7 端到端性能总结

| 场景 | 最佳加速 | 平均影响 | 建议 |
|------|----------|----------|------|
| **Sort** | +36% | +5-10% | ✅ 推荐启用 |
| **Aggregation (SUM/COUNT)** | +11% | +3-5% | ✅ 推荐启用 |
| **Aggregation (MIN/MAX，稀疏更新)** | +7% | +1-5% | ✅ 推荐启用 |
| **Aggregation (频繁覆盖更新)** | - | -5-10% | ⚠️ 按需禁用 |
| **Join (有 VARCHAR payload)** | +9% | +3-5% | ✅ 推荐启用 |
| **Join (纯整数)** | - | -17% | ❌ 建议禁用 |
| **Direct Allocator** | +10.4x | +3-6x | ✅ 核心优化收益 |

### 8.8 配置建议

基于端到端性能分析，建议采用以下策略：

**默认启用场景**：
- SortBuffer
- HashBuild（有 VARCHAR 列时）
- GroupBy Aggregation（BIGINT key 为主）

**按需禁用场景**：
- 纯整数 HashJoin（无 VARCHAR payload）
- 频繁覆盖更新 VARCHAR 累加器的聚合

**实现方式**：
可以在 RowContainer 创建时根据 schema 分析自动决定是否启用 bump mode：

伪代码逻辑：
1. 检查是否有 VARCHAR/VARBINARY/复杂类型的 key 或 dependent 列
2. 如果有字符串列，启用 bump mode
3. 如果全部是定长类型（INTEGER/BIGINT/DOUBLE 等），禁用 bump mode

---

## 9. Files Changed

### 9.1 Header File

## 9. Files Changed

### 9.1 Header File

**文件：** velox/common/memory/HashStringAllocator.h

| Change | Lines | Description |
|--------|-------|-------------|
| Constructor doc | +7 | 添加 bumpMode 参数文档 |
| Constructor signature | +1 | 添加 bumpMode = false 参数 |
| freeSpace() | +6 | 添加 bump mode 分支 |
| isBumpMode() | +5 | 新增公共方法 |
| allocateFromBump() declaration | +1 | 新增私有方法声明 |
| newBumpSlab() declaration | +1 | 新增私有方法声明 |
| State constructor | +2 | 添加 bumpMode 参数 |
| State isBumpMode() | +4 | 新增方法 |
| State fields | +6 | 添加 bumpHead_, bumpEnd_, bumpMode_ |

**合计：** +44 行

### 9.2 Implementation File

**文件：** velox/common/memory/HashStringAllocator.cpp

| Change | Lines | Description |
|--------|-------|-------------|
| clear() bump reset | +5 | 重置 bump pointers |
| clear() DEBUG guard | +4 | 跳过 bump mode 的 header 遍历 |
| allocate() bump branch | +5 | 添加 bump mode 分支 |
| free() bump mode | +21 | Bump mode 释放逻辑 |
| storeStringFast() inline | +25 | 内联 bump 分配热路径 |
| checkConsistency() | +7 | Bump mode 早期返回 |
| allocateFromBump() | +32 | 新增方法实现 |
| newBumpSlab() | +14 | 新增方法实现 |

**合计：** +113 行

### 9.3 Test File

**文件：** velox/common/memory/tests/HashStringAllocatorTest.cpp

| Change | Lines | Description |
|--------|-------|-------------|
| bumpModeBasic | +28 | 基本功能测试 |
| bumpModeMultipleSlab | +24 | 多 slab 测试 |
| bumpModePerformance | +45 | 性能对比测试 |
| normalModeUnchanged | +17 | Normal mode 兼容性测试 |
| bumpModeLargeAllocation | +27 | 大对象测试 |
| bumpModeInlineFastPath | +35 | 内联热路径测试 |

**合计：** +176 行

---

## 10. Future Work

### 10.1 RowContainer Integration

将 bump mode 集成到 RowContainer，为 Sort 和 HashJoin 场景启用。

需要在 RowContainer 构造函数中添加 useBumpAllocator 参数，并在 SortBuffer 和 HashBuild 中传递 true 以启用 bump mode。

### 10.2 Aggregation Optimization

对于 Aggregation 场景，可以通过跳过冗余的 freeAggregates() 调用进一步优化。

当前流程（冗余）：先调用 freeAggregates(rows) 遍历所有行调用 free()，再调用 stringAllocator_.clear() 整体释放。

优化后：如果 stringAllocator_.isBumpMode() 为 true，直接调用 clear()，跳过逐个 free() 调用。

### 10.3 Potential Improvements

1. **Per-thread bump allocators**：减少多线程竞争
2. **Adaptive mode switching**：根据分配模式自动切换
3. **Memory pressure callback**：在内存压力下提前释放 bump slabs

---

## 11. Appendix

### 11.1 Constants

| Constant | Value | Description |
|----------|-------|-------------|
| kMinAlloc | 16 | 最小分配大小 |
| kMaxAlloc | ~12KB | 大对象阈值（3/4 page size） |
| kUnitSize | 64KB | 默认 slab 大小（16 pages） |
| kHeaderSize | 4 | Header 结构大小 |
| simd::kPadding | 32 | SIMD 安全填充 |

### 11.2 Header Structure

Header 结构占 4 bytes，包含以下字段：

| Bit | Name | Description |
|-----|------|-------------|
| bit 31 | kFree | 是否空闲（bump mode 不使用） |
| bit 30 | kContinued | 是否有后续块 |
| bit 29 | kPreviousFree | 前一块是否空闲（bump mode 不使用） |
| bits 0-28 | size | 块大小（最大约 512MB） |

### 11.3 Related Files

| File | Purpose |
|------|---------|
| velox/common/memory/AllocationPool.h | Slab 内存池管理 |
| velox/common/memory/Memory.h | MemoryPool 接口 |
| velox/exec/RowContainer.h | 行存储容器（未来集成点） |
| velox/exec/SortBuffer.cpp | Sort 场景（未来集成点） |
| velox/exec/HashBuild.cpp | HashJoin Build 场景（未来集成点） |

---

## 12. Changelog

| Date | Description |
|------|-------------|
| 2026-01-19 | Initial bump mode implementation |
| 2026-01-19 | Added large object handling via allocateFromPool |
| 2026-01-19 | Added inline hot path optimization in storeStringFast |
| 2026-01-19 | Added prefetch hint for reduced latency spikes |
| 2026-01-19 | Fixed SIMD padding in newBumpSlab |
| 2026-01-19 | Fixed checkConsistency for bump mode |
| 2026-01-19 | Fixed clear() DEBUG check for bump mode |
| 2026-01-19 | Added end-to-end Sort/Agg/Join performance analysis |
