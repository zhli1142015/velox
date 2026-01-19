# Row Group 分区处理分析

## Executive Summary

本文档深入分析在 Velox RowContainer 排序和聚合操作中引入显式 Row Group 分区的必要性、潜在收益和代价。

**重要发现 (2026-01 更新)**:

经过对 DuckDB 和 DataFusion 源代码的详细分析，发现：

1. **DuckDB 的 `DEFAULT_ROW_GROUP_SIZE = 122880` 主要用于存储层物理分区**，而非排序时的缓存局部性优化
2. **DataFusion 同样没有实现显式的 Row Group 缓存优化**，其 batch_size 用于内存管理
3. **两者的实际缓存优化依赖于**: 归一化 Key 编码 + 连续内存布局 (Velox 已有 PrefixSort + KeyEncoder)

**核心结论**:

| 方面 | 评估 |
|------|------|
| 必要性 | ⚠️ 低-中等 - 主流系统未显式实现，理论收益存在但未被验证 |
| 潜在收益 | 理论上 5-15% (大数据集)，但缺乏工业实践验证 |
| 主要代价 | 代码复杂度增加，小数据集可能有 overhead |
| 推荐 | **暂不实现** - Velox 已有 PrefixSort/KeyEncoder，与主流系统对齐 |

---

## 1. 什么是 Row Group 分区

### 1.1 概念定义

**Row Group** 是将大量行数据按固定大小分组处理的策略，目的是优化 CPU 缓存利用率。

```
传统处理 (无 Row Group):
┌─────────────────────────────────────────────────────────────┐
│ Row 0 │ Row 1 │ Row 2 │ ... │ Row 999,999 │ Row 1,000,000 │
└─────────────────────────────────────────────────────────────┘
                    ↓ 一次性处理所有行

Row Group 分区处理:
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│ Group 0         │ │ Group 1         │ │ Group N         │
│ Row 0 ~ 131071  │ │ Row 131072 ~    │ │ ...             │
└─────────────────┘ └─────────────────┘ └─────────────────┘
        ↓                   ↓                   ↓
    处理 Group 0       处理 Group 1         处理 Group N
```

### 1.2 DuckDB 的实现 (修正)

**重要更正**: 经过对 DuckDB 源代码的详细分析，DuckDB 的 `DEFAULT_ROW_GROUP_SIZE = 122880` **主要用于存储层**，而非我之前描述的"排序时缓存局部性优化"。

```cpp
// DuckDB: src/include/duckdb/storage/storage_info.hpp
#define DEFAULT_ROW_GROUP_SIZE 122880ULL  // 约 120K 行

// 这个值主要用于:
// 1. 存储层 - RowGroupCollection 的物理分区大小
// 2. 并行扫描 - 任务划分的单位
// 3. 排序归并 - 并行分区边界 (但不是组内排序优化)
```

**DuckDB 排序的实际实现**:

```cpp
// DuckDB: src/common/sort/sort.cpp

// Sort::Finalize 中设置 partition_size
SinkFinalizeType Sort::Finalize(ClientContext &context, ...) {
    // ...
    // partition_size 用于归并阶段的并行分区
    // 注意：这是并行任务划分，不是组内排序优化
    gstate.partition_size = MinValue<idx_t>(gstate.total_count, DEFAULT_ROW_GROUP_SIZE);
}

// 排序策略:
// 1. 每个线程维护一个 SortedRun (使用归一化 key)
// 2. 线程本地排序完成后，添加到全局 sorted_runs
// 3. 最终通过 SortedRunMerger 进行 K 路归并
// 4. partition_size 用于归并时的并行分区边界计算
```

**关键区别**:

| 特性 | 我之前的理解 | DuckDB 实际实现 |
|------|------------|----------------|
| Row Group 122880 用途 | 排序时缓存优化 | 存储层物理分区 + 归并并行化 |
| 排序方式 | 组内排序 + K路归并 | 线程本地排序 + 全局K路归并 |
| 缓存优化机制 | 显式 Row Group 分区 | 归一化 Key (类似 Velox PrefixSort) |
| partition_size 作用 | 缓存局部性 | 并行任务划分边界 |

**DuckDB 的实际缓存优化**:

DuckDB 通过以下方式优化排序缓存性能：

1. **归一化 Key (Sort Key)** - 将多列排序键编码为单一可比较字节序列
2. **连续内存布局** - TupleDataCollection 保持行数据连续
3. **块迭代器** - 按块遍历数据，保持空间局部性

```cpp
// DuckDB 排序 Key 编码 (类似 Velox PrefixSort)
// src/common/types/row/tuple_data_layout.cpp
void TupleDataLayout::Initialize(const vector<BoundOrderByNode> &orders, ...) {
    // 根据 key 总宽度选择固定大小布局
    if (temp_row_width <= 8) {
        sort_key_type = SortKeyType::NO_PAYLOAD_FIXED_8;
    } else if (temp_row_width <= 16) {
        sort_key_type = SortKeyType::PAYLOAD_FIXED_16;
    }
    // ... 支持 8/16/24/32 字节固定布局和变长布局
}
```

### 1.4 Velox 当前状态

Velox 没有显式的 Row Group 概念，但有隐式的分块:

```cpp
// velox/exec/RowContainer.h
// 内存分配按 slab 进行，每个 slab 约 64KB-1MB
// 但处理时是连续遍历所有行，没有分组

// 排序时:
std::vector<char*> rows;  // 所有行指针
std::sort(rows.begin(), rows.end(), comparator);  // 一次性排序

// 聚合输出时:
for (auto row : rows) {
    extractRow(row, output);  // 逐行处理
}
```

---

## 2. 为什么需要 Row Group 分区

### 2.1 CPU 缓存层次结构

```
┌─────────────────────────────────────────────────────────────┐
│ CPU Core                                                    │
│ ┌─────────────────┐                                        │
│ │ L1 Cache        │  32-64 KB    ~1 ns     最快            │
│ │ (per core)      │                                        │
│ └─────────────────┘                                        │
│ ┌─────────────────┐                                        │
│ │ L2 Cache        │  256 KB-1 MB ~3-4 ns                   │
│ │ (per core)      │                                        │
│ └─────────────────┘                                        │
└─────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────┐
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ L3 Cache (shared)    8-64 MB   ~10-20 ns                │ │
│ └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────┐
│ Main Memory (DRAM)     GB 级     ~50-100 ns    最慢        │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 缓存失效问题

**场景: 排序 1000 万行数据**

```
数据大小: 10,000,000 行 × 64 字节/行 = 640 MB

无 Row Group:
1. 第一遍比较: Row[0] vs Row[5,000,000]
   - Row[0] 在缓存中
   - Row[5,000,000] 需要从内存加载 (~100ns)

2. 第二遍比较: Row[1] vs Row[2,500,000]
   - Row[1] 可能已被驱逐
   - Row[2,500,000] 需要从内存加载

结果: 大量随机内存访问，缓存命中率 < 10%
```

**有 Row Group (128K 行/组)**:

```
Group 大小: 128,000 行 × 64 字节 = 8 MB (适合 L3)

1. 处理 Group 0 (Row 0 ~ 127,999):
   - 整个 Group 加载到 L3 缓存
   - 组内排序: 所有比较都是缓存命中 (~10-20ns)

2. 处理 Group 1 (Row 128,000 ~ 255,999):
   - 加载新 Group 到缓存
   - 组内排序: 缓存命中

3. 最后: 归并各组结果
   - 只需比较每组的头部元素
   - 缓存友好的顺序访问

结果: 缓存命中率 > 80%
```

### 2.3 理论分析

**排序复杂度对比**:

| 方法 | 比较次数 | 缓存失效次数 | 实际时间 |
|------|----------|--------------|----------|
| 全局快排 | O(N log N) | O(N log N) 随机 | 慢 |
| 分组归并 | O(N log N) | O(N/B × log(N/B)) 顺序 | 快 |

其中 B = Row Group 大小，顺序访问比随机访问快 10-50 倍。

---

## 3. 潜在收益分析

### 3.1 排序场景

**基准测试设计**:

```cpp
// 测试场景: 排序不同大小的数据集
struct TestRow {
    int64_t key;
    char payload[56];  // 总共 64 字节
};

// 测试变量:
// - 数据集大小: 100K, 1M, 10M, 100M 行
// - Row Group 大小: 无, 64K, 128K, 256K 行
```

**预期性能数据** (基于缓存模型估算):

| 数据集大小 | 无 Row Group | 128K Row Group | 提升 |
|------------|--------------|----------------|------|
| 100K 行 (6.4 MB) | 15 ms | 16 ms | -6% (开销) |
| 1M 行 (64 MB) | 180 ms | 165 ms | +8% |
| 10M 行 (640 MB) | 2.5 s | 2.1 s | +16% |
| 100M 行 (6.4 GB) | 35 s | 28 s | +20% |

**关键观察**:
- 数据集 < L3 缓存: Row Group 可能带来负优化
- 数据集 > L3 缓存: Row Group 收益显著
- 数据集 >> 内存: Row Group + 外部排序效果最佳

### 3.2 聚合场景

**HashAggregation 输出**:

```cpp
// 当前实现: 遍历所有 group
for (auto& entry : hashTable) {
    extractAggregates(entry, output);
}

// Row Group 优化: 分批输出
for (size_t i = 0; i < numGroups; i += kRowGroupSize) {
    size_t end = std::min(i + kRowGroupSize, numGroups);
    extractBatch(i, end, output);
}
```

**预期收益**:

| 聚合类型 | 当前 | Row Group | 提升 |
|----------|------|-----------|------|
| 低基数 (1K groups) | 基准 | 无变化 | 0% |
| 中基数 (100K groups) | 基准 | -3% ~ +5% | 边际 |
| 高基数 (10M groups) | 基准 | +10-15% | 显著 |

### 3.3 Join 场景

**Hash Join Probe**:

```cpp
// 当前: 逐行 probe
for (auto& row : probeRows) {
    auto* match = hashTable.find(row.key);
    if (match) emit(row, match);
}

// Row Group 优化: 批量 probe + prefetch
for (size_t i = 0; i < probeRows.size(); i += kRowGroupSize) {
    // 1. Prefetch 下一组的 hash 槽位
    for (size_t j = i; j < std::min(i + kRowGroupSize, n); j++) {
        prefetch(hashTable.slot(hash(probeRows[j].key)));
    }

    // 2. 处理当前组
    for (size_t j = i; j < std::min(i + kRowGroupSize, n); j++) {
        processProbe(probeRows[j]);
    }
}
```

**预期收益**: 5-20% (取决于 hash table 大小和访问模式)

---

## 4. 代价分析

### 4.1 代码复杂度

**当前代码 (简洁)**:

```cpp
void SortBuffer::noMoreInput() {
    // 简单: 一次性排序
    std::sort(rows_.begin(), rows_.end(), comparator_);
}

void SortBuffer::getOutput(RowVectorPtr* result) {
    // 简单: 顺序输出
    for (int i = outputIndex_; i < rows_.size() && i < batchSize; ++i) {
        extractRow(rows_[i], result);
    }
}
```

**Row Group 代码 (复杂)**:

```cpp
void SortBuffer::noMoreInput() {
    // 复杂: 分组排序 + 归并

    // 1. 分组
    std::vector<std::vector<char*>> groups;
    for (size_t i = 0; i < rows_.size(); i += kRowGroupSize) {
        groups.emplace_back(
            rows_.begin() + i,
            rows_.begin() + std::min(i + kRowGroupSize, rows_.size()));
    }

    // 2. 组内排序 (可并行)
    for (auto& group : groups) {
        std::sort(group.begin(), group.end(), comparator_);
    }

    // 3. K 路归并
    sortedRows_ = kWayMerge(groups, comparator_);
}
```

**复杂度增加**:
- 额外的分组逻辑
- K 路归并实现
- 内存管理更复杂
- 调试难度增加

### 4.2 内存开销

```cpp
// 无 Row Group:
std::vector<char*> rows;  // N 个指针

// 有 Row Group:
std::vector<std::vector<char*>> groups;  // ceil(N/G) 个 vector
// 或
std::vector<char*> rows;
std::vector<size_t> groupBoundaries;  // ceil(N/G) 个边界

// 额外开销: O(N/G) 个边界值
// G = 128K 时, N = 10M -> 额外 78 个 size_t = 624 字节
// 开销可忽略
```

### 4.3 小数据集退化

**问题**: 小数据集上 Row Group 可能负优化

```cpp
// 数据集: 10,000 行 (640 KB, 完全在 L3 中)

// 无 Row Group:
std::sort(rows, rows + 10000, cmp);  // 直接排序

// 有 Row Group (G = 128K):
// 只有 1 个 Group，等同于无 Row Group
// 但有额外的分组检查开销

// 有 Row Group (G = 1K):
// 10 个 Group
// 组内排序 + 归并
// 归并开销 > 缓存收益
```

**解决方案**: 动态阈值

```cpp
void SortBuffer::noMoreInput() {
    // 动态决策
    if (rows_.size() < kRowGroupThreshold) {
        // 小数据集: 直接排序
        std::sort(rows_.begin(), rows_.end(), comparator_);
    } else {
        // 大数据集: Row Group 分区排序
        sortWithRowGroups();
    }
}

// 阈值计算
constexpr size_t kL3CacheSize = 8 * 1024 * 1024;  // 8 MB
constexpr size_t kRowGroupThreshold = kL3CacheSize / kEstimatedRowSize;
```

### 4.4 并行化复杂性

**无 Row Group 并行**:

```cpp
// 使用并行排序库
std::execution::par_unseq;
std::sort(std::execution::par, rows.begin(), rows.end(), cmp);
```

**Row Group 并行**:

```cpp
// 1. 并行组内排序
#pragma omp parallel for
for (size_t g = 0; g < numGroups; g++) {
    std::sort(groups[g].begin(), groups[g].end(), cmp);
}

// 2. 归并 (难以完全并行化)
// K 路归并本质上是顺序操作
// 可以用并行归并树，但实现复杂
```

**复杂性增加**: 需要处理线程同步、负载均衡

---

## 5. 不同场景的适用性

### 5.1 适用场景

| 场景 | 数据特征 | Row Group 收益 | 推荐 |
|------|----------|----------------|------|
| OLAP 大表排序 | 10M+ 行 | +15-25% | ✅ 强烈推荐 |
| 高基数聚合输出 | 1M+ groups | +10-15% | ✅ 推荐 |
| 大表 Hash Join | 10M+ 行 build | +5-15% | ⚠️ 评估 |
| 外部排序 | 超过内存 | +20-30% | ✅ 必需 |

### 5.2 不适用场景

| 场景 | 数据特征 | Row Group 影响 | 推荐 |
|------|----------|----------------|------|
| 小表排序 | < 100K 行 | -5% ~ 0% | ❌ 不推荐 |
| 低基数聚合 | < 10K groups | 0% | ❌ 不需要 |
| 内存充足的 Join | 数据在缓存中 | -2% ~ +2% | ❌ 不需要 |
| 流式处理 | 逐批处理 | N/A | ❌ 不适用 |

### 5.3 决策树

```
                    数据集大小
                        │
           ┌────────────┼────────────┐
           │            │            │
        < 100K      100K ~ 1M      > 1M
           │            │            │
      不使用 RG    评估数据访问    使用 RG
                    模式
                        │
              ┌─────────┼─────────┐
              │         │         │
          顺序访问   混合访问   随机访问
              │         │         │
         不使用 RG   可选 RG    使用 RG
```

---

## 6. 实施建议

### 6.1 推荐实现方案

**方案: 可配置的 Row Group 支持**

```cpp
// velox/exec/RowGroupConfig.h
struct RowGroupConfig {
    // 是否启用 Row Group 分区
    bool enabled = false;

    // Row Group 大小 (行数)
    size_t groupSize = 128 * 1024;  // 128K 行

    // 自动启用阈值 (行数)
    size_t autoEnableThreshold = 500 * 1024;  // 500K 行

    // 是否并行处理组
    bool parallelGroups = true;
};

// 全局配置
DECLARE_bool(velox_enable_row_group_partitioning);
DECLARE_int32(velox_row_group_size);
DECLARE_int32(velox_row_group_auto_threshold);
```

**排序实现**:

```cpp
// velox/exec/SortBuffer.cpp
void SortBuffer::noMoreInput() {
    if (!shouldUseRowGroups()) {
        // 传统排序
        PrefixSort::sort(data_.get(), compareFlags_, config_, pool_, rows_);
        return;
    }

    // Row Group 分区排序
    sortWithRowGroups();
}

bool SortBuffer::shouldUseRowGroups() const {
    if (!FLAGS_velox_enable_row_group_partitioning) {
        return false;
    }
    return rows_.size() >= FLAGS_velox_row_group_auto_threshold;
}

void SortBuffer::sortWithRowGroups() {
    const size_t groupSize = FLAGS_velox_row_group_size;
    const size_t numGroups = (rows_.size() + groupSize - 1) / groupSize;

    // 1. 创建组边界
    std::vector<std::pair<size_t, size_t>> groupRanges;
    for (size_t i = 0; i < rows_.size(); i += groupSize) {
        groupRanges.emplace_back(i, std::min(i + groupSize, rows_.size()));
    }

    // 2. 并行组内排序
    std::vector<std::future<void>> futures;
    for (const auto& [start, end] : groupRanges) {
        futures.push_back(std::async(std::launch::async, [&, start, end] {
            PrefixSort::sort(
                data_.get(),
                compareFlags_,
                config_,
                pool_,
                rows_.data() + start,
                end - start);
        }));
    }
    for (auto& f : futures) f.wait();

    // 3. K 路归并
    mergeGroups(groupRanges);
}

void SortBuffer::mergeGroups(
    const std::vector<std::pair<size_t, size_t>>& groupRanges) {

    // 使用优先队列实现 K 路归并
    using Element = std::pair<char*, size_t>;  // (row, groupIndex)
    auto cmp = [this](const Element& a, const Element& b) {
        return data_->compare(a.first, b.first, compareFlags_) > 0;
    };
    std::priority_queue<Element, std::vector<Element>, decltype(cmp)> pq(cmp);

    // 初始化: 每组第一个元素入队
    std::vector<size_t> groupPos(groupRanges.size());
    for (size_t g = 0; g < groupRanges.size(); g++) {
        pq.push({rows_[groupRanges[g].first], g});
        groupPos[g] = groupRanges[g].first + 1;
    }

    // 归并
    std::vector<char*> merged;
    merged.reserve(rows_.size());

    while (!pq.empty()) {
        auto [row, g] = pq.top();
        pq.pop();
        merged.push_back(row);

        if (groupPos[g] < groupRanges[g].second) {
            pq.push({rows_[groupPos[g]], g});
            groupPos[g]++;
        }
    }

    rows_ = std::move(merged);
}
```

### 6.2 渐进式实施路径

```
Phase 1: 基础支持
├── 添加配置开关
├── 实现 SortBuffer 的 Row Group 排序
└── 添加基准测试

Phase 2: 扩展应用
├── 聚合输出分批
├── Join probe 分批
└── 性能调优

Phase 3: 自动化
├── 基于数据统计自动决策
├── 自适应 Group 大小
└── 与执行计划优化器集成
```

### 6.3 测试计划

```cpp
// 基准测试
TEST_F(SortBufferTest, rowGroupSortPerformance) {
    std::vector<size_t> dataSizes = {
        100'000,     // 应该不使用 RG
        1'000'000,   // 边界情况
        10'000'000,  // 应该使用 RG
    };

    for (auto size : dataSizes) {
        auto data = generateRandomData(size);

        // 无 Row Group
        auto t1 = benchmark([&] { sortWithoutRowGroup(data); });

        // 有 Row Group
        auto t2 = benchmark([&] { sortWithRowGroup(data); });

        LOG(INFO) << "Size: " << size
                  << " NoRG: " << t1 << "ms"
                  << " RG: " << t2 << "ms"
                  << " Speedup: " << (t1 / t2 - 1) * 100 << "%";
    }
}

// 正确性测试
TEST_F(SortBufferTest, rowGroupSortCorrectness) {
    auto data = generateRandomData(1'000'000);
    auto expected = sortWithoutRowGroup(data);
    auto actual = sortWithRowGroup(data);
    EXPECT_EQ(expected, actual);
}
```

---

## 7. 结论 (更新)

### 7.1 工业实践验证结果

经过对 DuckDB 和 DataFusion 源代码的详细分析：

| 系统 | Row Group 概念 | 用于排序缓存优化? | 实际缓存优化方式 |
|------|---------------|-----------------|----------------|
| DuckDB | 122,880 行 | ❌ 否 (用于存储层) | 归一化 Key + 连续内存 |
| DataFusion | batch_size 8192 | ❌ 否 (用于内存管理) | Row 格式 + 流式处理 |
| Velox | 无显式 Row Group | N/A | PrefixSort + KeyEncoder |

**关键发现**: 我之前描述的"显式 Row Group 分区排序用于缓存优化"在主流系统中**并未被实现**。

### 7.2 为什么主流系统没有实现?

1. **归一化 Key 已足够**: PrefixSort/SortKey 编码使得比较操作在连续内存上进行
2. **现代排序算法**: pdqsort/timsort 等已针对缓存优化
3. **并行化更重要**: 线程本地排序 + K路归并的并行策略收益更大
4. **复杂度 vs 收益**: 显式分组增加复杂度，但边际收益不明显

### 7.3 修正后的建议

| 因素 | 评估 |
|------|------|
| 工业验证 | ❌ DuckDB/DataFusion 均未实现 |
| 理论收益 | ⚠️ 存在但未被验证 (5-15%) |
| 实现复杂度 | ⚠️ 中等增加 |
| Velox 现状 | ✅ 已有 PrefixSort + KeyEncoder |

**修正后的建议**:

1. **暂不实现显式 Row Group 分区** - 与主流系统对齐
2. **继续优化现有 PrefixSort** - 这是经过验证的缓存优化方式
3. **如需进一步优化**:
   - 考虑并行排序策略 (类似 DuckDB 的多线程 SortedRun)
   - 考虑外部排序优化 (大数据集 spill 场景)

### 7.4 原文档理论分析的价值

虽然主流系统未实现显式 Row Group 分区，但原文档中的理论分析仍有参考价值：

1. **缓存层次分析** - 理解 L1/L2/L3 缓存对性能的影响
2. **数据局部性原理** - 设计算法时的重要考量
3. **批处理思想** - 在其他场景 (如聚合输出、Join probe) 可能适用

**保留此文档作为理论参考，但实施优先级应降低。**

---

## 附录: 参考资料

### 源代码验证 (2026-01)

**DuckDB 排序实现**:
- `src/common/sort/sort.cpp` - Sort::Finalize 使用 DEFAULT_ROW_GROUP_SIZE 作为 partition_size
- `src/common/sort/sorted_run_merger.cpp` - K路归并实现
- `src/include/duckdb/storage/storage_info.hpp` - DEFAULT_ROW_GROUP_SIZE = 122880 定义

**DataFusion 排序实现**:
- `datafusion/physical-plan/src/sorts/sort.rs` - ExternalSorter 三种策略
- `datafusion/physical-plan/src/sorts/streaming_merge.rs` - K路归并

### 学术论文
- "Cache-Conscious Data Structures" - Frigo et al.
- "AlphaSort: A RISC Machine Sort" - Nyberg et al.

### 工业实现 (修正)
- DuckDB: `DEFAULT_ROW_GROUP_SIZE = 122880` - **用于存储层，非排序缓存优化**
- DataFusion: `batch_size = 8192` - **用于内存管理，非缓存优化**
- ClickHouse: 隐式块处理
- Apache Arrow: RecordBatch 分批

### Velox 相关代码
- SortBuffer: `velox/exec/SortBuffer.h`
- PrefixSort: `velox/exec/PrefixSort.h`
- RowContainer: `velox/exec/RowContainer.h`
