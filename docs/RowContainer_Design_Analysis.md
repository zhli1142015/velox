# RowContainer Row 结构设计与操作分析

## 目录
1. [Row 内存布局设计](#1-row-内存布局设计)
2. [RowColumn 元数据设计](#2-rowcolumn-元数据设计)
3. [RowContainer 核心 API](#3-rowcontainer-核心-api)
4. [不同算子的使用模式](#4-不同算子的使用模式)
5. [性能优化建议](#5-性能优化建议)

---

## 1. Row 内存布局设计

### 1.1 整体布局

每一行（Row）是一块连续的内存，布局如下：

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Row Memory Layout                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│  [Normalized Key]  │  Keys  │  Null Flags  │  Accumulators  │  Dependents  │ │
│    (optional)      │        │              │                │              │ │
│    8 bytes         │ var    │  flagBytes_  │    variable    │   variable   │ │
├─────────────────────────────────────────────────────────────────────────────┤
│  [Row Size]  │  [Next Pointer]  │                                           │
│  (optional)  │   (optional)     │                                           │
│   4 bytes    │    8 bytes       │                                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 详细字段说明

| 字段 | 大小 | 条件 | 描述 |
|------|------|------|------|
| **Normalized Key** | 8 bytes | `hasNormalizedKeys_=true` | 存储在 row 指针的 `-1` 位置，用于快速比较 |
| **Key Columns** | 变长 | 总是存在 | 固定宽度：按 `typeKindSize(type)` 计算 |
| **Null Flags** | `flagBytes_` | 总是存在 | 每个字段一个 bit，包括 accumulator 的初始化标志 |
| **Accumulators** | 变长 | 有聚合时 | 每个 accumulator 按其 `alignment_` 对齐 |
| **Dependent Columns** | 变长 | Join/OrderBy | 非 key 列的值存储 |
| **Row Size** | 4 bytes | 有变长字段时 | `rowSizeOffset_`，追踪变长数据大小 |
| **Next Pointer** | 8 bytes | `hasNext=true` | Hash Join 中指向相同 key 的下一行 |

### 1.3 Null 标志位布局

```cpp
// 标志位结构
┌────────────────────────────────────────────────────────────────────┐
│  Key Nulls  │ Acc Null+Init │ Dependent Nulls │ Probed │  Free    │
│  (if nullable)│  (pairs)     │                 │  Flag  │  Flag    │
└────────────────────────────────────────────────────────────────────┘

// Accumulator 特殊处理: null 和 initialized 标志交替出现
// 保证在同一个字节内，便于原子操作
flagOffset = (flagOffset + 7) & -8;  // 对齐到字节边界
for (accumulator : accumulators) {
    nullOffsets_.push_back(flagOffset);      // null bit
    flagOffset += kNumAccumulatorFlags;       // +2 (null + initialized)
}
```

### 1.4 对齐策略

```cpp
// 行对齐计算
fixedRowSize_ = bits::roundUp(offset, alignment_);

// Accumulator 对齐
for (const auto& accumulator : accumulators) {
    offset = bits::roundUp(offset, accumulator.alignment());
    offsets_.push_back(offset);
    offset += accumulator.fixedWidthSize();
}

// 整体对齐要求
alignment_ = combineAlignments(accumulator.alignment(), alignment_);
// combineAlignments 返回两个对齐值的最大值
```

### 1.5 变长数据存储

变长数据（VARCHAR, ARRAY, MAP, ROW）使用间接引用：

```cpp
// VARCHAR/VARBINARY: 使用 StringView（16 bytes）
// - 短字符串内联存储（<= 12 bytes，使用 kInlineSize）
// - 长字符串通过 HashStringAllocator 分配

// Complex types (ARRAY/MAP/ROW): 同样使用 StringView（16 bytes）作为 HashRowType
// - 实际存储时通过 ContainerRowSerde 序列化到 HashStringAllocator

struct StringView {
    // 16 bytes total (sizeof(StringView) == 16)
    uint32_t size_;       // 4 bytes: 字符串长度
    char prefix_[4];      // 4 bytes: 字符串前4字节（用于快速比较）
    union {
        char inlined[8];  // 8 bytes: 内联字符串的剩余部分（if size <= 12）
        const char* data; // 8 bytes: 外部数据指针（if size > 12）
    } value_;
    // kInlineSize = 12: prefix_[4] + inlined[8]
};
```

---

## 2. RowColumn 元数据设计

### 2.1 RowColumn 结构

```cpp
class RowColumn {
    // 打包的偏移量信息（8 bytes）
    // 高 32 位: offset（字段在 row 中的偏移）
    // 低 32 位: nullByte (24 bits) + nullMask (8 bits)
    const uint64_t packedOffsets_;

    // 访问方法
    int32_t offset() const { return packedOffsets_ >> 32; }
    int32_t nullByte() const { return static_cast<uint32_t>(packedOffsets_) >> 8; }
    uint8_t nullMask() const { return packedOffsets_ & 0xff; }
};
```

### 2.2 Null 检查优化

```cpp
// 非空列的特殊处理
static constexpr int32_t kNotNullOffset = -1;

static uint64_t PackOffsets(int32_t offset, int32_t nullOffset) {
    if (nullOffset == kNotNullOffset) {
        // nullMask = 0, 任何 AND 操作都返回 false
        return static_cast<uint64_t>(offset) << 32;
    }
    return (1UL << (nullOffset & 7)) |      // nullMask
           ((nullOffset & ~7UL) << 5) |      // nullByte
           static_cast<uint64_t>(offset) << 32;
}
```

### 2.3 列统计信息

```cpp
class RowColumn::Stats {
    int32_t minBytes_{0};      // 最小值大小
    int32_t maxBytes_{0};      // 最大值大小
    uint64_t sumBytes_{0};     // 总字节数
    uint32_t nonNullCount_{0}; // 非空行数
    uint32_t nullCount_{0};    // 空行数
    bool minMaxStatsValid_{true};
};
```

---

## 3. RowContainer 核心 API

### 3.1 行生命周期管理

| API | 功能 | 使用场景 |
|-----|------|----------|
| `newRow()` | 分配新行 | 所有写入场景 |
| `initializeRow(row, reuse)` | 初始化行 | 重用已有行时 |
| `eraseRows(rows)` | 删除行并回收内存 | 清理数据 |
| `setAllNull(row)` | 将所有字段设为 null | 特殊聚合场景 |

```cpp
char* RowContainer::newRow() {
    ++numRows_;
    char* row;
    if (firstFreeRow_) {
        // 从 free list 重用
        row = firstFreeRow_;
        firstFreeRow_ = nextFree(row);
        --numFreeRows_;
    } else {
        // 新分配
        row = rows_.allocateFixed(fixedRowSize_ + normalizedKeySize_, alignment_)
              + normalizedKeySize_;
        if (normalizedKeySize_) {
            ++numRowsWithNormalizedKey_;
        }
        if (useListRowIndex_) {
            rowPointers_.push_back(row);
        }
    }
    return initializeRow(row, false);
}
```

### 3.2 数据存储 API

| API | 功能 | 性能特点 |
|-----|------|----------|
| `store(decoded, rowIndex, row, col)` | 存储单个值 | 逐行存储 |
| `store(decoded, rows, col)` | 批量存储 | 向量化，更高效 |
| `storeSerializedRow(vector, index, row)` | 反序列化存储 | Spill 恢复 |

```cpp
void RowContainer::store(
    const DecodedVector& decoded,
    vector_size_t rowIndex,
    char* row,
    int32_t columnIndex) {
    bool isKey = columnIndex < keyTypes_.size();
    if (isKey && !nullableKeys_) {
        // 快速路径：key 不可为空
        VELOX_DYNAMIC_TYPE_DISPATCH(storeNoNulls, ...);
    } else {
        // 需要处理 null
        VELOX_DYNAMIC_TYPE_DISPATCH_ALL(storeWithNulls, ...);
    }
    updateColumnStats(decoded, rowIndex, row, columnIndex);
}
```

### 3.3 数据提取 API

| API | 功能 | 使用场景 |
|-----|------|----------|
| `extractColumn(rows, numRows, col, result)` | 提取列到 Vector | 输出结果 |
| `extractColumn(rows, rowNumbers, col, result)` | 按索引提取 | 稀疏访问 |
| `extractSerializedRows(rows, result)` | 序列化整行 | Spill |
| `extractNulls(rows, numRows, col, result)` | 提取 null bitmap | 特殊需求 |

### 3.4 行遍历 API

| API | 功能 | 性能特点 |
|-----|------|----------|
| `listRows(iter, maxRows, maxBytes, rows)` | 标准遍历 | 检查 free/probe 标志 |
| `listRowsFast(iter, maxRows, rows)` | 快速遍历 | 使用预存指针数组 |
| `listPartitionRows(...)` | 按分区遍历 | 并行 Join build |

```cpp
// 标准遍历：需要扫描内存块，检查标志
template <ProbeType probeType>
int32_t listRows(RowContainerIterator* iter, ...) {
    for (auto i = iter->allocationIndex; i < numAllocations; ++i) {
        auto range = rows_.rangeAt(i);
        while (row + rowSize <= limit) {
            rows[count++] = data + row + normalizedKeyOffset;
            // 检查 free 标志
            if (bits::isBitSet(rows[count - 1], freeFlagOffset_)) {
                --count;
                continue;
            }
            // 检查 probed 标志
            if constexpr (probeType == ProbeType::kNotProbed) {
                if (bits::isBitSet(rows[count - 1], probedFlagOffset_)) {
                    --count;
                    continue;
                }
            }
            ...
        }
    }
}

// 快速遍历：直接使用指针数组
int32_t listRowsFast(RowContainerIterator* iter, int32_t maxRows, char** rows) {
    while (count < maxRows && iter->listRowCursor < rowPointers_.size()) {
        rows[count++] = rowPointers_[iter->listRowCursor++];
    }
    return count;
}
```

### 3.5 比较 API

| API | 功能 | 使用场景 |
|-----|------|----------|
| `compare(row, col, decoded, index, flags)` | Row vs Vector | Hash 探测 |
| `compare(left, right, colIndex, flags)` | Row vs Row | 排序 |
| `compareRows(left, right, flags)` | 多列比较 | Order By |

```cpp
template <bool mayHaveNulls, TypeKind Kind>
int compare(const char* row, RowColumn column,
            const DecodedVector& decoded, vector_size_t index,
            CompareFlags flags) const {
    if constexpr (mayHaveNulls) {
        bool rowIsNull = isNullAt(row, column.nullByte(), column.nullMask());
        bool indexIsNull = decoded.isNullAt(index);
        if (rowIsNull) {
            return indexIsNull ? 0 : flags.nullsFirst ? -1 : 1;
        }
        if (indexIsNull) {
            return flags.nullsFirst ? 1 : -1;
        }
    }
    // 类型特定比较
    auto left = valueAt<T>(row, column.offset());
    auto right = decoded.valueAt<T>(index);
    return SimpleVector<T>::comparePrimitiveAsc(left, right);
}
```

### 3.6 Hash 计算 API

```cpp
void hash(int32_t columnIndex, folly::Range<char**> rows,
          bool mix, uint64_t* result) const;

// 用于计算行的 hash 值，支持增量 hash（mix=true）
```

### 3.7 Join 特定 API

| API | 功能 |
|-----|------|
| `setProbedFlag(rows, numRows)` | 设置 probed 标志 |
| `extractProbedFlags(rows, numRows, ...)` | 提取 probed 状态 |
| `appendNextRow(current, nextRow)` | 链接重复 key 的行 |
| `createRowPartitions(pool)` | 创建分区信息 |

---

## 4. 不同算子的使用模式

### 4.1 Hash Aggregation (GroupingSet)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Hash Aggregation Flow                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────┐    ┌──────────────┐    ┌─────────────────────┐               │
│  │  Input   │───>│  HashTable   │───>│   RowContainer      │               │
│  │  Batch   │    │  groupProbe  │    │ (keys + accumulators)│              │
│  └──────────┘    └──────────────┘    └─────────────────────┘               │
│                         │                       │                           │
│                         │ lookup.hits           │ rows->newRow()           │
│                         │ lookup.newGroups      │ rows->store()            │
│                         ▼                       ▼                           │
│                  ┌──────────────┐    ┌─────────────────────┐               │
│                  │  Aggregate   │───>│  extractColumn()    │               │
│                  │  Functions   │    │  for output         │               │
│                  └──────────────┘    └─────────────────────┘               │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Row 结构**：
```
[Keys] [Null Flags] [Accumulators] [Row Size]
```

**关键操作**：

```cpp
// GroupingSet.cpp
void GroupingSet::createHashTable() {
    table_ = HashTable<...>::createForAggregation(
        std::move(hashers_),
        accumulators(false),  // 创建 accumulator 布局
        pool_);
    RowContainer& rows = *table_->rows();
    initializeAggregates(aggregates_, rows, false);
}

// HashTable.cpp - groupProbe 时
char* HashTable::insertEntry(HashLookup& lookup, uint64_t index, vector_size_t row) {
    char* group = rows_->newRow();       // 分配新行
    lookup.hits[row] = group;
    storeKeys(lookup, row);               // 存储 key 列
    storeRowPointer(index, lookup.hashes[row], group);
    return group;
}

// 输出时
table_->rows()->listRows(&iter, maxRows, groups.data());
table_->rows()->extractColumn(groups, colIndex, result);
```

### 4.2 Hash Join Build (HashBuild)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Hash Join Build Flow                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────┐    ┌──────────────────────────────────────────────────────┐  │
│  │  Build   │───>│                  RowContainer                          │  │
│  │  Input   │    │  [Keys] [Dependents] [Probed] [Free] [Row Size] [Next] │  │
│  └──────────┘    └──────────────────────────────────────────────────────┘  │
│        │                              │                                     │
│        │ VectorHasher                 │ rows->newRow()                     │
│        │ analyze keys                 │ rows->store(keys)                  │
│        ▼                              │ rows->store(dependents)             │
│  ┌──────────────┐                     │ rows->setProbedFlag()              │
│  │  HashTable   │<────────────────────┘                                    │
│  │  insertion   │                                                           │
│  └──────────────┘                                                           │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Row 结构**：
```
[Keys] [Dependents] [Null Flags] [Probed Flag] [Free Flag] [Row Size] [Next Ptr]
```

**关键操作**：

```cpp
// HashBuild.cpp
void HashBuild::addInput() {
    auto rows = table_->rows();
    auto nextOffset = rows->nextOffset();

    activeRows_.applyToSelected([&](auto rowIndex) {
        char* newRow = rows->newRow();

        // 初始化 next 指针
        if (nextOffset) {
            *reinterpret_cast<char**>(newRow + nextOffset) = nullptr;
        }

        // 存储 key 列
        for (auto i = 0; i < hashers.size(); ++i) {
            rows->store(hashers[i]->decodedVector(), rowIndex, newRow, i);
        }

        // 存储 dependent 列
        for (auto i = 0; i < dependentChannels_.size(); ++i) {
            rows->store(*decoders_[i], rowIndex, newRow, i + hashers.size());
        }
    });
}
```

### 4.3 Hash Join Probe (HashProbe)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Hash Join Probe Flow                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────┐    ┌──────────────┐    ┌─────────────────────────────────┐   │
│  │  Probe   │───>│  HashTable   │───>│      Build RowContainer         │   │
│  │  Input   │    │  joinProbe() │    │  table->extractColumn(hits, ..) │   │
│  └──────────┘    └──────────────┘    └─────────────────────────────────┘   │
│        │                │                          │                        │
│        │                │ lookup.hits              │ extractColumn()        │
│        ▼                ▼                          ▼                        │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                    Output (Probe columns + Build columns)              │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  Right/Full Join additional:                                                │
│  ┌─────────────────────────────────────────────────────────────────┐       │
│  │  listNotProbedRows() / listProbedRows() / rows->setProbedFlag()  │       │
│  └─────────────────────────────────────────────────────────────────┘       │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**关键操作**：

```cpp
// HashProbe.cpp
void extractColumns(
    BaseHashTable* table,
    folly::Range<char* const*> rows,
    folly::Range<const IdentityProjection*> projections,
    ...) {
    for (auto projection : projections) {
        child->resize(rows.size());
        table->extractColumn(rows, projection.inputChannel, child);
    }
}

// Right/Full Join: 遍历未匹配的 build 行
table->listNotProbedRows(&iter, maxRows, maxBytes, rows);
```

### 4.4 Sort / Order By (SortBuffer)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Sort Buffer Flow                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────┐    ┌─────────────────────────────────────────────────────┐   │
│  │  Input   │───>│              RowContainer (data_)                     │   │
│  │  Batch   │    │  [Sort Columns First] [Other Columns] [Row Size]      │   │
│  └──────────┘    └─────────────────────────────────────────────────────┘   │
│        │                              │                                     │
│        │                              │ data_->newRow()                    │
│        │                              │ data_->store()                     │
│        ▼                              ▼                                     │
│  ┌──────────────┐    ┌──────────────────────────────────────────────────┐  │
│  │  noMoreInput │───>│  sortedRows_ = listRows()                         │  │
│  │              │    │  PrefixSort::sort(data_, sortedRows_)             │  │
│  └──────────────┘    └──────────────────────────────────────────────────┘  │
│                                              │                              │
│                                              │ RowComparator                │
│                                              │ data_->compare()             │
│                                              ▼                              │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  getOutput(): data_->extractColumn(sortedRows_[offset:], ...)          │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Row 结构**：
```
[Sort Key Columns] [Non-Sort Columns] [Null Flags] [Row Size]
```

**特点**：使用 `useListRowIndex_=true` 启用 `listRowsFast()`

**关键操作**：

```cpp
// SortBuffer.cpp
void SortBuffer::addInput(const VectorPtr& input) {
    std::vector<char*> rows(input->size());
    for (int row = 0; row < input->size(); ++row) {
        rows[row] = data_->newRow();
    }

    for (const auto& columnProjection : columnMap_) {
        DecodedVector decoded(*inputRow->childAt(columnProjection.outputChannel), allRows);
        data_->store(decoded, folly::Range(rows.data(), input->size()),
                     columnProjection.inputChannel);
    }
}

void SortBuffer::noMoreInput() {
    sortedRows_.resize(numInputRows_);
    RowContainerIterator iter;
    data_->listRows(&iter, numInputRows_, sortedRows_.data());

    // 排序指针数组，不移动实际数据
    PrefixSort::sort(data_.get(), sortCompareFlags_, prefixSortConfig_,
                     pool_, sortedRows_);
}

RowVectorPtr SortBuffer::getOutput(vector_size_t maxOutputRows) {
    for (int i = 0; i < numResultColumns; ++i) {
        data_->extractColumn(
            sortedRows_.data() + numOutputRows_,
            batchSize,
            i,
            resultVectors[i]);
    }
}
```

### 4.5 TopN

```cpp
// TopN.cpp - 维护一个大小为 N 的堆
void TopN::addInput() {
    for (int row = 0; row < numRows; ++row) {
        char* newRow = data_->newRow();
        for (auto col = 0; col < numColumns; ++col) {
            data_->store(decodedVectors_[col], row, newRow, col);
        }

        // 如果堆满了，比较并可能替换
        if (rows_.size() >= count_) {
            if (comparator_(newRow, rows_.front())) {
                // 替换堆顶
                data_->store(decodedVectors_[col], row, rows_.front(), col);
            }
        } else {
            rows_.push_back(newRow);
        }
    }
}
```

---

## 5. 性能优化建议

### 5.1 当前设计的性能特点

| 特点 | 优势 | 劣势 |
|------|------|------|
| Row-wise 存储 | 缓存友好的行访问 | 列扫描效率低 |
| 内联变长数据 | 小字符串零拷贝 | 大字符串需要间接访问 |
| Free list 重用 | 减少分配开销 | 内存碎片化 |
| Normalized key | 快速比较 | 额外 8 字节/行 |
| 指针数组 (listRowsFast) | O(1) 随机访问 | 额外内存开销 |

### 5.2 潜在优化方向

#### 5.2.1 列式布局优化

```
当前 (Row-wise):
Row0: [key0][key1][acc0][dep0]
Row1: [key0][key1][acc0][dep0]
Row2: [key0][key1][acc0][dep0]

优化 (Column-wise for hot columns):
Keys:   [key0_0, key0_1, key0_2, ...] [key1_0, key1_1, key1_2, ...]
Values: Row-wise for accumulators and dependents
```

**适用场景**：Hash 探测时只需要比较 key，列式存储更缓存友好

#### 5.2.2 压缩编码

```cpp
// Dictionary encoding for low-cardinality columns
struct EncodedRow {
    uint16_t key_dict_id;  // Instead of full StringView
    ...
};

// Delta encoding for sorted data
struct DeltaEncodedRow {
    int32_t delta_from_previous;
    ...
};
```

#### 5.2.3 SIMD 批量操作

```cpp
// 批量 null 检查
uint64_t nullBits = extractNullBits(rows, column);
uint64_t nonNullMask = ~nullBits;

// 批量值提取
simd::gather(basePtr, offsets, values);
```

#### 5.2.4 内存池化优化

```cpp
// 按大小分类的 slab 分配器
class SlabAllocator {
    // Small rows: < 64 bytes
    // Medium rows: 64-256 bytes
    // Large rows: > 256 bytes
};
```

#### 5.2.5 Prefetch 优化

```cpp
// 在遍历前预取下一批行
for (int i = 0; i < numRows; i += prefetchBatch) {
    for (int j = 0; j < prefetchBatch && i+j+prefetchAhead < numRows; ++j) {
        __builtin_prefetch(rows[i + j + prefetchAhead]);
    }
    // Process current batch
}
```

### 5.3 API 使用建议

| 场景 | 推荐做法 | 原因 |
|------|----------|------|
| 批量写入 | `store(decoded, rows, col)` | 向量化处理 |
| Sort 遍历 | `listRowsFast()` | 避免标志检查 |
| 稀疏访问 | `extractColumn(rows, rowNumbers, ...)` | 减少中间 Vector |
| Spill/Restore | `extractSerializedRows` / `storeSerializedRow` | 一次序列化整行 |

### 5.4 算子特定优化

| 算子 | 当前热点 | 优化建议 |
|------|----------|----------|
| **Hash Agg** | `store()` 逐列存储 | 列式 key 存储，批量 accumulator 更新 |
| **Hash Join Build** | `store()` + `next` 链表 | 预分配批量行，减少分配次数 |
| **Hash Join Probe** | `compare()` 逐 key 比较 | Normalized key 覆盖更多 key 类型 |
| **Sort** | `compare()` 逐对比较 | PrefixSort 已优化，考虑 SIMD 比较 |
| **TopN** | 堆操作 + `store()` 覆盖 | 避免不必要的 store（只比较不写入） |

---

## 6. 关键数据结构总结

### 6.1 RowContainer 成员变量

```cpp
class RowContainer {
    // 类型信息
    const std::vector<TypePtr> keyTypes_;
    const bool nullableKeys_;
    std::vector<TypePtr> types_;          // 所有列类型
    std::vector<TypeKind> typeKinds_;

    // 布局信息
    std::vector<int32_t> offsets_;        // 每列的偏移
    std::vector<int32_t> nullOffsets_;    // 每列的 null bit 位置
    std::vector<RowColumn> rowColumns_;   // 打包的列信息
    int32_t fixedRowSize_;
    int32_t flagBytes_;
    int alignment_;

    // 特殊偏移
    int32_t nextOffset_;                  // next 指针偏移 (Join)
    int32_t probedFlagOffset_;            // probed 标志偏移 (Right/Full Join)
    int32_t freeFlagOffset_;              // free 标志偏移
    int32_t rowSizeOffset_;               // 行大小偏移 (变长数据)

    // Normalized Key
    int32_t originalNormalizedKeySize_;
    int32_t normalizedKeySize_;
    int64_t numRowsWithNormalizedKey_;

    // 存储
    memory::AllocationPool rows_;         // 实际数据存储
    std::unique_ptr<HashStringAllocator> stringAllocator_;

    // 遍历优化
    std::vector<char*> rowPointers_;      // 快速遍历用
    bool useListRowIndex_;

    // Free list
    char* firstFreeRow_;
    uint64_t numFreeRows_;

    // 统计
    uint64_t numRows_;
    std::vector<RowColumn::Stats> rowColumnsStats_;
};
```

### 6.2 相关类关系

```
BaseHashTable
    └── HashTable<ignoreNullKeys>
            └── rows_ : unique_ptr<RowContainer>
            └── hashers_ : vector<VectorHasher>

GroupingSet
    └── table_ : unique_ptr<BaseHashTable>
    └── aggregates_ : vector<AggregateInfo>

SortBuffer
    └── data_ : unique_ptr<RowContainer>
    └── sortedRows_ : vector<char*>

HashBuild
    └── table_ : unique_ptr<BaseHashTable>

HashProbe
    └── table_ : BaseHashTable* (from HashBuild)
```

---

## 附录：类型大小参考

| TypeKind | Size (bytes) |
|----------|--------------|
| BOOLEAN | 1 |
| TINYINT | 1 |
| SMALLINT | 2 |
| INTEGER | 4 |
| BIGINT | 8 |
| REAL | 4 |
| DOUBLE | 8 |
| VARCHAR | 16 (StringView) |
| VARBINARY | 16 (StringView) |
| TIMESTAMP | 16 |
| HUGEINT | 16 |
| ARRAY/MAP/ROW | 16 (HashRowType=StringView) |
| UNKNOWN | 1 |
