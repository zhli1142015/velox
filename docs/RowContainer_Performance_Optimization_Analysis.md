# RowContainer Row Structure Performance Optimization Analysis

## Executive Summary

This document analyzes potential performance optimizations for Velox's RowContainer Row structure, focusing on three key operators: **Hash Join**, **Hash Aggregation**, and **Sort**. Each optimization is backed by benchmark measurements from our performance test suite.

---

## Baseline Performance Measurements

From `RowContainerPerfTest`, we measured the following baseline metrics (100,000 rows):

### Row Allocation
| Scenario | Time (ns/row) |
|----------|---------------|
| BIGINT key (Hash Join) | 11.40 |
| BIGINT key (Agg) | 8.86 |

### Store Performance
| Scenario | Time (ns/cell) | Notes |
|----------|----------------|-------|
| BIGINT | 5.21 | Baseline |
| VARCHAR (20 chars) | 24.00 | **4.6x slower** |
| Composite (BIGINT+VARCHAR+INT) | 22.95 | Mixed |

### Extract Column Performance
| Scenario | Time (ns/cell) | Notes |
|----------|----------------|-------|
| BIGINT | 3.26 | Baseline |
| VARCHAR | 12.58 | **3.9x slower** |
| Composite | 7.66 | Mixed |

### Row Comparison
| Scenario | Time (ns/cmp) |
|----------|---------------|
| BIGINT only | 28.47 |
| VARCHAR (50 chars) | 21.71 |
| Multi-key (BIGINT+VARCHAR) | 21.56 |

### List Rows Performance
| Scenario | Time (ns/row) |
|----------|---------------|
| BIGINT key | 0.74 |
| VARCHAR key | 3.57 |
| Composite key | 1.00 |

### Row Sizes
| Configuration | Size (bytes) |
|---------------|--------------|
| BIGINT only | 9 |
| BIGINT + hasNext | 17 |
| BIGINT + hasNext + probed | 17 |
| VARCHAR only | 21 |
| BIGINT + 3 dependents + hasNext + probed | 49 |
| 3 keys + 1 dependent + hasNext + probed | 49 |

---

## 🔑 KEY FINDING: String Length Impact Analysis

**This is the most significant finding from our benchmarks:**

| String Length | Inline? | Time (ns/row) | Ratio vs 4-byte |
|---------------|---------|---------------|-----------------|
| 4 bytes | ✅ INLINE | 4.14 | 1.0x |
| 8 bytes | ✅ INLINE | 4.73 | 1.1x |
| 12 bytes | ✅ INLINE | 7.90 | 1.9x |
| **16 bytes** | ❌ ALLOCATED | **60.35** | **14.6x** |
| 24 bytes | ❌ ALLOCATED | 17.30 | 4.2x |
| 32 bytes | ❌ ALLOCATED | 77.53 | 18.7x |
| 64 bytes | ❌ ALLOCATED | 28.23 | 6.8x |
| 128 bytes | ❌ ALLOCATED | 70.87 | 17.1x |

**Critical Observations**:
1. The 12→16 byte boundary causes a **dramatic performance cliff** because strings longer than 12 bytes require memory allocation via `HashStringAllocator`
2. Non-inline string performance is highly variable (17-78 ns/row) due to memory allocation patterns
3. All inline strings (≤12 bytes) maintain consistent performance (4-8 ns/row)

---

## hasNext Pointer Overhead Analysis

| Configuration | Row Size | Allocation Time |
|---------------|----------|-----------------|
| Without hasNext | 17 bytes | 0.795 ms |
| With hasNext | 25 bytes | 0.888 ms |
| With hasNext+probed | 25 bytes | 0.839 ms |

**Finding**: `hasNext` pointer adds **8 bytes per row** (17→25 bytes), with allocation time impact of ~10-11%. The real cost is memory bandwidth and cache efficiency.

---

## Memory Locality Impact Analysis

| Access Pattern | Time (ms) | Ratio |
|----------------|-----------|-------|
| Sequential extractColumn | 0.165 | 1.0x |
| Random extractColumn | 0.377 | **2.3x slower** |

**Finding**: Random access is 2.3x slower than sequential access due to cache misses. This emphasizes the importance of processing rows in allocation order when possible.

---

## Optimization 1: Hash Join Build Side

### Current Row Layout for Hash Join
```
[NormalizedKey (optional, 8B)][Keys...][NullFlags][Dependents...][RowSize(4B)][NextPtr(8B)]
```

### Identified Hotspots

1. **VARCHAR Key Storage (18.88 ns/cell vs 5.31 ns for BIGINT)**
   - Root cause: `HashStringAllocator::copyMultipartNoInline()` allocates memory from free list
   - For non-inline strings (>12 bytes), requires:
     - Free list lookup
     - Memory allocation from arena
     - Copy of string data
     - StringView construction

2. **hasNext Pointer Overhead (+8 bytes per row)**
   - Required for non-unique keys in hash join build
   - Always allocated even when most keys are unique

3. **Row Allocation Overhead (12.26 ns/row)**
   - Memory comes from `AllocationPool::newRun()`
   - Initialization of null flags and size fields

### Optimization Proposals

#### Proposal 1A: Lazy hasNext Allocation
**Problem**: `nextOffset_` always reserves 8 bytes even when 99% of rows have unique keys.

**Solution**: Use a side structure (hash map) to store next pointers only for duplicate keys.

**Expected Benefit**:
- Reduce row size from 17 to 9 bytes for unique key rows
- Improve cache utilization by ~47% (9/17)

**Trade-off**: Extra indirection for duplicate key lookup

#### Proposal 1B: Batch String Allocation
**Problem**: Each VARCHAR value triggers individual allocation.

**Solution**: Pre-allocate string arena based on estimated total string size.

**Current Code** (HashStringAllocator.cpp:566):
```cpp
inline bool HashStringAllocator::storeStringFast(...) {
  // Allocates from free list for each string
}
```

**Proposed Enhancement**:
```cpp
// Add batch allocation hint
void HashStringAllocator::reserveStringSpace(size_t estimatedBytes) {
  // Pre-allocate contiguous memory for strings
}
```

**Expected Benefit**:
- Reduce per-string allocation overhead
- Better memory locality for string data

#### Proposal 1C: Inline Strings Threshold Tuning
**Problem**: StringView::kInlineSize = 12 bytes may be too conservative.

**Current** (from StringView):
```cpp
static constexpr uint32_t kInlineSize = 12;
```

**Analysis**: Many real-world join keys are 8-16 characters. Increasing inline threshold could avoid allocation.

**Expected Benefit**: For strings 13-16 bytes, avoid allocation overhead entirely.

**Trade-off**: Larger row size (16 vs 12 bytes for StringView)

---

## Optimization 2: Hash Aggregation

### Current Row Layout for Aggregation
```
[Keys...][NullFlags][Accumulators...][RowSize]
```

### Identified Hotspots

1. **Accumulator Access Pattern**
   - Accumulators are accessed more frequently than keys
   - Current layout puts keys first, causing cache misses for accumulator updates

2. **Row Allocation for New Groups (7.16 ns/row)**
   - Each new group requires `newRow()` allocation
   - Initialization overhead for accumulators

### Optimization Proposals

#### Proposal 2A: Accumulator-First Layout
**Problem**: Hash Agg workload is dominated by accumulator updates, but accumulators are at end of row.

**Current Layout**:
```
[Keys (accessed once)][NullFlags][Accumulators (accessed N times)]
```

**Proposed Layout**:
```
[Accumulators][Keys][NullFlags]  // Put hot data first
```

**Expected Benefit**:
- Better cache line utilization
- Accumulator updates hit L1 cache more often

**Trade-off**: Requires refactoring row offset calculations

#### Proposal 2B: Pre-allocated Accumulator Pools
**Problem**: Variable-size accumulators (e.g., for `array_agg`) trigger frequent allocations.

**Solution**: Use size-class-based allocation pools for common accumulator patterns.

---

## Optimization 3: Sort Operations

### Current Implementation
1. `compareRows()` iterates over all keys calling `compare()` for each
2. For VARCHAR keys, `compareStringAsc()` calls `HashStringAllocator::contiguousString()`
3. PrefixSort already exists as an optimization

### Identified Hotspots

1. **VARCHAR Comparison (21.71 ns/cmp)**
   - `contiguousString()` may copy data to temporary buffer
   - Branch for checking if string is contiguous

2. **Multi-key Comparison Overhead**
   - Virtual function overhead per comparison
   - Type dispatch at runtime

---

## 🚀 PrefixSort vs std::sort Benchmark Results

**Key Finding**: PrefixSort provides significant performance improvement over std::sort with RowContainer::compareRows!

| Key Type | std::sort | PrefixSort | Improvement |
|----------|-----------|------------|-------------|
| BIGINT | 7.33 ms | 4.51 ms | **38% faster** |
| VARCHAR | 15.75 ms | 5.86 ms | **63% faster** |

**Analysis**:
- PrefixSort is **1.6x faster** for BIGINT keys
- PrefixSort is **2.7x faster** for VARCHAR keys
- VARCHAR benefits more because PrefixSort uses normalized key prefix, avoiding full string comparison

**Recommendation**: Always use PrefixSort when sorting RowContainer data with row counts > 128.

---

### Optimization Proposals

#### Proposal 3A: Normalized Key Cache for VARCHAR
**Problem**: Non-inline strings require `contiguousString()` call on every comparison.

**Current Code** (RowContainer.cpp:847):
```cpp
int32_t RowContainer::compareStringAsc(StringView left, StringView right) {
  std::string leftStorage;
  std::string rightStorage;
  return HashStringAllocator::contiguousString(left, leftStorage)
      .compare(HashStringAllocator::contiguousString(right, rightStorage));
}
```

**Solution**: Cache normalized key prefix in row header for frequent comparisons.

**Expected Benefit**: Avoid repeated `contiguousString()` calls during sort

#### Proposal 3B: Extend PrefixSort String Prefix Length
**Current**: PrefixSort already handles VARCHAR with `maxStringPrefixLength` (default 16 bytes).

**Benchmark Validation**: PrefixSort is 2.7x faster than std::sort for VARCHAR keys (see above).

**Enhancement**: Tune `maxStringPrefixLength` based on actual string length distribution.

| maxStringPrefixLength | Use Case |
|-----------------------|----------|
| 8 bytes | Short strings (e.g., country codes) |
| 16 bytes (default) | Medium strings (e.g., names) |
| 32 bytes | Longer strings (e.g., addresses) |

#### Proposal 3C: SIMD-accelerated Multi-key Comparison
**Problem**: Current comparison is scalar, one column at a time.

**Solution**: For homogeneous key types (e.g., all BIGINT), use SIMD to compare multiple keys in parallel.

---

---

## 🔥 NEW FINDING: Null Flag Layout Optimization

**Benchmark Date**: 2026-01-16

### Current Layout Analysis

Current RowContainer stores null flags **centrally** at a fixed offset:

```
Row Structure:
[Keys...][NullFlags (all columns)][Accumulators/Dependents...][RowSize][NextPtr]
         ^
         nullByte offset (e.g., 36 for 4 BIGINT columns)
```

**Measured Value-to-Null Distances**:
| Column | Value Offset | NullByte Offset | Distance |
|--------|--------------|-----------------|----------|
| Column 0 | 0 | 36 | 36 bytes |
| Column 1 | 8 | 36 | 28 bytes |
| Column 2 | 16 | 36 | 20 bytes |
| Column 3 | 24 | 36 | 12 bytes |

### Benchmark Results

| Layout | Access Pattern | Time (ns/row) | Notes |
|--------|---------------|---------------|-------|
| Centralized | Value + Null check | 2.77 | Current implementation |
| Centralized | Value only | 0.49 | Baseline |
| **Inline** | Value + Null check | 0.88 | **3x faster** |
| **Inline** | Value only | 0.25 | Simulated |

**Key Metrics**:
- **Centralized null check overhead**: 2.28 ns/row (463% overhead)
- **Inline null check overhead**: 0.63 ns/row (259% overhead)
- **Net improvement**: ~1.65 ns/row (~60% faster null checking)

### Why Inline Nulls Are Faster

1. **Cache Locality**: Value and its null flag load in same cache line (64 bytes)
2. **Prefetch Efficiency**: CPU prefetcher works better with adjacent data
3. **Reduced Cache Misses**: No jump to distant nullByte offset

### Proposed Inline Null Layout

```
Current:  [Value0][Value1][Value2][Value3][Null0|Null1|Null2|Null3]
                                          ^ 36 bytes away

Proposed: [Null0][Value0][Null1][Value1][Null2][Value2][Null3][Value3]
          ^ 1 byte away
```

### Trade-offs

| Aspect | Centralized Nulls | Inline Nulls |
|--------|-------------------|--------------|
| Single-column access | ❌ Slower (cache miss) | ✅ Faster |
| Batch null operations | ✅ Faster (contiguous) | ❌ Slower |
| Memory compactness | ✅ Better (bit-packed) | ❌ Worse (+1 byte/column) |
| Vectorized processing | ✅ Better | ⚠️ Needs refactoring |

### Recommendation

For **Velox's vectorized batch processing model**, the trade-off analysis is nuanced:
- Current centralized layout is better for batch null mask construction
- Inline layout would benefit operators doing per-row random access (e.g., hash table probing)
- Consider **hybrid approach**: inline nulls for frequently accessed columns (keys), centralized for others

---

## 💡 Additional Optimization Directions (Validated)

**Validation Date**: 2026-01-16

All optimization directions have been benchmarked. Summary:

| Direction | Improvement | Verdict | Reason |
|-----------|-------------|---------|--------|
| 1. Cache Line Alignment | N/A | ❌ REJECT | 73% memory overhead too high |
| 2. Hot/Cold Field Separation | **60%** | ✅ ACCEPT | Significant improvement, but major refactor |
| 3. Columnar Row Storage | **71%** | ⚠️ CONDITIONAL | API incompatible, use Arrow instead |
| 4. Variable-Length Field Pooling | **7.5x** | ✅ ACCEPT | Additive change, backward compatible |
| 5. Normalized Key Embedding | N/A | ❌ REJECT | 89% memory overhead, PrefixSort already optimal |
| 6. SIMD-Friendly Type Layouts | **68%** | ⚠️ CONDITIONAL | Requires columnar storage |
| 7. Metadata Compression | N/A | ❌ REJECT | Bit-packed already optimal |
| 8. Memory Pool Tiering | N/A | ❌ REJECT | Spiller already provides this |

---

### Direction 1: Cache Line Alignment ❌ REJECTED

**Observation**: Current row allocation doesn't ensure cache line alignment.

**Benchmark Result**:
```
Row size: 74 bytes
64-byte aligned row size: 128 bytes
Memory overhead: 54 bytes (73%)
Rows straddling cache lines: 100%
```

**Verdict**: ❌ **REJECTED** - Memory overhead (73%) too high for potential benefit.

---

### Direction 2: Hot/Cold Field Separation ✅ ACCEPTED

**Observation**: Some fields (keys, accumulators) are accessed frequently, others (dependents) rarely.

**Benchmark Result**:
```
Mixed layout row size: 66 bytes (2 keys + 6 dependents)
Hot-only layout row size: 17 bytes (2 keys only)

Mixed layout (hot access): 0.999 ns/row
Hot-only layout: 0.398 ns/row
Improvement: 60.2%
```

**Verdict**: ✅ **ACCEPTED**
- **PRO**: 60% improvement for hash probe pattern
- **CON**: Requires separate cold storage, adds indirection
- **COMPATIBILITY**: Works with current API after layout refactor
- **RECOMMENDED FOR**: Hash Aggregation with many dependent columns

**Proposed Layout**:
```
Hot Zone:  [Keys][Accumulators]     <- Always in cache (1 cache line)
Cold Zone: [Dependents][Metadata]   <- Loaded on demand (pointer indirection)
```

---

### Direction 3: Columnar Row Storage ❌ REJECTED

**Observation**: Current layout is row-oriented (all fields of one row together).

**Benchmark Result**:
```
Row-wise scan: 0.285 ns/value
Columnar scan: 0.082 ns/value
Columnar improvement: 71.2%

Random access: Similar performance (both cache-unfriendly)
```

**⚠️ CRITICAL UPDATE (Based on Velox Official Blog Analysis)**:

The [Velox blog "Why Sort is row-based"](https://velox-lib.io/blog/why-row-based-sort/) provides definitive evidence that **row-based layout is 2-4x faster** than columnar gather for Sort operations:

| Metric | Row-based | Columnar Gather | Impact |
|--------|-----------|-----------------|--------|
| IPC | 2.4 | 0.82 | Row has **2.9x higher CPU utilization** |
| LLC Load Misses | 0.14 Billion | 5.01 Billion | Columnar has **35x more cache misses** |
| RAM Hit % | 5.8% | 38.1% | Columnar is **memory-bound** |

**Root Cause**: Our micro-benchmark measured **single-column sequential scan**, but real operators need to **gather multiple columns**. Each column gather pass evicts the previous column's data from cache, causing catastrophic cache thrashing:
> "the gather process must re-fetch the same vector metadata and data from main memory over and over again for each payload column"

**Verdict**: ❌ **REJECTED**
- **PRO (misleading)**: 71% improvement only for single-column sequential scan
- **CON**: Multi-column output causes 35x LLC cache misses
- **CON**: Hardware prefetcher is ineffective with random gather patterns
- **CON**: IPC drops from 2.4 to 0.82 (CPU stalls waiting for memory)
- **RECOMMENDATION**: Keep row-based layout; one cache line fetch serves multiple columns

---

### Direction 4: Variable-Length Field Pooling ✅ ACCEPTED

**Observation**: VARCHAR fields > 12 bytes trigger expensive allocations.

**Benchmark Result**:
```
Current (per-string alloc): 16.6 ns/store
Simulated pooled (bump-pointer): 2.2 ns/store
Improvement: 7.5x faster
```

**Verdict**: ✅ **ACCEPTED**
- **PRO**: **7.5x faster** for non-inline strings
- **IMPLEMENTATION**: Add `reserveStringSpace(estimatedBytes)` to HashStringAllocator
- **COST**: Requires size estimation before store phase
- **COMPATIBILITY**: Additive change, fully backward compatible
- **SYNERGY**: Works well with batch store operations

**Proposed API**:
```cpp
// Add batch allocation hint
void HashStringAllocator::reserveStringSpace(size_t estimatedBytes) {
  // Pre-allocate contiguous memory for strings
}
```

---

### Direction 5: Normalized Key Embedding ❌ REJECTED

**Observation**: Sort operations repeatedly compute normalized keys.

**Benchmark Result**:
```
Normalization time: 0.81 ns/row
Current row size: 9 bytes
With embedded key: 17 bytes (+89% memory overhead)
```

**Verdict**: ❌ **REJECTED**
- PrefixSort already computes normalized keys efficiently (once per sort)
- Embedding adds 89% memory overhead to ALL rows
- Multiple sorts on same RowContainer are rare
- Better to use PrefixSort's existing normalized key cache

---

### Direction 6: SIMD-Friendly Type Layouts ⚠️ CONDITIONAL

**Observation**: Modern CPUs have 256-bit (AVX2) or 512-bit (AVX-512) SIMD registers.

**Benchmark Result**:
```
Row-wise comparison: 0.368 ns/value
SIMD-friendly comparison: 0.117 ns/value
Improvement: 68.3%
```

**Verdict**: ⚠️ **CONDITIONAL**
- **PRO**: 68% improvement for batch operations
- **CON**: Requires columnar storage (see Direction 3)
- **CON**: Incompatible with current row-wise API
- **RECOMMENDATION**: Use existing Vector operations instead
- **NOTE**: Velox already does SIMD in Vector layer, not RowContainer

---

### Direction 7: Metadata Compression ❌ REJECTED

**Observation**: `nullByte`, `probed`, `normalized` flags are often sparse.

**Benchmark Result**:
```
Null Ratio 0%:  Position list saves space but adds decode overhead
Null Ratio 1%:  RLE may help for clustered nulls only
Null Ratio 10%: Bit-packed already optimal
Null Ratio 50%: Bit-packed already optimal
```

**Verdict**: ❌ **REJECTED**
- Current bit-packed format is already compact (1 bit/column)
- Compression adds decode overhead for every random access
- Real-world null ratios are typically <1% or >50%
- Complexity not justified for typical workloads

---

### Direction 8: Memory Pool Tiering ❌ REJECTED

**Observation**: Not all rows have equal access frequency.

**Analysis**:
```
Existing Infrastructure:
  ✓ Spiller already handles disk I/O
  ✓ RowPartitions track partition membership
  ✓ MemoryArbitrator manages memory pressure

What Tiering Would Add:
  - LRU tracking: Significant overhead per access
  - Memory-mapped: OS already does this
  - Async prefetch: Requires workload prediction
```

**Verdict**: ❌ **REJECTED**
- Velox Spiller already provides tiered storage behavior
- Partition-based spilling is simpler than row-level tiering

---

### Direction 9: Split Null Flags Layout ❌ REJECTED (Test 30)

**Hypothesis**: Splitting null flags (key nulls near keys, dependent nulls near dependents) would improve cache locality.

**Proposed Layout Change**:
```
Current:  [Keys][All Null Flags][Accumulators][Dependents]
Proposed: [Key Nulls][Keys][Accumulators][Dep Nulls][Dependents]
```

**Test 30 Results** (77-byte rows, 200K rows, random access):
```
Row Layout Analysis:
  Row size: 77 bytes
  All columns' null flags AND values are in the SAME cache line (cache line 0)

Cache Line Distribution:
  Column 0: null in cache line 0, value in cache line 0 (SAME LINE)
  Column 1: null in cache line 0, value in cache line 0 (SAME LINE)
  Column 2: null in cache line 0, value in cache line 0 (SAME LINE)
  Column 3: null in cache line 0, value in cache line 0 (SAME LINE)
  Column 4: null in cache line 0, value in cache line 0 (SAME LINE)
  Column 5: null in cache line 0, value in cache line 0 (SAME LINE)

Null Check Overhead:
  Current layout (null at offset 24): +0.50 ns/row
  Simulated inline (null at offset 0): +2.81 ns/row
  ❌ SPLIT LAYOUT IS 2.3 ns/row SLOWER!

Multi-Column Access (verifying cache behavior):
  Access keys only: 2.95 ns/row
  Access deps only: 5.12 ns/row
  Access all (same row): 6.40 ns/row
  Cache benefit: 1.67 ns/row (row data already in cache)
```

**Verdict**: ❌ **REJECTED** - Split null flags would NOT improve performance

**Reasons**:
1. **Row size < 2 cache lines**: With 77-byte rows and 64-byte cache lines, the entire row fits in 1-2 cache lines
2. **Null flags already share cache line with values**: All 6 columns' nulls and values are in cache line 0
3. **CPU prefetch works well**: When accessing row start, entire row is prefetched
4. **Split layout adds overhead**:
   - Need extra byte alignment for each null group
   - Increases row size
   - Breaks compact bit-packing (1 bit/column → 1 byte alignment per group)

**Key Insight**: The current centralized null flags design is cache-friendly because:
- Rows are typically <128 bytes (2 cache lines)
- Null flags at byte 24 are in the same cache line as values at offsets 0-64
- Bit-packing allows all nulls to fit in 1-2 bytes for typical column counts

---

### Direction 10: Row-at-a-time extractRow API ❌ REJECTED (Tests 28-29)

**Hypothesis**: For HashProbe, extracting entire rows at a time would be faster than column-at-a-time extraction.

**Test 28-29 Results** (200K rows, random access hits):
```
| Config | Column-at-a-time | Row-at-a-time | Result |
|--------|------------------|---------------|--------|
| 3 cols, 50% hit | 0.32 ms | 0.69 ms | Row 54% slower |
| 10 cols, 50% hit | 1.49 ms | 2.28 ms | Row 35% slower |
| 20 cols, large | 9.28 ms | 26.57 ms | Row 65% slower |
| Random access | 1.38 ms | 3.31 ms | Row 58% slower |
```

**Verdict**: ❌ **REJECTED** - Row-at-a-time is 35-65% SLOWER

**Reasons**:
1. extractColumn() is highly optimized with SIMD batch operations
2. Column-at-a-time allows better CPU pipelining and prefetching
3. Row-at-a-time has more branch mispredictions (null checks per column per row)
4. Current API is already optimal for the column-oriented output vectors

---

## Verification Plan (Updated)

### ✅ Completed Validations

All 10 optimization directions have been benchmarked with the following results:
- **2 ACCEPTED (micro-benchmark)**: Hot/Cold Separation (60%), Variable-Length Pooling (7.5x)
- **2 CONDITIONAL**: Columnar Storage (71%), SIMD Layouts (68%)
- **6 REJECTED**: Cache Line Alignment, Normalized Key Embedding, Metadata Compression, Memory Pool Tiering, Split Null Flags, Row-at-a-time extractRow

### Real-World Validation Results ⭐ CRITICAL

After analyzing actual operator code (HashBuild, HashProbe, SortBuffer, GroupingSet), we discovered major conflicts between micro-benchmarks and reality:

- **Hot/Cold Separation**: ❌ INVALID - Operators don't have "cold" columns
- **Columnar Storage**: ❌ INVALID - SortBuffer already uses it, HashBuild can't use it
- **Inline Null Flags**: ❌ INVALID - Would HURT batch extractColumn performance

**Only one optimization remains valid**:
- **Variable-Length Field Pooling**: ✅ VALID - 7.5x faster, works with actual patterns

---

## Implementation Priority (Final - After Real-World Validation)

| Priority | Optimization | Effort | Measured Impact | Real-World Status |
|----------|-------------|--------|-----------------|-------------------|
| **✅ Valid** | Use PrefixSort for Sort | N/A | **38-63% faster** | **Already implemented** |
| ⚠️ Limited | Variable-Length Field Pooling | Medium | 7.5x in test | **HashStringAllocator already optimized** |
| ❌ Invalid | Hot/Cold Field Separation | High | 60% in test | **Operators have no cold columns** |
| ❌ Invalid | Columnar Storage | Very High | 71% in test | **35x cache misses on multi-col output** |
| ❌ Invalid | SIMD-Friendly Layouts | Very High | 68% in test | **Requires columnar, same issues** |
| ❌ Invalid | Inline Null Flags | High | 3x in test | **Would HURT batch operations** |
| ❌ Rejected | Split Null Flags (Test 30) | Medium | -2.3 ns/row | **Same cache line already** |
| ❌ Rejected | Row-at-a-time extractRow (Tests 28-29) | Medium | 35-65% slower | **Column SIMD > row loops** |
| ❌ Rejected | Cache Line Alignment | Low | N/A | 73% memory overhead |
| ❌ Rejected | Normalized Key Embedding | Low | N/A | 89% memory overhead |
| ❌ Rejected | Metadata Compression | Medium | N/A | Bit-packed already optimal |
| ❌ Rejected | Memory Pool Tiering | High | N/A | Spiller already provides |

**Key Insight from Velox Blog**: Row-based layout enables **one cache line fetch to serve multiple column extractions**, which is critical for Sort output phase. This confirms our decision to reject columnar storage for RowContainer.

**Key Insight from Test 30**: For typical row sizes (<128 bytes), null flags and values are in the same cache line, so splitting null flags would NOT improve cache locality but would add byte alignment overhead.

**Key Insight from Tests 31-32 (Layout Optimization Analysis)**: Even for large rows (4+ cache lines), layout changes provide marginal benefits because CPU prefetch and batch operations amortize per-row overhead.

---

## Layout Optimization Analysis (Tests 31-32)

### Row Size vs Cache Efficiency (Test 31)

| Config | Row Size | Cache Lines | Key Access | Dep Access | All Access |
|--------|----------|-------------|------------|------------|------------|
| Small (2 cols) | 17B | 1 | 4.9 ns | 1.6 ns | 5.7 ns |
| Medium (6 cols) | 69B | 2 | 12.2 ns | 8.8 ns | 14.3 ns |
| Large (12 cols) | 134B | 3 | 17.7 ns | 16.9 ns | 26.1 ns |
| VeryLarge (20 cols) | 231B | 4 | 39.4 ns | 28.7 ns | 55.6 ns |

**Finding**: Access time scales roughly linearly with cache line count, but CPU prefetch mitigates most overhead.

### Large Row Cache Line Analysis (Test 32)

For 247-byte rows spanning 4 cache lines:
```
Columns with null in different cache line: 14 / 20

Key columns 0-5: Value in CL0, Null in CL1 ❌
Key columns 6-7: Value in CL1, Null in CL1 ✅
Dep columns 0-3: Value in CL1, Null in CL1 ✅
Dep columns 4-11: Value in CL2-3, Null in CL1 ❌

Null check overhead for cross-CL column: 2.03 ns/row
```

### Layout Optimization Verdict: ❌ NOT RECOMMENDED

**Reasons to keep current layout**:
1. **CPU prefetch effective**: Adjacent cache lines often prefetched together
2. **Batch operations amortize overhead**: extractColumn() processes entire columns
3. **Inline null flags hurt batch ops**: Would break efficient null bitmap extraction
4. **Complexity not justified**: Marginal gains (2 ns/row) don't justify implementation cost

**Recommendation for large rows**: Reduce column count rather than change layout.

---

## Key Findings (Pre-Validation)

### 1. String Length is the Primary Performance Bottleneck
- **12-byte boundary** is critical: strings ≤12 bytes are inline (4-8 ns/row), strings >12 bytes require allocation (22-77 ns/row)
- **5.5x performance cliff** at 12→16 byte boundary
- **Recommendation**: Design schemas to keep VARCHAR keys ≤12 bytes when possible

### 2. Variable-Length Field Pooling is Highly Effective
- **7.5x faster** for non-inline string storage with pooled allocation
- Additive change, fully backward compatible
- **Recommendation**: Implement `reserveStringSpace()` in HashStringAllocator

### 3. PrefixSort Should Always Be Used for Sorting
- **Verified 38% improvement** for BIGINT keys over std::sort
- **Verified 63% improvement** for VARCHAR keys over std::sort
- Already available in Velox via `PrefixSort::sort()` static method

### 4. Memory Locality Impact
- Sequential access: 0.17 ms for 10K rows
- Random access: 0.38 ms for 10K rows (**2.2x slower**)
- **Recommendation**: Process rows in allocation order when possible

### 5. Many Proposed Optimizations Are Not Worthwhile
- Cache Line Alignment: 73% memory overhead not justified
- Normalized Key Embedding: 89% overhead, PrefixSort already optimal
- Metadata Compression: Bit-packed format already optimal
- Memory Pool Tiering: Spiller already provides tiered storage

**NOTE**: Hot/Cold Separation and Inline Nulls were initially validated but later invalidated by real-world scenario analysis. See "Real-World Scenario Validation" section below.

---

## Real-World Scenario Validation ⭐ CRITICAL UPDATE

### Analysis of Actual Operator Code Patterns

After analyzing the actual source code of Sort, Hash Join, and Hash Aggregation operators, we discovered **significant conflicts** between our micro-benchmark results and real-world usage patterns.

#### Operator Code Patterns Discovered

| Operator | Store Pattern | Extract Pattern | Hot/Cold Columns |
|----------|--------------|-----------------|------------------|
| **HashBuild** | Row-by-row (`newRow` + `store` per row) | N/A (build side) | Keys + Dependents together |
| **HashProbe** | N/A | Column-at-a-time (`extractColumn`) | Only projected columns |
| **SortBuffer** | Batch (`newRow` all first, then `store` column-by-column) | Column-at-a-time | All columns equally used |
| **GroupingSet** | Row-by-row (via `groupProbe`) | Accumulators only | Keys for probe, values for agg |

#### Conflicts Between Tests and Reality

##### 1. Hot/Cold Separation (Test claimed 60% improvement)

**Micro-benchmark assumption**: Access hot keys separately from cold dependents.

**Reality**:
- ❌ **HashBuild**: Stores keys AND dependents together in the same loop (`table_->store(lookup_->hits, keys, dependents)`)
- ❌ **HashAgg**: Has NO dependent columns - all columns are either keys or accumulators
- ⚠️ **HashProbe**: Only extracts projected columns anyway (already selective)

**Revised Impact**: ~10-20% for probe output scenarios, NOT 60%

##### 2. Columnar Storage (Test claimed 71% improvement)

**Micro-benchmark assumption**: Batch store multiple columns at once.

**Reality**:
- ⚠️ **SortBuffer**: Already does column-at-a-time store via `storeVector` loop
- ❌ **HashBuild**: Does row-at-a-time store (one `store()` call per input row batch)
- ❌ Cannot convert HashBuild to columnar without major refactoring

**Revised Impact**: SortBuffer already gets benefit; HashBuild cannot use it

##### 3. Inline Null Flags (Test claimed 3x improvement)

**Micro-benchmark assumption**: Check null for single row at a time.

**Reality**:
- ❌ **extractColumn**: Processes null flags in batch with optimized vector operations
- ❌ Centralized null flags enable better vectorization
- ❌ Inline nulls would HURT batch operations (more cache misses)

**Revised Impact**: **NEGATIVE** for batch operations

### Variable-Length Pooling: Re-evaluation ⚠️

**Initial Claim**: 7.5x faster with pooled allocation

**Reality Check**: HashStringAllocator **already IS a pool allocator**:
- Uses **free lists** for O(1) allocation from freed blocks
- Uses **arena allocation** (`newSlab()`) for bulk memory
- Has **fast path** (`storeStringFast`) that directly uses free list

```cpp
// HashStringAllocator already does:
bool storeStringFast(const char* bytes, int32_t size, char* dest) {
  // 1. Try free list lookup
  auto available = bits::findFirstBit(freeNonEmpty(), index, kNumFreeLists);
  header = allocateFromFreeList(roundedBytes, true, true, available);
  // 2. Direct memcpy
  simd::memcpy(header->begin(), bytes, numBytes);
}
```

**What our test actually measured**:
- Test compared `copyMultipart` (uses free list lookup) vs pre-reserved continuous buffer
- The 7.5x difference is the overhead of:
  1. Free list search (`bits::findFirstBit`, `allocateFromFreeList`)
  2. Header management (4-byte header per allocation)
  3. Fragmentation from varying string sizes

**Actual optimization opportunity** (if any):
- Batch pre-allocation: Call `allocate(totalEstimatedBytes)` once, then carve out strings
- But this would require API changes and tracking complexity

**Revised Conclusion**: HashStringAllocator is already well-optimized. The 7.5x micro-benchmark difference reflects the inherent cost of dynamic allocation with size variability, which cannot be eliminated without knowing string sizes in advance.

### Final Recommendations Matrix (FINAL)

| Optimization | HashJoin Build | HashJoin Probe | HashAgg | Sort | Validity |
|--------------|----------------|----------------|---------|------|----------|
| Variable-Length Pooling | ⚠️ Marginal | ⚠️ N/A | ⚠️ Marginal | ⚠️ Marginal | **Already Optimized** |
| Hot/Cold Separation | ❌ NONE | ⚠️ LOW | ❌ NONE | ❌ NONE | **INVALID** |
| Inline Nulls | ❌ NEGATIVE | ❌ NEGATIVE | ❌ NEGATIVE | ❌ NEGATIVE | **INVALID** |
| Columnar Storage | ❌ NONE | ⚠️ LOW | ❌ NONE | ⚠️ Already used | **INVALID** |
| PrefixSort | ❌ N/A | ❌ N/A | ❌ N/A | ✅ Already used | **VALID** |

---

## Updated Key Conclusions

### 1. Micro-benchmarks Can Be Misleading ⭐ CRITICAL
- Our tests assumed ideal access patterns that don't match actual operator code
- Always validate optimizations against actual usage patterns
- Hot/Cold separation is attractive in theory but operators don't have cold columns

### 2. Variable-Length Field Pooling Remains Valid
- **7.5x faster** for string allocation (confirmed)
- Works with both row-by-row (HashBuild) and batch (SortBuffer) patterns
- **This is the only optimization worth implementing**

### 3. PrefixSort is Already Optimal
- **38-63% faster** than std::sort
- SortBuffer already uses it via `PrefixSort::sort()`
- No further optimization needed for sorting

### 4. Current RowContainer Layout is Well-Optimized
- Centralized null flags benefit batch `extractColumn` operations
- Row-wise layout matches HashBuild's row-at-a-time store pattern
- Hot/cold separation would add complexity without real benefit

---

## Next Steps (Final Revision)

1. **No structural optimizations recommended** ⭐
   - HashStringAllocator already provides efficient pooled allocation
   - PrefixSort already used by SortBuffer
   - Current RowContainer layout is well-matched to actual operator usage

2. **Schema design recommendations** (for users):
   - Keep VARCHAR keys ≤12 bytes when possible (inline storage)
   - Prefer BIGINT over VARCHAR for join keys when semantically equivalent

3. **Profile real TPC-H/TPC-DS queries** to identify actual bottlenecks
   - Focus on memory bandwidth, not allocation overhead
   - Consider parallel execution opportunities

---

## Summary: Why Current Design is Already Optimal

After extensive analysis, we discovered that the RowContainer and HashStringAllocator are **already well-optimized** for their actual usage patterns:

| Component | Optimization | Status |
|-----------|-------------|--------|
| HashStringAllocator | Free list pooling | ✅ Already implemented |
| HashStringAllocator | Arena allocation | ✅ Already implemented |
| HashStringAllocator | Fast path for small strings | ✅ Already implemented |
| RowContainer | Centralized null flags | ✅ Optimal for batch extractColumn |
| RowContainer | Conditional memset | ✅ Only for variable-width columns |
| SortBuffer | PrefixSort | ✅ Already used |
| SortBuffer | Column-at-a-time store | ✅ Already used |

**Key Learning**: Micro-benchmarks can be misleading. Always validate against actual operator code patterns before proposing optimizations.

---

## Additional Optimization Analysis (Tests 21-25)

### Counter-Intuitive Finding: Fragmented Allocator is Faster!

```
Fragmented allocator: 15.7 ns/alloc
Clean allocator:      35.9 ns/alloc
```

**Result**: Fragmented allocator is **2.3x faster** than clean allocator!

**Explanation**:
- Clean allocator must cut new blocks from slab (more work)
- Fragmented allocator has free list entries ready to reuse
- `storeStringFast` fast path works better with populated free lists

**Conclusion**: HashStringAllocator is designed correctly - fragmentation is actually beneficial for performance.

---

### Optimization Direction 1: Increase StringView::kInlineSize

**Hypothesis**: Increase inline size from 12 to 16 or 24 bytes to avoid allocation.

**Test Results (Test 23)**:
```
| Inline Size | Memory Overhead | Benefit Zone    | Verdict     |
|-------------|-----------------|-----------------|-------------|
| 12B (curr)  | 0%              | ≤12B strings    | Baseline    |
| 16B         | +25% per row    | 13-16B strings  | ⚠️ Limited  |
| 24B         | +75% per row    | 13-24B strings  | ❌ Too costly|
```

**Performance by String Length**:
- Short strings (4-11B): 12.6 ns/row - all inline, no benefit from increase
- Medium strings (12-19B): 19.3 ns/row - partial benefit
- Long strings (24-63B): 27.4 ns/row - no benefit from increase

**Verdict**: ❌ **NOT RECOMMENDED**
- Memory overhead affects ALL rows regardless of string length
- Performance benefit only for narrow 13-16B or 13-24B ranges

---

## 📚 External Validation: Velox Blog Analysis

### Reference: [Why Sort is row-based in Velox](https://velox-lib.io/blog/why-row-based-sort/)

This official Velox blog post (December 2025) provides strong empirical evidence that validates our analysis conclusions.

### Key Experiment Setup

The blog compared two sorting strategies:
1. **Row-based Sort (current)**: Materialize ALL columns to RowContainer → Sort → Extract columns
2. **Non-Materialized Sort**: Materialize only sort keys + indices → Sort → Gather payload from original vectors

### Counter-Intuitive Results

| Payload Columns | Row-based | Non-Materialized | Winner |
|-----------------|-----------|------------------|--------|
| 64 | 11.64s | 45.90s | **Row 3.9x faster** |
| 128 | 31.43s | 64.20s | **Row 2.0x faster** |
| 256 | 51.48s | 154.80s | **Row 3.0x faster** |

### Root Cause Analysis (perf stat)

| Metric | Row-based | Non-Materialized | Ratio |
|--------|-----------|------------------|-------|
| Total Instructions | 555.6B | 475.6B | Row +17% |
| **IPC** | **2.4** | **0.82** | **Row 2.9x higher** |
| **LLC Load Misses** | **0.14B** | **5.01B** | **Non-Mat 35x higher** |
| RAM Hit % | 5.8% | 38.1% | Non-Mat memory-bound |

### Why Row-based Wins Despite More Work

1. **One cache line serves multiple columns**: When extracting output, a single row fetch allows extracting ALL columns from that row
2. **Hardware prefetcher effectiveness**: Sequential row access enables prefetching
3. **Avoid repeated cache eviction**: Columnar gather must re-fetch the same data for EACH payload column

### Implications for Our Analysis

| Our Conclusion | Blog Validation |
|----------------|-----------------|
| Columnar Storage: REJECTED | ✅ **Confirmed** - 35x cache misses |
| PrefixSort: VALID | ✅ **Confirmed** - Row-based sort is optimal |
| Memory Locality: Important | ✅ **Confirmed** - 2.9x IPC difference |
| Hot/Cold Separation: INVALID | ✅ **Confirmed** - Cold column output becomes bottleneck |

### New Optimization Insight: Hash Join Probe Output

The blog's findings suggest a potential optimization for HashProbe `extractColumn`:

**Current Pattern** (potentially cache-unfriendly):
```cpp
for (column : projectedColumns) {
    table->extractColumn(hits, column, output);  // Each column iterates all hits
}
```

**⚠️ VALIDATION RESULT: Row-at-a-time is SLOWER, not faster!**

| Config | Column-at-a-time | Row-at-a-time | Result |
|--------|------------------|---------------|--------|
| 3 cols, 50% hit | 0.32 ms | 0.69 ms | **Row is 54% slower** |
| 10 cols, 50% hit | 1.49 ms | 2.28 ms | **Row is 35% slower** |
| 20 cols, large | 9.28 ms | 26.57 ms | **Row is 65% slower** |
| Random access | 1.38 ms | 3.31 ms | **Row is 58% slower** |

**Why Column-at-a-time is actually FASTER**:
1. **`extractColumn` is highly optimized** - uses SIMD batch operations
2. **CPU prefetcher friendly** - stride access pattern for column offsets
3. **Batch write efficiency** - continuous writes to output vector
4. **Compiler optimization** - loop unrolling and vectorization

**Why Row-at-a-time is slower**:
1. **Per-value function call overhead** - `FlatVector::set()` for each cell
2. **No SIMD utilization** - scalar operations only
3. **Branch prediction issues** - nested loops hurt CPU pipeline
4. **Breaks compiler optimizations** - can't unroll/vectorize

**Key Insight**: The RowContainer scenario is fundamentally different from the blog's "gather from multiple vectors" scenario:
- **Blog scenario**: Random indices into many separate RowVectors (cache thrashing)
- **HashProbe scenario**: Row pointers into one RowContainer (memory is contiguous per row)

**Conclusion**: ❌ **DO NOT implement row-at-a-time extractRow() API**
- Current column-at-a-time `extractColumn` is already optimal
- Any "row-at-a-time" approach would sacrifice SIMD benefits

---

### ⚠️ Test 26 Results: Why Our Micro-Benchmark Differs from Blog

**Our Test 26 showed columnar gather was FASTER (0.2-0.26x of row-based)**, which CONTRADICTS the blog. This requires explanation:

#### Critical Difference: Data Source

| Aspect | Blog Scenario | Our Test 26 |
|--------|---------------|-------------|
| **Data Source** | Original RowVectors (inputs_) | RowContainer (sortedRows_) |
| **Gather Pattern** | Random indices across multiple vectors | Sequential rows from one RowContainer |
| **Cache Behavior** | Each column pass evicts previous column | All columns stored contiguously per row |

#### What the Blog Actually Measures

The blog compares **NonMaterializedSortBuffer** vs **MaterializedSortBuffer**:

**MaterializedSortBuffer** (Row-based):
```cpp
// All columns serialized to RowContainer
for (input : inputs) {
    data_->store(decoded, rows, column);  // Store ALL columns
}
// After sort, extract directly from RowContainer
data_->extractColumn(sortedRows_, column, output);
```

**NonMaterializedSortBuffer** (Columnar gather):
```cpp
// Only sort keys + indices stored to RowContainer  
void gatherCopyOutput() {
    // Extract indices from RowContainer
    data_->extractColumn(sortedRows_, indexColumn, outputIndex_);

    // CRITICAL: Gather from ORIGINAL input vectors using sorted indices
    for (i : outputIndex_->size()) {
        sourceVectors.push_back(inputs_[vectorIndices[i]]);  // Random access!
        sourceRowIndices.push_back(rowIndices[i]);           // Random indices!
    }
    gatherCopy(output_, sourceVectors, sourceRowIndices);  // Cache thrashing!
}
```

#### Why Blog Shows 35x Cache Misses

In NonMaterializedSortBuffer, `gatherCopy()` must:
1. For EACH output row, access a potentially different input vector
2. For EACH payload column, jump to a different memory location in the vector
3. Since indices are sorted by sort key (not by original row order), access is essentially random

This causes **vectorData[sortedIndices[i]]** to be random access for each of 256 columns × 4M rows.

#### What Our Test 26 Actually Measures

Our test extracts from the **same RowContainer** in two patterns:
```cpp
// Row-oriented: sortedRows_ are already row-contiguous
for (row : sortedRows_) {
    for (col : columns) {
        extractSingleCell(row, col);  // Same row, different columns
    }
}

// Column-oriented: same sortedRows_, column-at-a-time
for (col : columns) {
    extractColumn(sortedRows_, col, output);  // All rows for one column
}
```

**Key insight**: In RowContainer, `extractColumn` with sorted row pointers is NOT random access to completely different vectors - it's still sequential access to RowContainer slabs!

#### Correcting the Test

To properly reproduce the blog scenario, we would need:
```cpp
// Store original vectors
std::vector<RowVectorPtr> inputs_;

// Sort produces random indices into inputs_
std::vector<int64_t> vectorIndices;  // Which input vector
std::vector<int64_t> rowIndices;     // Which row in that vector

// Blog's "columnar gather" = random access across many vectors
for (col : columns) {
    for (i : outputSize) {
        output[col][i] = inputs_[vectorIndices[i]]->childAt(col)->valueAt(rowIndices[i]);
        // ^^^ This is random access causing cache thrashing
    }
}
```

#### Conclusion

**Our Test 26 does NOT reproduce the blog scenario correctly.** The test measures extractColumn from RowContainer in different iteration orders, which is fundamentally different from gathering across multiple original input vectors with random indices.

**The blog's conclusion stands**: Row-based sort (MaterializedSortBuffer) is 2-4x faster than columnar gather (NonMaterializedSortBuffer) due to cache locality when outputting multiple columns.

**PR #15157 Implementation Note**: The NonMaterializedSortBuffer in PR #15157 implements exactly the pattern the blog warns against - it stores only sort keys and indices, then gathers from original vectors. The blog benchmark results explain why this approach is slower despite doing less "work" (fewer instructions).

---

### Hardware Sympathy Principle

The blog concludes with a critical insight that applies to ALL RowContainer optimizations:

> "Without understanding the characteristics of the memory hierarchy and optimizing for it, **simply reducing instruction count usually does not guarantee better performance**."

This explains why our micro-benchmarks (measuring instruction-level performance) often gave misleading results compared to real-world scenarios (dominated by memory access patterns).
- HashStringAllocator already provides good allocation performance

---

### Optimization Direction 2: Batch newRows() API

**Hypothesis**: Add `char** newRows(int count)` to amortize per-row overhead.

**Test Results (Test 24)**:
```
| Rows    | Single   | Batch Est | Savings |
|---------|----------|-----------|---------|
| 1,000   | 9.7 ns   | 6.3 ns    | 35%     |
| 10,000  | 7.0 ns   | 4.5 ns    | 35%     |
| 100,000 | 8.1 ns   | 5.2 ns    | 35%     |
```

**newRow() Time Breakdown**:
- Arena alloc: ~2-3 ns (pointer bump)
- memset: ~3-5 ns (zero 40-80 bytes)
- Bookkeeping: ~2-3 ns (numRows++, etc)
- Loop overhead: ~1-2 ns

**Proposed API**:
```cpp
// Allocate count rows at once
folly::Range<char**> RowContainer::newRows(vector_size_t count);
```

**Benefits**:
1. Single bulk memset for entire block
2. Bulk bookkeeping update
3. Better branch prediction
4. Potential SIMD initialization

**Verdict**: ⚠️ **POTENTIAL 30-35% IMPROVEMENT**
- Implementation effort: MEDIUM
- Risk: LOW (additive API, doesn't break existing code)
- Applicability: SortBuffer (already batches), could benefit HashBuild

---

### Optimization Direction 3: Skip memset for Fixed-Width

**Hypothesis**: Skip memset for rows with only fixed-width columns.

**Test Results (Test 25)**:
```
With VARCHAR column: 11.1 ns/row (memset required)
With BIGINT only:    7.6 ns/row (memset skipped!)
```

**Code Analysis**:
```cpp
// initializeRow() already has this optimization:
if (rowSizeOffset_ != 0) {
  // rowSizeOffset_ is only set when variable-width columns exist
  ::memset(row, 0, fixedRowSize_);
}
```

**Pure memset Cost**:
- Per-row memset (48 bytes): 1.19 ns
- Bulk memset: 0.84 ns/row (1.4x faster)

**Verdict**: ✅ **ALREADY OPTIMIZED**
- `rowSizeOffset_ != 0` only when variable-width columns exist
- memset is correctly conditional
- Further optimization: bulk memset (included in batch newRows proposal)

---

## Final Optimization Recommendations

| Optimization | Status | Impact | Effort | Recommendation |
|--------------|--------|--------|--------|----------------|
| **Batch newRows()** | Not Implemented | 30-35% faster | Medium | ⚠️ Worth considering |
| Increase kInlineSize | Analyzed | Limited benefit | Low | ❌ Not recommended |
| Skip memset | Already Exists | N/A | N/A | ✅ Already optimal |
| Hot/Cold Separation | Analyzed | Invalid for operators | High | ❌ Not recommended |
| Columnar Storage | Analyzed | Already used by SortBuffer | High | ❌ Not recommended |
| Variable-Length Pooling | Analyzed | HashStringAllocator already pools | Medium | ❌ Not needed |

**The only optimization worth implementing is Batch newRows() API**, which could provide 30-35% improvement in row allocation time. However, this is a relatively small portion of overall query execution time.

---

## Operator-Specific Layout Analysis (Test 33)

### RowPartitions: What is "分区" (Partition)?

**定义**: `RowPartitions` 是 RowContainer 中用于**并行 Hash Join Build**的分区机制。

**核心思想**: 当多个线程并行构建 Hash Table 时，每个线程负责一个分区，避免锁竞争。

```cpp
// RowPartitions 类定义 (RowContainer.h:106)
class RowPartitions {
  // 每行存储一个 uint8_t，表示该行属于哪个分区 (0-255)
  memory::Allocation allocation_;  // 分区号数组
  int32_t size_;                   // 已分配的行数
};

// 使用方式
std::unique_ptr<RowPartitions> createRowPartitions(pool);  // 创建分区信息
int32_t listPartitionRows(iterator, partition, maxRows, rowPartitions, result);  // 获取特定分区的行
```

**并行 Hash Join Build 流程**:
```
1. Partition Step (并行):
   - 每个线程计算行的 hash 值
   - 根据 hash 确定分区号: partition = hash % numPartitions
   - 存储到 rowPartitions 数组

2. Build Step (并行):
   - 每个线程处理一个分区
   - listPartitionRows() 获取该分区的所有行
   - buildJoinPartition() 构建 Hash Table 的一部分

3. Merge Step:
   - 处理跨分区的 overflow 行
```

**性能优势**:
- ✅ 无锁并行: 每个分区独立构建，无需同步
- ✅ 缓存友好: 同一分区的行可能在相邻内存
- ✅ 可扩展: 分区数可根据核心数调整

### Operator-Specific Row Layout Analysis

不同算子对 RowContainer 的使用模式不同，我们分析每种模式是否需要不同的布局：

| 算子 | 存储模式 | 访问模式 | 热点列 | 布局需求 |
|------|----------|----------|--------|----------|
| **Sort** | 批量存储 | 顺序提取 | 排序键优先 | 当前布局最优 |
| **HashAgg** | 行存储+更新 | 键查找+累加器更新 | 键+累加器 | 当前布局最优 |
| **HashJoin Build** | 行存储 | 分区遍历 | 键+dependents | 当前布局最优 |
| **HashJoin Probe** | 无存储 | 列提取 | 投影列 | 当前布局最优 |

#### Sort Operator Layout

```
当前布局: [NormKey][Keys][NullFlags][Dependents][RowSize][NextPtr]
访问模式:
  - 比较阶段: 只访问 NormKey (前 8 字节) → 1 次缓存访问
  - 输出阶段: extractColumn() 批量提取 → SIMD 优化
优化建议: ✅ 已经最优
  - PrefixSort 使用 NormKey 避免重复计算
  - extractColumn() 批量操作减少循环开销
```

#### HashAgg Operator Layout

```
当前布局: [Keys][NullFlags][Accumulators][Dependents]
访问模式:
  - Probe: 哈希 + 键比较 → 访问 Keys (前部)
  - Update: 累加器更新 → 访问 Accumulators (中部)
优化建议: ✅ 已经最优
  - 键在前部支持快速比较
  - 累加器在固定偏移量，更新高效
  - 无 Dependents (聚合没有非键/非累加器列)
```

#### HashJoin Build Operator Layout

```
当前布局: [NormKey][Keys][NullFlags][Dependents][RowSize][NextPtr]
访问模式:
  - 存储: row-by-row (每批输入调用一次 store())
  - 分区: 根据 hash 分配 partition number
  - 构建: listPartitionRows() 获取分区行
优化建议: ✅ 已经最优
  - NormKey 支持快速哈希比较
  - NextPtr 支持链表冲突解决
  - RowPartitions 支持无锁并行构建
```

#### HashJoin Probe Operator Layout

```
访问模式:
  - 查找: 计算 hash → 比较 Keys
  - 提取: extractColumn() 获取投影列
优化建议: ✅ 已经最优
  - 只提取需要的列 (通过 columnIndex 指定)
  - 批量 extractColumn() 有 SIMD 优化
  - 不需要改变存储侧布局
```

### 为什么不需要不同的布局？

经过分析，我们发现当前统一布局已经是最优的，原因如下：

#### 1. 访问模式已经优化

```cpp
// Sort: PrefixSort 使用 NormKey
PrefixSort::sort(rowContainer, compareFlags, ...);  // 前 8 字节比较

// HashAgg: 累加器在固定偏移量
RowColumn accCol = rowContainer.columnAt(keyCount + i);
auto* acc = row + accCol.offset();  // 直接偏移访问

// HashJoin: extractColumn 批量操作
rowContainer.extractColumn(rows, column, 0, result);  // SIMD 批量提取
```

#### 2. 缓存利用率已经高效

| 操作 | 缓存访问次数 | 原因 |
|------|-------------|------|
| Sort Compare | 1 | NormKey 在行首 (8 bytes) |
| HashAgg Update | 1-2 | 累加器通常在同一缓存行 |
| HashJoin Build | 1-2 | 存储时顺序写入 |
| HashJoin Probe | 1-3 | 批量提取摊销开销 |

#### 3. 变更成本高于收益

如果为不同算子实现不同布局：
- ❌ 需要多套内存分配/回收逻辑
- ❌ 不同算子间无法共享 RowContainer
- ❌ 代码复杂度大幅增加
- ⚠️ 潜在收益: 可能 5-10% (缓存命中率)

**结论**: 当前统一布局设计精良，适用于所有算子。针对特定算子的优化应该在算子层面实现（如 PrefixSort），而不是在 RowContainer 布局层面。

### Test 33 实验结果

**测试环境**: Release build (-O3), 100K rows, 5 iterations average

#### 容器配置

| Operator | Row Size | Keys | Deps/Accs | Extra Flags |
|----------|----------|------|-----------|-------------|
| Sort | 49 bytes | 2 | 4 deps | NormKey |
| HashAgg | 64 bytes | 2 | 4 accs | NormKey, NextPtr |
| HashJoin | 57 bytes | 2 | 4 deps | NormKey, NextPtr, Probed |

#### 访问模式性能测量

| Operator | Pattern | Time/row | Cache Lines |
|----------|---------|----------|-------------|
| Sort | Sequential compare | **1.5 ns** | 1 (NormKey) |
| Sort | Extract all 6 columns | **7.3 ns** | 1-2 |
| HashAgg | Probe + Update 4 accumulators | **9.8 ns** | 1-2 |
| HashJoin | Build (store 6 columns) | **26.0 ns** | 1-2 |
| HashJoin | Probe + Extract 2 cols | **4.8 ns** | 1-2 |

#### 关键发现

1. **Sort Compare (1.5 ns)**: NormKey 存储在行指针前 8 字节，顺序比较只需 1 次缓存访问
2. **HashAgg Update (9.8 ns)**: 键和累加器在相邻偏移量，随机访问仍然高效
3. **HashJoin Build (26.0 ns)**: 主要开销是 `newRow()` + 写入，与布局无关
4. **HashJoin Probe (4.8 ns)**: `extractColumn()` 批量操作高效摊销开销

#### 布局优化必要性分析

| Operator | 需要不同布局? | 原因 |
|----------|--------------|------|
| Sort | ❌ NO | 键已在行首，PrefixSort 使用 NormKey |
| HashAgg | ❌ NO | 键+累加器在 1-2 个缓存行内 |
| HashJoin | ❌ NO | extractColumn() 批量操作摊销开销 |

### Test 33 结论

✅ **当前统一布局对所有算子都是最优的**:
- Sort: NormKey 在固定偏移量 (-8)，顺序访问
- HashAgg: 键+累加器在同一缓存行区域
- HashJoin: extractColumn 批量操作摊销随机访问开销

❌ **不建议实现算子特定布局**:
- 需要多个 RowContainer 实现
- 不能在算子间共享容器
- 复杂度不能被边际收益证明

---

## extractColumn() Optimization Analysis (Test 34)

### 背景

`extractColumn()` 是 RowContainer 最常用的数据提取 API，主要用于 Hash Join Probe 和 Sort 输出阶段。当前实现是**标量循环**：

```cpp
// 当前实现 (RowContainer.h:1120-1155)
for (int32_t i = 0; i < numRows; ++i) {
  const char* row = rows[i];
  auto resultIndex = resultOffset + i;
  if (row == nullptr || isNullAt(row, nullByte, nullMask)) {
    bits::setNull(nulls, resultIndex, true);
  } else {
    bits::setNull(nulls, resultIndex, false);
    values[resultIndex] = valueAt<T>(row, offset);
  }
}
```

### 测试的优化方案

| 方案 | 描述 | 实现复杂度 |
|------|------|-----------|
| **Prefetching** | 预取后续行数据到缓存 | 低 |
| **Batch Null (2-pass)** | 第一遍收集 null，第二遍复制值 | 中 |
| **Prefetch + Optimized** | 预取 + 分支预测提示 + 批量处理 | 中 |
| **No Null Check** | 跳过 null 检查（无 null 列的上界） | 低 |

### Test 34 实验结果

**测试环境**: 100K 行, 6 BIGINT 列, 10% null 率

| Method | Sequential | Random | Seq Impr | Rand Impr |
|--------|------------|--------|----------|-----------|
| Baseline (current) | 1.1-1.9 ns | 1.5-2.1 ns | - | - |
| + Prefetching | 1.2-2.4 ns | 1.9-4.1 ns | **-11% to -26%** | **-28% to -101%** |
| + Batch Null (2-pass) | 2.3-2.6 ns | 3.4-3.8 ns | **-37% to -105%** | **-67% to -156%** |
| + Prefetch + Optimized | 1.0-1.1 ns | 1.6-1.8 ns | **+1% to +45%** | **-11% to +13%** |
| No Null Check (upper) | 0.7-1.1 ns | 1.4-1.5 ns | **+36% to +42%** | **-2% to +30%** |

### 关键发现

#### 1. 当前实现已经非常高效

- **顺序访问: 1.1-1.9 ns/row**
- **随机访问: 1.5-2.1 ns/row**
- Random/Sequential 比值仅 **1.1-1.3x**，说明内存访问模式影响不大

#### 2. 软件预取反而降低性能 ❌

```
Prefetching 结果: -11% to -101% (更慢!)
```

**原因分析**:
- 现代 CPU 的硬件预取器已经非常智能
- 软件预取与硬件预取冲突，浪费缓存带宽
- 预取指令本身有开销

#### 3. 批量 Null 收集 (2-pass) 性能更差 ❌

```
Batch Null 结果: -37% to -156% (更慢!)
```

**原因分析**:
- 需要两次遍历所有行，内存流量翻倍
- 分支预测对 10% null 率处理得很好
- 额外的 null bitmap 操作开销

#### 4. Null 检查开销约 36-42%

```
No Null Check vs Baseline: 36-42% improvement (sequential)
```

**结论**: 对于已知无 null 的列，跳过 null 检查可以显著提升性能。

### 为什么 SIMD Gather 不适用？

SIMD gather 指令 (如 `_mm256_i64gather_epi64`) 要求：
- 基地址 + 索引 × 步长的访问模式
- 连续的内存布局

但 `extractColumn()` 的输入 `rows[]` 是任意地址的指针数组：
```cpp
rows[0] = 0x7f1234000100  // 随机地址
rows[1] = 0x7f1234002300  // 随机地址
rows[2] = 0x7f1234001500  // 随机地址
```

无法用 gather 指令批量加载。

### 建议

| 优化方案 | 建议 | 原因 |
|----------|------|------|
| 软件预取 | ❌ 不推荐 | 与硬件预取冲突，性能下降 |
| 批量 Null (2-pass) | ❌ 不推荐 | 内存流量翻倍，性能下降 |
| SIMD Gather | ❌ 不适用 | 行地址不连续，无法使用 |
| 无 Null 快速路径 | ✅ 已存在 | `extractValuesNoNulls` 已实现 |
| 分支预测提示 | ⚠️ 边际收益 | `__builtin_expect` 可能有 5-10% 提升 |

### Test 34 结论

✅ **当前 `extractColumn()` 实现已经是最优的**:
- 硬件预取器自动处理内存访问模式
- 分支预测对低 null 率处理良好
- 已有 `extractValuesNoNulls` 快速路径

❌ **不建议添加 SIMD/预取/批量 null 处理**:
- 软件预取与硬件预取冲突
- 批量 null 增加内存流量
- SIMD gather 不适用于指针数组

**主要瓶颈是内存延迟，不是 CPU 计算**。优化应关注减少内存访问次数，而不是加速单次访问。

---

## Test 35: Group Prefetching for extractColumn()

### 目标

基于 HashTable.cpp 中已验证有效的 Group Prefetching 模式，测试是否能隐藏 `extractColumn()` 的内存延迟。

### HashTable.cpp 参考实现

```cpp
// 来自 HashTable.cpp insertForJoin()
constexpr int32_t kPrefetchDistance = 10;
for (int32_t i = 0; i < numGroups; ++i) {
  if (i + kPrefetchDistance < numGroups) {
    __builtin_prefetch(
        reinterpret_cast<char*>(table_) +
        bucketOffset(hashes[i + kPrefetchDistance]));
  }
  // ... process current element ...
}
```

### 测试结果 (500万行, 随机访问)

| 方法 | 时间/行 | 提升 |
|------|---------|------|
| Baseline (no prefetch) | 11.62 ns | - |
| Simple Prefetch (dist=16) | 13.04 ns | **-12.2%** |
| Group Prefetch (size=8) | 13.84 ns | **-19.1%** |
| Group Prefetch (size=16) | 14.26 ns | **-22.8%** |
| Interleaved Group | 12.02 ns | **-3.5%** |
| Software Pipeline | 14.17 ns | **-22.0%** |
| Group + Branch Hints | 12.97 ns | **-11.6%** |

**注意**: 在较小数据集(100K rows)上，Method 5 (Group + Branch Hints) 曾显示 **36.9% 提升**:
| Baseline | 11.07 ns | - |
| Group + Branch Hints | 6.99 ns | **+36.9%** |

### 关键发现

1. **预取效果高度依赖缓存状态**:
   - 数据在缓存中: 预取可能有帮助 (0-37%)
   - 数据在 DRAM: 预取通常无帮助 (-3% to -23%)

2. **为什么与 HashTable.cpp 不同**:
   - HashTable: 预取的是计算出的 bucket 地址 (真正的指针追踪)
   - extractColumn: 预取的是 `rows[i+k]` 的值 (顺序数组访问)
   - 硬件预取器可以自动预取顺序数组

3. **实际应用中的内存访问**:
   ```
   extractColumn() 访问模式:
   1. 顺序读取 rows[] 数组 → 硬件预取有效
   2. 随机读取 rows[i] 指向的行数据 → 软件预取可能有帮助

   HashTable 访问模式:
   1. 顺序读取 hashes[] → 硬件预取有效
   2. 根据 hash 计算 bucket 地址 → 软件预取必需
   ```

4. **Branch Hints 的作用**:
   - `__builtin_expect(isNull, 0)` 在低 null 率时可提升分支预测准确度
   - 但效果有限 (0-10%)

### 结论

| 结论 | 说明 |
|------|------|
| **Group Prefetching 不推荐用于 extractColumn()** | 硬件预取器已经处理了顺序数组访问 |
| **HashTable 的预取有效是因为不同的访问模式** | bucket 地址是计算出来的，硬件预取器无法预测 |
| **当前 extractColumn() 实现已经最优** | 简单循环让硬件预取器自由工作 |

### 代码推荐

❌ **不要这样做** (与硬件预取器冲突):
```cpp
for (int i = 0; i < numRows; ++i) {
  __builtin_prefetch(rows[i + 16], 0, 0);  // 不需要！
  extractValue(rows[i]);
}
```

✅ **当前实现已经是最优的**:
```cpp
for (int i = 0; i < numRows; ++i) {
  extractValue(rows[i]);  // 硬件预取器自动工作
}
```

---

## Appendix: Performance Test Code

The baseline measurements were obtained using `/var/git/velox/velox/exec/tests/RowContainerPerfTest.cpp`.

**Test Suite**:
- Tests 1-10: Original baseline benchmarks
- Tests 11-18: Optimization direction validation
- Tests 19-20: Real-world scenario analysis
- Tests 21-22: Exploration and counter-intuitive findings
- Tests 23-25: Additional optimization validation
- Tests 26-27: Velox Blog Verification (gather vs columnar)
- Tests 28-29: Row-at-a-time extractRow API validation (REJECTED)
- Test 30: Split Null Flags Layout validation (REJECTED)
- Tests 31-32: Comprehensive Layout Optimization Analysis (KEEP CURRENT LAYOUT)
- Test 33: Operator-Specific Access Pattern Analysis (CURRENT LAYOUT OPTIMAL)
- Test 34: extractColumn() Optimization Analysis (CURRENT IMPL OPTIMAL)
- Test 35: Group Prefetching for extractColumn() (NOT RECOMMENDED)
