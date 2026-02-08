# Plan: Add Q18 High-Cardinality Query to Benchmark

## Context

Currently benchmarks simplified TPC-H Q1 (low cardinality, 6 groups). Adding simplified Q18 (high cardinality, ~1.5M groups at SF1) to compare algorithm behavior across cardinalities. Both queries run in a single execution for SLURM.

**Q1 (current):**
```sql
SELECT l_returnflag, l_linestatus, SUM(l_quantity)
FROM lineitem
GROUP BY l_returnflag, l_linestatus
```

**Q18 (new):**
```sql
SELECT l_orderkey, SUM(l_quantity)
FROM lineitem
GROUP BY l_orderkey
```

## Approach: Simple Duplication

Duplicate algorithm functions per query. Keep it straightforward — no templates, no abstractions.

## Steps

### 1. Add config flags
Add `run_q1` and `run_q18` bools to `PlaygroundConfig`.

### 2. Duplicate hash functions for Q18
Create Q18 versions alongside existing Q1 functions:
- `hash_single_baseline_q18()` — AggregateHash with `GROUP BY l_orderkey`
- `hash_single_optimized_q18()` — key=int32_t, larger unordered_map
- `hash_multi_naive_q18()` — per-worker `unordered_map<int32_t, double>`
- `hash_multi_optimized_q18()` — FolkloreHashTable with int32_t keys, capacity sized for high cardinality

### 3. Sort Q18 — stubs only
- `sort_single_baseline_q18()` — AggregateSort with `GROUP BY l_orderkey`
- `sort_multi_naive_q18()` / `sort_multi_optimized_q18()` — return nullptr

### 4. Add `run_hash_micro_benchmark_q18()` and `run_sort_micro_benchmark_q18()` drivers

### 5. Update main() to loop both queries per scale factor

### 6. Fix `verify_tables_equal` — add `DataType::Int` (int32_t) handling

## Key Differences: Q1 vs Q18 Implementations

| Aspect | Q1 | Q18 |
|--------|----|----|
| Group-by columns | l_returnflag, l_linestatus (pmr_string) | l_orderkey (int32_t) |
| Key type in optimized | uint16_t (2 chars packed) | int32_t |
| Number of groups | 6 | ~SF * 1,500,000 |
| FolkloreHashTable capacity | 16 | next_power_of_2(estimated_groups * 2) |
| Cardinality estimation | Hardcoded (6 groups) | ~row_count / 4 (TPC-H: ~4 lineitems per order) |
| Output columns | l_returnflag (String), l_linestatus (String), sum_qty (Double) | l_orderkey (Int), sum_qty (Double) |
| segment_iterate type for group cols | `segment_iterate<pmr_string>` | `segment_iterate<int32_t>` |
| Naive multi key type | `pair<pmr_string, pmr_string>` | `int32_t` |
| encode_key / decode_key | Pack 2 chars into uint16_t | Identity (int32_t is the key) |

## FolkloreHashTable Sizing for Q18

The paper "Global Hash Tables Strike Back" emphasizes cardinality estimation for hash table sizing.

For our controlled TPC-H benchmark:
- **Estimate:** `row_count / 4` (each order has ~4 line items on average)
- **Hash table capacity:** `next_power_of_2(estimated_groups * 2)` for ~50% load factor
- At SF1: ~1.5M groups → capacity ~4M slots → ~32MB (8 bytes/slot)
- At SF32: ~48M groups → capacity ~128M slots → ~1GB

Helper for power-of-2 rounding:
```cpp
inline size_t next_power_of_2(size_t n) {
  size_t p = 1;
  while (p < n) p <<= 1;
  return p;
}
```

## Implementation Notes

### hash_single_optimized_q18
- Key = `int32_t` (l_orderkey value directly, no encoding needed)
- `std::unordered_map<int32_t, uint32_t>` for key→ticket
- Reserve based on estimated groups: `key_to_ticket.reserve(input->row_count() / 4)`
- Materialize l_orderkey with `segment_iterate<int32_t>`

### hash_multi_naive_q18
- Per-worker `std::unordered_map<int32_t, double>` (simpler than Q1's pair hash)
- Merge phase: iterate all worker maps, sum by key
- Output: l_orderkey (Int), sum_qty (Double)

### hash_multi_optimized_q18
- Duplicate FolkloreHashTable class (or make it take key type as template param)
  - `std::atomic<int32_t> key` instead of `std::atomic<uint16_t>`
  - Capacity: `next_power_of_2(input->row_count() / 2)` (~50% load)
- FuzzyTicketer: reuse as-is (already generic)
- Per-worker partial sums: `std::vector<double>` indexed by ticket (same pattern, but vectors will be much larger)

### sort_single_baseline_q18
- Same pattern as Q1 baseline but with `std::vector<ColumnID>{orderkey_col_id}`
- AggregateSort handles int32_t internally

### Output table for Q18
```cpp
auto column_definitions = TableColumnDefinitions{
    TableColumnDefinition{"l_orderkey", DataType::Int, false},
    TableColumnDefinition{"sum_quantity", DataType::Double, false}};
```

### verify_tables_equal fix
Add to the type dispatch in `table_to_set` lambda:
```cpp
} else if (data_type == DataType::Int) {
  segment_iterate<int32_t>(segment, [&](const auto& pos) {
    column_values[col_idx].push_back(pos.is_null() ? "NULL" : std::to_string(pos.value()));
  });
}
```

## main() Structure After Change

```
for each scale_factor in {1, 2, 4, 8, 16, 32}:
  if run_q1:
    print "QUERY: Q1 (Low Cardinality)"
    run_hash_micro_benchmark(sf)       // existing
    run_sort_micro_benchmark(sf)       // existing
  if run_q18:
    print "QUERY: Q18 (High Cardinality)"
    run_hash_micro_benchmark_q18(sf)   // new
    run_sort_micro_benchmark_q18(sf)   // new
```
