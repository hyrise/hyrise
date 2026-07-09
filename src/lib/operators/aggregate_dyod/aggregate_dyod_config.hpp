#pragma once

#include <cstddef>
#include <cstdint>

namespace hyrise {

// Shared vocabulary and tuning constants for the AggregateDYOD operator. AggregateDYOD is a parallel,
// radix-partitioned hash aggregation operator: it estimates group-by cardinality, scatters input rows into cache-sized
// radix partitions using software write-combining (SWWC) buffers, then merges each partition independently into a dense
// hash map. See aggregate_dyod.hpp for the operator itself and the phase overview.
//
// The tuning constants below are machine-independent inputs, NOT baked-in hardware assumptions. Anything that should
// scale with the deployment machine (most importantly the partition count) is DERIVED from these at runtime -- see
// choose_partition_count() in hyperloglog.hpp. Overriding these for experimentation is expected; the eventual home for
// hardware-derived values (e.g. cache sizes) is Hyrise's Topology, not this file.

// The number of radix partitions produced by the scatter phase. A per-query runtime value in [MIN_PARTITION_COUNT,
// MAX_PARTITION_COUNT], always a power of two so a partition index is the low log2(P) bits of a key's hash.
using PartitionCount = uint32_t;
using PartitionId = uint32_t;

// Floor and ceiling for the derived partition count. The floor keeps at least one partition per worker so the merge
// phase can occupy every core; the ceiling bounds per-partition bookkeeping and the SWWC staging footprint (which grows
// linearly in P).
constexpr PartitionCount MAX_PARTITION_COUNT = 8192;

// Target number of distinct keys per partition. The partition count is chosen so that a single partition's dense merge
// map (its probe index plus dense key/accumulator storage) is expected to stay resident in a mid-level cache during the
// merge phase -- this cache-residency of the merge working set is the property the whole radix split exists to buy.
// This is a distinct-key budget, not a byte budget; it is deliberately conservative so the accumulator columns (whose
// per-slot width depends on the requested aggregates) also fit.
constexpr size_t KEYS_BUDGET = 8192;

// Row-count threshold above which the estimate phase runs in parallel (per-worker sketches merged register-wise). Below
// it, a single-threaded pass over the group-by columns is cheaper than the scheduling overhead.
constexpr size_t PARALLEL_ESTIMATE_THRESHOLD = 100'000;

// HyperLogLog register precision (2^HLL_PRECISION registers). Precision 12 gives ~1.6% standard error at a few KiB per
// sketch -- accurate enough to size the partition count and the per-partition map, cheap enough to keep one sketch per
// worker. Fed the SAME packed-key hash the scatter/merge phases use, so the estimate predicts the exact quantity those
// phases encounter.
constexpr uint8_t HLL_PRECISION = 12;

// SWWC staging line size, in bytes. Each (stream, partition) pair stages this many bytes before a non-temporal flush to
// the partition's region; sized to one cache line so the flush is exactly one write-combining transaction. Decoupled
// from the scatter morsel granularity (one input chunk) -- morsel size does not change this.
constexpr size_t SWWC_LINE_BYTES = 64;

// Row tile size for the merge phase's resolve+fold step. Each partition's rows are folded in tiles of this many rows so
// the transient row->slot index scratch (MERGE_TILE_ROWS * sizeof(dense index)) stays L1-resident rather than growing
// to the whole partition. Also the granularity at which AbstractAccumulatorColumn::fold is dispatched (see
// accumulator_column.hpp) -- the virtual call is amortized over a whole tile, never per row.
constexpr size_t MERGE_TILE_ROWS = 2048;

// Inline string-blob capacity budget, in bytes, per string group-by column. The total inline blob width of a
// string-involving key scales as (STRING_BLOB_BYTES_PER_COLUMN * number_of_string_columns); a row whose length-prefixed
// string content exceeds the blob spills to a per-partition spill buffer (see key_schema.hpp). Chosen so typical short
// strings stay inline (no pointer chase on the equality hot path) while the fixed part of the key stays compact.
constexpr size_t STRING_BLOB_BYTES_PER_COLUMN = 16;

}  // namespace hyrise
