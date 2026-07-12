#pragma once

#include <cstddef>
#include <cstdint>

namespace hyrise {

// Shared vocabulary and tuning constants for the AggregateDYOD operator, a parallel radix-partitioned hash
// aggregation: it estimates group-by cardinality, scatters input rows into cache-sized radix partitions using
// software write-combining (SWWC) buffers, then merges each partition independently into a dense hash map. See
// aggregate_dyod.hpp for the operator itself and the phase overview.
//
// The tuning constants below are machine-independent inputs, not baked-in hardware assumptions. Anything that should
// scale with the deployment machine (most importantly the partition count) is derived from them at runtime; see
// choose_partition_count() in hyperloglog.hpp. Overriding them for experimentation is expected. The eventual home for
// hardware-derived values such as cache sizes is Hyrise's Topology, not this file.

/**
 * A count of radix partitions produced by the scatter phase.
 *
 * Holds the per-query partition count P, chosen once in the estimate phase and used unchanged by scatter and merge. P
 * governs how finely the key space is split: larger P means smaller per-partition merge maps.
 *
 * Invariants: P is a power of two, so a partition index is the low log2(P) bits of a key's hash; P lies in
 *   [min(next_pow2(max(worker_count, 1)), MAX_PARTITION_COUNT), MAX_PARTITION_COUNT].
 *
 * Ownership/lifetime/threading: a plain value computed at the estimate barrier and thereafter read-only, so every
 *   worker shares it across the scatter and merge phases without synchronization.
 *
 * @see choose_partition_count() (hyperloglog.hpp) for how P is derived, MAX_PARTITION_COUNT for the ceiling, and
 *   PartitionId for a single-partition index.
 */
using PartitionCount = uint32_t;

/**
 * Index identifying a single radix partition.
 *
 * A value in [0, P) selecting one of the P partitions; it equals the low log2(P) bits of a key's hash, which is how
 * the scatter phase routes each row and the merge phase claims work.
 *
 * Invariants: a valid id is strictly less than the current PartitionCount.
 *
 * @see PartitionCount for the number of partitions.
 */
using PartitionId = uint32_t;

/**
 * Upper clamp on the derived partition count P (unit: partitions; always a power of two).
 *
 * Bounds per-partition bookkeeping and the SWWC staging footprint, which grows linearly in P (one staging line per
 * (stream, partition) pair). The lower bound on P is the worker count, applied in choose_partition_count() rather than
 * fixed here.
 *
 * Raising it permits a finer radix split on very high-cardinality inputs at the cost of more per-partition state and a
 * larger staging footprint; lowering it caps that split and can leave per-partition merge maps too large to stay
 * cache-resident.
 *
 * @see choose_partition_count() (hyperloglog.hpp), KEYS_BUDGET, SWWC_LINE_BYTES.
 */
constexpr PartitionCount MAX_PARTITION_COUNT = 8192;

/**
 * Target number of distinct keys per partition (unit: distinct keys); the divisor when sizing P from the estimate.
 *
 * The partition count is chosen so that a single partition's dense merge map -- its probe index plus dense
 * key/accumulator storage -- is expected to stay resident in a mid-level cache during the merge phase, the
 * cache-residency the radix split is designed to provide. This is a distinct-key budget, not a byte budget, and is
 * deliberately conservative so the accumulator columns (whose per-slot width depends on the requested aggregates) also
 * fit.
 *
 * Raising it yields fewer, larger partitions whose merge maps risk spilling out of cache; lowering it yields more,
 * smaller partitions at the cost of higher per-partition overhead.
 *
 * @see choose_partition_count() (hyperloglog.hpp), MAX_PARTITION_COUNT.
 */
constexpr size_t KEYS_BUDGET = 8192;

/**
 * Row-count cutoff at or above which the estimate phase runs in parallel (unit: input rows).
 *
 * At or above this many rows the estimate builds per-worker HyperLogLog sketches merged register-wise; below it a
 * single-threaded pass over the group-by columns is cheaper than the scheduling overhead.
 *
 * Raising it forces larger inputs through the single-threaded path; lowering it parallelizes smaller inputs and pays
 * scheduling overhead sooner.
 *
 * @see HLL_PRECISION for the per-worker sketch precision.
 */
constexpr size_t PARALLEL_ESTIMATE_THRESHOLD = 100'000;

/**
 * HyperLogLog register precision: the sketch uses 2^HLL_PRECISION registers (unit: bits of precision).
 *
 * Precision 12 gives roughly 1.6% standard error at a few KiB per sketch -- accurate enough to size the partition
 * count and the per-partition map, cheap enough to keep one sketch per worker. The sketch is fed the same packed-key
 * hash the scatter and merge phases use, so the estimate predicts the exact quantity those phases encounter.
 *
 * Raising it shrinks the estimate's standard error (steadier P sizing) at more memory and work per sketch; lowering it
 * saves memory but degrades the estimate and thus the partition-count choice.
 */
constexpr uint8_t HLL_PRECISION = 12;

/**
 * Software write-combining staging line size (unit: bytes).
 *
 * Each (stream, partition) pair buffers this many bytes before a non-temporal flush to the partition's region; sized
 * to one cache line so the flush is exactly one write-combining transaction. Decoupled from the scatter morsel
 * granularity (one input chunk), so morsel size does not affect it.
 *
 * Keep it a whole cache line: a value that is not the hardware cache-line size splits or wastes write-combining
 * transactions and undoes the non-temporal store benefit.
 */
constexpr size_t SWWC_LINE_BYTES = 64;

/**
 * Row tile size for the merge phase's resolve+fold step (unit: rows).
 *
 * Each partition's rows are folded in tiles of this many rows so the transient row->slot index scratch
 * (MERGE_TILE_ROWS * sizeof(dense index)) stays L1-resident rather than growing to the whole partition. It is also the
 * granularity at which AbstractAccumulatorColumn::fold is dispatched: one virtual call per tile, amortized over a tight
 * typed loop, never per row.
 *
 * Raising it grows the scratch until it no longer fits in L1 (defeating the tiling); lowering it shrinks the scratch
 * but pays more virtual fold dispatches per partition.
 *
 * @see accumulator_column.hpp for the fold interface.
 */
constexpr size_t MERGE_TILE_ROWS = 2048;

/**
 * Inline string-blob capacity budget per string group-by column (unit: bytes).
 *
 * The total inline blob width of a string-involving key scales as
 * (STRING_BLOB_BYTES_PER_COLUMN * number_of_string_columns); a row whose length-prefixed string content exceeds its
 * blob spills to a per-partition spill buffer. Chosen so typical short strings stay inline -- no pointer chase on the
 * equality hot path -- while the fixed part of the key stays compact.
 *
 * Raising it keeps longer strings inline (fewer spills) but widens every key's fixed part, so fewer keys stay
 * cache-resident; lowering it keeps keys compact but spills more strings to the per-partition buffer.
 *
 * @see key_schema.hpp for the packed-key layout and the spill path.
 */
constexpr size_t STRING_BLOB_BYTES_PER_COLUMN = 16;

}  // namespace hyrise
