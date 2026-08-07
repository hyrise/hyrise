#pragma once

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>

#include "operators/aggregate_dyod/cache_info.hpp"

namespace hyrise {

// Shared vocabulary and tuning values for the AggregateDYOD operator, a parallel radix-partitioned hash
// aggregation: it estimates group-by cardinality, scatters input rows into cache-sized radix partitions using
// software write-combining (SWWC) buffers, then merges each partition independently into a dense hash map. See
// aggregate_dyod.hpp for the operator itself and the phase overview.
//
// The compile-time constants below are machine-independent inputs, not baked-in hardware assumptions. Values that
// should scale with the deployment machine come in two steps: cache-dependent budgets (merge_tile_rows() from L1d,
// max_partition_count() from L2, keys_budget() from L3) are a compile-time fraction applied to the queried cache
// sizes (see cache_info.hpp), and the partition count is derived from those budgets per query; see
// choose_partition_count() in hyperloglog.hpp.
// Overriding the constants for experimentation is expected. The fractions are calibrated against a parameter sweep
// on EPYC 7742 (32 KiB L1d, 512 KiB L2, 16 MiB L3 per 4-core CCX), which matches FALLBACK_CACHE_SIZES, so machines
// without cache reporting behave exactly like the calibration target.

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
 * Absolute clamp on the derived partition count P (unit: partitions; always a power of two).
 *
 * Bounds per-partition bookkeeping independently of any cache size, so a machine reporting an implausibly large L2
 * cannot drive the per-partition state arbitrarily high. The cache-dependent ceiling is max_partition_count(), which
 * is the binding one on the calibration target; the lower bound on P is the worker count, applied in
 * choose_partition_count() rather than fixed here.
 *
 * @see max_partition_count() for the L2-derived ceiling, choose_partition_count() (hyperloglog.hpp), keys_budget().
 */
constexpr PartitionCount MAX_PARTITION_COUNT = 8192;

/**
 * Last-level cache bytes budgeted per distinct key in a partition's merge map (unit: bytes per key).
 *
 * Covers the probe index (8 bytes per key at the 0.5 max load factor), the packed key, and the accumulator columns
 * (whose per-slot width depends on the requested aggregates), with headroom for the input tiles streaming through
 * the same cache. 128 bytes reproduces the swept optimum of 32768 keys per partition on the calibration target.
 */
constexpr size_t MERGE_MAP_BYTES_PER_KEY = 128;

/**
 * Target number of distinct keys per partition (unit: distinct keys); the divisor when sizing P from the estimate.
 *
 * The partition count is chosen so that a single partition's dense merge map -- its probe index plus dense
 * key/accumulator storage -- is expected to stay resident in the merge worker's share of the last-level cache, the
 * cache-residency the radix split is designed to provide. The worker's share is the reported slice divided by the
 * CPUs sharing it, since every one of them runs a merge worker.
 *
 * A larger budget yields fewer, larger partitions whose merge maps risk spilling out of cache; a smaller one yields
 * more, smaller partitions at the cost of higher per-partition overhead.
 *
 * @see choose_partition_count() (hyperloglog.hpp), MAX_PARTITION_COUNT, cache_sizes() (cache_info.hpp).
 */
inline size_t keys_budget_for(const CacheSizes& sizes) {
  return sizes.l3_bytes / sizes.llc_sharing_cpus / MERGE_MAP_BYTES_PER_KEY;
}

inline size_t keys_budget() {
  return keys_budget_for(cache_sizes());
}

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
 * Number of chunks the estimate phase aims to feed into its sketch (unit: chunks).
 *
 * The estimate exists to size the partition count and the per-partition maps, so it does not need every row: above
 * this many chunks, the phase samples every estimate_sample_stride()-th chunk and rescales the result via
 * scale_sampled_estimate(). 16 full chunks are about one million rows, plenty for a power-of-two choice of P.
 */
constexpr size_t ESTIMATE_SAMPLE_CHUNKS = 16;

/** Chunk stride of the estimate phase: sample every k-th chunk so about ESTIMATE_SAMPLE_CHUNKS chunks are read. */
inline size_t estimate_sample_stride(const size_t chunk_count) {
  return std::max(size_t{1}, chunk_count / ESTIMATE_SAMPLE_CHUNKS);
}

/**
 * Rescale a sketch estimate taken over every stride-th chunk to the full input.
 *
 * Rows-per-key inside the sample cannot tell a plateaued key space from keys that merely repeat within their own
 * chunk while every chunk brings new ones, so the growth between a half-size sample and the full sample decides the
 * scaling: an estimate that did not grow is the true cardinality, one that doubled grows linearly with the input and
 * is scaled by the stride, and partial growth is extrapolated as stride^log2(growth). The result is clamped to
 * [estimate, min(total rows, estimate * stride)]; an empty sample carries no information and yields the row count.
 */
inline size_t scale_sampled_estimate(const size_t estimate, const size_t half_sample_estimate,
                                     const size_t total_row_count, const size_t stride) {
  if (stride == 1) {
    return estimate;
  }
  if (estimate == 0) {
    return total_row_count;
  }
  const auto growth = std::clamp(
      static_cast<double>(estimate) / static_cast<double>(std::max(half_sample_estimate, size_t{1})), 1.0, 2.0);
  const auto scaled = static_cast<double>(estimate) * std::pow(static_cast<double>(stride), std::log2(growth));
  const auto ceiling = std::min(static_cast<double>(total_row_count), static_cast<double>(estimate * stride));
  return static_cast<size_t>(std::llround(std::clamp(scaled, static_cast<double>(estimate), ceiling)));
}

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
 * Cache-derived ceiling on the partition count P for a query scattering stream_count streams (unit: partitions).
 *
 * The scatter phase keeps one SWWC_LINE_BYTES staging line plus one fill counter per (stream, partition) pair, so its
 * hot working set is stream_count * P * (SWWC_LINE_BYTES + sizeof(size_t)) bytes and is touched on every scattered
 * row. Once that outgrows the scattering core's L2 the scatter loop slows down measurably, so P is bounded to keep it
 * resident.
 *
 * The result is rounded down to a power of two (P indexes by the low bits of a hash) and clamped by the absolute
 * MAX_PARTITION_COUNT.
 *
 * @see MAX_PARTITION_COUNT for the machine-independent clamp, choose_partition_count() (hyperloglog.hpp),
 *   ScatterHeads (scatter_store.hpp) for the staging layout.
 */
inline PartitionCount max_partition_count_for(const CacheSizes& sizes, const size_t stream_count) {
  const auto staging_bytes_per_partition = std::max(size_t{1}, stream_count) * (SWWC_LINE_BYTES + sizeof(size_t));
  const auto fitting_partitions = std::bit_floor(std::max(size_t{1}, sizes.l2_bytes / staging_bytes_per_partition));
  return static_cast<PartitionCount>(std::min(fitting_partitions, static_cast<size_t>(MAX_PARTITION_COUNT)));
}

inline PartitionCount max_partition_count(const size_t stream_count) {
  return max_partition_count_for(cache_sizes(), stream_count);
}

/**
 * Fraction of L1d granted to the merge phase's row->slot scratch: scratch budget = L1d / this (unit: divisor).
 *
 * A quarter leaves the rest of L1d to the key tile and accumulator lines the same loop touches.
 */
constexpr size_t MERGE_SCRATCH_L1_DIVISOR = 4;

/**
 * Row tile size for the merge phase's resolve+fold step (unit: rows).
 *
 * Each partition's rows are folded in tiles of this many rows so the transient row->slot index scratch
 * (merge_tile_rows() * sizeof(dense index)) stays L1-resident rather than growing to the whole partition: the tile is
 * sized so the scratch fills the L1d fraction granted by MERGE_SCRATCH_L1_DIVISOR. It is also the granularity at
 * which AbstractAccumulatorColumn::fold is dispatched: one virtual call per tile, amortized over a tight typed loop,
 * never per row.
 *
 * A larger tile grows the scratch until it no longer fits in L1 (defeating the tiling); a smaller one shrinks the
 * scratch but pays more virtual fold dispatches per partition.
 *
 * @see accumulator_column.hpp for the fold interface, cache_sizes() (cache_info.hpp).
 */
inline size_t merge_tile_rows_for(const CacheSizes& sizes) {
  return sizes.l1d_bytes / MERGE_SCRATCH_L1_DIVISOR / sizeof(uint32_t);
}

inline size_t merge_tile_rows() {
  return merge_tile_rows_for(cache_sizes());
}

/**
 * Inline string-blob capacity budget per string group-by column (unit: bytes).
 *
 * The total inline blob width of a string-involving key scales as
 * (STRING_BLOB_BYTES_PER_COLUMN * number_of_string_columns); a row whose length-prefixed string content exceeds its
 * blob spills to a per-partition spill buffer. Chosen so short codes and flags stay inline -- no pointer chase on
 * the equality hot path -- while the fixed part of the key stays compact; the narrow key pays off on the hash and
 * scatter hot paths even when longer strings spill.
 *
 * Raising it keeps longer strings inline (fewer spills) but widens every key's fixed part, so fewer keys stay
 * cache-resident; lowering it keeps keys compact but spills more strings to the per-partition buffer.
 *
 * @see key_schema.hpp for the packed-key layout and the spill path.
 */
constexpr size_t STRING_BLOB_BYTES_PER_COLUMN = 8;

/**
 * Number of distinct output groups where the low-cardinality path is taken
*/
inline size_t low_cardinality_threshold() {
  return keys_budget() / 2;
}

/**
 * Ceiling on the number of workers a phase fans out to (unit: workers).
 *
 * Under the immediate scheduler every JobTask runs sequentially on the calling thread, so per-worker state beyond one
 * store or map is pure setup and teardown; the limit is 1 there and the CPU count otherwise. It also lower-bounds the
 * partition count via choose_partition_count(), so single-threaded runs size P from the cardinality alone.
 */
inline size_t worker_limit_for(const bool is_multi_threaded, const size_t num_cpus) {
  return is_multi_threaded ? std::max(size_t{1}, num_cpus) : size_t{1};
}

}  // namespace hyrise
