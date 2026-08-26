#pragma once

#include <cstddef>
#include <cstdint>

#include "operators/aggregate_dyod/cache_info.hpp"

namespace hyrise {

using PartitionCount = uint32_t;

/**
 * Index identifying a single radix partition.
 */
using PartitionId = uint32_t;

/**
 * Absolute clamp on the derived partition count P (unit: partitions; always a power of two).
 */
constexpr PartitionCount MAX_PARTITION_COUNT = 8192;

/**
 * Last-level cache bytes budgeted per distinct key in a partition's merge map (unit: bytes per key).
 */
constexpr size_t MERGE_MAP_BYTES_PER_KEY = 128;

/**
 * Target number of distinct keys per partition; the divisor when sizing P from the estimate.
 *
 * A larger budget yields fewer, larger partitions whose merge maps risk spilling out of cache; a smaller one yields
 * more, smaller partitions at the cost of higher per-partition overhead.
 */
size_t keys_budget_for(const CacheSizes& sizes);

size_t keys_budget();

/**
 * Row-count cutoff at or above which the estimate phase runs in parallel.
 *
 * Raising it forces larger inputs through the single-threaded path; lowering it parallelizes smaller inputs and pays
 * scheduling overhead sooner.
 */
constexpr size_t PARALLEL_ESTIMATE_THRESHOLD = 100'000;

/**
 * Number of chunks the estimate phase aims to feed into its sketch.
 */
constexpr size_t ESTIMATE_SAMPLE_CHUNKS = 16;

/** Chunk stride of the estimate phase: sample every k-th chunk so about ESTIMATE_SAMPLE_CHUNKS chunks are read. */
size_t estimate_sample_stride(size_t chunk_count);

/**
 * Rescale a sketch estimate taken over every stride-th chunk to the full input.
 *
 * Rows-per-key inside the sample cannot tell a plateaued key space from keys that merely repeat within their own
 * chunk while every chunk brings new ones, so the growth between a half-size sample and the full sample decides the
 * scaling: an estimate that did not grow is the true cardinality, one that doubled grows linearly with the input and
 * is scaled by the stride, and partial growth is extrapolated as stride^log2(growth). The result is clamped to
 * [estimate, min(total rows, estimate * stride)]; an empty sample carries no information and yields the row count.
 */
size_t scale_sampled_estimate(size_t estimate, size_t half_sample_estimate, size_t total_row_count, size_t stride);

/**
 * HyperLogLog register precision: the sketch uses 2^HLL_PRECISION registers.
 */
constexpr uint8_t HLL_PRECISION = 12;

/**
 * Software write-combining staging line size. Each (stream, partition) pair buffers this many bytes before a
 * non-temporal flush to the partition's region; sized to one cache line so the flush is exactly one write-combining
 * transaction.
 */
constexpr size_t SWWC_LINE_BYTES = 64;

/**
 * Cache-derived ceiling on the partition count P for a query scattering stream_count streams.
 *
 * The scatter phase keeps one SWWC_LINE_BYTES staging line plus one fill counter per (stream, partition) pair, so its
 * hot working set is stream_count * P * (SWWC_LINE_BYTES + sizeof(size_t)) bytes and is touched on every scattered
 * row. Once that outgrows the scattering core's L2 the scatter loop slows down measurably, so P is bounded to keep it
 * resident.
 *
 * The result is rounded down to a power of two (P indexes by the low bits of a hash) and clamped by the absolute
 * MAX_PARTITION_COUNT.
 */
PartitionCount max_partition_count_for(const CacheSizes& sizes, size_t stream_count);

PartitionCount max_partition_count(size_t stream_count);

/**
 * Fraction of L1d granted to the merge phase's row->slot scratch: scratch budget = L1d / this.
 *
 * A quarter leaves the rest of L1d to the key tile and accumulator lines the same loop touches.
 */
constexpr size_t MERGE_SCRATCH_L1_DIVISOR = 4;

/**
 * Row tile size for the merge phase's resolve+fold step.
 *
 * A larger tile grows the scratch until it no longer fits in L1 (defeating the tiling); a smaller one shrinks the
 * scratch but pays more virtual fold dispatches per partition. Always a multiple of 8, so per-tile slices of a
 * bit-per-row null bitmap start on byte boundaries.
 */
size_t merge_tile_rows_for(const CacheSizes& sizes);

size_t merge_tile_rows();

/**
 * Rows at which a merge partition counts as oversized, as a multiple of the mean partition's rows.
 *
 * Below this the partition is close enough to the others for the merge phase to stay balanced, and the extra maps and
 * the per-key combine a split costs are not repaid.
 */
constexpr size_t MERGE_SPLIT_MEAN_ROW_FACTOR = 4;

/**
 * Rows per expected distinct key an oversized partition must hold before it is split.
 *
 * A split folds disjoint store ranges into separate maps and combines them per key, so it pays only when the partition
 * is large because few keys repeat often: at few rows per key the combine touches nearly as many keys as the fold it
 * parallelizes.
 */
constexpr size_t MERGE_SPLIT_ROWS_PER_KEY = 8;

/**
 * Number of ways one merge partition is split across workers; 1 leaves it to a single worker.
 *
 * Each way folds a contiguous range of the scatter stores into its own map, and the maps are combined once the last
 * way is done, so a split is bounded by the stores it can be cut along as well as by the workers available to run the
 * ways. Single-threaded runs pass a worker_limit of 1 and never split. A partition qualifies only when it is oversized
 * against both the mean partition and the keys a partition is expected to hold; the number of ways then brings the
 * largest way down to about the mean partition's rows.
 */
size_t merge_split_ways_for(size_t partition_rows, size_t mean_partition_rows, size_t expected_keys_per_partition,
                            size_t store_count, size_t worker_limit);

/**
 * Inline string-blob capacity budget per string group-by column.
 *
 * Raising it keeps longer strings inline (fewer spills) but widens every key's fixed part, so fewer keys stay
 * cache-resident; lowering it keeps keys compact but spills more strings to the per-partition buffer.
 */
constexpr size_t STRING_BLOB_BYTES_PER_COLUMN = 8;

/**
 * Cap on the dictionary entries read per string group-by column when bounding the key layout.
 *
 * Raising it lets wider dictionaries be bounded at more resolve-time work; lowering it settles resolve faster but
 * leaves more group-bys on the default layout.
 */
constexpr size_t DICTIONARY_BOUND_SCAN_LIMIT = size_t{1024} * 1024;

/**
 * Number of distinct output groups where the low-cardinality path is taken.
 */
size_t low_cardinality_threshold();

/**
 * Width of the pieces a packed key is staged in during the scatter phase (unit: bytes).
 *
 * The widest of {16, 8, 4} that divides the key width, so a key needs as few push calls as possible while every
 * piece still evenly divides the SWWC staging line. Key widths are always multiples of 4.
 */
size_t key_piece_width(size_t key_width);

/**
 * Number of rows a worker claims at a time in the scanning phases: estimate, scatter, and the low-cardinality fold.
 *
 * Raising it drifts back towards chunk-granularity quantization at high worker counts; lowering it balances more
 * finely but pays the per-morsel setup more often.
 */
constexpr size_t MORSEL_ROWS = 16'384;

/** Morsels a chunk of `chunk_rows` rows is claimed in; a chunk shorter than one morsel is a single morsel. */
size_t morsel_count_for(size_t chunk_rows, size_t rows_per_morsel);

/**
 * Ceiling on the number of workers a phase fans out to.
 *
 * Under the immediate scheduler every JobTask runs sequentially on the calling thread, so per-worker state beyond one
 * store or map is pure setup and teardown; the limit is 1 there and the CPU count otherwise. It also lower-bounds the
 * partition count via choose_partition_count(), so single-threaded runs size P from the cardinality alone.
 */
size_t worker_limit_for(bool is_multi_threaded, size_t num_cpus);

}  // namespace hyrise
