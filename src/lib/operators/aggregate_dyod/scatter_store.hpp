#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "operators/aggregate_dyod/key_schema.hpp"

namespace hyrise {

struct AlignedFree {
  void operator()(std::byte* ptr) const noexcept {
    std::free(ptr);
  }
};

/**
 * A growable, 64-byte-aligned byte buffer that holds one stream's scattered bytes for a single partition.
 *
 * One Region backs exactly one (partition, stream) slot of a worker's ScatterStore during the scatter phase. Full SWWC
 * staging lines are appended with non-temporal (cache-bypassing) stores via push_line(); the single trailing partial
 * line is drained with ordinary stores at end-of-scatter via drain_partial(). Overflow grows the buffer in place --
 * there is no separate spill path. The 64-byte alignment matches a cache line and satisfies the aligned non-temporal
 * store instructions (e.g. movntdq).
 *
 * Invariants:
 *   - The backing storage stays 64-byte aligned and grows only in whole SWWC lines, so every push_line() destination
 *     is 64-byte aligned.
 *   - Over one scatter pass, push_line() handles every full line and drain_partial() is called at most once, for the
 *     final run of fewer than SWWC_LINE_BYTES bytes.
 *
 * Ownership/lifetime/threading: owned by the enclosing ScatterStore, one per (partition, stream). Written by exactly
 * one worker during scatter and never shared, so it carries no internal synchronization; its non-temporal writes
 * become visible to merge-phase readers only after the owning worker's ScatterHeads::finish() sfence plus the phase
 * barrier. Must outlive the merge reads of its partition. See ScatterStore, ScatterHeads, and SWWC_LINE_BYTES.
 */
class Region : private Noncopyable {
 public:
  /**
   * Append exactly SWWC_LINE_BYTES bytes to the buffer via a non-temporal (cache-bypassing) store.
   *
   * @param line  Source staging line; read for exactly SWWC_LINE_BYTES bytes and copied into the region. Borrowed for
   *   the duration of the call only; typically a completed ScatterHeads staging line.
   * @pre Runs on the single worker that owns this Region, during the scatter phase.
   * @post The written bytes are not guaranteed visible to other threads yet; the owning worker must issue exactly one
   *   store fence at end-of-scatter (ScatterHeads::finish()) before any reader observes them.
   * @note The non-temporal store requires a 64-byte-aligned destination, which the buffer guarantees.
   * Complexity: amortized O(1); an occasional geometric grow reallocates and copies the existing content.
   */
  void push_line(const std::byte* line);

  /**
   * Append the final partial line (fewer than SWWC_LINE_BYTES bytes) with ordinary (temporal) stores.
   *
   * @param bytes   Source bytes; read for exactly `length` bytes and copied into the region. Borrowed.
   * @param length  Number of bytes to append; must be < SWWC_LINE_BYTES (a full line goes through push_line()).
   * @pre Called at most once per Region, at end-of-scatter, after the last push_line(), on the owning worker.
   * @note Uses ordinary stores, but the trailing sfence in ScatterHeads::finish() still orders them before the merge
   *   phase reads.
   */
  void drain_partial(const std::byte* bytes, size_t length);

  /** @return Number of bytes written so far (the sum of all push_line() and drain_partial() bytes). */
  size_t size() const;

  /**
   * @return Pointer to the first buffer byte, 64-byte aligned; the readable range is [data(), data() + size()). Valid
   *   until the next clear() or destruction.
   * @pre Before a merge-phase reader dereferences the result, the owning worker must have completed
   *   ScatterHeads::finish() (sfence) and the phase barrier must have passed; otherwise the non-temporal writes may not
   *   yet be visible.
   */
  const std::byte* data() const;

  /**
   * Reset size() to zero while retaining the allocated capacity; no memory is freed.
   *
   * Reused across the successive input chunks one worker scatters within a query, and is the hook a later cross-query
   * per-worker region pool will reuse.
   * @post size() == 0; any pointer previously returned by data() is invalidated.
   */
  void clear();

  void grow();

private:
  std::unique_ptr<std::byte[], AlignedFree> _data{};
  size_t _size{0};
  size_t _capacity{0};
};

/**
 * Per-worker scatter storage across all P partitions: the destination every input row a worker claims lands in.
 *
 * A worker owns exactly one ScatterStore and, over the whole scatter phase, funnels every input chunk it claims into
 * it -- so the merge phase sees at most one store per worker per partition, keeping the number of stores merged per
 * partition bounded by the worker count, not the chunk count. Per partition it holds: the packed-key region; one value
 * region per distinct scattered source column (a column aggregated by several aggregates is scattered once and shared
 * -- COUNT(*) needs no value stream); a value-null-bitmap region when any value stream is nullable (one bit per
 * nullable stream per row); a StringSpillBuffer for spilled string-key content; and, when any value stream is a string
 * stream, a value arena (a StringSpillBuffer holding string value bytes, referenced by (offset, length) from the value
 * region).
 *
 * Cross-query pooling of these regions (expected to be a substantial performance win) is deliberately deferred; v1
 * allocates fresh per query. The clear()/capacity-retaining shape here is what a later per-worker RegionPool will hook
 * into.
 *
 * Invariants:
 *   - Every held Region and StringSpillBuffer is indexed by a PartitionId in [0, partition_count).
 *   - The value-null-bitmap regions are populated iff the query has a nullable value stream, and the value arenas iff
 *     it has a string value stream (see value_null_bitmap_region()/value_arena()).
 *   - Within a partition, all value regions advance in lockstep row-for-row, so a row's fields line up across streams.
 *
 * Ownership/lifetime/threading: one instance per worker, constructed before scatter and touched by that worker alone
 * during scatter (no internal synchronization). During merge, other workers read its regions, safely because the
 * end-of-scatter sfence plus the phase barrier establish happens-before. Must outlive the merge phase. Front-ended by
 * ScatterHeads during scatter and consumed by the MergeMap during merge. See Region, ScatterHeads, StringSpillBuffer.
 */
class ScatterStore : private Noncopyable {
 public:
  /**
   * Allocate the per-partition regions and buffers for one worker's scatter store.
   *
   * @param partition_count         Number of radix partitions P (a power of two in [max(worker_count, 1),
   *   MAX_PARTITION_COUNT]); every accessor's PartitionId must be < this value.
   * @param key_width               Packed-key width in bytes (schema-defined, a multiple of 4); sizes the per-partition
   *   key regions.
   * @param value_stream_widths     One entry per distinct scattered value stream, in stream order, each the stream's
   *   per-row byte width; borrowed for the duration of the call only. Empty for a COUNT(*)-only query.
   * @param value_null_bitmap_width Per-row width in bytes of the value-null-bitmap stream, or 0 when no value stream is
   *   nullable; when 0 the null-bitmap regions are not allocated and value_null_bitmap_region() must not be called.
   * @param needs_value_arena       Whether any value stream carries strings and thus needs a per-partition value arena;
   *   when false, value_arena() must not be called.
   */
  ScatterStore(PartitionCount partition_count, size_t key_width, std::span<const size_t> value_stream_widths,
               size_t value_null_bitmap_width, bool needs_value_arena);

  /**
   * @param partition  Radix partition id; must be < partition_count.
   * @return The partition's packed-key region, written during scatter and streamed during merge. Valid for the store's
   *   lifetime.
   */
  Region& key_region(PartitionId partition);

  /**
   * @param partition           Radix partition id; must be < partition_count.
   * @param value_stream_index  Distinct-value-stream index in [0, value_stream_count); the order matches the
   *   value_stream_widths passed to the constructor.
   * @return The partition's value region for that stream. For a string value stream this region holds (offset, length)
   *   references into value_arena(partition), not the bytes themselves.
   */
  Region& value_region(PartitionId partition, size_t value_stream_index);

  /**
   * @param partition  Radix partition id; must be < partition_count.
   * @return The partition's value-null-bitmap region (one bit per nullable value stream per row).
   * @pre Valid only when the store was constructed with value_null_bitmap_width > 0 (i.e. some value stream is
   *   nullable); calling it otherwise is undefined.
   */
  Region& value_null_bitmap_region(PartitionId partition);

  /**
   * @param partition  Radix partition id; must be < partition_count.
   * @return The partition's spill buffer for string-key content that overflows a key's inline blob (see
   *   StringSpillBuffer). Allocated for every partition, but only receives content for string-involving key schemas.
   */
  StringSpillBuffer& key_spill_buffer(PartitionId partition);

  /**
   * @param partition  Radix partition id; must be < partition_count.
   * @return The partition's string value arena, holding the bytes referenced by the (offset, length) pairs stored in
   *   that partition's string value regions.
   * @pre Valid only when the store was constructed with needs_value_arena == true (i.e. some value stream is a string
   *   stream); calling it otherwise is undefined.
   */
  StringSpillBuffer& value_arena(PartitionId partition);

  /**
   * Reset every region and spill buffer to empty while retaining capacity, readying the store for the next input chunk
   * (within a query) or a future pooled reuse (across queries).
   * @post Every held region reports size() == 0; no memory is freed.
   */
  void clear();

 private:
  PartitionCount _partition_count;                     // Radix partition count P; valid PartitionId range [0, P).
  std::vector<Region> _key_regions;                    // One packed-key region per partition; indexed [partition].
  std::vector<Region> _value_regions;                  // [partition * _value_stream_count + stream].
  std::vector<Region> _value_null_bitmap_regions;      // Per partition; empty if no nullable value stream.
  std::vector<StringSpillBuffer> _key_spill_buffers;   // Per partition; used only for string-involving key schemas.
  std::vector<StringSpillBuffer> _value_arenas;        // Per partition; empty if no string value stream.
  size_t _value_stream_count{0};                       // Distinct scattered value streams; the _value_regions stride.
};

/**
 * Software write-combining (SWWC) staging front-end for one worker's ScatterStore.
 *
 * Holds one SWWC_LINE_BYTES staging line per (stream, partition) pair, where the streams in order are [packed key,
 * value stream 0 .. value stream n-1, value-null-bitmap (present only if any value stream is nullable)]. push()
 * appends a field's bytes to the matching (stream, partition) line; when a line fills, it is flushed to the
 * corresponding Region via a non-temporal store. This converts the P-way random scatter (one cache-line-polluting
 * write per row) into batched, cache-bypassing line writes.
 *
 * finish() MUST be called at end-of-scatter: it drains every partial line with ordinary stores and then issues a
 * single store fence (sfence). The fence is load-bearing and invisible to thread sanitizers -- without it,
 * non-temporal stores may still sit in write-combining buffers when the merge phase (after the scheduler barrier)
 * begins reading, causing rare, data-dependent loss of scattered rows. Do not remove it.
 *
 * Invariants:
 *   - No (stream, partition) staging line ever holds SWWC_LINE_BYTES or more pending bytes: filling one triggers its
 *     flush.
 *   - A pushed field's width must match its stream's per-row width, and streams are indexed in the order above.
 *
 * Ownership/lifetime/threading: one instance per worker, used single-threaded by that worker for the whole scatter
 * phase. It does not own the ScatterStore it flushes into; that store is passed in per call, must be the same store
 * across all calls, and must outlive this ScatterHeads. See ScatterStore, Region, and SWWC_LINE_BYTES.
 */
class ScatterHeads : private Noncopyable {
 public:
  /**
   * Allocate the per-(stream, partition) staging lines for one worker.
   *
   * @param partition_count  Number of radix partitions P; must match the ScatterStore this will flush into.
   * @param stream_count     Number of scatter streams (packed key + value streams + optional value-null-bitmap); must
   *   equal the number of streams the target ScatterStore was built for.
   * @param stream_widths    Per-row byte width of each stream, in stream order; borrowed for the duration of the call.
   */
  ScatterHeads(PartitionCount partition_count, size_t stream_count, std::span<const size_t> stream_widths);

  /**
   * Stage `width` bytes of one field into the (stream, partition) line, flushing a completed line into `store`.
   *
   * @param store      The worker's own ScatterStore to flush completed lines into; must be the same store across every
   *   push()/finish() call and outlive this ScatterHeads. Mutated when a full line is written (non-temporal store).
   * @param stream     Stream index in [0, stream_count): 0 is the packed key, then one per value stream, then the
   *   value-null-bitmap (if present).
   * @param partition  Destination radix partition id; must be < partition_count.
   * @param bytes      Source bytes for the field; read for exactly `width` bytes and copied into the staging line.
   *   Borrowed.
   * @param width      Number of bytes to stage; must equal the stream's per-row field width and not exceed
   *   SWWC_LINE_BYTES.
   * @pre Runs on the single owning worker during the scatter phase.
   * @post At most one line is flushed; the staged data is not globally visible until finish() issues the sfence.
   */
  void push(ScatterStore& store, size_t stream, PartitionId partition, const std::byte* bytes, size_t width);

  /**
   * Drain every partial staging line into `store` with ordinary stores, then issue a single store fence (sfence).
   *
   * @param store  The worker's ScatterStore -- the same one used for every push(); receives the drained partial lines.
   * @pre Call exactly once, after the worker's last push(), before its scatter JobTask returns.
   * @post Every staged byte has been written to `store`, and the sfence makes the non-temporal stores globally visible;
   *   together with the phase barrier this establishes happens-before for the merge phase's reads. Skipping this call
   *   can make merge-phase readers observe lost rows.
   * @note The sfence is required for correctness and is invisible to thread sanitizers; do not remove it.
   * Complexity: O(stream_count * partition_count) -- it visits every staging line once.
   */
  void finish(ScatterStore& store);
};

}  // namespace hyrise
