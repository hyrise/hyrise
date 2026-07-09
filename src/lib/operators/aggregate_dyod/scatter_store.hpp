#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "operators/aggregate_dyod/key_schema.hpp"

namespace hyrise {

// A growable, 64-byte-aligned byte buffer that a partition's scattered data lands in. Full SWWC lines are appended with
// non-temporal (cache-bypassing) stores; a trailing partial line is drained with ordinary stores at end-of-scatter.
// Overflow grows the buffer (there is no separate spill path). The 64-byte alignment is required for the non-temporal
// store instructions.
class Region : private Noncopyable {
 public:
  // Append exactly SWWC_LINE_BYTES via non-temporal store. Must be followed (eventually) by a store fence before any
  // reader observes the data -- see ScatterHeads::finish().
  void push_line(const std::byte* line);

  // Append `length` (< SWWC_LINE_BYTES) trailing bytes with ordinary stores.
  void drain_partial(const std::byte* bytes, size_t length);

  // Bytes written so far.
  size_t size() const;
  const std::byte* data() const;

  // Drop content, retaining capacity (for the deferred cross-query region pool; within a query, reused across the
  // chunks a worker scatters).
  void clear();
};

// Per-worker scatter storage across all P partitions. A worker owns exactly one ScatterStore and, over the whole
// scatter phase, funnels every input chunk it claims into it -- so the merge phase sees at most one store per worker
// per partition, keeping the number of stores merged per partition bounded by the worker count (NOT the chunk count).
//
// Per partition it holds: the packed-key region; one value region per DISTINCT scattered source column (a column
// aggregated by several aggregates is scattered once and shared -- COUNT(*) needs no value stream); a value-null-bitmap
// region when any value stream is nullable (one bit per nullable stream per row); a StringSpillBuffer for spilled
// string-KEY content; and, when any value stream is a string stream, a value arena (a StringSpillBuffer holding string
// VALUE bytes, referenced by (offset,length) from the value region).
//
// Cross-query pooling of these regions (the reference PoC's biggest measured win) is deliberately deferred; v1
// allocates fresh per query. The clear()/capacity-retaining shape here is what a later per-worker RegionPool will hook
// into.
class ScatterStore : private Noncopyable {
 public:
  ScatterStore(PartitionCount partition_count, size_t key_width, std::span<const size_t> value_stream_widths,
               size_t value_null_bitmap_width, bool needs_value_arena);

  Region& key_region(PartitionId partition);
  Region& value_region(PartitionId partition, size_t value_stream_index);
  Region& value_null_bitmap_region(PartitionId partition);  // valid iff value_null_bitmap_width > 0
  StringSpillBuffer& key_spill_buffer(PartitionId partition);
  StringSpillBuffer& value_arena(PartitionId partition);     // valid iff needs_value_arena

  void clear();

 private:
  PartitionCount _partition_count;
  std::vector<Region> _key_regions;                    // [partition]
  std::vector<Region> _value_regions;                  // [partition * value_stream_count + stream]
  std::vector<Region> _value_null_bitmap_regions;      // [partition]; empty if no nullable value stream
  std::vector<StringSpillBuffer> _key_spill_buffers;   // [partition]
  std::vector<StringSpillBuffer> _value_arenas;        // [partition]; empty if no string value stream
  size_t _value_stream_count{0};
};

// Software write-combining staging in front of one worker's ScatterStore. Holds one SWWC_LINE_BYTES staging line per
// (stream, partition) pair, where the streams in order are [packed key, value stream 0 .. value stream n-1,
// value-null-bitmap (only if any value stream is nullable)]. push() appends a field's bytes to the (stream, partition)
// line; when a line fills, it is flushed to the corresponding Region via non-temporal store. This converts the P-way
// random scatter (one cache-line-polluting write per row) into batched, cache-bypassing line writes.
//
// finish() MUST be called at end of scatter: it drains every partial line with ordinary stores and then issues a
// single store fence (sfence). The fence is load-bearing and invisible to sanitizers -- without it, non-temporal stores
// may still sit in write-combining buffers when the merge phase (after the scheduler barrier) begins reading, causing
// rare, data-dependent loss of scattered rows. Do not remove it.
class ScatterHeads : private Noncopyable {
 public:
  ScatterHeads(PartitionCount partition_count, size_t stream_count, std::span<const size_t> stream_widths);

  // Stage `width` bytes of stream `stream` for partition `partition`, flushing a completed line into `store`.
  void push(ScatterStore& store, size_t stream, PartitionId partition, const std::byte* bytes, size_t width);

  // Drain all partial lines into `store` and issue the store fence. Call once, after the last push, before the worker's
  // scatter JobTask returns.
  void finish(ScatterStore& store);
};

}  // namespace hyrise
