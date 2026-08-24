#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <span>
#include <vector>

#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "operators/aggregate_dyod/key_primitives.hpp"
#include "utils/assert.hpp"

#ifdef __SSE2__
#include <emmintrin.h>
#endif

namespace hyrise {

inline void sfence() noexcept {
#ifdef __SSE2__
  _mm_sfence();
#else
  std::atomic_thread_fence(std::memory_order_release);
#endif
}

inline void copy_line(std::byte* destination, const std::byte* source) {
#ifdef __SSE2__
  static_assert(SWWC_LINE_BYTES % 16 == 0, "A line must be a whole number of 128-bit stores.");

  for (auto offset = size_t{0}; offset < SWWC_LINE_BYTES; offset += 16) {
    const auto vec = _mm_loadu_si128(reinterpret_cast<const __m128i*>(source + offset));
    _mm_stream_si128(reinterpret_cast<__m128i*>(destination + offset), vec);
  }
#else
  std::memcpy(destination, source, SWWC_LINE_BYTES);
#endif
}

struct AlignedFree {
  void operator()(std::byte* ptr) const noexcept {
    ::operator delete(ptr, std::align_val_t{64});
  }
};

/**
 * A growable, 64-byte-aligned byte buffer that holds one stream's scattered bytes for a single partition.
 *
 * One Region backs exactly one (partition, stream) slot of a worker's ScatterStore during the scatter phase. Full SWWC
 * staging lines are appended with non-temporal (cache-bypassing) stores via push_line(); the single trailing partial
 * line is drained with ordinary stores at end-of-scatter via drain_partial().
 */
class Region : private Noncopyable {
 public:
  /**
   * Append exactly SWWC_LINE_BYTES bytes to the buffer via a non-temporal store.
   */
  void push_line(const std::byte* line);

  /**
   * Append the final partial line (fewer than SWWC_LINE_BYTES bytes) with ordinary stores.
   */
  void drain_partial(const std::byte* bytes, size_t length);

  /**
   * Number of bytes written so far (the sum of all push_line() and drain_partial() bytes).
   */
  size_t size() const;

  const std::byte* data() const;

  /**
   * Reset size() to zero while retaining the allocated capacity; no memory is freed.
   */
  void clear();

  /** Free the backing storage. */
  void release();

 private:
  static constexpr size_t INITIAL_LINES = 16;
  static constexpr size_t INITIAL_CAPACITY = INITIAL_LINES * SWWC_LINE_BYTES;

  void _grow();

  std::unique_ptr<std::byte, AlignedFree> _data;
  size_t _size{0};
  size_t _capacity{0};
};

inline void Region::_grow() {
  const auto round_up_to_lines = [](const size_t size) noexcept {
    return (size + SWWC_LINE_BYTES - 1) / SWWC_LINE_BYTES * SWWC_LINE_BYTES;
  };

  const auto required = size_t{_size + SWWC_LINE_BYTES};
  const auto doubled = size_t{_capacity * 2};
  const auto new_capacity = round_up_to_lines(std::max({required, doubled, INITIAL_CAPACITY}));

  auto* new_data = static_cast<std::byte*>(::operator new(new_capacity, std::align_val_t{64}));

  if (_size > 0) {
    std::memcpy(new_data, _data.get(), _size);
  }

  _data.reset(new_data);
  _capacity = new_capacity;
}

inline void Region::push_line(const std::byte* line) {
  DebugAssert(_size % SWWC_LINE_BYTES == 0, "_size has to be line aligned before push_line is called.");

  if (_size + SWWC_LINE_BYTES > _capacity) {
    _grow();
  }

  auto* destination = _data.get() + _size;
  copy_line(destination, line);
  _size += SWWC_LINE_BYTES;
}

inline void Region::drain_partial(const std::byte* bytes, const size_t length) {
  Assert(length < SWWC_LINE_BYTES, "A full line must use push_line().");
  Assert(_size % SWWC_LINE_BYTES == 0, "_size has to be line aligned before drain_partial is called.");

  if (length == 0) {
    return;
  }
  if (_size + length > _capacity) {
    _grow();
  }

  std::memcpy(_data.get() + _size, bytes, length);
  _size += length;
}

inline size_t Region::size() const {
  return _size;
}

inline const std::byte* Region::data() const {
  return _data.get();
}

inline void Region::clear() {
  _size = 0;
}

inline void Region::release() {
  _data.reset();
  _size = 0;
  _capacity = 0;
}

/**
 * Per-worker scatter storage across all P partitions: the destination every input row a worker claims lands in.
 *
 * A worker owns exactly one ScatterStore and, over the whole scatter phase, funnels every input chunk it claims into
 * it. Per partition it holds: the packed-key region; one value region per distinct scattered source column (a column
 * aggregated by several aggregates is scattered once and shared -> COUNT(*) needs no value stream); a value-null-bitmap
 * region when any value stream is nullable (one bit per nullable stream per row); a StringSpillBuffer for spilled
 * string-key content; and, when any value stream is a string stream, a value arena (a StringSpillBuffer holding string
 * value bytes, referenced by (offset, length) from the value region).
 */
class ScatterStore : private Noncopyable {
 public:
  /**
   * Allocate the per-partition regions and buffers for one worker's scatter store.
   */
  ScatterStore(PartitionCount partition_count, size_t key_width, std::span<const size_t> value_stream_widths,
               size_t value_null_bitmap_width, bool needs_value_arena);

  Region& key_region(PartitionId partition);

  Region& value_region(PartitionId partition, size_t value_stream_index);

  Region& value_null_bitmap_region(PartitionId partition);

  StringSpillBuffer& key_spill_buffer(PartitionId partition);

  StringSpillBuffer& value_arena(PartitionId partition);

  /**
   * Reset every region and spill buffer to empty while retaining capacity, readying the store for the next input chunk.
   */
  void clear();

  /** Free the storage of every held region and buffer. */
  void release();

 private:
  [[maybe_unused]] PartitionCount _partition_count;
  std::vector<Region> _key_regions;
  std::vector<Region> _value_regions;
  std::vector<Region> _value_null_bitmap_regions;
  std::vector<StringSpillBuffer> _key_spill_buffers;
  std::vector<StringSpillBuffer> _value_arenas;
  size_t _value_stream_count{0};
};

inline ScatterStore::ScatterStore(const PartitionCount partition_count, const size_t key_width,
                                  const std::span<const size_t> value_stream_widths,
                                  const size_t value_null_bitmap_width, const bool needs_value_arena)
    : _partition_count{partition_count}, _value_stream_count{value_stream_widths.size()} {
  const auto partition_count_value = static_cast<size_t>(partition_count);
  Assert(partition_count_value >= 1 && partition_count_value <= MAX_PARTITION_COUNT,
         "The partition count must be in [1, MAX_PARTITION_COUNT].");
  Assert((partition_count_value & (partition_count_value - 1)) == 0, "The partition count must be a power of two.");
  Assert(key_width > 0 && key_width % 4 == 0, "The key width must be a positive multiple of 4 bytes.");

  _key_regions = std::vector<Region>(partition_count_value);

  _value_regions = std::vector<Region>(partition_count_value * _value_stream_count);

  if (value_null_bitmap_width > 0) {
    _value_null_bitmap_regions = std::vector<Region>(partition_count_value);
  }

  _key_spill_buffers = std::vector<StringSpillBuffer>(partition_count_value);

  if (needs_value_arena) {
    _value_arenas = std::vector<StringSpillBuffer>(partition_count_value);
  }
}

inline Region& ScatterStore::key_region(const PartitionId partition) {
  DebugAssert(static_cast<size_t>(partition) < static_cast<size_t>(_partition_count), "Partition id out of range.");
  return _key_regions[partition];
}

inline Region& ScatterStore::value_region(const PartitionId partition, const size_t value_stream_index) {
  DebugAssert(static_cast<size_t>(partition) < static_cast<size_t>(_partition_count), "Partition id out of range.");
  DebugAssert(value_stream_index < _value_stream_count, "Value stream index out of range.");

  const auto index = (static_cast<size_t>(partition) * _value_stream_count) + value_stream_index;

  return _value_regions[index];
}

inline Region& ScatterStore::value_null_bitmap_region(const PartitionId partition) {
  DebugAssert(static_cast<size_t>(partition) < static_cast<size_t>(_partition_count), "Partition id out of range.");
  DebugAssert(!_value_null_bitmap_regions.empty(), "No value-null-bitmap regions were allocated.");

  return _value_null_bitmap_regions[partition];
}

inline StringSpillBuffer& ScatterStore::key_spill_buffer(const PartitionId partition) {
  DebugAssert(static_cast<size_t>(partition) < static_cast<size_t>(_partition_count), "Partition id out of range.");

  return _key_spill_buffers[partition];
}

inline StringSpillBuffer& ScatterStore::value_arena(const PartitionId partition) {
  DebugAssert(static_cast<size_t>(partition) < static_cast<size_t>(_partition_count), "Partition id out of range.");
  DebugAssert(!_value_arenas.empty(), "No value arenas were allocated.");

  return _value_arenas[partition];
}

inline void ScatterStore::clear() {
  for (auto& region : _key_regions) {
    region.clear();
  }
  for (auto& region : _value_regions) {
    region.clear();
  }
  for (auto& region : _value_null_bitmap_regions) {
    region.clear();
  }
  for (auto& buffer : _key_spill_buffers) {
    buffer.clear();
  }
  for (auto& arena : _value_arenas) {
    arena.clear();
  }
}

inline void ScatterStore::release() {
  for (auto& region : _key_regions) {
    region.release();
  }
  for (auto& region : _value_regions) {
    region.release();
  }
  for (auto& region : _value_null_bitmap_regions) {
    region.release();
  }
  for (auto& buffer : _key_spill_buffers) {
    buffer.release();
  }
  for (auto& arena : _value_arenas) {
    arena.release();
  }
}

/**
 * Software write-combining (SWWC) staging front-end for one worker's ScatterStore.
 *
 * Holds one SWWC_LINE_BYTES staging line per (stream, partition) pair, where the streams in order are [packed key,
 * value stream 0 .. value stream n-1, value-null-bitmap (present only if any value stream is nullable)]. push()
 * appends a field's bytes to the matching (stream, partition) line; when a line fills, it is flushed to the
 * corresponding Region via a non-temporal store.
 *
 * finish() must be called at end-of-scatter: it drains every partial line with ordinary stores and then issues a
 * single store fence (sfence).
 */
class ScatterHeads : private Noncopyable {
 public:
  ScatterHeads(PartitionCount partition_count, size_t stream_count, std::span<const size_t> stream_widths,
               bool has_value_null_bitmap);

  /**
   * Stage `width` bytes of one field into the (stream, partition) line, flushing a completed line into `store`.
   */
  void push(ScatterStore& store, size_t stream, PartitionId partition, const std::byte* bytes, size_t width);

  /**
   * Drain every partial staging line into `store` with ordinary stores, then issue a single store fence (sfence).
   */
  void finish(ScatterStore& store);

 private:
  size_t _line_offset(const size_t stream, const size_t partition) const {
    return (stream * _partition_count + partition) * SWWC_LINE_BYTES;
  }

  Region& _region_for(ScatterStore& store, size_t stream, PartitionId partition) const;
  void _store_line_flush(ScatterStore& store, size_t stream, PartitionId partition, size_t line_offset,
                         size_t fill) const;

  size_t _partition_count;
  size_t _stream_count;
  size_t _value_stream_count;
  std::vector<size_t> _stream_widths;
  std::vector<std::byte> _staging;
  std::vector<size_t> _fill;
};

inline ScatterHeads::ScatterHeads(const PartitionCount partition_count, const size_t stream_count,
                                  std::span<const size_t> stream_widths, const bool has_value_null_bitmap)
    : _partition_count{partition_count},
      _stream_count{stream_count},
      _stream_widths(stream_widths.begin(), stream_widths.end()) {
  Assert(_partition_count >= 1, "At least one partition is required.");
  Assert(stream_widths.size() == stream_count, "One width per stream is required.");

  const auto non_value_streams = size_t{1} + (has_value_null_bitmap ? 1 : 0);
  Assert(stream_count >= non_value_streams, "stream_count is too small for its schema.");
  _value_stream_count = stream_count - non_value_streams;

  for (auto stream = size_t{0}; stream < stream_count; ++stream) {
    Assert(stream_widths[stream] > 0, "The stream width must be positive.");
    Assert(SWWC_LINE_BYTES % stream_widths[stream] == 0, "The stream width must evenly divide the SWWC line.");
  }

  _staging.assign(_stream_count * _partition_count * SWWC_LINE_BYTES, std::byte{0});
  _fill.assign(_stream_count * _partition_count, 0);
}

inline Region& ScatterHeads::_region_for(ScatterStore& store, const size_t stream, const PartitionId partition) const {
  DebugAssert(stream < _stream_count, "Stream index out of range.");
  if (stream == 0) {
    return store.key_region(partition);
  }
  if (stream <= _value_stream_count) {
    return store.value_region(partition, stream - 1);
  }
  return store.value_null_bitmap_region(partition);
}

inline void ScatterHeads::_store_line_flush(ScatterStore& store, const size_t stream, const PartitionId partition,
                                            const size_t line_offset, const size_t fill) const {
  auto& region = _region_for(store, stream, partition);
  if (fill == SWWC_LINE_BYTES) {
    region.push_line(_staging.data() + line_offset);
  } else {
    region.drain_partial(_staging.data() + line_offset, fill);
  }
}

inline void ScatterHeads::push(ScatterStore& store, const size_t stream, const PartitionId partition,
                               const std::byte* bytes, const size_t width) {
  DebugAssert(stream < _stream_count, "Stream index out of range.");
  DebugAssert(partition < _partition_count, "Partition id out of range.");
  DebugAssert(width == _stream_widths[stream], "The field width must match the stream's per-row width.");

  const auto line_offset = _line_offset(stream, partition);
  auto& fill = _fill[(stream * _partition_count) + static_cast<size_t>(partition)];

  std::memcpy(_staging.data() + line_offset + fill, bytes, width);
  fill += width;

  if (fill == SWWC_LINE_BYTES) {
    _store_line_flush(store, stream, partition, line_offset, fill);
    fill = 0;
  }
}

inline void ScatterHeads::finish(ScatterStore& store) {
  for (auto stream = size_t{0}; stream < _stream_count; ++stream) {
    for (auto partition = size_t{0}; partition < _partition_count; ++partition) {
      if (auto& fill = _fill[(stream * _partition_count) + partition]; fill > 0) {
        _store_line_flush(store, stream, static_cast<PartitionId>(partition), _line_offset(stream, partition), fill);
        fill = 0;
      }
    }
  }
  sfence();
}

}  // namespace hyrise
