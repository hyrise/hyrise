#include "operators/aggregate_dyod/scatter_store.hpp"

#include <algorithm>
#include <cstddef>
#include <vector>

#if defined(__SSE2__) || defined(_M_X64) || (defined(_M_IX86_FP) && _M_IX86_FP >= 2)
#include <emmintrin.h>
#define REGION_STREAM_SSE2 1
#else
#define REGION_STREAM_SSE2 0
#endif

#if defined(__aarch64__) || defined(__arm__)
#include <arm_acle.h>
#endif

namespace {
void sfence() noexcept {
#if REGION_STREAM_SSE2
  _mm_sfence();
#elif defined(__aarch64__) || defined(__arm__)
  __dmb(0xE);
#else
  std::atomic_thread_fence(std::memory_order_release);
#endif
}

void copy_line(std::byte* destination, const std::byte* source) {
#if REGION_STREAM_SSE2
  static_assert(hyrise::SWWC_LINE_BYTES % 16 == 0, "line must be a whole number of 128-bit stores");

  for (auto offset = size_t{0}; offset < hyrise::SWWC_LINE_BYTES; offset += 16) {
    __m128i vec = _mm_loadu_si128(reinterpret_cast<const __m128i*>(source + offset));
    _mm_stream_si128(reinterpret_cast<__m128i*>(destination + offset), vec);
  }
#else
  std::memcpy(destination, source, hyrise::SWWC_LINE_BYTES);
#endif
}

constexpr size_t REGION_INITIAL_LINES = 16;
constexpr size_t REGION_INITIAL_CAPACITY = REGION_INITIAL_LINES * hyrise::SWWC_LINE_BYTES;

constexpr size_t round_up_to_lines(const size_t size) noexcept {
  return (size + hyrise::SWWC_LINE_BYTES - 1) / hyrise::SWWC_LINE_BYTES * hyrise::SWWC_LINE_BYTES;
}
}  // namespace

namespace hyrise {

void Region::grow() {
  const auto required = size_t{_size + SWWC_LINE_BYTES};
  const auto doubled = size_t{_capacity * 2};
  const auto new_capacity = round_up_to_lines(std::max({required, doubled, REGION_INITIAL_CAPACITY}));

  auto* block = std::aligned_alloc(64, new_capacity);
  if (!block) {
    Fail("Allocation failed");
  }

  auto* new_data = static_cast<std::byte*>(block);

  if (_size > 0) {
    std::memcpy(new_data, _data.get(), _size);
  }

  _data.reset(new_data);
  _capacity = new_capacity;
}

void Region::push_line(const std::byte* line) {
  if (_size + SWWC_LINE_BYTES > _capacity) {
    grow();
  }

  std::byte* destination = _data.get() + _size;
  copy_line(destination, line);
  _size += SWWC_LINE_BYTES;
}

void Region::drain_partial(const std::byte* bytes, const size_t length) {
  Assert(length < SWWC_LINE_BYTES, "A full line must use push_line()");
  Assert(_size % SWWC_LINE_BYTES == 0, "_size has to be line aligned before drain_partial is called");

  if (length == 0) {
    return;
  }
  if (_size + length > _capacity) {
    grow();
  }

  std::memcpy(_data.get() + _size, bytes, length);
  _size += length;
}

size_t Region::size() const {
  return _size;
}

const std::byte* Region::data() const {
  return _data.get();
}

void Region::clear() {
  _size = 0;
}

ScatterStore::ScatterStore(PartitionCount partition_count, size_t key_width,
                           std::span<const size_t> value_stream_widths, size_t value_null_bitmap_width,
                           bool needs_value_arena)
    : _partition_count(partition_count), _value_stream_count(value_stream_widths.size()) {
  const auto partition_count_value = static_cast<size_t>(partition_count);
  Assert(partition_count_value >= 1 && partition_count_value <= MAX_PARTITION_COUNT,
         "partition count must be in [1, MAX_PARTITION_COUNT]");
  Assert((partition_count_value & (partition_count_value - 1)) == 0, "partition count must be a power of two");
  Assert(key_width > 0 && key_width % 4 == 0, "key width must be a positive multiple of 4 bytes");

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

Region& ScatterStore::key_region(PartitionId partition) {
  DebugAssert(static_cast<size_t>(partition) < static_cast<size_t>(_partition_count), "partition id out of range");
  return _key_regions[partition];
}

Region& ScatterStore::value_region(PartitionId partition, size_t value_stream_index) {
  DebugAssert(static_cast<size_t>(partition) < static_cast<size_t>(_partition_count), "partition id out of range");
  DebugAssert(value_stream_index < _value_stream_count, "value stream index out of range");

  const auto index = static_cast<size_t>(partition) * _value_stream_count + value_stream_index;

  return _value_regions[index];
}

Region& ScatterStore::value_null_bitmap_region(PartitionId partition) {
  DebugAssert(static_cast<size_t>(partition) < static_cast<size_t>(_partition_count), "partition id out of range");
  DebugAssert(!_value_null_bitmap_regions.empty(), "no value-null-bitmap regions");

  return _value_null_bitmap_regions[partition];
}

StringSpillBuffer& ScatterStore::key_spill_buffer(PartitionId partition) {
  DebugAssert(static_cast<size_t>(partition) < static_cast<size_t>(_partition_count), "partition id out of range");

  return _key_spill_buffers[partition];
}

StringSpillBuffer& ScatterStore::value_arena(PartitionId partition) {
  DebugAssert(static_cast<size_t>(partition) < static_cast<size_t>(_partition_count), "partition id out of range");
  DebugAssert(!_value_arenas.empty(), "no value arenas");

  return _value_arenas[partition];
}

void ScatterStore::clear() {
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

ScatterHeads::ScatterHeads(PartitionCount partition_count, size_t stream_count, std::span<const size_t> stream_widths,
                           bool has_value_null_bitmap)
    : _partition_count(partition_count),
      _stream_count(stream_count),
      _stream_widths(stream_widths.begin(), stream_widths.end()) {
  Assert(_partition_count >= 1, "need at least one partition");
  Assert(stream_widths.size() == stream_count, "one width per stream required");

  const auto non_value_streams = size_t{1} + (has_value_null_bitmap ? 1 : 0);
  Assert(stream_count >= non_value_streams, "stream_count too small for its schema");
  _value_stream_count = stream_count - non_value_streams;

  for (auto stream = size_t{0}; stream < stream_count; ++stream) {
    Assert(stream_widths[stream] > 0, "stream width must be positive");
    Assert(SWWC_LINE_BYTES % stream_widths[stream] == 0, "stream width must evenly divide the SWWC line");
  }

  _staging.assign(_stream_count * _partition_count * SWWC_LINE_BYTES, std::byte{0});
  _fill.assign(_stream_count * _partition_count, 0);
}

Region& ScatterHeads::_region_for(ScatterStore& store, size_t stream, PartitionId partition) const {
  DebugAssert(stream < _stream_count, "stream index out of range");
  if (stream == 0) {
    return store.key_region(partition);
  }
  if (stream <= _value_stream_count) {
    return store.value_region(partition, stream - 1);
  }
  return store.value_null_bitmap_region(partition);
}

void ScatterHeads::_store_line_flush(ScatterStore& store, size_t stream, PartitionId partition, size_t line,
                                     size_t fill) const {
  Region& region = _region_for(store, stream, partition);
  if (fill == SWWC_LINE_BYTES) {
    region.push_line(_staging.data() + line);
  } else {
    region.drain_partial(_staging.data() + line, fill);
  }
}

void ScatterHeads::push(ScatterStore& store, size_t stream, PartitionId partition, const std::byte* bytes,
                        size_t width) {
  DebugAssert(stream < _stream_count, "stream index out of range");
  DebugAssert(partition < _partition_count, "partition out of range");
  DebugAssert(width == _stream_widths[stream], "field width must match the stream's per-row width");

  const auto line = _line_offset(stream, partition);
  auto& fill = _fill[stream * _partition_count + static_cast<size_t>(partition)];

  std::memcpy(_staging.data() + line + fill, bytes, width);
  fill += width;

  if (fill == SWWC_LINE_BYTES) {
    _store_line_flush(store, stream, partition, line, fill);
    fill = 0;
  }
}

void ScatterHeads::finish(ScatterStore& store) {
  for (auto stream = size_t{0}; stream < _stream_count; ++stream) {
    for (auto partition = size_t{0}; partition < _partition_count; ++partition) {
      if (auto& fill = _fill[stream * _partition_count + partition]; fill > 0) {
        _store_line_flush(store, stream, static_cast<PartitionId>(partition), _line_offset(stream, partition), fill);
        fill = 0;
      }
    }
  }
  sfence();
}

}  // namespace hyrise
