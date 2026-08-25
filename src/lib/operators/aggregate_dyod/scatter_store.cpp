#include "scatter_store.hpp"

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <new>
#include <span>
#include <vector>

#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "operators/aggregate_dyod/key_primitives.hpp"
#include "utils/assert.hpp"

namespace hyrise {

void Region::_grow() {
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

void Region::release() {
  _data.reset();
  _size = 0;
  _capacity = 0;
}

ScatterStore::ScatterStore(const PartitionCount partition_count, const size_t key_width,
                           const std::span<const size_t> value_stream_widths, const size_t value_null_bitmap_width,
                           const bool needs_value_arena)
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

void ScatterStore::release() {
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

ScatterHeads::ScatterHeads(const PartitionCount partition_count, const size_t stream_count,
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

}  // namespace hyrise
