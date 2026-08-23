#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <vector>

#include "all_type_variant.hpp"
#include "operators/aggregate_dyod/hyperloglog.hpp"
#include "operators/aggregate_dyod/key_primitives.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

/**
 * Numeric-only group-by key schema whose total packed width is the compile-time constant width.
 *
 * Selected by resolve_key_schema when every group-by column is numeric and the packed width (null bitmap + numeric
 * prefix) is one of {4,8,12,16,20,24} bytes; one instantiation per bucket. Because the width is known at compile
 * time, hash() and equals() are fixed-size and branch-free, while pack()/unpack() loop the resolved lanes.
 */
template <size_t width>
class NumericShortKeySchema {
 public:
  static constexpr KeyComposition COMPOSITION = KeyComposition::NumericOnly;
  static constexpr size_t WIDTH = width;
  static constexpr bool HAS_STRINGS = false;

  /**
   * Build the schema for a query's group-by columns: resolve one NumericKeyLane per column and lay out the fields.
   */
  static NumericShortKeySchema build(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table);

  size_t packed_width() const;
  size_t column_count() const;

  /**
   * Decode rows [row_begin, row_end) of one chunk's group-by columns into the worker's scratch, one pass per column.
   *
   * The scratch holds the window's rows alone, so pack() addresses a row by its distance from `row_begin` rather than
   * by its chunk offset. Decoding a window instead of a whole chunk is what lets several workers share one chunk.
   */
  void decode(std::span<const AbstractSegment* const> group_by_segments, size_t row_begin, size_t row_end,
              KeyDecodeScratch& scratch) const;
  /** Decode a whole chunk's group-by columns via the windowed overload. */
  void decode(std::span<const AbstractSegment* const> group_by_segments, KeyDecodeScratch& scratch) const;
  /**
   * Pack one decoded row's group-by tuple into a key buffer.
   */
  void pack(const KeyDecodeScratch& scratch, ChunkOffset chunk_offset, std::byte* key_out,
            StringSpillBuffer& spill_buffer) const;
  /**
   * Unpack a packed key back into typed output values, one appended cell per group-by column.
   */
  void unpack(const std::byte* key, OutputColumns& output, size_t output_row) const;
  /**
   * Hash a packed key over its full fixed width.
   */
  uint64_t hash(const std::byte* key) const;
  /**
   * Test two packed keys for equality by comparing their full fixed width.
   */
  bool equals(const std::byte* lhs, const std::byte* rhs) const;

 private:
  NumericKeyLanes _lanes;
};

template <size_t width>
NumericShortKeySchema<width> NumericShortKeySchema<width>::build(const std::vector<ColumnID>& group_by_column_ids,
                                                                 const Table& input_table) {
  const auto layout = compute_key_layout(group_by_column_ids, input_table, 0);
  Assert(layout.string_count == 0, "NumericShortKeySchema requires numeric-only group-by columns.");
  Assert(layout.fixed_part_width == width, "Resolved packed width does not match the schema's template width.");

  auto schema = NumericShortKeySchema{};
  const auto column_count = group_by_column_ids.size();
  for (auto index = size_t{0}; index < column_count; ++index) {
    const auto& column = layout.columns[index];
    schema._lanes.emplace_back(
        make_numeric_lane(column.data_type, group_by_column_ids[index], column.field_offset, column.null_bit_index));
  }
  return schema;
}

template <size_t width>
size_t NumericShortKeySchema<width>::packed_width() const {
  return width;
}

template <size_t width>
size_t NumericShortKeySchema<width>::column_count() const {
  return _lanes.size();
}

template <size_t width>
void NumericShortKeySchema<width>::decode(const std::span<const AbstractSegment* const> group_by_segments,
                                          const size_t row_begin, const size_t row_end,
                                          KeyDecodeScratch& scratch) const {
  decode_numeric_lanes(_lanes, group_by_segments, row_begin, row_end, scratch);
}

template <size_t width>
void NumericShortKeySchema<width>::decode(const std::span<const AbstractSegment* const> group_by_segments,
                                          KeyDecodeScratch& scratch) const {
  decode(group_by_segments, 0, group_by_segments.front()->size(), scratch);
}

template <size_t width>
void NumericShortKeySchema<width>::unpack(const std::byte* key, OutputColumns& output, const size_t output_row) const {
  const auto lane_count = _lanes.size();
  for (auto index = size_t{0}; index < lane_count; ++index) {
    _lanes[index].lane->unpack(key, key, output, index, output_row);
  }
}

template <size_t width>
void NumericShortKeySchema<width>::pack(const KeyDecodeScratch& scratch, const ChunkOffset chunk_offset,
                                        std::byte* key_out, StringSpillBuffer& /*spill_buffer*/) const {
  std::memset(key_out, 0, width);
  pack_numeric_lanes<std::min(width, size_t{8})>(_lanes, scratch, chunk_offset, key_out);
}

template <size_t width>
uint64_t NumericShortKeySchema<width>::hash(const std::byte* key) const {
  // One multiply-mix round per word beats byte-wise FNV-1a; the hash runs up to three times per row (estimate,
  // scatter routing, merge probing), and its low bits pick the partition.
  auto hash = uint64_t{0};
  auto offset = size_t{0};
  for (; offset + 8 <= width; offset += 8) {
    auto word = uint64_t{};
    std::memcpy(&word, key + offset, 8);
    hash = mix64(hash ^ word);
  }
  if constexpr (width % 8 != 0) {
    auto word = uint32_t{};
    std::memcpy(&word, key + offset, 4);
    hash = mix64(hash ^ word);
  }
  return hash;
}

template <size_t width>
bool NumericShortKeySchema<width>::equals(const std::byte* lhs, const std::byte* rhs) const {
  return std::memcmp(lhs, rhs, width) == 0;
}

}  // namespace hyrise
