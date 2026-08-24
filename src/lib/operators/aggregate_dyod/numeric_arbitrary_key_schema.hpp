#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <vector>

#include "all_type_variant.hpp"
#include "operators/aggregate_dyod/key_primitives.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

/**
 * Numeric-only group-by key schema for widths greater than 24 bytes, where the width is a runtime value.
 *
 * Same interface and semantics as NumericShortKeySchema, but because the packed width is not a compile-time constant,
 * hash() and equals() operate over packed_width() rather than a fixed WIDTH. Selected by resolve_key_schema when all
 * group-by columns are numeric but the packed width exceeds the largest NumericShortKeySchema bucket.
 */
class NumericArbitraryKeySchema {
 public:
  static constexpr KeyComposition COMPOSITION = KeyComposition::NumericOnly;
  static constexpr bool HAS_STRINGS = false;

  /**
   * Build the schema for a query's group-by columns: resolve one NumericKeyLane per column and record the width.
   */
  static NumericArbitraryKeySchema build(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table);

  size_t packed_width() const;
  size_t column_count() const;

  // decode/pack/unpack/hash/equals match NumericShortKeySchema, over the runtime packed_width() not a fixed width.
  void decode(std::span<const AbstractSegment* const> group_by_segments, size_t row_begin, size_t row_end,
              KeyDecodeScratch& scratch) const;
  void decode(std::span<const AbstractSegment* const> group_by_segments, KeyDecodeScratch& scratch) const;
  void pack(const KeyDecodeScratch& scratch, ChunkOffset chunk_offset, std::byte* key_out,
            StringSpillBuffer& spill_buffer) const;
  void unpack(const std::byte* key, OutputColumns& output) const;
  uint64_t hash(const std::byte* key) const;
  bool equals(const std::byte* lhs, const std::byte* rhs) const;

 private:
  NumericKeyLanes _lanes;
  uint32_t _packed_width{0};
};

inline NumericArbitraryKeySchema NumericArbitraryKeySchema::build(const std::vector<ColumnID>& group_by_column_ids,
                                                                  const Table& input_table) {
  const auto layout = compute_key_layout(group_by_column_ids, input_table, 0);
  Assert(layout.string_count == 0, "NumericArbitraryKeySchema requires numeric-only group-by columns.");

  auto schema = NumericArbitraryKeySchema{};
  schema._packed_width = static_cast<uint32_t>(layout.fixed_part_width);
  const auto column_count = group_by_column_ids.size();
  for (auto index = size_t{0}; index < column_count; ++index) {
    const auto& column = layout.columns[index];
    schema._lanes.emplace_back(
        make_numeric_lane(column.data_type, group_by_column_ids[index], column.field_offset, column.null_bit_index));
  }
  return schema;
}

inline size_t NumericArbitraryKeySchema::packed_width() const {
  return _packed_width;
}

inline size_t NumericArbitraryKeySchema::column_count() const {
  return _lanes.size();
}

inline void NumericArbitraryKeySchema::decode(const std::span<const AbstractSegment* const> group_by_segments,
                                              const size_t row_begin, const size_t row_end,
                                              KeyDecodeScratch& scratch) const {
  decode_numeric_lanes(_lanes, group_by_segments, row_begin, row_end, scratch);
}

inline void NumericArbitraryKeySchema::decode(const std::span<const AbstractSegment* const> group_by_segments,
                                              KeyDecodeScratch& scratch) const {
  decode(group_by_segments, 0, group_by_segments.front()->size(), scratch);
}

inline void NumericArbitraryKeySchema::unpack(const std::byte* key, OutputColumns& output) const {
  const auto lane_count = _lanes.size();
  for (auto index = size_t{0}; index < lane_count; ++index) {
    _lanes[index].lane->unpack(key, key, output, index);
  }
}

inline void NumericArbitraryKeySchema::pack(const KeyDecodeScratch& scratch, const ChunkOffset chunk_offset,
                                            std::byte* key_out, StringSpillBuffer& /*spill_buffer*/) const {
  std::memset(key_out, 0, _packed_width);
  pack_numeric_lanes(_lanes, scratch, chunk_offset, key_out);
}

inline uint64_t NumericArbitraryKeySchema::hash(const std::byte* key) const {
  return mix64(hash_bytes(key, _packed_width));
}

inline bool NumericArbitraryKeySchema::equals(const std::byte* lhs, const std::byte* rhs) const {
  return std::memcmp(lhs, rhs, _packed_width) == 0;
}

}  // namespace hyrise
