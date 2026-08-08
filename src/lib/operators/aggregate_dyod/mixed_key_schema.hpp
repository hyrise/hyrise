#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
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
 * Group-by key schema for a mix of at least one string and at least one non-string column.
 *
 * Layout: a runtime-width fixed part (null bitmap + numeric prefix + inline string blob) followed by an 8-byte spill
 * pointer. LenWidth in {1,2,4,8} is the per-string length-prefix field width; the fixed-part width is a runtime value.
 *
 * Equality:
 *   1. If the two keys disagree on spill mode (one pointer null, the other not) they are not equal and never fall
 *      through: identical content always makes the identical inline-vs-spill decision, so different modes prove
 *      different content, and this also avoids a deep compare through a null pointer.
 *   2. Otherwise memcmp the fixed part; a mismatch means not equal.
 *   3. Otherwise, if inline (both pointers null), they are equal.
 *   4. Otherwise (both spilled, and the fixed bytes matched only on the content-hash) deep-compare the spilled bytes.
 * hash() reuses whatever the inline field holds (raw bytes hashed, or the stored content-hash reused). Equal keys are
 * always same-mode, so equal keys always hash equal.
 */
template <size_t LenWidth>
class MixedKeySchema {
 public:
  static constexpr KeyComposition COMPOSITION = KeyComposition::Mixed;
  static constexpr bool HAS_STRINGS = true;
  static constexpr size_t LENGTH_FIELD_WIDTH = LenWidth;

  /**
   * Build the schema: resolve numeric lanes and string columns, lay out the fixed part, and record its width.
   */
  static MixedKeySchema build(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table,
                              std::optional<size_t> string_blob_bytes = std::nullopt);

  /**
   * Upper bound of a key's footprint in bytes: fixed_part_width() plus the 8-byte spill pointer.
   */
  size_t packed_width() const;

  size_t fixed_part_width() const;

  size_t column_count() const;

  // decode/pack/unpack/hash/equals as on NumericShortKeySchema; pack() spills overlong strings and equals() runs the
  // spill-mode-aware protocol documented on the class.
  void decode(std::span<const AbstractSegment* const> group_by_segments, size_t row_begin, size_t row_end,
              KeyDecodeScratch& scratch) const;
  void decode(std::span<const AbstractSegment* const> group_by_segments, KeyDecodeScratch& scratch) const;
  void pack(const KeyDecodeScratch& scratch, ChunkOffset chunk_offset, std::byte* key_out,
            StringSpillBuffer& spill_buffer) const;
  void unpack(const std::byte* key, OutputColumns& output, size_t output_row) const;
  uint64_t hash(const std::byte* key) const;
  bool equals(const std::byte* a, const std::byte* b) const;
  /**
   * Re-intern a spilled key's string content into `spill_buffer` and repoint the key's spill pointer there.
   */
  void reintern_spill(std::byte* key, StringSpillBuffer& spill_buffer) const;

 private:
  NumericKeyLanes _numeric_lanes;
  KeyTupleIndices _numeric_tuple_indices;
  StringKeyColumns _string_columns;
  uint32_t _blob_offset{0};
  uint32_t _fixed_part_width{0};
};

template <size_t LenWidth>
MixedKeySchema<LenWidth> MixedKeySchema<LenWidth>::build(const std::vector<ColumnID>& group_by_column_ids,
                                                         const Table& input_table,
                                                         const std::optional<size_t> string_blob_bytes) {
  const auto layout = compute_key_layout(group_by_column_ids, input_table, LenWidth, string_blob_bytes);
  Assert(layout.string_count > 0 && layout.string_count < group_by_column_ids.size(),
         "MixedKeySchema requires at least one string and at least one non-string group-by column.");

  auto schema = MixedKeySchema{};
  schema._blob_offset = static_cast<uint32_t>(layout.blob_offset);
  schema._fixed_part_width = static_cast<uint32_t>(layout.fixed_part_width);
  const auto column_count = group_by_column_ids.size();
  for (auto index = size_t{0}; index < column_count; ++index) {
    const auto& column = layout.columns[index];
    if (column.is_string) {
      schema._string_columns.emplace_back(StringKeyColumn{group_by_column_ids[index], static_cast<uint32_t>(index),
                                                          column.field_offset, column.null_bit_index});
    } else {
      schema._numeric_lanes.emplace_back(
          make_numeric_lane(column.data_type, group_by_column_ids[index], column.field_offset, column.null_bit_index));
      schema._numeric_tuple_indices.emplace_back(static_cast<uint32_t>(index));
    }
  }
  return schema;
}

template <size_t LenWidth>
void MixedKeySchema<LenWidth>::decode(const std::span<const AbstractSegment* const> group_by_segments,
                                      const size_t row_begin, const size_t row_end, KeyDecodeScratch& scratch) const {
  const auto lane_count = _numeric_lanes.size();
  scratch.numeric_lanes.resize(lane_count);
  for (auto index = size_t{0}; index < lane_count; ++index) {
    _numeric_lanes[index].lane->decode(*group_by_segments[_numeric_tuple_indices[index]], row_begin, row_end,
                                       scratch.numeric_lanes[index]);
  }
  decode_string_key_columns(_string_columns, group_by_segments, row_begin, row_end, scratch);
}

template <size_t LenWidth>
void MixedKeySchema<LenWidth>::decode(const std::span<const AbstractSegment* const> group_by_segments,
                                      KeyDecodeScratch& scratch) const {
  decode(group_by_segments, 0, group_by_segments.front()->size(), scratch);
}

template <size_t LenWidth>
void MixedKeySchema<LenWidth>::unpack(const std::byte* key, OutputColumns& output, const size_t output_row) const {
  const auto lane_count = _numeric_lanes.size();
  for (auto index = size_t{0}; index < lane_count; ++index) {
    _numeric_lanes[index].lane->unpack(key, key, output, _numeric_tuple_indices[index], output_row);
  }
  unpack_string_columns(_string_columns, LenWidth, _blob_offset, _fixed_part_width, key, output);
}

template <size_t LenWidth>
void MixedKeySchema<LenWidth>::reintern_spill(std::byte* key, StringSpillBuffer& spill_buffer) const {
  reintern_spilled_key(_string_columns, LenWidth, _fixed_part_width, key, spill_buffer);
}

template <size_t LenWidth>
size_t MixedKeySchema<LenWidth>::packed_width() const {
  return _fixed_part_width + sizeof(uintptr_t);
}

template <size_t LenWidth>
size_t MixedKeySchema<LenWidth>::fixed_part_width() const {
  return _fixed_part_width;
}

template <size_t LenWidth>
size_t MixedKeySchema<LenWidth>::column_count() const {
  return _numeric_lanes.size() + _string_columns.size();
}

template <size_t LenWidth>
void MixedKeySchema<LenWidth>::pack(const KeyDecodeScratch& scratch, const ChunkOffset chunk_offset, std::byte* key_out,
                                    StringSpillBuffer& spill_buffer) const {
  std::memset(key_out, 0, packed_width());
  pack_numeric_lanes(_numeric_lanes, scratch, chunk_offset, key_out);
  pack_string_columns(_string_columns, LenWidth, _blob_offset, _fixed_part_width, scratch, chunk_offset, key_out,
                      spill_buffer);
}

template <size_t LenWidth>
uint64_t MixedKeySchema<LenWidth>::hash(const std::byte* key) const {
  return mix64(hash_bytes(key, _fixed_part_width));
}

template <size_t LenWidth>
bool MixedKeySchema<LenWidth>::equals(const std::byte* a, const std::byte* b) const {
  return equals_string_keys(_string_columns, LenWidth, _fixed_part_width, a, b);
}

}  // namespace hyrise
