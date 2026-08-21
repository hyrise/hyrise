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
 * Group-by key schema for a tuple of only string columns: a MixedKeySchema whose numeric prefix has zero width.
 */
template <size_t LenWidth>
class StringOnlyKeySchema {
 public:
  static_assert(LenWidth == 1 || LenWidth == 2 || LenWidth == 4 || LenWidth == 8,
                "LenWidth is the byte width of each string's length prefix and must be 1, 2, 4, or 8.");

  static constexpr KeyComposition COMPOSITION = KeyComposition::StringOnly;
  static constexpr bool HAS_STRINGS = true;
  static constexpr size_t LENGTH_FIELD_WIDTH = LenWidth;

  /**
   * Build the schema: resolve one string column per group-by column and lay out the fixed part.
   */
  static StringOnlyKeySchema build(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table,
                                   std::optional<size_t> string_blob_bytes = std::nullopt);

  /**
   * Upper bound of a key's footprint in bytes: fixed_part_width() plus the 8-byte spill pointer.
   */
  size_t packed_width() const;

  size_t fixed_part_width() const;

  size_t column_count() const;

  // decode/pack/unpack/hash/equals/reintern_spill match MixedKeySchema; this schema is a MixedKeySchema whose numeric
  // prefix has zero width.
  void decode(std::span<const AbstractSegment* const> group_by_segments, size_t row_begin, size_t row_end,
              KeyDecodeScratch& scratch) const;
  void decode(std::span<const AbstractSegment* const> group_by_segments, KeyDecodeScratch& scratch) const;
  void pack(const KeyDecodeScratch& scratch, ChunkOffset chunk_offset, std::byte* key_out,
            StringSpillBuffer& spill_buffer) const;
  void unpack(const std::byte* key, OutputColumns& output, size_t output_row) const;
  uint64_t hash(const std::byte* key) const;
  bool equals(const std::byte* a, const std::byte* b) const;
  void reintern_spill(std::byte* key, StringSpillBuffer& spill_buffer) const;

 private:
  StringKeyColumns _string_columns;
  uint32_t _blob_offset{0};
  uint32_t _fixed_part_width{0};
};

template <size_t LenWidth>
StringOnlyKeySchema<LenWidth> StringOnlyKeySchema<LenWidth>::build(const std::vector<ColumnID>& group_by_column_ids,
                                                                   const Table& input_table,
                                                                   const std::optional<size_t> string_blob_bytes) {
  const auto layout = compute_key_layout(group_by_column_ids, input_table, LenWidth, string_blob_bytes);
  Assert(layout.string_count == group_by_column_ids.size(),
         "StringOnlyKeySchema requires string-only group-by columns.");

  auto schema = StringOnlyKeySchema{};
  schema._blob_offset = static_cast<uint32_t>(layout.blob_offset);
  schema._fixed_part_width = static_cast<uint32_t>(layout.fixed_part_width);
  const auto column_count = group_by_column_ids.size();
  for (auto index = size_t{0}; index < column_count; ++index) {
    const auto& column = layout.columns[index];
    schema._string_columns.emplace_back(StringKeyColumn{group_by_column_ids[index], static_cast<uint32_t>(index),
                                                        column.field_offset, column.null_bit_index});
  }
  return schema;
}

template <size_t LenWidth>
void StringOnlyKeySchema<LenWidth>::decode(const std::span<const AbstractSegment* const> group_by_segments,
                                           const size_t row_begin, const size_t row_end,
                                           KeyDecodeScratch& scratch) const {
  decode_string_key_columns(_string_columns, group_by_segments, row_begin, row_end, scratch);
}

template <size_t LenWidth>
void StringOnlyKeySchema<LenWidth>::decode(const std::span<const AbstractSegment* const> group_by_segments,
                                           KeyDecodeScratch& scratch) const {
  decode(group_by_segments, 0, group_by_segments.front()->size(), scratch);
}

template <size_t LenWidth>
void StringOnlyKeySchema<LenWidth>::unpack(const std::byte* key, OutputColumns& output,
                                           const size_t /*output_row*/) const {
  unpack_string_columns(_string_columns, LenWidth, _blob_offset, _fixed_part_width, key, output);
}

template <size_t LenWidth>
void StringOnlyKeySchema<LenWidth>::reintern_spill(std::byte* key, StringSpillBuffer& spill_buffer) const {
  reintern_spilled_key(_string_columns, LenWidth, _fixed_part_width, key, spill_buffer);
}

template <size_t LenWidth>
size_t StringOnlyKeySchema<LenWidth>::packed_width() const {
  return _fixed_part_width + sizeof(uintptr_t);
}

template <size_t LenWidth>
size_t StringOnlyKeySchema<LenWidth>::fixed_part_width() const {
  return _fixed_part_width;
}

template <size_t LenWidth>
size_t StringOnlyKeySchema<LenWidth>::column_count() const {
  return _string_columns.size();
}

template <size_t LenWidth>
void StringOnlyKeySchema<LenWidth>::pack(const KeyDecodeScratch& scratch, const ChunkOffset chunk_offset,
                                         std::byte* key_out, StringSpillBuffer& spill_buffer) const {
  std::memset(key_out, 0, packed_width());
  pack_string_columns(_string_columns, LenWidth, _blob_offset, _fixed_part_width, scratch, chunk_offset, key_out,
                      spill_buffer);
}

template <size_t LenWidth>
uint64_t StringOnlyKeySchema<LenWidth>::hash(const std::byte* key) const {
  return mix64(hash_bytes(key, _fixed_part_width));
}

template <size_t LenWidth>
bool StringOnlyKeySchema<LenWidth>::equals(const std::byte* a, const std::byte* b) const {
  return equals_string_keys(_string_columns, LenWidth, _fixed_part_width, a, b);
}

}  // namespace hyrise
