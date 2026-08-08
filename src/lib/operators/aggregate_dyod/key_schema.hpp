#pragma once

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/container/small_vector.hpp>

#include "all_type_variant.hpp"
#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "operators/aggregate_dyod/hyperloglog.hpp"
#include "operators/aggregate_dyod/output_columns.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

// ============================================================================================================
// Group-by key representation for AggregateDYOD.
//
// A "key" is the group-by column tuple of one row, encoded into a flat, comparable byte buffer.
//
// Monomorphization. The schema type is selected once per query by resolve_key_schema(), which inspects the
// group-by columns and dispatches to one of a bounded set of concrete schema types; the scatter and merge pipelines are
// then instantiated over that one concrete type. The types:
//
//   NumericShortKeySchema<Width>: Width in {4,8,12,16,20,24} bytes (numeric-only group-by. hash/equals fixed-size).
//   NumericArbitraryKeySchema: numeric-only group-by wider than 24 bytes; runtime-length hash/equals.
//   MixedKeySchema<LenWidth>: at least one string and at least one non-string column. LenWidth in {1,2,4,8} is the
//                             per-string length-prefix field width.
//   StringOnlyKeySchema<LenWidth>: all columns are strings; a MixedKeySchema with a zero-width numeric prefix.
//
// Layout (all four families):
//   [ null bitmap | numeric prefix | inline string blob | 8-byte spill pointer ]
// The null bitmap carries one bit per nullable group-by column (present only if any group-by column is nullable) and is
// padded so the fixed part stays a multiple of 4. The inline string blob is padded at its tail for the same reason,
// which is what holds the fixed part on a multiple of 4 once the blob capacity is sized from measured string lengths
// rather than from STRING_BLOB_BYTES_PER_COLUMN (see choose_string_key_budget). The string blob and spill pointer are
// absent for numeric-only schemas.
//
// NULL is carried out of band in the null bitmap, never as an in-band sentinel: a "+1 and reserve 0" scheme collides
// at TYPE_MAX, and no byte pattern is safe for a full-range column.
// ============================================================================================================

/**
 * Which column-type families a query's group-by tuple contains. Fixed once per query and used to pick the schema
 * type.
 */
enum class KeyComposition : uint8_t { NumericOnly, Mixed, StringOnly };

/**
 * Rewrite -0.0 to +0.0 and every NaN to the one quiet-NaN pattern, so byte-equality on the result matches value
 * equality.
 */
inline float canonicalize(const float value) {
  if (std::isnan(value)) {
    return std::numeric_limits<float>::quiet_NaN();
  }
  if (value == 0.0f) {
    // Rewrites -0.0 to +0.0.
    return 0.0f;
  }
  return value;
}

inline double canonicalize(const double value) {
  if (std::isnan(value)) {
    return std::numeric_limits<double>::quiet_NaN();
  }
  if (value == 0.0) {
    return 0.0;
  }
  return value;
}

/** FNV-1a over a byte range; the seeded overload continues a previous hash. */
inline uint64_t hash_bytes(const std::byte* data, const size_t length, const uint64_t seed) {
  constexpr auto FNV_PRIME = uint64_t{1099511628211ull};
  auto hash = seed;
  for (auto index = size_t{0}; index < length; ++index) {
    hash ^= static_cast<uint64_t>(data[index]);
    hash *= FNV_PRIME;
  }
  return hash;
}

inline uint64_t hash_bytes(const std::byte* data, const size_t length) {
  constexpr auto FNV_OFFSET_BASIS = uint64_t{14695981039346656037ull};
  return hash_bytes(data, length, FNV_OFFSET_BASIS);
}

/**
 * Overflow storage for string key content that does not fit a key's inline string blob.
 *
 * When a group-by string is too long for the inline blob, its bytes are copied here and the key instead holds a
 * content-hash in its inline field plus an 8-byte pointer into this buffer. This bounds the fixed part of every key
 * while still supporting arbitrarily long strings, and lets equality short-circuit on the hash before a deep compare.
 *
 * A pointer returned by append() stays valid until clear() or destruction: the buffer chains fresh blocks and never
 * relocates live content, which is what lets a key hold a raw pointer rather than a relocatable offset.
 */
class StringSpillBuffer : private Noncopyable {
 public:
  /**
   * Copy `length` bytes of string content into the buffer and return a stable pointer to the interned copy.
   */
  const std::byte* append(const std::byte* content, size_t length);

  /**
   * Drop all stored content while retaining allocated capacity for reuse.
   */
  void clear();

  /** Free the blocks instead of keeping them for reuse. */
  void release();

  size_t memory_usage() const;

 private:
  struct Block {
    std::unique_ptr<std::byte[]> data;
    size_t capacity{0};
    size_t used{0};
  };

  static constexpr size_t MIN_BLOCK_BYTES = 16 * 1024;

  std::vector<Block> _blocks;
  size_t _current_block{0};
};

inline const std::byte* StringSpillBuffer::append(const std::byte* content, const size_t length) {
  while (_current_block < _blocks.size() && _blocks[_current_block].used + length > _blocks[_current_block].capacity) {
    ++_current_block;
  }
  if (_current_block == _blocks.size()) {
    auto block = Block{};
    block.capacity = std::max(MIN_BLOCK_BYTES, length);
    block.data = std::make_unique<std::byte[]>(block.capacity);
    _blocks.emplace_back(std::move(block));
  }

  auto& block = _blocks[_current_block];
  auto* destination = block.data.get() + block.used;
  std::memcpy(destination, content, length);
  block.used += length;
  return destination;
}

inline void StringSpillBuffer::clear() {
  for (auto& block : _blocks) {
    block.used = 0;
  }
  _current_block = 0;
}

inline void StringSpillBuffer::release() {
  _blocks = std::vector<Block>{};
  _current_block = 0;
}

inline size_t StringSpillBuffer::memory_usage() const {
  auto bytes = sizeof(*this);
  bytes += _blocks.capacity() * sizeof(Block);
  for (const auto& block : _blocks) {
    bytes += block.capacity;
  }
  return bytes;
}

// Marks a column that has no bit in the null bitmap because it is not nullable.
constexpr uint32_t NO_NULL_BIT = std::numeric_limits<uint32_t>::max();

// Inline capacity for the per-schema lane/column vectors: most group-bys have at most this many columns, so the lanes
// stay inline (no heap allocation) in the common case.
constexpr size_t EXPECTED_GROUP_BY_COLUMNS = 4;

/**
 * Worker-local decoded copy of a row window of one chunk's group-by columns, filled by KeySchema::decode() and
 * consumed row-wise by KeySchema::pack().
 */
struct KeyDecodeScratch {
  struct NumericLane {
    std::vector<std::byte> values;
    std::vector<uint8_t> nulls;
  };

  struct StringColumn {
    std::vector<std::string_view> values;
    std::vector<uint8_t> nulls;
    std::vector<pmr_string> owned;
  };

  boost::container::small_vector<NumericLane, EXPECTED_GROUP_BY_COLUMNS> numeric_lanes;
  boost::container::small_vector<StringColumn, EXPECTED_GROUP_BY_COLUMNS> string_columns;
};

/**
 * Polymorphic handler for one numeric group-by column: a fixed-width lane at a byte offset in the numeric prefix.
 *
 * One instance is resolved per numeric group-by column at schema build (concrete subclass NumericKeyLane<T> per
 * DataType) and held by the schema; decode() is then called once per chunk and unpack() once per output row.
 */
class AbstractNumericKeyLane {
 public:
  virtual ~AbstractNumericKeyLane() = default;

  /**
   * Decode rows [row_begin, row_end) of one chunk's column into the lane's flat scratch buffer.
   *
   * Integer lanes apply the sign-bit-XOR bias; float/double lanes canonicalize -0.0 and NaN, so whole-buffer
   * byte-equality matches value-equality. The schema's per-row pack loop copies the stored bytes into the key. NULL
   * rows store zero bytes and set their null flag.
   */
  virtual void decode(const AbstractSegment& segment, size_t row_begin, size_t row_end,
                      KeyDecodeScratch::NumericLane& lane) const = 0;

  /**
   * Reverse of pack(): decode this lane's value and append it (or a NULL) to its output column.
   */
  virtual void unpack(const std::byte* key, const std::byte* null_bitmap, OutputColumns& output,
                      size_t output_column_index, size_t output_row) const = 0;
};

/**
 * Concrete numeric lane, monomorphized over the column's type T (int32_t/int64_t/float/double).
 *
 * Implements AbstractNumericKeyLane: decode() applies the sign-bit-XOR bias for integers or the -0.0/NaN
 * canonicalization for floats, and unpack() inverts it.
 */
template <typename T>
class NumericKeyLane : public AbstractNumericKeyLane {
 public:
  NumericKeyLane(ColumnID column_id, uint32_t field_offset, uint32_t null_bit_index);

  void decode(const AbstractSegment& segment, size_t row_begin, size_t row_end,
              KeyDecodeScratch::NumericLane& lane) const override;
  void unpack(const std::byte* key, const std::byte* null_bitmap, OutputColumns& output, size_t output_column_index,
              size_t output_row) const override;

 private:
  ColumnID _column_id;
  uint32_t _field_offset;
  uint32_t _null_bit_index;
};

/**
 * Descriptor for one string group-by column: where its length-prefix field lives and which null bit it owns.
 *
 * Unlike the numeric lanes there is no polymorphic handler: string cells are always pmr_string, so there is no type
 * axis to dispatch on. The schema drives string packing/unpacking itself, row-wise, because the inline-vs-spill
 * decision spans all string columns of a key (a key carries a single spill pointer).
 */
struct StringKeyColumn {
  ColumnID column_id;
  uint32_t tuple_index;
  uint32_t length_field_offset;
  uint32_t null_bit_index;
};

struct NumericLaneField {
  uint32_t field_offset;
  uint32_t width;
  uint32_t null_bit_index;
};

// One resolved numeric group-by column: the polymorphic lane plus its layout facts.
struct NumericKeyLaneEntry {
  std::unique_ptr<AbstractNumericKeyLane> lane;
  NumericLaneField field;
};

using NumericKeyLanes = boost::container::small_vector<NumericKeyLaneEntry, EXPECTED_GROUP_BY_COLUMNS>;
using StringKeyColumns = boost::container::small_vector<StringKeyColumn, EXPECTED_GROUP_BY_COLUMNS>;
using KeyTupleIndices = boost::container::small_vector<uint32_t, EXPECTED_GROUP_BY_COLUMNS>;

// Helpers shared by the schema types below.

inline void set_null_bit(std::byte* null_bitmap, const size_t bit_index) {
  null_bitmap[bit_index / 8] |= std::byte{1} << (bit_index % 8);
}

inline bool null_bit_set(const std::byte* null_bitmap, const size_t bit_index) {
  return (null_bitmap[bit_index / 8] & (std::byte{1} << (bit_index % 8))) != std::byte{0};
}

inline size_t numeric_lane_width(const DataType data_type) {
  switch (data_type) {
    case DataType::Int:
    case DataType::Float:
      return 4;
    case DataType::Long:
    case DataType::Double:
      return 8;
    default:
      Fail("Not a numeric data type.");
  }
}

inline uint32_t encode_lane_value(const int32_t value) {
  return static_cast<uint32_t>(value) ^ (uint32_t{1} << 31);
}

inline uint64_t encode_lane_value(const int64_t value) {
  return static_cast<uint64_t>(value) ^ (uint64_t{1} << 63);
}

inline uint32_t encode_lane_value(const float value) {
  return std::bit_cast<uint32_t>(canonicalize(value));
}

inline uint64_t encode_lane_value(const double value) {
  return std::bit_cast<uint64_t>(canonicalize(value));
}

template <typename T>
T decode_lane_value(const std::byte* field) {
  if constexpr (std::is_same_v<T, int32_t>) {
    auto encoded = uint32_t{};
    std::memcpy(&encoded, field, sizeof(encoded));
    return static_cast<int32_t>(encoded ^ (uint32_t{1} << 31));
  } else if constexpr (std::is_same_v<T, int64_t>) {
    auto encoded = uint64_t{};
    std::memcpy(&encoded, field, sizeof(encoded));
    return static_cast<int64_t>(encoded ^ (uint64_t{1} << 63));
  } else {
    auto value = T{};
    std::memcpy(&value, field, sizeof(value));
    return value;
  }
}

// Assumes a little-endian machine.
inline void write_length_field(std::byte* key, const uint32_t field_offset, const size_t length_field_width,
                               const size_t length) {
  if (length_field_width < sizeof(uint64_t)) {
    Assert(length < (uint64_t{1} << (8 * length_field_width)), "String length exceeds the length-prefix field.");
  }
  const auto value = static_cast<uint64_t>(length);
  std::memcpy(key + field_offset, &value, length_field_width);
}

inline size_t read_length_field(const std::byte* key, const uint32_t field_offset, const size_t length_field_width) {
  auto value = uint64_t{0};
  std::memcpy(&value, key + field_offset, length_field_width);
  return static_cast<size_t>(value);
}

inline uintptr_t read_spill_pointer(const std::byte* key, const size_t fixed_part_width) {
  auto pointer_value = uintptr_t{0};
  std::memcpy(&pointer_value, key + fixed_part_width, sizeof(pointer_value));
  return pointer_value;
}

inline void pack_numeric_lanes(const NumericKeyLanes& lanes, const KeyDecodeScratch& scratch,
                               const ChunkOffset chunk_offset, std::byte* key_out) {
  const auto lane_count = lanes.size();
  for (auto index = size_t{0}; index < lane_count; ++index) {
    const auto& field = lanes[index].field;
    const auto& lane = scratch.numeric_lanes[index];
    if (lane.nulls[chunk_offset]) {
      set_null_bit(key_out, field.null_bit_index);
      continue;
    }
    const auto* source = lane.values.data() + size_t{chunk_offset} * field.width;
    if (field.width == 4) {
      std::memcpy(key_out + field.field_offset, source, 4);
    } else {
      std::memcpy(key_out + field.field_offset, source, 8);
    }
  }
}

inline void pack_string_columns(const StringKeyColumns& string_columns, const size_t length_field_width,
                                const size_t blob_offset, const size_t fixed_part_width,
                                const KeyDecodeScratch& scratch, const ChunkOffset chunk_offset, std::byte* key_out,
                                StringSpillBuffer& spill_buffer) {
  auto total_length = size_t{0};
  const auto column_count = string_columns.size();
  for (auto index = size_t{0}; index < column_count; ++index) {
    const auto& column = string_columns[index];
    const auto& decoded = scratch.string_columns[index];
    if (decoded.nulls[chunk_offset]) {
      DebugAssert(column.null_bit_index != NO_NULL_BIT, "NULL in a non-nullable group-by column.");
      set_null_bit(key_out, column.null_bit_index);
      continue;
    }
    const auto& value = decoded.values[chunk_offset];
    write_length_field(key_out, column.length_field_offset, length_field_width, value.size());
    total_length += value.size();
  }

  // NULL rows decode to empty views with a null data pointer, which must not reach memcpy.
  const auto blob_capacity = fixed_part_width - blob_offset;
  if (total_length <= blob_capacity) {
    auto* cursor = key_out + blob_offset;
    for (auto index = size_t{0}; index < column_count; ++index) {
      if (scratch.string_columns[index].nulls[chunk_offset]) {
        continue;
      }
      const auto& value = scratch.string_columns[index].values[chunk_offset];
      std::memcpy(cursor, value.data(), value.size());
      cursor += value.size();
    }
    return;
  }

  auto content = std::vector<std::byte>{};
  content.reserve(total_length);
  for (auto index = size_t{0}; index < column_count; ++index) {
    if (scratch.string_columns[index].nulls[chunk_offset]) {
      continue;
    }
    const auto& value = scratch.string_columns[index].values[chunk_offset];
    const auto* bytes = reinterpret_cast<const std::byte*>(value.data());
    content.insert(content.end(), bytes, bytes + value.size());
  }
  const auto* interned = spill_buffer.append(content.data(), content.size());
  const auto content_hash = hash_bytes(content.data(), content.size());
  std::memcpy(key_out + blob_offset, &content_hash, sizeof(content_hash));
  const auto pointer_value = reinterpret_cast<uintptr_t>(interned);
  std::memcpy(key_out + fixed_part_width, &pointer_value, sizeof(pointer_value));
}

inline bool equals_string_keys(const StringKeyColumns& string_columns, const size_t length_field_width,
                               const size_t fixed_part_width, const std::byte* a, const std::byte* b) {
  const auto pointer_a = read_spill_pointer(a, fixed_part_width);
  const auto pointer_b = read_spill_pointer(b, fixed_part_width);
  if ((pointer_a != 0) != (pointer_b != 0)) {
    return false;
  }
  if (std::memcmp(a, b, fixed_part_width) != 0) {
    return false;
  }
  if (pointer_a == 0) {
    return true;
  }

  auto total_length = size_t{0};
  for (const auto& column : string_columns) {
    total_length += read_length_field(a, column.length_field_offset, length_field_width);
  }
  return std::memcmp(reinterpret_cast<const std::byte*>(pointer_a), reinterpret_cast<const std::byte*>(pointer_b),
                     total_length) == 0;
}

struct KeyLayout {
  struct Column {
    DataType data_type{DataType::Null};
    bool is_string{false};
    uint32_t field_offset{0};  // numeric lane offset, or string length-field offset
    uint32_t null_bit_index{NO_NULL_BIT};
  };

  boost::container::small_vector<Column, EXPECTED_GROUP_BY_COLUMNS> columns;
  size_t string_count{0};
  size_t blob_offset{0};
  size_t fixed_part_width{0};
};

// `string_blob_bytes` is the total inline blob capacity; std::nullopt spends STRING_BLOB_BYTES_PER_COLUMN on every
// string column, while a caller that has bounded the string lengths passes the tighter budget it measured. Either way
// the blob is padded at its tail so the fixed part stays a multiple of 4 bytes.
inline KeyLayout compute_key_layout(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table,
                                    const size_t length_field_width,
                                    const std::optional<size_t> string_blob_bytes = std::nullopt) {
  auto layout = KeyLayout{};
  layout.columns.reserve(group_by_column_ids.size());

  auto nullable_count = uint32_t{0};
  auto numeric_width = size_t{0};
  for (const auto column_id : group_by_column_ids) {
    auto column = KeyLayout::Column{};
    column.data_type = input_table.column_data_type(column_id);
    column.is_string = column.data_type == DataType::String;
    if (input_table.column_is_nullable(column_id)) {
      column.null_bit_index = nullable_count;
      ++nullable_count;
    }
    if (!column.is_string) {
      numeric_width += numeric_lane_width(column.data_type);
    } else {
      ++layout.string_count;
    }
    layout.columns.emplace_back(column);
  }

  // The null bitmap region is padded to a multiple of 4 bytes.
  const auto bitmap_region = nullable_count > 0 ? ((nullable_count + 7) / 8 + 3) / 4 * 4 : size_t{0};

  auto numeric_cursor = bitmap_region;
  auto length_field_cursor = bitmap_region + numeric_width;
  for (auto& column : layout.columns) {
    if (column.is_string) {
      column.field_offset = static_cast<uint32_t>(length_field_cursor);
      length_field_cursor += length_field_width;
    } else {
      column.field_offset = static_cast<uint32_t>(numeric_cursor);
      numeric_cursor += numeric_lane_width(column.data_type);
    }
  }

  layout.blob_offset = length_field_cursor;
  if (layout.string_count == 0) {
    layout.fixed_part_width = layout.blob_offset;
    return layout;
  }

  const auto blob_bytes = string_blob_bytes.value_or(STRING_BLOB_BYTES_PER_COLUMN * layout.string_count);
  layout.fixed_part_width = (layout.blob_offset + blob_bytes + 3) / 4 * 4;
  return layout;
}

inline NumericKeyLaneEntry make_numeric_lane(const DataType data_type, const ColumnID column_id,
                                             const uint32_t field_offset, const uint32_t null_bit_index) {
  auto lane = std::unique_ptr<AbstractNumericKeyLane>{};
  switch (data_type) {
    case DataType::Int:
      lane = std::make_unique<NumericKeyLane<int32_t>>(column_id, field_offset, null_bit_index);
      break;
    case DataType::Long:
      lane = std::make_unique<NumericKeyLane<int64_t>>(column_id, field_offset, null_bit_index);
      break;
    case DataType::Float:
      lane = std::make_unique<NumericKeyLane<float>>(column_id, field_offset, null_bit_index);
      break;
    case DataType::Double:
      lane = std::make_unique<NumericKeyLane<double>>(column_id, field_offset, null_bit_index);
      break;
    default:
      Fail("Not a numeric group-by column.");
  }
  return {std::move(lane),
          NumericLaneField{field_offset, static_cast<uint32_t>(numeric_lane_width(data_type)), null_bit_index}};
}

/**
 * Visit rows [row_begin, row_end) of a segment that stores its values where a single row can be addressed directly,
 * calling `visitor(window_row, value)` in ascending row order.
 */
template <typename T, typename Visitor>
bool iterate_stable_segment_window(const AbstractSegment& segment, const size_t row_begin, const size_t row_end,
                                   const Visitor& visitor) {
  // The attribute vector's compression is resolved once per window, so reading a value id costs no virtual call.
  const auto visit_dictionary = [&](const DictionarySegment<T>& dictionary_segment, const auto& row_to_id) {
    const auto& dictionary = *dictionary_segment.dictionary();
    const auto null_value_id = dictionary_segment.null_value_id();
    resolve_compressed_vector_type(*dictionary_segment.attribute_vector(), [&](const auto& attribute_vector) {
      const auto decompressor = attribute_vector.create_decompressor();
      for (auto row = row_begin; row < row_end; ++row) {
        const auto value_id = row_to_id(decompressor, row);
        visitor(row - row_begin, value_id == null_value_id ? nullptr : &dictionary[value_id]);
      }
    });
  };

  if (const auto* dictionary_segment = dynamic_cast<const DictionarySegment<T>*>(&segment)) {
    visit_dictionary(*dictionary_segment, [](const auto& decompressor, const size_t row) {
      return ValueID{decompressor.get(row)};
    });
    return true;
  }

  if (const auto* value_segment = dynamic_cast<const ValueSegment<T>*>(&segment)) {
    const auto& values = value_segment->values();
    const auto* null_values = value_segment->is_nullable() ? &value_segment->null_values() : nullptr;
    for (auto row = row_begin; row < row_end; ++row) {
      const auto is_null = null_values && (*null_values)[row];
      visitor(row - row_begin, is_null ? nullptr : &values[row]);
    }
    return true;
  }

  const auto* reference_segment = dynamic_cast<const ReferenceSegment*>(&segment);
  if (!reference_segment) {
    return false;
  }
  const auto& pos_list = *reference_segment->pos_list();
  if (!pos_list.references_single_chunk() || pos_list.empty()) {
    return false;
  }

  const auto target = reference_segment->referenced_table()
                          ->get_chunk(pos_list.common_chunk_id())
                          ->get_segment(reference_segment->referenced_column_id());
  if (const auto* dictionary_segment = dynamic_cast<const DictionarySegment<T>*>(target.get())) {
    visit_dictionary(*dictionary_segment, [&](const auto& decompressor, const size_t row) {
      return ValueID{decompressor.get(pos_list[row].chunk_offset)};
    });
    return true;
  }
  if (const auto* value_segment = dynamic_cast<const ValueSegment<T>*>(target.get())) {
    const auto& values = value_segment->values();
    const auto* null_values = value_segment->is_nullable() ? &value_segment->null_values() : nullptr;
    for (auto row = row_begin; row < row_end; ++row) {
      const auto chunk_offset = pos_list[row].chunk_offset;
      const auto is_null = null_values && (*null_values)[chunk_offset];
      visitor(row - row_begin, is_null ? nullptr : &values[chunk_offset]);
    }
    return true;
  }
  return false;
}

/**
 * Visit rows [row_begin, row_end) of any segment, calling `visitor(window_row, value)` as
 * iterate_stable_segment_window() does.
 *
 * A segment outside the directly addressable shapes is scanned whole and the rows outside the window are dropped, so
 * it costs one scan of the chunk per window: a phase claiming windows over such a segment falls back to chunk
 * granularity in cost, never in correctness. Its values reach the visitor as a pointer that is valid for the duration
 * of the call only, so a visitor that keeps the value must copy it.
 */
template <typename T, typename Visitor>
void iterate_segment_window(const AbstractSegment& segment, const size_t row_begin, const size_t row_end,
                            const Visitor& visitor) {
  if (iterate_stable_segment_window<T>(segment, row_begin, row_end, visitor)) {
    return;
  }

  auto row = size_t{0};
  segment_iterate<T>(segment, [&](const auto& position) {
    const auto segment_row = row++;
    if (segment_row < row_begin || segment_row >= row_end) {
      return;
    }
    if (position.is_null()) {
      visitor(segment_row - row_begin, nullptr);
      return;
    }
    const auto& value = position.value();
    visitor(segment_row - row_begin, &value);
  });
}

inline void decode_numeric_lanes(const NumericKeyLanes& lanes,
                                 const std::span<const AbstractSegment* const> group_by_segments,
                                 const size_t row_begin, const size_t row_end, KeyDecodeScratch& scratch) {
  const auto lane_count = lanes.size();
  scratch.numeric_lanes.resize(lane_count);
  for (auto index = size_t{0}; index < lane_count; ++index) {
    lanes[index].lane->decode(*group_by_segments[index], row_begin, row_end, scratch.numeric_lanes[index]);
  }
}

// The decoded views point into the segment's own storage wherever that storage is stable for the chunk's lifetime;
// only segments outside those shapes are copied into the scratch's `owned` backing.
inline void decode_string_column(const AbstractSegment& segment, const size_t row_begin, const size_t row_end,
                                 KeyDecodeScratch::StringColumn& column) {
  const auto window_rows = row_end - row_begin;
  column.values.resize(window_rows);
  column.nulls.resize(window_rows);

  const auto viewed = iterate_stable_segment_window<pmr_string>(
      segment, row_begin, row_end, [&](const size_t row, const pmr_string* value) {
        if (value) {
          column.values[row] = std::string_view{*value};
          column.nulls[row] = 0;
        } else {
          column.values[row] = {};
          column.nulls[row] = 1;
        }
      });
  if (viewed) {
    return;
  }

  column.owned.resize(window_rows);
  auto row = size_t{0};
  segment_iterate<pmr_string>(segment, [&](const auto& position) {
    const auto segment_row = row++;
    if (segment_row < row_begin || segment_row >= row_end) {
      return;
    }
    const auto window_row = segment_row - row_begin;
    if (position.is_null()) {
      column.values[window_row] = {};
      column.nulls[window_row] = 1;
    } else {
      column.owned[window_row] = position.value();
      column.values[window_row] = std::string_view{column.owned[window_row]};
      column.nulls[window_row] = 0;
    }
  });
}

inline void decode_string_column(const AbstractSegment& segment, KeyDecodeScratch::StringColumn& column) {
  decode_string_column(segment, 0, segment.size(), column);
}

inline void decode_string_key_columns(const StringKeyColumns& string_columns,
                                      const std::span<const AbstractSegment* const> group_by_segments,
                                      const size_t row_begin, const size_t row_end, KeyDecodeScratch& scratch) {
  const auto string_count = string_columns.size();
  scratch.string_columns.resize(string_count);
  for (auto index = size_t{0}; index < string_count; ++index) {
    decode_string_column(*group_by_segments[string_columns[index].tuple_index], row_begin, row_end,
                         scratch.string_columns[index]);
  }
}

inline void unpack_string_columns(const StringKeyColumns& string_columns, const size_t length_field_width,
                                  const size_t blob_offset, const size_t fixed_part_width, const std::byte* key,
                                  OutputColumns& output) {
  const auto pointer_value = read_spill_pointer(key, fixed_part_width);
  const auto* cursor = pointer_value != 0 ? reinterpret_cast<const std::byte*>(pointer_value) : key + blob_offset;

  for (const auto& column : string_columns) {
    auto& output_column = static_cast<TypedOutputColumn<pmr_string>&>(output.column(column.tuple_index));
    if (column.null_bit_index != NO_NULL_BIT && null_bit_set(key, column.null_bit_index)) {
      output_column.append_null();
      continue;
    }
    const auto length = read_length_field(key, column.length_field_offset, length_field_width);
    output_column.append(pmr_string{reinterpret_cast<const char*>(cursor), length});
    cursor += length;
  }
}

inline void reintern_spilled_key(const StringKeyColumns& string_columns, const size_t length_field_width,
                                 const size_t fixed_part_width, std::byte* key, StringSpillBuffer& spill_buffer) {
  const auto pointer_value = read_spill_pointer(key, fixed_part_width);
  if (pointer_value == 0) {
    return;
  }

  auto total_length = size_t{0};
  for (const auto& column : string_columns) {
    total_length += read_length_field(key, column.length_field_offset, length_field_width);
  }
  const auto* interned = spill_buffer.append(reinterpret_cast<const std::byte*>(pointer_value), total_length);
  const auto new_pointer = reinterpret_cast<uintptr_t>(interned);
  std::memcpy(key + fixed_part_width, &new_pointer, sizeof(new_pointer));
}

template <typename T>
NumericKeyLane<T>::NumericKeyLane(const ColumnID column_id, const uint32_t field_offset, const uint32_t null_bit_index)
    : _column_id{column_id}, _field_offset{field_offset}, _null_bit_index{null_bit_index} {}

template <typename T>
void NumericKeyLane<T>::decode(const AbstractSegment& segment, const size_t row_begin, const size_t row_end,
                               KeyDecodeScratch::NumericLane& lane) const {
  using Encoded = decltype(encode_lane_value(T{}));
  const auto window_rows = row_end - row_begin;
  lane.values.resize(window_rows * sizeof(Encoded));
  lane.nulls.resize(window_rows);
  auto* values = lane.values.data();
  iterate_segment_window<T>(segment, row_begin, row_end, [&](const size_t row, const T* value) {
    auto encoded = Encoded{};
    if (value) {
      encoded = encode_lane_value(*value);
      lane.nulls[row] = 0;
    } else {
      DebugAssert(_null_bit_index != NO_NULL_BIT, "NULL in a non-nullable group-by column.");
      lane.nulls[row] = 1;
    }
    std::memcpy(values + row * sizeof(Encoded), &encoded, sizeof(encoded));
  });
}

template <typename T>
void NumericKeyLane<T>::unpack(const std::byte* key, const std::byte* null_bitmap, OutputColumns& output,
                               const size_t output_column_index, const size_t /*output_row*/) const {
  auto& output_column = static_cast<TypedOutputColumn<T>&>(output.column(output_column_index));
  if (_null_bit_index != NO_NULL_BIT && null_bit_set(null_bitmap, _null_bit_index)) {
    output_column.append_null();
    return;
  }
  output_column.append(decode_lane_value<T>(key + _field_offset));
}

/**
 * Numeric-only group-by key schema whose total packed width is the compile-time constant PackedWidth.
 *
 * Selected by resolve_key_schema when every group-by column is numeric and the packed width (null bitmap + numeric
 * prefix) is one of {4,8,12,16,20,24} bytes; one instantiation per bucket. Because the width is known at compile
 * time, hash() and equals() are fixed-size and branch-free, while pack()/unpack() loop the resolved lanes.
 */
template <size_t PackedWidth>
class NumericShortKeySchema {
 public:
  static constexpr KeyComposition COMPOSITION = KeyComposition::NumericOnly;
  static constexpr size_t WIDTH = PackedWidth;
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
  bool equals(const std::byte* a, const std::byte* b) const;

 private:
  NumericKeyLanes _lanes;
};

template <size_t PackedWidth>
NumericShortKeySchema<PackedWidth> NumericShortKeySchema<PackedWidth>::build(
    const std::vector<ColumnID>& group_by_column_ids, const Table& input_table) {
  const auto layout = compute_key_layout(group_by_column_ids, input_table, 0);
  Assert(layout.string_count == 0, "NumericShortKeySchema requires numeric-only group-by columns.");
  Assert(layout.fixed_part_width == PackedWidth, "Resolved packed width does not match the schema's template width.");

  auto schema = NumericShortKeySchema{};
  const auto column_count = group_by_column_ids.size();
  for (auto index = size_t{0}; index < column_count; ++index) {
    const auto& column = layout.columns[index];
    schema._lanes.emplace_back(
        make_numeric_lane(column.data_type, group_by_column_ids[index], column.field_offset, column.null_bit_index));
  }
  return schema;
}

template <size_t PackedWidth>
size_t NumericShortKeySchema<PackedWidth>::packed_width() const {
  return PackedWidth;
}

template <size_t PackedWidth>
size_t NumericShortKeySchema<PackedWidth>::column_count() const {
  return _lanes.size();
}

template <size_t PackedWidth>
void NumericShortKeySchema<PackedWidth>::decode(const std::span<const AbstractSegment* const> group_by_segments,
                                                const size_t row_begin, const size_t row_end,
                                                KeyDecodeScratch& scratch) const {
  decode_numeric_lanes(_lanes, group_by_segments, row_begin, row_end, scratch);
}

template <size_t PackedWidth>
void NumericShortKeySchema<PackedWidth>::decode(const std::span<const AbstractSegment* const> group_by_segments,
                                                KeyDecodeScratch& scratch) const {
  decode(group_by_segments, 0, group_by_segments.front()->size(), scratch);
}

template <size_t PackedWidth>
void NumericShortKeySchema<PackedWidth>::unpack(const std::byte* key, OutputColumns& output,
                                                const size_t output_row) const {
  const auto lane_count = _lanes.size();
  for (auto index = size_t{0}; index < lane_count; ++index) {
    _lanes[index].lane->unpack(key, key, output, index, output_row);
  }
}

template <size_t PackedWidth>
void NumericShortKeySchema<PackedWidth>::pack(const KeyDecodeScratch& scratch, const ChunkOffset chunk_offset,
                                              std::byte* key_out, StringSpillBuffer& /*spill_buffer*/) const {
  std::memset(key_out, 0, PackedWidth);
  pack_numeric_lanes(_lanes, scratch, chunk_offset, key_out);
}

template <size_t PackedWidth>
uint64_t NumericShortKeySchema<PackedWidth>::hash(const std::byte* key) const {
  // One multiply-mix round per word beats byte-wise FNV-1a; the hash runs up to three times per row (estimate,
  // scatter routing, merge probing), and its low bits pick the partition.
  auto hash = uint64_t{0};
  auto offset = size_t{0};
  for (; offset + 8 <= PackedWidth; offset += 8) {
    auto word = uint64_t{};
    std::memcpy(&word, key + offset, 8);
    hash = mix64(hash ^ word);
  }
  if constexpr (PackedWidth % 8 != 0) {
    auto word = uint32_t{};
    std::memcpy(&word, key + offset, 4);
    hash = mix64(hash ^ word);
  }
  return hash;
}

template <size_t PackedWidth>
bool NumericShortKeySchema<PackedWidth>::equals(const std::byte* a, const std::byte* b) const {
  return std::memcmp(a, b, PackedWidth) == 0;
}

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
  void unpack(const std::byte* key, OutputColumns& output, size_t output_row) const;
  uint64_t hash(const std::byte* key) const;
  bool equals(const std::byte* a, const std::byte* b) const;

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

inline void NumericArbitraryKeySchema::unpack(const std::byte* key, OutputColumns& output,
                                              const size_t output_row) const {
  const auto lane_count = _lanes.size();
  for (auto index = size_t{0}; index < lane_count; ++index) {
    _lanes[index].lane->unpack(key, key, output, index, output_row);
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

inline bool NumericArbitraryKeySchema::equals(const std::byte* a, const std::byte* b) const {
  return std::memcmp(a, b, _packed_width) == 0;
}

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

/**
 * Group-by key schema for a tuple of only string columns: a MixedKeySchema whose numeric prefix has zero width.
 */
template <size_t LenWidth>
class StringOnlyKeySchema {
 public:
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

/**
 * How a query's string key fields are sized: the per-string length-prefix field width and the total inline blob
 * capacity, as chosen by choose_string_key_budget.
 */
struct StringKeyBudget {
  size_t length_field_width{4};
  std::optional<size_t> blob_bytes{};
};

/**
 * Derive the tightest string-key field sizing the input table's encodings prove correct.
 *
 * A string group-by column stored as a DictionarySegment<pmr_string> in every chunk has its value lengths bounded
 * exactly by those dictionaries: no row of that column can be longer than the longest dictionary entry. When that holds
 * for every string group-by column and every maximum fits a 1-byte length field, the key can carry 1-byte length fields
 * and an inline blob sized to the summed maxima instead of the flat STRING_BLOB_BYTES_PER_COLUMN per column. The blob
 * is capped at the default capacity, so one long dictionary outlier cannot widen every key; within the cap the bound
 * covers every row and keys never spill, past it the affected rows spill exactly as on the default sizing. Any column
 * outside that shape puts the whole key back on the default sizing, which handles arbitrary lengths via the spill path.
 */
inline StringKeyBudget choose_string_key_budget(const std::vector<ColumnID>& group_by_column_ids,
                                                const Table& input_table, const size_t dictionary_scan_limit) {
  const auto chunk_count = input_table.chunk_count();
  auto blob_bytes = size_t{0};
  auto string_column_count = size_t{0};
  for (const auto column_id : group_by_column_ids) {
    if (input_table.column_data_type(column_id) != DataType::String) {
      continue;
    }
    ++string_column_count;

    auto scanned_entries = size_t{0};
    auto max_length = size_t{0};
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = input_table.get_chunk(chunk_id);
      if (!chunk) {
        continue;
      }
      const auto segment = chunk->get_segment(column_id);
      const auto* dictionary_segment = dynamic_cast<const DictionarySegment<pmr_string>*>(segment.get());
      if (!dictionary_segment) {
        return {};
      }

      const auto& dictionary = *dictionary_segment->dictionary();
      scanned_entries += dictionary.size();
      if (scanned_entries > dictionary_scan_limit) {
        return {};
      }
      for (const auto& entry : dictionary) {
        max_length = std::max(max_length, entry.size());
      }
    }

    if (max_length > 255) {
      return {};
    }
    blob_bytes += max_length;
  }
  return {1, std::min(blob_bytes, STRING_BLOB_BYTES_PER_COLUMN * string_column_count)};
}

/**
 * The schema family, short-width bucket, and string field sizing resolve_key_schema dispatches on, computed by
 * choose_key_schema.
 */
struct KeySchemaChoice {
  KeyComposition composition{KeyComposition::NumericOnly};
  size_t short_packed_width{0};
  StringKeyBudget string_budget{};
};

/**
 * Inspect the group-by columns and compute which schema type (and, for numeric-only tuples, which short-width
 * bucket) fits them; the type dispatch itself happens in resolve_key_schema.
 */
inline KeySchemaChoice choose_key_schema(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table) {
  auto has_string = false;
  auto has_numeric = false;
  for (const auto column_id : group_by_column_ids) {
    if (input_table.column_data_type(column_id) == DataType::String) {
      has_string = true;
    } else {
      has_numeric = true;
    }
  }

  if (!has_string) {
    const auto layout = compute_key_layout(group_by_column_ids, input_table, 0);
    const auto width = layout.fixed_part_width;
    return {KeyComposition::NumericOnly, width <= 24 ? width : size_t{0}};
  }
  return {has_numeric ? KeyComposition::Mixed : KeyComposition::StringOnly, 0,
          choose_string_key_budget(group_by_column_ids, input_table, DICTIONARY_BOUND_SCAN_LIMIT)};
}

/**
 * Resolve the concrete key-schema type for a query's group-by columns and invoke `functor` with the built schema.
 *
 * Inspects the group-by columns, selects one of NumericShortKeySchema / NumericArbitraryKeySchema / MixedKeySchema /
 * StringOnlyKeySchema, builds it, and calls functor with that concrete instance, mirroring resolve_data_type's
 * compile-time dispatch. The entire scatter+merge pipeline runs inside the functor, monomorphized over the concrete
 * schema type so pack/unpack/hash/equals compile to fixed, branch-free code.
 */
template <typename Functor>
void resolve_key_schema(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table,
                        const Functor& functor) {
  Assert(!group_by_column_ids.empty(), "resolve_key_schema requires at least one group-by column.");
  const auto choice = choose_key_schema(group_by_column_ids, input_table);
  switch (choice.composition) {
    case KeyComposition::NumericOnly:
      switch (choice.short_packed_width) {
        case 4:
          functor(NumericShortKeySchema<4>::build(group_by_column_ids, input_table));
          return;
        case 8:
          functor(NumericShortKeySchema<8>::build(group_by_column_ids, input_table));
          return;
        case 12:
          functor(NumericShortKeySchema<12>::build(group_by_column_ids, input_table));
          return;
        case 16:
          functor(NumericShortKeySchema<16>::build(group_by_column_ids, input_table));
          return;
        case 20:
          functor(NumericShortKeySchema<20>::build(group_by_column_ids, input_table));
          return;
        case 24:
          functor(NumericShortKeySchema<24>::build(group_by_column_ids, input_table));
          return;
        default:
          functor(NumericArbitraryKeySchema::build(group_by_column_ids, input_table));
          return;
      }
    case KeyComposition::Mixed:
      switch (choice.string_budget.length_field_width) {
        case 1:
          functor(MixedKeySchema<1>::build(group_by_column_ids, input_table, choice.string_budget.blob_bytes));
          return;
        default:
          functor(MixedKeySchema<4>::build(group_by_column_ids, input_table, choice.string_budget.blob_bytes));
          return;
      }
    case KeyComposition::StringOnly:
      switch (choice.string_budget.length_field_width) {
        case 1:
          functor(StringOnlyKeySchema<1>::build(group_by_column_ids, input_table, choice.string_budget.blob_bytes));
          return;
        default:
          functor(StringOnlyKeySchema<4>::build(group_by_column_ids, input_table, choice.string_budget.blob_bytes));
          return;
      }
  }
  Fail("Invalid KeyComposition.");
}

}  // namespace hyrise
