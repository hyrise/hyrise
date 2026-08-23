#pragma once

#include <algorithm>
#include <array>
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

// Group-by key representation for AggregateDYOD: the shared building blocks every key schema is made of.
//
// A "key" is the group-by column tuple of one row, encoded into a flat, comparable byte buffer. This header holds the
// pieces common to every schema: the byte layout and its helpers (KeyLayout/compute_key_layout), the per-column lanes,
// packing/unpacking, hashing, and the string spill buffer. The concrete per-query schema types that assemble these
// pieces live in the numeric_short/numeric_arbitrary/mixed/string_only_key_schema headers, and resolve_key_schema()
// (key_schema.hpp) picks one.
//
// Layout (all four schema families):
//   [ null bitmap | numeric prefix | inline string blob | 8-byte spill pointer ]
// The null bitmap carries one bit per nullable group-by column (present only if any group-by column is nullable) and is
// padded so the fixed part stays a multiple of 4. The inline string blob is padded at its tail for the same reason,
// which is what holds the fixed part on a multiple of 4 once the blob capacity is sized from measured string lengths
// rather than from STRING_BLOB_BYTES_PER_COLUMN (see choose_string_key_budget). The string blob and spill pointer are
// absent for numeric-only schemas.
//
// NULL is carried out of band in the null bitmap, never as an in-band sentinel: a "+1 and reserve 0" scheme collides
// at TYPE_MAX, and no byte pattern is safe for a full-range column.

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
    std::vector<std::byte> data;
    size_t capacity{0};
    size_t used{0};
  };

  static constexpr size_t MIN_BLOCK_BYTES = size_t{16} * 1024;

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
    block.data = std::vector<std::byte>(block.capacity);
    _blocks.emplace_back(std::move(block));
  }

  auto& block = _blocks[_current_block];
  auto* destination = block.data.data() + block.used;
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
  uint32_t tuple_index{0};
  uint32_t length_field_offset{0};
  uint32_t null_bit_index{NO_NULL_BIT};
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

// Helpers shared by the key schemas.

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
  return static_cast<uint32_t>(value) ^ uint32_t { 0x80000000 };
}

inline uint64_t encode_lane_value(const int64_t value) {
  return static_cast<uint64_t>(value) ^ uint64_t { 0x8000000000000000 };
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
    return static_cast<int32_t>(encoded ^ uint32_t{0x80000000});
  } else if constexpr (std::is_same_v<T, int64_t>) {
    auto encoded = uint64_t{};
    std::memcpy(&encoded, field, sizeof(encoded));
    return static_cast<int64_t>(encoded ^ uint64_t{0x8000000000000000});
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

using SpillPointerBytes = std::array<std::byte, sizeof(const std::byte*)>;

inline const std::byte* read_spill_pointer(const std::byte* key, const size_t fixed_part_width) {
  auto bytes = SpillPointerBytes{};
  std::memcpy(bytes.data(), key + fixed_part_width, bytes.size());
  return std::bit_cast<const std::byte*>(bytes);
}

inline void write_spill_pointer(std::byte* key, const size_t fixed_part_width, const std::byte* spill) {
  const auto bytes = std::bit_cast<SpillPointerBytes>(spill);
  std::memcpy(key + fixed_part_width, bytes.data(), bytes.size());
}

template <size_t max_lane_width = 8>
void pack_numeric_lanes(const NumericKeyLanes& lanes, const KeyDecodeScratch& scratch, const ChunkOffset chunk_offset,
                        std::byte* key_out) {
  const auto lane_count = lanes.size();
  for (auto index = size_t{0}; index < lane_count; ++index) {
    const auto& field = lanes[index].field;
    const auto& lane = scratch.numeric_lanes[index];
    if (lane.nulls[chunk_offset] != 0) {
      set_null_bit(key_out, field.null_bit_index);
      continue;
    }
    const auto* source = lane.values.data() + (size_t{chunk_offset} * field.width);
    if constexpr (max_lane_width >= 8) {
      if (field.width == 8) {
        std::memcpy(key_out + field.field_offset, source, 8);
        continue;
      }
    }
    std::memcpy(key_out + field.field_offset, source, 4);
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
    if (decoded.nulls[chunk_offset] != 0) {
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
      if (scratch.string_columns[index].nulls[chunk_offset] != 0) {
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
    if (scratch.string_columns[index].nulls[chunk_offset] != 0) {
      continue;
    }
    const auto& value = scratch.string_columns[index].values[chunk_offset];
    const auto* bytes = reinterpret_cast<const std::byte*>(value.data());
    content.insert(content.end(), bytes, bytes + value.size());
  }
  const auto* interned = spill_buffer.append(content.data(), content.size());
  const auto content_hash = hash_bytes(content.data(), content.size());
  std::memcpy(key_out + blob_offset, &content_hash, sizeof(content_hash));
  write_spill_pointer(key_out, fixed_part_width, interned);
}

inline bool equals_string_keys(const StringKeyColumns& string_columns, const size_t length_field_width,
                               const size_t fixed_part_width, const std::byte* lhs, const std::byte* rhs) {
  const auto* lhs_spill = read_spill_pointer(lhs, fixed_part_width);
  const auto* rhs_spill = read_spill_pointer(rhs, fixed_part_width);
  if ((lhs_spill != nullptr) != (rhs_spill != nullptr)) {
    return false;
  }
  if (std::memcmp(lhs, rhs, fixed_part_width) != 0) {
    return false;
  }
  if (lhs_spill == nullptr) {
    return true;
  }

  auto total_length = size_t{0};
  for (const auto& column : string_columns) {
    total_length += read_length_field(lhs, column.length_field_offset, length_field_width);
  }
  return std::memcmp(lhs_spill, rhs_spill, total_length) == 0;
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
  const auto bitmap_region = nullable_count > 0 ? ((size_t{nullable_count} + 7) / 8 + 3) / 4 * 4 : size_t{0};

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
  return {.lane = std::move(lane),
          .field = NumericLaneField{.field_offset = field_offset,
                                    .width = static_cast<uint32_t>(numeric_lane_width(data_type)),
                                    .null_bit_index = null_bit_index}};
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

  const auto viewed = iterate_stable_segment_window<pmr_string>(segment, row_begin, row_end,
                                                                [&](const size_t row, const pmr_string* value) {
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
  const auto* spill = read_spill_pointer(key, fixed_part_width);
  const auto* cursor = spill != nullptr ? spill : key + blob_offset;

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
  const auto* spill = read_spill_pointer(key, fixed_part_width);
  if (spill == nullptr) {
    return;
  }

  auto total_length = size_t{0};
  for (const auto& column : string_columns) {
    total_length += read_length_field(key, column.length_field_offset, length_field_width);
  }
  const auto* interned = spill_buffer.append(spill, total_length);
  write_spill_pointer(key, fixed_part_width, interned);
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
    std::memcpy(values + (row * sizeof(Encoded)), &encoded, sizeof(encoded));
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

}  // namespace hyrise
