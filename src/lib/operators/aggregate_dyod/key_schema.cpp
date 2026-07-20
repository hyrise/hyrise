#include "operators/aggregate_dyod/key_schema.hpp"

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/container/small_vector.hpp>

#include "all_type_variant.hpp"
#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "operators/aggregate_dyod/output_columns.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace {

using namespace hyrise;

void set_null_bit(std::byte* null_bitmap, const uint32_t bit_index) {
  null_bitmap[bit_index / 8] |= std::byte{1} << (bit_index % 8);
}

bool null_bit_set(const std::byte* null_bitmap, const uint32_t bit_index) {
  return (null_bitmap[bit_index / 8] & (std::byte{1} << (bit_index % 8))) != std::byte{0};
}

size_t numeric_lane_width(const DataType data_type) {
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

uint32_t encode_lane_value(const int32_t value) {
  return static_cast<uint32_t>(value) ^ (uint32_t{1} << 31);
}

uint64_t encode_lane_value(const int64_t value) {
  return static_cast<uint64_t>(value) ^ (uint64_t{1} << 63);
}

uint32_t encode_lane_value(const float value) {
  return std::bit_cast<uint32_t>(canonicalize(value));
}

uint64_t encode_lane_value(const double value) {
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
void write_length_field(std::byte* key, const uint32_t field_offset, const size_t length_field_width,
                        const size_t length) {
  if (length_field_width < sizeof(uint64_t)) {
    Assert(length < (uint64_t{1} << (8 * length_field_width)), "String length exceeds the length-prefix field.");
  }
  const auto value = static_cast<uint64_t>(length);
  std::memcpy(key + field_offset, &value, length_field_width);
}

size_t read_length_field(const std::byte* key, const uint32_t field_offset, const size_t length_field_width) {
  auto value = uint64_t{0};
  std::memcpy(&value, key + field_offset, length_field_width);
  return static_cast<size_t>(value);
}

uintptr_t read_spill_pointer(const std::byte* key, const size_t fixed_part_width) {
  auto pointer_value = uintptr_t{0};
  std::memcpy(&pointer_value, key + fixed_part_width, sizeof(pointer_value));
  return pointer_value;
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

KeyLayout compute_key_layout(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table,
                             const size_t length_field_width) {
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
  layout.fixed_part_width =
      layout.blob_offset + (layout.string_count > 0 ? STRING_BLOB_BYTES_PER_COLUMN * layout.string_count : size_t{0});
  return layout;
}

std::unique_ptr<AbstractNumericKeyLane> make_numeric_lane(const DataType data_type, const ColumnID column_id,
                                                          const uint32_t field_offset, const uint32_t null_bit_index) {
  switch (data_type) {
    case DataType::Int:
      return std::make_unique<NumericKeyLane<int32_t>>(column_id, field_offset, null_bit_index);
    case DataType::Long:
      return std::make_unique<NumericKeyLane<int64_t>>(column_id, field_offset, null_bit_index);
    case DataType::Float:
      return std::make_unique<NumericKeyLane<float>>(column_id, field_offset, null_bit_index);
    case DataType::Double:
      return std::make_unique<NumericKeyLane<double>>(column_id, field_offset, null_bit_index);
    default:
      Fail("Not a numeric group-by column.");
  }
}

void decode_string_column(const AbstractSegment& segment, KeyDecodeScratch::StringColumn& column) {
  const auto row_count = static_cast<size_t>(segment.size());
  column.values.resize(row_count);
  column.nulls.assign(row_count, 0);
  auto row = size_t{0};
  segment_iterate<pmr_string>(segment, [&](const auto& position) {
    if (position.is_null()) {
      column.values[row].clear();
      column.nulls[row] = 1;
    } else {
      column.values[row] = position.value();
    }
    ++row;
  });
}

void pack_string_columns(const StringKeyColumns& string_columns, const size_t length_field_width,
                         const size_t blob_offset, const size_t fixed_part_width, const KeyDecodeScratch& scratch,
                         const ChunkOffset chunk_offset, std::byte* key_out, StringSpillBuffer& spill_buffer) {
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

  const auto blob_capacity = STRING_BLOB_BYTES_PER_COLUMN * column_count;
  if (total_length <= blob_capacity) {
    auto* cursor = key_out + blob_offset;
    for (auto index = size_t{0}; index < column_count; ++index) {
      const auto& value = scratch.string_columns[index].values[chunk_offset];
      std::memcpy(cursor, value.data(), value.size());
      cursor += value.size();
    }
    return;
  }

  auto content = std::vector<std::byte>{};
  content.reserve(total_length);
  for (auto index = size_t{0}; index < column_count; ++index) {
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

void unpack_string_columns(const StringKeyColumns& string_columns, const size_t length_field_width,
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

bool equals_string_keys(const StringKeyColumns& string_columns, const size_t length_field_width,
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

void reintern_spilled_key(const StringKeyColumns& string_columns, const size_t length_field_width,
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

}  // namespace

namespace hyrise {

float canonicalize(const float value) {
  if (std::isnan(value)) {
    return std::numeric_limits<float>::quiet_NaN();
  }
  if (value == 0.0f) {
    // Rewrites -0.0 to +0.0.
    return 0.0f;
  }
  return value;
}

double canonicalize(const double value) {
  if (std::isnan(value)) {
    return std::numeric_limits<double>::quiet_NaN();
  }
  if (value == 0.0) {
    return 0.0;
  }
  return value;
}

uint64_t hash_bytes(const std::byte* data, const size_t length) {
  constexpr auto FNV_OFFSET_BASIS = uint64_t{14695981039346656037ull};
  return hash_bytes(data, length, FNV_OFFSET_BASIS);
}

uint64_t hash_bytes(const std::byte* data, const size_t length, const uint64_t seed) {
  constexpr auto FNV_PRIME = uint64_t{1099511628211ull};
  auto hash = seed;
  for (auto index = size_t{0}; index < length; ++index) {
    hash ^= static_cast<uint64_t>(data[index]);
    hash *= FNV_PRIME;
  }
  return hash;
}

const std::byte* StringSpillBuffer::append(const std::byte* content, const size_t length) {
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

void StringSpillBuffer::clear() {
  for (auto& block : _blocks) {
    block.used = 0;
  }
  _current_block = 0;
}

template <typename T>
NumericKeyLane<T>::NumericKeyLane(const ColumnID column_id, const uint32_t field_offset, const uint32_t null_bit_index)
    : _column_id{column_id}, _field_offset{field_offset}, _null_bit_index{null_bit_index} {}

template <typename T>
void NumericKeyLane<T>::decode(const AbstractSegment& segment, KeyDecodeScratch::NumericLane& lane) const {
  using Encoded = decltype(encode_lane_value(T{}));
  const auto row_count = static_cast<size_t>(segment.size());
  lane.values.resize(row_count * sizeof(Encoded));
  lane.nulls.assign(row_count, 0);
  auto* values = reinterpret_cast<Encoded*>(lane.values.data());
  auto row = size_t{0};
  segment_iterate<T>(segment, [&](const auto& position) {
    if (position.is_null()) {
      DebugAssert(_null_bit_index != NO_NULL_BIT, "NULL in a non-nullable group-by column.");
      values[row] = Encoded{};
      lane.nulls[row] = 1;
    } else {
      values[row] = encode_lane_value(position.value());
    }
    ++row;
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

template class NumericKeyLane<int32_t>;
template class NumericKeyLane<int64_t>;
template class NumericKeyLane<float>;
template class NumericKeyLane<double>;

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
    schema._lane_fields.emplace_back(NumericLaneField{
        column.field_offset, static_cast<uint32_t>(numeric_lane_width(column.data_type)), column.null_bit_index});
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
                                                KeyDecodeScratch& scratch) const {
  const auto lane_count = _lanes.size();
  scratch.numeric_lanes.resize(lane_count);
  for (auto index = size_t{0}; index < lane_count; ++index) {
    _lanes[index]->decode(*group_by_segments[index], scratch.numeric_lanes[index]);
  }
}

template <size_t PackedWidth>
void NumericShortKeySchema<PackedWidth>::pack(const KeyDecodeScratch& scratch, const ChunkOffset chunk_offset,
                                              std::byte* key_out, StringSpillBuffer& /*spill_buffer*/) const {
  std::memset(key_out, 0, PackedWidth);
  const auto lane_count = _lane_fields.size();
  for (auto index = size_t{0}; index < lane_count; ++index) {
    const auto& field = _lane_fields[index];
    const auto& lane = scratch.numeric_lanes[index];
    if (lane.nulls[chunk_offset]) {
      set_null_bit(key_out, field.null_bit_index);
      continue;
    }
    std::memcpy(key_out + field.field_offset, lane.values.data() + size_t{chunk_offset} * field.width, field.width);
  }
}

template <size_t PackedWidth>
void NumericShortKeySchema<PackedWidth>::unpack(const std::byte* key, OutputColumns& output,
                                                const size_t output_row) const {
  const auto lane_count = _lanes.size();
  for (auto index = size_t{0}; index < lane_count; ++index) {
    _lanes[index]->unpack(key, key, output, index, output_row);
  }
}

template <size_t PackedWidth>
uint64_t NumericShortKeySchema<PackedWidth>::hash(const std::byte* key) const {
  return hash_bytes(key, PackedWidth);
}

template <size_t PackedWidth>
bool NumericShortKeySchema<PackedWidth>::equals(const std::byte* a, const std::byte* b) const {
  return std::memcmp(a, b, PackedWidth) == 0;
}

template class NumericShortKeySchema<4>;
template class NumericShortKeySchema<8>;
template class NumericShortKeySchema<12>;
template class NumericShortKeySchema<16>;

NumericArbitraryKeySchema NumericArbitraryKeySchema::build(const std::vector<ColumnID>& group_by_column_ids,
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
    schema._lane_fields.emplace_back(NumericLaneField{
        column.field_offset, static_cast<uint32_t>(numeric_lane_width(column.data_type)), column.null_bit_index});
  }
  return schema;
}

size_t NumericArbitraryKeySchema::packed_width() const {
  return _packed_width;
}

size_t NumericArbitraryKeySchema::column_count() const {
  return _lanes.size();
}

void NumericArbitraryKeySchema::decode(const std::span<const AbstractSegment* const> group_by_segments,
                                       KeyDecodeScratch& scratch) const {
  const auto lane_count = _lanes.size();
  scratch.numeric_lanes.resize(lane_count);
  for (auto index = size_t{0}; index < lane_count; ++index) {
    _lanes[index]->decode(*group_by_segments[index], scratch.numeric_lanes[index]);
  }
}

void NumericArbitraryKeySchema::pack(const KeyDecodeScratch& scratch, const ChunkOffset chunk_offset,
                                     std::byte* key_out, StringSpillBuffer& /*spill_buffer*/) const {
  std::memset(key_out, 0, _packed_width);
  const auto lane_count = _lane_fields.size();
  for (auto index = size_t{0}; index < lane_count; ++index) {
    const auto& field = _lane_fields[index];
    const auto& lane = scratch.numeric_lanes[index];
    if (lane.nulls[chunk_offset]) {
      set_null_bit(key_out, field.null_bit_index);
      continue;
    }
    std::memcpy(key_out + field.field_offset, lane.values.data() + size_t{chunk_offset} * field.width, field.width);
  }
}

void NumericArbitraryKeySchema::unpack(const std::byte* key, OutputColumns& output, const size_t output_row) const {
  const auto lane_count = _lanes.size();
  for (auto index = size_t{0}; index < lane_count; ++index) {
    _lanes[index]->unpack(key, key, output, index, output_row);
  }
}

uint64_t NumericArbitraryKeySchema::hash(const std::byte* key) const {
  return hash_bytes(key, _packed_width);
}

bool NumericArbitraryKeySchema::equals(const std::byte* a, const std::byte* b) const {
  return std::memcmp(a, b, _packed_width) == 0;
}

template <size_t LenWidth>
MixedKeySchema<LenWidth> MixedKeySchema<LenWidth>::build(const std::vector<ColumnID>& group_by_column_ids,
                                                         const Table& input_table) {
  const auto layout = compute_key_layout(group_by_column_ids, input_table, LenWidth);
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
      schema._lane_fields.emplace_back(NumericLaneField{
          column.field_offset, static_cast<uint32_t>(numeric_lane_width(column.data_type)), column.null_bit_index});
      schema._numeric_tuple_indices.emplace_back(static_cast<uint32_t>(index));
    }
  }
  return schema;
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
void MixedKeySchema<LenWidth>::decode(const std::span<const AbstractSegment* const> group_by_segments,
                                      KeyDecodeScratch& scratch) const {
  const auto lane_count = _numeric_lanes.size();
  scratch.numeric_lanes.resize(lane_count);
  for (auto index = size_t{0}; index < lane_count; ++index) {
    _numeric_lanes[index]->decode(*group_by_segments[_numeric_tuple_indices[index]], scratch.numeric_lanes[index]);
  }
  const auto string_count = _string_columns.size();
  scratch.string_columns.resize(string_count);
  for (auto index = size_t{0}; index < string_count; ++index) {
    decode_string_column(*group_by_segments[_string_columns[index].tuple_index], scratch.string_columns[index]);
  }
}

template <size_t LenWidth>
void MixedKeySchema<LenWidth>::pack(const KeyDecodeScratch& scratch, const ChunkOffset chunk_offset, std::byte* key_out,
                                    StringSpillBuffer& spill_buffer) const {
  std::memset(key_out, 0, packed_width());
  const auto lane_count = _lane_fields.size();
  for (auto index = size_t{0}; index < lane_count; ++index) {
    const auto& field = _lane_fields[index];
    const auto& lane = scratch.numeric_lanes[index];
    if (lane.nulls[chunk_offset]) {
      set_null_bit(key_out, field.null_bit_index);
      continue;
    }
    std::memcpy(key_out + field.field_offset, lane.values.data() + size_t{chunk_offset} * field.width, field.width);
  }
  pack_string_columns(_string_columns, LenWidth, _blob_offset, _fixed_part_width, scratch, chunk_offset, key_out,
                      spill_buffer);
}

template <size_t LenWidth>
void MixedKeySchema<LenWidth>::unpack(const std::byte* key, OutputColumns& output, const size_t output_row) const {
  const auto lane_count = _numeric_lanes.size();
  for (auto index = size_t{0}; index < lane_count; ++index) {
    _numeric_lanes[index]->unpack(key, key, output, _numeric_tuple_indices[index], output_row);
  }
  unpack_string_columns(_string_columns, LenWidth, _blob_offset, _fixed_part_width, key, output);
}

template <size_t LenWidth>
uint64_t MixedKeySchema<LenWidth>::hash(const std::byte* key) const {
  return hash_bytes(key, _fixed_part_width);
}

template <size_t LenWidth>
bool MixedKeySchema<LenWidth>::equals(const std::byte* a, const std::byte* b) const {
  return equals_string_keys(_string_columns, LenWidth, _fixed_part_width, a, b);
}

template <size_t LenWidth>
void MixedKeySchema<LenWidth>::reintern_spill(std::byte* key, StringSpillBuffer& spill_buffer) const {
  reintern_spilled_key(_string_columns, LenWidth, _fixed_part_width, key, spill_buffer);
}

template class MixedKeySchema<4>;

template <size_t LenWidth>
StringOnlyKeySchema<LenWidth> StringOnlyKeySchema<LenWidth>::build(const std::vector<ColumnID>& group_by_column_ids,
                                                                   const Table& input_table) {
  const auto layout = compute_key_layout(group_by_column_ids, input_table, LenWidth);
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
void StringOnlyKeySchema<LenWidth>::decode(const std::span<const AbstractSegment* const> group_by_segments,
                                           KeyDecodeScratch& scratch) const {
  const auto string_count = _string_columns.size();
  scratch.string_columns.resize(string_count);
  for (auto index = size_t{0}; index < string_count; ++index) {
    decode_string_column(*group_by_segments[_string_columns[index].tuple_index], scratch.string_columns[index]);
  }
}

template <size_t LenWidth>
void StringOnlyKeySchema<LenWidth>::pack(const KeyDecodeScratch& scratch, const ChunkOffset chunk_offset,
                                         std::byte* key_out, StringSpillBuffer& spill_buffer) const {
  std::memset(key_out, 0, packed_width());
  pack_string_columns(_string_columns, LenWidth, _blob_offset, _fixed_part_width, scratch, chunk_offset, key_out,
                      spill_buffer);
}

template <size_t LenWidth>
void StringOnlyKeySchema<LenWidth>::unpack(const std::byte* key, OutputColumns& output,
                                           const size_t /*output_row*/) const {
  unpack_string_columns(_string_columns, LenWidth, _blob_offset, _fixed_part_width, key, output);
}

template <size_t LenWidth>
uint64_t StringOnlyKeySchema<LenWidth>::hash(const std::byte* key) const {
  return hash_bytes(key, _fixed_part_width);
}

template <size_t LenWidth>
bool StringOnlyKeySchema<LenWidth>::equals(const std::byte* a, const std::byte* b) const {
  return equals_string_keys(_string_columns, LenWidth, _fixed_part_width, a, b);
}

template <size_t LenWidth>
void StringOnlyKeySchema<LenWidth>::reintern_spill(std::byte* key, StringSpillBuffer& spill_buffer) const {
  reintern_spilled_key(_string_columns, LenWidth, _fixed_part_width, key, spill_buffer);
}

template class StringOnlyKeySchema<4>;

KeySchemaChoice choose_key_schema(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table) {
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
    return {KeyComposition::NumericOnly, width <= 16 ? width : size_t{0}};
  }
  return {has_numeric ? KeyComposition::Mixed : KeyComposition::StringOnly, 0};
}

}  // namespace hyrise
