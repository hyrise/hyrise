#include "key_primitives.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

void StringSpillBuffer::clear() {
  for (auto& block : _blocks) {
    block.used = 0;
  }
  _current_block = 0;
}

void StringSpillBuffer::release() {
  _blocks = std::vector<Block>{};
  _current_block = 0;
}

size_t StringSpillBuffer::memory_usage() const {
  auto bytes = sizeof(*this);
  bytes += _blocks.capacity() * sizeof(Block);
  for (const auto& block : _blocks) {
    bytes += block.capacity;
  }
  return bytes;
}

KeyLayout compute_key_layout(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table,
                             const size_t length_field_width, const std::optional<size_t> string_blob_bytes) {
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

NumericKeyLaneEntry make_numeric_lane(const DataType data_type, const ColumnID column_id, const uint32_t field_offset,
                                      const uint32_t null_bit_index) {
  auto lane = std::unique_ptr<BaseNumericKeyLane>{};
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

}  // namespace hyrise
