#include "ticketing.hpp"

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace hyrise {

constexpr uint64_t PREFIX_LENGTH = 8;

RowFormat _create_row_format(const TableColumnDefinitions& column_definitions,
                             const std::vector<ColumnID>& groupby_column_ids) {
  const auto group_by_column_count = groupby_column_ids.size();
  auto col_offsets = std::vector<uint64_t>(group_by_column_count);
  auto string_column_count = uint64_t{0};

  auto curr_offset = uint64_t{0};
  for (auto group_index = size_t{0}; group_index < group_by_column_count; ++group_index) {
    const auto column_id = groupby_column_ids[group_index];
    const auto& column_definition = column_definitions[column_id];

    if (column_definition.data_type == DataType::String) {
      col_offsets[group_index] = curr_offset;
      curr_offset += sizeof(char) * PREFIX_LENGTH + sizeof(size_t);  // prefix + length
      string_column_count++;
    } else {
      resolve_data_type(column_definition.data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;
        const auto column_size = sizeof(ColumnDataType);
        col_offsets[group_index] = curr_offset;
        curr_offset += column_size;
      });
    }
  }
  const auto row_size = sizeof(uint64_t) + sizeof(uint64_t) + curr_offset +
                        string_column_count * sizeof(const char*);  // hash + null_bitmap + data + string_pointers

  return RowFormat{.row_size = row_size,
                   .hash_offset = 0,
                   .null_bitmap_offset = sizeof(uint64_t),
                   .data_offset = sizeof(uint64_t) + sizeof(uint64_t),
                   .string_ptr_offset = sizeof(uint64_t) + sizeof(uint64_t) + curr_offset,
                   .col_offsets = std::move(col_offsets)};
}

// TODO(@forUnity): think about alignment and padding, also sort string_columns to be last in groupby columns?
// NOTE: We copy the format here because we do not want to dereference a ptr or reference all the time.
std::shared_ptr<MaterializedRows> _materialize_rows(const RowFormat format, const std::shared_ptr<const Chunk>& chunk,
                                                    const std::vector<ColumnID>& groupby_column_ids) {
  const auto chunk_size = chunk->size();
  auto rows = std::make_unique<uint8_t[]>(chunk_size * format.row_size);
  auto string_col_index = uint64_t{0};

  for (auto group_by_column_index = size_t{0}; group_by_column_index < groupby_column_ids.size();
       ++group_by_column_index) {
    const auto column_id = groupby_column_ids[group_by_column_index];
    const auto& segment = chunk->get_segment(column_id);

    const auto null_mask_bit = static_cast<uint64_t>(1) << group_by_column_index;

    // Pointers to the respective column and null bitmap positions of the current row.
    auto* row_col_ptr = rows.get() + format.data_offset + format.col_offsets[group_by_column_index];

    // TODO(@forUnity): think about how to remove this for non-nullable columns.
    auto* null_bitmap_ptr = rows.get() + format.null_bitmap_offset;

    // TODO(@forUnity): think whether we can remove this for non-string columns / how.
    // Pointer to the string pointer slot of this specific column within the current row
    auto str_ptr_ptr = rows.get() + format.string_ptr_offset + string_col_index * sizeof(const char*);

    resolve_data_type(segment->data_type(), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
        if (position.is_null()) {
          // Set the corresponding bit in the null bitmap
          auto new_mask_value = *reinterpret_cast<uint64_t*>(null_bitmap_ptr) | null_mask_bit;
          std::memcpy(null_bitmap_ptr, &new_mask_value, sizeof(uint64_t));
        } else {
          // Write the value to the appropriate offset in the row
          if constexpr (std::is_same_v<ColumnDataType, std::string>) {
            const auto str_value = position.value();
            const auto str_length = str_value.size();

            // Build short string representation: length() + prefix
            std::memcpy(row_col_ptr, &str_length, sizeof(size_t));
            const auto prefix_length = std::min(str_length, static_cast<size_t>(PREFIX_LENGTH));
            std::memcpy(row_col_ptr + sizeof(size_t), str_value.c_str(), prefix_length);

            // If the string is longer than 8 bytes, we store a pointer to the string at the end of the row as well.
            if (str_length > PREFIX_LENGTH) {
              // TODO(@forUnity): Do not write the first 8 bytes here again.
              // TODO(@forUnity): maybe try malloc here??
              // TODO(@forUnity): we null terminate here.
              // This way we do not have to resolve the length of the strings everytime.
              const auto str_ptr = new char[str_length + 1];
              std::memcpy(str_ptr, str_value.c_str(), str_length);
              str_ptr[str_length] = '\0';  // Null terminate the string
              std::memcpy(str_ptr_ptr, &str_ptr, sizeof(const char*));
            }
            str_ptr_ptr += format.row_size;  // Move to the string pointer slot of the next row for this column

          } else {
            std::memcpy(row_col_ptr, &position.value(), sizeof(ColumnDataType));
          }

          // Move the pointers to the slots of the next row for this column.
          row_col_ptr += format.row_size;
          null_bitmap_ptr += format.row_size;
        }
      });
      // If this was a string column, we should move the string column pointer for the following columns.
      if (std::is_same_v<ColumnDataType, std::string>) {
        string_col_index++;
      }
    });
  }

  // Compute the hash for each row and write it to the beginning.
  // TODO(@forUnity): use a real hash function here (instead of this hacky solution)
  auto* row_ptr = rows.get();
  const auto hash_function = std::hash<std::string_view>{};
  auto* null_bitmap_ptr = reinterpret_cast<const char*>(rows.get() + format.null_bitmap_offset);
  for (auto row_index = size_t{0}; row_index < chunk_size; ++row_index) {
    const auto hash =
        hash_function(std::string_view{null_bitmap_ptr, format.string_ptr_offset - format.null_bitmap_offset});
    std::memcpy(row_ptr, &hash, sizeof(uint64_t));
    null_bitmap_ptr += format.row_size;  // Move to the null bitmap of the next row
  }

  return std::make_shared<MaterializedRows>(chunk_size, rows.release(), format);
}

// CAVEAT: This destroys all non-short strings.
// Therefore we need to set to nullptr when inserting into the Global Hash Table.
MaterializedRows::~MaterializedRows() {
  const auto group_by_column_count = format.col_offsets.size();

  // Delete the string pointers at the end of each row if they exist.
  auto string_col_count = (format.row_size - format.string_ptr_offset) / sizeof(const char*);
  if (string_col_count > 0) {
    auto* str_ptr_ptr = rows + format.string_ptr_offset;
    for (auto row_index = size_t{0}; row_index < row_count; ++row_index) {
      for (auto string_col_index = size_t{0}; string_col_index < string_col_count; ++string_col_index) {
        const auto* str_ptr = *reinterpret_cast<const char* const*>(str_ptr_ptr);
        if (str_ptr != nullptr) {
          delete[] str_ptr;
        }
        str_ptr_ptr += sizeof(const char*);
      }
      str_ptr_ptr += format.row_size;
    }
  }

  // Then simply delete the rows array itself.
  delete[] rows;
}

std::shared_ptr<GroupKeyData> _compute_group_keys_materialized(const std::vector<ColumnID>& groupby_column_ids,
                                                               const std::shared_ptr<const Table>& input_table) {
  const auto group_key_data = std::make_shared<GroupKeyData>();
  const auto group_by_column_count = groupby_column_ids.size();
  const auto chunk_count = input_table->chunk_count();

  // Single shared row format for all chunks and threads later on.
  const auto row_format = _create_row_format(input_table->column_definitions(), groupby_column_ids);

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);

    const auto materialized_rows = _materialize_rows(row_format, chunk, groupby_column_ids);

    auto row_ptr = materialized_rows->rows;
    auto buffer_group_key = GroupKey{nullptr, materialized_rows->format};
    for (auto row_index = size_t{0}; row_index < materialized_rows->row_count; ++row_index) {
      buffer_group_key.row = row_ptr;

      auto iter = group_key_data->global_hash_table.find(buffer_group_key);
      if (iter == group_key_data->global_hash_table.end()) {
        // Copy row into new memory so that it is not destroyed along the entire materialized rows buffer.
        const auto row_copy = new uint8_t[materialized_rows->format.row_size];
        std::memcpy(row_copy, row_ptr, materialized_rows->format.row_size);

        // Set string pointers to nullptr so we do not delete them when the row is destroyed.
        const auto str_ptr_slot = reinterpret_cast<char*>(row_ptr + materialized_rows->format.string_ptr_offset);
        std::memset(str_ptr_slot, 0,
                    (materialized_rows->format.row_size - materialized_rows->format.string_ptr_offset));

        const auto group_key = GroupKey{row_copy, materialized_rows->format};
        group_key_data->row_counts.push_back(0);
        iter = group_key_data->global_hash_table.emplace(group_key, group_key_data->global_hash_table.size()).first;
      }
      ++group_key_data->row_counts[iter->second];
      group_key_data->tickets.push_back(iter->second);

      row_ptr += materialized_rows->format.row_size;
    }
  }
  return group_key_data;
}

}  // namespace hyrise
