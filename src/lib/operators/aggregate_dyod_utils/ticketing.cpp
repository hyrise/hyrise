#include "ticketing.hpp"

#include <algorithm>
#include <cstring>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "storage/segment_iterate.hpp"

namespace hyrise {

RowFormat _create_row_format(const TableColumnDefinitions& column_definitions,
                             const std::vector<ColumnID>& groupby_column_ids) {
  const auto group_by_column_count = groupby_column_ids.size();
  auto col_offsets = std::vector<uint64_t>(group_by_column_count);
  auto column_is_nullable = std::vector<uint8_t>(group_by_column_count);
  auto string_column_count = uint64_t{0};
  auto stores_nulls = false;

  auto curr_offset = uint64_t{0};
  for (auto group_index = size_t{0}; group_index < group_by_column_count; ++group_index) {
    const auto column_id = groupby_column_ids[group_index];
    const auto& column_definition = column_definitions[column_id];

    column_is_nullable[group_index] = column_definition.nullable ? 1 : 0;
    stores_nulls |= column_definition.nullable;

    col_offsets[group_index] = curr_offset;
    if (column_definition.data_type == DataType::String) {
      curr_offset += sizeof(char) * PREFIX_LENGTH + sizeof(size_t);  // prefix + length
      string_column_count++;
    } else {
      resolve_data_type(column_definition.data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;
        curr_offset += sizeof(ColumnDataType);
      });
    }
  }

  // The null bitmap is only present when at least one group-by column is nullable; otherwise the rows are 8 bytes
  // shorter and `null_bitmap_offset` collapses onto `data_offset` so `key_bytes()` starts directly at the data.
  const auto null_bitmap_size = stores_nulls ? sizeof(uint64_t) : uint64_t{0};
  const auto data_offset = sizeof(uint64_t) + null_bitmap_size;  // hash (+ null bitmap)
  const auto null_bitmap_offset = stores_nulls ? sizeof(uint64_t) : data_offset;
  const auto string_ptr_offset = data_offset + curr_offset;
  const auto row_size = string_ptr_offset + string_column_count * sizeof(char*);

  return RowFormat{.row_size = row_size,
                   .hash_offset = 0,
                   .null_bitmap_offset = null_bitmap_offset,
                   .data_offset = data_offset,
                   .string_ptr_offset = string_ptr_offset,
                   .stores_nulls = stores_nulls,
                   .col_offsets = std::move(col_offsets),
                   .column_is_nullable = std::move(column_is_nullable)};
}

// TODO(@forUnity): think about alignment and padding, also sort string_columns to be last in groupby columns?
std::shared_ptr<MaterializedRows> _materialize_rows(const RowFormat& format, const std::shared_ptr<const Chunk>& chunk,
                                                    const std::vector<ColumnID>& groupby_column_ids) {
  const auto chunk_size = chunk->size();

  auto materialized = std::make_shared<MaterializedRows>();
  materialized->row_count = chunk_size;
  materialized->rows = std::make_unique<uint8_t[]>(chunk_size * format.row_size);
  auto* const rows = materialized->rows.get();
  auto& string_arena = materialized->string_arena;

  // Index of the current string column among the group-by columns. Selects which string-pointer slot to write.
  auto string_col_index = size_t{0};
  for (auto group_by_column_index = size_t{0}; group_by_column_index < groupby_column_ids.size();
       ++group_by_column_index) {
    const auto column_id = groupby_column_ids[group_by_column_index];
    const auto& segment = chunk->get_segment(column_id);
    const auto null_mask_bit = uint64_t{1} << group_by_column_index;

    resolve_data_type(segment->data_type(), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      auto chunk_offset = size_t{0};
      segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
        const auto row = RowView{rows + chunk_offset * format.row_size, format};
        ++chunk_offset;

        if (position.is_null()) {
          row.set_null_bitmap(row.null_bitmap() | null_mask_bit);
          return;
        }

        if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
          const auto& str_value = position.value();
          const auto str_length = str_value.size();

          // Inline representation: length + prefix.
          auto* const inline_data = row.column_data(group_by_column_index);
          std::memcpy(inline_data, &str_length, sizeof(size_t));
          const auto prefix_length = std::min(str_length, static_cast<size_t>(PREFIX_LENGTH));
          std::memcpy(inline_data + sizeof(size_t), str_value.c_str(), prefix_length);

          // Strings longer than the prefix additionally store the full, null-terminated value in the chunk's arena
          // so equality can resolve prefix collisions without re-reading the source segment.
          if (str_length > PREFIX_LENGTH) {
            auto* const str_copy = static_cast<char*>(string_arena.allocate(str_length + 1));
            std::memcpy(str_copy, str_value.c_str(), str_length);
            str_copy[str_length] = '\0';
            row.set_string_ptr(string_col_index, str_copy);
          }
        } else {
          row.write_value(group_by_column_index, position.value());
        }
      });

      if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
        ++string_col_index;
      }
    });
  }

  // Compute and store the hash of each row's key bytes once; it is reused on every hash-table probe.
  for (auto row_index = size_t{0}; row_index < chunk_size; ++row_index) {
    const auto row = RowView{rows + row_index * format.row_size, format};
    row.set_hash();
  }

  return materialized;
}

std::shared_ptr<GroupKeyData> _compute_group_keys(const std::vector<ColumnID>& groupby_column_ids,
                                                  const std::shared_ptr<const Table>& input_table) {
  const auto row_format = _create_row_format(input_table->column_definitions(), groupby_column_ids);
  const auto group_key_data = std::make_shared<GroupKeyData>(row_format);
  const auto& format = group_key_data->row_format;
  auto& arena = group_key_data->key_arena;
  const auto chunk_count = input_table->chunk_count();

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    const auto materialized = _materialize_rows(format, chunk, groupby_column_ids);

    auto* row_ptr = materialized->rows.get();
    for (auto row_index = size_t{0}; row_index < materialized->row_count; ++row_index) {
      const auto row_hash = RowView{row_ptr, format}.hash();
      const auto probe_key = GroupKey{.row = row_ptr, .hash = row_hash};

      auto iter = group_key_data->global_hash_table.find(probe_key);
      if (iter == group_key_data->global_hash_table.end()) {
        // First time we see this group: copy the key row into the arena so it outlives the per-chunk materialized
        // buffer, and copy any long strings alongside it, repointing the copied row at the arena copies.
        auto* const row_copy = static_cast<uint8_t*>(arena.allocate(format.row_size, alignof(uint64_t)));
        std::memcpy(row_copy, row_ptr, format.row_size);

        const auto copy_view = RowView{row_copy, format};
        const auto string_col_count = copy_view.string_col_count();
        for (auto string_col_index = size_t{0}; string_col_index < string_col_count; ++string_col_index) {
          auto* const str_ptr = copy_view.string_ptr(string_col_index);
          if (str_ptr != nullptr) {
            const auto length = std::strlen(str_ptr) + 1;
            auto* const arena_str = static_cast<char*>(arena.allocate(length));
            std::memcpy(arena_str, str_ptr, length);
            copy_view.set_string_ptr(string_col_index, arena_str);
          }
        }

        const auto group_key = GroupKey{.row = row_copy, .hash = row_hash};
        group_key_data->row_counts.push_back(0);
        iter = group_key_data->global_hash_table
                   .emplace(group_key, static_cast<uint64_t>(group_key_data->global_hash_table.size()))
                   .first;
      }
      ++group_key_data->row_counts[iter->second];
      group_key_data->tickets.push_back(iter->second);

      row_ptr += format.row_size;
    }
  }
  return group_key_data;
}

}  // namespace hyrise
