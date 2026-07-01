#include "ticketing.hpp"

#include <algorithm>
#include <cstring>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/unordered/unordered_flat_map.hpp>

#include "resolve_type.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"

namespace hyrise {

// Build the group-by output columns from  the distinct key rows of the byte-row path. Each output value is read back
// out of its group's row.
pmr_vector<std::shared_ptr<AbstractSegment>> _build_groupby_segments(const GroupKeyData& groups,
                                                                     const std::vector<ColumnID>& groupby_column_ids,
                                                                     const std::shared_ptr<const Table>& input_table) {
  const auto& row_format = groups.row_format;
  const auto group_count = groups.global_hash_table.size();
  const auto group_by_column_count = groupby_column_ids.size();

  auto output_segments = pmr_vector<std::shared_ptr<AbstractSegment>>{};
  output_segments.reserve(group_by_column_count);

  // Index of the current string column among the group-by columns. It selects which string-pointer slot to read.
  auto string_col_index = size_t{0};
  for (auto group_by_column_index = size_t{0}; group_by_column_index < group_by_column_count; ++group_by_column_index) {
    const auto column_id = groupby_column_ids[group_by_column_index];
    const auto data_type = input_table->column_data_type(column_id);
    const auto column_is_nullable = input_table->column_is_nullable(column_id);
    resolve_data_type(data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      auto values = pmr_vector<ColumnDataType>(group_count);
      // Only nullable columns carry a null bitmap, so only they need a nulls vector.
      auto nulls = column_is_nullable ? pmr_vector<bool>(group_count, false) : pmr_vector<bool>{};

      const auto null_mask_bit = uint64_t{1} << group_by_column_index;
      for (const auto& [group_key, ticket] : groups.global_hash_table) {
        const auto row = RowView{group_key.row, row_format};

        // Only nullable columns carry a null bitmap.
        if (column_is_nullable) {
          nulls[ticket] = (row.null_bitmap() & null_mask_bit) != 0;
          if (nulls[ticket]) {
            continue;
          }
        }

        if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
          // The string column is materialized as [length, prefix] inline. Strings up to PREFIX_LENGTH bytes are stored
          // entirely in the prefix. Longer strings additionally store a heap pointer to the full value.
          const auto str_length = row.string_length(group_by_column_index);
          if (str_length <= PREFIX_LENGTH) {
            values[ticket] = pmr_string{row.string_prefix(group_by_column_index), str_length};
          } else {
            // The string is copied into the segment, so the row owning the pointer can be freed afterwards.
            values[ticket] = pmr_string{row.string_ptr(string_col_index)};
          }
        } else {
          values[ticket] = row.read_value<ColumnDataType>(group_by_column_index);
        }
      }

      if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
        ++string_col_index;
      }

      // Match the output column's nullability.
      if (column_is_nullable) {
        output_segments.push_back(std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(nulls)));
      } else {
        output_segments.push_back(std::make_shared<ValueSegment<ColumnDataType>>(std::move(values)));
      }
    });
  }

  return output_segments;
}

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
  const auto data_offset = null_bitmap_size;  // null bitmap (if present)
  const auto null_bitmap_offset = uint64_t{0};
  const auto string_ptr_offset = data_offset + curr_offset;
  const auto row_size = string_ptr_offset + string_column_count * sizeof(char*);

  return RowFormat{.row_size = row_size,
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

  return materialized;
}

// Fast path for a single non-string group-by column: the value itself is the key, so a typed hash map replaces the
// row materialization. NULL is its own group (via `null_ticket`).
GroupingResult _compute_groups_single_column(const ColumnID groupby_column_id,
                                             const std::shared_ptr<const Table>& input_table) {
  auto result = GroupingResult{};
  result.tickets.reserve(input_table->row_count());

  const auto data_type = input_table->column_data_type(groupby_column_id);
  const auto column_is_nullable = input_table->column_is_nullable(groupby_column_id);
  const auto chunk_count = input_table->chunk_count();

  resolve_data_type(data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
      Fail("The single-column fast path is not used for string columns.");
    } else {
      auto value_to_ticket = boost::unordered_flat_map<ColumnDataType, uint32_t>{};
      // The row count is the upper bound on the number of groups; reserving avoids repeated rehashing on
      // high-cardinality group-bys (it over-allocates for low-cardinality inputs).
      value_to_ticket.reserve(input_table->row_count());
      auto null_ticket = uint32_t{0};
      auto has_null = false;

      // Representative value per group, used to build the output column. The NULL group's slot is never read.
      auto group_values = pmr_vector<ColumnDataType>{};
      auto group_nulls = pmr_vector<bool>{};

      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto& segment = input_table->get_chunk(chunk_id)->get_segment(groupby_column_id);
        segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
          auto ticket = uint32_t{0};
          if (position.is_null()) {
            if (!has_null) {
              has_null = true;
              null_ticket = static_cast<uint32_t>(group_values.size());
              group_values.emplace_back();
              group_nulls.push_back(true);
            }
            ticket = null_ticket;
          } else {
            const auto [iter, inserted] =
                value_to_ticket.try_emplace(position.value(), static_cast<uint32_t>(group_values.size()));
            if (inserted) {
              group_values.push_back(position.value());
              group_nulls.push_back(false);
            }
            ticket = iter->second;
          }
          result.tickets.push_back(ticket);
        });
      }

      result.group_count = group_values.size();
      if (column_is_nullable) {
        result.groupby_segments.push_back(
            std::make_shared<ValueSegment<ColumnDataType>>(std::move(group_values), std::move(group_nulls)));
      } else {
        result.groupby_segments.push_back(std::make_shared<ValueSegment<ColumnDataType>>(std::move(group_values)));
      }
    }
  });

  return result;
}

// Standard path: materialize each row's group-by key into a packed row format, hash it and probe a global hash table.
GroupingResult _compute_groups_byte_row(const std::vector<ColumnID>& groupby_column_ids,
                                        const std::shared_ptr<const Table>& input_table) {
  const auto row_format = _create_row_format(input_table->column_definitions(), groupby_column_ids);
  auto group_key_data = std::make_shared<GroupKeyData>(row_format);
  group_key_data->tickets.reserve(input_table->row_count());
  // The row count is the upper bound on the number of groups. Reserving it up front avoids repeated rehashing on
  // high-cardinality group-bys; for low-cardinality inputs it over-allocates the table.
  group_key_data->global_hash_table.reserve(input_table->row_count());
  const auto& format = group_key_data->row_format;
  auto& arena = group_key_data->key_arena;
  const auto chunk_count = input_table->chunk_count();

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    const auto materialized = _materialize_rows(format, chunk, groupby_column_ids);

    auto* row_ptr = materialized->rows.get();
    for (auto row_index = size_t{0}; row_index < materialized->row_count; ++row_index) {
      const auto row_view = RowView{row_ptr, format};
      const auto row_hash = compute_hash(row_view.key_bytes(), format.string_ptr_offset - format.null_bitmap_offset);
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
        iter = group_key_data->global_hash_table
                   .emplace(group_key, static_cast<uint64_t>(group_key_data->global_hash_table.size()))
                   .first;
      }
      group_key_data->tickets.push_back(iter->second);

      row_ptr += format.row_size;
    }
  }

  // Build the group-by output columns while the key rows (and the arena backing their long strings) are still alive,
  // then hand back only the slim result; `GroupKeyData` does not escape this function.
  auto result = GroupingResult{};
  result.group_count = group_key_data->global_hash_table.size();
  result.groupby_segments = _build_groupby_segments(*group_key_data, groupby_column_ids, input_table);
  result.tickets = std::move(group_key_data->tickets);
  return result;
}

GroupingResult _compute_groups(const std::vector<ColumnID>& groupby_column_ids,
                               const std::shared_ptr<const Table>& input_table) {
  if (groupby_column_ids.size() == 1 && input_table->column_data_type(groupby_column_ids[0]) != DataType::String) {
    return _compute_groups_single_column(groupby_column_ids[0], input_table);
  }
  return _compute_groups_byte_row(groupby_column_ids, input_table);
}

}  // namespace hyrise
