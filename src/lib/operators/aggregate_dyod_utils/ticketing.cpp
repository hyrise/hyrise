#include "ticketing.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/unordered/unordered_flat_map.hpp>

#include "resolve_type.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

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
                   .key_length = string_ptr_offset - null_bitmap_offset,
                   .stores_nulls = stores_nulls,
                   .col_offsets = std::move(col_offsets),
                   .column_is_nullable = std::move(column_is_nullable)};
}

// Materializes one string group-by column of a chunk into the packed rows (see `_materialize_rows`). Dispatches on the
// segment's concrete type so value/dictionary segments (and single-chunk references to them) can point rows straight
// at the segment's own string storage instead of copying. Other kinds fall back to the generic copying iterator.
// `materialized.string_pointer_needs_copy` records, per string column, whether its long-string pointers reference the
// transient per-chunk arena (and so must be promoted on insert) or stable source memory.
void _materialize_string_column(const RowFormat& format, const AbstractSegment& segment,
                                const size_t group_by_column_index, const size_t string_col_index,
                                const uint64_t null_mask_bit, MaterializedRows& materialized) {
  auto* const rows = materialized.rows.get();
  const auto chunk_size = materialized.row_count;
  auto& string_arena = materialized.string_arena;

  const auto row_at = [&](const size_t offset) {
    return RowView{rows + offset * format.row_size, format};
  };

  // Writes the inline part of a string value ([length, prefix]) into `row`'s slot for this column. Returns whether the
  // value is longer than the prefix, i.e. whether the caller must additionally set a heap pointer to the full value.
  const auto write_inline = [&](const RowView& row, const char* const data, const size_t length) {
    auto* const inline_data = row.column_data(group_by_column_index);
    std::memcpy(inline_data, &length, sizeof(size_t));
    const auto prefix_length = std::min(length, static_cast<size_t>(PREFIX_LENGTH));
    std::memcpy(inline_data + sizeof(size_t), data, prefix_length);
    return length > PREFIX_LENGTH;
  };

  // Writes a string that lives in stable memory (a value/dictionary segment owned by the input or by a
  // referenced table): the inline bytes, plus a direct pointer into that memory for long strings.
  // NOTE(@Rob2U): We could actually do this for more segment types.
  const auto write_stable_string = [&](const RowView& row, const pmr_string& value) {
    if (write_inline(row, value.c_str(), value.size())) {
      row.set_string_ptr(string_col_index, const_cast<char*>(value.c_str()));
    }
  };

  // Generic fallback: the iterator materializes a transient `pmr_string` per row, so long strings must be copied into
  // the per-chunk arena and promoted into the key arena when a group is first inserted.
  const auto materialize_via_iterator = [&] {
    materialized.string_pointer_needs_copy.push_back(true);
    auto chunk_offset = size_t{0};
    segment_iterate<pmr_string>(segment, [&](const auto& position) {
      const auto row = row_at(chunk_offset);
      ++chunk_offset;
      if (position.is_null()) {
        row.set_null_bitmap(row.null_bitmap() | null_mask_bit);
        return;
      }
      const auto& str_value = position.value();
      if (write_inline(row, str_value.c_str(), str_value.size())) {
        const auto length = str_value.size();
        auto* const str_copy = static_cast<char*>(string_arena.allocate(length + 1));
        std::memcpy(str_copy, str_value.c_str(), length);
        str_copy[length] = '\0';
        row.set_string_ptr(string_col_index, str_copy);
      }
    });
  };

  if (const auto* const value_segment = dynamic_cast<const ValueSegment<pmr_string>*>(&segment)) {
    // Value segment: the segment owns its strings for the whole query, so point long strings straight at them
    // instead of copying.
    materialized.string_pointer_needs_copy.push_back(false);
    const auto& values = value_segment->values();
    if (value_segment->is_nullable()) {
      const auto& null_values = value_segment->null_values();
      for (auto offset = size_t{0}; offset < chunk_size; ++offset) {
        const auto row = row_at(offset);
        if (null_values[offset]) {
          row.set_null_bitmap(row.null_bitmap() | null_mask_bit);
          continue;
        }
        write_stable_string(row, values[offset]);
      }
    } else {
      for (auto offset = size_t{0}; offset < chunk_size; ++offset) {
        write_stable_string(row_at(offset), values[offset]);
      }
    }
  } else if (const auto* const dictionary_segment = dynamic_cast<const DictionarySegment<pmr_string>*>(&segment)) {
    // Dictionary segment: pack each distinct dictionary entry's inline bytes once, then per row copy the packed
    // bytes indexed by the row's value id. Long strings point straight at the dictionary entry (owned for the
    // whole query), so no per-row or per-group string copy is needed.
    materialized.string_pointer_needs_copy.push_back(false);
    const auto& dictionary = *dictionary_segment->dictionary();

    resolve_compressed_vector_type(*dictionary_segment->attribute_vector(), [&](const auto& attribute_vector) {
      auto decompressor = attribute_vector.create_decompressor();
      const auto null_value_id = dictionary_segment->null_value_id();
      for (auto offset = size_t{0}; offset < chunk_size; ++offset) {
        const auto value_id = static_cast<ValueID>(decompressor.get(offset));
        const auto row = row_at(offset);

        if (value_id == null_value_id) {
          row.set_null_bitmap(row.null_bitmap() | null_mask_bit);
          continue;
        }
        write_stable_string(row, dictionary[value_id]);
      }
    });
  } else if (const auto* const reference_segment = dynamic_cast<const ReferenceSegment*>(&segment)) {
    // Reference segment: in the common case the pos-list references a single chunk of a stable referenced table,
    // so we resolve that chunk's segment once and point long strings straight at its (value/dictionary) string
    // storage, exactly like the direct value/dictionary paths. Anything else (multi-chunk pos-lists, other
    // referenced encodings) uses the generic copying fallback.
    const auto& pos_list = reference_segment->pos_list();
    auto handled = false;

    if (pos_list->references_single_chunk() && !pos_list->empty()) {
      // A single-chunk pos-list is guaranteed to contain no NULL row ids, so only value-level NULLs are handled.
      const auto& referenced_table = reference_segment->referenced_table();
      const auto referenced_column_id = reference_segment->referenced_column_id();
      const auto referenced_segment =
          referenced_table->get_chunk(pos_list->common_chunk_id())->get_segment(referenced_column_id);

      if (const auto* const referenced_value =
              dynamic_cast<const ValueSegment<pmr_string>*>(referenced_segment.get())) {
        materialized.string_pointer_needs_copy.push_back(false);
        const auto& values = referenced_value->values();
        if (referenced_value->is_nullable()) {
          const auto& null_values = referenced_value->null_values();
          for (auto offset = size_t{0}; offset < chunk_size; ++offset) {
            const auto chunk_offset = (*pos_list)[offset].chunk_offset;
            const auto row = row_at(offset);
            if (null_values[chunk_offset]) {
              row.set_null_bitmap(row.null_bitmap() | null_mask_bit);
              continue;
            }
            write_stable_string(row, values[chunk_offset]);
          }
        } else {
          for (auto offset = size_t{0}; offset < chunk_size; ++offset) {
            write_stable_string(row_at(offset), values[(*pos_list)[offset].chunk_offset]);
          }
        }
        handled = true;
      } else if (const auto* const referenced_dictionary =
                     dynamic_cast<const DictionarySegment<pmr_string>*>(referenced_segment.get())) {
        materialized.string_pointer_needs_copy.push_back(false);
        const auto& dictionary = *referenced_dictionary->dictionary();
        resolve_compressed_vector_type(*referenced_dictionary->attribute_vector(), [&](const auto& attribute_vector) {
          const auto null_value_id = referenced_dictionary->null_value_id();
          auto decompressor = attribute_vector.create_decompressor();
          for (auto offset = size_t{0}; offset < chunk_size; ++offset) {
            const auto chunk_offset = (*pos_list)[offset].chunk_offset;
            const auto value_id = static_cast<ValueID>(decompressor.get(chunk_offset));
            const auto row = row_at(offset);
            if (value_id == null_value_id) {
              row.set_null_bitmap(row.null_bitmap() | null_mask_bit);
              continue;
            }
            write_stable_string(row, dictionary[value_id]);
          }
        });
        handled = true;
      }
    }

    if (!handled) {
      materialize_via_iterator();
    }
  } else {
    // Fallback for every other segment kind (fixed-string dictionaries, other encodings).
    materialize_via_iterator();
  }
}

// TODO(@forUnity): think about alignment and padding, also sort string_columns to be last in groupby columns?
std::shared_ptr<MaterializedRows> _materialize_rows(const RowFormat& format, const std::shared_ptr<const Chunk>& chunk,
                                                    const std::vector<ColumnID>& groupby_column_ids) {
  const auto chunk_size = chunk->size();

  auto materialized = std::make_shared<MaterializedRows>();
  materialized->row_count = chunk_size;
  materialized->rows = std::make_unique<uint8_t[]>(chunk_size * format.row_size);
  auto* const rows = materialized->rows.get();

  // Index of the current string column among the group-by columns. Selects which string-pointer slot to write.
  auto string_col_index = size_t{0};
  for (auto group_by_column_index = size_t{0}; group_by_column_index < groupby_column_ids.size();
       ++group_by_column_index) {
    const auto column_id = groupby_column_ids[group_by_column_index];
    const auto& segment = chunk->get_segment(column_id);
    const auto null_mask_bit = uint64_t{1} << group_by_column_index;

    resolve_data_type(segment->data_type(), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
        _materialize_string_column(format, *segment, group_by_column_index, string_col_index, null_mask_bit,
                                   *materialized);
        ++string_col_index;
      } else {
        auto chunk_offset = size_t{0};
        segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
          const auto row = RowView{rows + chunk_offset * format.row_size, format};
          ++chunk_offset;
          if (position.is_null()) {
            row.set_null_bitmap(row.null_bitmap() | null_mask_bit);
            return;
          }
          row.write_value(group_by_column_index, position.value());
        });
      }
    });
  }

  return materialized;
}

// Fast path for a single non-string group-by column: the value is the key, so we do not need to materialize rows.
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
      const auto input_row_count = input_table->row_count();
      value_to_ticket.reserve(input_row_count);
      auto null_ticket = uint32_t{0};
      auto has_null = false;

      // Representative value per group, used to build the output column. The NULL group's slot is never read.
      auto group_values = pmr_vector<ColumnDataType>{};
      group_values.reserve(input_row_count);
      auto group_nulls = pmr_vector<bool>{};
      group_nulls.reserve(input_row_count);

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
              group_values.push_back(std::move(position.value()));
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
  // The global hash table is intentionally NOT pre-reserved to `row_count`. That upper bound is only tight for
  // high-cardinality group-bys; for low-cardinality ones it allocates a huge, sparse table whose every probe is a
  // cache/TLB miss into cold memory. Instead we let the first chunk grow the table naturally and then size it from
  // the observed group density (see below), keeping it compact and cache-resident when there are few groups.
  auto& global_hash_table = group_key_data->global_hash_table;
  const auto& format = group_key_data->row_format;
  auto& arena = group_key_data->key_arena;
  const auto chunk_count = input_table->chunk_count();

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    const auto materialized = _materialize_rows(format, chunk, groupby_column_ids);

    auto* row_ptr = materialized->rows.get();
    for (auto row_index = size_t{0}; row_index < materialized->row_count; ++row_index) {
      const auto row_view = RowView{row_ptr, format};
      const auto row_hash = compute_hash(row_view.key_bytes(), format.key_length);
      const auto probe_key = GroupKey{.row = row_ptr, .hash = row_hash};

      auto iter = global_hash_table.find(probe_key);
      if (iter == global_hash_table.end()) {
        // First time we see this group: copy the key row into the arena so it outlives the per-chunk materialized
        // buffer. Long strings that live in the per-chunk arena are copied alongside it.
        // Strings that already point at stable source memory (value/dictionary paths) are left as is.
        auto* const row_copy = static_cast<uint8_t*>(arena.allocate(format.row_size, alignof(uint64_t)));
        std::memcpy(row_copy, row_ptr, format.row_size);

        const auto copy_view = RowView{row_copy, format};
        const auto string_col_count = copy_view.string_col_count();
        for (auto string_col_index = size_t{0}; string_col_index < string_col_count; ++string_col_index) {
          if (!materialized->string_pointer_needs_copy[string_col_index]) {
            continue;
          }
          auto* const str_ptr = copy_view.string_ptr(string_col_index);
          if (str_ptr != nullptr) {
            const auto length = std::strlen(str_ptr) + 1;
            auto* const arena_str = static_cast<char*>(arena.allocate(length));
            std::memcpy(arena_str, str_ptr, length);
            copy_view.set_string_ptr(string_col_index, arena_str);
          }
        }

        const auto group_key = GroupKey{.row = row_copy, .hash = row_hash};
        iter = global_hash_table.emplace(group_key, static_cast<uint64_t>(global_hash_table.size())).first;
      }
      group_key_data->tickets.push_back(iter->second);

      row_ptr += format.row_size;
    }

    // After the first chunk, size the table from the observed group density rather than the `row_count` upper bound.
    // We extrapolate the distinct groups seen so far linearly to the remaining rows (capped at `row_count`): a
    // low-cardinality group-by that already saw all its groups reserves almost nothing and stays cache-resident, while
    // a high-cardinality one reserves close to `row_count` and avoids repeated rehashing over the remaining chunks.
    if (chunk_id == ChunkID{0} && chunk_count > 1) {
      const auto rows_seen = materialized->row_count;
      const auto groups_seen = global_hash_table.size();
      if (rows_seen > 0) {
        const auto remaining_rows = input_table->row_count() - rows_seen;
        const auto estimated_groups =
            std::min<size_t>(input_table->row_count(), groups_seen + remaining_rows * groups_seen / rows_seen);
        global_hash_table.reserve(estimated_groups);
      }
    }
  }

  // Build the group-by output columns while the key rows (and the arena backing their long strings) are still alive,
  // then hand back only the slim result; `GroupKeyData` does not escape this function.
  auto result = GroupingResult{};
  result.group_count = global_hash_table.size();
  result.groupby_segments = _build_groupby_segments(*group_key_data, groupby_column_ids, input_table);
  result.tickets = std::move(group_key_data->tickets);
  return result;
}

GroupingResult _compute_groups(const std::vector<ColumnID>& groupby_column_ids,
                               const std::shared_ptr<const Table>& input_table) {
  if (groupby_column_ids.size() == 1 && input_table->column_data_type(groupby_column_ids[0]) != DataType::String) {
    const auto column_id = groupby_column_ids[0];
    return _compute_groups_single_column(column_id, input_table);
  }
  return _compute_groups_byte_row(groupby_column_ids, input_table);
}

}  // namespace hyrise
