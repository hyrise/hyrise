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
// at the segment's own string storage instead of copying; other kinds fall back to the generic copying iterator.
// `materialized.string_pointer_needs_copy` records, per string column, whether its long-string pointers reference the
// transient per-chunk arena (and so must be promoted on insert) or stable, query-lifetime source memory.
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
  // value is longer than the prefix, i.e. whether the caller must additionally set a heap pointer to the full value so
  // equality can resolve prefix collisions. Reads straight from `data`, so no intermediate `pmr_string` is constructed.
  const auto write_inline = [&](const RowView& row, const char* const data, const size_t length) {
    auto* const inline_data = row.column_data(group_by_column_index);
    std::memcpy(inline_data, &length, sizeof(size_t));
    const auto prefix_length = std::min(length, static_cast<size_t>(PREFIX_LENGTH));
    std::memcpy(inline_data + sizeof(size_t), data, prefix_length);
    return length > PREFIX_LENGTH;
  };

  // Writes a string that lives in stable, query-lifetime memory (a value/dictionary segment owned by the input or by a
  // referenced table): the inline bytes, plus a direct pointer into that memory for long strings. `const_cast` is safe
  // because the pointer is only ever read (hash equality, output build).
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
    TRACE_EVENT("Aggregate", "Ticketing::materialize::ValueSegment<string>");
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
    TRACE_EVENT("Aggregate", "Ticketing::materialize::DictionarySegment<string>");
    // Dictionary segment: pack each distinct dictionary entry's inline bytes once, then per row copy the packed
    // bytes indexed by the row's value id. Long strings point straight at the dictionary entry (owned for the
    // whole query), so no per-row or per-group string copy is needed.
    materialized.string_pointer_needs_copy.push_back(false);
    const auto& dictionary = *dictionary_segment->dictionary();
    const auto null_value_id = dictionary_segment->null_value_id();
    const auto dictionary_size = dictionary.size();

    static constexpr auto INLINE_SIZE = sizeof(size_t) + PREFIX_LENGTH;
    auto inline_blobs = std::vector<std::array<uint8_t, INLINE_SIZE>>(dictionary_size);
    auto long_string_ptrs = std::vector<char*>(dictionary_size, nullptr);
    for (auto value_id = size_t{0}; value_id < dictionary_size; ++value_id) {
      const auto& str = dictionary[value_id];
      const auto length = str.size();
      std::memcpy(inline_blobs[value_id].data(), &length, sizeof(size_t));
      const auto prefix_length = std::min(length, static_cast<size_t>(PREFIX_LENGTH));
      std::memcpy(inline_blobs[value_id].data() + sizeof(size_t), str.c_str(), prefix_length);
      if (length > PREFIX_LENGTH) {
        // Points into the dictionary; `const_cast` is safe because the pointer is only ever read.
        long_string_ptrs[value_id] = const_cast<char*>(str.c_str());
      }
    }

    resolve_compressed_vector_type(*dictionary_segment->attribute_vector(), [&](const auto& attribute_vector) {
      auto offset = size_t{0};
      for (auto it = attribute_vector.cbegin(), end = attribute_vector.cend(); it != end; ++it, ++offset) {
        const auto value_id = static_cast<ValueID>(*it);
        const auto row = row_at(offset);
        if (value_id == null_value_id) {
          row.set_null_bitmap(row.null_bitmap() | null_mask_bit);
          continue;
        }
        std::memcpy(row.column_data(group_by_column_index), inline_blobs[value_id].data(), INLINE_SIZE);
        if (long_string_ptrs[value_id] != nullptr) {
          row.set_string_ptr(string_col_index, long_string_ptrs[value_id]);
        }
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

      if (const auto* const referenced_value = dynamic_cast<const ValueSegment<pmr_string>*>(referenced_segment.get())) {
        TRACE_EVENT("Aggregate", "Ticketing::materialize::ReferenceSegment<string> (value)");
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
        TRACE_EVENT("Aggregate", "Ticketing::materialize::ReferenceSegment<string> (dictionary)");
        materialized.string_pointer_needs_copy.push_back(false);
        const auto& dictionary = *referenced_dictionary->dictionary();
        const auto null_value_id = referenced_dictionary->null_value_id();
        resolve_compressed_vector_type(*referenced_dictionary->attribute_vector(), [&](const auto& attribute_vector) {
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
      TRACE_EVENT("Aggregate", "Ticketing::materialize::ReferenceSegment<string> (fallback)");
      materialize_via_iterator();
    }
  } else {
    // Fallback for every other segment kind (fixed-string dictionaries, other encodings).
    TRACE_EVENT("Aggregate", "Ticketing::materialize::fallback<string>");
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
        TRACE_EVENT("Aggregate", "Ticketing::materialize::numeric");
        // Non-string columns hold trivially-copyable fixed-width values; the generic iterator's by-value position is
        // free here, so there is nothing to gain from bypassing it.
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
  // The global hash table is intentionally NOT pre-reserved to `row_count`. That upper bound is only tight for
  // high-cardinality group-bys; for low-cardinality ones it allocates a huge, sparse table whose every probe is a
  // cache/TLB miss into cold memory. Instead we let the first chunk grow the table naturally and then size it from
  // the observed group density (see below), keeping it compact and cache-resident when there are few groups.
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

      auto iter = group_key_data->global_hash_table.find(probe_key);
      if (iter == group_key_data->global_hash_table.end()) {
        // First time we see this group: copy the key row into the arena so it outlives the per-chunk materialized
        // buffer. Long strings that live in the per-chunk arena (fallback path) are copied alongside it and the copied
        // row is repointed; strings that already point at stable source memory (value/dictionary paths) are left as is.
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
        iter = group_key_data->global_hash_table
                   .emplace(group_key, static_cast<uint64_t>(group_key_data->global_hash_table.size()))
                   .first;
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
      const auto groups_seen = group_key_data->global_hash_table.size();
      if (rows_seen > 0) {
        const auto remaining_rows = input_table->row_count() - rows_seen;
        const auto estimated_groups =
            std::min<size_t>(input_table->row_count(), groups_seen + remaining_rows * groups_seen / rows_seen);
        group_key_data->global_hash_table.reserve(estimated_groups);
      }
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

// Transparent hashing/equality so the single-string-column grouping map can be probed with a `std::string_view` (no
// allocation) and only materialize an owning `pmr_string` when a brand-new group is inserted.
struct StringViewHash {
  using is_transparent = void;

  size_t operator()(const std::string_view value) const {
    return compute_hash(value.data(), value.size());
  }

  size_t operator()(const pmr_string& value) const {
    return compute_hash(value.data(), value.size());
  }
};

struct StringViewEqual {
  using is_transparent = void;

  bool operator()(const std::string_view lhs, const std::string_view rhs) const {
    return lhs == rhs;
  }
};

// Fast path for a single string group-by column. Groups are keyed on the actual string, but dictionary-encoded chunks
// (directly, or referenced through a single-chunk reference segment) are grouped by their value ids: each distinct
// value id is resolved to a group ticket once per chunk and cached in `value_id_ticket`, so a string is only hashed and
// copied when its value id is first seen. Value segments are read by reference; anything else falls back to the generic
// iterator. The string is only copied when a genuinely new group is inserted into the global map.
GroupingResult _compute_groups_single_string_column(const ColumnID column_id,
                                                    const std::shared_ptr<const Table>& input_table) {
  const auto column_is_nullable = input_table->column_is_nullable(column_id);
  const auto chunk_count = input_table->chunk_count();

  auto result = GroupingResult{};
  result.tickets.reserve(input_table->row_count());

  // Global grouping structure keyed on the group string. Transparent lookup lets us probe with a `string_view`; the
  // string is only copied into the map when a new group is inserted.
  auto value_to_ticket = boost::unordered_flat_map<pmr_string, uint32_t, StringViewHash, StringViewEqual>{};
  auto next_ticket = uint32_t{0};
  auto has_null = false;
  auto null_ticket = uint32_t{0};

  const auto ticket_for_string = [&](const std::string_view value) {
    const auto iter = value_to_ticket.find(value);
    if (iter != value_to_ticket.end()) {
      return iter->second;
    }
    const auto ticket = next_ticket++;
    value_to_ticket.emplace(pmr_string{value}, ticket);
    return ticket;
  };

  const auto ticket_for_null = [&] {
    if (!has_null) {
      has_null = true;
      null_ticket = next_ticket++;
    }
    return null_ticket;
  };

  constexpr auto UNRESOLVED = std::numeric_limits<uint32_t>::max();

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& segment = input_table->get_chunk(chunk_id)->get_segment(column_id);
    const auto chunk_size = segment->size();

    // Try to reach a dictionary or value segment, directly or through a single-chunk reference. When `pos_list` is set,
    // a sequential row offset maps to the referenced chunk offset; `referenced_holder` keeps the referenced segment
    // alive for the duration of this chunk.
    const auto* dictionary_segment = dynamic_cast<const DictionarySegment<pmr_string>*>(segment.get());
    const auto* value_segment = dynamic_cast<const ValueSegment<pmr_string>*>(segment.get());
    const AbstractPosList* pos_list = nullptr;
    auto referenced_holder = std::shared_ptr<const AbstractSegment>{};

    if (!dictionary_segment && !value_segment) {
      if (const auto* const reference_segment = dynamic_cast<const ReferenceSegment*>(segment.get())) {
        const auto& reference_pos_list = reference_segment->pos_list();
        if (reference_pos_list->references_single_chunk() && !reference_pos_list->empty()) {
          referenced_holder = reference_segment->referenced_table()
                                  ->get_chunk(reference_pos_list->common_chunk_id())
                                  ->get_segment(reference_segment->referenced_column_id());
          dictionary_segment = dynamic_cast<const DictionarySegment<pmr_string>*>(referenced_holder.get());
          value_segment = dynamic_cast<const ValueSegment<pmr_string>*>(referenced_holder.get());
          if (dictionary_segment || value_segment) {
            pos_list = reference_pos_list.get();
          }
        }
      }
    }

    if (dictionary_segment) {
      TRACE_EVENT("Aggregate", "Ticketing::single-string::dictionary");
      // Compare value ids within the chunk: resolve each distinct value id to a global ticket once, then reuse it.
      const auto& dictionary = *dictionary_segment->dictionary();
      const auto null_value_id = dictionary_segment->null_value_id();
      auto value_id_ticket = std::vector<uint32_t>(dictionary.size(), UNRESOLVED);

      const auto emit_value_id = [&](const ValueID value_id) {
        if (value_id == null_value_id) {
          result.tickets.push_back(ticket_for_null());
          return;
        }
        auto& cached = value_id_ticket[value_id];
        if (cached == UNRESOLVED) {
          cached = ticket_for_string(std::string_view{dictionary[value_id]});
        }
        result.tickets.push_back(cached);
      };

      resolve_compressed_vector_type(*dictionary_segment->attribute_vector(), [&](const auto& attribute_vector) {
        auto decompressor = attribute_vector.create_decompressor();
        for (auto offset = size_t{0}; offset < chunk_size; ++offset) {
          const auto row_offset = pos_list ? size_t{(*pos_list)[offset].chunk_offset} : offset;
          emit_value_id(static_cast<ValueID>(decompressor.get(row_offset)));
        }
      });
    } else if (value_segment) {
      TRACE_EVENT("Aggregate", "Ticketing::single-string::value");
      const auto& values = value_segment->values();
      const auto is_nullable = value_segment->is_nullable();
      const auto* const null_values = is_nullable ? &value_segment->null_values() : nullptr;
      for (auto offset = size_t{0}; offset < chunk_size; ++offset) {
        const auto row_offset = pos_list ? size_t{(*pos_list)[offset].chunk_offset} : offset;
        if (is_nullable && (*null_values)[row_offset]) {
          result.tickets.push_back(ticket_for_null());
          continue;
        }
        result.tickets.push_back(ticket_for_string(std::string_view{values[row_offset]}));
      }
    } else {
      TRACE_EVENT("Aggregate", "Ticketing::single-string::fallback");
      // Multi-chunk references and other encodings: the generic iterator handles NULL row ids and materializes a
      // transient string per row, which we probe by view (copied only on insert).
      segment_iterate<pmr_string>(*segment, [&](const auto& position) {
        if (position.is_null()) {
          result.tickets.push_back(ticket_for_null());
          return;
        }
        result.tickets.push_back(ticket_for_string(std::string_view{position.value()}));
      });
    }
  }

  // Build the group-by output column from the distinct strings. Each group's representative lives in the map; the NULL
  // group's slot stays default-constructed and is flagged in `nulls`.
  const auto group_count = next_ticket;
  auto values = pmr_vector<pmr_string>(group_count);
  auto nulls = column_is_nullable ? pmr_vector<bool>(group_count, false) : pmr_vector<bool>{};
  for (const auto& [group_string, ticket] : value_to_ticket) {
    values[ticket] = group_string;
  }
  if (has_null && column_is_nullable) {
    nulls[null_ticket] = true;
  }

  result.group_count = group_count;
  if (column_is_nullable) {
    result.groupby_segments.push_back(std::make_shared<ValueSegment<pmr_string>>(std::move(values), std::move(nulls)));
  } else {
    result.groupby_segments.push_back(std::make_shared<ValueSegment<pmr_string>>(std::move(values)));
  }
  return result;
}

GroupingResult _compute_groups(const std::vector<ColumnID>& groupby_column_ids,
                               const std::shared_ptr<const Table>& input_table) {
  if (groupby_column_ids.size() == 1) {
    const auto column_id = groupby_column_ids[0];
    if (input_table->column_data_type(column_id) == DataType::String) {
      return _compute_groups_single_string_column(column_id, input_table);
    }
    return _compute_groups_single_column(column_id, input_table);
  }
  return _compute_groups_byte_row(groupby_column_ids, input_table);
}

}  // namespace hyrise
