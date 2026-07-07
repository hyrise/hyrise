#include "ticketing.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/unordered/concurrent_flat_map.hpp>

#include "hyrise.hpp"
#include "operators/aggregate_dyod_utils/concurrent_ticket_map.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

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
  const auto write_stable_string = [&](const RowView& row, const pmr_string& value) {
    if (write_inline(row, value.c_str(), value.size())) {
      row.set_string_ptr(string_col_index, const_cast<char*>(value.c_str()));
    }
  };

  // Fallback: the iterator materializes a transient `pmr_string` per row, so long strings must be copied into
  // the per-chunk arena and promoted into the key arena when a group is first inserted.

  // TODO(@Rob2U): We should write a specialization for FixedStringDictionarySegment and maybe think about a generic
  // way.
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
void _materialize_rows(const RowFormat& format, const std::shared_ptr<const Chunk>& chunk,
                       const std::vector<ColumnID>& groupby_column_ids, MaterializedRows& materialized) {
  const auto chunk_size = chunk->size();

  // The row buffer and string arena are owned by the caller and reused across chunks.
  materialized.row_count = chunk_size;
  materialized.string_pointer_needs_copy.clear();
  materialized.string_arena.release();
  auto* const rows = materialized.rows.get();
  std::memset(rows, 0, static_cast<size_t>(chunk_size) * format.row_size);

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
                                   materialized);
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
}

// To ensure contiguous assigment of tickets we first insert this placeholder and only the thread that inserts this
// increments the global ticket counter
constexpr auto PLACE_HOLDER_TICKET = std::numeric_limits<uint64_t>::max();

// The number of threads to use for parallelization.
const auto THREAD_COUNT = Hyrise::get().topology.num_cpus() - 1;  // leave one core for the scheduler

// Fuzzy ticketing: 128 tickets per thread. Once range is full, the thread fights for the next range.
constexpr auto TICKET_RANGE_LENGTH = uint64_t{1} << 10;  // 1024 tickets per thread

// TODO(@Rob2U): Use small local lookup table (direct-mapped here)
// Fast path for a single non-string group-by column. Here the value is the key, so we do not need to materialize rows.
// Like the byte-row path, ticketing is kept separate from building the output column: the hot loop only grows the
// value->ticket map and emits tickets.

std::shared_ptr<GroupKeyData<true>> _compute_groups_single_column_concurrent(
    const ColumnID groupby_column_id, const std::shared_ptr<const Table>& input_table) {
  // The single-column fast path carries no hash table (`has_hash_table` stays false), so `GroupKeyData` here is
  // only a tickets + group-count carrier and its `row_format`/`global_hash_table` stay unused.
  auto group_key_data = std::make_shared<GroupKeyData<true>>(RowFormat{});
  auto& tickets = group_key_data->tickets;
  tickets.resize(input_table->row_count());

  const auto data_type = input_table->column_data_type(groupby_column_id);
  const auto chunk_count = input_table->chunk_count();

  if (chunk_count == 0) {
    group_key_data->group_count = 0;
    group_key_data->has_hash_table = true;  // empty table, no groups, but the hash table is trivially built
    return group_key_data;
  }

  resolve_data_type(data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
      Fail("The single-column fast path is not used for string columns.");
    } else {
      // The value is the key, so no rows are materialized: the hot loop only grows a `value -> ticket` map and emits
      // tickets. Fuzzy ticketing is preserved - each thread hands out tickets from its own claimed range - so the map
      // just stores and returns the caller's candidate ticket (see `ConcurrentTicketMap`).
      constexpr auto null_ticket =
          std::numeric_limits<uint64_t>::max() - 1;  // NULL rows carry this sentinel until compaction; never inserted.
      auto has_null = std::atomic<bool>{false};

      // Row index of each chunk's first row, so `(chunk_id, chunk_offset)` maps into the flat `tickets` vector.
      auto ticket_offsets = std::vector<uint64_t>(chunk_count, 0);
      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count - 1; ++chunk_id) {
        ticket_offsets[chunk_id + 1] =
            ticket_offsets[chunk_id] + static_cast<uint64_t>(input_table->get_chunk(chunk_id)->size());
      }

      // `ConcurrentTicketMap` never resizes, so its capacity is fixed up front from the first chunk's group density,
      // extrapolated to the whole table and capped at `row_count` (a hard upper bound on the number of distinct
      // groups). We assume this estimate is accurate: the map keeps ~1/load-factor headroom above it, but a gross
      // undershoot would overfill the fixed table. The first chunk is scanned once here for the estimate and again
      // (with the real map) in the parallel phase below - one extra chunk of work.
      const auto& first_chunk = input_table->get_chunk(ChunkID{0});
      const auto first_chunk_size = size_t{first_chunk->size()};
      auto estimated_groups = std::max<size_t>(first_chunk_size, 1);
      {
        auto estimate_map = ConcurrentTicketMap<ColumnDataType>(first_chunk_size);
        const auto& segment = first_chunk->get_segment(groupby_column_id);
        auto counter = uint64_t{0};
        segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
          if (!position.is_null() && estimate_map.try_emplace(position.value(), counter) == counter) {
            ++counter;
          }
        });
        if (first_chunk_size > 0) {
          const auto groups_seen = counter;
          const auto remaining_rows = input_table->row_count() - first_chunk_size;
          estimated_groups =
              std::max<size_t>(1, std::min<size_t>(input_table->row_count(),
                                                   groups_seen + remaining_rows * groups_seen / first_chunk_size));
        }
      }

      auto value_to_ticket = ConcurrentTicketMap<ColumnDataType>(estimated_groups);
      auto next_ticket_range_start = std::atomic<uint64_t>{0};

      const auto process_chunk = [&](const ChunkID chunk_id, uint64_t& next_ticket, uint64_t& ticket_range_end) {
        const auto& chunk = input_table->get_chunk(chunk_id);
        const auto& segment = chunk->get_segment(groupby_column_id);
        const auto chunk_start = ticket_offsets[chunk_id];

        segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
          auto current_ticket = null_ticket;
          if (position.is_null()) {
            has_null.store(true, std::memory_order_relaxed);
          } else {
            // Offer the candidate ticket `next_ticket`; the map returns this group's ticket. Because ticket ranges are
            // disjoint across threads, the returned ticket equals `next_ticket` exactly when this call inserted, in
            // which case we consume the candidate and refill the range when it runs out.
            current_ticket = value_to_ticket.try_emplace(position.value(), next_ticket);
            if (current_ticket == next_ticket) {
              ++next_ticket;
              if (next_ticket >= ticket_range_end) {
                next_ticket = next_ticket_range_start.fetch_add(TICKET_RANGE_LENGTH);
                ticket_range_end = next_ticket + TICKET_RANGE_LENGTH;
              }
            }
          }
          tickets[chunk_start + position.chunk_offset()] = current_ticket;
        });
      };

      const auto job_count = std::min<size_t>(THREAD_COUNT, chunk_count);

      // Threads steal chunks from a shared cursor and hand out tickets from their own ranges. When a thread stops it
      // usually leaves a trailing gap [next_ticket, ticket_range_end) of unused tickets at the end of its last claimed
      // range. We record these per-thread gaps here and compact them out below so the final tickets form a dense
      // [0, group_count) range. The ranges tile the ticket space contiguously, so the only gaps are these trailing
      // ones.
      auto ticket_gaps = std::vector<std::pair<uint64_t, uint64_t>>(job_count);
      auto next_chunk_id = std::atomic<uint32_t>{0};

      auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
      jobs.reserve(job_count);
      for (auto job_id = uint32_t{0}; job_id < job_count; ++job_id) {
        jobs.emplace_back(std::make_shared<JobTask>([&, job_id] {
          auto next_ticket = next_ticket_range_start.fetch_add(TICKET_RANGE_LENGTH);
          auto ticket_range_end = next_ticket + TICKET_RANGE_LENGTH;
          while (true) {
            const auto chunk_id = next_chunk_id.fetch_add(1);
            if (chunk_id >= chunk_count) {
              break;
            }
            process_chunk(ChunkID{chunk_id}, next_ticket, ticket_range_end);
          }
          ticket_gaps[job_id] = {next_ticket, ticket_range_end};
        }));
      }
      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

      // Sort the gaps by their start so we can prefix-sum the unused ticket counts. `std::pair` orders by start first,
      // and because the gaps are disjoint this also orders them by end.
      std::sort(ticket_gaps.begin(), ticket_gaps.end());
      auto sorted_gap_starts = std::vector<uint64_t>(job_count);
      // `unused_before_gap[i]` is the total number of unused tickets in all gaps ordered before gap `i`.
      auto unused_before_gap = std::vector<uint64_t>(job_count + 1, 0);
      for (auto i = size_t{0}; i < job_count; ++i) {
        sorted_gap_starts[i] = ticket_gaps[i].first;
        unused_before_gap[i + 1] = unused_before_gap[i] + (ticket_gaps[i].second - ticket_gaps[i].first);
      }

      // The distinct non-NULL groups own tickets in first-seen order; the NULL group (if any) is appended last. The
      // number of distinct groups is the total claimed ticket space (the top of the highest range) minus the unused
      // tickets in all trailing gaps.
      const auto null_group_ticket =
          next_ticket_range_start.load(std::memory_order_relaxed) - unused_before_gap[job_count];
      group_key_data->group_count = null_group_ticket + (has_null.load(std::memory_order_relaxed) ? 1 : 0);

      // Compact the tickets: every non-NULL ticket sits below its own range's gap, so it must be shifted down by the
      // total size of all gaps lying entirely below it. A used ticket never falls inside a gap, so the number of such
      // gaps is simply how many gap starts precede it.
      const auto row_count = input_table->row_count();

      auto compact_range = [&](const size_t start, const size_t end) {
        for (auto row_index = size_t{start}; row_index < end; ++row_index) {
          const auto ticket = tickets[row_index];
          if (ticket == null_ticket) {
            tickets[row_index] = null_group_ticket;  // NULL groups are always last.
            continue;
          }
          const auto gaps_below = static_cast<size_t>(
              std::lower_bound(sorted_gap_starts.begin(), sorted_gap_starts.end(), ticket) - sorted_gap_starts.begin());
          tickets[row_index] = ticket - unused_before_gap[gaps_below];
        }
      };

      auto jobs_compactions = std::vector<std::shared_ptr<AbstractTask>>{};
      jobs_compactions.reserve(job_count);
      auto ticket_chunk_size = row_count / job_count;
      for (auto job_id = uint32_t{0}; job_id < job_count; ++job_id) {
        jobs_compactions.emplace_back(std::make_shared<JobTask>([&, job_id] {
          const auto start = job_id * ticket_chunk_size;
          const auto end = (job_id == job_count - 1) ? row_count : start + ticket_chunk_size;
          compact_range(start, end);
        }));
      }
      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs_compactions);
    }
  });

  return group_key_data;
}

std::shared_ptr<GroupKeyData<false>> _compute_groups_single_column_sequential(
    const ColumnID groupby_column_id, const std::shared_ptr<const Table>& input_table) {
  // Fast path for a single non-string group-by column. Here the value is the key, so we do not need to materialize rows.
  // Like the byte-row path, ticketing is kept separate from building the output column: the hot loop only grows the
  // value->ticket map and emits tickets.
  // The single-column fast path carries no hash table (`has_hash_table` stays false), so `GroupKeyData` here is only a
  // tickets + group-count carrier and its `row_format`/`global_hash_table` stay unused.
  auto group_key_data = std::make_shared<GroupKeyData<false>>(RowFormat{});
  auto& tickets = group_key_data->tickets;
  tickets.reserve(input_table->row_count());

  const auto data_type = input_table->column_data_type(groupby_column_id);
  const auto chunk_count = input_table->chunk_count();

  resolve_data_type(data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
      Fail("The single-column fast path is not used for string columns.");
    } else {
      auto value_to_ticket = boost::unordered_flat_map<ColumnDataType, uint64_t>{};
      value_to_ticket.reserve(TARGET_CHUNK_SIZE);

      // Tickets are handed out densely in first-seen order across the NULL group and the value groups alike, so a
      // single counter suffices and no output vectors are touched in the hot loop.
      auto next_ticket = uint64_t{0};
      auto null_ticket = uint64_t{0};
      auto has_null = false;

      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto& chunk = input_table->get_chunk(chunk_id);
        const auto& segment = chunk->get_segment(groupby_column_id);
        auto ticket = uint64_t{0};
        segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
          if (position.is_null()) {
            if (!has_null) {
              has_null = true;
              null_ticket = next_ticket++;
            }
            ticket = null_ticket;
          } else {
            const auto [iter, inserted] = value_to_ticket.try_emplace(position.value(), next_ticket);
            if (inserted) {
              ++next_ticket;
            }
            ticket = iter->second;
          }
          tickets.push_back(ticket);
        });

        // Adaptively reserve space in the hashmap just as in `_compute_groups_byte_row`.
        if (chunk_id == ChunkID{0} && chunk_count > 1) {
          const auto rows_seen = chunk->size();
          const auto groups_seen = value_to_ticket.size();
          if (rows_seen > 0) {
            const auto remaining_rows = input_table->row_count() - rows_seen;
            const auto estimated_groups =
                std::min<size_t>(input_table->row_count(), groups_seen + remaining_rows * groups_seen / rows_seen);
            value_to_ticket.reserve(estimated_groups);
          }
        }
      }

      group_key_data->group_count = size_t{next_ticket};
    }
  });

  return group_key_data;
}

template <bool Concurrent>
std::shared_ptr<GroupKeyData<Concurrent>> _compute_groups_single_column(
    const ColumnID groupby_column_id, const std::shared_ptr<const Table>& input_table) {
  if constexpr (Concurrent) {
    return _compute_groups_single_column_concurrent(groupby_column_id, input_table);
  } else {
    return _compute_groups_single_column_sequential(groupby_column_id, input_table);
  }
}

// A single slot of the direct-mapped cache that sits in front of the global hash table.
// `key.row == nullptr` marks an empty slot. Occupied slots store a *stable* key pointer into one of
// `GroupKeyData::key_arenas`, so entries stay valid across chunks.
struct GroupCacheSlot {
  GroupKey key{nullptr, 0};
  uint64_t ticket = 0;
};

// Size of the direct-mapped cache. 4096 slots * sizeof(GroupCacheSlot) (24 B) -> 96 KiB
constexpr auto GROUP_CACHE_SLOTS = size_t{1} << 12;
constexpr auto GROUP_CACHE_MASK = GROUP_CACHE_SLOTS - 1;

// Standard path: materialize each row's group-by key into a packed row format, hash it and probe a global hash table.
template <bool Concurrent>
std::shared_ptr<GroupKeyData<Concurrent>> _compute_groups_byte_row(const std::vector<ColumnID>& groupby_column_ids,
                                                                   const std::shared_ptr<const Table>& input_table) {
  const auto row_format = _create_row_format(input_table->column_definitions(), groupby_column_ids);
  const auto chunk_count = input_table->chunk_count();

  auto group_key_data = std::make_shared<GroupKeyData<Concurrent>>(row_format);
  group_key_data->tickets.resize(input_table->row_count());

  if (chunk_count == 0) {
    group_key_data->group_count = 0;
    group_key_data->has_hash_table = true;
    return group_key_data;
  }

  // We let the first chunk grow the table naturally and then size it from the observed cardinality.
  auto& global_hash_table = group_key_data->global_hash_table;
  const auto& format = group_key_data->row_format;

  const auto key_equal = GroupKeyEqual{&format};
  auto current_ticket = std::atomic<uint64_t>{0};

  // One reusable row buffer for the materialize step, allocated once and sized to the largest chunk.
  auto max_chunk_size = size_t{0};
  auto ticket_offsets = std::vector<uint64_t>(chunk_count, 0);

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count - 1; ++chunk_id) {
    const auto chunk_size = input_table->get_chunk(chunk_id)->size();
    ticket_offsets[chunk_id + 1] = ticket_offsets[chunk_id] + static_cast<uint64_t>(chunk_size);
    max_chunk_size = std::max(max_chunk_size, static_cast<size_t>(chunk_size));
  }
  max_chunk_size =
      std::max(max_chunk_size, static_cast<size_t>(input_table->get_chunk(ChunkID{chunk_count - 1})->size()));

  const auto process_chunk = [&](const ChunkID chunk_id, std::array<GroupCacheSlot, GROUP_CACHE_SLOTS>& local_cache,
                                 MaterializedRows& materialized, std::pmr::monotonic_buffer_resource& arena) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    _materialize_rows(format, chunk, groupby_column_ids, materialized);

    auto* row_ptr = materialized.rows.get();
    const auto chunk_start = ticket_offsets[chunk_id];
    for (auto chunk_offset = size_t{0}; chunk_offset < materialized.row_count; ++chunk_offset) {
      const auto row_view = RowView{row_ptr, format};
      const auto row_hash = compute_hash(row_view.key_bytes(), format.key_length);
      const auto probe_key = GroupKey{.row = row_ptr, .hash = row_hash};

      auto& slot = local_cache[row_hash & GROUP_CACHE_MASK];
      if (slot.key.row != nullptr && slot.key.hash == row_hash && key_equal(slot.key, probe_key)) {
        group_key_data->tickets[chunk_start + chunk_offset] = slot.ticket;
        row_ptr += format.row_size;
        continue;
      }

      // This group is not in the cache, so look it up in (or insert it into) the global table. `ticket` starts at the
      // placeholder so the read-back spin below runs whenever we observe a group that another thread has claimed but
      // not yet finished inserting (its entry still holds `PLACE_HOLDER_TICKET`).
      auto ticket = PLACE_HOLDER_TICKET;
      auto global_group_key = GroupKey{nullptr, 0};
      auto exists = false;

      if constexpr (Concurrent) {
        exists = global_hash_table.cvisit(probe_key, [&global_group_key, &ticket](const auto& entry) {
          global_group_key = entry.first;
          ticket = entry.second;
        }) == 1;
      } else {
        const auto it = global_hash_table.find(probe_key);
        if (it != global_hash_table.end()) {
          global_group_key = it->first;
          ticket = it->second;
          exists = true;
        }
      }

      if (!exists) {
        // First time we see this group so copy the key row into this thread's arena so it outlives the "materialized"
        // buffer. Long strings that live in the per-chunk arena are copied alongside it.
        // Strings that already point at stable source memory (value/dictionary paths) are left as is.
        // The arena belongs to this thread alone, so no synchronization is needed for the copies below.
        auto* const row_copy = static_cast<uint8_t*>(arena.allocate(format.row_size, alignof(uint64_t)));
        std::memcpy(row_copy, row_ptr, format.row_size);

        const auto copy_view = RowView{row_copy, format};
        const auto string_col_count = copy_view.string_col_count();
        for (auto string_col_index = size_t{0}; string_col_index < string_col_count; ++string_col_index) {
          if (!materialized.string_pointer_needs_copy[string_col_index]) {
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
        global_group_key = group_key;
        if constexpr (Concurrent) {
          // Try to claim this group's ticket by writing a PLACE_HOLDER_TICKET.
          if (global_hash_table.try_emplace(group_key, PLACE_HOLDER_TICKET)) {
            // We won the race, so NOW hand out the real ticket.
            ticket = current_ticket++;
            global_hash_table.insert_or_assign(group_key, ticket);
          }
          // If we lost the race `try_emplace` returned false and `ticket` is still the placeholder; the spin below reads
          // the winner's ticket once it is written.
        } else {
          ticket = current_ticket++;
          global_hash_table.emplace(group_key, ticket);
        }
      }

      if constexpr (Concurrent) {
        // Whether we found an entry mid-insert or lost the `try_emplace` race, `ticket` still holds the placeholder until
        // the claiming thread writes the real ticket. Spin until it does. `cvisit` returns the number of matched entries
        // (always 1 here); the ticket is delivered through the lambda, not the return value.
        while (ticket == PLACE_HOLDER_TICKET) {
          global_hash_table.cvisit(global_group_key, [&ticket](const auto& entry) {
            ticket = entry.second;
          });
        }
      }
      // Fill the cache slot with the group's stable arena key (from the global entry, not the transient probe row) so
      // later rows of this group, in this or a later chunk, hit above. This overwrites any group previously in the
      // slot.
      slot.key = GroupKey{.row = global_group_key.row, .hash = row_hash};
      slot.ticket = ticket;
      group_key_data->tickets[chunk_start + chunk_offset] = ticket;

      row_ptr += format.row_size;
    }

    // After the first chunk, size the table from the observed group density rather than the `row_count` upper bound.
    // We extrapolate the distinct groups seen so far linearly to the remaining rows (capped at `row_count`): a
    // low-cardinality group-by that already saw all its groups reserves almost nothing and stays cache-resident, while
    // a high-cardinality one reserves close to `row_count` and avoids repeated rehashing over the remaining chunks.
    if (chunk_id == ChunkID{0} && chunk_count > 1) {
      const auto rows_seen = materialized.row_count;
      const auto groups_seen = global_hash_table.size();
      if (rows_seen > 0) {
        const auto remaining_rows = input_table->row_count() - rows_seen;
        const auto estimated_groups =
            std::min<size_t>(input_table->row_count(), groups_seen + remaining_rows * groups_seen / rows_seen);
        global_hash_table.reserve(estimated_groups);
      }
    }
  };

  auto materialized = MaterializedRows{};
  materialized.rows = std::make_unique<uint8_t[]>(max_chunk_size * format.row_size);

  // Direct-mapped cache in front of the global table indexed by the low bits of the row hash. It is
  // probed before the global table so that a recently seen group (within or across chunks) skips the often-cold global
  // lookup and its string comparisons. Each occupied slot stores a stable key pointer into one of the `key_arenas`, so
  // entries survive across chunks.
  auto local_cache = std::array<GroupCacheSlot, GROUP_CACHE_SLOTS>{};

  if constexpr (Concurrent) {
    // One monotonic arena per grouping thread (plus the main thread, which processes the first chunk before the jobs
    // start and reuses arena 0). Because each thread only ever allocates from its own arena, copying newly seen group
    // keys needs no locking. The arenas are owned by `group_key_data` and outlive this call because the global hash
    // table's keys point into them.
    const auto job_count = std::min<size_t>(THREAD_COUNT, chunk_count);
    auto& arenas = group_key_data->key_arenas;
    arenas.reserve(job_count);
    for (auto arena_id = size_t{0}; arena_id < job_count; ++arena_id) {
      arenas.emplace_back(std::make_unique<std::pmr::monotonic_buffer_resource>());
    }

    // Process first chunk to estimate the group count and reserve in the global_hash_table accordingly. This runs before
    // the jobs start, so it can safely reuse job 0's arena.
    process_chunk(ChunkID{0}, local_cache, materialized, *arenas[0]);

    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    jobs.reserve(job_count);
    for (auto job_id = uint32_t{0}; job_id < job_count; ++job_id) {
      jobs.emplace_back(std::make_shared<JobTask>(
          [&process_chunk, &format, &arenas, max_chunk_size, chunk_count, job_count, job_id]() {
            auto materialized = MaterializedRows{};
            materialized.rows = std::make_unique<uint8_t[]>(max_chunk_size * format.row_size);

            // Direct-mapped cache in front of the global table indexed by the low bits of the row hash. It is
            // probed before the global table so that a recently seen group (within or across chunks) skips the
            // often-cold global lookup and its string comparisons. Each occupied slot stores a stable key pointer into
            // one of the `key_arenas`, so entries survive across chunks.
            auto local_cache = std::array<GroupCacheSlot, GROUP_CACHE_SLOTS>{};
            auto& arena = *arenas[job_id];
            for (auto chunk_id = ChunkID{job_id}; chunk_id < chunk_count; chunk_id += job_count) {
              if (chunk_id == ChunkID{0}) {
                continue;
              }
              process_chunk(chunk_id, local_cache, materialized, arena);
            }
          }));
    }
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  } else {
    // Single-threaded path: process all chunks in the main thread, reusing the same arena for all chunks.
    auto& arenas = group_key_data->key_arenas;
    auto& arena = *arenas.emplace_back(std::make_unique<std::pmr::monotonic_buffer_resource>());
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      process_chunk(chunk_id, local_cache, materialized, arena);
    }
  }

  // The group-by output columns are built afterwards (see `AggregateDYOD::_on_execute`): either by scanning the source
  // columns or, for low-cardinality group-bys, by reading each group's value from its key row. We therefore hand back
  // `GroupKeyData` so its hash table and key-row arena outlive this function.
  group_key_data->group_count = global_hash_table.size();
  group_key_data->has_hash_table = true;
  return group_key_data;
}

template <bool Concurrent>
std::shared_ptr<GroupKeyData<Concurrent>> _compute_groups(const std::vector<ColumnID>& groupby_column_ids,
                                                          const std::shared_ptr<const Table>& input_table) {
  if (groupby_column_ids.size() == 1 && input_table->column_data_type(groupby_column_ids[0]) != DataType::String) {
    const auto column_id = groupby_column_ids[0];
    return _compute_groups_single_column<Concurrent>(column_id, input_table);
  }
  return _compute_groups_byte_row<Concurrent>(groupby_column_ids, input_table);
}

//explicitly instantiate the template for both concurrent and non-concurrent execution
template std::shared_ptr<GroupKeyData<true>> _compute_groups<true>(const std::vector<ColumnID>& groupby_column_ids,
                                                                   const std::shared_ptr<const Table>& input_table);
template std::shared_ptr<GroupKeyData<false>> _compute_groups<false>(const std::vector<ColumnID>& groupby_column_ids,
                                                                     const std::shared_ptr<const Table>& input_table);

}  // namespace hyrise
