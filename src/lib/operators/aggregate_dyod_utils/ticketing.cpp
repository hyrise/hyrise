#include "ticketing.hpp"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "cardinality_estimation.hpp"
#include "hyrise.hpp"
#include "operators/aggregate_dyod_utils/concurrent_ticket_map.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

static constexpr uint64_t align(const uint64_t offset, const uint64_t alignment) {
  return (offset % alignment == 0) ? offset : (offset + (alignment - (offset % alignment)));
}

RowFormat create_row_format(const TableColumnDefinitions& column_definitions,
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
      curr_offset += (sizeof(char) * PREFIX_LENGTH) + sizeof(size_t);  // prefix + length
      string_column_count++;
    } else {
      resolve_data_type(column_definition.data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;
        curr_offset += sizeof(ColumnDataType);
      });
    }
  }

  // The null bitmap is only present when at least one group-by column is nullable.
  const auto null_bitmap_size = stores_nulls ? sizeof(uint64_t) : uint64_t{0};
  const auto data_offset = null_bitmap_size;  // null bitmap (if present)
  const auto null_bitmap_offset = uint64_t{0};
  const auto key_length = (data_offset + curr_offset) - null_bitmap_offset;
  const auto string_ptr_offset = align(data_offset + curr_offset, alignof(char*));
  const auto row_size = align(string_ptr_offset + (string_column_count * sizeof(char*)), ROW_ALIGNMENT);

  return RowFormat{.row_size = row_size,
                   .null_bitmap_offset = null_bitmap_offset,
                   .data_offset = data_offset,
                   .string_ptr_offset = string_ptr_offset,
                   .key_length = key_length,
                   .string_column_count = string_column_count,
                   .stores_nulls = stores_nulls,
                   .col_offsets = std::move(col_offsets),
                   .column_is_nullable = std::move(column_is_nullable)};
}

// Materializes one string group-by column of a chunk into the packed rows. Dispatches on the
// segment's concrete type so value/dictionary segments (and single-chunk references to them) can point rows straight
// at the segment's own string storage instead of copying. Other kinds fall back to the generic copying iterator.
// materialized.string_pointer_needs_copy` records, per string column, whether its long-string pointers reference the
// transient per-chunk arena (and so must be promoted on insert) or stable source memory.
static void materialize_string_column(const RowFormat& format, const std::shared_ptr<AbstractSegment>& segment,
                                      const size_t group_by_column_index, const size_t string_col_index,
                                      const uint64_t null_mask_bit, MaterializedRows& materialized) {
  auto* const rows = materialized.rows.get();
  auto& string_arena = materialized.string_arena;

  const auto row_at = [&](const size_t offset) {
    return RowView{.base = rows + (offset * format.row_size), .format = format};
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
  // referenced table).
  const auto write_stable_string = [&](const RowView& row, const pmr_string& value) {
    if (write_inline(row, value.c_str(), value.size())) {
      row.set_string_ptr(string_col_index, value.c_str());
    }
  };

  const auto write_volatile_string = [&](const RowView& row, const pmr_string& value) {
    if (write_inline(row, value.c_str(), value.size())) {
      const auto length = value.size();
      auto* const str_copy = static_cast<char*>(string_arena.allocate(length + 1));
      std::memcpy(str_copy, value.c_str(), length);
      str_copy[length] = '\0';
      row.set_string_ptr(string_col_index, str_copy);
    }
  };

  auto chunk_offset = size_t{0};

  // `needs_copy` is a `std::bool_constant` (see `with_string_segment_iterate`). It is set depending on, so only the
  // taken branch is compiled for any given segment kind and no per-row check remains.
  const auto callback = [&](const auto& str_value, const bool is_null, const auto needs_copy) {
    const auto row = row_at(chunk_offset);
    ++chunk_offset;

    if (is_null) {
      row.set_null_bitmap(row.null_bitmap() | null_mask_bit);
      return;
    }

    if constexpr (std::is_same_v<decltype(needs_copy), std::true_type>) {
      write_volatile_string(row, str_value);
    } else {
      write_stable_string(row, str_value);
    }
  };

  const auto needs_copy = with_string_segment_iterate<pmr_string>(segment, callback);

  materialized.string_pointer_needs_copy.push_back(needs_copy);
}

// TODO(@anyone): sort string_columns to be last in groupby columns?
void materialize_rows(const RowFormat& format, const std::shared_ptr<const Chunk>& chunk,
                      const std::vector<ColumnID>& groupby_column_ids, MaterializedRows& materialized) {
  const auto chunk_size = chunk->size();

  // The row buffer and string arena are owned by the caller and reused across chunks.
  materialized.row_count = chunk_size;
  materialized.string_pointer_needs_copy.clear();
  materialized.string_arena.release();
  auto* const rows = materialized.rows.get();
  // `row_size` is a multiple of `ROW_ALIGNMENT`, so an aligned buffer start makes every row aligned.
  DebugAssert(reinterpret_cast<uintptr_t>(rows) % ROW_ALIGNMENT == 0, "Row buffer is not sufficiently aligned.");
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
        materialize_string_column(format, segment, group_by_column_index, string_col_index, null_mask_bit,
                                  materialized);
        ++string_col_index;
      } else {
        auto chunk_offset = size_t{0};
        segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
          const auto row = RowView{.base = rows + (chunk_offset * format.row_size), .format = format};
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

constexpr auto TICKET_RANGE_LENGTH = uint64_t{1} << 10U;  // 1024 tickets per thread

// Copies a materialized key row into `arena` so it outlives the transient materialize buffer. Long strings that still
// live in the per-chunk arena are copied alongside it. Strings that already point at stable source memory (value,
// dictionary segments or referenced ones) are left as is.
static GroupKey promote_key_row(const RowFormat& format, const uint8_t* const row_ptr, const uint64_t row_hash,
                                const MaterializedRows& materialized, std::pmr::monotonic_buffer_resource& arena) {
  auto* const row_copy = static_cast<uint8_t*>(arena.allocate(format.row_size, ROW_ALIGNMENT));
  std::memcpy(row_copy, row_ptr, format.row_size);

  const auto copy_view = RowView{.base = row_copy, .format = format};
  const auto string_col_count = copy_view.string_col_count();
  for (auto string_col_index = size_t{0}; string_col_index < string_col_count; ++string_col_index) {
    if (!materialized.string_pointer_needs_copy[string_col_index]) {
      continue;
    }
    const auto* const str_ptr = copy_view.string_ptr(string_col_index);
    if (str_ptr != nullptr) {
      const auto length = std::strlen(str_ptr) + 1;
      auto* const arena_str = static_cast<char*>(arena.allocate(length));
      std::memcpy(arena_str, str_ptr, length);
      copy_view.set_string_ptr(string_col_index, arena_str);
    }
  }
  return GroupKey{.row = row_copy, .hash = row_hash};
}

// Returns the number of unused tickets, after removing all trailing gaps from the fuzzy ticketing.
template <typename HashTable>
// NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,hicpp-avoid-c-arrays,modernize-avoid-c-arrays): (`GroupKeyDataBase`)
static uint64_t remove_fuzzy_ticketing_gaps(std::vector<std::pair<uint64_t, uint64_t>>& ticket_gaps,
                                            std::unique_ptr<uint64_t[]>& tickets, const uint64_t row_count,
                                            HashTable& global_hash_table, bool ignore_hash_map = false) {
  const auto job_count = ticket_gaps.size();

  // Sort gaps to prefix-sum the unused ticket counts.
  std::ranges::sort(ticket_gaps);
  auto sorted_gap_starts = std::vector<uint64_t>(job_count);
  // `unused_before_gap[i]` is the total number of unused tickets in all gaps ordered before gap `i`.
  auto unused_before_gap = std::vector<uint64_t>(job_count + 1, 0);
  for (auto gap_index = size_t{0}; gap_index < job_count; ++gap_index) {
    sorted_gap_starts[gap_index] = ticket_gaps[gap_index].first;
    unused_before_gap[gap_index + 1] =
        unused_before_gap[gap_index] + (ticket_gaps[gap_index].second - ticket_gaps[gap_index].first);
  }

  // A used ticket must be shifted down by the total size of all gaps preceeding it.
  const auto compact = [&](const uint64_t ticket) {
    const auto gaps_below =
        static_cast<size_t>(std::ranges::lower_bound(sorted_gap_starts, ticket) - sorted_gap_starts.begin());
    return ticket - unused_before_gap[gaps_below];
  };

  // Compact the per-row tickets in parallel and the map's stored tickets (a single pass) with the same transform, so
  // the tickets in `tickets` and in the hash table agree and both form a dense [0, group_count) range.
  auto compaction_jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  compaction_jobs.reserve(job_count);
  const auto rows_per_job = row_count / job_count;
  for (auto job_id = uint32_t{0}; job_id < job_count; ++job_id) {
    compaction_jobs.emplace_back(std::make_shared<JobTask>([&, job_id] {
      const auto start = job_id * rows_per_job;
      const auto end = (job_id == job_count - 1) ? row_count : start + rows_per_job;
      for (auto row_index = size_t{start}; row_index < end; ++row_index) {
        tickets[row_index] = compact(tickets[row_index]);
      }
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(compaction_jobs);

  // When we only have a single groupby-column, the global hash table is not used for emitting the result. So we do not
  // need to remap it.
  if (!ignore_hash_map) {
    global_hash_table.remap_tickets(compact);
  }

  return unused_before_gap[job_count];
}

// Multi-column path that materialize each row's group-by key into a packed row format, hashes it and probes
// a shared, lock-free `ConcurrentTicketMap`. Each thread hands out tickets from its own claimed range, so the shared
// range 'cursor' is fought over once per range rather than once per group (= fuzzy ticketing). The trailing unused
// tickets of each thread's last range are compacted afterwards so the final tickets form a dense [0, group_count)
// range.
static std::shared_ptr<GroupKeyData> compute_groups_multi_column(const std::vector<ColumnID>& groupby_column_ids,
                                                                 const std::shared_ptr<const Table>& input_table) {
  const auto row_format = create_row_format(input_table->column_definitions(), groupby_column_ids);
  const auto chunk_count = input_table->chunk_count();

  // Guard the offset computation below, which would underflow `chunk_count - 1` on an empty table.
  if (chunk_count == 0) {
    auto group_key_data = std::make_shared<GroupKeyData>(row_format, 0);
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,hicpp-avoid-c-arrays,modernize-avoid-c-arrays)
    group_key_data->tickets = std::make_unique_for_overwrite<uint64_t[]>(0);
    group_key_data->group_count = 0;
    group_key_data->has_hash_table = true;
    return group_key_data;
  }

  auto max_chunk_size = size_t{0};
  auto ticket_offsets = std::vector<uint64_t>(chunk_count, 0);
  // Compute the starting offset into the ticket vector for each chunk.
  {
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count - 1; ++chunk_id) {
      const auto chunk_size = input_table->get_chunk(chunk_id)->size();
      ticket_offsets[chunk_id + 1] = ticket_offsets[chunk_id] + static_cast<uint64_t>(chunk_size);
      max_chunk_size = std::max(max_chunk_size, static_cast<size_t>(chunk_size));
    }
    max_chunk_size =
        std::max(max_chunk_size, static_cast<size_t>(input_table->get_chunk(ChunkID{chunk_count - 1})->size()));
  }

  const auto row_count = input_table->row_count();
  const auto estimated_groups =
      estimate_group_count_multi_column(row_format, groupby_column_ids, input_table, max_chunk_size);

  auto group_key_data = std::make_shared<GroupKeyData>(row_format, estimated_groups);
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,hicpp-avoid-c-arrays,modernize-avoid-c-arrays)
  group_key_data->tickets = std::make_unique_for_overwrite<uint64_t[]>(row_count);
  auto& global_hash_table = group_key_data->global_hash_table;

  // Fuzzy ticketing: each thread claims a disjoint [n * TICKET_RANGE_LENGTH, (n + 1) * TICKET_RANGE_LENGTH) range from
  // this shared cursor and only touches it once per range, not once per group.
  auto next_ticket_range_start = std::atomic<uint64_t>{0};

  const auto process_chunk = [&](const ChunkID chunk_id, MaterializedRows& materialized,
                                 std::pmr::monotonic_buffer_resource& arena, uint64_t& next_ticket,
                                 uint64_t& ticket_range_end) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    materialize_rows(row_format, chunk, groupby_column_ids, materialized);

    const auto promote_key = [&](const GroupKey& key) {
      return promote_key_row(row_format, key.row, key.hash, materialized, arena);
    };

    global_hash_table.register_prober();
    auto* row_ptr = materialized.rows.get();
    const auto chunk_start = ticket_offsets[chunk_id];
    for (auto chunk_offset = size_t{0}; chunk_offset < materialized.row_count; ++chunk_offset) {
      const auto row_view = RowView{.base = row_ptr, .format = row_format};
      const auto row_hash = compute_hash(row_view.key_bytes(), row_format.key_length);

      const auto ticket = global_hash_table.try_emplace(GroupKey{.row = row_view.key_bytes(), .hash = row_hash},
                                                        next_ticket, promote_key);

      // If the ticket was inserted, then increment the ticket. If our range is exhausted, claim a new one.
      if (ticket == next_ticket) {
        ++next_ticket;
        if (next_ticket >= ticket_range_end) {
          next_ticket = next_ticket_range_start.fetch_add(TICKET_RANGE_LENGTH);
          ticket_range_end = next_ticket + TICKET_RANGE_LENGTH;
        }
      }

      group_key_data->tickets[chunk_start + chunk_offset] = ticket;
      row_ptr += row_format.row_size;
    }

    global_hash_table.unregister_prober();
  };

  // One arena per grouping thread. Because each thread only ever allocates from its own arena, copying newly
  // seen group keys needs no locking.
  const auto job_count = std::min<size_t>(Hyrise::get().topology.num_cpus(), chunk_count);
  auto& arenas = group_key_data->key_arenas;
  arenas.reserve(job_count);
  for (auto arena_id = size_t{0}; arena_id < job_count; ++arena_id) {
    arenas.emplace_back(std::make_unique<std::pmr::monotonic_buffer_resource>());
  }

  // When a thread stops it leaves a trailing gap [next_ticket, ticket_range_end) of unused tickets at the end
  // of its last claimed range. We record these per-thread gaps here and compact them out below so the final tickets
  // form a contiguous [0, group_count) range.
  auto ticket_gaps = std::vector<std::pair<uint64_t, uint64_t>>(job_count);
  auto next_chunk_id = std::atomic<uint32_t>{0};
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(job_count);
  for (auto job_id = uint32_t{0}; job_id < job_count; ++job_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&process_chunk, &row_format, &arenas, &next_ticket_range_start,
                                                 &ticket_gaps, max_chunk_size, chunk_count, &next_chunk_id, job_id]() {
      auto materialized = MaterializedRows{};
      // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,hicpp-avoid-c-arrays,modernize-avoid-c-arrays)
      materialized.rows = std::make_unique<uint8_t[]>(max_chunk_size * row_format.row_size);
      auto& arena = *arenas[job_id];

      auto next_ticket = next_ticket_range_start.fetch_add(TICKET_RANGE_LENGTH);
      auto ticket_range_end = next_ticket + TICKET_RANGE_LENGTH;
      while (true) {
        const auto chunk_id = next_chunk_id.fetch_add(1);
        if (chunk_id >= chunk_count) {
          break;
        }
        process_chunk(ChunkID{chunk_id}, materialized, arena, next_ticket, ticket_range_end);
      }
      ticket_gaps[job_id] = {next_ticket, ticket_range_end};
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  // The number of distinct groups is the total claimed ticket space (the top of the highest range) minus the unused
  // tickets in all trailing gaps. Unlike the single-column path there is no separate NULL group: NULLs are part of the
  // row's null bitmap and hash into ordinary group keys.
  group_key_data->group_count =
      next_ticket_range_start.load(std::memory_order_relaxed) -
      remove_fuzzy_ticketing_gaps(ticket_gaps, group_key_data->tickets, row_count, global_hash_table);

  group_key_data->has_hash_table = true;
  return group_key_data;
}

// Fast path for a single non-string group-by column. Here the value is the key, so we do not need to materialize rows.
static std::shared_ptr<GroupKeyData> compute_groups_single_column(const ColumnID groupby_column_id,
                                                                  const std::shared_ptr<const Table>& input_table) {
  const auto data_type = input_table->column_data_type(groupby_column_id);
  const auto chunk_count = input_table->chunk_count();

  auto return_group_key_data = std::make_shared<GroupKeyData>(RowFormat{}, 0);

  resolve_data_type(data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
      Fail("The single-column fast path is not used for string columns.");
    } else {
      // The value is the key, so no rows are materialized: the hot loop only populates a `value -> ticket` map and
      // emits tickets. Fuzzy ticketing is used as well: Each thread hands out tickets from its own claimed range.
      // NULL is a group of its own, but the threads cannot agree on a ticket for it without synchronizing. We
      // therefore reserve ticket 0 for it up front whenever the column can contain NULLs, so every thread can emit it
      // without coordination. If no NULL shows up, the reserved ticket is compacted away like any other unused ticket
      // (see the gap we register below).
      const auto reserves_null_ticket = input_table->column_is_nullable(groupby_column_id);
      constexpr auto NULL_TICKET = uint64_t{0};
      auto has_null = std::atomic<bool>{false};

      // Row index of each chunk's first row, so `(chunk_id, chunk_offset)` maps into the flat `tickets` vector.
      auto ticket_offsets = std::vector<uint64_t>(chunk_count, 0);
      for (auto chunk_id = ChunkID{1}; chunk_id < chunk_count; ++chunk_id) {
        const auto previous_chunk_id = ChunkID{chunk_id - 1};
        ticket_offsets[chunk_id] = ticket_offsets[previous_chunk_id] +
                                   static_cast<uint64_t>(input_table->get_chunk(previous_chunk_id)->size());
      }

      const auto estimated_groups = estimate_group_count_single_column<ColumnDataType>(groupby_column_id, input_table);
      // The single-column fast path carries no hash table (`has_hash_table` stays false), so `GroupKeyData` here is
      // only a tickets + group-count carrier and its `row_format`/`global_hash_table` stay unused. Grouping runs
      // against the local `value_to_ticket` below, so the carrier's map is sized for nothing rather than for
      // `estimated_groups`.
      auto group_key_data = std::make_shared<GroupKeyData>(RowFormat{}, 0);
      auto& tickets = group_key_data->tickets;
      // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,hicpp-avoid-c-arrays,modernize-avoid-c-arrays)
      tickets = std::make_unique_for_overwrite<uint64_t[]>(input_table->row_count());

      if (chunk_count == 0) {
        group_key_data->group_count = 0;
        group_key_data->has_hash_table = true;  // empty table, no groups, but the hash table is trivially built
        return_group_key_data.swap(group_key_data);
        return;
      }

      auto value_to_ticket = ConcurrentTicketMap<ColumnDataType>(estimated_groups);
      auto next_ticket_range_start = std::atomic<uint64_t>{reserves_null_ticket ? uint64_t{1} : uint64_t{0}};

      const auto process_chunk = [&](const ChunkID chunk_id, uint64_t& next_ticket, uint64_t& ticket_range_end) {
        const auto& chunk = input_table->get_chunk(chunk_id);
        const auto& segment = chunk->get_segment(groupby_column_id);
        const auto chunk_start = ticket_offsets[chunk_id];
        auto chunk_offset = size_t{0};
        value_to_ticket.register_prober();

        segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
          auto current_ticket = NULL_TICKET;
          if (position.is_null()) {
            DebugAssert(reserves_null_ticket, "Found a NULL in a column that the table declares as non-nullable.");
            has_null.store(true, std::memory_order_relaxed);
          } else {
            current_ticket = value_to_ticket.try_emplace(position.value(), next_ticket);
            if (current_ticket == next_ticket) {
              ++next_ticket;
              if (next_ticket >= ticket_range_end) {
                next_ticket = next_ticket_range_start.fetch_add(TICKET_RANGE_LENGTH);
                ticket_range_end = next_ticket + TICKET_RANGE_LENGTH;
              }
            }
          }
          tickets[chunk_start + chunk_offset] = current_ticket;
          chunk_offset++;
        });

        value_to_ticket.unregister_prober();
      };

      const auto job_count = std::min<size_t>(Hyrise::get().topology.num_cpus(), chunk_count);

      // Threads steal chunks from a shared cursor and hand out tickets from their own ranges. When a thread stops it
      // usually leaves a trailing gap [next_ticket, ticket_range_end) of unused tickets at the end of its last claimed
      // range. We record these per-thread gaps here and compact them out below.
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

      if (reserves_null_ticket && !has_null.load(std::memory_order_relaxed)) {
        // The column is nullable, but holds no NULL: hand the reserved ticket to the compaction as an unused range so
        // that it neither ends up in `group_count` nor leaves a hole in the [0, group_count) ticket range.
        ticket_gaps.emplace_back(NULL_TICKET, NULL_TICKET + 1);
      }

      group_key_data->group_count =
          next_ticket_range_start.load(std::memory_order_relaxed) -
          remove_fuzzy_ticketing_gaps(ticket_gaps, tickets, input_table->row_count(), value_to_ticket, true);
      return_group_key_data.swap(group_key_data);
    }
  });

  return return_group_key_data;
}

std::shared_ptr<GroupKeyData> compute_groups(const std::vector<ColumnID>& groupby_column_ids,
                                             const std::shared_ptr<const Table>& input_table) {
  // We do not support non-trivial types in the concurrent HashMap, so. we fall back to the multi-column path for
  // single-column group-bys on strings.
  if (groupby_column_ids.size() == 1 && input_table->column_data_type(groupby_column_ids[0]) != DataType::String) {
    // For a single column, we can use the concurrent ticketing path, which is faster than the multi-column path.
    const auto column_id = groupby_column_ids[0];
    return compute_groups_single_column(column_id, input_table);
  }
  return compute_groups_multi_column(groupby_column_ids, input_table);
}

}  // namespace hyrise
