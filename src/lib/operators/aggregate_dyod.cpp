#include "aggregate_dyod.hpp"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <format>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "aggregate_dyod_utils/aggregate_helpers.hpp"
#include "aggregate_dyod_utils/ticketing.hpp"
#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "hyrise.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/aggregate/window_function_traits.hpp"
#include "operators/operator_state.hpp"
#include "resolve_type.hpp"
#include "scheduler/immediate_execution_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "storage/chunk.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

// Threshold that decides how the group-by output columns are built. When the input has at least this many rows per
// group (low cardinality), each group-by column is materialized by reading every group's value directly from the
// ticketing hash table.
constexpr auto GROUPBY_HASH_TABLE_MIN_ROWS_PER_GROUP = size_t{4};

// Slots of the grouping hash table that one job reads when a group-by output column is built from it.
constexpr auto GROUPBY_HASH_TABLE_SLOTS_PER_JOB = size_t{1} << 16;

// State of one worker of the no-group-by aggregation: one aggregation state per aggregate of the operator. The states
// are created lazily on the worker's first chunk, as `OperatorSharedState` default-constructs the worker states, which
// therefore cannot know the operator's aggregates.
struct NoGroupByWorkerState : public Noncopyable {
  void merge(NoGroupByWorkerState& other) {
    if (other.aggregate_states.empty()) {
      // This worker was handed a state, but never processed a chunk.
      return;
    }
    if (aggregate_states.empty()) {
      aggregate_states = std::move(other.aggregate_states);
      return;
    }

    const auto aggregate_count = aggregate_states.size();
    for (auto aggregate_id = size_t{0}; aggregate_id < aggregate_count; ++aggregate_id) {
      // Aggregates that need no per-chunk work (see `AggregateInfo::counts_all_rows`) have no state.
      const auto& aggregate_state = aggregate_states[aggregate_id];
      const auto& current_aggregate_info = aggregate_info[aggregate_id];
      const auto window_function = current_aggregate_info.window_function;

      resolve_data_type(current_aggregate_info.data_type, [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;
        resolve_window_function(window_function, [&](const auto window_function_t) {
          const auto window_function = decltype(window_function_t)::value;
          auto& state = *std::static_pointer_cast<BaseAggregateState<ColumnDataType, window_function>>(aggregate_state);
          auto& other_state = *std::static_pointer_cast<BaseAggregateState<ColumnDataType, window_function>>(
              other.aggregate_states[aggregate_id]);

          state.merge(other_state);
        });
      });
    }
  }

  std::vector<std::shared_ptr<void>> aggregate_states;
  std::vector<AggregateInfo> aggregate_info;
};

AggregateDYOD::AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                             const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                             const std::vector<ColumnID>& groupby_column_ids)
    : AbstractAggregateOperator(input_operator, aggregates, groupby_column_ids) {}

const std::string& AggregateDYOD::name() const {
  static const auto name = std::string{"AggregateDYOD"};
  return name;
}

// Incrementally computable aggregates (MIN/MAX/SUM/AVG/COUNT), indexed per group. A group with no contributing
// (non-NULL) value yields NULL, except COUNT which yields 0.
template <typename ColumnDataType, typename AggregateType, WindowFunction window_function>
std::pair<ChunkedVector<AggregateType>, ChunkedVector<bool>> _aggregate_grouped(
    const uint64_t* const tickets, const size_t group_count, const std::shared_ptr<const Table>& input_table,
    const ColumnID input_column_id) {
  const auto aggregate_function =
      WindowFunctionBuilder<ColumnDataType, AggregateType, window_function>().get_aggregate_function();
  auto values = ChunkedVector<AggregateType>(group_count);

  // Only AVG needs a per-group count of contributing (non-NULL) values, for its final division. MIN/MAX/SUM detect
  // their first contributing value via `nulls`, and COUNT accumulates directly into `values`, so neither allocates it.
  auto value_counts = std::vector<size_t>(window_function == WindowFunction::Avg ? group_count : 0, 0);
  auto nulls = ChunkedVector<bool>(group_count, window_function != WindowFunction::Count);

  const auto chunk_count = input_table->chunk_count();
  auto row_index = uint32_t{0};  // global row index across chunks, used to look up the group ticket in `tickets`

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    const auto& aggregate_segment = chunk->get_segment(input_column_id);

    with_string_segment_iterate<ColumnDataType>(
        aggregate_segment, [&](const auto& value, const bool is_null, const auto needs_copy) {
          if (is_null) {
            ++row_index;
            return;
          }
          const auto ticket = tickets[row_index++];
          if constexpr (window_function == WindowFunction::Avg) {
            aggregate_function(value, value_counts[ticket], values[ticket]);
            ++value_counts[ticket];
            nulls[ticket] = false;
          } else if constexpr (window_function == WindowFunction::Count) {
            values[ticket]++;
          } else {
            // MIN/MAX/SUM: the aggregate function only needs to know whether this is the group's first contributing
            // value (it checks `aggregate_count == 0`). `nulls[ticket]` is still true until that first value, so it
            // doubles as the first-seen flag and we avoid maintaining a separate per-group count.
            aggregate_function(value, nulls[ticket] ? size_t{0} : size_t{1}, values[ticket]);
            nulls[ticket] = false;
          }
        });
  }

  // We have aggregated all values per group, but need to apply some 'post-processing' to finalize the results.
  if constexpr (window_function == WindowFunction::Avg && std::is_arithmetic_v<AggregateType>) {
    for (auto ticket = size_t{0}; ticket < group_count; ++ticket) {
      if (value_counts[ticket] != 0) {
        values[ticket] = values[ticket] / static_cast<AggregateType>(value_counts[ticket]);
      }
    }
  }
  return {std::move(values), std::move(nulls)};
}

constexpr auto MAX_LOCAL_HASH_TABLE_SIZE = size_t{1 << 12};  // 4096 entries

template <typename ColumnDataType, typename AggregateType, WindowFunction window_function, typename AggregateState,
          bool force_spill = false>
void spill_local_hash_table_to_global_aggregate_result(
    boost::unordered_flat_map<uint64_t, AggregateState>& local_hash_table,
    std::shared_ptr<std::vector<AggregateState>>& global_aggregate_result,
    std::vector<std::atomic_flag>& intermediate_result_atomics) {
  if constexpr (force_spill) {
    for (auto& [ticket, state] : local_hash_table) {
      while (intermediate_result_atomics[ticket].test_and_set()) {
        // Spin until the atomic flag is cleared by the thread that is currently merging this ticket's state.
      }
      global_aggregate_result->operator[](ticket).merge(state);
      intermediate_result_atomics[ticket].clear();
    }
    local_hash_table.clear();
  } else {
    boost::unordered::erase_if(local_hash_table, [&](auto& entry) {
      auto& [ticket, state] = entry;
      if (intermediate_result_atomics[ticket].test_and_set()) {
        return false;  // we do not spill and skip this entry.
      }
      global_aggregate_result->operator[](ticket).merge(state);
      intermediate_result_atomics[ticket].clear();
      return true;  // sucessfully spilled - erase the entry.
    });
  }
}

// COUNT(*) does not reference an input column. It counts all rows per group (NULLs included), so every row of the
// chunk contributes to its group's count. `RegularAggregateState::finalize` derives COUNT from `value_count` alone.
template <typename ColumnDataType, typename AggregateType, WindowFunction window_function, typename AggregateState>
void _accumulate_count_star_concurrently(const uint64_t* const tickets, const size_t row_index, const size_t chunk_size,
                                         boost::unordered_flat_map<uint64_t, AggregateState>& local_hash_table,
                                         std::shared_ptr<std::vector<AggregateState>>& global_aggregate_result,
                                         std::vector<std::atomic_flag>& intermediate_result_atomics) {
  for (auto chunk_offset = size_t{0}; chunk_offset < chunk_size; ++chunk_offset) {
    ++local_hash_table[tickets[row_index + chunk_offset]].value_count;

    if (local_hash_table.size() >= MAX_LOCAL_HASH_TABLE_SIZE) {
      spill_local_hash_table_to_global_aggregate_result<ColumnDataType, AggregateType, window_function, AggregateState>(
          local_hash_table, global_aggregate_result, intermediate_result_atomics);
    }
  }
}

template <typename ColumnDataType, typename AggregateType, WindowFunction window_function, typename AggregateState>
void _accumulate_concurrently(const uint64_t* const tickets, uint32_t row_index,
                              const std::shared_ptr<AbstractSegment> aggregate_segment,
                              boost::unordered_flat_map<uint64_t, AggregateState>& local_hash_table,
                              std::shared_ptr<std::vector<AggregateState>>& global_aggregate_result,
                              std::vector<std::atomic_flag>& intermediate_result_atomics) {
  auto hash_table_size = local_hash_table.size();
  const auto aggregate_function =
      WindowFunctionBuilder<ColumnDataType, AggregateType, window_function>().get_aggregate_function();
  with_string_segment_iterate<ColumnDataType>(
      aggregate_segment, [&](auto& value, const bool is_null, const auto needs_copy) {
        if (is_null) {
          ++row_index;
          return;
        }
        const auto ticket = tickets[row_index++];

        if (local_hash_table.find(ticket) == local_hash_table.end()) {
          local_hash_table[ticket] = AggregateState();
          hash_table_size += 1;
        }

        if constexpr (window_function != WindowFunction::StandardDeviationSample &&
                      window_function != WindowFunction::CountDistinct) {
          aggregate_function(value, local_hash_table[ticket].value_count, local_hash_table[ticket].accumulator);
          local_hash_table[ticket].value_count += 1;
        } else if constexpr (window_function == WindowFunction::CountDistinct) {
          local_hash_table[ticket].distinct_values.insert(value);
        } else if constexpr (window_function == WindowFunction::StandardDeviationSample) {
          aggregate_function(value, size_t{0}, local_hash_table[ticket].standard_deviation);
        }

        if (hash_table_size >= MAX_LOCAL_HASH_TABLE_SIZE) {
          spill_local_hash_table_to_global_aggregate_result<ColumnDataType, AggregateType, window_function,
                                                            AggregateState, true>(
              local_hash_table, global_aggregate_result, intermediate_result_atomics);
          hash_table_size = local_hash_table.size();
        }
      });
}

// COUNT(DISTINCT): number of distinct non-NULL values. Never NULL (0 for an all-NULL group).
template <typename ColumnDataType>
ChunkedVector<int64_t> _count_distinct_grouped(const uint64_t* const tickets, const size_t group_count,
                                               const std::shared_ptr<const Table>& input_table,
                                               const ColumnID input_column_id) {
  auto distinct_values = std::vector<std::unordered_set<ColumnDataType>>(group_count);

  const auto chunk_count = input_table->chunk_count();
  auto row_index = uint32_t{0};  // global row index across chunks, used to look up the group ticket in `tickets`

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    const auto& aggregate_segment = chunk->get_segment(input_column_id);
    with_string_segment_iterate<ColumnDataType>(aggregate_segment,
                                                [&](const auto& value, const bool is_null, const auto needs_copy) {
                                                  if (is_null) {
                                                    ++row_index;
                                                    return;
                                                  }
                                                  distinct_values[tickets[row_index++]].insert(value);
                                                });
  }

  auto values = ChunkedVector<int64_t>(group_count);
  for (auto i = size_t{0}; i < group_count; ++i) {
    values[i] = static_cast<int64_t>(distinct_values[i].size());
  }
  return values;
}

// ANY: the first value seen per group, NULL included (The value is passed through. All-NULL groups stay).
template <typename ColumnDataType>
std::pair<ChunkedVector<ColumnDataType>, ChunkedVector<bool>> _any_grouped(
    const uint64_t* const tickets, const size_t group_count, const std::shared_ptr<const Table>& input_table,
    const ColumnID input_column_id) {
  auto seen = std::vector<bool>(group_count, false);
  auto values = ChunkedVector<ColumnDataType>(group_count);
  auto nulls = ChunkedVector<bool>(group_count, false);

  const auto chunk_count = input_table->chunk_count();
  auto row_index = uint32_t{0};  // global row index across chunks, used to look up the group ticket in `tickets`

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    const auto& aggregate_segment = chunk->get_segment(input_column_id);

    with_string_segment_iterate<ColumnDataType>(aggregate_segment,
                                                [&](const auto& value, const bool is_null, const auto needs_copy) {
                                                  const auto index = tickets[row_index++];
                                                  if (seen[index]) {
                                                    return;
                                                  }
                                                  seen[index] = true;
                                                  if (is_null) {
                                                    nulls[index] = true;
                                                  } else {
                                                    values[index] = value;
                                                  }
                                                });
  }
  return {std::move(values), std::move(nulls)};
}

// Builds one group-by output column by reading each group's representative value directly from its distinct key row in
// the grouping hash table. Every group appears exactly once as a hash-table key, so a single const pass over the table
// (`group_count` entries) yields all values without re-scanning the source column. Preferred for low-cardinality
// group-bys, where there are far fewer groups than input rows; otherwise the sequential scan in `_any_grouped` wins.
//
// `groupby_index` is the column's position among the group-by columns (its slot in the row's null bitmap and column
// offsets); `string_col_index` is its position among the string group-by columns (its heap string-pointer slot).
// `nulls` is written through `nulls[ticket] = true`, so it takes both a `ChunkedVector<bool>` and a byte-per-group
// vector (see `_any_grouped_chunk` for why a concurrent build cannot use a packed bitmap).
template <typename ColumnDataType, typename NullContainer>
void _write_groupby_value_from_key_row(const GroupKey& key, const uint64_t ticket, const RowFormat& format,
                                       const size_t groupby_index, const size_t string_col_index,
                                       ChunkedVector<ColumnDataType>& values, NullContainer& nulls) {
  const auto row_view = RowView{key.row, format};
  const auto null_mask_bit = uint64_t{1} << groupby_index;

  // `stores_nulls` is only false when no group-by column is nullable, so `null_bitmap()` is only read when present.
  if (format.stores_nulls && (row_view.null_bitmap() & null_mask_bit)) {
    nulls[ticket] = true;
    return;
  }

  if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
    const auto length = row_view.string_length(groupby_index);
    if (length <= PREFIX_LENGTH) {
      // Short string: the whole value lives inline in the prefix.
      values[ticket] = pmr_string{row_view.string_prefix(groupby_index), length};
    } else {
      // Long string: the full, null-terminated value lives at the row's heap pointer.
      values[ticket] = pmr_string{row_view.string_ptr(string_col_index)};
    }
  } else {
    values[ticket] = row_view.read_value<ColumnDataType>(groupby_index);
  }
}

template <typename ColumnDataType, bool Concurrent>
std::pair<ChunkedVector<ColumnDataType>, ChunkedVector<bool>> _groupby_from_hash_table(
    const GroupKeyData<Concurrent>& group_key_data, const size_t group_count, const size_t groupby_index,
    const size_t string_col_index) {
  const auto& format = group_key_data.row_format;
  const auto& hash_table = group_key_data.global_hash_table;
  auto values = ChunkedVector<ColumnDataType>(group_count);
  auto nulls = ChunkedVector<bool>(group_count, false);

  const auto process_entry = [&](const GroupKey& key, const uint64_t ticket) {
    _write_groupby_value_from_key_row(key, ticket, format, groupby_index, string_col_index, values, nulls);
  };

  if constexpr (Concurrent) {
    // `ConcurrentTicketMap::for_each` hands the stored key and ticket directly (no `entry` pair). Called after the
    // grouping jobs have joined, so a plain single-threaded pass is safe.
    hash_table.for_each(process_entry);
  } else {
    for (auto it = hash_table.cbegin(); it != hash_table.cend(); ++it) {
      process_entry(it->first, it->second);
    }
  }

  return {std::move(values), std::move(nulls)};
}

// Builds the part of a group-by output column that a range of the grouping hash table's slots covers. Every group is
// stored in exactly one slot, so slot ranges write disjoint output slots and need no synchronization.
template <typename ColumnDataType, typename NullContainer>
void _groupby_from_hash_table_slots(const GroupKeyData<true>& group_key_data, const size_t groupby_index,
                                    const size_t string_col_index, const size_t first_slot, const size_t end_slot,
                                    ChunkedVector<ColumnDataType>& values, NullContainer& nulls) {
  const auto& format = group_key_data.row_format;
  group_key_data.global_hash_table.for_each_slot_range(
      first_slot, end_slot, [&](const GroupKey& key, const uint64_t ticket) {
        _write_groupby_value_from_key_row(key, ticket, format, groupby_index, string_col_index, values, nulls);
      });
}

// Builds the part of a group-by output column that one chunk of the source column covers. Every row of a group carries
// the same group-by value, so the first row seen per group wins; `seen` claims a group for exactly one job, so that no
// two jobs write the same output slot. NULLs are recorded as one byte per group rather than in a packed
// `ChunkedVector<bool>`: the jobs write scattered tickets, and neighbouring bits of a packed bitmap share a word,
// which cannot be written concurrently. The caller folds the bytes into the output bitmap once the jobs joined.
template <typename ColumnDataType>
void _any_grouped_chunk(const uint64_t* const tickets, const std::shared_ptr<const Table>& input_table,
                        const ColumnID input_column_id, const ChunkID chunk_id, const size_t first_row_index,
                        std::vector<std::atomic_flag>& seen, ChunkedVector<ColumnDataType>& values,
                        std::vector<uint8_t>& nulls) {
  auto row_index = first_row_index;  // global row index, used to look up the group ticket in `tickets`
  const auto& groupby_segment = input_table->get_chunk(chunk_id)->get_segment(input_column_id);

  with_string_segment_iterate<ColumnDataType>(
      groupby_segment, [&](const auto& value, const bool is_null, const auto needs_copy) {
        const auto ticket = tickets[row_index++];
        // The claim only needs to be atomic; the written values are published by the jobs joining.
        if (seen[ticket].test(std::memory_order_relaxed) || seen[ticket].test_and_set(std::memory_order_relaxed)) {
          return;  // Another row of this group already provided the value.
        }
        if (is_null) {
          nulls[ticket] = 1;
        } else {
          values[ticket] = value;
        }
      });
}

// STDDEV: NULL for groups with fewer than two contributing values.
template <typename ColumnDataType>
std::pair<ChunkedVector<double>, ChunkedVector<bool>> _standard_deviation_sample_grouped(
    const uint64_t* const tickets, const size_t group_count, const std::shared_ptr<const Table>& input_table,
    const ColumnID input_column_id) {
  static_assert(std::is_arithmetic_v<ColumnDataType>, "StandardDeviationSample is only defined on arithmetic types.");
  const auto aggregate_function =
      WindowFunctionBuilder<ColumnDataType, double, WindowFunction::StandardDeviationSample>().get_aggregate_function();
  auto accumulators = std::vector<StandardDeviationSampleData>(group_count);

  const auto chunk_count = input_table->chunk_count();
  auto row_index = uint32_t{0};  // global row index across chunks, used to look up the group ticket in `tickets`
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    const auto& aggregate_segment = chunk->get_segment(input_column_id);
    segment_iterate<ColumnDataType>(*aggregate_segment, [&](const auto& position) {
      if (position.is_null()) {
        ++row_index;
        return;
      }
      // Welford's algorithm tracks its own count in `accumulator[0]`, so the `aggregate_count` argument is unused.
      aggregate_function(std::move(position.value()), size_t{0}, accumulators[tickets[row_index++]]);
    });
  }

  auto values = ChunkedVector<double>(group_count);
  auto nulls = ChunkedVector<bool>(group_count, false);
  for (auto i = size_t{0}; i < group_count; ++i) {
    if (accumulators[i][0] < 2) {
      nulls[i] = true;
    } else {
      values[i] = accumulators[i][3];
    }
  }
  return {std::move(values), std::move(nulls)};
}

std::shared_ptr<const Table> AggregateDYOD::no_groupby_aggregate() {
  const auto input_table = left_input_table();
  const auto aggregate_count = _aggregates.size();

  auto aggregate_infos = std::vector<AggregateInfo>(aggregate_count);
  for (auto aggregate_id = size_t{0}; aggregate_id < aggregate_count; ++aggregate_id) {
    const auto& aggregate = _aggregates[aggregate_id];
    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto input_column_id = pqp_column.column_id;

    auto& info = aggregate_infos[aggregate_id];
    info.input_column_id = input_column_id;
    info.data_type =
        input_column_id == INVALID_COLUMN_ID ? DataType::Long : input_table->column_data_type(input_column_id);
    info.counts_all_rows = aggregate->window_function == WindowFunction::Count &&
                           (input_column_id == INVALID_COLUMN_ID || !input_table->column_is_nullable(input_column_id));
    info.window_function = aggregate->window_function;
    info.is_count_star = aggregate->window_function == WindowFunction::Count && input_column_id == INVALID_COLUMN_ID;
  }

  // Every worker aggregates the chunks it processes into its own state, so the jobs below never share an accumulator.
  // The states are combined into the single result row once all chunks have been processed.
  auto operator_state = OperatorSharedState<NoGroupByWorkerState>{};

  // Returns the calling worker's state, creating its per-aggregate aggregation states on first use.
  const auto initialized_worker_state = [&]() -> NoGroupByWorkerState& {
    auto& worker_state = operator_state.current_worker_state();
    worker_state.aggregate_info = aggregate_infos;  // every worker needs the same info to create its states
    if (!worker_state.aggregate_states.empty()) {
      return worker_state;
    }

    worker_state.aggregate_states.resize(aggregate_count);
    for (auto aggregate_id = size_t{0}; aggregate_id < aggregate_count; ++aggregate_id) {
      const auto& info = aggregate_infos[aggregate_id];
      if (info.counts_all_rows) {
        // The result is the input's row count, so this aggregate needs no state and no per-chunk work.
        continue;
      }
      worker_state.aggregate_states[aggregate_id] =
          _make_no_groupby_aggregate_state(info.data_type, _aggregates[aggregate_id]->window_function);
    }
    return worker_state;
  };

  const auto chunk_count = input_table->chunk_count();
  if (chunk_count > 0) {
    const auto job_count = chunk_count;
    auto next_chunk_id = std::atomic<uint32_t>{0};
    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    jobs.reserve(job_count);

    for (auto job_id = size_t{0}; job_id < job_count; ++job_id) {
      jobs.emplace_back(std::make_shared<JobTask>([&, chunk_count]() {
        // A job runs to completion on the worker that picked it up, so the states obtained here are ours alone for as
        // long as we process chunks.
        auto& aggregate_states = initialized_worker_state().aggregate_states;

        while (true) {
          const auto chunk_id = next_chunk_id.fetch_add(1, std::memory_order_relaxed);
          if (chunk_id >= chunk_count) {
            break;
          }

          const auto& chunk = input_table->get_chunk(ChunkID{chunk_id});
          for (auto aggregate_id = size_t{0}; aggregate_id < aggregate_count; ++aggregate_id) {
            if (aggregate_states[aggregate_id]) {
              resolve_data_type(aggregate_infos[aggregate_id].data_type, [&](const auto data_type_t) {
                using ColumnDataType = typename decltype(data_type_t)::type;
                resolve_window_function(
                    aggregate_infos[aggregate_id].window_function, [&](const auto window_function_t) {
                      const auto window_function = decltype(window_function_t)::value;
                      auto& state = *std::static_pointer_cast<BaseAggregateState<ColumnDataType, window_function>>(
                          aggregate_states[aggregate_id]);
                      state.accumulate_entire_chunk(chunk, aggregate_infos[aggregate_id].input_column_id);
                    });
              });
            }
          }
        }
      }));
    }
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  } else {
    // No chunks means no jobs, so we initialize the main thread's state here. Its empty aggregation states yield the
    // results for an empty input (NULL, or 0 for the counting aggregates).
    initialized_worker_state();
  }

  // Combine the per-worker states (at most one per worker, regardless of the number of chunks) into the result row.
  const auto& aggregate_states = operator_state.merge_worker_states().aggregate_states;

  auto column_definitions = TableColumnDefinitions{};
  auto result_values = std::vector<AllTypeVariant>{};
  column_definitions.reserve(aggregate_count);
  result_values.reserve(aggregate_count);

  for (auto aggregate_id = size_t{0}; aggregate_id < aggregate_count; ++aggregate_id) {
    const auto& aggregate = _aggregates[aggregate_id];
    const auto& info = aggregate_infos[aggregate_id];

    if (aggregate->window_function == WindowFunction::Any) {
      // ANY() passes the source column through, keeping its name, data type, and nullability.
      column_definitions.emplace_back(input_table->column_name(info.input_column_id),
                                      input_table->column_data_type(info.input_column_id),
                                      input_table->column_is_nullable(info.input_column_id));
    } else if (aggregate->window_function == WindowFunction::Count ||
               aggregate->window_function == WindowFunction::CountDistinct) {
      // COUNT and COUNT DISTINCT never produce NULL values.
      column_definitions.emplace_back(aggregate->as_column_name(), aggregate->data_type(), false);
    } else {
      column_definitions.emplace_back(aggregate->as_column_name(), aggregate->data_type(), true);
    }

    if (info.counts_all_rows) {
      result_values.emplace_back(static_cast<int64_t>(input_table->row_count()));
      continue;
    }

    resolve_data_type(aggregate_infos[aggregate_id].data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      resolve_window_function(aggregate_infos[aggregate_id].window_function, [&](const auto window_function_t) {
        const auto window_function = decltype(window_function_t)::value;
        auto& state = *std::static_pointer_cast<BaseAggregateState<ColumnDataType, window_function>>(
            aggregate_states[aggregate_id]);
        const auto [value, is_null] = state.finalize();
        result_values.emplace_back(is_null ? AllTypeVariant{} : AllTypeVariant{value});
      });
    });
  }

  auto result_table = std::make_shared<Table>(column_definitions, TableType::Data);
  result_table->append(result_values);
  return result_table;
}

// group-by path. We determine the distinct groups once and then derive every output column (the group-by columns
// and each aggregate) from that shared, index-aligned structure, so all columns line up row-for-row.
std::shared_ptr<const Table> AggregateDYOD::groupby_aggregate() {
  const auto input_table = _left_input->get_output();
  const auto aggregate_count = _aggregates.size();
  const auto groupby_column_count = _groupby_column_ids.size();

  // -- Determine parameters for possibly concurrent execution --
  const auto THREAD_COUNT =
      Hyrise::get().topology.num_cpus() - 1;  // TODO(@forUnity): decide this elsewhere and make sure this is correct
  const auto is_not_immediate_scheduler =
      std::dynamic_pointer_cast<ImmediateExecutionScheduler>(Hyrise::get().scheduler()) == nullptr;
  const auto CONCURRENT = THREAD_COUNT > 1 && is_not_immediate_scheduler;

  // -- Create ticketed groups. The underlying hash table differs in the concurrent versus non-concurrent case --
  std::shared_ptr<GroupKeyDataBase> groups;
  std::shared_ptr<GroupKeyData<true>> concurrent_groups;
  std::shared_ptr<GroupKeyData<false>> nonconcurrent_groups;
  if (CONCURRENT) {
    concurrent_groups = _compute_groups<true>(_groupby_column_ids, input_table);
    groups = concurrent_groups;
  } else {
    nonconcurrent_groups = _compute_groups<false>(_groupby_column_ids, input_table);
    groups = nonconcurrent_groups;
  }
  const auto group_count = groups->group_count;

  // -- Prepare output table --
  auto column_definitions = TableColumnDefinitions{};
  column_definitions.reserve(groupby_column_count + aggregate_count);

  // The output schema is [group-by columns..., aggregate columns...]. Here we only define the columns; the group-by
  // output segments are filled below (either from the fast path or via ticket-pass jobs) and the aggregate segments
  // by their own jobs.
  for (const auto groupby_column_id : _groupby_column_ids) {
    column_definitions.emplace_back(input_table->column_name(groupby_column_id),
                                    input_table->column_data_type(groupby_column_id),
                                    input_table->column_is_nullable(groupby_column_id));
  }

  // Output layout: `output_chunks[chunk][column]`, where the group-by columns occupy the first `groupby_column_count`
  // column slots, followed by one slot per aggregate. Every job produces its column directly as chunk-sized pieces
  // (`ChunkedVector`) and emits them into its fixed column slot of every chunk (`_emit_output_column`), so none of
  // them touch a shared, growing container and the final table assembly is move-only.
  const auto output_chunk_count = (group_count + TARGET_CHUNK_SIZE - 1) / TARGET_CHUNK_SIZE;
  auto output_chunks = std::vector<Segments>(output_chunk_count, Segments(groupby_column_count + aggregate_count));

  // Build the aggregate column definitions serially (cheap metadata lookups). This must not run inside the
  // per-aggregate jobs below, as they would race on `column_definitions`.
  for (auto aggregate_id = uint32_t{0}; aggregate_id < aggregate_count; ++aggregate_id) {
    const auto& aggregate = _aggregates[aggregate_id];
    const auto window_function = aggregate->window_function;

    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto input_column_id = pqp_column.column_id;

    if (window_function == WindowFunction::Any) {
      // ANY() is a pass-through of a column that is functionally dependent on the group-by columns. The output
      // therefore keeps the source column's name, data type, and nullability rather than the "ANY(...)" name.
      column_definitions.emplace_back(input_table->column_name(input_column_id),
                                      input_table->column_data_type(input_column_id),
                                      input_table->column_is_nullable(input_column_id));
    } else if (window_function == WindowFunction::Count || window_function == WindowFunction::CountDistinct) {
      // COUNT and COUNT DISTINCT never produce NULL.
      column_definitions.emplace_back(aggregate->as_column_name(), aggregate->data_type(), false);
    } else {
      // All other aggregates can produce NULL.
      column_definitions.emplace_back(aggregate->as_column_name(), aggregate->data_type(), true);
    }
  }

  // -- Compute aggregates --
  // Old version that easily shares code between concurrent and non-concurrent.
  if (!CONCURRENT) {
    // Each aggregate column is computed independently from the shared grouping structure (`groups->tickets`) and
    // input table, and writes into its own `output_chunks` column slot. There are no cross-column dependencies, so we
    // compute one aggregate per job.
    const auto compute_aggregate = [&](const uint32_t aggregate_id) {
      const auto& aggregate = _aggregates[aggregate_id];
      const auto window_function = aggregate->window_function;

      const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
      const auto input_column_id = pqp_column.column_id;
      const auto target_index = groupby_column_count + aggregate_id;

      // COUNT(*) does not reference an input column. It counts all rows per group (NULLs included). Every input row
      // contributes its group's ticket exactly once, so the per-group count is just a histogram over the tickets.
      if (window_function == WindowFunction::Count && input_column_id == INVALID_COLUMN_ID) {
        auto values = ChunkedVector<int64_t>(group_count, 0);
        const auto* const tickets = groups->tickets.get();
        const auto row_count = input_table->row_count();
        for (auto row_index = size_t{0}; row_index < row_count; ++row_index) {
          ++values[tickets[row_index]];
        }
        _emit_output_column(std::move(values), {}, false, output_chunks, target_index);
        return;
      }

      resolve_data_type(input_table->column_data_type(input_column_id), [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;

        switch (window_function) {
          case WindowFunction::Min: {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Min>::ReturnType;
            auto [values, nulls] = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Min>(
                groups->tickets.get(), group_count, input_table, input_column_id);
            _emit_output_column(std::move(values), std::move(nulls), true, output_chunks, target_index);
            break;
          }
          case WindowFunction::Max: {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Max>::ReturnType;
            auto [values, nulls] = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Max>(
                groups->tickets.get(), group_count, input_table, input_column_id);
            _emit_output_column(std::move(values), std::move(nulls), true, output_chunks, target_index);
            break;
          }
          case WindowFunction::Sum: {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Sum>::ReturnType;
            auto [values, nulls] = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Sum>(
                groups->tickets.get(), group_count, input_table, input_column_id);
            _emit_output_column(std::move(values), std::move(nulls), true, output_chunks, target_index);
            break;
          }
          case WindowFunction::Avg: {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Avg>::ReturnType;
            auto [values, nulls] = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Avg>(
                groups->tickets.get(), group_count, input_table, input_column_id);
            _emit_output_column(std::move(values), std::move(nulls), true, output_chunks, target_index);
            break;
          }
          case WindowFunction::Count: {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Count>::ReturnType;
            auto [values, nulls] = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Count>(
                groups->tickets.get(), group_count, input_table, input_column_id);
            // COUNT never produces NULL.
            _emit_output_column(std::move(values), std::move(nulls), false, output_chunks, target_index);
            break;
          }
          case WindowFunction::CountDistinct: {
            auto values = _count_distinct_grouped<ColumnDataType>(groups->tickets.get(), group_count, input_table,
                                                                  input_column_id);
            _emit_output_column(std::move(values), {}, false, output_chunks, target_index);
            break;
          }
          case WindowFunction::StandardDeviationSample: {
            if constexpr (std::is_arithmetic_v<ColumnDataType>) {
              auto [values, nulls] = _standard_deviation_sample_grouped<ColumnDataType>(
                  groups->tickets.get(), group_count, input_table, input_column_id);
              _emit_output_column(std::move(values), std::move(nulls), true, output_chunks, target_index);
            } else {
              Fail("StandardDeviationSample is not available on non-arithmetic types.");
            }
            break;
          }
          case WindowFunction::Any: {
            auto [values, nulls] =
                _any_grouped<ColumnDataType>(groups->tickets.get(), group_count, input_table, input_column_id);
            // ANY() passes the source column through, so the output keeps its nullability.
            _emit_output_column(std::move(values), std::move(nulls), input_table->column_is_nullable(input_column_id),
                                output_chunks, target_index);
            break;
          }
          default:
            Fail(std::format("Unsupported aggregate function '{}'.",
                             window_function_to_string.left.at(window_function)));
        }
      });
    };

    // For low-cardinality group-bys (far fewer groups than input rows), each group-by column is cheaper to build by
    // reading every group's value once from its distinct key row in the hash table than by scanning the whole source
    // column; above that ratio the scattered key-row access loses to a sequential source scan. Only the multi-column
    // grouping path exposes a hash table (`has_hash_table`); the single-column fast path recovers group-by values by
    // scanning.
    const auto input_row_count = input_table->row_count();

    // TODO(@V1nce1): Right now the single column fast path has `has_hash_table` set to false, so it always uses
    // the sequential scan. We could change that.
    const auto use_hash_table_for_groupby =
        groups->has_hash_table && group_count * GROUPBY_HASH_TABLE_MIN_ROWS_PER_GROUP <= input_row_count;

    // Builds one group-by output column. Every row in a group carries the same group-by value, so we only need one
    // value per group. Depending on cardinality (`use_hash_table_for_groupby`) we either read it from the group's
    // hash-table key row or recover it with a sequential ANY scan of the source column
    // (the first row seen per group wins).
    const auto build_groupby_column = [&](const uint32_t groupby_index) {
      const auto groupby_column_id = _groupby_column_ids[groupby_index];
      resolve_data_type(input_table->column_data_type(groupby_column_id), [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;

        auto [values, nulls] = [&]() -> std::pair<ChunkedVector<ColumnDataType>, ChunkedVector<bool>> {
          if (!use_hash_table_for_groupby) {
            // High cardinality: a sequential scan of the source column beats chasing the scattered key rows.
            return _any_grouped<ColumnDataType>(groups->tickets.get(), group_count, input_table, groupby_column_id);
          }
          // Low cardinality: read each group's value straight from its hash-table key row. `string_col_index` locates
          // this column among the string group-by columns (see `RowView::string_ptr`).
          auto string_col_index = size_t{0};
          for (auto index = uint32_t{0}; index < groupby_index; ++index) {
            if (input_table->column_data_type(_groupby_column_ids[index]) == DataType::String) {
              ++string_col_index;
            }
          }
          if (CONCURRENT) {
            return _groupby_from_hash_table<ColumnDataType, true>(*concurrent_groups, group_count, groupby_index,
                                                                  string_col_index);
          } else {
            return _groupby_from_hash_table<ColumnDataType, false>(*nonconcurrent_groups, group_count, groupby_index,
                                                                   string_col_index);
          }
        }();

        _emit_output_column(std::move(values), std::move(nulls), input_table->column_is_nullable(groupby_column_id),
                            output_chunks, groupby_index);
      });
    };

    // One job per output column: build each group-by column and compute each aggregate. They all read the
    // shared, read-only grouping structure and input table and write disjoint output slots, so there are no
    // dependencies between them. With fewer than two units we run inline to avoid the scheduling overhead.
    const auto unit_count = groupby_column_count + aggregate_count;
    const auto run_unit = [&](const size_t unit) {
      if (unit < groupby_column_count) {
        build_groupby_column(static_cast<uint32_t>(unit));
      } else {
        compute_aggregate(static_cast<uint32_t>(unit - groupby_column_count));
      }
    };

    if (unit_count < 2) {
      for (auto unit = size_t{0}; unit < unit_count; ++unit) {
        run_unit(unit);
      }
    } else {
      auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
      jobs.reserve(unit_count);
      for (auto unit = size_t{0}; unit < unit_count; ++unit) {
        jobs.emplace_back(std::make_shared<JobTask>([&run_unit, unit]() {
          run_unit(unit);
        }));
      }
      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
    }

    if (CONCURRENT) {
      auto cleanup_job =
          std::make_shared<JobTask>([groups = std::move(groups), concurrent_groups = std::move(concurrent_groups),
                                     nonconcurrent_groups = std::move(nonconcurrent_groups)]() mutable {
            groups.reset();
            concurrent_groups.reset();
            nonconcurrent_groups.reset();
          });
      cleanup_job->schedule();
    }

    // Every output column was already produced as chunk-sized segments by its own job, so assembling the result table
    // is move-only: no values are copied here.
    auto result_table = std::make_shared<Table>(column_definitions, TableType::Data);
    for (auto& chunk_segments : output_chunks) {
      result_table->append_chunk(std::move(chunk_segments));
    }
    return result_table;
  }

  // -- New more concurrent version --

  // Each aggregate has its own intermediate result state, which is shared between the jobs that process its chunks and
  // the jobs that finalize its results.
  auto intermediate_results = std::vector<std::shared_ptr<void>>{};
  for (auto aggregate_id = size_t{0}; aggregate_id < aggregate_count; ++aggregate_id) {
    const auto& aggregate = _aggregates[aggregate_id];
    const auto window_function = aggregate->window_function;

    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto input_column_id = pqp_column.column_id;
    const auto data_type =
        input_column_id == INVALID_COLUMN_ID ? DataType::Long : input_table->column_data_type(input_column_id);

    intermediate_results.emplace_back(_make_global_aggregate_state(window_function, data_type, group_count));
  }

  const auto result_column_count = groupby_column_count + aggregate_count;

  // The final results are built from the intermediate results once all chunks have been processed.
  auto final_results = std::vector<std::shared_ptr<BaseChunkedVector>>(result_column_count);
  auto final_result_nulls = std::vector<std::shared_ptr<ChunkedVector<bool>>>(result_column_count);

  for (auto output_column_id = size_t{0}; output_column_id < result_column_count; ++output_column_id) {
    const auto is_groupby_column = output_column_id < groupby_column_count;
    const auto data_type = is_groupby_column ? input_table->column_data_type(_groupby_column_ids[output_column_id])
                                             : _aggregates[output_column_id - groupby_column_count]->data_type();

    resolve_data_type(data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;

      final_results[output_column_id] = std::make_shared<ChunkedVector<ColumnDataType>>(group_count);
      final_result_nulls[output_column_id] = std::make_shared<ChunkedVector<bool>>(group_count);
    });
  }

  // Exclusive prefix sum of the chunk sizes: `chunk_offsets[chunk_id]` is the global row index of the chunk's first
  // row, which is where the chunk starts indexing into `tickets`.
  auto chunk_offsets = std::vector<size_t>(input_table->chunk_count(), 0);
  auto row_offset = size_t{0};
  for (auto chunk_id = ChunkID{0}; chunk_id < input_table->chunk_count(); ++chunk_id) {
    chunk_offsets[chunk_id] = row_offset;
    row_offset += input_table->get_chunk(chunk_id)->size();
  }

  const auto* const tickets = groups->tickets.get();

  auto chunk_id_per_aggregate = std::vector<std::atomic<size_t>>(aggregate_count);  // ChunkID
  // One flag per (aggregate, group): guards the merge of a local hash table entry into the global state.
  auto intermediate_result_atomics = std::vector<std::vector<std::atomic_flag>>(aggregate_count);
  for (auto& per_aggregate_atomics : intermediate_result_atomics) {
    per_aggregate_atomics = std::vector<std::atomic_flag>(group_count);
  }

  // We have one job as a worker. This creates virtual sub-jobs per aggregate and per chunk.
  const auto job_main = [&](const uint32_t job_id) {
    const auto chunk_count = input_table->chunk_count();
    const auto initial_aggregate_id = job_id % aggregate_count;
    auto current_aggregate_id = initial_aggregate_id;
    do {
      // resolve data type for current aggregate
      const auto& aggregate = _aggregates[current_aggregate_id];
      auto& this_column_intermediate_result_atomics = intermediate_result_atomics[current_aggregate_id];
      const auto window_function = aggregate->window_function;
      const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
      const auto input_column_id = pqp_column.column_id;
      const auto data_type =
          input_column_id == INVALID_COLUMN_ID ? DataType::Long : input_table->column_data_type(input_column_id);
      resolve_data_type(data_type, [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;
        resolve_window_function(window_function, [&](const auto window_function_t) {
          const auto window_function = decltype(window_function_t)::value;
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, window_function>::ReturnType;

          // resolve the intermediate result type for the current aggregate
          using AggregateState = IntermediateState<ColumnDataType, window_function>;
          auto this_column_intermediate_results =
              std::static_pointer_cast<std::vector<AggregateState>>(intermediate_results[current_aggregate_id]);

          // Build the local hash table of intermediate results.
          auto local_hash_table = boost::unordered_flat_map<uint64_t, AggregateState>{};

          // for every chunk
          while (true) {
            const auto next_chunk = chunk_id_per_aggregate[current_aggregate_id].fetch_add(1);
            if (next_chunk >= static_cast<size_t>(chunk_count)) {
              break;
            }
            const auto chunk_id = static_cast<ChunkID::base_type>(next_chunk);
            const auto& chunk = input_table->get_chunk(ChunkID{chunk_id});
            const auto row_index = chunk_offsets[chunk_id];

            if constexpr (window_function == WindowFunction::Count) {
              // COUNT(*) references no input column, so there is no segment to iterate.
              if (input_column_id == INVALID_COLUMN_ID) {
                _accumulate_count_star_concurrently<ColumnDataType, AggregateType, window_function, AggregateState>(
                    tickets, row_index, chunk->size(), local_hash_table, this_column_intermediate_results,
                    this_column_intermediate_result_atomics);
                continue;
              }
            }

            _accumulate_concurrently<ColumnDataType, AggregateType, window_function, AggregateState>(
                tickets, row_index, chunk->get_segment(input_column_id), local_hash_table,
                this_column_intermediate_results, this_column_intermediate_result_atomics);
          }
          // Finally, force a spill of all entries that remain in the local hash table. This also clears the local
          // hashtable.
          spill_local_hash_table_to_global_aggregate_result<ColumnDataType, AggregateType, window_function,
                                                            AggregateState, true>(
              local_hash_table, this_column_intermediate_results, this_column_intermediate_result_atomics);
        });
      });

      // move on to next aggregate
      current_aggregate_id = (current_aggregate_id + 1) % aggregate_count;
    } while (current_aggregate_id != initial_aggregate_id);
  };

  // A pure DISTINCT (group-by without any aggregate) has nothing to accumulate. `job_main` also assumes at least one
  // aggregate, as it round-robins over them modulo `aggregate_count`.
  if (aggregate_count > 0) {
    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    jobs.reserve(THREAD_COUNT);

    for (auto job_id = size_t{0}; job_id < THREAD_COUNT; ++job_id) {
      jobs.emplace_back(std::make_shared<JobTask>([&, job_id]() {
        job_main(static_cast<uint32_t>(job_id));
      }));
    }
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  }

  // For low-cardinality group-bys (far fewer groups than input rows), each group-by column is cheaper to build by
  // reading every group's value once from its distinct key row in the hash table than by scanning the whole source
  // column; above that ratio the scattered key-row access loses to a sequential source scan. Only the multi-column
  // grouping path exposes a hash table (`has_hash_table`); the single-column fast path recovers group-by values by
  // scanning.
  const auto use_hash_table_for_groupby =
      groups->has_hash_table && group_count * GROUPBY_HASH_TABLE_MIN_ROWS_PER_GROUP <= input_table->row_count();

  // `string_col_index` locates a column among the string group-by columns (see `RowView::string_ptr`).
  auto string_col_index_per_groupby_column = std::vector<size_t>(groupby_column_count, 0);
  {
    auto string_col_index = size_t{0};
    for (auto groupby_index = uint32_t{0}; groupby_index < groupby_column_count; ++groupby_index) {
      string_col_index_per_groupby_column[groupby_index] = string_col_index;
      if (input_table->column_data_type(_groupby_column_ids[groupby_index]) == DataType::String) {
        ++string_col_index;
      }
    }
  }

  // The group-by columns are built into their `final_results` slots, just like the aggregate columns. Their per-group
  // NULL flags are collected as one byte per group and folded into the output bitmap once the jobs joined, as the jobs
  // write scattered groups and cannot share a packed bitmap's words. `seen` claims a group for one job of the scan
  // path; the hash-table path visits every group exactly once and needs no claim.
  auto groupby_nulls = std::vector<std::vector<uint8_t>>(groupby_column_count);
  auto groupby_seen = std::vector<std::vector<std::atomic_flag>>(groupby_column_count);
  for (auto groupby_index = uint32_t{0}; groupby_index < groupby_column_count; ++groupby_index) {
    groupby_nulls[groupby_index] = std::vector<uint8_t>(group_count, 0);
    if (!use_hash_table_for_groupby) {
      groupby_seen[groupby_index] = std::vector<std::atomic_flag>(group_count);
    }
  }

  // Builds one part of one group-by column: a single chunk of the source column for the scan path, a range of
  // hash-table slots for the hash-table path. The parts of a column write disjoint output slots, and the columns write
  // disjoint `final_results` slots, so all of them run as one flat set of jobs.
  const auto build_groupby_part = [&](const uint32_t groupby_index, const size_t first_part_index,
                                      const size_t end_part_index) {
    const auto groupby_column_id = _groupby_column_ids[groupby_index];

    resolve_data_type(input_table->column_data_type(groupby_column_id), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      auto& values = *std::static_pointer_cast<ChunkedVector<ColumnDataType>>(final_results[groupby_index]);
      auto& nulls = groupby_nulls[groupby_index];

      if (use_hash_table_for_groupby) {
        // Low cardinality: read each group's value straight from its hash-table key row.
        _groupby_from_hash_table_slots<ColumnDataType>(*concurrent_groups, groupby_index,
                                                       string_col_index_per_groupby_column[groupby_index],
                                                       first_part_index, end_part_index, values, nulls);
        return;
      }
      // High cardinality: a sequential scan of the source column beats chasing the scattered key rows.
      const auto chunk_id = ChunkID{static_cast<ChunkID::base_type>(first_part_index)};
      _any_grouped_chunk<ColumnDataType>(tickets, input_table, groupby_column_id, chunk_id, chunk_offsets[chunk_id],
                                         groupby_seen[groupby_index], values, nulls);
    });
  };

  {
    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    for (auto groupby_index = uint32_t{0}; groupby_index < groupby_column_count; ++groupby_index) {
      if (use_hash_table_for_groupby) {
        // One job per slot range of the grouping hash table. Only the multi-column path builds one, and only the
        // concurrent version of it reaches this code (the sequential one returns above).
        const auto slot_count = concurrent_groups->global_hash_table.capacity();
        for (auto first_slot = size_t{0}; first_slot < slot_count; first_slot += GROUPBY_HASH_TABLE_SLOTS_PER_JOB) {
          const auto end_slot = std::min(first_slot + GROUPBY_HASH_TABLE_SLOTS_PER_JOB, slot_count);
          jobs.emplace_back(std::make_shared<JobTask>([&build_groupby_part, groupby_index, first_slot, end_slot]() {
            build_groupby_part(groupby_index, first_slot, end_slot);
          }));
        }
        continue;
      }
      // One job per chunk of the source column.
      const auto chunk_count = input_table->chunk_count();
      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        jobs.emplace_back(std::make_shared<JobTask>([&build_groupby_part, groupby_index, chunk_id]() {
          build_groupby_part(groupby_index, chunk_id, chunk_id + 1);
        }));
      }
    }

    if (!jobs.empty()) {
      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
    }
  }

  // Emit the group-by columns. All that is left per column is folding the per-group NULL bytes into the output bitmap,
  // which the jobs above could not write concurrently.
  for (auto groupby_index = uint32_t{0}; groupby_index < groupby_column_count; ++groupby_index) {
    const auto groupby_column_id = _groupby_column_ids[groupby_index];
    const auto column_is_nullable = input_table->column_is_nullable(groupby_column_id);

    resolve_data_type(input_table->column_data_type(groupby_column_id), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      auto& values = *std::static_pointer_cast<ChunkedVector<ColumnDataType>>(final_results[groupby_index]);
      auto& nulls = *final_result_nulls[groupby_index];

      if (column_is_nullable) {
        const auto& null_bytes = groupby_nulls[groupby_index];
        for (auto group_id = size_t{0}; group_id < group_count; ++group_id) {
          nulls[group_id] = null_bytes[group_id] != 0;
        }
      }

      _emit_output_column(std::move(values), std::move(nulls), column_is_nullable, output_chunks, groupby_index);
    });
  }

  // Finalize the aggregate results here to make sure that all the results are actually available before we start
  // emitting them.
  const auto FINALIZE_ROWS_BATCH_SIZE = size_t{65536};  // 65,536 rows per batch
  const auto jobs_per_aggregate = group_count / FINALIZE_ROWS_BATCH_SIZE + 1;
  const auto finalize_job_count = aggregate_count * jobs_per_aggregate;

  const auto finalize_job_main = [&](const uint32_t job_id) {
    const auto aggregate_id = job_id / jobs_per_aggregate;
    const auto start_row_id = (job_id % jobs_per_aggregate) * FINALIZE_ROWS_BATCH_SIZE;
    const auto end_row_id = std::min(start_row_id + FINALIZE_ROWS_BATCH_SIZE, group_count);

    const auto& aggregate = _aggregates[aggregate_id];
    const auto window_function = aggregate->window_function;
    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto input_column_id = pqp_column.column_id;
    const auto data_type =
        input_column_id == INVALID_COLUMN_ID ? DataType::Long : input_table->column_data_type(input_column_id);

    resolve_data_type(data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      resolve_window_function(window_function, [&](const auto window_function_t) {
        const auto window_function = decltype(window_function_t)::value;
        using AggregateType = typename WindowFunctionTraits<ColumnDataType, window_function>::ReturnType;
        using AggregateState = IntermediateState<ColumnDataType, window_function>;

        auto this_column_intermediate_results =
            std::static_pointer_cast<std::vector<AggregateState>>(intermediate_results[aggregate_id]);
        auto final_result_vector =
            std::static_pointer_cast<ChunkedVector<AggregateType>>(final_results[groupby_column_count + aggregate_id]);
        auto& final_result_nulls_vector = final_result_nulls[groupby_column_count + aggregate_id];

        for (auto row_id_to_finalize = start_row_id; row_id_to_finalize < end_row_id; ++row_id_to_finalize) {
          auto& intermediate_result = this_column_intermediate_results->operator[](row_id_to_finalize);
          const auto [value, is_null] = intermediate_result.finalize();
          final_result_vector->operator[](row_id_to_finalize) = value;
          final_result_nulls_vector->operator[](row_id_to_finalize) = is_null;
        }
      });
    });
  };

  {
    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    for (auto job_id = size_t{0}; job_id < finalize_job_count; ++job_id) {
      auto job = std::make_shared<JobTask>([&, job_id]() {
        finalize_job_main(static_cast<uint32_t>(job_id));
      });
      jobs.emplace_back(job);
    }
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  }

  // Also emit the aggregate columns
  for (auto aggregate_column_id = groupby_column_count; aggregate_column_id < result_column_count;
       ++aggregate_column_id) {
    const auto aggregate_index = static_cast<uint32_t>(aggregate_column_id - groupby_column_count);
    const auto& aggregate = _aggregates[aggregate_index];
    const auto window_function = aggregate->window_function;

    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto input_column_id = pqp_column.column_id;
    const auto data_type =
        input_column_id == INVALID_COLUMN_ID ? DataType::Long : input_table->column_data_type(input_column_id);

    resolve_data_type(data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      resolve_window_function(window_function, [&](const auto window_function_t) {
        using AggregateType =
            typename WindowFunctionTraits<ColumnDataType, decltype(window_function_t)::value>::ReturnType;
        auto final_result_vector =
            std::static_pointer_cast<ChunkedVector<AggregateType>>(final_results[aggregate_column_id]);
        auto& final_result_nulls_vector = final_result_nulls[aggregate_column_id];
        // Must match the nullability declared in `column_definitions` above: COUNT and COUNT DISTINCT never produce
        // NULL, ANY passes the source column's nullability through, everything else can produce NULL.
        const auto column_is_nullable =
            window_function != WindowFunction::Count && window_function != WindowFunction::CountDistinct &&
            (window_function != WindowFunction::Any || input_table->column_is_nullable(input_column_id));

        _emit_output_column(std::move(*final_result_vector), std::move(*final_result_nulls_vector), column_is_nullable,
                            output_chunks, aggregate_column_id);
      });
    });
  }

  // --- COPYPASTA from old version wrap up ---

  if (CONCURRENT) {
    auto cleanup_job =
        std::make_shared<JobTask>([groups = std::move(groups), concurrent_groups = std::move(concurrent_groups),
                                   nonconcurrent_groups = std::move(nonconcurrent_groups)]() mutable {
          groups.reset();
          concurrent_groups.reset();
          nonconcurrent_groups.reset();
        });
    cleanup_job->schedule();
  }

  // Every output column was already produced as chunk-sized segments by its own job, so assembling the result table
  // is move-only: no values are copied here.
  auto result_table = std::make_shared<Table>(column_definitions, TableType::Data);
  for (auto& chunk_segments : output_chunks) {
    result_table->append_chunk(std::move(chunk_segments));
  }
  return result_table;
}

std::shared_ptr<AbstractOperator> AggregateDYOD::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<AggregateDYOD>(copied_left_input, _aggregates, _groupby_column_ids);
}

std::shared_ptr<const Table> AggregateDYOD::_on_execute() {
  // const auto input_table = _left_input->get_output();

  _validate_aggregates();

  if (_groupby_column_ids.empty()) {
    return no_groupby_aggregate();
  }
  return groupby_aggregate();
}

void AggregateDYOD::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void AggregateDYOD::_on_cleanup() {}

}  // namespace hyrise
