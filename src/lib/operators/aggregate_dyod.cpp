#include "aggregate_dyod.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
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
#include "operators/aggregate_hash.hpp"
#include "operators/operator_state.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_scheduler.hpp"
#include "scheduler/immediate_execution_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace hyrise {

// Threshold that decides how the group-by output columns are built. When the input has at least this many rows per
// group, each group-by column is materialized by reading every group's value directly from the ticketing hashtable.
constexpr auto GROUPBY_HASH_TABLE_MIN_ROWS_PER_GROUP = size_t{4};

// Slots of the grouping hash table that one job reads when a group-by output column is built from it.
constexpr auto GROUPBY_HASH_TABLE_SLOTS_PER_JOB = size_t{1} << 16;

constexpr auto AGG_MAX_LOCAL_HASH_TABLE_SIZE = size_t{1 << 12};  // 4096 entries

// Rows of the per-group intermediate results that one job finalizes.
constexpr auto FINALIZE_ROWS_BATCH_SIZE = size_t{1} << 16;

// State of one worker for the no-group-by aggregation.
struct NoGroupByWorkerState : public Noncopyable {
  void merge(NoGroupByWorkerState& other) {
    if (other.aggregate_states.empty()) {
      // This worker was handed a state, but never processed a chunk.
      return;
    }
    if (aggregate_states.empty()) {
      aggregate_states = std::move(other.aggregate_states);
      aggregate_info = std::move(other.aggregate_info);
      return;
    }

    const auto aggregate_count = aggregate_states.size();
    for (auto aggregate_id = size_t{0}; aggregate_id < aggregate_count; ++aggregate_id) {
      // Aggregates that need no per-chunk work (see `AggregateInfo::counts_all_rows`) have no state.
      const auto& aggregate_state = aggregate_states[aggregate_id];
      const auto& current_aggregate_info = aggregate_info[aggregate_id];

      resolve_data_type(current_aggregate_info.data_type, [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;
        resolve_window_function(current_aggregate_info.window_function, [&](const auto window_function_t) {
          using AggregateState = IntermediateState<ColumnDataType, decltype(window_function_t)::value>;
          auto& state = *std::static_pointer_cast<AggregateState>(aggregate_state);
          auto& other_state = *std::static_pointer_cast<AggregateState>(other.aggregate_states[aggregate_id]);

          state.merge(other_state);
        });
      });
    }
  }

  std::vector<std::shared_ptr<void>> aggregate_states;
  std::vector<AggregateInfo> aggregate_info;
};

// Resolves the per-aggregate information that both paths need from the aggregate expressions and the input schema.
std::vector<AggregateInfo> _build_aggregate_infos(
    const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
    const std::shared_ptr<const Table>& input_table) {
  const auto aggregate_count = aggregates.size();
  auto aggregate_infos = std::vector<AggregateInfo>(aggregate_count);

  for (auto aggregate_id = size_t{0}; aggregate_id < aggregate_count; ++aggregate_id) {
    const auto& aggregate = *aggregates[aggregate_id];
    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate.argument());
    const auto input_column_id = pqp_column.column_id;

    auto& info = aggregate_infos[aggregate_id];
    info.input_column_id = input_column_id;
    info.data_type =
        input_column_id == INVALID_COLUMN_ID ? DataType::Long : input_table->column_data_type(input_column_id);
    info.window_function = aggregate.window_function;
    info.is_count_star = info.window_function == WindowFunction::Count && input_column_id == INVALID_COLUMN_ID;
    info.counts_all_rows = info.window_function == WindowFunction::Count &&
                           (input_column_id == INVALID_COLUMN_ID || !input_table->column_is_nullable(input_column_id));
  }
  return aggregate_infos;
}

// Output schema of both paths: [group-by columns..., aggregate columns...] (no group-by columns for the no-group-by
// path).
TableColumnDefinitions _build_output_column_definitions(
    const Table& input_table, const std::vector<ColumnID>& groupby_column_ids,
    const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
    const std::vector<AggregateInfo>& aggregate_infos) {
  const auto aggregate_count = aggregates.size();
  auto column_definitions = TableColumnDefinitions{};
  column_definitions.reserve(groupby_column_ids.size() + aggregate_count);

  for (const auto groupby_column_id : groupby_column_ids) {
    column_definitions.emplace_back(input_table.column_name(groupby_column_id),
                                    input_table.column_data_type(groupby_column_id),
                                    input_table.column_is_nullable(groupby_column_id));
  }

  for (auto aggregate_id = size_t{0}; aggregate_id < aggregate_count; ++aggregate_id) {
    const auto& aggregate = *aggregates[aggregate_id];
    const auto& info = aggregate_infos[aggregate_id];

    if (info.window_function == WindowFunction::Any) {
      // ANY() is a pass-through of a column that is functionally dependent on the group-by columns. The output
      // therefore keeps the source column's name, data type, and nullability rather than the "ANY(...)" name.
      column_definitions.emplace_back(input_table.column_name(info.input_column_id),
                                      input_table.column_data_type(info.input_column_id),
                                      input_table.column_is_nullable(info.input_column_id));
    } else if (info.window_function == WindowFunction::Count || info.window_function == WindowFunction::CountDistinct) {
      // COUNT and COUNT DISTINCT never produce NULL.
      column_definitions.emplace_back(aggregate.as_column_name(), aggregate.data_type(), false);
    } else {
      // All other aggregates can produce NULL.
      column_definitions.emplace_back(aggregate.as_column_name(), aggregate.data_type(), true);
    }
  }
  return column_definitions;
}

// ---------------------------------------------- No-group-by path -----------------------------------------------

// Aggregates every chunk of the input into per-worker states: one job per chunk, with the jobs pulling chunks from a
// shared counter. Every worker accumulates into its own state, so the jobs never share an accumulator. For an empty
// input, the main thread's state is initialized instead; its empty aggregation states yield the results for zero rows
// (NULL, or 0 for the counting aggregates).
void _accumulate_no_groupby_states(const std::shared_ptr<const Table>& input_table,
                                   const std::vector<AggregateInfo>& aggregate_infos,
                                   OperatorSharedState<NoGroupByWorkerState>& operator_state) {
  const auto aggregate_count = aggregate_infos.size();

  // Returns the calling worker's state, creating its per-aggregate aggregation states on first use.
  const auto initialized_worker_state = [&]() -> NoGroupByWorkerState& {
    auto& worker_state = operator_state.current_worker_state();
    if (!worker_state.aggregate_states.empty()) {
      return worker_state;
    }

    worker_state.aggregate_info = aggregate_infos;  // every worker needs the same info to merge its states
    worker_state.aggregate_states.resize(aggregate_count);
    for (auto aggregate_id = size_t{0}; aggregate_id < aggregate_count; ++aggregate_id) {
      const auto& info = aggregate_infos[aggregate_id];
      if (info.counts_all_rows) {
        // The result is the input's row count, so this aggregate needs no state and no per-chunk work.
        continue;
      }
      worker_state.aggregate_states[aggregate_id] =
          _make_no_groupby_aggregate_state(info.data_type, info.window_function);
    }
    return worker_state;
  };

  const auto chunk_count = input_table->chunk_count();
  if (chunk_count == 0) {
    initialized_worker_state();
    return;
  }

  auto next_chunk_id = std::atomic<uint32_t>{0};
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(chunk_count);

  for (auto job_id = size_t{0}; job_id < chunk_count; ++job_id) {
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
          if (!aggregate_states[aggregate_id]) {
            continue;  // Aggregates that need no per-chunk work (`counts_all_rows`) have no state.
          }

          const auto& info = aggregate_infos[aggregate_id];
          resolve_data_type(info.data_type, [&](const auto data_type_t) {
            using ColumnDataType = typename decltype(data_type_t)::type;
            resolve_window_function(info.window_function, [&](const auto window_function_t) {
              using AggregateState = IntermediateState<ColumnDataType, decltype(window_function_t)::value>;
              auto& state = *std::static_pointer_cast<AggregateState>(aggregate_states[aggregate_id]);
              state.accumulate_entire_chunk(chunk, info.input_column_id);
            });
          });
        }
      }
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
}

// Produces the single output row by finalizing the merged aggregate states.
std::vector<AllTypeVariant> _no_groupby_result_row(const std::shared_ptr<const Table>& input_table,
                                                   const std::vector<AggregateInfo>& aggregate_infos,
                                                   const std::vector<std::shared_ptr<void>>& aggregate_states) {
  const auto aggregate_count = aggregate_infos.size();
  auto result_values = std::vector<AllTypeVariant>{};
  result_values.reserve(aggregate_count);

  for (auto aggregate_id = size_t{0}; aggregate_id < aggregate_count; ++aggregate_id) {
    const auto& info = aggregate_infos[aggregate_id];
    if (info.counts_all_rows) {
      result_values.emplace_back(static_cast<int64_t>(input_table->row_count()));
      continue;
    }

    resolve_data_type(info.data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      resolve_window_function(info.window_function, [&](const auto window_function_t) {
        using AggregateState = IntermediateState<ColumnDataType, decltype(window_function_t)::value>;
        const auto& state = *std::static_pointer_cast<AggregateState>(aggregate_states[aggregate_id]);
        const auto [value, is_null] = state.finalize();
        result_values.emplace_back(is_null ? AllTypeVariant{} : AllTypeVariant{value});
      });
    });
  }
  return result_values;
}

std::shared_ptr<const Table> AggregateDYOD::no_groupby_aggregate() {
  const auto input_table = left_input_table();
  const auto aggregate_infos = _build_aggregate_infos(_aggregates, input_table);

  // Every worker aggregates the chunks it processes into its own state; the states are combined into the single result
  // row once all chunks have been processed (at most one state per worker, regardless of the number of chunks).
  auto operator_state = OperatorSharedState<NoGroupByWorkerState>{};
  _accumulate_no_groupby_states(input_table, aggregate_infos, operator_state);
  const auto& aggregate_states = operator_state.merge_worker_states().aggregate_states;

  auto result_table = std::make_shared<Table>(
      _build_output_column_definitions(*input_table, {}, _aggregates, aggregate_infos), TableType::Data);
  result_table->append(_no_groupby_result_row(input_table, aggregate_infos, aggregate_states));
  return result_table;
}

// ------------------------------------------------ Group-by path ------------------------------------------------

// Emits one job per chunk of `vector`, each value-initializing its chunk's storage. This spreads the zeroing and the
// first-touch page faults of the large per-group containers over all cores instead of a single allocating thread.
template <typename T>
void _emplace_chunk_allocation_jobs(const std::shared_ptr<ChunkedVector<T>>& vector, const size_t size,
                                    std::vector<std::shared_ptr<AbstractTask>>& jobs) {
  constexpr auto CHUNK_SIZE = ChunkedVector<T>::CHUNK_SIZE;
  vector->chunks.resize((size + CHUNK_SIZE - 1) / CHUNK_SIZE);
  const auto chunk_count = vector->chunks.size();
  for (auto chunk_index = size_t{0}; chunk_index < chunk_count; ++chunk_index) {
    jobs.emplace_back(std::make_shared<JobTask>([vector, chunk_index, size, CHUNK_SIZE]() {
      vector->chunks[chunk_index] = pmr_vector<T>(std::min(CHUNK_SIZE, size - chunk_index * CHUNK_SIZE));
    }));
  }
}

std::vector<std::shared_ptr<void>> _allocate_intermediate_results(const std::vector<AggregateInfo>& aggregate_infos,
                                                                  const size_t group_count) {
  auto intermediate_results = std::vector<std::shared_ptr<void>>{};
  intermediate_results.reserve(aggregate_infos.size());
  for (const auto& info : aggregate_infos) {
    resolve_data_type(info.data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      resolve_window_function(info.window_function, [&](const auto window_function_t) {
        intermediate_results.push_back(
            std::make_shared<std::vector<IntermediateState<ColumnDataType, decltype(window_function_t)::value>>>(
                group_count));
      });
    });
  }
  return intermediate_results;
}

// The returned vectors' chunks are only allocated once the emitted `allocation_jobs` have run; the caller schedules
// them so that they overlap with the accumulate phase.
std::pair<std::vector<std::shared_ptr<BaseChunkedVector>>, std::vector<std::shared_ptr<ChunkedVector<bool>>>>
_allocate_final_results(const TableColumnDefinitions& column_definitions, const size_t group_count,
                        std::vector<std::shared_ptr<AbstractTask>>& allocation_jobs) {
  const auto result_column_count = column_definitions.size();
  auto final_results = std::vector<std::shared_ptr<BaseChunkedVector>>(result_column_count);
  auto final_result_nulls = std::vector<std::shared_ptr<ChunkedVector<bool>>>(result_column_count);

  for (auto output_column_id = size_t{0}; output_column_id < result_column_count; ++output_column_id) {
    const auto& column_definition = column_definitions[output_column_id];

    resolve_data_type(column_definition.data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;

      const auto values = std::make_shared<ChunkedVector<ColumnDataType>>();
      _emplace_chunk_allocation_jobs(values, group_count, allocation_jobs);
      final_results[output_column_id] = values;

      // Non-nullable output columns (COUNT, non-nullable group-by keys) never read their nulls vector
      // (see `_emit_output_column`), so it stays unallocated (nullptr).
      if (column_definition.nullable) {
        const auto nulls = std::make_shared<ChunkedVector<bool>>();
        _emplace_chunk_allocation_jobs(nulls, group_count, allocation_jobs);
        final_result_nulls[output_column_id] = nulls;
      }
    });
  }
  return {std::move(final_results), std::move(final_result_nulls)};
}

// --------------------------------------------- Group-by: Aggregate ---------------------------------------------

template <typename ColumnDataType, typename AggregateType, WindowFunction window_function, typename AggregateState,
          bool force_spill = false>
void spill_local_hash_table_to_global_aggregate_result(
    boost::unordered_flat_map<uint64_t, AggregateState>& local_hash_table,
    const std::shared_ptr<std::vector<AggregateState>>& global_aggregate_result,
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
// chunk contributes to its group's count.
template <typename ColumnDataType, typename AggregateType, WindowFunction window_function, typename AggregateState>
void _accumulate_count_star(const uint64_t* const tickets, const size_t row_index, const size_t chunk_size,
                            boost::unordered_flat_map<uint64_t, AggregateState>& local_hash_table,
                            const std::shared_ptr<std::vector<AggregateState>>& global_aggregate_result,
                            std::vector<std::atomic_flag>& intermediate_result_atomics) {
  for (auto chunk_offset = size_t{0}; chunk_offset < chunk_size; ++chunk_offset) {
    ++local_hash_table[tickets[row_index + chunk_offset]].value_count;

    if (local_hash_table.size() >= AGG_MAX_LOCAL_HASH_TABLE_SIZE) {
      spill_local_hash_table_to_global_aggregate_result<ColumnDataType, AggregateType, window_function, AggregateState>(
          local_hash_table, global_aggregate_result, intermediate_result_atomics);
    }
  }
}

// Aggregate a single chunk of a single aggregate.
template <typename ColumnDataType, typename AggregateType, WindowFunction window_function, typename AggregateState>
void _accumulate_job(const uint64_t* const tickets, uint32_t row_index,
                     const std::shared_ptr<AbstractSegment>& aggregate_segment,
                     boost::unordered_flat_map<uint64_t, AggregateState>& local_hash_table,
                     const std::shared_ptr<std::vector<AggregateState>>& global_aggregate_result,
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

        if (hash_table_size >= AGG_MAX_LOCAL_HASH_TABLE_SIZE) {
          spill_local_hash_table_to_global_aggregate_result<ColumnDataType, AggregateType, window_function,
                                                            AggregateState, true>(
              local_hash_table, global_aggregate_result, intermediate_result_atomics);
          hash_table_size = local_hash_table.size();
        }
      });
}

// Accumulates all chunks of all aggregates into the shared per-group intermediate results. Each of the `thread_count`
// jobs round-robins over the aggregates and pulls chunks from a per-aggregate counter, accumulating into a bounded
// thread-local hash table that is spilled into the shared states (guarded by one atomic flag per group).
void _delegate_accumulate(const std::shared_ptr<const Table>& input_table,
                          const std::vector<AggregateInfo>& aggregate_infos, const uint64_t* const tickets,
                          const std::vector<size_t>& chunk_offsets,
                          const std::vector<std::shared_ptr<void>>& intermediate_results, const size_t group_count,
                          const size_t thread_count) {
  const auto aggregate_count = aggregate_infos.size();
  // A pure DISTINCT (group-by without any aggregate) has nothing to accumulate. `job_main` below also assumes at
  // least one aggregate, as it round-robins over them modulo `aggregate_count`.
  if (aggregate_count == 0) {
    return;
  }

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
      const auto& info = aggregate_infos[current_aggregate_id];
      auto& this_column_intermediate_result_atomics = intermediate_result_atomics[current_aggregate_id];

      resolve_data_type(info.data_type, [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;
        resolve_window_function(info.window_function, [&](const auto window_function_t) {
          const auto window_function = decltype(window_function_t)::value;
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, window_function>::ReturnType;
          using AggregateState = IntermediateState<ColumnDataType, window_function>;

          const auto this_column_intermediate_results =
              std::static_pointer_cast<std::vector<AggregateState>>(intermediate_results[current_aggregate_id]);

          // Bounded thread-local hash table of intermediate results.
          auto local_hash_table = boost::unordered_flat_map<uint64_t, AggregateState>{};

          while (true) {
            const auto next_chunk = chunk_id_per_aggregate[current_aggregate_id].fetch_add(1);
            if (next_chunk >= static_cast<size_t>(chunk_count)) {
              break;
            }
            const auto chunk_id = ChunkID{static_cast<ChunkID::base_type>(next_chunk)};
            const auto& chunk = input_table->get_chunk(chunk_id);
            const auto row_index = chunk_offsets[chunk_id];

            if constexpr (window_function == WindowFunction::Count) {
              // COUNT(*) references no input column, so there is no segment to iterate.
              if (info.is_count_star) {
                _accumulate_count_star<ColumnDataType, AggregateType, window_function, AggregateState>(
                    tickets, row_index, chunk->size(), local_hash_table, this_column_intermediate_results,
                    this_column_intermediate_result_atomics);
                continue;
              }
            }

            _accumulate_job<ColumnDataType, AggregateType, window_function, AggregateState>(
                tickets, row_index, chunk->get_segment(info.input_column_id), local_hash_table,
                this_column_intermediate_results, this_column_intermediate_result_atomics);
          }
          // Finally, force a spill of all entries that remain in the local hash table. This also clears the local
          // hashtable.
          spill_local_hash_table_to_global_aggregate_result<ColumnDataType, AggregateType, window_function,
                                                            AggregateState, true>(
              local_hash_table, this_column_intermediate_results, this_column_intermediate_result_atomics);
        });
      });

      current_aggregate_id = (current_aggregate_id + 1) % aggregate_count;
    } while (current_aggregate_id != initial_aggregate_id);
  };

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(thread_count);
  for (auto job_id = size_t{0}; job_id < thread_count; ++job_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, job_id]() {
      job_main(static_cast<uint32_t>(job_id));
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
}

// --------------------------------------------- Group-by: Output ------------------------------------------------

// Performs ANY(column) on a single chunk. Now it is only used for group-by columns with high cardinality.
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

// Builds the group-by output columns and emits them into `output_chunks`.
void _build_groupby_output_columns(const std::shared_ptr<const Table>& input_table,
                                   const std::vector<ColumnID>& groupby_column_ids, const GroupKeyData& groups,
                                   const uint64_t* const tickets, const std::vector<size_t>& chunk_offsets,
                                   const std::vector<std::shared_ptr<BaseChunkedVector>>& final_results,
                                   const std::vector<std::shared_ptr<ChunkedVector<bool>>>& final_result_nulls,
                                   std::vector<Segments>& output_chunks) {
  const auto groupby_column_count = groupby_column_ids.size();
  const auto group_count = groups.group_count;

  // For low-cardinality group-bys (far fewer groups than input rows), each group-by column is cheaper to build by
  // reading every group's value once from its distinct key row in the hash table than by scanning the whole source
  // column. above that ratio the scattered key-row access loses to a sequential source scan. Only the multi-column
  // grouping path exposes a hash table (`has_hash_table`); the single-column fast path recovers group-by values by
  // scanning.
  const auto use_hash_table_for_groupby =
      groups.has_hash_table && group_count * GROUPBY_HASH_TABLE_MIN_ROWS_PER_GROUP <= input_table->row_count();

  // `string_col_index` locates a column among the string group-by columns (see `RowView::string_ptr`).
  auto string_col_index_per_groupby_column = std::vector<size_t>(groupby_column_count, 0);
  {
    auto string_col_index = size_t{0};
    for (auto groupby_index = uint32_t{0}; groupby_index < groupby_column_count; ++groupby_index) {
      string_col_index_per_groupby_column[groupby_index] = string_col_index;
      if (input_table->column_data_type(groupby_column_ids[groupby_index]) == DataType::String) {
        ++string_col_index;
      }
    }
  }

  // `seen` claims a group for one job of the scan path; the hash-table path visits every group exactly once and needs
  // no claim.
  auto groupby_nulls = std::vector<std::vector<uint8_t>>(groupby_column_count);
  auto groupby_seen = std::vector<std::vector<std::atomic_flag>>(groupby_column_count);
  for (auto groupby_index = uint32_t{0}; groupby_index < groupby_column_count; ++groupby_index) {
    groupby_nulls[groupby_index] = std::vector<uint8_t>(group_count, 0);
    if (!use_hash_table_for_groupby) {
      groupby_seen[groupby_index] = std::vector<std::atomic_flag>(group_count);
    }
  }

  // Builds one part of one group-by column: a single chunk of the source column for the scan path, a range of
  // hash-table slots for the hash-table path. The parts of a column write disjoint output slots, and the columns
  // write disjoint `final_results` slots, so all of them run as one flat set of jobs.
  const auto build_groupby_part = [&](const uint32_t groupby_index, const size_t first_part_index,
                                      const size_t end_part_index) {
    const auto groupby_column_id = groupby_column_ids[groupby_index];

    resolve_data_type(input_table->column_data_type(groupby_column_id), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      auto& values = *std::static_pointer_cast<ChunkedVector<ColumnDataType>>(final_results[groupby_index]);
      auto& nulls = groupby_nulls[groupby_index];

      if (use_hash_table_for_groupby) {
        // Low cardinality: read each group's value straight from its hash-table key row.
        groups.global_hash_table.for_each_slot_range(
            first_part_index, end_part_index, [&](const GroupKey& key, const uint64_t ticket) {
              _write_groupby_value_from_key_row(key, ticket, groups.row_format, groupby_index,
                                                string_col_index_per_groupby_column[groupby_index], values, nulls);
            });

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
        // One job per slot range of the grouping hash table. Only the multi-column path builds one.
        const auto slot_count = groups.global_hash_table.capacity();
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

  // Emit the group-by columns. All that is left per column is folding the per-group NULL bytes into the output
  // bitmap, which the jobs above could not write concurrently.
  for (auto groupby_index = uint32_t{0}; groupby_index < groupby_column_count; ++groupby_index) {
    const auto groupby_column_id = groupby_column_ids[groupby_index];
    const auto column_is_nullable = input_table->column_is_nullable(groupby_column_id);

    resolve_data_type(input_table->column_data_type(groupby_column_id), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      auto& values = *std::static_pointer_cast<ChunkedVector<ColumnDataType>>(final_results[groupby_index]);

      if (!column_is_nullable) {
        // Non-nullable columns have no nulls vector (see `_allocate_final_results`).
        _emit_output_column(std::move(values), ChunkedVector<bool>{}, false, output_chunks, groupby_index);
        return;
      }

      auto& nulls = *final_result_nulls[groupby_index];
      const auto& null_bytes = groupby_nulls[groupby_index];
      for (auto group_id = size_t{0}; group_id < group_count; ++group_id) {
        nulls[group_id] = null_bytes[group_id] != 0;
      }

      _emit_output_column(std::move(values), std::move(nulls), true, output_chunks, groupby_index);
    });
  }
}

// Finalize intermediate state into the final result (and NULL vectors).
void _finalize_grouped_aggregates(const std::vector<AggregateInfo>& aggregate_infos, const size_t group_count,
                                  const size_t groupby_column_count,
                                  const std::vector<std::shared_ptr<void>>& intermediate_results,
                                  const std::vector<std::shared_ptr<BaseChunkedVector>>& final_results,
                                  const std::vector<std::shared_ptr<ChunkedVector<bool>>>& final_result_nulls) {
  const auto jobs_per_aggregate = group_count / FINALIZE_ROWS_BATCH_SIZE + 1;
  const auto finalize_job_count = aggregate_infos.size() * jobs_per_aggregate;

  const auto finalize_job_main = [&](const uint32_t job_id) {
    const auto aggregate_id = job_id / jobs_per_aggregate;
    const auto start_row_id = (job_id % jobs_per_aggregate) * FINALIZE_ROWS_BATCH_SIZE;
    const auto end_row_id = std::min(start_row_id + FINALIZE_ROWS_BATCH_SIZE, group_count);
    const auto& info = aggregate_infos[aggregate_id];

    resolve_data_type(info.data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      resolve_window_function(info.window_function, [&](const auto window_function_t) {
        const auto window_function = decltype(window_function_t)::value;
        using AggregateType = typename WindowFunctionTraits<ColumnDataType, window_function>::ReturnType;
        using AggregateState = IntermediateState<ColumnDataType, window_function>;

        const auto this_column_intermediate_results =
            std::static_pointer_cast<std::vector<AggregateState>>(intermediate_results[aggregate_id]);
        const auto final_result_vector =
            std::static_pointer_cast<ChunkedVector<AggregateType>>(final_results[groupby_column_count + aggregate_id]);
        // nullptr for aggregates whose output column is not nullable (COUNT, COUNT DISTINCT); their `is_null` is
        // always false.
        const auto& final_result_nulls_vector = final_result_nulls[groupby_column_count + aggregate_id];

        for (auto row_id = start_row_id; row_id < end_row_id; ++row_id) {
          const auto& intermediate_result = (*this_column_intermediate_results)[row_id];
          const auto [value, is_null] = intermediate_result.finalize();
          (*final_result_vector)[row_id] = value;
          if (final_result_nulls_vector) {
            (*final_result_nulls_vector)[row_id] = is_null;
          }
        }
      });
    });
  };

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(finalize_job_count);
  for (auto job_id = size_t{0}; job_id < finalize_job_count; ++job_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, job_id]() {
      finalize_job_main(static_cast<uint32_t>(job_id));
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
}

// Transform the ChunkedVector into segments of the output chunks.
void _emit_aggregate_columns(const std::vector<AggregateInfo>& aggregate_infos,
                             const TableColumnDefinitions& column_definitions, const size_t groupby_column_count,
                             const std::vector<std::shared_ptr<BaseChunkedVector>>& final_results,
                             const std::vector<std::shared_ptr<ChunkedVector<bool>>>& final_result_nulls,
                             std::vector<Segments>& output_chunks) {
  const auto aggregate_count = aggregate_infos.size();
  for (auto aggregate_id = size_t{0}; aggregate_id < aggregate_count; ++aggregate_id) {
    const auto output_column_id = groupby_column_count + aggregate_id;
    const auto& info = aggregate_infos[aggregate_id];

    resolve_data_type(info.data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      resolve_window_function(info.window_function, [&](const auto window_function_t) {
        using AggregateType =
            typename WindowFunctionTraits<ColumnDataType, decltype(window_function_t)::value>::ReturnType;
        const auto final_result_vector =
            std::static_pointer_cast<ChunkedVector<AggregateType>>(final_results[output_column_id]);
        const auto& final_result_nulls_vector = final_result_nulls[output_column_id];

        _emit_output_column(std::move(*final_result_vector),
                            final_result_nulls_vector ? std::move(*final_result_nulls_vector) : ChunkedVector<bool>{},
                            column_definitions[output_column_id].nullable, output_chunks, output_column_id);
      });
    });
  }
}

std::shared_ptr<const Table> AggregateDYOD::groupby_aggregate() {
  const auto input_table = left_input_table();
  const auto aggregate_count = _aggregates.size();
  const auto groupby_column_count = _groupby_column_ids.size();

  // TODO(@forUnity): decide this elsewhere and make sure this is correct
  const auto thread_count = std::max(size_t{1}, Hyrise::get().topology.num_cpus() - 1);

  auto groups = _compute_groups(_groupby_column_ids, input_table);
  const auto group_count = groups->group_count;
  const auto* const tickets = groups->tickets.get();

  const auto aggregate_infos = _build_aggregate_infos(_aggregates, input_table);

  // The output schema is [group-by columns..., aggregate columns...]. Here we only define the columns; the group-by
  // output segments and the aggregate segments are each filled by their own jobs.
  const auto column_definitions =
      _build_output_column_definitions(*input_table, _groupby_column_ids, _aggregates, aggregate_infos);

  // Output layout: `output_chunks[chunk][column]`, where the group-by columns occupy the first `groupby_column_count`
  // column slots, followed by one slot per aggregate. Every job produces its column directly as chunk-sized pieces
  // (`ChunkedVector`) and emits them into its fixed column slot of every chunk (`_emit_output_column`), so none of
  // them touch a shared, growing container and the final table assembly is move-only.
  const auto result_column_count = groupby_column_count + aggregate_count;
  const auto output_chunk_count = (group_count + TARGET_CHUNK_SIZE - 1) / TARGET_CHUNK_SIZE;
  auto output_chunks = std::vector<Segments>(output_chunk_count, Segments(result_column_count));

  const auto intermediate_results = _allocate_intermediate_results(aggregate_infos, group_count);

  // The final result columns are first read after the accumulate phase (by `_build_groupby_output_columns` and
  // `_finalize_grouped_aggregates`), so their allocation jobs run concurrently with it and stay off the critical path.
  auto final_result_allocation_jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  const auto [final_results, final_result_nulls] =
      _allocate_final_results(column_definitions, group_count, final_result_allocation_jobs);
  for (const auto& job : final_result_allocation_jobs) {
    job->schedule();
  }

  const auto chunk_count = input_table->chunk_count();
  auto chunk_offsets = std::vector<size_t>(chunk_count, 0);
  auto row_offset = size_t{0};
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    chunk_offsets[chunk_id] = row_offset;
    row_offset += input_table->get_chunk(chunk_id)->size();
  }

  _delegate_accumulate(input_table, aggregate_infos, tickets, chunk_offsets, intermediate_results, group_count,
                       thread_count);

  AbstractScheduler::wait_for_tasks(final_result_allocation_jobs);

  _build_groupby_output_columns(input_table, _groupby_column_ids, *groups, tickets, chunk_offsets, final_results,
                                final_result_nulls, output_chunks);

  _finalize_grouped_aggregates(aggregate_infos, group_count, groupby_column_count, intermediate_results, final_results,
                               final_result_nulls);

  _emit_aggregate_columns(aggregate_infos, column_definitions, groupby_column_count, final_results, final_result_nulls,
                          output_chunks);

  auto cleanup_job = std::make_shared<JobTask>([groups = std::move(groups)]() mutable {
    groups.reset();
  });
  cleanup_job->schedule();

  auto result_table = std::make_shared<Table>(column_definitions, TableType::Data);
  for (auto& chunk_segments : output_chunks) {
    result_table->append_chunk(std::move(chunk_segments));
  }
  return result_table;
}

// --------------------------------------------- Operator --------------------------------------------------------

AggregateDYOD::AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                             const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                             const std::vector<ColumnID>& groupby_column_ids)
    : AbstractAggregateOperator(input_operator, aggregates, groupby_column_ids) {}

const std::string& AggregateDYOD::name() const {
  static const auto name = std::string{"AggregateDYOD"};
  return name;
}

std::shared_ptr<AbstractOperator> AggregateDYOD::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<AggregateDYOD>(copied_left_input, _aggregates, _groupby_column_ids);
}

std::shared_ptr<const Table> AggregateDYOD::_on_execute() {
  // Fallback to the standard aggregate operator if the code is running concurrently.
  const auto is_immediate_scheduler =
      std::dynamic_pointer_cast<ImmediateExecutionScheduler>(Hyrise::get().scheduler()) != nullptr;
  if (is_immediate_scheduler && !_groupby_column_ids.empty()) {
    const auto fallback_operator =
        std::make_shared<AggregateHash>(mutable_left_input(), _aggregates, _groupby_column_ids);
    fallback_operator->execute();
    return fallback_operator->get_output();
  }
  _validate_aggregates();

  if (_groupby_column_ids.empty()) {
    return no_groupby_aggregate();
  }
  return groupby_aggregate();
}

void AggregateDYOD::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void AggregateDYOD::_on_cleanup() {}

}  // namespace hyrise
