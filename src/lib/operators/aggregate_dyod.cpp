#include "aggregate_dyod.hpp"

#include <algorithm>
#include <atomic>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <boost/iterator/indirect_iterator.hpp>

#include "aggregate/aggregate_vector.hpp"
#include "aggregate/resolve_window_function.hpp"
#include "aggregate/serialize.hpp"
#include "aggregate/window_function_traits.hpp"
#include "aggregate/types.hpp"
#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "hyrise.hpp"
#include "operator_state.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_operator.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/pos_lists/entire_chunk_pos_list.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

WorkerState::WorkerState(const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                         const std::function<std::pair<GroupID, GroupID>()>& reserve_new_group_id_range)
    : _reserve_new_group_id_range{reserve_new_group_id_range} {
  const auto [initial_next_group_id, initial_max_group_id] = _reserve_new_group_id_range();
  _next_group_id = initial_next_group_id;
  _max_group_id = initial_max_group_id;

  const auto aggregate_count = aggregates.size();
  _vectors.resize(aggregate_count);

  // Initialize a TypedAggregateVector for each aggregate
  for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
    const auto& aggregate = aggregates[aggregate_index];
    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto data_type = pqp_column.data_type();

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      resolve_window_function(aggregate->window_function, [&](auto type) {
        constexpr auto AGGREGATE_FUNCTION = decltype(type)::value;
        _vectors[aggregate_index] = std::make_unique<TypedAggregateVector<ColumnDataType, AGGREGATE_FUNCTION>>();
      });
    });
  }
}

void WorkerState::merge(WorkerState& other) {
  const auto size = _vectors.size();

  for (auto index = size_t{0}; index < size; ++index) {
    _vectors[index]->merge(other.aggregate_vector(index));
  }
}

GroupID WorkerState::next_group_id() {
  // Reserve new group ID range if there are no remaining locally reserved group IDs
  if (_next_group_id > _max_group_id) {
    auto [next_group_id, max_group_id] = _reserve_new_group_id_range();
    _next_group_id = next_group_id;
    _max_group_id = max_group_id;
  }

  return _next_group_id++;
}

AbstractAggregateVector& WorkerState::aggregate_vector(const size_t index) {
  return *_vectors[index];
}

std::vector<std::unique_ptr<AbstractAggregateVector>>& WorkerState::aggregate_vectors() {
  return _vectors;
}

AggregateDYOD::AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                             const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                             const std::vector<ColumnID>& groupby_column_ids)
    : AbstractAggregateOperator(input_operator, aggregates, groupby_column_ids) {}

const std::string& AggregateDYOD::name() const {
  static const auto name = std::string{"AggregateDYOD"};
  return name;
}

std::shared_ptr<const Table> AggregateDYOD::_on_execute() {
  const auto input_table = left_input_table();

  _validate_aggregates();

  // Initialize state depending on scheduler type
  if (Hyrise::get().is_multi_threaded()) {
    _state.emplace<MultiThreadedState>();
  } else {
    _state.emplace<SingleThreadedState>();
  }

  // clang-format off
  std::visit([&](auto& state) {
    state.row_ids.reserve(GROUP_ID_INITIAL_CARDINALITY);
    state.occupied_group_ids.reserve(GROUP_ID_INITIAL_CARDINALITY);
  }, _state);
  // clang-format on

  const auto chunk_count = input_table->chunk_count();

  // Initialize a byte buffer for every chunk
  _group_key_buffers.resize(chunk_count);

  const auto reserve_new_group_id_range = [&]() {
    return _reserve_new_group_id_range();
  };
  auto state = OperatorSharedState<WorkerState>{_aggregates, reserve_new_group_id_range};
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>(chunk_count);

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    jobs[chunk_id] = std::make_shared<JobTask>([&, chunk_id]() {
      auto& worker_state = state.current_worker_state();
      const auto chunk = input_table->get_chunk(chunk_id);
      _aggregate_chunk(worker_state, chunk_id, *chunk);
    });
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  // If we haven't run any jobs, attempting to merge the worker states would fail
  auto merged_worker_states = [&]() {
    return jobs.empty() ? WorkerState(_aggregates, reserve_new_group_id_range) : std::move(state.merge_worker_states());
  }();

  // SQL requires a single output row if the input table is empty and there is no GROUP BY clause.
  // We ensure this by inserting a single group into the group ID mapping before writing the output table.
  std::visit(
      [&](auto& state) {
        if (state.group_id_map.empty() && _groupby_column_ids.empty()) {
          // Use dummy row IDs and group key. There are no groupby columns, so the they are never needed anyway.
          auto dummy_row_ids = RowIDs(0);
          auto dummy_group_key = GroupKey{};
          const auto group_id = _group_id(state, dummy_row_ids, dummy_group_key, merged_worker_states);

          for (auto& aggregate_vector : merged_worker_states.aggregate_vectors()) {
            aggregate_vector->grow_if_necessary(group_id + 1);
          }
        }
      },
      _state);

  return _write_output_table(merged_worker_states);
}

std::shared_ptr<Table> AggregateDYOD::_write_output_table(WorkerState& worker_state) {
  // We use ReferenceSegments referencing the input table for groupby columns, and ValueSegments for aggregate
  // columns. Hyrise requires that a table contains either only ReferenceSegments or only ValueSegments. Therefore,
  // we first create a temporary table containing only the ValueSegments for aggregate columns. The final output
  // table contains ReferenceSegments pointing to the input table (for groupby columns) or this temporary table
  // (for aggregate columns).

  const auto aggregate_count = _aggregates.size();
  const auto total_group_count = std::visit(
      [&](auto& state) {
        return state.group_id_map.size();
      },
      _state);
  const auto chunk_count = (total_group_count + Chunk::DEFAULT_SIZE - 1) / Chunk::DEFAULT_SIZE;
  const auto occupied_group_ids = _get_occupied_group_ids();

  // Chunks for the temporary data table containing aggregate columns
  auto aggregate_chunks = std::vector<std::shared_ptr<Chunk>>(chunk_count);

  if (total_group_count > 0 && aggregate_count > 0) {
    auto jobs = std::vector<std::shared_ptr<AbstractTask>>(chunk_count);

    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      jobs[chunk_id] = std::make_shared<JobTask>([&, chunk_id]() {
        const auto start_index = size_t{chunk_id} * Chunk::DEFAULT_SIZE;
        const auto end_index = std::min(start_index + Chunk::DEFAULT_SIZE, total_group_count);
        aggregate_chunks[chunk_id] =
            _write_aggregate_output_chunk(worker_state, occupied_group_ids, start_index, end_index);
      });
    }

    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  }

  // Construct the temporary data table for aggregate columns
  auto aggregates_table =
      std::make_shared<Table>(_aggregate_column_definitions(), TableType::Data, std::move(aggregate_chunks));

  // Chunks for the actual output reference table.
  auto reference_chunks = std::vector<std::shared_ptr<Chunk>>(chunk_count);

  if (total_group_count > 0) {
    auto jobs = std::vector<std::shared_ptr<AbstractTask>>(chunk_count);

    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      jobs[chunk_id] = std::make_shared<JobTask>([&, chunk_id]() {
        const auto start_index = size_t{chunk_id} * Chunk::DEFAULT_SIZE;
        const auto end_index = std::min(start_index + Chunk::DEFAULT_SIZE, total_group_count);
        reference_chunks[chunk_id] =
            _write_reference_output_chunk(aggregates_table, chunk_id, occupied_group_ids, start_index, end_index);
      });
    }

    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  }

  const auto column_definitions = _output_column_definitions();
  return std::make_shared<Table>(column_definitions, TableType::References, std::move(reference_chunks));
}

TableColumnDefinitions AggregateDYOD::_output_column_definitions() const {
  auto column_definitions = _groupby_column_definitions();
  const auto aggregate_column_definitions = _aggregate_column_definitions();
  column_definitions.insert(column_definitions.end(), aggregate_column_definitions.begin(),
                            aggregate_column_definitions.end());
  return column_definitions;
}

TableColumnDefinitions AggregateDYOD::_groupby_column_definitions() const {
  const auto input_table = left_input_table();
  auto column_definitions = TableColumnDefinitions{};

  column_definitions.reserve(_groupby_column_ids.size());
  for (const auto column_id : groupby_column_ids()) {
    column_definitions.emplace_back(input_table->column_name(column_id), input_table->column_data_type(column_id),
                                    input_table->column_is_nullable(column_id));
  }

  return column_definitions;
}

TableColumnDefinitions AggregateDYOD::_aggregate_column_definitions() const {
  auto column_definitions = TableColumnDefinitions{};
  column_definitions.reserve(_aggregates.size());

  for (auto aggregate_index = size_t{0}; aggregate_index < _aggregates.size(); ++aggregate_index) {
    const auto& aggregate = _aggregates[aggregate_index];
    resolve_data_type(_aggregate_column_data_type(aggregate_index), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      resolve_window_function(aggregate->window_function, [&](auto type) {
        constexpr auto AGGREGATE_FUNCTION = decltype(type)::value;
        const auto data_type = WindowFunctionTraits<ColumnDataType, AGGREGATE_FUNCTION>::RESULT_TYPE;

        // The expected name of `ANY(my_col)` is `any` and not `ANY(my_col)` as for all other aggregates.
        const auto name = aggregate->window_function == WindowFunction::Any ? _aggregate_column_name(aggregate_index)
                                                                            : aggregate->as_column_name();
        column_definitions.emplace_back(name, data_type, _aggregate_is_nullable(aggregate_index));
      });
    });
  }

  return column_definitions;
}

std::shared_ptr<Chunk> AggregateDYOD::_write_aggregate_output_chunk(WorkerState& worker_state,
                                                                    const std::vector<size_t>& occupied_group_ids,
                                                                    const size_t start_index, const size_t end_index) {
  const auto aggregate_count = _aggregates.size();

  auto segments = Segments{};
  segments.reserve(aggregate_count);

  for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
    const auto& aggregate = _aggregates[aggregate_index];

    resolve_data_type(_aggregate_column_data_type(aggregate_index), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      resolve_window_function(aggregate->window_function, [&](auto type) {
        constexpr auto AGGREGATE_FUNCTION = decltype(type)::value;
        auto& aggregate_vector = static_cast<TypedAggregateVector<ColumnDataType, AGGREGATE_FUNCTION>&>(
            worker_state.aggregate_vector(aggregate_index));
        segments.emplace_back(_write_aggregate_segment<ColumnDataType, AGGREGATE_FUNCTION>(
            aggregate_vector, _aggregate_is_nullable(aggregate_index), occupied_group_ids, start_index, end_index));
      });
    });
  }

  return std::make_shared<Chunk>(segments);
}

std::shared_ptr<Chunk> AggregateDYOD::_write_reference_output_chunk(const std::shared_ptr<Table>& aggregates_table,
                                                                    ChunkID chunk_id,
                                                                    const std::vector<size_t>& occupied_group_ids,
                                                                    const size_t start_index, const size_t end_index) {
  const auto groupby_column_count = _groupby_column_ids.size();
  const auto aggregate_count = _aggregates.size();
  const auto chunk_size = static_cast<ChunkOffset>(end_index - start_index);

  auto segments = Segments{};
  segments.reserve(groupby_column_count + aggregate_count);

  // Append a ReferenceSegment (referencing the input table) for each groupby column
  for (auto groupby_column_index = size_t{0}; groupby_column_index < groupby_column_count; ++groupby_column_index) {
    segments.emplace_back(_write_groupby_segment(groupby_column_index, occupied_group_ids, start_index, end_index));
  }

  // Append a ReferenceSegment (referencing the temporary aggregates table) for each aggregate
  const auto entire_chunk_pos_list = std::make_shared<EntireChunkPosList>(chunk_id, chunk_size);
  for (auto aggregate_index = ColumnID{0}; aggregate_index < aggregate_count; ++aggregate_index) {
    segments.emplace_back(
        std::make_shared<ReferenceSegment>(aggregates_table, ColumnID{aggregate_index}, entire_chunk_pos_list));
  }

  return std::make_shared<Chunk>(segments);
}

std::shared_ptr<AbstractSegment> AggregateDYOD::_write_groupby_segment(const size_t groupby_column_index,
                                                                       const std::vector<size_t>& occupied_group_ids,
                                                                       const size_t start_index,
                                                                       const size_t end_index) {
  const auto input_table = left_input_table();
  const auto column_id = _groupby_column_ids[groupby_column_index];
  const auto chunk_size = end_index - start_index;

  auto referenced_table = input_table;
  auto referenced_column_id = _groupby_column_ids[groupby_column_index];

  if (input_table->type() == TableType::References) {
    // Unless we are processing an empty input, obtain the referenced table and column from the first chunk. We
    // assume that segments of the same column do not reference different tables (checked in the Table constructor).
    // When this assumption changes (e.g., due to a better support of Unions), this code needs to be revisited.
    // This is the same assumption also made in AggregateHash.
    const auto& first_reference_segment =
        static_cast<const ReferenceSegment&>(*input_table->get_chunk(ChunkID{0})->get_segment(column_id));
    referenced_table = first_reference_segment.referenced_table();
    referenced_column_id = first_reference_segment.referenced_column_id();
  }

  auto row_ids = pmr_vector<RowID>(chunk_size);

  // clang-format off
  std::visit([&](auto& state) {
    for (auto index = start_index; index < end_index; ++index) {
      const auto group_id = occupied_group_ids[index];
      const auto chunk_offset = index - start_index;
      row_ids[chunk_offset] = state.row_ids[group_id][groupby_column_index];
    }
  }, _state);
  // clang-format on

  const auto pos_list = std::make_shared<const RowIDPosList>(std::move(row_ids));
  return std::make_shared<ReferenceSegment>(referenced_table, referenced_column_id, pos_list);
}

template <typename ColumnDataType, WindowFunction aggregate_function>
  requires(aggregate_function == WindowFunction::Avg && std::is_arithmetic_v<ColumnDataType>)
std::shared_ptr<AbstractSegment> AggregateDYOD::_write_aggregate_segment(
    TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, bool /*is_nullable*/,
    const std::vector<size_t>& occupied_group_ids, const size_t start_index, const size_t end_index) {
  using AggregateDataType = typename WindowFunctionTraits<ColumnDataType, aggregate_function>::ReturnType;
  const auto& sums = aggregate_vector.accumulators();
  const auto& counts = aggregate_vector.counts();
  const auto chunk_size = end_index - start_index;

  auto averages = pmr_vector<AggregateDataType>(chunk_size);
  auto null_values = pmr_vector<bool>(chunk_size);

  for (auto index = start_index; index < end_index; ++index) {
    const auto group_id = occupied_group_ids[index];
    const auto chunk_offset = index - start_index;
    if (counts[group_id] == 0) {
      null_values[chunk_offset] = true;
    } else {
      // TODO(anyone): The maximum representable RowID in Hyrise is 2^64 (minus a few reserved sentinel values).
      // So in theory, the count could exceed the range of double, although in practice, it is rather unlikely.
      averages[chunk_offset] = sums[group_id] / static_cast<double>(counts[group_id]);
    }
  }

  return std::make_shared<ValueSegment<AggregateDataType>>(std::move(averages), std::move(null_values));
}

template <typename ColumnDataType, WindowFunction aggregate_function>
  requires(aggregate_function == WindowFunction::Count)
std::shared_ptr<AbstractSegment> AggregateDYOD::_write_aggregate_segment(
    TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, bool /*is_nullable*/,
    const std::vector<size_t>& occupied_group_ids, const size_t start_index, const size_t end_index) {
  using AggregateDataType = typename WindowFunctionTraits<ColumnDataType, aggregate_function>::ReturnType;
  const auto& counts = aggregate_vector.counts();
  auto values = pmr_vector<AggregateDataType>(end_index - start_index);

  for (auto index = start_index; index < end_index; ++index) {
    const auto chunk_offset = index - start_index;
    const auto group_id = occupied_group_ids[index];
    values[chunk_offset] = counts[group_id];
  }

  return std::make_shared<ValueSegment<AggregateDataType>>(std::move(values));
}

template <typename ColumnDataType, WindowFunction aggregate_function>
  requires(aggregate_function == WindowFunction::CountDistinct)
std::shared_ptr<AbstractSegment> AggregateDYOD::_write_aggregate_segment(
    TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, bool /*is_nullable*/,
    const std::vector<size_t>& occupied_group_ids, const size_t start_index, const size_t end_index) {
  using AggregateDataType = typename WindowFunctionTraits<ColumnDataType, aggregate_function>::ReturnType;
  const auto chunk_size = end_index - start_index;
  const auto& distinct_values = aggregate_vector.accumulators();
  auto values = pmr_vector<AggregateDataType>(chunk_size);

  for (auto index = start_index; index < end_index; ++index) {
    const auto group_id = occupied_group_ids[index];
    const auto chunk_offset = index - start_index;
    values[chunk_offset] = distinct_values[group_id].size();
  }

  return std::make_shared<ValueSegment<AggregateDataType>>(std::move(values));
}

template <typename ColumnDataType, WindowFunction aggregate_function>
std::shared_ptr<AbstractSegment> AggregateDYOD::_write_aggregate_segment(
    TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, bool is_nullable,
    const std::vector<size_t>& occupied_group_ids, const size_t start_index, const size_t end_index) {
  using AggregateDataType = typename WindowFunctionTraits<ColumnDataType, aggregate_function>::ReturnType;
  const auto chunk_size = end_index - start_index;
  auto& aggregate_values = aggregate_vector.accumulators();

  if (is_nullable) {
    auto values = pmr_vector<AggregateDataType>(chunk_size);
    auto null_values = pmr_vector<bool>(chunk_size);
    const auto& counts = aggregate_vector.counts();

    for (auto index = start_index; index < end_index; ++index) {
      const auto group_id = occupied_group_ids[index];
      const auto chunk_offset = index - start_index;
      if (counts[group_id] == 0) {
        null_values[chunk_offset] = true;
      } else {
        // Move aggregate values in case AggregateDataType is not trivially copyable
        values[chunk_offset] = std::move(aggregate_values[group_id]);
      }
    }
    return std::make_shared<ValueSegment<AggregateDataType>>(std::move(values), std::move(null_values));
  }

  auto values = pmr_vector<AggregateDataType>(chunk_size);
  for (auto index = start_index; index < end_index; ++index) {
    const auto group_id = occupied_group_ids[index];
    values[index - start_index] = std::move(aggregate_values[group_id]);
  }
  return std::make_shared<ValueSegment<AggregateDataType>>(std::move(values));
}

template <typename StateType>
GroupID AggregateDYOD::_group_id(StateType& state, RowIDs& row_ids, const GroupKey& group_key,
                                 WorkerState& worker_state) {
  auto it = state.group_id_map.find(group_key);
  if (it != state.group_id_map.end()) {
    return it->second;
  }

  // TODO(anyone): This may allocate a group ID even if there was a race and another thread inserted the key first.
  // Not a correctness problem, but it might hurt performance.
  const auto& [insert_it, inserted] = state.group_id_map.insert({group_key, worker_state.next_group_id()});
  const auto group_id = insert_it->second;

  if (inserted) {
    state.row_ids[group_id] = std::move(row_ids);
    state.occupied_group_ids[group_id] = true;
  }

  return group_id;
}

std::pair<GroupID, GroupID> AggregateDYOD::_reserve_new_group_id_range() {
  // clang-format off
  return std::visit([&](auto& state) {
    return _reserve_new_group_id_range(state);
  }, _state);
  // clang-format on
}

std::pair<GroupID, GroupID> AggregateDYOD::_reserve_new_group_id_range(SingleThreadedState& state) {
  // TODO(anyone): In theory, the fuzzy ticketing is unnecessary when the aggregator is executed on a single-thread.
  state.next_group_id += FUZZY_STEP_SIZE;
  auto max_group_id = state.next_group_id + FUZZY_STEP_SIZE - 1;

  state.row_ids.resize(max_group_id + 1);
  state.occupied_group_ids.resize(max_group_id + 1);

  return {state.next_group_id, max_group_id};
}

std::pair<GroupID, GroupID> AggregateDYOD::_reserve_new_group_id_range(MultiThreadedState& state) {
  auto next_group_id = state.next_group_id.fetch_add(FUZZY_STEP_SIZE, std::memory_order_relaxed);
  auto max_group_id = next_group_id + FUZZY_STEP_SIZE - 1;

  {
    // TODO(anyone): Figure out how to avoid this lock. The two parallel vectors need to be resized
    // synchronously. Using a single vector where each element stores row IDs and and occupied flag
    // has worse performance.
    const std::lock_guard<std::mutex> lock(state.lock);
    state.row_ids.grow_to_at_least(max_group_id + 1);
    state.occupied_group_ids.grow_to_at_least(max_group_id + 1);
  }

  return {next_group_id, max_group_id};
}

std::vector<size_t> AggregateDYOD::_get_occupied_group_ids() {
  // clang-format off
  return std::visit([&](auto& state) {
    auto view = std::views::iota(size_t{0}, state.occupied_group_ids.size())
      | std::views::filter([&](size_t index) { return state.occupied_group_ids[index]; });
    return std::vector<size_t>(view.begin(), view.end());
  }, _state);
  // clang-format on
}

void AggregateDYOD::_aggregate_chunk(WorkerState& worker_state, ChunkID chunk_id, const Chunk& chunk) {
  const auto [group_ids, max_group_id] = _group_ids_for_chunk(chunk_id, chunk, worker_state);

  // Grow the aggregate vectors once per chunk (to the size needed to contain the maximum group ID)
  if (!group_ids.empty()) {
    for (auto& aggregate_vector : worker_state.aggregate_vectors()) {
      aggregate_vector->grow_if_necessary(max_group_id + 1);
    }
  }

  // Compute aggregates
  const auto aggregate_count = _aggregates.size();
  for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
    const auto& aggregate = _aggregates[aggregate_index];

    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto column_id = pqp_column.column_id;

    // COUNT(*): Skip the generic path and just count the number of rows in each group
    if (aggregate->window_function == WindowFunction::Count && column_id == INVALID_COLUMN_ID) {
      _aggregate_count_star(worker_state.aggregate_vector(aggregate_index), group_ids);
      continue;
    }

    const auto segment = chunk.get_segment(column_id);

    resolve_data_type(pqp_column.data_type(), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      resolve_window_function(aggregate->window_function, [&](auto type) {
        constexpr auto AGGREGATE_FUNCTION = decltype(type)::value;
        auto& aggregate_vector = static_cast<TypedAggregateVector<ColumnDataType, AGGREGATE_FUNCTION>&>(
            worker_state.aggregate_vector(aggregate_index));
        _aggregate_segment<ColumnDataType, AGGREGATE_FUNCTION>(aggregate_vector, *segment, group_ids);
      });
    });
  }
}

std::pair<std::vector<GroupID>, GroupID> AggregateDYOD::_group_ids_for_chunk(ChunkID chunk_id, const Chunk& chunk,
                                                                             WorkerState& worker_state) {
  // This function computes group IDs for each row in a chunk. We do this in two stages:
  //
  // 1. Iterate through the segments corresponding to the groupby columns and write the serialized values
  //    to a separate byte buffer per column. For each row, keep track of the position in the column buffer.
  // 2. For every row, store the serialized values of all groupby columns in a single byte buffer. The group
  //    key is a std::span referencing the subslice of the buffer. Use the group key to get the group ID.

  const auto input_table = left_input_table();
  const auto groupby_column_count = _groupby_column_ids.size();
  const auto row_count = chunk.size();

  // For input columns that are reference segments, store the dereferenced RowIDs.
  auto column_row_ids = std::vector<std::optional<std::vector<RowID>>>(groupby_column_count);

  // Per grouping column, the serialized values of all rows in a chunk
  auto column_buffers = std::vector<std::vector<std::byte>>(groupby_column_count);

  // Per grouping column, the start position of each row into the respective column buffer
  using ColumnStart = uint32_t;
  auto column_starts = std::vector<std::vector<ColumnStart>>(groupby_column_count,
                                                             std::vector<ColumnStart>(row_count + 1, ColumnStart{0}));

  // Stage 1: Serialize the group keys within each column and append it to the respective column buffer.
  for (auto groupby_column_index = size_t{0}; groupby_column_index < groupby_column_count; ++groupby_column_index) {
    const auto groupby_column_id = _groupby_column_ids[groupby_column_index];
    const auto data_type = input_table->column_data_type(groupby_column_id);
    const auto is_nullable = input_table->column_is_nullable(groupby_column_id);
    auto& column_buffer = column_buffers[groupby_column_index];
    auto& starts = column_starts[groupby_column_index];

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      const auto segment = chunk.get_segment(groupby_column_id);

      segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
        const auto chunk_offset = position.chunk_offset();
        starts[chunk_offset] = static_cast<ColumnStart>(column_buffer.size());

        if (is_nullable) {
          serialize_value(column_buffer, position.value(), position.is_null());
        } else {
          serialize_value(column_buffer, position.value());
        }
      });

      const auto reference_segment = std::dynamic_pointer_cast<const ReferenceSegment>(segment);

      DebugAssert(column_buffer.size() <= std::numeric_limits<ColumnStart>::max(), "Column buffer is too large.");

      // Store the end position of the last entry (so we can compute the length of each entry).
      starts[row_count] = static_cast<ColumnStart>(column_buffer.size());

      if (reference_segment) {
        // If the segment is a ReferenceSegment, iterate through the position list and store the dereferenced RowIDs.
        // We need to store RowIDs for each GroupID to be able to construct the groupby columns in the output table.
        auto row_ids = std::vector<RowID>(row_count);
        auto chunk_offset = ChunkOffset{0};

        resolve_pos_list_type(reference_segment->pos_list(), [&](const auto& pos_list) {
          for (const auto row_id : *pos_list) {
            row_ids[chunk_offset++] = row_id;
          }
        });

        column_row_ids[groupby_column_index] = std::optional{std::move(row_ids)};
      }
    });
  }

  // Stage 2: Assemble the group keys per row and get the GroupID.
  auto group_ids = std::vector<GroupID>(row_count);
  auto max_group_id = GroupID{0};

  // One buffer for all group keys in a chunk
  auto& chunk_buffer = _group_key_buffers[chunk_id];

  using LengthPrefix = uint32_t;

  // Reserve capacity in the chunk buffer first so that spans referencing it are stable.
  auto total_bytes = groupby_column_count * row_count * sizeof(LengthPrefix);

  for (const auto& column_buffer : column_buffers) {
    total_bytes += column_buffer.size();
  }

  chunk_buffer.reserve(total_bytes);

  std::visit(
      [&](auto& state) {
        for (auto offset = ChunkOffset{0}; offset < row_count; ++offset) {
          const auto chunk_buffer_start = chunk_buffer.size();

          for (auto groupby_column_index = size_t{0}; groupby_column_index < groupby_column_count;
               ++groupby_column_index) {
            const auto& column_buffer = column_buffers[groupby_column_index];
            const auto& column_buffer_starts = column_starts[groupby_column_index];

            const auto column_buffer_start = column_buffer_starts[offset];
            const auto column_buffer_end = column_buffer_starts[offset + 1];

            // TODO(anyone): 16-byte int might be enough for the key lenght prefix
            DebugAssert(column_buffer_end - column_buffer_start <= std::numeric_limits<LengthPrefix>::max(),
                        "Key entry is too long.");
            const auto length = static_cast<LengthPrefix>(column_buffer_end - column_buffer_start);

            // Copy serialized group key entries from the local column buffers to the global chunk buffer
            // and prefix them with their length to prevent collisions.
            const auto length_bytes = std::bit_cast<std::array<std::byte, sizeof(LengthPrefix)>>(length);
            chunk_buffer.insert(chunk_buffer.end(), length_bytes.begin(), length_bytes.end());
            chunk_buffer.insert(chunk_buffer.end(), column_buffer.begin() + column_buffer_start,
                                column_buffer.begin() + column_buffer_end);
          }

          const auto chunk_buffer_end = chunk_buffer.size();
          const auto group_key = std::span<const std::byte>(chunk_buffer)
                                     .subspan(chunk_buffer_start, chunk_buffer_end - chunk_buffer_start);

          auto row_ids = RowIDs(groupby_column_count);

          for (auto groupby_column_index = size_t{0}; groupby_column_index < groupby_column_count;
               ++groupby_column_index) {
            if (column_row_ids[groupby_column_index].has_value()) {
              row_ids[groupby_column_index] = column_row_ids[groupby_column_index].value()[offset];
            } else {
              row_ids[groupby_column_index] = RowID{chunk_id, offset};
            }
          }

          const auto group_id = _group_id(state, row_ids, group_key, worker_state);
          group_ids[offset] = group_id;
          max_group_id = std::max(max_group_id, group_id);
        }
      },
      _state);

  Assert(chunk_buffer.size() <= total_bytes,
         "Chunk buffer is larger than initially reserved capacity which may have invalidated group key spans.");

  return {group_ids, max_group_id};
}

template <typename ColumnDataType, WindowFunction aggregate_function>
void AggregateDYOD::_aggregate_segment(TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector,
                                       const AbstractSegment& segment, const std::vector<GroupID>& group_ids) {
  using AggregateDataType = typename WindowFunctionTraits<ColumnDataType, aggregate_function>::ReturnType;
  auto aggregator =
      WindowFunctionBuilder<ColumnDataType, AggregateDataType, aggregate_function>().get_aggregate_function();
  segment_iterate<ColumnDataType>(segment, [&](const auto& position) {
    if (!position.is_null()) {
      const auto group_id = group_ids[position.chunk_offset()];
      aggregator(position.value(), aggregate_vector.count(group_id), aggregate_vector.accumulator(group_id));
      aggregate_vector.increment_count(group_id);
    }
  });
}

template <typename ColumnDataType, WindowFunction aggregate_function>
  requires(aggregate_function == WindowFunction::CountDistinct)
void AggregateDYOD::_aggregate_segment(TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector,
                                       const AbstractSegment& segment, const std::vector<GroupID>& group_ids) {
  segment_iterate<ColumnDataType>(segment, [&](const auto& position) {
    if (!position.is_null()) {
      const auto group_id = group_ids[position.chunk_offset()];
      aggregate_vector.accumulator(group_id).insert(position.value());
    }
  });
}

template <typename ColumnDataType, WindowFunction aggregate_function>
  requires(aggregate_function == WindowFunction::Any)
void AggregateDYOD::_aggregate_segment(TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector,
                                       const AbstractSegment& segment, const std::vector<GroupID>& group_ids) {
  // TODO(anyone): We don’t need to iterate through the segment if we’ve already found any
  // value for all groups (i.e., the count for the respective group_id is greater than 0).
  segment_iterate<ColumnDataType>(segment, [&](const auto& position) {
    if (!position.is_null()) {
      const auto group_id = group_ids[position.chunk_offset()];

      if (aggregate_vector.count(group_id) == 0) {
        aggregate_vector.accumulator(group_id) = position.value();
        aggregate_vector.increment_count(group_id);
      }
    }
  });
}

void AggregateDYOD::_aggregate_count_star(AbstractAggregateVector& aggregate_vector,
                                          const std::vector<GroupID>& group_ids) {
  for (const auto group_id : group_ids) {
    aggregate_vector.increment_count(group_id);
  }
}

bool AggregateDYOD::_aggregate_is_nullable(const size_t aggregate_index) const {
  const auto aggregate_function = _aggregates[aggregate_index]->window_function;

  if (aggregate_function == WindowFunction::Count || aggregate_function == WindowFunction::CountDistinct) {
    return false;
  }

  if (aggregate_function == WindowFunction::Any) {
    const auto input_table = left_input_table();
    const auto& aggregate = _aggregates[aggregate_index];
    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    return input_table->column_is_nullable(pqp_column.column_id);
  }

  return true;
}

DataType AggregateDYOD::_aggregate_column_data_type(const size_t aggregate_index) const {
  const auto& aggregate = _aggregates[aggregate_index];
  const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
  return pqp_column.data_type();
}

std::string AggregateDYOD::_aggregate_column_name(const size_t aggregate_index) const {
  const auto input_table = left_input_table();
  const auto& aggregate = _aggregates[aggregate_index];
  const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
  return input_table->column_name(pqp_column.column_id);
}

std::shared_ptr<AbstractOperator> AggregateDYOD::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<AggregateDYOD>(copied_left_input, _aggregates, _groupby_column_ids);
}

void AggregateDYOD::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void AggregateDYOD::_on_cleanup() {}

}  // namespace hyrise
