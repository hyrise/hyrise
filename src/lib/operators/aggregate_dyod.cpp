#include "aggregate_dyod.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <format>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <memory_resource>
#include <numeric>
#include <optional>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/iterator/indirect_iterator.hpp>

#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "hyrise.hpp"
#include "operator_state.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_operator.hpp"
#include "perfetto.h"
#include "storage/abstract_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"
#include "utils/tracing.hpp"

namespace hyrise {

template <typename T>
  requires std::is_trivially_copyable_v<T>
std::vector<std::byte> serialize_value(T value) {
  auto bytes = std::vector<std::byte>(sizeof(T));
  std::memcpy(bytes.data(), &value, sizeof(T));
  return bytes;
}

std::vector<std::byte> serialize_value(const pmr_string& value) {
  auto bytes = std::vector<std::byte>(value.size());
  std::memcpy(bytes.data(), value.data(), value.size());
  return bytes;
}

template <typename T>
  requires std::is_trivially_copyable_v<T>
std::vector<std::byte> serialize_value(T value, bool is_null) {
  if (is_null) {
    return std::vector<std::byte>{std::byte{0x01}};
  }
  auto bytes = std::vector<std::byte>(1 + sizeof(T));
  bytes[0] = std::byte{0x00};
  std::memcpy(bytes.data() + 1, &value, sizeof(T));
  return bytes;
}

std::vector<std::byte> serialize_value(const pmr_string& value, bool is_null) {
  if (is_null) {
    return std::vector<std::byte>{std::byte{0x01}};
  }
  auto bytes = std::vector<std::byte>(1 + value.size());
  bytes[0] = std::byte{0x00};
  std::memcpy(bytes.data() + 1, value.data(), value.size());
  return bytes;
}

template <typename T, bool Nullable>
  requires std::is_trivially_copyable_v<T> && (!Nullable)
T deserialize_value(const std::vector<std::byte>& bytes) {
  T value;
  std::memcpy(&value, bytes.data(), sizeof(T));
  return value;
}

template <typename T, bool Nullable>
  requires std::is_trivially_copyable_v<T> && Nullable
std::optional<T> deserialize_value(const std::vector<std::byte>& bytes) {
  if (bytes[0] == std::byte{0x01}) {
    return std::nullopt;
  }
  T value;
  std::memcpy(&value, bytes.data() + 1, sizeof(T));
  return value;
}

template <typename T, bool Nullable>
  requires std::is_same_v<T, pmr_string> && (!Nullable)
pmr_string deserialize_value(const std::vector<std::byte>& bytes) {
  return pmr_string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
}

template <typename T, bool Nullable>
  requires std::is_same_v<T, pmr_string> && Nullable
std::optional<pmr_string> deserialize_value(const std::vector<std::byte>& bytes) {
  if (bytes[0] == std::byte{0x01}) {
    return std::nullopt;
  }
  return pmr_string(reinterpret_cast<const char*>(bytes.data() + 1), bytes.size() - 1);
}

template <typename Functor>
void resolve_window_function(WindowFunction window_function, Functor&& functor) {
  switch (window_function) {
    case WindowFunction::Min:
      functor(std::integral_constant<WindowFunction, WindowFunction::Min>{});
      break;
    case WindowFunction::Max:
      functor(std::integral_constant<WindowFunction, WindowFunction::Max>{});
      break;
    case WindowFunction::Sum:
      functor(std::integral_constant<WindowFunction, WindowFunction::Sum>{});
      break;
    case WindowFunction::Count:
      functor(std::integral_constant<WindowFunction, WindowFunction::Count>{});
      break;
    case WindowFunction::Avg:
      functor(std::integral_constant<WindowFunction, WindowFunction::Avg>{});
      break;
    case WindowFunction::CountDistinct:
      functor(std::integral_constant<WindowFunction, WindowFunction::CountDistinct>{});
      break;
    case WindowFunction::Any:
      functor(std::integral_constant<WindowFunction, WindowFunction::Any>{});
      break;
    default:
      Fail("Unsupported aggregate function.");
  }
}

WorkerState::WorkerState(const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                         std::function<std::pair<GroupID, GroupID>()> get_new_group_id_range) {
  auto [initial_next_group_id, initial_max_group_id] = get_new_group_id_range();
  _next_group_id = initial_next_group_id;
  _max_group_id = initial_max_group_id;
  _get_new_group_id_range = get_new_group_id_range;

  const auto aggregate_count = aggregates.size();
  _vectors.resize(aggregate_count);

  for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
    const auto aggregate = aggregates[aggregate_index];
    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto data_type = pqp_column.data_type();

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      resolve_window_function(aggregate->window_function, [&](auto type) {
        constexpr auto aggregate_function = decltype(type)::value;
        _vectors[aggregate_index] = std::make_unique<TypedAggregateVector<ColumnDataType, aggregate_function>>();
        _vectors[aggregate_index]->grow_if_necessary(_max_group_id + 1);
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
  if (_next_group_id > _max_group_id) {
    auto [next_group_id, max_group_id] = _get_new_group_id_range();
    _next_group_id = next_group_id;
    _max_group_id = max_group_id;

    for (const auto& vector : _vectors) {
      vector->grow_if_necessary(_max_group_id + 1);
    }
  }

  return _next_group_id++;
}

AbstractAggregateVector& WorkerState::aggregate_vector(size_t index) {
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
  TRACE_EVENT("aggregate_operator", "_on_execute");
  const auto input_table = left_input_table();

  _validate_aggregates();

  // Aggregate chunk by chunk
  const auto chunk_count = input_table->chunk_count();
  const auto get_new_group_id_range = [&]() {
    return _get_new_group_id_range();
  };
  auto state = OperatorSharedState<WorkerState>{_aggregates, get_new_group_id_range};
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>(chunk_count);

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    jobs[chunk_id] = std::make_shared<JobTask>([&, chunk_id]() {
      auto& worker_state = state.current_worker_state();
      const auto chunk = input_table->get_chunk(chunk_id);
      _aggregate_chunk(worker_state, chunk);
    });
  }

  {
    TRACE_EVENT("aggregate_operator", "schedule_and_wait_for_tasks");
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  }

  // If we haven't run any jobs, attempting to merge the worker states would fail
  auto merged_worker_states = [&]() {
    TRACE_EVENT("aggregate_operator", "merge_worker_states");
    return jobs.empty() ? WorkerState(_aggregates, get_new_group_id_range) : std::move(state.merge_worker_states());
  }();

  // SQL requires a single output row if the input table is empty and there is no GROUP BY clause.
  // We ensure this by inserting a single group into the group ID mapping before writing the output table.
  if (_group_id_map.empty() && _groupby_column_ids.empty()) {
    _group_id(GroupKey{}, merged_worker_states);
  }

  return _write_output_table(merged_worker_states);
}

std::shared_ptr<Table> AggregateDYOD::_write_output_table(WorkerState& worker_state) {
  TRACE_EVENT("aggregate_operator", "_write_output_table");
  const auto column_definitions = _output_column_definitions();

  const auto total_group_count = _group_id_map.size();
  const auto chunk_count = (total_group_count + Chunk::DEFAULT_SIZE - 1) / Chunk::DEFAULT_SIZE;
  auto chunks = std::vector<std::shared_ptr<Chunk>>(chunk_count);

  if (total_group_count > 0) {
    auto jobs = std::vector<std::shared_ptr<AbstractTask>>(chunk_count);
    const auto occupied_group_ids = _get_occupied_group_ids();

    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      jobs[chunk_id] = std::make_shared<JobTask>([&, chunk_id]() {
        const auto start_index = size_t{chunk_id} * Chunk::DEFAULT_SIZE;
        const auto end_index = std::min(start_index + Chunk::DEFAULT_SIZE, total_group_count);
        chunks[chunk_id] = _write_output_chunk(worker_state, occupied_group_ids, start_index, end_index);
      });
    }

    {
      TRACE_EVENT("aggregate_operator", "schedule_and_wait_for_tasks");
      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
    }
  }

  return std::make_shared<Table>(column_definitions, TableType::Data, chunks);
}

TableColumnDefinitions AggregateDYOD::_output_column_definitions() {
  const auto input_table = left_input_table();
  const auto aggregate_count = _aggregates.size();
  auto column_definitions = TableColumnDefinitions();

  for (const auto column_id : groupby_column_ids()) {
    column_definitions.emplace_back(input_table->column_name(column_id), input_table->column_data_type(column_id),
                                    input_table->column_is_nullable(column_id));
  }

  for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
    const auto& aggregate = _aggregates[aggregate_index];
    resolve_data_type(_aggregate_column_data_type(aggregate_index), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      resolve_window_function(aggregate->window_function, [&](auto type) {
        constexpr auto aggregate_function = decltype(type)::value;
        const auto data_type = WindowFunctionTraits<ColumnDataType, aggregate_function>::RESULT_TYPE;

        // TODO(anyone): For some reason, the expected name of `ANY(my_col)` is `any` and not `ANY(my_col)` as for
        // all other aggregates.
        const auto name = aggregate->window_function == WindowFunction::Any ? _aggregate_column_name(aggregate_index)
                                                                            : aggregate->as_column_name();
        column_definitions.emplace_back(name, data_type, _aggregate_is_nullable(aggregate_index));
      });
    });
  }

  return column_definitions;
}

std::shared_ptr<Chunk> AggregateDYOD::_write_output_chunk(WorkerState& worker_state,
                                                          const std::vector<size_t>& occupied_group_ids,
                                                          size_t start_index, size_t end_index) {
  TRACE_EVENT("aggregate_operator", "_write_output_chunk");
  const auto input_table = left_input_table();
  const auto aggregate_count = _aggregates.size();
  const auto groupby_column_count = _groupby_column_ids.size();

  auto segments = Segments{};
  segments.reserve(groupby_column_count + aggregate_count);

  // Create one ValueSegment per grouping column
  for (auto groupby_column_index = size_t{0}; groupby_column_index < groupby_column_count; ++groupby_column_index) {
    const auto column_id = _groupby_column_ids[groupby_column_index];
    const auto data_type = input_table->column_data_type(column_id);

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      segments.emplace_back(
          _write_groupby_segment<ColumnDataType>(groupby_column_index, occupied_group_ids, start_index, end_index));
    });
  }

  // Create one ValueSegment per aggregate
  for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
    const auto aggregate = _aggregates[aggregate_index];

    resolve_data_type(_aggregate_column_data_type(aggregate_index), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      resolve_window_function(aggregate->window_function, [&](auto type) {
        constexpr auto aggregate_function = decltype(type)::value;
        auto& aggregate_vector = static_cast<TypedAggregateVector<ColumnDataType, aggregate_function>&>(
            worker_state.aggregate_vector(aggregate_index));
        segments.emplace_back(_write_aggregate_segment<ColumnDataType, aggregate_function>(
            aggregate_vector, _aggregate_is_nullable(aggregate_index), occupied_group_ids, start_index, end_index));
      });
    });
  }

  return std::make_shared<Chunk>(segments);
}

template <typename ColumnDataType>
std::shared_ptr<AbstractSegment> AggregateDYOD::_write_groupby_segment(size_t groupby_column_index,
                                                                       const std::vector<size_t>& occupied_group_ids,
                                                                       size_t start_index, size_t end_index) {
  const auto input_table = left_input_table();
  const auto column_id = _groupby_column_ids[groupby_column_index];
  const auto is_nullable = input_table->column_is_nullable(column_id);
  const auto chunk_size = end_index - start_index;

  if (is_nullable) {
    auto values = pmr_vector<ColumnDataType>(chunk_size);
    auto null_values = pmr_vector<bool>(chunk_size);

    for (auto index = start_index; index < end_index; ++index) {
      const auto group_id = occupied_group_ids[index];
      const auto chunk_offset = index - start_index;
      const auto& group_key_entry = _group_keys[group_id][groupby_column_index];
      const auto deserialized = deserialize_value<ColumnDataType, true>(group_key_entry);

      if (deserialized.has_value()) {
        values[chunk_offset] = deserialized.value();
      } else {
        null_values[chunk_offset] = true;
      }
    }

    return std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(null_values));
  }

  auto values = pmr_vector<ColumnDataType>(chunk_size);

  for (auto index = start_index; index < end_index; ++index) {
    const auto group_id = occupied_group_ids[index];
    const auto chunk_offset = index - start_index;
    const auto& group_key_entry = _group_keys[group_id][groupby_column_index];
    values[chunk_offset] = deserialize_value<ColumnDataType, false>(group_key_entry);
  }

  return std::make_shared<ValueSegment<ColumnDataType>>(std::move(values));
}

template <typename ColumnDataType, WindowFunction aggregate_function>
std::shared_ptr<AbstractSegment> AggregateDYOD::_write_aggregate_segment(
    TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, bool is_nullable,
    const std::vector<size_t>& occupied_group_ids, size_t start_index, size_t end_index) {
  constexpr auto data_type = WindowFunctionTraits<ColumnDataType, aggregate_function>::RESULT_TYPE;

  if constexpr (data_type == DataType::Null) {
    Fail("Invalid combination of column type and aggregate function.");
  } else {
    if constexpr (aggregate_function == WindowFunction::Count) {
      return _write_count_aggregate_segment(aggregate_vector, occupied_group_ids, start_index, end_index);
    } else if constexpr (aggregate_function == WindowFunction::Avg) {
      return _write_avg_aggregate_segment(aggregate_vector, occupied_group_ids, start_index, end_index);
    } else if constexpr (aggregate_function == WindowFunction::CountDistinct) {
      return _write_count_distinct_aggregate_segment(aggregate_vector, occupied_group_ids, start_index, end_index);
    } else {
      return _write_default_aggregate_segment(aggregate_vector, is_nullable, occupied_group_ids, start_index,
                                              end_index);
    }
  }
}

template <typename ColumnDataType, WindowFunction aggregate_function>
std::shared_ptr<AbstractSegment> AggregateDYOD::_write_avg_aggregate_segment(
    TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector,
    const std::vector<size_t>& occupied_group_ids, size_t start_index, size_t end_index) {
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
std::shared_ptr<AbstractSegment> AggregateDYOD::_write_count_aggregate_segment(
    TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector,
    const std::vector<size_t>& occupied_group_ids, size_t start_index, size_t end_index) {
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
std::shared_ptr<AbstractSegment> AggregateDYOD::_write_count_distinct_aggregate_segment(
    TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector,
    const std::vector<size_t>& occupied_group_ids, size_t start_index, size_t end_index) {
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
std::shared_ptr<AbstractSegment> AggregateDYOD::_write_default_aggregate_segment(
    TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector, bool is_nullable,
    const std::vector<size_t>& occupied_group_ids, size_t start_index, size_t end_index) {
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
    values[index - start_index] = aggregate_values[group_id];
  }
  return std::make_shared<ValueSegment<AggregateDataType>>(std::move(values));
}

GroupID AggregateDYOD::_group_id(const GroupKey& group_key, WorkerState& worker_state) {
  auto it = _group_id_map.find(group_key);
  if (it != _group_id_map.end()) {
    const auto group_id = it->second;
    for (auto& aggregate_vector : worker_state.aggregate_vectors()) {
      aggregate_vector->grow_if_necessary(group_id + 1);
    }
    return group_id;
  }

  auto [insert_it, inserted] = _group_id_map.insert({group_key, worker_state.next_group_id()});
  const auto group_id = insert_it->second;

  if (inserted) {
    _group_keys[group_id] = group_key;
    _occupied_group_ids[group_id] = true;
  }

  return group_id;
}

std::pair<GroupID, GroupID> AggregateDYOD::_get_new_group_id_range() {
  auto next_group_id = _next_group_id.fetch_add(FUZZY_STEP_SIZE, std::memory_order_relaxed);
  auto max_group_id = next_group_id + FUZZY_STEP_SIZE - 1;

  {
    // TODO(anyone): Figure out how to avoid this lock. Maybe use a single vector of pairs?
    std::lock_guard<std::mutex> lock(_group_keys_mutex);
    _group_keys.grow_to_at_least(max_group_id + 1);
    _occupied_group_ids.grow_to_at_least(max_group_id + 1);
  }

  return {next_group_id, max_group_id};
}

std::vector<size_t> AggregateDYOD::_get_occupied_group_ids() {
  // clang-format off
  auto view = std::views::iota(size_t{0}, _group_keys.size())
    | std::views::filter([&](size_t index) { return _occupied_group_ids[index]; });
  // clang-format on

  return {view.begin(), view.end()};
}

void AggregateDYOD::_aggregate_chunk(WorkerState& worker_state, const std::shared_ptr<const Chunk> chunk) {
  TRACE_EVENT("aggregate_operator", "_aggregate_chunk");
  const auto group_ids = _group_ids_for_chunk(*chunk, worker_state);

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

    const auto segment = chunk->get_segment(column_id);

    resolve_data_type(pqp_column.data_type(), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      resolve_window_function(aggregate->window_function, [&](auto type) {
        constexpr auto aggregate_function = decltype(type)::value;
        auto& aggregate_vector = static_cast<TypedAggregateVector<ColumnDataType, aggregate_function>&>(
            worker_state.aggregate_vector(aggregate_index));
        _aggregate_segment<ColumnDataType, aggregate_function>(aggregate_vector, *segment, group_ids);
      });
    });
  }
}

std::vector<GroupID> AggregateDYOD::_group_ids_for_chunk(const Chunk& chunk, WorkerState& worker_state) {
  TRACE_EVENT("aggregate_operator", "_group_ids_for_chunk");
  const auto input_table = left_input_table();

  // This is a two-dimensional vector, with the first dimension being the index of the grouping column, and the second
  // being the chunk offset of the row.
  auto group_keys_by_column =
      std::vector<std::vector<GroupKeyEntry>>(_groupby_column_ids.size(), std::vector<GroupKeyEntry>(chunk.size()));

  // First, compute the group keys within each column.
  const auto groupby_column_count = _groupby_column_ids.size();
  for (auto groupby_column_index = size_t{0}; groupby_column_index < groupby_column_count; ++groupby_column_index) {
    const auto groupby_column_id = _groupby_column_ids[groupby_column_index];
    const auto data_type = input_table->column_data_type(groupby_column_id);
    const auto is_nullable = input_table->column_is_nullable(groupby_column_id);

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      const auto segment = chunk.get_segment(groupby_column_id);

      segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
        group_keys_by_column[groupby_column_index][position.chunk_offset()] =
            is_nullable ? serialize_value(position.value(), position.is_null()) : serialize_value(position.value());
      });
    });
  }

  // Then assemble the group keys per row and get the GroupID.
  auto group_ids = std::vector<GroupID>(chunk.size());

  const auto row_count = chunk.size();
  for (auto offset = ChunkOffset{0}; offset < row_count; ++offset) {
    auto group_key = GroupKey(groupby_column_count);
    for (auto groupby_column_index = size_t{0}; groupby_column_index < groupby_column_count; ++groupby_column_index) {
      group_key[groupby_column_index] = std::move(group_keys_by_column[groupby_column_index][offset]);
    }
    group_ids[offset] = _group_id(group_key, worker_state);
  }

  return group_ids;
}

template <typename ColumnDataType, WindowFunction aggregate_function>
void AggregateDYOD::_aggregate_segment(TypedAggregateVector<ColumnDataType, aggregate_function>& aggregate_vector,
                                       const AbstractSegment& segment, const std::vector<GroupID>& group_ids) {
  TRACE_EVENT("aggregate_operator", "_aggregate_segment", "aggregate_function", "default");
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
  TRACE_EVENT("aggregate_operator", "_aggregate_segment", "aggregate_function", "CountDistinct");
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
  TRACE_EVENT("aggregate_operator", "_aggregate_segment", "aggregate_function", "Any");
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
  TRACE_EVENT("aggregate_operator", "_aggregate_count_star");
  for (const auto group_id : group_ids) {
    aggregate_vector.increment_count(group_id);
  }
}

bool AggregateDYOD::_aggregate_is_nullable(size_t aggregate_index) {
  const auto aggregate_function = _aggregates[aggregate_index]->window_function;

  if (aggregate_function == WindowFunction::Count || aggregate_function == WindowFunction::CountDistinct) {
    return false;
  }

  if (aggregate_function == WindowFunction::Any) {
    // TODO(anyone): Figure out why exactly this is true only for ANY
    const auto input_table = left_input_table();
    const auto aggregate = _aggregates[aggregate_index];
    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    return input_table->column_is_nullable(pqp_column.column_id);
  }

  return true;
}

DataType AggregateDYOD::_aggregate_column_data_type(size_t aggregate_index) {
  const auto aggregate = _aggregates[aggregate_index];
  const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
  return pqp_column.data_type();
}

std::string AggregateDYOD::_aggregate_column_name(size_t aggregate_index) {
  const auto input_table = left_input_table();
  const auto aggregate = _aggregates[aggregate_index];
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
