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
#include <memory>
#include <memory_resource>
#include <numeric>
#include <optional>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "hyrise.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_operator.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "types.hpp"

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
    default:
      Fail("Unsupported aggregate function.");
  }
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
  _prepare_aggregate_vectors();

  // Aggregate chunk by chunk
  const auto chunk_count = input_table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = input_table->get_chunk(chunk_id);
    _aggregate_chunk(chunk);
  }

  // SQL requires a single output row if the input table is empty and there is no GROUP BY clause.
  // We ensure this by inserting a single group into the group ID mapping before writing the output table.
  if (_group_id_map.empty() && _groupby_column_ids.empty()) {
    _group_id(GroupKey{});
  }

  return _write_output_table();
}

void AggregateDYOD::_prepare_aggregate_vectors() {
  const auto aggregate_count = _aggregates.size();
  _aggregate_vectors.resize(aggregate_count);

  for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
    const auto aggregate = _aggregates[aggregate_index];

    resolve_data_type(_aggregate_column_data_type(aggregate_index), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      resolve_window_function(aggregate->window_function, [&](auto type) {
        constexpr auto aggregate_function = decltype(type)::value;
        using AggregateDataType = typename WindowFunctionTraits<ColumnDataType, aggregate_function>::ReturnType;
        _aggregate_vectors[aggregate_index] = std::make_unique<TypedAggregateVector<AggregateDataType>>();
      });
    });
  }
}

std::shared_ptr<Table> AggregateDYOD::_write_output_table() {
  const auto input_table = left_input_table();
  auto column_definitions = TableColumnDefinitions();

  const auto aggregate_count = _aggregates.size();
  const auto groupby_column_count = _groupby_column_ids.size();

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
        column_definitions.emplace_back(aggregate->as_column_name(), data_type,
                                        _aggregate_is_nullable(aggregate_index));
      });
    });
  }

  auto segments = Segments{};
  segments.reserve(groupby_column_count + aggregate_count);

  // Create one ValueSegment per grouping column
  // TODO(anyone): Open new chunk when max number of rows is reached
  for (auto groupby_column_index = size_t{0}; groupby_column_index < groupby_column_count; ++groupby_column_index) {
    const auto column_id = _groupby_column_ids[groupby_column_index];
    const auto data_type = input_table->column_data_type(column_id);

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      segments.push_back(_write_groupby_segment<ColumnDataType>(groupby_column_index));
    });
  }

  // Create one ValueSegment per aggregate
  for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
    const auto aggregate = _aggregates[aggregate_index];

    resolve_data_type(_aggregate_column_data_type(aggregate_index), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      resolve_window_function(aggregate->window_function, [&](auto type) {
        constexpr auto aggregate_function = decltype(type)::value;
        segments.push_back(_write_aggregate_segment<ColumnDataType, aggregate_function>(aggregate_index));
      });
    });
  }

  const auto output_table = std::make_shared<Table>(column_definitions, TableType::Data);
  output_table->append_chunk(segments);

  return output_table;
}

template <typename ColumnDataType>
std::shared_ptr<AbstractSegment> AggregateDYOD::_write_groupby_segment(size_t groupby_column_index) {
  const auto input_table = left_input_table();
  const auto column_id = _groupby_column_ids[groupby_column_index];
  const auto is_nullable = input_table->column_is_nullable(column_id);
  const auto group_count = _group_count();

  if (is_nullable) {
    auto values = pmr_vector<ColumnDataType>(group_count);
    auto null_values = pmr_vector<bool>(group_count);

    for (auto group_id = GroupID{0}; group_id < group_count; ++group_id) {
      const auto group_key_entry = _group_keys[group_id][groupby_column_index];
      const auto deserialized = deserialize_value<ColumnDataType, true>(group_key_entry);

      if (deserialized.has_value()) {
        values[group_id] = deserialized.value();
      } else {
        null_values[group_id] = true;
      }
    }

    return std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(null_values));
  }

  auto values = pmr_vector<ColumnDataType>(group_count);

  for (auto group_id = GroupID{0}; group_id < group_count; ++group_id) {
    const auto group_key_entry = _group_keys[group_id][groupby_column_index];
    values[group_id] = deserialize_value<ColumnDataType, false>(group_key_entry);
  }

  return std::make_shared<ValueSegment<ColumnDataType>>(std::move(values));
}

template <typename ColumnDataType, WindowFunction aggregate_function>
std::shared_ptr<AbstractSegment> AggregateDYOD::_write_aggregate_segment(size_t aggregate_index) {
  using AggregateDataType = typename WindowFunctionTraits<ColumnDataType, aggregate_function>::ReturnType;
  constexpr auto data_type = WindowFunctionTraits<ColumnDataType, aggregate_function>::RESULT_TYPE;
  auto& aggregate_vector = static_cast<TypedAggregateVector<AggregateDataType>&>(*_aggregate_vectors[aggregate_index]);

  if constexpr (data_type == DataType::Null) {
    Fail("Invalid combination of column type and aggregate function.");
  } else {
    if constexpr (aggregate_function == WindowFunction::Count) {
      return _write_count_aggregate_segment(aggregate_index, aggregate_vector);
    } else if constexpr (aggregate_function == WindowFunction::Avg) {
      return _write_avg_aggregate_segment(aggregate_index, aggregate_vector);
    } else {
      return _write_default_aggregate_segment(aggregate_index, aggregate_vector);
    }
  }
}

template <typename AggregateDataType>
std::shared_ptr<AbstractSegment> AggregateDYOD::_write_avg_aggregate_segment(
    size_t aggregate_index, TypedAggregateVector<AggregateDataType>& aggregate_vector) {
  const auto& sums = aggregate_vector.values();
  const auto& counts = aggregate_vector.counts();
  const auto group_count = _group_count();

  auto averages = pmr_vector<AggregateDataType>(group_count);
  auto null_values = pmr_vector<bool>(group_count);

  for (auto group_id = GroupID{0}; group_id < group_count; ++group_id) {
    if (counts[group_id] == 0) {
      null_values[group_id] = true;
    } else {
      // TODO(anyone): The maximum representable RowID in Hyrise is 2^64 (minus a few reserved sentinel values).
      // So in theory, the count could exceed the range of double, although in practice, it is rather unlikely.
      averages[group_id] = sums[group_id] / static_cast<double>(counts[group_id]);
    }
  }

  return std::make_shared<ValueSegment<AggregateDataType>>(std::move(averages), std::move(null_values));
}

template <typename AggregateDataType>
std::shared_ptr<AbstractSegment> AggregateDYOD::_write_count_aggregate_segment(
    size_t aggregate_index, TypedAggregateVector<AggregateDataType>& aggregate_vector) {
  const auto& counts = aggregate_vector.counts();
  auto values = pmr_vector<AggregateDataType>(counts.begin(), counts.end());
  return std::make_shared<ValueSegment<AggregateDataType>>(std::move(values));
}

template <typename AggregateDataType>
std::shared_ptr<AbstractSegment> AggregateDYOD::_write_default_aggregate_segment(
    size_t aggregate_index, TypedAggregateVector<AggregateDataType>& aggregate_vector) {
  if (_aggregate_is_nullable(aggregate_index)) {
    const auto group_count = _group_count();
    auto null_values = pmr_vector<bool>(group_count);
    for (auto group_id = GroupID{0}; group_id < group_count; ++group_id) {
      if (aggregate_vector.count(group_id) == 0) {
        null_values[group_id] = true;
      }
    }
    return std::make_shared<ValueSegment<AggregateDataType>>(std::move(aggregate_vector.values()),
                                                             std::move(null_values));
  }

  return std::make_shared<ValueSegment<AggregateDataType>>(std::move(aggregate_vector.values()));
}

GroupID AggregateDYOD::_group_id(const GroupKey& group_key) {
  auto [it, inserted] = _group_id_map.try_emplace(group_key, _group_count());

  // The key was already present, so all we need to do is to return it.
  if (!inserted) {
    return it->second;
  }

  // Otherwise, store the group key for later retrieval.
  // TODO(anyone): Consider storing group key entries by column, not by row
  _group_keys.push_back(group_key);

  // And append a new item for the new group to the aggregate vectors.
  for (auto& aggregate_vector : _aggregate_vectors) {
    aggregate_vector->push_back_default();
  }

  return it->second;
}

GroupID AggregateDYOD::_group_count() {
  return _group_id_map.size();
}

void AggregateDYOD::_aggregate_chunk(const std::shared_ptr<const Chunk> chunk) {
  const auto group_ids = _group_ids_for_chunk(*chunk);

  // Compute aggregates
  const auto aggregate_count = _aggregates.size();
  for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
    const auto& aggregate = _aggregates[aggregate_index];

    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto column_id = pqp_column.column_id;

    // COUNT(*): Skip the generic path and just count the number of rows in each group
    if (aggregate->window_function == WindowFunction::Count && column_id == INVALID_COLUMN_ID) {
      _aggregate_count_star(aggregate_index, group_ids);
      continue;
    }

    const auto segment = chunk->get_segment(column_id);

    resolve_data_type(pqp_column.data_type(), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      resolve_window_function(aggregate->window_function, [&](auto type) {
        constexpr auto aggregate_function = decltype(type)::value;
        _aggregate_segment<ColumnDataType, aggregate_function>(aggregate_index, *segment, group_ids);
      });
    });
  }
}

std::vector<GroupID> AggregateDYOD::_group_ids_for_chunk(const Chunk& chunk) {
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
    group_ids[offset] = _group_id(group_key);
  }

  return group_ids;
}

template <typename ColumnDataType, WindowFunction aggregate_function>
void AggregateDYOD::_aggregate_segment(size_t aggregate_index, const AbstractSegment& segment,
                                       const std::vector<GroupID>& group_ids) {
  using AggregateDataType = typename WindowFunctionTraits<ColumnDataType, aggregate_function>::ReturnType;
  auto aggregator =
      WindowFunctionBuilder<ColumnDataType, AggregateDataType, aggregate_function>().get_aggregate_function();
  auto& aggregate_vector = static_cast<TypedAggregateVector<AggregateDataType>&>(*_aggregate_vectors[aggregate_index]);

  segment_iterate<ColumnDataType>(segment, [&](const auto& position) {
    if (!position.is_null()) {
      const auto group_id = group_ids[position.chunk_offset()];
      aggregator(position.value(), aggregate_vector.count(group_id), aggregate_vector[group_id]);
      aggregate_vector.increment_count(group_id);
    }
  });
}

void AggregateDYOD::_aggregate_count_star(size_t aggregate_index, const std::vector<GroupID>& group_ids) {
  auto& aggregate_vector = *_aggregate_vectors[aggregate_index];

  for (const auto group_id : group_ids) {
    aggregate_vector.increment_count(group_id);
  }
}

bool AggregateDYOD::_aggregate_is_nullable(size_t aggregate_index) {
  const auto aggregate_function = _aggregates[aggregate_index]->window_function;
  return aggregate_function != WindowFunction::Count && aggregate_function != WindowFunction::CountDistinct;
}

DataType AggregateDYOD::_aggregate_column_data_type(size_t aggregate_index) {
  const auto aggregate = _aggregates[aggregate_index];
  const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
  return pqp_column.data_type();
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
