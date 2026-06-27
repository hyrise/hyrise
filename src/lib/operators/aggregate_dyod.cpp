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
  auto bytes = std::vector<std::byte>(1 + sizeof(T));
  bytes[0] = is_null ? std::byte{0x01} : std::byte{0x00};
  std::memcpy(bytes.data() + 1, &value, sizeof(T));
  return bytes;
}

std::vector<std::byte> serialize_value(const pmr_string& value, bool is_null) {
  auto bytes = std::vector<std::byte>(1 + value.size());
  bytes[0] = is_null ? std::byte{0x01} : std::byte{0x00};
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
  _resolve_aggregate_data_types();

  // Prepare aggregate vectors
  const auto aggregate_count = _aggregates.size();
  _aggregate_results.resize(aggregate_count);
  _aggregate_counts.resize(aggregate_count);
  for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
    resolve_data_type(_aggregate_data_types[aggregate_index], [&](auto type) {
      using AggregateDataType = typename decltype(type)::type;
      _aggregate_results[aggregate_index] = std::make_unique<TypedAggregateVector<AggregateDataType>>();
    });
  }

  // Aggregate chunk by chunk
  const auto chunk_count = input_table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = input_table->get_chunk(chunk_id);
    _aggregate_chunk(chunk);
  }

  return _create_output_table();
}

void AggregateDYOD::_resolve_aggregate_data_types() {
  for (const auto& aggregate : _aggregates) {
    // TODO(anyone): Is this cast guaranteed to work? Or are there cases where there is no argument or the
    // argument is not a PQPColumnExpression?
    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());

    resolve_data_type(pqp_column.data_type(), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      switch (aggregate->window_function) {
        // TODO(anyone): Add missing cases
        case WindowFunction::Min:
          _aggregate_data_types.emplace_back(WindowFunctionTraits<ColumnDataType, WindowFunction::Min>::RESULT_TYPE);
          break;
        case WindowFunction::Max:
          _aggregate_data_types.emplace_back(WindowFunctionTraits<ColumnDataType, WindowFunction::Max>::RESULT_TYPE);
          break;
        default:
          Fail("Unsupported aggregate function.");
      }
    });
  }
}

std::shared_ptr<Table> AggregateDYOD::_create_output_table() {
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
    const auto aggregate_function = aggregate->window_function;
    const auto needs_null =
        (aggregate_function != WindowFunction::Count && aggregate_function != WindowFunction::CountDistinct);
    column_definitions.emplace_back(aggregate->as_column_name(), _aggregate_data_types[aggregate_index], needs_null);
  }

  const auto group_count = _ticket_table.size();
  auto segments = Segments{};
  segments.reserve(groupby_column_count + aggregate_count);

  // Create one ValueSegment per grouping column
  // TODO(anyone): Open new chunk when max number of rows is reached
  for (auto groupby_column_index = size_t{0}; groupby_column_index < groupby_column_count; ++groupby_column_index) {
    const auto column_id = _groupby_column_ids[groupby_column_index];
    const auto data_type = input_table->column_data_type(column_id);
    const auto nullable = input_table->column_is_nullable(column_id);

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      if (nullable) {
        auto values = pmr_vector<ColumnDataType>(group_count);
        auto null_values = pmr_vector<bool>(group_count);

        for (auto group_index = size_t{0}; group_index < group_count; ++group_index) {
          const auto group_key_entry = _group_keys[group_index][groupby_column_index];
          const auto deserialized = deserialize_value<ColumnDataType, true>(group_key_entry);

          if (deserialized.has_value()) {
            values[group_index] = deserialized.value();
          } else {
            null_values[group_index] = true;
          }
        }

        const auto segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(null_values));
        segments.push_back(std::move(segment));
      } else {
        auto values = pmr_vector<ColumnDataType>(group_count);

        for (auto group_index = ChunkOffset{0}; group_index < group_count; ++group_index) {
          const auto group_key_entry = _group_keys[group_index][groupby_column_index];
          values[group_index] = deserialize_value<ColumnDataType, false>(group_key_entry);
        }

        const auto segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values));
        segments.push_back(std::move(segment));
      }
    });
  }

  // Create one ValueSegment per aggregate
  for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
    resolve_data_type(_aggregate_data_types[aggregate_index], [&](auto type) {
      using AggregateDataType = typename decltype(type)::type;
      auto& aggregate_vector =
          static_cast<TypedAggregateVector<AggregateDataType>&>(*_aggregate_results[aggregate_index]);
      const auto segment = std::make_shared<ValueSegment<AggregateDataType>>(std::move(aggregate_vector.results));
      segments.push_back(std::move(segment));
    });
  }

  const auto output_table = std::make_shared<Table>(column_definitions, TableType::Data);
  output_table->append_chunk(segments);

  return output_table;
}

size_t AggregateDYOD::_get_ticket(const GroupKey& group_key) {
  const auto it = _ticket_table.find(group_key);
  if (it != _ticket_table.end()) {
    return it->second;
  }

  const auto ticket = _ticket_table.size();
  _ticket_table.emplace(group_key, ticket);
  // TODO(anyone): Consider storing group key entries by column, not by row
  _group_keys.push_back(group_key);

  const auto aggregate_count = _aggregates.size();
  for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
    _aggregate_counts[aggregate_index].push_back(0);
  }

  for (auto& aggregate_result : _aggregate_results) {
    aggregate_result->push_back_default();
  }

  return ticket;
}

void AggregateDYOD::_aggregate_chunk(const std::shared_ptr<const Chunk> chunk) {
  const auto input_table = left_input_table();

  // This is a two-dimensional vector, with the first dimension being the index of the grouping column, and the second
  // being the chunk offset of the row.
  auto group_keys_by_column =
      std::vector<std::vector<GroupKeyEntry>>(_groupby_column_ids.size(), std::vector<GroupKeyEntry>(chunk->size()));

  // First, compute the group keys within each column.
  const auto groupby_column_count = _groupby_column_ids.size();
  for (auto groupby_column_index = size_t{0}; groupby_column_index < groupby_column_count; ++groupby_column_index) {
    const auto groupby_column_id = _groupby_column_ids[groupby_column_index];
    const auto data_type = input_table->column_data_type(groupby_column_id);
    const auto is_nullable = input_table->column_is_nullable(groupby_column_id);

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      const auto segment = chunk->get_segment(groupby_column_id);

      segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
        group_keys_by_column[groupby_column_index][position.chunk_offset()] =
            is_nullable ? serialize_value(position.value(), position.is_null()) : serialize_value(position.value());
      });
    });
  }

  // Then assemble the group keys per row and get a ticket for each group key.
  auto tickets = std::vector<Ticket>(chunk->size());

  const auto row_count = chunk->size();
  for (auto offset = ChunkOffset{0}; offset < row_count; ++offset) {
    auto group_key = GroupKey(groupby_column_count);
    for (auto groupby_column_index = size_t{0}; groupby_column_index < groupby_column_count; ++groupby_column_index) {
      group_key[groupby_column_index] = std::move(group_keys_by_column[groupby_column_index][offset]);
    }
    tickets[offset] = _get_ticket(group_key);
  }

  // Compute aggregates
  const auto aggregate_count = _aggregates.size();
  for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
    const auto& aggregate = _aggregates[aggregate_index];

    // TODO(anyone): Same as above
    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());

    const auto segment = chunk->get_segment(pqp_column.column_id);

    resolve_data_type(pqp_column.data_type(), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      switch (aggregate->window_function) {
        // TODO(anyone): Add missing cases
        case WindowFunction::Min:
          _aggregate_segment<ColumnDataType, WindowFunction::Min>(aggregate_index, *segment, tickets);
          break;
        case WindowFunction::Max:
          _aggregate_segment<ColumnDataType, WindowFunction::Max>(aggregate_index, *segment, tickets);
          break;
        default:
          Fail("Unsupported aggregate function.");
      }
    });
  }
}

template <typename ColumnDataType, WindowFunction aggregate_function>
void AggregateDYOD::_aggregate_segment(size_t aggregate_index, const AbstractSegment& segment,
                                       const std::vector<Ticket>& tickets) {
  using AggregateDataType = typename WindowFunctionTraits<ColumnDataType, aggregate_function>::ReturnType;
  auto aggregator =
      WindowFunctionBuilder<ColumnDataType, AggregateDataType, aggregate_function>().get_aggregate_function();
  auto& aggregate_vector = static_cast<TypedAggregateVector<AggregateDataType>&>(*_aggregate_results[aggregate_index]);

  segment_iterate<ColumnDataType>(segment, [&](const auto& position) {
    const auto ticket = tickets[position.chunk_offset()];
    aggregator(position.value(), _aggregate_counts[aggregate_index][ticket], aggregate_vector.results[ticket]);
    _aggregate_counts[aggregate_index][ticket]++;
  });
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
