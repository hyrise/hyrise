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

AggregateDYOD::AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                             const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                             const std::vector<ColumnID>& groupby_column_ids)
    : AbstractAggregateOperator(input_operator, aggregates, groupby_column_ids) {}

const std::string& AggregateDYOD::name() const {
  static const auto name = std::string{"AggregateDYOD"};
  return name;
}

std::shared_ptr<const Table> AggregateDYOD::_on_execute() {
  _validate_aggregates();
  _resolve_aggregate_data_types();

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

  for (const auto column_id : groupby_column_ids()) {
    column_definitions.emplace_back(input_table->column_name(column_id), input_table->column_data_type(column_id),
                                    input_table->column_is_nullable(column_id));
  }

  const auto aggregate_count = _aggregates.size();
  for (auto aggregate_index = size_t{0}; aggregate_index < aggregate_count; ++aggregate_index) {
    const auto& aggregate = _aggregates[aggregate_index];
    const auto aggregate_function = aggregate->window_function;
    const auto needs_null =
        (aggregate_function != WindowFunction::Count && aggregate_function != WindowFunction::CountDistinct);
    column_definitions.emplace_back(aggregate->as_column_name(), _aggregate_data_types[aggregate_index], needs_null);
  }

  return std::make_shared<Table>(column_definitions, TableType::Data);
}

void AggregateDYOD::_aggregate_chunk(std::shared_ptr<Chunk> chunk) {
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

  // Then assemble the group keys per row. Now, the first dimension is the chunk offset of the row, and the second
  // dimension is the index of the grouping column.
  auto group_keys_by_row = std::vector<GroupKey>(chunk->size(), std::vector<GroupKeyEntry>(_groupby_column_ids.size()));

  const auto row_count = chunk->size();
  for (auto groupby_column_index = size_t{0}; groupby_column_index < groupby_column_count; ++groupby_column_index) {
    for (auto offset = ChunkOffset{0}; offset < row_count; ++offset) {
      // TODO(anyone): Can we move instead of copying?
      group_keys_by_row[offset][groupby_column_index] = group_keys_by_column[groupby_column_index][offset];
    }
  }
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
