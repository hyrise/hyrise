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
#include "resolve_type.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace hyrise {

AggregateDYOD::AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                             const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                             const std::vector<ColumnID>& groupby_column_ids)
    : AbstractAggregateOperator(input_operator, aggregates, groupby_column_ids) {}

const std::string& AggregateDYOD::name() const {
  static const auto name = std::string{"AggregateDYOD"};
  return name;
}

// using AggregateType = typename WindowFunctionTraits<ColumnDataType, aggregate_function>::ReturnType;
template <typename ColumnDataType, typename AggregateType, WindowFunction window_function>
AggregateType _aggregate_all_values(const std::shared_ptr<const Table>& input_table, const ColumnID input_column_id) {
  const auto aggregate_function =
      WindowFunctionBuilder<ColumnDataType, AggregateType, window_function>().get_aggregate_function();
  auto accumulator = typename WindowFunctionTraits<ColumnDataType, window_function>::ReturnType{};
  auto value_count = size_t{0};
  const auto chunk_count = input_table->chunk_count();

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& segment = input_table->get_chunk(chunk_id)->get_segment(input_column_id);
    segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
      if (position.is_null()) {
        return;
      }
      aggregate_function(position.value(), value_count, accumulator);
      ++value_count;
    });
  }

  return accumulator;
}

std::shared_ptr<const Table> AggregateDYOD::_on_execute() {
  const auto input_table = _left_input->get_output();
  if (_groupby_column_ids.empty()) {
    // Produces only a single per aggregate.

    const auto aggregate_count = _aggregates.size();
    const auto chunk_count = input_table->chunk_count();

    auto column_definitions = TableColumnDefinitions{};
    auto result_values = std::vector<AllTypeVariant>{};
    column_definitions.reserve(aggregate_count);
    result_values.reserve(aggregate_count);

    for (auto aggregate_id = uint32_t{0}; aggregate_id < aggregate_count; ++aggregate_id) {
      const auto& aggregate = _aggregates[aggregate_id];
      column_definitions.emplace_back(aggregate->as_column_name(), aggregate->data_type(), true);

      const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
      const auto input_column_id = pqp_column.column_id;

      const auto data_type =
          input_column_id == INVALID_COLUMN_ID ? DataType::Long : input_table->column_data_type(input_column_id);
      resolve_data_type(data_type, [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;

        switch (aggregate->window_function) {
          case WindowFunction::Min: {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Min>::ReturnType;
            const auto result =
                _aggregate_all_values<ColumnDataType, AggregateType, WindowFunction::Min>(input_table, input_column_id);
            result_values.emplace_back(result);
            break;
          }
          case WindowFunction::Max: {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Max>::ReturnType;
            const auto result =
                _aggregate_all_values<ColumnDataType, AggregateType, WindowFunction::Max>(input_table, input_column_id);
            result_values.emplace_back(result);
            break;
          }
          case WindowFunction::Sum: {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Sum>::ReturnType;
            const auto result =
                _aggregate_all_values<ColumnDataType, AggregateType, WindowFunction::Sum>(input_table, input_column_id);
            result_values.emplace_back(result);
            break;
          }
          case WindowFunction::Avg: {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Avg>::ReturnType;
            const auto result =
                _aggregate_all_values<ColumnDataType, AggregateType, WindowFunction::Avg>(input_table, );
            result_values.emplace_back(result);
            break;
          }
          case WindowFunction::Count: {
            // TODO: handle COUNT(*)
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Count>::ReturnType;
            const auto result = _aggregate_all_values<ColumnDataType, AggregateType, WindowFunction::Count>(
                input_table, input_column_id);
            result_values.emplace_back(result);
            break;
          }
          case WindowFunction::CountDistinct: {
            using AggregateType =
                typename WindowFunctionTraits<ColumnDataType, WindowFunction::CountDistinct>::ReturnType;
            const auto result = _aggregate_all_values<ColumnDataType, AggregateType, WindowFunction::CountDistinct>(
                input_table, input_column_id);
            result_values.emplace_back(result);
            break;
          }
          // case WindowFunction::StandardDeviationSample: {
          //   using AggregateType =
          //       typename WindowFunctionTraits<ColumnDataType, WindowFunction::StandardDeviationSample>::ReturnType;
          //   const auto result = _aggregate_all_values<ColumnDataType, AggregateType, WindowFunction::StandardDeviationSample>(
          //       input_table, input_column_id);
          //   result_values.emplace_back(result);

          //   break;
          // }
          // TODO:
          // case WindowFunction::Any: {
          //   using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Any>::ReturnType;
          //   const auto result = _aggregate_all_values<ColumnDataType, AggregateType, WindowFunction::Any>(input_table, input_column_id);
          //   break;
          // }
          default:
            Fail(std::format("Unsupported aggregate function '{}'.",
                             window_function_to_string.left.at(aggregate->window_function)));
        }
      });
    }

    auto result_table = std::make_shared<Table>(column_definitions, TableType::Data);
    result_table->append(result_values);
    return result_table;
  }

  if (_aggregates.empty()) {}

  return std::make_shared<Table>(TableColumnDefinitions{{"dummy", DataType::Int, false}}, TableType::Data);
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
