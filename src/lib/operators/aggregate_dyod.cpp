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

std::shared_ptr<const Table> AggregateDYOD::_on_execute() {
  return _create_output_table();
}

std::shared_ptr<Table> AggregateDYOD::_create_output_table() {
  const auto input_table = left_input_table();
  auto column_definitions = TableColumnDefinitions();

  for (const auto column_id : groupby_column_ids()) {
    column_definitions.emplace_back(input_table->column_name(column_id), input_table->column_data_type(column_id),
                                    input_table->column_is_nullable(column_id));
  }

  for (const auto& aggregate : _aggregates) {
    // TODO(anyone): Is this cast guaranteed to work? Or are there cases where there is no argument or the
    // argument is not a PQPColumnExpression?
    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());

    resolve_data_type(pqp_column.data_type(), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      switch (aggregate->window_function) {
        // TODO(anyone): Add missing cases
        case WindowFunction::Min:
          _append_aggregate_column_definition<ColumnDataType, WindowFunction::Min>(column_definitions, *aggregate);
          break;
        case WindowFunction::Max:
          _append_aggregate_column_definition<ColumnDataType, WindowFunction::Max>(column_definitions, *aggregate);
          break;
        default:
          Fail("Unsupported aggregate function.");
      }
    });
  }

  return std::make_shared<Table>(column_definitions, TableType::Data);
}

template <typename ColumnDataType, WindowFunction aggregate_function>
void AggregateDYOD::_append_aggregate_column_definition(TableColumnDefinitions& column_definitions,
                                                        const WindowFunctionExpression& aggregate) {
  // Retrieve type information from the aggregation traits.
  auto result_type = WindowFunctionTraits<ColumnDataType, aggregate_function>::RESULT_TYPE;

  constexpr auto NEEDS_NULL =
      (aggregate_function != WindowFunction::Count && aggregate_function != WindowFunction::CountDistinct);

  column_definitions.emplace_back(aggregate.as_column_name(), result_type, NEEDS_NULL);
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
