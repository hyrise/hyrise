#include "abstract_aggregate_operator.hpp"

#include "abstract_operator.hpp"
#include "abstract_read_only_operator.hpp"
#include "expression/pqp_column_expression.hpp"
#include "types.hpp"

namespace opossum {

AbstractAggregateOperator::AbstractAggregateOperator(
    const std::shared_ptr<AbstractOperator>& in, const std::vector<std::shared_ptr<AggregateExpression>>& aggregates,
    const std::vector<ColumnID>& groupby_column_ids,
    std::unique_ptr<AbstractOperatorPerformanceData> init_performance_data)
    : AbstractReadOnlyOperator(OperatorType::Aggregate, in, nullptr, std::move(init_performance_data)),
      _aggregates{aggregates},
      _groupby_column_ids{groupby_column_ids} {
  /*
   * We can either have no group by columns or no aggregates, but not both:
   *   No group by columns -> aggregate on the whole input table, not subgroups of it
   *   No aggregates -> effectively performs a DISTINCT operation over all group by columns
   */
  Assert(!(aggregates.empty() && groupby_column_ids.empty()),
         "Neither aggregate nor groupby columns have been specified");
}

const std::vector<std::shared_ptr<AggregateExpression>>& AbstractAggregateOperator::aggregates() const {
  return _aggregates;
}
const std::vector<ColumnID>& AbstractAggregateOperator::groupby_column_ids() const { return _groupby_column_ids; }

std::string AbstractAggregateOperator::description(DescriptionMode description_mode) const {
  const auto* const separator = description_mode == DescriptionMode::SingleLine ? " " : "\n";

  std::stringstream desc;
  desc << name() << separator << "GroupBy ColumnIDs: ";
  for (size_t groupby_column_idx = 0; groupby_column_idx < _groupby_column_ids.size(); ++groupby_column_idx) {
    desc << _groupby_column_ids[groupby_column_idx];

    if (groupby_column_idx + 1 < _groupby_column_ids.size()) {
      desc << ", ";
    }
  }

  desc << " Aggregates: ";
  for (size_t expression_idx = 0; expression_idx < _aggregates.size(); ++expression_idx) {
    const auto& aggregate = _aggregates[expression_idx];
    desc << aggregate->as_column_name();

    if (expression_idx + 1 < _aggregates.size()) desc << ", ";
  }
  return desc.str();
}

/**
 * Asserts that all aggregates are valid.
 * Invalid aggregates are e.g. MAX(*) or AVG(<string column>).
 */
void AbstractAggregateOperator::_validate_aggregates() const {
  const auto input_table = left_input_table();
  for (const auto& aggregate : _aggregates) {
    const auto pqp_column = std::dynamic_pointer_cast<PQPColumnExpression>(aggregate->argument());
    DebugAssert(pqp_column,
                "Aggregate operators can currently only handle physical columns, no complicated expressions");
    const auto column_id = pqp_column->column_id;
    if (column_id == INVALID_COLUMN_ID) {
      Assert(aggregate->aggregate_function == AggregateFunction::Count, "Aggregate: Asterisk is only valid with COUNT");
    } else {
      DebugAssert(column_id < input_table->column_count(), "Aggregate column index out of bounds");
      DebugAssert(pqp_column->data_type() == input_table->column_data_type(column_id),
                  "Mismatching column_data_type for input column");
      Assert(input_table->column_data_type(column_id) != DataType::String ||
                 (aggregate->aggregate_function != AggregateFunction::Sum &&
                  aggregate->aggregate_function != AggregateFunction::Avg &&
                  aggregate->aggregate_function != AggregateFunction::StandardDeviationSample),
             "Aggregate: Cannot calculate SUM, AVG or STDDEV_SAMP on string column");
    }
  }
}

}  // namespace opossum
