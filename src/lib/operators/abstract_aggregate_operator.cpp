#include "abstract_aggregate_operator.hpp"
#include "abstract_operator.hpp"
#include "abstract_read_only_operator.hpp"

#include "types.hpp"

namespace opossum {

AbstractAggregateOperator::AbstractAggregateOperator(const std::shared_ptr<AbstractOperator>& in,
                                                     const std::vector<AggregateColumnDefinition>& aggregates,
                                                     const std::vector<ColumnID>& groupby_column_ids)
    : AbstractReadOnlyOperator(OperatorType::Aggregate, in),
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

const std::vector<AggregateColumnDefinition>& AbstractAggregateOperator::aggregates() const { return _aggregates; }
const std::vector<ColumnID>& AbstractAggregateOperator::groupby_column_ids() const { return _groupby_column_ids; }

std::string AbstractAggregateOperator::description(DescriptionMode description_mode) const {
  std::stringstream desc;
  desc << "[" << name() << "] "
       << "GroupBy ColumnIDs: ";
  for (size_t groupby_column_idx = 0; groupby_column_idx < _groupby_column_ids.size(); ++groupby_column_idx) {
    desc << _groupby_column_ids[groupby_column_idx];

    if (groupby_column_idx + 1 < _groupby_column_ids.size()) {
      desc << ", ";
    }
  }

  desc << " Aggregates: ";
  for (size_t expression_idx = 0; expression_idx < _aggregates.size(); ++expression_idx) {
    const auto& aggregate = _aggregates[expression_idx];
    desc << aggregate.function;

    if (aggregate.column != INVALID_COLUMN_ID) {
      desc << "(Column #" << aggregate.column << ")";
    } else {
      // COUNT(*) does not use a column
      desc << "(*)";
    }

    if (expression_idx + 1 < _aggregates.size()) desc << ", ";
  }
  return desc.str();
}

/**
 * Asserts that all aggregates are valid.
 * Invalid aggregates are e.g. MAX(*) or AVG(<string column>).
 */
void AbstractAggregateOperator::_validate_aggregates() const {
  const auto input_table = input_table_left();
  for (const auto& aggregate : _aggregates) {
    if (aggregate.column == INVALID_COLUMN_ID) {
      Assert(aggregate.function == AggregateFunction::Count, "Aggregate: Asterisk is only valid with COUNT");
    } else {
      DebugAssert(aggregate.column < input_table->column_count(), "Aggregate column index out of bounds");
      Assert(input_table->column_data_type(aggregate.column) != DataType::String ||
                 (aggregate.function != AggregateFunction::Sum && aggregate.function != AggregateFunction::Avg &&
                  aggregate.function != AggregateFunction::StandardDeviationSample),
             "Aggregate: Cannot calculate SUM, AVG or STDDEV_SAMP on string column");
    }
  }
}

}  // namespace opossum
