#include "abstract_aggregate_operator.hpp"
#include "abstract_operator.hpp"
#include "abstract_read_only_operator.hpp"
#include "operators/aggregate/aggregate_traits.hpp"

#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

bool AggregateColumnDefinition::supported(const std::optional<DataType>& column_data_type,
                                          const AggregateFunction function) {
  if (!column_data_type) {
    return function == AggregateFunction::CountRows;
  }

  if (*column_data_type == DataType::String) {
    return function != AggregateFunction::Sum && function != AggregateFunction::Avg;
  }

  return true;
}

AggregateColumnDefinition::AggregateColumnDefinition(const std::optional<ColumnID>& column,
                                                     const AggregateFunction function)
    : column(column), function(function) {}

bool operator<(const AggregateColumnDefinition& lhs, const AggregateColumnDefinition& rhs) {
  return std::tie(lhs.column, lhs.function) < std::tie(rhs.column, rhs.function);
}

bool operator==(const AggregateColumnDefinition& lhs, const AggregateColumnDefinition& rhs) {
  return std::tie(lhs.column, lhs.function) == std::tie(rhs.column, rhs.function);
}

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

const std::string AbstractAggregateOperator::description(DescriptionMode description_mode) const {
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

    if (aggregate.column) {
      desc << "(Column #" << *aggregate.column << ")";
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
    if (!aggregate.column) {
      Assert(aggregate.function == AggregateFunction::CountRows, "Aggregate: Asterisk is only valid with COUNT");
    } else {
      DebugAssert(*aggregate.column < input_table->column_count(), "Aggregate column index out of bounds");
      Assert(input_table->column_data_type(*aggregate.column) != DataType::String ||
                 (aggregate.function != AggregateFunction::Sum && aggregate.function != AggregateFunction::Avg &&
                  aggregate.function != AggregateFunction::StandardDeviationSample),
             "Aggregate: Cannot calculate SUM, AVG or STDDEV_SAMP on string column");
    }
  }
}

TableColumnDefinitions AbstractAggregateOperator::_get_output_column_defintions() const {
  auto table_column_definitions = TableColumnDefinitions{};
  table_column_definitions.reserve(_groupby_column_ids.size() + _aggregates.size());

  for (const auto& group_by_column_id : _groupby_column_ids) {
    table_column_definitions.emplace_back(input_table_left()->column_definitions()[group_by_column_id]);
  }

  for (const auto& aggregate : _aggregates) {
    std::stringstream column_name_stream;
    if (aggregate.function == AggregateFunction::CountDistinct) {
      column_name_stream << "COUNT(DISTINCT ";
    } else {
      column_name_stream << aggregate.function << "(";
    }

    if (aggregate.column) {
      column_name_stream << input_table_left()->column_name(*aggregate.column);
    } else {
      column_name_stream << "*";
    }
    column_name_stream << ")";

    auto aggregate_data_type = DataType{};

    if (aggregate.column) {
      resolve_data_type(input_table_left()->column_data_type(*aggregate.column), [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;

        switch (aggregate.function) {
          case AggregateFunction::Min:
            aggregate_data_type = AggregateTraits<ColumnDataType, AggregateFunction::Min>::AGGREGATE_DATA_TYPE;
            break;
          case AggregateFunction::Max:
            aggregate_data_type = AggregateTraits<ColumnDataType, AggregateFunction::Max>::AGGREGATE_DATA_TYPE;
            break;
          case AggregateFunction::Sum:
            aggregate_data_type = AggregateTraits<ColumnDataType, AggregateFunction::Sum>::AGGREGATE_DATA_TYPE;
            break;
          case AggregateFunction::Avg:
            aggregate_data_type = AggregateTraits<ColumnDataType, AggregateFunction::Avg>::AGGREGATE_DATA_TYPE;
            break;
          case AggregateFunction::CountRows:
            aggregate_data_type = AggregateTraits<ColumnDataType, AggregateFunction::CountRows>::AGGREGATE_DATA_TYPE;
            break;
          case AggregateFunction::CountNonNull:
            aggregate_data_type = AggregateTraits<ColumnDataType, AggregateFunction::CountNonNull>::AGGREGATE_DATA_TYPE;
            break;
          case AggregateFunction::CountDistinct:
            aggregate_data_type =
                AggregateTraits<ColumnDataType, AggregateFunction::CountDistinct>::AGGREGATE_DATA_TYPE;
            break;
          case AggregateFunction::StandardDeviationSample:
            aggregate_data_type =
                AggregateTraits<ColumnDataType, AggregateFunction::StandardDeviationSample>::AGGREGATE_DATA_TYPE;
            break;
        }
      });
    } else {
      aggregate_data_type = DataType::Long;
    }

    const auto nullable =
        (aggregate.function != AggregateFunction::CountRows && aggregate.function != AggregateFunction::CountNonNull &&
         aggregate.function != AggregateFunction::CountDistinct);
    table_column_definitions.emplace_back(column_name_stream.str(), aggregate_data_type, nullable);
  }

  return table_column_definitions;
}

}  // namespace opossum
