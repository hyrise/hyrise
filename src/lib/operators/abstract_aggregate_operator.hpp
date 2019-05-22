#pragma once

#include "expression/aggregate_expression.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "type_comparison.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Aggregates are defined by the column (ColumnID for Operators, LQPColumnReference in LQP) they operate on and the aggregate
 * function they use. COUNT() is the exception that does not necessarily use a column, which is why column is optional.
 *   COUNT(*) does not use a column and returns the number of rows
 *   COUNT(<column>) does use a column and returns the number of rows with non-null values in <column>
 * Optionally, an alias can be specified for the columns holding the result of an aggregate function.
 *
 * Further, the aggregate operator is used to perform DISTINCT operations. This functionality is achieved by having no aggregates.
 */
struct AggregateColumnDefinition final {
  AggregateColumnDefinition(const std::optional<ColumnID>& column, const AggregateFunction function)
      : column(column), function(function) {}

  const std::optional<ColumnID> column;
  const AggregateFunction function;
};

/*
The AggregateFunctionBuilder is used to create the lambda function that will be used by
the AggregateVisitor. It is a separate class because methods cannot be partially specialized.
Therefore, we partially specialize the whole class and define the get_aggregate_function anew every time.
*/
template <typename ColumnDataType, typename AggregateType, AggregateFunction function>
class AggregateFunctionBuilder {
 public:
  void get_aggregate_function() { Fail("Invalid aggregate function"); }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Min> {
 public:
  auto get_aggregate_function() {
    return [](const ColumnDataType& new_value, std::optional<AggregateType>& current_primary_aggregate,
              std::optional<AggregateType>& current_secondary_aggregate) {
      if (!current_primary_aggregate || value_smaller(new_value, *current_primary_aggregate)) {
        // New minimum found
        current_primary_aggregate = new_value;
      }
    };
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Max> {
 public:
  auto get_aggregate_function() {
    return [](const ColumnDataType& new_value, std::optional<AggregateType>& current_primary_aggregate,
              std::optional<AggregateType>& current_secondary_aggregate) {
      if (!current_primary_aggregate || value_greater(new_value, *current_primary_aggregate)) {
        // New maximum found
        current_primary_aggregate = new_value;
      }
    };
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Sum> {
 public:
  auto get_aggregate_function() {
    return [](const ColumnDataType& new_value, std::optional<AggregateType>& current_primary_aggregate,
              std::optional<AggregateType>& current_secondary_aggregate) {
      // add new value to sum
      if (current_primary_aggregate) {
        *current_primary_aggregate += new_value;
      } else {
        current_primary_aggregate = new_value;
      }
    };
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Avg> {
 public:
  auto get_aggregate_function() {
    /*
     * We reuse Sum here, as updating an average value for every row is costly and prone to problems regarding precision.
     * To get the average, the aggregate operator needs to count the number of elements contributing to this sum,
     * and divide the final sum by that number.
     */
    return AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Sum>{}.get_aggregate_function();
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::StdDevSamp> {
 public:
  auto get_aggregate_function() {
    return [](const ColumnDataType& new_value, std::optional<AggregateType>& current_primary_aggregate,
              std::optional<AggregateType>& current_secondary_aggregate) {
      if constexpr (std::is_arithmetic_v<ColumnDataType>) {
        // add new value to sum of x
        if (current_primary_aggregate) {
          *current_primary_aggregate += new_value;
        } else {
          current_primary_aggregate = new_value;
        }
        // add new value to sum of x²
        if (current_secondary_aggregate) {
          *current_secondary_aggregate += new_value * new_value;
        } else {
          current_secondary_aggregate = new_value * new_value;
        }
      } else {
        Fail("Stddev not available for non-arithmetic types.");
      }
    };
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Count> {
 public:
  auto get_aggregate_function() {
    return [](const ColumnDataType&, std::optional<AggregateType>& current_primary_aggregate,
              std::optional<AggregateType>& current_secondary_aggregate) {};
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::CountDistinct> {
 public:
  auto get_aggregate_function() {
    return [](const ColumnDataType&, std::optional<AggregateType>& current_primary_aggregate,
              std::optional<AggregateType>& current_secondary_aggregate) {};
  }
};

class AbstractAggregateOperator : public AbstractReadOnlyOperator {
 public:
  AbstractAggregateOperator(const std::shared_ptr<AbstractOperator>& in,
                            const std::vector<AggregateColumnDefinition>& aggregates,
                            const std::vector<ColumnID>& groupby_column_ids);

  const std::vector<AggregateColumnDefinition>& aggregates() const;

  const std::vector<ColumnID>& groupby_column_ids() const;

  const std::string name() const override = 0;

  const std::string description(DescriptionMode description_mode) const override;

 protected:
  void _validate_aggregates() const;

  Segments _output_segments;
  const std::vector<AggregateColumnDefinition> _aggregates;
  const std::vector<ColumnID> _groupby_column_ids;

  TableColumnDefinitions _output_column_definitions;
};

}  // namespace opossum
