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
  static bool supported(const std::optional<DataType>& column_data_type, const AggregateFunction function);

  AggregateColumnDefinition(const std::optional<ColumnID>& column, const AggregateFunction function);

  std::optional<ColumnID> column;
  AggregateFunction function;
};

// gtest
bool operator<(const AggregateColumnDefinition& lhs, const AggregateColumnDefinition& rhs);
bool operator==(const AggregateColumnDefinition& lhs, const AggregateColumnDefinition& rhs);

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
    return [](const ColumnDataType& new_value, std::optional<AggregateType>& current_aggregate) {
      if (!current_aggregate || value_smaller(new_value, *current_aggregate)) {
        // New minimum found
        current_aggregate = new_value;
      }
    };
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Max> {
 public:
  auto get_aggregate_function() {
    return [](const ColumnDataType& new_value, std::optional<AggregateType>& current_aggregate) {
      if (!current_aggregate || value_greater(new_value, *current_aggregate)) {
        // New maximum found
        current_aggregate = new_value;
      }
    };
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Sum> {
 public:
  auto get_aggregate_function() {
    return [](const ColumnDataType& new_value, std::optional<AggregateType>& current_aggregate) {
      // add new value to sum
      if (current_aggregate) {
        *current_aggregate += new_value;
      } else {
        current_aggregate = new_value;
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
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::CountRows> {
 public:
  auto get_aggregate_function() {
    return [](const ColumnDataType&, std::optional<AggregateType>& current_aggregate) {};
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::CountNonNull> {
 public:
  auto get_aggregate_function() {
    return [](const ColumnDataType&, std::optional<AggregateType>& current_aggregate) {};
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::CountDistinct> {
 public:
  auto get_aggregate_function() {
    return [](const ColumnDataType&, std::optional<AggregateType>& current_aggregate) {};
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
  TableColumnDefinitions _get_output_column_defintions() const;

  Segments _output_segments;
  const std::vector<AggregateColumnDefinition> _aggregates;
  const std::vector<ColumnID> _groupby_column_ids;

  TableColumnDefinitions _output_column_definitions;
};

}  // namespace opossum
