#pragma once

#include "expression/aggregate_expression.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "type_comparison.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Aggregates are defined by the column (ColumnID for Operators, LQPColumnReference in LQP) they operate on and the aggregate
 * function they use. COUNT() is the exception that doesn't use a column, which is why column is optional
 * Optionally, an alias can be specified to use as the output name.
 */
struct AggregateColumnDefinition final {
  AggregateColumnDefinition(const std::optional<ColumnID>& column, const AggregateFunction function)
      : column(column), function(function) {}

  std::optional<ColumnID> column;
  AggregateFunction function;
};

/*
The AggregateFunctionBuilder is used to create the lambda function that will be used by
the AggregateVisitor. It is a separate class because methods cannot be partially specialized.
Therefore, we partially specialize the whole class and define the get_aggregate_function anew every time.
*/
template <typename ColumnDataType, typename AggregateType, AggregateFunction function>
struct AggregateFunctionBuilder {
  void get_aggregate_function() { Fail("Invalid aggregate function"); }
};

template <typename ColumnDataType, typename AggregateType>
struct AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Min> {
  auto get_aggregate_function() {
    return [](const ColumnDataType& new_value, std::optional<AggregateType>& current_aggregate) {
      if (!current_aggregate || value_smaller(new_value, *current_aggregate)) {
        // New minimum found
        current_aggregate = new_value;
      }
      return *current_aggregate;
    };
  }
};

template <typename ColumnDataType, typename AggregateType>
struct AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Max> {
  auto get_aggregate_function() {
    return [](const ColumnDataType& new_value, std::optional<AggregateType>& current_aggregate) {
      if (!current_aggregate || value_greater(new_value, *current_aggregate)) {
        // New maximum found
        current_aggregate = new_value;
      }
      return *current_aggregate;
    };
  }
};

template <typename ColumnDataType, typename AggregateType>
struct AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Sum> {
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
struct AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Avg> {
  auto get_aggregate_function() {
    // We reuse Sum here and use it together with aggregate_count to calculate the average
    return AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Sum>{}.get_aggregate_function();
  }
};

template <typename ColumnDataType, typename AggregateType>
struct AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Count> {
  auto get_aggregate_function() {
    return [](const ColumnDataType&, std::optional<AggregateType>& current_aggregate) { return std::nullopt; };
  }
};

template <typename ColumnDataType, typename AggregateType>
struct AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::CountDistinct> {
  auto get_aggregate_function() {
    return [](const ColumnDataType&, std::optional<AggregateType>& current_aggregate) { return std::nullopt; };
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
  Segments _output_segments;
  const std::vector<AggregateColumnDefinition> _aggregates;
  const std::vector<ColumnID> _groupby_column_ids;

  TableColumnDefinitions _output_column_definitions;
};

}  // namespace opossum

