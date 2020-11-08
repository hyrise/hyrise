#pragma once

#include "expression/aggregate_expression.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "type_comparison.hpp"
#include "types.hpp"

namespace opossum {

/*
The AggregateFunctionBuilder is used to create the lambda function that will be used by
the AggregateVisitor. It is a separate class because methods cannot be partially specialized.
Therefore, we partially specialize the whole class and define the get_aggregate_function anew every time.
*/
template <typename ColumnDataType, typename AggregateType, AggregateFunction aggregate_function>
class AggregateFunctionBuilder {
 public:
  void get_aggregate_function() { Fail("Invalid aggregate function"); }
};

using StandardDeviationSampleData = std::array<double, 4>;

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Min> {
 public:
  auto get_aggregate_function() {
    return [](const ColumnDataType& new_value, const size_t aggregate_count, AggregateType& accumulator) {
      // We need to check if we have already seen a value before (`aggregate_count > 0`) - otherwise, `accumulator`
      // holds an invalid value. While we might initialize `accumulator` with the smallest possible numerical value,
      // this approach does not work for `max` on strings. To keep the code simple, we check `aggregate_count` here.
      if (aggregate_count == 0 || value_smaller(new_value, accumulator)) {
        // New minimum found
        accumulator = new_value;
      }
    };
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Max> {
 public:
  auto get_aggregate_function() {
    return [](const ColumnDataType& new_value, const size_t aggregate_count, AggregateType& accumulator) {
      if (aggregate_count == 0 || value_greater(new_value, accumulator)) {
        // New maximum found
        accumulator = new_value;
      }
    };
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Sum> {
 public:
  auto get_aggregate_function() {
    return [](const ColumnDataType& new_value, const size_t aggregate_count, AggregateType& accumulator) {
      // Add new value to sum - no need to check if this is the first value as `sum` is only defined on numerical values
      // and the accumulator is initialized with 0.
      accumulator += static_cast<AggregateType>(new_value);
    };
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Avg> {
 public:
  auto get_aggregate_function() {
    // We reuse Sum here, as updating an average value for every row is costly and prone to problems regarding
    // precision. To get the average, the aggregate operator needs to count the number of elements contributing to this
    // sum, and divide the final sum by that number.
    return AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Sum>{}.get_aggregate_function();
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::StandardDeviationSample> {
 public:
  auto get_aggregate_function() {
    return [](const ColumnDataType& new_value, const size_t aggregate_count, StandardDeviationSampleData& accumulator) {
      if constexpr (std::is_arithmetic_v<ColumnDataType>) {
        // Welford's online algorithm
        // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
        // For a new value, compute the new count, new mean and the new squared_distance_from_mean.

        // get values
        auto& count = accumulator[0];  // We could probably reuse aggregate_count here
        auto& mean = accumulator[1];
        auto& squared_distance_from_mean = accumulator[2];
        auto& result = accumulator[3];

        // update values
        ++count;
        const double delta = static_cast<double>(new_value) - mean;
        mean += delta / count;
        const double delta2 = static_cast<double>(new_value) - mean;
        squared_distance_from_mean += delta * delta2;

        if (count > 1) {
          // The SQL standard defines VAR_SAMP (which is the basis of STDDEV_SAMP) as NULL if the number of values is 1.
          const auto variance = squared_distance_from_mean / (count - 1);
          result = std::sqrt(variance);
        }

      } else {
        Fail("StandardDeviationSample not available for non-arithmetic types.");
      }
    };
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Count> {
 public:
  auto get_aggregate_function() {
    return [](const ColumnDataType&, const size_t aggregate_count, AggregateType& accumulator) {};
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::CountDistinct> {
 public:
  auto get_aggregate_function() {
    return [](const ColumnDataType&, const size_t aggregate_count, AggregateType& accumulator) {};
  }
};

class AbstractAggregateOperator : public AbstractReadOnlyOperator {
 public:
  AbstractAggregateOperator(const std::shared_ptr<AbstractOperator>& in,
                            const std::vector<std::shared_ptr<AggregateExpression>>& aggregates,
                            const std::vector<ColumnID>& groupby_column_ids,
                            std::unique_ptr<AbstractOperatorPerformanceData> performance_data =
                                std::make_unique<OperatorPerformanceData<AbstractOperatorPerformanceData::NoSteps>>());

  const std::vector<std::shared_ptr<AggregateExpression>>& aggregates() const;

  const std::vector<ColumnID>& groupby_column_ids() const;

  const std::string& name() const override = 0;

  std::string description(DescriptionMode description_mode) const override;

 protected:
  void _validate_aggregates() const;

  Segments _output_segments;
  const std::vector<std::shared_ptr<AggregateExpression>> _aggregates;
  const std::vector<ColumnID> _groupby_column_ids;

  TableColumnDefinitions _output_column_definitions;
};

}  // namespace opossum
