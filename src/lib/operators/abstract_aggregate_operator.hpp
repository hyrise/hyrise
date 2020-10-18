#pragma once

#include <boost/container/small_vector.hpp>

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
template <typename ColumnDataType, typename AggregateType, AggregateFunction function>
class AggregateFunctionBuilder {
 public:
  void get_aggregate_function() { Fail("Invalid aggregate function"); }
};

// Some aggregates need to hold more than a single value. Currently, this is used for StandardDeviationSample, which
// requires three fields. As such, we use a small_vector, which could also be used for other purposes and set the
// in-object storage size to three.
template <typename AggregateType>
using SecondaryAggregates = boost::container::small_vector<AggregateType, 3, PolymorphicAllocator<AggregateType>>;

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::Min> {
 public:
  auto get_aggregate_function() {
    return [](const ColumnDataType& new_value, std::optional<AggregateType>& current_primary_aggregate) {
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
    return [](const ColumnDataType& new_value, std::optional<AggregateType>& current_primary_aggregate) {
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
    return [](const ColumnDataType& new_value, std::optional<AggregateType>& current_primary_aggregate) {
      // add new value to sum
      if (current_primary_aggregate) {
        *current_primary_aggregate += static_cast<AggregateType>(new_value);
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
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::StandardDeviationSample> {
 public:
  auto get_aggregate_function() {
    return [](const ColumnDataType& new_value, std::optional<AggregateType>& current_primary_aggregate,
              SecondaryAggregates<AggregateType>& current_secondary_aggregates) {
      if constexpr (std::is_arithmetic_v<ColumnDataType>) {
        // Welford's online algorithm
        // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
        // For a new value, compute the new count, new mean and the new squared_distance_from_mean.
        // mean accumulates the mean of the entire dataset.
        // squared_distance_from_mean aggregates the squared distance from the mean.
        // count aggregates the number of samples seen so far.
        // The sample standard deviation is stored as current_primary_aggregate.
        if (current_secondary_aggregates.empty()) {
          current_secondary_aggregates.resize(3);
          // current_secondary_aggregates[0]:   storage for count
          // current_secondary_aggregates[1]:   storage for mean
          // current_secondary_aggregates[2]:   storage for squared_distance_from_mean
        }

        // get values
        auto& count = current_secondary_aggregates[0];
        auto& mean = current_secondary_aggregates[1];
        auto& squared_distance_from_mean = current_secondary_aggregates[2];

        // update values
        ++count;
        const double delta = static_cast<double>(new_value) - mean;
        mean += delta / count;
        const double delta2 = static_cast<double>(new_value) - mean;
        squared_distance_from_mean += delta * delta2;

        if (count > 1) {
          const auto variance = squared_distance_from_mean / (count - 1);
          current_primary_aggregate = std::sqrt(variance);
        } else {
          current_primary_aggregate = std::nullopt;
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
    return [](const ColumnDataType&, std::optional<AggregateType>& current_primary_aggregate) {};
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::CountDistinct> {
 public:
  auto get_aggregate_function() {
    return [](const ColumnDataType&, std::optional<AggregateType>& current_primary_aggregate) {};
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
