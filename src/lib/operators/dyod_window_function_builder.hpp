#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "expression/window_function_expression.hpp"
#include "operators/abstract_aggregate_operator.hpp"

namespace hyrise {

/*
The DYODWindowFunctionBuilder is used to create the lambda function that will be used by
the AggregateVisitor. It is a separate class because methods cannot be partially specialized.
Therefore, we partially specialize the whole class and define the get_aggregate_function anew every time.
See aggregate_dyod.hpp::DYODAggregateResult for an explanation of AccumulatorType
*/
template <typename ColumnDataType, typename AccumulatorType, WindowFunction aggregate_function>
class DYODWindowFunctionBuilder {
 public:
  constexpr void get_aggregate_function() {
    Fail("Invalid aggregate function");
  }
};

template <typename ColumnDataType, typename AccumulatorType>
class DYODWindowFunctionBuilder<ColumnDataType, AccumulatorType, WindowFunction::Min> {
 public:
  constexpr auto get_aggregate_function() {
    return [](const ColumnDataType& new_value, const bool has_aggregates, AccumulatorType& accumulator) {
      // We need to check if we have already seen a value before (`!has_aggregates`) - otherwise, `accumulator`
      // holds an invalid value. While we might initialize `accumulator` with the smallest possible numerical value,
      // this approach does not work for `max` on strings. To keep the code simple, we check `has_aggregates` here.
      if (!has_aggregates || value_smaller(new_value, accumulator)) {
        // New minimum found
        accumulator = new_value;
      }
    };
  }
};

template <typename ColumnDataType, typename AccumulatorType>
class DYODWindowFunctionBuilder<ColumnDataType, AccumulatorType, WindowFunction::Max> {
 public:
  constexpr auto get_aggregate_function() {
    return [](const ColumnDataType& new_value, const bool has_aggregates, AccumulatorType& accumulator) {
      if (!has_aggregates || value_greater(new_value, accumulator)) {
        // New maximum found
        accumulator = new_value;
      }
    };
  }
};

template <typename ColumnDataType, typename AccumulatorType>
class DYODWindowFunctionBuilder<ColumnDataType, AccumulatorType, WindowFunction::Sum> {
 public:
  constexpr auto get_aggregate_function() {
    return [](const ColumnDataType& new_value, const bool /*has_aggregates*/, AccumulatorType& accumulator) {
      // Add new value to sum - no need to check if this is the first value as `sum` is only defined on numerical values
      // and the accumulator is initialized with 0.
      accumulator += static_cast<AccumulatorType>(new_value);
    };
  }
};

template <typename ColumnDataType, typename AccumulatorType>
class DYODWindowFunctionBuilder<ColumnDataType, AccumulatorType, WindowFunction::Avg> {
 public:
  constexpr auto get_aggregate_function() {
    // We reuse Sum here, as updating an average value for every row is costly and prone to problems regarding
    // precision. To get the average, the aggregate operator needs to count the number of elements contributing to this
    // sum, and divide the final sum by that number.
    return [](const ColumnDataType& new_value, const bool /*has_aggregates*/, AccumulatorType& accumulator) {
      // Add new value to sum - no need to check if this is the first value as `sum` is only defined on numerical values
      // and the accumulator is initialized with 0.
      accumulator.first += static_cast<decltype(accumulator.first)>(new_value);
      ++accumulator.second;
    };
  }
};

template <typename ColumnDataType, typename AccumulatorType>
class DYODWindowFunctionBuilder<ColumnDataType, AccumulatorType, WindowFunction::StandardDeviationSample> {
 public:
  constexpr auto get_aggregate_function() {
    return [](const ColumnDataType& new_value, const bool /*has_aggregate*/, AccumulatorType& accumulator) {
      if constexpr (std::is_arithmetic_v<ColumnDataType>) {
        // Welford's online algorithm
        // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
        // For a new value, compute the new count, new mean and the new squared_distance_from_mean.

        // Get values
        auto& count = accumulator[0];
        auto& mean = accumulator[1];
        auto& squared_distance_from_mean = accumulator[2];
        auto& result = accumulator[3];

        // Update values
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

template <typename ColumnDataType, typename AccumulatorType>
class DYODWindowFunctionBuilder<ColumnDataType, AccumulatorType, WindowFunction::Count> {
 public:
  constexpr auto get_aggregate_function() {
    return [](const ColumnDataType& /*new_value*/, const bool /*has_aggregates*/, AccumulatorType& accumulator) {
      ++accumulator;
    };
  }
};

template <typename ColumnDataType, typename AccumulatorType>
class DYODWindowFunctionBuilder<ColumnDataType, AccumulatorType, WindowFunction::CountDistinct> {
 public:
  constexpr auto get_aggregate_function() {
    return [](const ColumnDataType& new_value, const bool /*has_aggregates*/, AccumulatorType& accumulator) {
      // For the case of CountDistinct, insert the current value into the set to keep track of distinct values.
      accumulator.emplace(new_value);
    };
  }
};

}  // namespace hyrise
