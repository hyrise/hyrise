#pragma once

#include "resolve_type.hpp"

namespace opossum {

/*
The following structs describe the different aggregate traits.
Given a ColumnType and AggregateFunction, certain traits like the aggregate type
can be deduced.
*/
template <typename ColumnType, AggregateFunction aggregate_function, class Enable = void>
struct AggregateTraits {};

// COUNT on all types
template <typename ColumnType>
struct AggregateTraits<ColumnType, AggregateFunction::Count> {
  typedef int64_t AggregateType;
  static constexpr DataType AGGREGATE_DATA_TYPE = DataType::Long;
};

// COUNT(DISTINCT) on all types
template <typename ColumnType>
struct AggregateTraits<ColumnType, AggregateFunction::CountDistinct> {
  typedef int64_t AggregateType;
  static constexpr DataType AGGREGATE_DATA_TYPE = DataType::Long;
};

// MIN/MAX/ANY on all types
template <typename ColumnType, AggregateFunction aggregate_function>
struct AggregateTraits<ColumnType, aggregate_function,
                       typename std::enable_if_t<aggregate_function == AggregateFunction::Min ||
                                                     aggregate_function == AggregateFunction::Max ||
                                                     aggregate_function == AggregateFunction::Any,
                                                 void>> {
  typedef ColumnType AggregateType;
  static constexpr DataType AGGREGATE_DATA_TYPE = data_type_from_type<ColumnType>();
};

// AVG on arithmetic types
template <typename ColumnType, AggregateFunction aggregate_function>
struct AggregateTraits<
    ColumnType, aggregate_function,
    typename std::enable_if_t<aggregate_function == AggregateFunction::Avg && std::is_arithmetic_v<ColumnType>, void>> {
  typedef double AggregateType;
  static constexpr DataType AGGREGATE_DATA_TYPE = DataType::Double;
};

// SUM on integers
template <typename ColumnType, AggregateFunction aggregate_function>
struct AggregateTraits<
    ColumnType, aggregate_function,
    typename std::enable_if_t<aggregate_function == AggregateFunction::Sum && std::is_integral_v<ColumnType>, void>> {
  typedef int64_t AggregateType;
  static constexpr DataType AGGREGATE_DATA_TYPE = DataType::Long;
};

// SUM on floating point numbers
template <typename ColumnType, AggregateFunction aggregate_function>
struct AggregateTraits<
    ColumnType, aggregate_function,
    typename std::enable_if_t<aggregate_function == AggregateFunction::Sum && std::is_floating_point_v<ColumnType>,
                              void>> {
  typedef double AggregateType;
  static constexpr DataType AGGREGATE_DATA_TYPE = DataType::Double;
};

// STDDEV_SAMP on arithmetic types
template <typename ColumnType, AggregateFunction aggregate_function>
struct AggregateTraits<
    ColumnType, aggregate_function,
    typename std::enable_if_t<
        aggregate_function == AggregateFunction::StandardDeviationSample && std::is_arithmetic_v<ColumnType>, void>> {
  typedef double AggregateType;
  static constexpr DataType AGGREGATE_DATA_TYPE = DataType::Double;
};

// invalid: AVG, SUM or STDDEV_SAMP on non-arithmetic types
template <typename ColumnType, AggregateFunction aggregate_function>
struct AggregateTraits<ColumnType, aggregate_function,
                       typename std::enable_if_t<!std::is_arithmetic_v<ColumnType> &&
                                                     (aggregate_function == AggregateFunction::Avg ||
                                                      aggregate_function == AggregateFunction::Sum ||
                                                      aggregate_function == AggregateFunction::StandardDeviationSample),
                                                 void>> {
  typedef ColumnType AggregateType;
  static constexpr DataType AGGREGATE_DATA_TYPE = DataType::Null;
};

}  // namespace opossum
