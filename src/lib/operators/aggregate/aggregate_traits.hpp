#pragma once

#include "resolve_type.hpp"

namespace hyrise {

/**
 * The following structs describe the different window function traits. Given a ColumnType and WindowFunction, certain
 * traits, such as the resulting data type, can be deduced.
*/
template <typename ColumnType, WindowFunction aggregate_function, class Enable = void>
struct AggregateTraits {};

// COUNT on all types
template <typename ColumnType>
struct AggregateTraits<ColumnType, WindowFunction::Count> {
  typedef int64_t AggregateType;
  static constexpr DataType RESULT_TYPE = DataType::Long;
};

// COUNT(DISTINCT) on all types
template <typename ColumnType>
struct AggregateTraits<ColumnType, WindowFunction::CountDistinct> {
  typedef int64_t AggregateType;
  static constexpr DataType RESULT_TYPE = DataType::Long;
};

// MIN/MAX/ANY on all types
template <typename ColumnType, WindowFunction aggregate_function>
struct AggregateTraits<
    ColumnType, aggregate_function,
    typename std::enable_if_t<aggregate_function == WindowFunction::Min || aggregate_function == WindowFunction::Max ||
                                  aggregate_function == WindowFunction::Any,
                              void>> {
  typedef ColumnType AggregateType;
  static constexpr DataType RESULT_TYPE = data_type_from_type<ColumnType>();
};

// AVG on arithmetic types
template <typename ColumnType, WindowFunction aggregate_function>
struct AggregateTraits<
    ColumnType, aggregate_function,
    typename std::enable_if_t<aggregate_function == WindowFunction::Avg && std::is_arithmetic_v<ColumnType>, void>> {
  typedef double AggregateType;
  static constexpr DataType RESULT_TYPE = DataType::Double;
};

// SUM on integers
template <typename ColumnType, WindowFunction aggregate_function>
struct AggregateTraits<
    ColumnType, aggregate_function,
    typename std::enable_if_t<aggregate_function == WindowFunction::Sum && std::is_integral_v<ColumnType>, void>> {
  typedef int64_t AggregateType;
  static constexpr DataType RESULT_TYPE = DataType::Long;
};

// SUM on floating point numbers
template <typename ColumnType, WindowFunction aggregate_function>
struct AggregateTraits<ColumnType, aggregate_function,
                       typename std::enable_if_t<
                           aggregate_function == WindowFunction::Sum && std::is_floating_point_v<ColumnType>, void>> {
  typedef double AggregateType;
  static constexpr DataType RESULT_TYPE = DataType::Double;
};

// STDDEV_SAMP on arithmetic types
template <typename ColumnType, WindowFunction aggregate_function>
struct AggregateTraits<
    ColumnType, aggregate_function,
    typename std::enable_if_t<
        aggregate_function == WindowFunction::StandardDeviationSample && std::is_arithmetic_v<ColumnType>, void>> {
  typedef double AggregateType;
  static constexpr DataType RESULT_TYPE = DataType::Double;
};

// invalid: AVG, SUM or STDDEV_SAMP on non-arithmetic types
template <typename ColumnType, WindowFunction aggregate_function>
struct AggregateTraits<ColumnType, aggregate_function,
                       typename std::enable_if_t<!std::is_arithmetic_v<ColumnType> &&
                                                     (aggregate_function == WindowFunction::Avg ||
                                                      aggregate_function == WindowFunction::Sum ||
                                                      aggregate_function == WindowFunction::StandardDeviationSample),
                                                 void>> {
  typedef ColumnType AggregateType;
  static constexpr DataType RESULT_TYPE = DataType::Null;
};

// CUME_DIST on all types
template <typename ColumnType>
struct AggregateTraits<ColumnType, WindowFunction::CumeDist> {
  typedef int64_t AggregateType;
  static constexpr DataType RESULT_TYPE = DataType::Double;
};

// DENSE_RANK on all types
template <typename ColumnType>
struct AggregateTraits<ColumnType, WindowFunction::DenseRank> {
  typedef int64_t AggregateType;
  static constexpr DataType RESULT_TYPE = DataType::Long;
};

// PERCENT_RANK on all types
template <typename ColumnType>
struct AggregateTraits<ColumnType, WindowFunction::PercentRank> {
  typedef int64_t AggregateType;
  static constexpr DataType RESULT_TYPE = DataType::Double;
};

// RANK on all types
template <typename ColumnType>
struct AggregateTraits<ColumnType, WindowFunction::Rank> {
  typedef int64_t AggregateType;
  static constexpr DataType RESULT_TYPE = DataType::Long;
};

// ROW_NUMBER on all types
template <typename ColumnType>
struct AggregateTraits<ColumnType, WindowFunction::RowNumber> {
  typedef int64_t AggregateType;
  static constexpr DataType RESULT_TYPE = DataType::Long;
};

}  // namespace hyrise
