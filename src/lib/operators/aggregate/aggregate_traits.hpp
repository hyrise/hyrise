#pragma once

#include "resolve_type.hpp"

namespace opossum {

/*
The following structs describe the different aggregate traits.
Given a ColumnType and AggregateFunction, certain traits like the aggregate type
can be deduced.
*/
template <typename ColumnType, AggregateFunction function, class Enable = void>
struct AggregateTraits {};

// COUNT on all types
template <typename ColumnType>
struct AggregateTraits<ColumnType, AggregateFunction::Count> {
  typedef ColumnType column_type;
  typedef int64_t aggregate_type;
  static constexpr DataType aggregate_data_type = DataType::Long;
};

// COUNT(DISTINCT) on all types
template <typename ColumnType>
struct AggregateTraits<ColumnType, AggregateFunction::CountDistinct> {
  typedef ColumnType column_type;
  typedef int64_t aggregate_type;
  static constexpr DataType aggregate_data_type = DataType::Long;
};

// MIN/MAX on all types
template <typename ColumnType, AggregateFunction function>
struct AggregateTraits<
    ColumnType, function,
    typename std::enable_if_t<function == AggregateFunction::Min || function == AggregateFunction::Max, void>> {
  typedef ColumnType column_type;
  typedef ColumnType aggregate_type;
  static constexpr DataType aggregate_data_type = data_type_from_type<ColumnType>();
};

// AVG on arithmetic types
template <typename ColumnType, AggregateFunction function>
struct AggregateTraits<
    ColumnType, function,
    typename std::enable_if_t<function == AggregateFunction::Avg && std::is_arithmetic<ColumnType>::value, void>> {
  typedef ColumnType column_type;
  typedef double aggregate_type;
  static constexpr DataType aggregate_data_type = DataType::Double;
};

// SUM on integers
template <typename ColumnType, AggregateFunction function>
struct AggregateTraits<
    ColumnType, function,
    typename std::enable_if_t<function == AggregateFunction::Sum && std::is_integral<ColumnType>::value, void>> {
  typedef ColumnType column_type;
  typedef int64_t aggregate_type;
  static constexpr DataType aggregate_data_type = DataType::Long;
};

// SUM on floating point numbers
template <typename ColumnType, AggregateFunction function>
struct AggregateTraits<
    ColumnType, function,
    typename std::enable_if_t<function == AggregateFunction::Sum && std::is_floating_point<ColumnType>::value, void>> {
  typedef ColumnType column_type;
  typedef double aggregate_type;
  static constexpr DataType aggregate_data_type = DataType::Double;
};

// invalid: AVG on non-arithmetic types
template <typename ColumnType, AggregateFunction function>
struct AggregateTraits<
    ColumnType, function,
    typename std::enable_if_t<!std::is_arithmetic<ColumnType>::value &&
                                  (function == AggregateFunction::Avg || function == AggregateFunction::Sum),
                              void>> {
  typedef ColumnType column_type;
  typedef ColumnType aggregate_type;
  static constexpr DataType aggregate_data_type = DataType::Null;
};

}  // namespace opossum
