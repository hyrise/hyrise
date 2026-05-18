#pragma once

#include "resolve_type.hpp"

namespace hyrise {

/**
 * The following structs describe the different window function traits. Given a ColumnType and WindowFunction, certain
 * traits, such as the resulting data type, can be deduced.
*/
template <typename ColumnType, WindowFunction window_function, class Enable = void>
struct WindowFunctionTraits {};

// COUNT on all types
template <typename ColumnType>
struct WindowFunctionTraits<ColumnType, WindowFunction::Count> {
  using ReturnType = int64_t;
  static constexpr DataType RESULT_TYPE = DataType::Long;
};

// COUNT(DISTINCT) on all types
template <typename ColumnType>
struct WindowFunctionTraits<ColumnType, WindowFunction::CountDistinct> {
  using ReturnType = int64_t;
  static constexpr DataType RESULT_TYPE = DataType::Long;
};

// MIN/MAX/ANY on all types
template <typename ColumnType, WindowFunction window_function>
  requires(window_function == WindowFunction::Min) || (window_function == WindowFunction::Max) ||
          (window_function == WindowFunction::Any)
struct WindowFunctionTraits<ColumnType, window_function> {
  using ReturnType = ColumnType;
  static constexpr DataType RESULT_TYPE = data_type_from_type<ColumnType>();
};

// AVG on arithmetic types
template <typename ColumnType, WindowFunction window_function>
  requires(window_function == WindowFunction::Avg) && (std::is_arithmetic_v<ColumnType>)
struct WindowFunctionTraits<ColumnType, window_function> {
  using ReturnType = double;
  static constexpr DataType RESULT_TYPE = DataType::Double;
};

// SUM on integers
template <typename ColumnType, WindowFunction window_function>
  requires(window_function == WindowFunction::Sum && std::is_integral_v<ColumnType>)
struct WindowFunctionTraits<ColumnType, window_function> {
  using ReturnType = int64_t;
  static constexpr DataType RESULT_TYPE = DataType::Long;
};

// SUM on floating point numbers
template <typename ColumnType, WindowFunction window_function>
  requires(window_function == WindowFunction::Sum && std::is_floating_point_v<ColumnType>)
struct WindowFunctionTraits<ColumnType, window_function> {
  using ReturnType = double;
  static constexpr DataType RESULT_TYPE = DataType::Double;
};

// STDDEV_SAMP on arithmetic types
template <typename ColumnType, WindowFunction window_function>
  requires(window_function == WindowFunction::StandardDeviationSample && std::is_arithmetic_v<ColumnType>)
struct WindowFunctionTraits<ColumnType, window_function> {
  using ReturnType = double;
  static constexpr DataType RESULT_TYPE = DataType::Double;
};

// invalid: AVG, SUM or STDDEV_SAMP on non-arithmetic types
template <typename ColumnType, WindowFunction window_function>
  requires(!std::is_arithmetic_v<ColumnType> &&
           (window_function == WindowFunction::Avg || window_function == WindowFunction::Sum ||
            window_function == WindowFunction::StandardDeviationSample))
struct WindowFunctionTraits<ColumnType, window_function> {
  using ReturnType = ColumnType;
  static constexpr DataType RESULT_TYPE = DataType::Null;
};

// CUME_DIST on all types
template <typename ColumnType>
struct WindowFunctionTraits<ColumnType, WindowFunction::CumeDist> {
  using ReturnType = double;
  static constexpr DataType RESULT_TYPE = DataType::Double;
};

// DENSE_RANK on all types
template <typename ColumnType>
struct WindowFunctionTraits<ColumnType, WindowFunction::DenseRank> {
  using ReturnType = int64_t;
  static constexpr DataType RESULT_TYPE = DataType::Long;
};

// PERCENT_RANK on all types
template <typename ColumnType>
struct WindowFunctionTraits<ColumnType, WindowFunction::PercentRank> {
  using ReturnType = double;
  static constexpr DataType RESULT_TYPE = DataType::Double;
};

// RANK on all types
template <typename ColumnType>
struct WindowFunctionTraits<ColumnType, WindowFunction::Rank> {
  using ReturnType = int64_t;
  static constexpr DataType RESULT_TYPE = DataType::Long;
};

// ROW_NUMBER on all types
template <typename ColumnType>
struct WindowFunctionTraits<ColumnType, WindowFunction::RowNumber> {
  using ReturnType = int64_t;
  static constexpr DataType RESULT_TYPE = DataType::Long;
};

}  // namespace hyrise
