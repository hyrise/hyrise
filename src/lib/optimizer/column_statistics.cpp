#include "column_statistics.hpp"

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "all_parameter_variant.hpp"
#include "common.hpp"
#include "operators/aggregate.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"
#include "table_statistics.hpp"
#include "type_cast.hpp"
#include "types.hpp"

namespace opossum {

template <typename ColumnType>
ColumnStatistics<ColumnType>::ColumnStatistics(const ColumnID column_id, const std::weak_ptr<Table> table)
    : _column_id(column_id), _table(table) {}

template <typename ColumnType>
ColumnStatistics<ColumnType>::ColumnStatistics(const ColumnID column_id, float distinct_count, ColumnType min,
                                               ColumnType max)
    : _column_id(column_id), _table(std::weak_ptr<Table>()), _distinct_count(distinct_count), _min(min), _max(max) {}

template <typename ColumnType>
float ColumnStatistics<ColumnType>::distinct_count() const {
  if (_distinct_count) {
    return *_distinct_count;
  }

  // Calculation of distinct_count is delegated to aggregate operator.
  auto table = _table.lock();
  DebugAssert(table != nullptr, "Corresponding table of column statistics is deleted.");
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  auto aggregate = std::make_shared<Aggregate>(table_wrapper, std::vector<AggregateDefinition>{},
                                               std::vector<std::string>{table->column_name(_column_id)});
  aggregate->execute();
  auto aggregate_table = aggregate->get_output();
  _distinct_count = aggregate_table->row_count();
  return *_distinct_count;
}

template <typename ColumnType>
ColumnType ColumnStatistics<ColumnType>::min() const {
  if (!_min) {
    initialize_min_max();
  }
  return *_min;
}

template <typename ColumnType>
ColumnType ColumnStatistics<ColumnType>::max() const {
  if (!_max) {
    initialize_min_max();
  }
  return *_max;
}

template <typename ColumnType>
void ColumnStatistics<ColumnType>::initialize_min_max() const {
  // Calculation is delegated to aggregate operator.
  auto table = _table.lock();
  DebugAssert(table != nullptr, "Corresponding table of column statistics is deleted.");
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  const std::string &column_name = table->column_name(_column_id);
  auto aggregate_args = std::vector<AggregateDefinition>{{column_name, Min}, {column_name, Max}};
  auto aggregate = std::make_shared<Aggregate>(table_wrapper, aggregate_args, std::vector<std::string>{});
  aggregate->execute();
  auto aggregate_table = aggregate->get_output();
  _min = aggregate_table->template get_value<ColumnType>(ColumnID{0}, 0);
  _max = aggregate_table->template get_value<ColumnType>(ColumnID{1}, 0);
}

template <typename ColumnType>
ColumnSelectivityResult ColumnStatistics<ColumnType>::estimate_selectivity_for_range_and_create_column_statistics(
    ColumnType minimum, ColumnType maximum) {
  minimum = std::max(minimum, min());
  maximum = std::min(maximum, max());
  if (minimum == min() && maximum == max()) {
    return {1.f, nullptr};
  } else if (maximum < minimum) {
    return {0.f, nullptr};
  }
  float selectivity = estimate_selectivity_for_range(minimum, maximum);
  auto column_statistics =
      std::make_shared<ColumnStatistics>(_column_id, selectivity * distinct_count(), minimum, maximum);
  return {selectivity, column_statistics};
}

/**
 * Specialization for strings as they cannot be used in subtractions.
 */
template <>
ColumnSelectivityResult ColumnStatistics<std::string>::estimate_selectivity_for_range_and_create_column_statistics(
    std::string minimum, std::string maximum) {
  minimum = std::max(minimum, min());
  maximum = std::min(maximum, max());
  if (maximum < minimum) {
    return {0.f, nullptr};
  }
  return {1.f, nullptr};
}

template <typename ColumnType>
float ColumnStatistics<ColumnType>::estimate_selectivity_for_range(ColumnType minimum, ColumnType maximum) {
  // distinction between integers and decimals
  // for integers the number of possible integers is used within the inclusive ranges
  // for decimals the size of the range is used
  if (std::is_integral<ColumnType>::value) {
    return static_cast<float>(maximum - minimum + 1) / static_cast<float>(max() - min() + 1);
  } else {
    return static_cast<float>(maximum - minimum) / static_cast<float>(max() - min());
  }
}

/**
 * Specialization for strings as they cannot be used in subtractions.
 */
template <>
float ColumnStatistics<std::string>::estimate_selectivity_for_range(std::string minimum, std::string maximum) {
  return 1.f;
}

template <typename ColumnType>
ColumnSelectivityResult ColumnStatistics<ColumnType>::estimate_selectivity_for_equals(ColumnType value) {
  if (value < min() || value > max()) {
    return {0.f, nullptr};
  }
  auto column_statistics = std::make_shared<ColumnStatistics>(_column_id, 1, value, value);
  return {1.f / distinct_count(), column_statistics};
}

template <typename ColumnType>
ColumnSelectivityResult ColumnStatistics<ColumnType>::selectivity_for_unequals(ColumnType value) {
  if (value < min() || value > max()) {
    return {1.f, nullptr};
  }
  auto column_statistics = std::make_shared<ColumnStatistics>(_column_id, distinct_count() - 1, min(), max());
  return {1 - 1.f / distinct_count(), column_statistics};
}

template <typename ColumnType>
ColumnSelectivityResult ColumnStatistics<ColumnType>::estimate_selectivity_for_predicate(
    const ScanType scan_type, const AllTypeVariant &value, const optional<AllTypeVariant> &value2) {
  auto casted_value = type_cast<ColumnType>(value);

  switch (scan_type) {
    case ScanType::OpEquals: {
      return estimate_selectivity_for_equals(casted_value);
    }
    case ScanType::OpNotEquals: {
      return selectivity_for_unequals(casted_value);
    }
    case ScanType::OpLessThan: {
      // distinction between integers and decimals
      // for integers "< value" means that the new max is value <= value - 1
      // for decimals "< value" means that the new max is value <= value - ε
      if (std::is_integral<ColumnType>::value) {
        return estimate_selectivity_for_range_and_create_column_statistics(min(), casted_value - 1);
      }
// intentionally no break
// if ColumnType is a floating point number,
// OpLessThanEquals behaviour is expected instead of OpLessThan
#if __has_cpp_attribute(fallthrough)
      [[fallthrough]];
#endif
    }
    case ScanType::OpLessThanEquals: {
      return estimate_selectivity_for_range_and_create_column_statistics(min(), casted_value);
    }
    case ScanType::OpGreaterThan: {
      // distinction between integers and decimals
      // for integers "> value" means that the new min value is >= value + 1
      // for decimals "> value" means that the new min value is >= value + ε
      if (std::is_integral<ColumnType>::value) {
        return estimate_selectivity_for_range_and_create_column_statistics(casted_value + 1, max());
      }
// intentionally no break
// if ColumnType is a floating point number,
// OpGreaterThanEquals behaviour is expected instead of OpGreaterThan
#if __has_cpp_attribute(fallthrough)
      [[fallthrough]];
#endif
    }
    case ScanType::OpGreaterThanEquals: {
      return estimate_selectivity_for_range_and_create_column_statistics(casted_value, max());
    }
    case ScanType::OpBetween: {
      DebugAssert(static_cast<bool>(value2), "Operator BETWEEN should get two parameters, second is missing!");
      auto casted_value2 = type_cast<ColumnType>(*value2);
      return estimate_selectivity_for_range_and_create_column_statistics(casted_value, casted_value2);
    }
    default: { return {1.f, nullptr}; }
  }
}

/**
 * Specialization for strings as they cannot be used in subtractions.
 */
template <>
ColumnSelectivityResult ColumnStatistics<std::string>::estimate_selectivity_for_predicate(
    const ScanType scan_type, const AllTypeVariant &value, const optional<AllTypeVariant> &value2) {
  auto casted_value = type_cast<std::string>(value);
  switch (scan_type) {
    case ScanType::OpEquals: {
      return estimate_selectivity_for_equals(casted_value);
    }
    case ScanType::OpNotEquals: {
      return selectivity_for_unequals(casted_value);
    }
    // TODO(anybody) implement other table-scan operators for string.
    default: { return {1.f, nullptr}; }
  }
}

template <typename ColumnType>
ColumnSelectivityResult ColumnStatistics<ColumnType>::estimate_selectivity_for_predicate(
    const ScanType scan_type, const ValuePlaceholder &value, const optional<AllTypeVariant> &value2) {
  switch (scan_type) {
    case ScanType::OpEquals: {
      auto column_statistics = std::make_shared<ColumnStatistics>(_column_id, 1, min(), max());
      return {1.f / distinct_count(), column_statistics};
    }
    case ScanType::OpNotEquals: {
      auto column_statistics = std::make_shared<ColumnStatistics>(_column_id, distinct_count() - 1, min(), max());
      return {(distinct_count() - 1.f) / distinct_count(), column_statistics};
    }
    case ScanType::OpLessThan:
    case ScanType::OpLessThanEquals:
    case ScanType::OpGreaterThan:
    case ScanType::OpGreaterThanEquals: {
      auto column_statistics = std::make_shared<ColumnStatistics>(
          _column_id, distinct_count() * DEFAULT_OPEN_ENDED_SELECTIVITY, min(), max());
      return {DEFAULT_OPEN_ENDED_SELECTIVITY, column_statistics};
    }
    case ScanType::OpBetween: {
      // since the value2 is known,
      // first, statistics for the operation <= value are calulated
      // then, the open ended selectivity is applied on the result
      DebugAssert(static_cast<bool>(value2), "Operator BETWEEN should get two parameters, second is missing!");
      auto casted_value2 = type_cast<ColumnType>(*value2);
      ColumnSelectivityResult output =
          estimate_selectivity_for_range_and_create_column_statistics(min(), casted_value2);
      // return, if value2 < min
      if (output.selectivity == 0.f) {
        return output;
      }
      // create statistics, if value2 >= max
      if (output.column_statistics == nullptr) {
        output.column_statistics = std::make_shared<ColumnStatistics>(_column_id, distinct_count(), min(), max());
      }
      // apply default selectivity for open ended
      output.selectivity *= DEFAULT_OPEN_ENDED_SELECTIVITY;
      // column statistis have just been created, therefore, cast to the column type cannot fail
      auto column_statistics = std::dynamic_pointer_cast<ColumnStatistics<ColumnType>>(output.column_statistics);
      *(column_statistics->_distinct_count) *= DEFAULT_OPEN_ENDED_SELECTIVITY;
      return output;
    }
    default: { return {1.f, nullptr}; }
  }
}

template <typename ColumnType>
TwoColumnSelectivityResult ColumnStatistics<ColumnType>::estimate_selectivity_for_predicate(
    const ScanType scan_type, const std::shared_ptr<BaseColumnStatistics> base_value_column_statistics,
    const optional<AllTypeVariant> &value2) {
  auto right_stats = std::dynamic_pointer_cast<ColumnStatistics<ColumnType>>(base_value_column_statistics);
  DebugAssert(right_stats != nullptr, "Cannot compare columns of different type");

  auto common_min = std::max(min(), right_stats->min());
  auto common_max = std::min(max(), right_stats->max());

  // calculate percentage of values before, in and above the common value range
  float overlapping_ratio_left = estimate_selectivity_for_range(common_min, common_max);
  float overlapping_ratio_right = right_stats->estimate_selectivity_for_range(common_min, common_max);

  float below_left, above_left, below_right, above_right;
  if (std::is_integral<ColumnType>::value) {
    below_left = (min() < common_min) ? estimate_selectivity_for_range(min(), common_min - 1) : 0;
    above_left = (common_max < max()) ? estimate_selectivity_for_range(common_max + 1, max()) : 0;
    bool below_min = right_stats->min() < common_min;
    below_right = below_min ? right_stats->estimate_selectivity_for_range(right_stats->min(), common_min - 1) : 0;
    bool above_max = common_max < right_stats->max();
    above_right = above_max ? right_stats->estimate_selectivity_for_range(common_max + 1, right_stats->max()) : 0;
  } else {
    below_left = estimate_selectivity_for_range(min(), common_min);
    above_left = estimate_selectivity_for_range(common_max, max());
    below_right = right_stats->estimate_selectivity_for_range(right_stats->min(), common_min);
    above_right = right_stats->estimate_selectivity_for_range(common_max, right_stats->max());
  }

  // calculate percentage of distinct values in common value range
  auto overlapping_distinct_count_this = overlapping_ratio_left * distinct_count();
  auto overlapping_distinct_count_value = overlapping_ratio_right * right_stats->distinct_count();

  float equal_values_ratio;
  // calculate percentage of rows with equal values
  if (overlapping_distinct_count_this < overlapping_distinct_count_value) {
    equal_values_ratio = overlapping_ratio_left / right_stats->distinct_count();
  } else {
    equal_values_ratio = overlapping_ratio_right / distinct_count();
  }

  switch (scan_type) {
    case ScanType::OpEquals: {
      auto overlapping_distinct_count = std::min(overlapping_distinct_count_this, overlapping_distinct_count_value);

      auto column_statistics_left =
          std::make_shared<ColumnStatistics>(_column_id, overlapping_distinct_count, common_min, common_max);
      auto column_statistics_right = std::make_shared<ColumnStatistics>(
          right_stats->_column_id, overlapping_distinct_count, common_min, common_max);
      return {equal_values_ratio, column_statistics_left, column_statistics_right};
    }
    case ScanType::OpNotEquals: {
      auto column_statistics_left = std::make_shared<ColumnStatistics>(_column_id, distinct_count(), min(), max());
      auto column_statistics_right = std::make_shared<ColumnStatistics>(
          right_stats->_column_id, right_stats->distinct_count(), right_stats->min(), right_stats->max());
      return {1.f - equal_values_ratio, column_statistics_left, column_statistics_right};
    }
    case ScanType::OpLessThan:
    case ScanType::OpLessThanEquals: {
      // selectivity calculated by adding up percentages that values are below, in or above overlapping range
      float selectivity = 0.f;
      // percentage of values on left hand sight which are smaller than overlapping range
      selectivity += below_left;
      // selectivity of not equal numbers n1, n2 in overlapping range where n1 < n2 is 0.5
      selectivity += (overlapping_ratio_left * overlapping_ratio_right - equal_values_ratio) * 0.5f;
      if (scan_type == ScanType::OpLessThanEquals) {
        selectivity += equal_values_ratio;
      }
      // percentage of values on right hand side which are greater than overlapping range
      selectivity += above_right;
      // remove percentage of rows, where one value is below and one value above the common value range
      selectivity -= below_left * above_right;

      auto column_statistics_left =
          std::make_shared<ColumnStatistics>(_column_id, overlapping_distinct_count_this, min(), right_stats->max());
      auto column_statistics_right = std::make_shared<ColumnStatistics>(
          right_stats->_column_id, overlapping_distinct_count_value, min(), right_stats->max());
      return {selectivity, column_statistics_left, column_statistics_right};
    }
    case ScanType::OpGreaterThan:
    case ScanType::OpGreaterThanEquals: {
      // selectivity calculated by adding up percentages that values are below, in or above overlapping range
      float selectivity = 0.f;
      // percentage of values on right hand side which are smaller than overlapping range
      selectivity += below_right;
      // selectivity of not equal numbers n1, n2 in overlapping range where n1 > n2 is 0.5
      selectivity += (overlapping_ratio_left * overlapping_ratio_right - equal_values_ratio) * 0.5f;
      if (scan_type == ScanType::OpGreaterThanEquals) {
        selectivity += equal_values_ratio;
      }
      // percentage of values on left hand side which are greater than overlapping range
      selectivity += above_left;
      // remove percentage of rows, where one value is below and one value above the common value range
      selectivity -= below_right * above_left;

      auto column_statistics_left =
          std::make_shared<ColumnStatistics>(_column_id, overlapping_distinct_count_this, right_stats->min(), max());
      auto column_statistics_right = std::make_shared<ColumnStatistics>(
          right_stats->_column_id, overlapping_distinct_count_value, right_stats->min(), max());
      return {selectivity, column_statistics_left, column_statistics_right};
    }
    // case ScanType::OpBetween not supported for ColumnName as TableScan does not support this
    default: { return {1.f, nullptr, nullptr}; }
  }
}

/**
 * Specialization for strings as they cannot be used in subtractions.
 */
template <>
TwoColumnSelectivityResult ColumnStatistics<std::string>::estimate_selectivity_for_predicate(
    const ScanType scan_type, const std::shared_ptr<BaseColumnStatistics> base_value_column_statistics,
    const optional<AllTypeVariant> &value2) {
  // TODO(anybody) implement special case for strings
  return {1.f, nullptr, nullptr};
}

template <typename ColumnType>
std::ostream &ColumnStatistics<ColumnType>::print_to_stream(std::ostream &os) const {
  os << "Col Stats id: " << _column_id << std::endl;
  os << "  dist. " << _distinct_count << std::endl;
  os << "  min   " << _min << std::endl;
  os << "  max   " << _max;
  return os;
}

template class ColumnStatistics<int32_t>;
template class ColumnStatistics<int64_t>;
template class ColumnStatistics<float>;
template class ColumnStatistics<double>;
template class ColumnStatistics<std::string>;

}  // namespace opossum
