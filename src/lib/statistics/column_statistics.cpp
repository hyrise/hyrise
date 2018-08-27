#include "column_statistics.hpp"

#include <sstream>

#include "resolve_type.hpp"
#include "table_statistics.hpp"
#include "type_cast.hpp"

namespace opossum {

template <typename ColumnDataType>
ColumnStatistics<ColumnDataType> ColumnStatistics<ColumnDataType>::dummy() {
  if constexpr (std::is_same_v<ColumnDataType, std::string>) {
    return ColumnStatistics{1.0f, 1.0f, {}, {}};
  } else {
    return ColumnStatistics{1.0f, 1.0f, {0}, {0}};
  }
}

template <typename ColumnDataType>
ColumnStatistics<ColumnDataType>::ColumnStatistics(const float null_value_ratio, const float distinct_count,
                                                   const ColumnDataType min, const ColumnDataType max)
    : BaseColumnStatistics(data_type_from_type<ColumnDataType>(), null_value_ratio, distinct_count),
      _min(min),
      _max(max) {
  Assert(null_value_ratio >= 0.0f && null_value_ratio <= 1.0f, "NullValueRatio out of range");
}

template <typename ColumnDataType>
ColumnDataType ColumnStatistics<ColumnDataType>::min() const {
  return _min;
}

template <typename ColumnDataType>
ColumnDataType ColumnStatistics<ColumnDataType>::max() const {
  return _max;
}

template <typename ColumnDataType>
std::shared_ptr<BaseColumnStatistics> ColumnStatistics<ColumnDataType>::clone() const {
  return std::make_shared<ColumnStatistics<ColumnDataType>>(null_value_ratio(), distinct_count(), _min, _max);
}

template <typename ColumnDataType>
FilterByValueEstimate ColumnStatistics<ColumnDataType>::estimate_predicate_with_value(
    const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& value2) const {
  const auto value = type_cast<ColumnDataType>(variant_value);

  switch (predicate_condition) {
    case PredicateCondition::Equals:
      return estimate_equals_with_value(value);
    case PredicateCondition::NotEquals:
      return estimate_not_equals_with_value(value);

    case PredicateCondition::LessThan: {
      // distinction between integers and floats
      // for integers "< value" means that the new max is value <= value - 1
      // for floats "< value" means that the new max is value <= value - ε
      if (std::is_integral_v<ColumnDataType>) {
        return estimate_range(_min, value - 1);
      }
      // intentionally no break
      // if ColumnType is a floating point number, OpLessThanEquals behaviour is expected instead of OpLessThan
      [[fallthrough]];
    }
    case PredicateCondition::LessThanEquals:
      return estimate_range(_min, value);

    case PredicateCondition::GreaterThan: {
      // distinction between integers and floats
      // for integers "> value" means that the new min value is >= value + 1
      // for floats "> value" means that the new min value is >= value + ε
      if (std::is_integral_v<ColumnDataType>) {
        return estimate_range(value + 1, _max);
      }
      // intentionally no break
      // if ColumnType is a floating point number,
      // OpGreaterThanEquals behaviour is expected instead of OpGreaterThan
      [[fallthrough]];
    }
    case PredicateCondition::GreaterThanEquals:
      return estimate_range(value, _max);

    case PredicateCondition::Between: {
      DebugAssert(static_cast<bool>(value2), "Operator BETWEEN should get two parameters, second is missing!");
      auto casted_value2 = type_cast<ColumnDataType>(*value2);
      return estimate_range(value, casted_value2);
    }

    default:
      Fail("Estimation not implemented for requested PredicateCondition");
  }
}

/**
 * Specialization for strings as they cannot be used in subtractions.
 */
template <>
FilterByValueEstimate ColumnStatistics<std::string>::estimate_predicate_with_value(
    const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& value2) const {
  // if column has no distinct values, it can only have null values which cannot be selected with this predicate
  if (distinct_count() == 0) {
    return {0.f, without_null_values()};
  }

  auto casted_value = type_cast<std::string>(variant_value);
  switch (predicate_condition) {
    case PredicateCondition::Equals: {
      return estimate_equals_with_value(casted_value);
    }
    case PredicateCondition::NotEquals: {
      return estimate_not_equals_with_value(casted_value);
    }
    // TODO(anybody) implement other table-scan operators for string.
    default: { return {non_null_value_ratio(), without_null_values()}; }
  }
}

template <typename ColumnDataType>
FilterByValueEstimate ColumnStatistics<ColumnDataType>::estimate_predicate_with_value_placeholder(
    const PredicateCondition predicate_condition, const std::optional<AllTypeVariant>& value2) const {
  switch (predicate_condition) {
    // Simply assume the value will be in (_min, _max) and pick _min as the representative
    case PredicateCondition::Equals:
      return estimate_equals_with_value(_min);
    case PredicateCondition::NotEquals:
      return estimate_not_equals_with_value(_min);

    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals:
    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals: {
      auto column_statistics = std::make_shared<ColumnStatistics<ColumnDataType>>(
          0.0f, distinct_count() * TableStatistics::DEFAULT_OPEN_ENDED_SELECTIVITY, _min, _max);
      return {non_null_value_ratio() * TableStatistics::DEFAULT_OPEN_ENDED_SELECTIVITY, column_statistics};
    }
    case PredicateCondition::Between: {
      // since the value2 is known,
      // first, statistics for the operation <= value are calculated
      // then, the open ended selectivity is applied on the result
      DebugAssert(static_cast<bool>(value2), "Operator BETWEEN should get two parameters, second is missing!");
      auto casted_value2 = type_cast<ColumnDataType>(*value2);
      auto output = estimate_range(_min, casted_value2);
      // return, if value2 < min
      if (output.selectivity == 0.f) {
        return output;
      }
      // create statistics, if value2 >= max
      if (output.column_statistics.get() == this) {
        output.column_statistics = std::make_shared<ColumnStatistics>(0.0f, distinct_count(), _min, _max);
      }
      // apply default selectivity for open ended
      output.selectivity *= TableStatistics::DEFAULT_OPEN_ENDED_SELECTIVITY;
      // column statistics have just been created, therefore, cast to the column type cannot fail
      auto column_statistics = std::dynamic_pointer_cast<ColumnStatistics<ColumnDataType>>(output.column_statistics);
      column_statistics->_distinct_count *= TableStatistics::DEFAULT_OPEN_ENDED_SELECTIVITY;
      return output;
    }
    default: { return {non_null_value_ratio(), without_null_values()}; }
  }
}

template <typename ColumnDataType>
FilterByColumnComparisonEstimate ColumnStatistics<ColumnDataType>::estimate_predicate_with_column(
    const PredicateCondition predicate_condition, const BaseColumnStatistics& base_right_column_statistics) const {
  /**
   * Calculate expected selectivity by looking at what ratio of values of both columns are in the overlapping value
   * range of both columns. 
   * 
   * For the different predicate conditions the appropriate ratios of values below, within and above the overlapping 
   * range from both columns are taken to compute the selectivity.
   *
   * Example estimation:
   *
   * |  Column name     |  col_left  |  col_right  |
   * |  Min value       |  1         |  11         |
   * |  Max value       |  20        |  40         |
   * |  Distinct count  |  20        |  15         |
   *
   * Overlapping value range: 11 to 20  -->  overlapping_range_min = 11,  overlapping_range_max = 20
   * left_overlapping_ratio = (20 - 11 + 1) / (20 - 1 + 1) = 1 / 2
   * right_overlapping_ratio = (20 - 11 + 1) / (40 - 11 + 1) = 1 / 3
   *
   * left_below_overlapping_ratio = (10 - 1 + 1) / (20 - 1 + 1) = 1 / 2
   * left_above_overlapping_ratio = 0 as col_left max value within overlapping range
   * right_below_overlapping_ratio = (40 - 21 + 1) / (40 - 11 + 1) = 2 / 3
   * right_above_overlapping_ratio = 0 as col_right min value within overlapping range
   *
   * left_overlapping_distinct_count = (1 / 2) * 20 = 10
   * right_overlapping_distinct_count = (1 / 3) * 15 = 5
   *
   * For predicate condition equals only the ratios of values in the overlapping range is considered as values. If values could
   * match outside the overlapping range, the range would be false as it would be too small. In order to calculate the
   * equal value ratio, the column with fewer distinct values within the overlapping range is determined. In this case
   * this is col_right. Statistics component assumes that for two value sets for the same range the smaller set is
   * part of the bigger set. Therefore, it assumes that the 5 distinct values within the overlapping range of the right
   * column also exist in the left column. The equal value ratio is then calculated by multiplying
   * right_overlapping_ratio (= 1 / 2) with the probability to hit any distinct value of the left column (= 1 / 20):
   * equal_values_ratio = (1 / 2) * (1 / 20) = (1 / 40)
   * This is also the selectivity for the predicate condition equals: (1 / 40) = 2.5 %
   *
   * For predicate condition less the ratios left_below_overlapping_ratio and right_above_overlapping_ratio are also considered as
   * table entries where the col_left value is below the common range or the col_right value is above it will always be
   * in the result. The probability that both values are within the overlapping range and that col_left < col_right is
   * (probability of col_left != col_right where left and right values are in overlapping range) / 2
   *
   * The selectivity for predicate condition less is the sum of different probabilities: // NOLINT
   *    prob. that left value is below overlapping range (= 1 / 2) // NOLINT
   *  + prob. that right value is above overlapping range (= 1 / 3) // NOLINT
   *  - prob. that left value is below overlapping range and right value is above overlapping range (= 1 / 6) // NOLINT
   *  + prob. that left value < right value and both values are in common range // NOLINT
   *                                                                    (= ((1 / 6) - (1 / 20)) / 2 = 7 / 120) // NOLINT
   *  = 29 / 40 = 72.5 % // NOLINT
   */

  // Cannot compare columns of different type
  if (_data_type != base_right_column_statistics.data_type()) {
    return {1.0f, without_null_values(), base_right_column_statistics.without_null_values()};
  }

  const auto& right_column_statistics =
      static_cast<const ColumnStatistics<ColumnDataType>&>(base_right_column_statistics);

  // if columns have no distinct values, they can only have null values which cannot be selected with this predicate
  if (distinct_count() == 0 || right_column_statistics.distinct_count() == 0) {
    return {0.f, without_null_values(), right_column_statistics.without_null_values()};
  }

  const auto overlapping_range_min = std::max(_min, right_column_statistics.min());
  const auto overlapping_range_max = std::min(_max, right_column_statistics.max());

  // if no overlapping range exists, the result is empty
  if (overlapping_range_min > overlapping_range_max) {
    return {0.f, without_null_values(), right_column_statistics.without_null_values()};
  }

  // calculate ratio of values before, in and above the common value range
  const auto left_overlapping_ratio = estimate_range_selectivity(overlapping_range_min, overlapping_range_max);
  const auto right_overlapping_ratio =
      right_column_statistics.estimate_range_selectivity(overlapping_range_min, overlapping_range_max);

  auto left_below_overlapping_ratio = 0.f;
  auto left_above_overlapping_ratio = 0.f;
  auto right_below_overlapping_ratio = 0.f;
  auto right_above_overlapping_ratio = 0.f;

  if (std::is_integral<ColumnDataType>::value) {
    if (_min < overlapping_range_min) {
      left_below_overlapping_ratio = estimate_range_selectivity(_min, overlapping_range_min - 1);
    }
    if (overlapping_range_max < _max) {
      left_above_overlapping_ratio = estimate_range_selectivity(overlapping_range_max + 1, _max);
    }
    if (right_column_statistics.min() < overlapping_range_min) {
      right_below_overlapping_ratio =
          right_column_statistics.estimate_range_selectivity(right_column_statistics.min(), overlapping_range_min - 1);
    }
    if (overlapping_range_max < right_column_statistics.max()) {
      right_above_overlapping_ratio =
          right_column_statistics.estimate_range_selectivity(overlapping_range_max + 1, right_column_statistics.max());
    }
  } else {
    left_below_overlapping_ratio = estimate_range_selectivity(min(), overlapping_range_min);
    left_above_overlapping_ratio = estimate_range_selectivity(overlapping_range_max, max());
    right_below_overlapping_ratio =
        right_column_statistics.estimate_range_selectivity(right_column_statistics.min(), overlapping_range_min);
    right_above_overlapping_ratio =
        right_column_statistics.estimate_range_selectivity(overlapping_range_max, right_column_statistics.max());
  }

  // calculate ratio of distinct values in common value range
  const auto left_overlapping_distinct_count = left_overlapping_ratio * distinct_count();
  const auto right_overlapping_distinct_count = right_overlapping_ratio * right_column_statistics.distinct_count();

  auto equal_values_ratio = 0.0f;
  // calculate ratio of rows with equal values
  if (left_overlapping_distinct_count < right_overlapping_distinct_count) {
    equal_values_ratio = left_overlapping_ratio / right_column_statistics.distinct_count();
  } else {
    equal_values_ratio = right_overlapping_ratio / distinct_count();
  }

  const auto combined_non_null_ratio = non_null_value_ratio() * right_column_statistics.non_null_value_ratio();

  // used for <, <=, > and >= predicate_conditions
  auto estimate_selectivity_for_open_ended_operators = [&](float values_below_ratio, float values_above_ratio,
                                                           ColumnDataType new_min, ColumnDataType new_max,
                                                           bool add_equal_values) -> FilterByColumnComparisonEstimate {
    // selectivity calculated by adding up ratios that values are below, in or above overlapping range
    float selectivity = 0.f;
    // ratio of values on left hand side which are smaller than overlapping range
    selectivity += values_below_ratio;
    // selectivity of not equal numbers n1, n2 in overlapping range where n1 < n2 is 0.5
    selectivity += (left_overlapping_ratio * right_overlapping_ratio - equal_values_ratio) * 0.5f;
    if (add_equal_values) {
      selectivity += equal_values_ratio;
    }
    // ratio of values on right hand side which are greater than overlapping range
    selectivity += values_above_ratio;
    // remove ratio of rows, where one value is below and one value is above the overlapping range
    selectivity -= values_below_ratio * values_above_ratio;

    auto new_left_column_stats = estimate_range(new_min, new_max).column_statistics;
    auto new_right_column_stats = right_column_statistics.estimate_range(new_min, new_max).column_statistics;
    return {combined_non_null_ratio * selectivity, new_left_column_stats, new_right_column_stats};
  };

  // Currently the distinct count, min and max calculation is incorrect if predicate condition is OpLessThan or
  // OpGreaterThan and right column min = left column min or right column max = left column max.
  //
  // E.g. Two integer columns have 3 distinct values and same min and max value of 1 and 3.
  //
  // Both new left and right column statistics will have the same min and max values of 1 and 3.
  // However, for predicate condition OpLessThan, the left column max is actually 2 as there is no possibility
  // for 3 < 3. Additionally, the right column min is actually 2, as there is no possibility for 1 < 1.
  // The same also applies for predicate condition OpGreaterThan vice versa.
  // The smaller range between min and max values of a column will also lead to a smaller distinct count.
  //
  // TODO(Anyone): Fix issue mentioned above.

  switch (predicate_condition) {
    case PredicateCondition::Equals: {
      auto overlapping_distinct_count = std::min(left_overlapping_distinct_count, right_overlapping_distinct_count);

      auto new_left_column_stats = std::make_shared<ColumnStatistics>(0.0f, overlapping_distinct_count,
                                                                      overlapping_range_min, overlapping_range_max);
      auto new_right_column_stats = std::make_shared<ColumnStatistics>(0.0f, overlapping_distinct_count,
                                                                       overlapping_range_min, overlapping_range_max);
      return {combined_non_null_ratio * equal_values_ratio, new_left_column_stats, new_right_column_stats};
    }
    case PredicateCondition::NotEquals: {
      auto new_left_column_stats = std::make_shared<ColumnStatistics>(0.0f, distinct_count(), _min, _max);
      auto new_right_column_stats = std::make_shared<ColumnStatistics>(
          0.0f, right_column_statistics.distinct_count(), right_column_statistics._min, right_column_statistics._max);
      return {combined_non_null_ratio * (1.f - equal_values_ratio), new_left_column_stats, new_right_column_stats};
    }
    case PredicateCondition::LessThan: {
      return estimate_selectivity_for_open_ended_operators(left_below_overlapping_ratio, right_above_overlapping_ratio,
                                                           _min, right_column_statistics._max, false);
    }
    case PredicateCondition::LessThanEquals: {
      return estimate_selectivity_for_open_ended_operators(left_below_overlapping_ratio, right_above_overlapping_ratio,
                                                           _min, right_column_statistics._max, true);
    }
    case PredicateCondition::GreaterThan: {
      return estimate_selectivity_for_open_ended_operators(right_below_overlapping_ratio, left_above_overlapping_ratio,
                                                           right_column_statistics._min, _max, false);
    }
    case PredicateCondition::GreaterThanEquals: {
      return estimate_selectivity_for_open_ended_operators(right_below_overlapping_ratio, left_above_overlapping_ratio,
                                                           right_column_statistics._min, _max, true);
    }
    // case PredicateCondition::Between is not supported for ColumnID as TableScan does not support this
    default: { return {combined_non_null_ratio, without_null_values(), right_column_statistics.without_null_values()}; }
  }
}

/**
 * Specialization for strings as they cannot be used in subtractions.
 */
template <>
FilterByColumnComparisonEstimate ColumnStatistics<std::string>::estimate_predicate_with_column(
    const PredicateCondition predicate_condition, const BaseColumnStatistics& base_right_column_statistics) const {
  // TODO(anybody) implement special case for strings
  Assert(_data_type == base_right_column_statistics.data_type(), "Cannot compare columns of different type");

  const auto& right_column_statistics = static_cast<const ColumnStatistics<std::string>&>(base_right_column_statistics);

  // if columns have no distinct values, they can only have null values which cannot be selected with this predicate
  if (distinct_count() == 0 || right_column_statistics.distinct_count() == 0) {
    return {0.f, without_null_values(), right_column_statistics.without_null_values()};
  }

  return {non_null_value_ratio() * right_column_statistics.non_null_value_ratio(), without_null_values(),
          right_column_statistics.without_null_values()};
}

template <typename ColumnDataType>
std::string ColumnStatistics<ColumnDataType>::description() const {
  std::stringstream stream;
  stream << "Col Stats: " << std::endl;
  stream << "  dist.    " << _distinct_count << std::endl;
  stream << "  min      " << _min << std::endl;
  stream << "  max      " << _max << std::endl;
  stream << "  non-null " << non_null_value_ratio() << std::endl;
  return stream.str();
}

template <typename ColumnDataType>
float ColumnStatistics<ColumnDataType>::estimate_range_selectivity(const ColumnDataType minimum,
                                                                   const ColumnDataType maximum) const {
  DebugAssert(minimum <= maximum, "Minimum parameter is larger than maximum parameter.");
  // minimum must be smaller or equal than maximum
  // distinction between integers and decimals
  // for integers the number of possible integers is used within the inclusive ranges
  // for decimals the size of the range is used
  if (std::is_integral<ColumnDataType>::value) {
    return static_cast<float>(maximum - minimum + 1) / static_cast<float>(_max - _min + 1);
  } else {
    if (_max == _min) {
      return 1.0f;
    } else {
      return static_cast<float>(maximum - minimum) / static_cast<float>(_max - _min);
    }
  }
}

/**
 * Specialization for strings as they cannot be used in subtractions.
 */
template <>
float ColumnStatistics<std::string>::estimate_range_selectivity(const std::string minimum,          // NOLINT
                                                                const std::string maximum) const {  // NOLINT
  // TODO(anyone) implement selectivity for range approximation for column type string.
  return (maximum < minimum) ? 0.f : 1.f;
}

template <typename ColumnDataType>
FilterByValueEstimate ColumnStatistics<ColumnDataType>::estimate_range(const ColumnDataType minimum,
                                                                       const ColumnDataType maximum) const {
  // NOTE: minimum can be greater than maximum (e.g. a predicate >= 2 on a column with only values of 1)
  // new minimum/maximum of table cannot be smaller/larger than the current minimum/maximum
  const auto common_min = std::max(minimum, _min);  // NOLINT (false performance-unnecessary-copy-initialization)
  const auto common_max = std::min(maximum, _max);  // NOLINT
  if (common_min == _min && common_max == _max) {
    return {non_null_value_ratio(), without_null_values()};
  }
  auto selectivity = 0.f;
  // estimate_selectivity_for_range function expects that the minimum must not be greater than the maximum
  if (common_min <= common_max) {
    selectivity = estimate_range_selectivity(common_min, common_max);
  }
  auto column_statistics =
      std::make_shared<ColumnStatistics<ColumnDataType>>(0.0f, selectivity * distinct_count(), common_min, common_max);
  return {non_null_value_ratio() * selectivity, column_statistics};
}

template <typename ColumnDataType>
FilterByValueEstimate ColumnStatistics<ColumnDataType>::estimate_equals_with_value(const ColumnDataType value) const {
  DebugAssert(distinct_count() > 0, "Distinct count has to be greater zero");
  float new_distinct_count = 1.f;
  if (value < _min || value > _max) {
    new_distinct_count = 0.f;
  }
  auto column_statistics = std::make_shared<ColumnStatistics<ColumnDataType>>(0.0f, new_distinct_count, value, value);
  if (distinct_count() == 0.0f) {
    return {0.0f, column_statistics};
  } else {
    return {non_null_value_ratio() * new_distinct_count / distinct_count(), column_statistics};
  }
}

template <typename ColumnDataType>
FilterByValueEstimate ColumnStatistics<ColumnDataType>::estimate_not_equals_with_value(
    const ColumnDataType value) const {
  DebugAssert(distinct_count() > 0, "Distinct count has to be greater zero");
  if (value < _min || value > _max) {
    return {non_null_value_ratio(), without_null_values()};
  }
  auto column_statistics = std::make_shared<ColumnStatistics<ColumnDataType>>(0.0f, distinct_count() - 1, _min, _max);
  if (distinct_count() == 0.0f) {
    return {0.0f, column_statistics};
  } else {
    return {non_null_value_ratio() * (1 - 1.f / distinct_count()), column_statistics};
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(ColumnStatistics);
}  // namespace opossum
