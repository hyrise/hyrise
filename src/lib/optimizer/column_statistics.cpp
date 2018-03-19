#include "column_statistics.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "all_parameter_variant.hpp"
#include "operators/aggregate.hpp"
#include "operators/table_wrapper.hpp"
#include "resolve_type.hpp"
#include "storage/table.hpp"
#include "table_statistics.hpp"
#include "type_cast.hpp"
#include "types.hpp"

namespace opossum {

template <typename ColumnType>
ColumnStatistics<ColumnType>::ColumnStatistics(const ColumnID column_id, const std::weak_ptr<Table> table)
    : BaseColumnStatistics(data_type_from_type<ColumnType>()), _column_id(column_id), _table(table) {}

template <typename ColumnType>
ColumnStatistics<ColumnType>::ColumnStatistics(const ColumnID column_id, float distinct_count, const ColumnType min,
                                               const ColumnType max, const float non_null_value_ratio)
    : BaseColumnStatistics(data_type_from_type<ColumnType>(), non_null_value_ratio),
      _column_id(column_id),
      _table(std::weak_ptr<Table>()),
      _distinct_count(distinct_count),
      _min(min),
      _max(max) {}

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
  auto aggregate = std::make_shared<Aggregate>(table_wrapper, std::vector<AggregateColumnDefinition>{},
                                               std::vector<ColumnID>{_column_id});
  aggregate->execute();
  auto aggregate_table = aggregate->get_output();
  _distinct_count = aggregate_table->row_count();
  return *_distinct_count;
}

template <typename ColumnType>
std::shared_ptr<BaseColumnStatistics> ColumnStatistics<ColumnType>::clone() const {
  return std::make_shared<ColumnStatistics>(*this);
}

template <typename ColumnType>
ColumnType ColumnStatistics<ColumnType>::_get_or_calculate_min() const {
  if (!_min) {
    _initialize_min_max();
  }
  return *_min;
}

template <typename ColumnType>
ColumnType ColumnStatistics<ColumnType>::_get_or_calculate_max() const {
  if (!_max) {
    _initialize_min_max();
  }
  return *_max;
}

template <typename ColumnType>
void ColumnStatistics<ColumnType>::_initialize_min_max() const {
  // Calculation is delegated to aggregate operator.
  auto table = _table.lock();
  DebugAssert(table != nullptr, "Corresponding table of column statistics is deleted.");

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  auto aggregate_args = std::vector<AggregateColumnDefinition>{{_column_id, AggregateFunction::Min},
                                                               {_column_id, AggregateFunction::Max}};
  auto aggregate = std::make_shared<Aggregate>(table_wrapper, aggregate_args, std::vector<ColumnID>{});
  aggregate->execute();

  auto aggregate_table = aggregate->get_output();

  auto min_column = std::static_pointer_cast<const ValueColumn<ColumnType>>(
      aggregate_table->get_chunk(ChunkID{0})->get_column(ColumnID{0}));
  _min = min_column->values()[0];

  auto max_column = std::static_pointer_cast<const ValueColumn<ColumnType>>(
      aggregate_table->get_chunk(ChunkID{0})->get_column(ColumnID{1}));
  _max = max_column->values()[0];
}

template <typename ColumnType>
std::shared_ptr<BaseColumnStatistics> ColumnStatistics<ColumnType>::_this_without_null_values() {
  if (_non_null_value_ratio == 1.f) {
    return shared_from_this();
  }
  // this needs to be copied, as the non-null value ratio is changed
  auto clone = std::make_shared<ColumnStatistics>(*this);
  clone->_non_null_value_ratio = 1.f;
  return clone;
}

template <typename ColumnType>
ColumnSelectivityResult ColumnStatistics<ColumnType>::_create_column_stats_for_range_predicate(ColumnType minimum,
                                                                                               ColumnType maximum) {
  // NOTE: minimum can be greater than maximum (e.g. a predicate >= 2 on a column with only values of 1)
  // new minimum/maximum of table cannot be smaller/larger than the current minimum/maximum
  auto common_min = std::max(minimum, _get_or_calculate_min());
  auto common_max = std::min(maximum, _get_or_calculate_max());
  if (common_min == _get_or_calculate_min() && common_max == _get_or_calculate_max()) {
    return {_non_null_value_ratio, _this_without_null_values()};
  }
  float selectivity = 0.f;
  // estimate_selectivity_for_range function expects that the minimum must not be greater than the maximum
  if (common_min <= common_max) {
    selectivity = estimate_selectivity_for_range(common_min, common_max);
  }
  auto column_statistics =
      std::make_shared<ColumnStatistics>(_column_id, selectivity * distinct_count(), common_min, common_max);
  return {_non_null_value_ratio * selectivity, column_statistics};
}

template <typename ColumnType>
float ColumnStatistics<ColumnType>::estimate_selectivity_for_range(ColumnType minimum, ColumnType maximum) {
  // minimum must be smaller or equal than maximum
  // distinction between integers and decimals
  // for integers the number of possible integers is used within the inclusive ranges
  // for decimals the size of the range is used
  if (std::is_integral<ColumnType>::value) {
    return static_cast<float>(maximum - minimum + 1) /
           static_cast<float>(_get_or_calculate_max() - _get_or_calculate_min() + 1);
  } else {
    return static_cast<float>(maximum - minimum) /
           static_cast<float>(_get_or_calculate_max() - _get_or_calculate_min());
  }
}

/**
 * Specialization for strings as they cannot be used in subtractions.
 */
template <>
float ColumnStatistics<std::string>::estimate_selectivity_for_range(std::string minimum, std::string maximum) {
  // TODO(anyone) implement selectivity for range approximation for column type string.
  return (maximum < minimum) ? 0.f : 1.f;
}

template <typename ColumnType>
ColumnSelectivityResult ColumnStatistics<ColumnType>::_create_column_stats_for_equals_predicate(ColumnType value) {
  DebugAssert(distinct_count() > 0, "Distinct count has to be greater zero");
  float new_distinct_count = 1.f;
  if (value < _get_or_calculate_min() || value > _get_or_calculate_max()) {
    new_distinct_count = 0.f;
  }
  auto column_statistics = std::make_shared<ColumnStatistics>(_column_id, new_distinct_count, value, value);
  return {_non_null_value_ratio * new_distinct_count / distinct_count(), column_statistics};
}

template <typename ColumnType>
ColumnSelectivityResult ColumnStatistics<ColumnType>::_create_column_stats_for_not_equals_predicate(ColumnType value) {
  DebugAssert(distinct_count() > 0, "Distinct count has to be greater zero");
  if (value < _get_or_calculate_min() || value > _get_or_calculate_max()) {
    return {_non_null_value_ratio, _this_without_null_values()};
  }
  auto column_statistics = std::make_shared<ColumnStatistics>(_column_id, distinct_count() - 1, _get_or_calculate_min(),
                                                              _get_or_calculate_max());
  return {_non_null_value_ratio * (1 - 1.f / distinct_count()), column_statistics};
}

template <typename ColumnType>
ColumnSelectivityResult ColumnStatistics<ColumnType>::estimate_selectivity_for_predicate(
    const PredicateCondition predicate_condition, const AllTypeVariant& value,
    const std::optional<AllTypeVariant>& value2) {
  auto casted_value = type_cast<ColumnType>(value);

  switch (predicate_condition) {
    case PredicateCondition::Equals: {
      return _create_column_stats_for_equals_predicate(casted_value);
    }
    case PredicateCondition::NotEquals: {
      return _create_column_stats_for_not_equals_predicate(casted_value);
    }
    case PredicateCondition::LessThan: {
      // distinction between integers and decimals
      // for integers "< value" means that the new max is value <= value - 1
      // for decimals "< value" means that the new max is value <= value - ε
      if (std::is_integral<ColumnType>::value) {
        return _create_column_stats_for_range_predicate(_get_or_calculate_min(), casted_value - 1);
      }
      // intentionally no break
      // if ColumnType is a floating point number,
      // OpLessThanEquals behaviour is expected instead of OpLessThan
      [[fallthrough]];
    }
    case PredicateCondition::LessThanEquals: {
      return _create_column_stats_for_range_predicate(_get_or_calculate_min(), casted_value);
    }
    case PredicateCondition::GreaterThan: {
      // distinction between integers and decimals
      // for integers "> value" means that the new min value is >= value + 1
      // for decimals "> value" means that the new min value is >= value + ε
      if (std::is_integral<ColumnType>::value) {
        return _create_column_stats_for_range_predicate(casted_value + 1, _get_or_calculate_max());
      }
      // intentionally no break
      // if ColumnType is a floating point number,
      // OpGreaterThanEquals behaviour is expected instead of OpGreaterThan
      [[fallthrough]];
    }
    case PredicateCondition::GreaterThanEquals: {
      return _create_column_stats_for_range_predicate(casted_value, _get_or_calculate_max());
    }
    case PredicateCondition::Between: {
      DebugAssert(static_cast<bool>(value2), "Operator BETWEEN should get two parameters, second is missing!");
      auto casted_value2 = type_cast<ColumnType>(*value2);
      return _create_column_stats_for_range_predicate(casted_value, casted_value2);
    }
    default: { return {_non_null_value_ratio, _this_without_null_values()}; }
  }
}

/**
 * Specialization for strings as they cannot be used in subtractions.
 */
template <>
ColumnSelectivityResult ColumnStatistics<std::string>::estimate_selectivity_for_predicate(
    const PredicateCondition predicate_condition, const AllTypeVariant& value,
    const std::optional<AllTypeVariant>& value2) {
  // if column has no distinct values, it can only have null values which cannot be selected with this predicate
  if (distinct_count() == 0) {
    return {0.f, _this_without_null_values()};
  }

  auto casted_value = type_cast<std::string>(value);
  switch (predicate_condition) {
    case PredicateCondition::Equals: {
      return _create_column_stats_for_equals_predicate(casted_value);
    }
    case PredicateCondition::NotEquals: {
      return _create_column_stats_for_not_equals_predicate(casted_value);
    }
    // TODO(anybody) implement other table-scan operators for string.
    default: { return {_non_null_value_ratio, _this_without_null_values()}; }
  }
}

template <typename ColumnType>
ColumnSelectivityResult ColumnStatistics<ColumnType>::estimate_selectivity_for_predicate(
    const PredicateCondition predicate_condition, const ValuePlaceholder& value,
    const std::optional<AllTypeVariant>& value2) {
  // if column has no distinct values, it can only have null values which cannot be selected with this predicate
  if (distinct_count() == 0) {
    return {0.f, _this_without_null_values()};
  }

  switch (predicate_condition) {
    case PredicateCondition::Equals: {
      auto column_statistics =
          std::make_shared<ColumnStatistics>(_column_id, 1, _get_or_calculate_min(), _get_or_calculate_max());
      return {1.f / distinct_count(), column_statistics};
    }
    case PredicateCondition::NotEquals: {
      auto column_statistics = std::make_shared<ColumnStatistics>(_column_id, distinct_count() - 1,
                                                                  _get_or_calculate_min(), _get_or_calculate_max());
      return {(distinct_count() - 1.f) / distinct_count(), column_statistics};
    }
    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals:
    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals: {
      auto column_statistics =
          std::make_shared<ColumnStatistics>(_column_id, distinct_count() * DEFAULT_OPEN_ENDED_SELECTIVITY,
                                             _get_or_calculate_min(), _get_or_calculate_max());
      return {_non_null_value_ratio * DEFAULT_OPEN_ENDED_SELECTIVITY, column_statistics};
    }
    case PredicateCondition::Between: {
      // since the value2 is known,
      // first, statistics for the operation <= value are calculated
      // then, the open ended selectivity is applied on the result
      DebugAssert(static_cast<bool>(value2), "Operator BETWEEN should get two parameters, second is missing!");
      auto casted_value2 = type_cast<ColumnType>(*value2);
      ColumnSelectivityResult output = _create_column_stats_for_range_predicate(_get_or_calculate_min(), casted_value2);
      // return, if value2 < min
      if (output.selectivity == 0.f) {
        return output;
      }
      // create statistics, if value2 >= max
      if (output.column_statistics == shared_from_this()) {
        output.column_statistics = std::make_shared<ColumnStatistics>(_column_id, distinct_count(),
                                                                      _get_or_calculate_min(), _get_or_calculate_max());
      }
      // apply default selectivity for open ended
      output.selectivity *= DEFAULT_OPEN_ENDED_SELECTIVITY;
      // column statistics have just been created, therefore, cast to the column type cannot fail
      auto column_statistics = std::dynamic_pointer_cast<ColumnStatistics<ColumnType>>(output.column_statistics);
      *(column_statistics->_distinct_count) *= DEFAULT_OPEN_ENDED_SELECTIVITY;
      return output;
    }
    default: { return {_non_null_value_ratio, _this_without_null_values()}; }
  }
}

template <typename ColumnType>
TwoColumnSelectivityResult ColumnStatistics<ColumnType>::estimate_selectivity_for_two_column_predicate(
    const PredicateCondition predicate_condition,
    const std::shared_ptr<BaseColumnStatistics>& right_base_column_statistics,
    const std::optional<AllTypeVariant>& value2) {
  /**
   * Calculate expected selectivity by looking at what ratio of values of both columns are in the overlapping value
   * range of both columns. If the two columns have different min values, then the column with the smaller min value
   * will have values below the overlapping range. The same applies to values above the overlapping range. If the max
   * values are not the same, then the column with the larger max value will have values above the overlapping range.
   *
   * For the different predicate conditions the appropriate ratios of values below, within and above the overlapping range from
   * both columns are taken to compute the selectivity.
   *
   * Example estimation:
   *
   * |  Column name     |  col_left  |  col_right  |
   * |  Column type     |  int       |  int        |
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

  auto right_stats = std::dynamic_pointer_cast<ColumnStatistics<ColumnType>>(right_base_column_statistics);
  DebugAssert(right_stats != nullptr, "Cannot compare columns of different type");

  // if columns have no distinct values, they can only have null values which cannot be selected with this predicate
  if (distinct_count() == 0 || right_stats->distinct_count() == 0) {
    return {0.f, _this_without_null_values(), right_stats->_this_without_null_values()};
  }

  // for predicate "col_left < col_right": col_left statistics = this and col_right statistics = right_stats

  auto overlapping_range_min = std::max(_get_or_calculate_min(), right_stats->_get_or_calculate_min());
  auto overlapping_range_max = std::min(_get_or_calculate_max(), right_stats->_get_or_calculate_max());

  // calculate ratio of values before, in and above the common value range
  float left_overlapping_ratio = estimate_selectivity_for_range(overlapping_range_min, overlapping_range_max);
  float right_overlapping_ratio =
      right_stats->estimate_selectivity_for_range(overlapping_range_min, overlapping_range_max);

  float left_below_overlapping_ratio = 0.f;
  float left_above_overlapping_ratio = 0.f;
  float right_below_overlapping_ratio = 0.f;
  float right_above_overlapping_ratio = 0.f;
  if (std::is_integral<ColumnType>::value) {
    if (_get_or_calculate_min() < overlapping_range_min) {
      left_below_overlapping_ratio = estimate_selectivity_for_range(_get_or_calculate_min(), overlapping_range_min - 1);
    }
    if (overlapping_range_max < _get_or_calculate_max()) {
      left_above_overlapping_ratio = estimate_selectivity_for_range(overlapping_range_max + 1, _get_or_calculate_max());
    }
    if (right_stats->_get_or_calculate_min() < overlapping_range_min) {
      right_below_overlapping_ratio =
          right_stats->estimate_selectivity_for_range(right_stats->_get_or_calculate_min(), overlapping_range_min - 1);
    }
    if (overlapping_range_max < right_stats->_get_or_calculate_max()) {
      right_above_overlapping_ratio =
          right_stats->estimate_selectivity_for_range(overlapping_range_max + 1, right_stats->_get_or_calculate_max());
    }
  } else {
    left_below_overlapping_ratio = estimate_selectivity_for_range(_get_or_calculate_min(), overlapping_range_min);
    left_above_overlapping_ratio = estimate_selectivity_for_range(overlapping_range_max, _get_or_calculate_max());
    right_below_overlapping_ratio =
        right_stats->estimate_selectivity_for_range(right_stats->_get_or_calculate_min(), overlapping_range_min);
    right_above_overlapping_ratio =
        right_stats->estimate_selectivity_for_range(overlapping_range_max, right_stats->_get_or_calculate_max());
  }

  // calculate ratio of distinct values in common value range
  auto left_overlapping_distinct_count = left_overlapping_ratio * distinct_count();
  auto right_overlapping_distinct_count = right_overlapping_ratio * right_stats->distinct_count();

  float equal_values_ratio;
  // calculate ratio of rows with equal values
  if (left_overlapping_distinct_count < right_overlapping_distinct_count) {
    equal_values_ratio = left_overlapping_ratio / right_stats->distinct_count();
  } else {
    equal_values_ratio = right_overlapping_ratio / distinct_count();
  }

  float combined_non_null_ratio = _non_null_value_ratio * right_stats->_non_null_value_ratio;

  // used for <, <=, > and >= predicate_conditions
  auto estimate_selectivity_for_open_ended_operators = [&](float values_below_ratio, float values_above_ratio,
                                                           ColumnType new_min, ColumnType new_max,
                                                           bool add_equal_values) -> TwoColumnSelectivityResult {
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

    auto new_left_column_stats = _create_column_stats_for_range_predicate(new_min, new_max).column_statistics;
    auto new_right_column_stats =
        right_stats->_create_column_stats_for_range_predicate(new_min, new_max).column_statistics;
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

      auto new_left_column_stats = std::make_shared<ColumnStatistics>(_column_id, overlapping_distinct_count,
                                                                      overlapping_range_min, overlapping_range_max);
      auto new_right_column_stats = std::make_shared<ColumnStatistics>(
          right_stats->_column_id, overlapping_distinct_count, overlapping_range_min, overlapping_range_max);
      return {combined_non_null_ratio * equal_values_ratio, new_left_column_stats, new_right_column_stats};
    }
    case PredicateCondition::NotEquals: {
      auto new_left_column_stats = std::make_shared<ColumnStatistics>(_column_id, distinct_count(),
                                                                      _get_or_calculate_min(), _get_or_calculate_max());
      auto new_right_column_stats = std::make_shared<ColumnStatistics>(
          right_stats->_column_id, right_stats->distinct_count(), right_stats->_get_or_calculate_min(),
          right_stats->_get_or_calculate_max());
      return {combined_non_null_ratio * (1.f - equal_values_ratio), new_left_column_stats, new_right_column_stats};
    }
    case PredicateCondition::LessThan: {
      return estimate_selectivity_for_open_ended_operators(left_below_overlapping_ratio, right_above_overlapping_ratio,
                                                           _get_or_calculate_min(),
                                                           right_stats->_get_or_calculate_max(), false);
    }
    case PredicateCondition::LessThanEquals: {
      return estimate_selectivity_for_open_ended_operators(left_below_overlapping_ratio, right_above_overlapping_ratio,
                                                           _get_or_calculate_min(),
                                                           right_stats->_get_or_calculate_max(), true);
    }
    case PredicateCondition::GreaterThan: {
      return estimate_selectivity_for_open_ended_operators(right_below_overlapping_ratio, left_above_overlapping_ratio,
                                                           right_stats->_get_or_calculate_min(),
                                                           _get_or_calculate_max(), false);
    }
    case PredicateCondition::GreaterThanEquals: {
      return estimate_selectivity_for_open_ended_operators(right_below_overlapping_ratio, left_above_overlapping_ratio,
                                                           right_stats->_get_or_calculate_min(),
                                                           _get_or_calculate_max(), true);
    }
    // case PredicateCondition::Between is not supported for ColumnID as TableScan does not support this
    default: {
      return {combined_non_null_ratio, _this_without_null_values(), right_stats->_this_without_null_values()};
    }
  }
}

/**
 * Specialization for strings as they cannot be used in subtractions.
 */
template <>
TwoColumnSelectivityResult ColumnStatistics<std::string>::estimate_selectivity_for_two_column_predicate(
    const PredicateCondition predicate_condition,
    const std::shared_ptr<BaseColumnStatistics>& right_base_column_statistics,
    const std::optional<AllTypeVariant>& value2) {
  // TODO(anybody) implement special case for strings
  auto right_stats = std::dynamic_pointer_cast<ColumnStatistics<std::string>>(right_base_column_statistics);
  DebugAssert(right_stats != nullptr, "Cannot compare columns of different type");

  // if columns have no distinct values, they can only have null values which cannot be selected with this predicate
  if (distinct_count() == 0 || right_stats->distinct_count() == 0) {
    return {0.f, _this_without_null_values(), right_stats->_this_without_null_values()};
  }

  return {_non_null_value_ratio * right_stats->_non_null_value_ratio, _this_without_null_values(),
          right_stats->_this_without_null_values()};
}

template <typename ColumnType>
std::ostream& ColumnStatistics<ColumnType>::_print_to_stream(std::ostream& os) const {
  os << "Col Stats id: " << _column_id << std::endl;
  os << "  dist.    " << _distinct_count << std::endl;
  os << "  min      " << _min << std::endl;
  os << "  max      " << _max << std::endl;
  os << "  non-null " << _non_null_value_ratio;
  return os;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(ColumnStatistics);

}  // namespace opossum
