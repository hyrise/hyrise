#include "histogram_column_statistics.hpp"

#include <sstream>

#include "minimal_column_statistics.hpp"
#include "table_statistics.hpp"

#include "resolve_type.hpp"
#include "type_cast.hpp"

namespace opossum {

template <typename ColumnDataType>
HistogramColumnStatistics<ColumnDataType>::HistogramColumnStatistics(
    const std::shared_ptr<AbstractHistogram<ColumnDataType>>& histogram, const float null_value_ratio)
    : BaseColumnStatistics(data_type_from_type<ColumnDataType>(), null_value_ratio), _histogram(histogram) {}

template <typename ColumnDataType>
std::string HistogramColumnStatistics<ColumnDataType>::_description() const {
  std::stringstream stream;
  stream << _histogram->description();
  return stream.str();
}

template <typename ColumnDataType>
const std::shared_ptr<const AbstractHistogram<ColumnDataType>> HistogramColumnStatistics<ColumnDataType>::histogram()
    const {
  return _histogram;
}

template <typename ColumnDataType>
AllTypeVariant HistogramColumnStatistics<ColumnDataType>::min() const {
  return AllTypeVariant{_histogram->min()};
}

template <typename ColumnDataType>
AllTypeVariant HistogramColumnStatistics<ColumnDataType>::max() const {
  return AllTypeVariant{_histogram->max()};
}

template <typename ColumnDataType>
float HistogramColumnStatistics<ColumnDataType>::distinct_count() const {
  return _histogram->total_distinct_count();
}

template <typename ColumnDataType>
std::shared_ptr<BaseColumnStatistics> HistogramColumnStatistics<ColumnDataType>::clone() const {
  return std::make_shared<HistogramColumnStatistics<ColumnDataType>>(_histogram, null_value_ratio());
}

template <typename ColumnDataType>
FilterByValueEstimate HistogramColumnStatistics<ColumnDataType>::estimate_predicate_with_value(
    const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  const auto can_prune = _histogram->can_prune(predicate_condition, variant_value, variant_value2);

  const auto value = type_cast<ColumnDataType>(variant_value);
  ColumnDataType value2;
  if (predicate_condition == PredicateCondition::Between) {
    Assert(static_cast<bool>(variant_value2), "Between operator needs two values.");
    value2 = type_cast<ColumnDataType>(*variant_value2);
  }
  const auto selectivity = can_prune ? 0.f : _histogram->estimate_selectivity(predicate_condition, value, value2);

  switch (predicate_condition) {
    case PredicateCondition::Equals:
      return estimate_equals(selectivity, can_prune, value);
    case PredicateCondition::NotEquals:
      return estimate_not_equals(selectivity, can_prune, value);
    case PredicateCondition::LessThanEquals:
      return estimate_range(selectivity, can_prune, _histogram->min(), value);
    case PredicateCondition::LessThan:
      return estimate_range(selectivity, can_prune, _histogram->min(), _histogram->get_previous_value(value));
    case PredicateCondition::GreaterThanEquals:
      return estimate_range(selectivity, can_prune, value, _histogram->max());
    case PredicateCondition::GreaterThan:
      return estimate_range(selectivity, can_prune, _histogram->_get_next_value(value), _histogram->max());
    case PredicateCondition::Between:
      return estimate_range(selectivity, can_prune, value, value2);
    case PredicateCondition::Like:
    case PredicateCondition::NotLike:
      // TODO(anybody): think about better min/max values
      return estimate_range(selectivity, can_prune, _histogram->min(), _histogram->max());
    default:
      Fail("Predicate type not supported yet.");
  }
}

template <typename ColumnDataType>
FilterByValueEstimate HistogramColumnStatistics<ColumnDataType>::estimate_predicate_with_value_placeholder(
    const PredicateCondition predicate_condition, const std::optional<AllTypeVariant>& variant_value2) const {
  switch (predicate_condition) {
    // Simply assume the value will be in the domain of the column.
    // Pick the min as the dummy value and do not update min and max statistics.
    case PredicateCondition::Equals:
      return estimate_equals(1.f / _histogram->total_distinct_count(), false, _histogram->min(), false);
    case PredicateCondition::NotEquals:
      return estimate_not_equals(1 - 1.f / _histogram->total_distinct_count(), false, _histogram->min(), false);

    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals:
    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals:
      return estimate_range(TableStatistics::DEFAULT_OPEN_ENDED_SELECTIVITY, false, _histogram->min(),
                            _histogram->max());

    case PredicateCondition::Between: {
      // Since value2 is known first calculate statistics for "<= value2".
      DebugAssert(static_cast<bool>(variant_value2), "Operator BETWEEN should get two parameters, second is missing!");
      const auto casted_value2 = type_cast<ColumnDataType>(*variant_value2);

      const auto can_prune = _histogram->can_prune(PredicateCondition::LessThanEquals, *variant_value2);
      if (can_prune) {
        return estimate_range(0.f, true, _histogram->min(), casted_value2);
      }

      // If it cannot be pruned, combine the selectivity for "<= value" with DEFAULT_OPEN_ENDED_SELECTIVITY.
      const auto value2_selectivity =
          _histogram->estimate_selectivity(PredicateCondition::LessThanEquals, casted_value2);

      return estimate_range(value2_selectivity * TableStatistics::DEFAULT_OPEN_ENDED_SELECTIVITY, false,
                            _histogram->min(), casted_value2);
    }
    default: { return {non_null_value_ratio(), without_null_values()}; }
  }
}

template <typename ColumnDataType>
FilterByColumnComparisonEstimate HistogramColumnStatistics<ColumnDataType>::estimate_predicate_with_column(
    const PredicateCondition predicate_condition,
    const std::shared_ptr<const BaseColumnStatistics>& base_right_column_statistics) const {
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

  // Cannot currently compare columns of different types.
  // TODO(anybody): fix
  if (_data_type != base_right_column_statistics->data_type()) {
    return {1.0f, without_null_values(), base_right_column_statistics->without_null_values()};
  }

  const auto& right_histogram_column_statistics =
      std::dynamic_pointer_cast<const HistogramColumnStatistics<ColumnDataType>>(base_right_column_statistics);

  const auto right_min = type_cast<ColumnDataType>(base_right_column_statistics->min());
  const auto right_max = type_cast<ColumnDataType>(base_right_column_statistics->max());

  // if columns have no distinct values, they can only have null values which cannot be selected with this predicate
  if (distinct_count() == 0 || base_right_column_statistics->distinct_count() == 0) {
    return {0.f, without_null_values(), base_right_column_statistics->without_null_values()};
  }

  const auto overlapping_range_min = std::max(_histogram->min(), right_min);
  const auto overlapping_range_max = std::min(_histogram->max(), right_max);

  // if no overlapping range exists, the result is empty
  if (overlapping_range_min > overlapping_range_max) {
    return {0.f, without_null_values(), base_right_column_statistics->without_null_values()};
  }

  // calculate ratio of values before, in and above the common value range
  const auto left_overlapping_ratio = estimate_range_selectivity(overlapping_range_min, overlapping_range_max);
  const auto right_overlapping_ratio =
      base_right_column_statistics->estimate_range_selectivity(overlapping_range_min, overlapping_range_max);

  const auto left_below_overlapping_ratio =
      _histogram->min() >= overlapping_range_min
          ? 0.f
          : _histogram->estimate_selectivity(PredicateCondition::LessThan, overlapping_range_min);
  const auto left_above_overlapping_ratio =
      _histogram->max() < overlapping_range_max
          ? 0.f
          : _histogram->estimate_selectivity(PredicateCondition::GreaterThan, overlapping_range_max);
  auto right_below_overlapping_ratio = 0.f;
  auto right_above_overlapping_ratio = 0.f;

  if (right_histogram_column_statistics) {
    right_below_overlapping_ratio = right_histogram_column_statistics->histogram()->estimate_selectivity(
        PredicateCondition::LessThan, overlapping_range_min);
    right_above_overlapping_ratio = right_histogram_column_statistics->histogram()->estimate_selectivity(
        PredicateCondition::GreaterThan, overlapping_range_max);
  } else {
    if constexpr (std::is_integral_v<ColumnDataType>) {
      if (right_min < overlapping_range_min) {
        right_below_overlapping_ratio =
            base_right_column_statistics->estimate_range_selectivity(right_min, overlapping_range_min - 1);
      }
      if (overlapping_range_max < right_max) {
        right_above_overlapping_ratio =
            base_right_column_statistics->estimate_range_selectivity(overlapping_range_max + 1, right_max);
      }
    } else {
      right_below_overlapping_ratio =
          base_right_column_statistics->estimate_range_selectivity(right_min, overlapping_range_min);
      right_above_overlapping_ratio =
          base_right_column_statistics->estimate_range_selectivity(overlapping_range_max, right_max);
    }
  }

  // calculate ratio of distinct values in common value range
  const auto left_overlapping_distinct_count =
      _histogram->estimate_distinct_count(PredicateCondition::Between, overlapping_range_min, overlapping_range_max);
  const auto right_overlapping_distinct_count = base_right_column_statistics->estimate_distinct_count(
      PredicateCondition::Between, overlapping_range_min, overlapping_range_max);

  auto equal_values_ratio = 0.f;
  // calculate ratio of rows with equal values
  if (left_overlapping_distinct_count < right_overlapping_distinct_count) {
    equal_values_ratio = left_overlapping_ratio / base_right_column_statistics->distinct_count();
  } else {
    equal_values_ratio = right_overlapping_ratio / distinct_count();
  }

  const auto combined_non_null_ratio = non_null_value_ratio() * base_right_column_statistics->non_null_value_ratio();

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

    auto new_left_column_stats = estimate_range(selectivity, false, new_min, new_max).column_statistics;
    const std::shared_ptr<BaseColumnStatistics> new_right_column_stats =
        base_right_column_statistics->estimate_range(new_min, new_max).column_statistics;
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
      auto new_column_stats = std::make_shared<MinimalColumnStatistics<ColumnDataType>>(
          0.0f, overlapping_distinct_count, overlapping_range_min, overlapping_range_max);
      return {combined_non_null_ratio * equal_values_ratio, new_column_stats, new_column_stats->clone()};
    }
    case PredicateCondition::NotEquals: {
      auto new_left_column_stats = std::make_shared<MinimalColumnStatistics<ColumnDataType>>(
          0.0f, distinct_count(), _histogram->min(), _histogram->max());
      auto new_right_column_stats = std::make_shared<MinimalColumnStatistics<ColumnDataType>>(
          0.0f, base_right_column_statistics->distinct_count(), right_min, right_max);
      return {combined_non_null_ratio * (1.f - equal_values_ratio), new_left_column_stats, new_right_column_stats};
    }
    case PredicateCondition::LessThan: {
      return estimate_selectivity_for_open_ended_operators(left_below_overlapping_ratio, right_above_overlapping_ratio,
                                                           _histogram->min(), right_max, false);
    }
    case PredicateCondition::LessThanEquals: {
      return estimate_selectivity_for_open_ended_operators(left_below_overlapping_ratio, right_above_overlapping_ratio,
                                                           _histogram->min(), right_max, true);
    }
    case PredicateCondition::GreaterThan: {
      return estimate_selectivity_for_open_ended_operators(right_below_overlapping_ratio, left_above_overlapping_ratio,
                                                           right_min, _histogram->max(), false);
    }
    case PredicateCondition::GreaterThanEquals: {
      return estimate_selectivity_for_open_ended_operators(right_below_overlapping_ratio, left_above_overlapping_ratio,
                                                           right_min, _histogram->max(), true);
    }
    // case PredicateCondition::Between is not supported for ColumnID as TableScan does not support this
    default: {
      return {combined_non_null_ratio, without_null_values(), base_right_column_statistics->without_null_values()};
    }
  }
}

template <typename ColumnDataType>
float HistogramColumnStatistics<ColumnDataType>::estimate_range_selectivity(
    const AllTypeVariant& variant_minimum, const AllTypeVariant& variant_maximum) const {
  const auto min = type_cast<ColumnDataType>(variant_minimum);
  const auto max = type_cast<ColumnDataType>(variant_maximum);
  return _histogram->estimate_selectivity(PredicateCondition::Between, min, max);
}

template <typename ColumnDataType>
FilterByValueEstimate HistogramColumnStatistics<ColumnDataType>::estimate_equals(const float selectivity,
                                                                                 const bool can_prune,
                                                                                 const ColumnDataType value,
                                                                                 const bool update_min_max) const {
  const auto new_distinct_count = can_prune ? 0.f : 1.f;
  const auto new_min = update_min_max ? value : _histogram->min();
  const auto new_max = update_min_max ? value : _histogram->max();
  const auto column_statistics =
      std::make_shared<MinimalColumnStatistics<ColumnDataType>>(0.0f, new_distinct_count, new_min, new_max);
  return {selectivity, column_statistics};
}

template <typename ColumnDataType>
FilterByValueEstimate HistogramColumnStatistics<ColumnDataType>::estimate_not_equals(const float selectivity,
                                                                                     const bool can_prune,
                                                                                     const ColumnDataType value,
                                                                                     const bool update_min_max) const {
  // If the value filtered for is either the min or max, we can update the min/max.
  // Unfortunately, we do not know exactly which value is the second lowest/highest,
  // so we simply take the next higher/lower one.
  const auto new_min =
      _histogram->min() == value && update_min_max ? _histogram->_get_next_value(value) : _histogram->min();
  const auto new_max =
      _histogram->max() == value && update_min_max ? _histogram->get_previous_value(value) : _histogram->max();

  const auto new_distinct_count = can_prune ? 0.f : distinct_count() - 1;
  auto column_statistics =
      std::make_shared<MinimalColumnStatistics<ColumnDataType>>(0.0f, new_distinct_count, new_min, new_max);
  return {selectivity, column_statistics};
}

template <typename ColumnDataType>
FilterByValueEstimate HistogramColumnStatistics<ColumnDataType>::estimate_range(const float selectivity,
                                                                                const bool can_prune,
                                                                                const ColumnDataType min,
                                                                                const ColumnDataType max) const {
  // NOTE: minimum can be greater than maximum (e.g. a predicate >= 2 on a column with only values of 1)
  // new minimum/maximum of table cannot be smaller/larger than the current minimum/maximum
  const auto new_min = std::max(min, _histogram->min());
  const auto new_max = std::min(max, _histogram->max());
  const auto new_distinct_count =
      can_prune ? 0.f : _histogram->estimate_distinct_count(PredicateCondition::Between, new_min, new_max);

  auto column_statistics =
      std::make_shared<MinimalColumnStatistics<ColumnDataType>>(0.0f, new_distinct_count, new_min, new_max);
  return {selectivity, column_statistics};
}

template <typename ColumnDataType>
FilterByValueEstimate HistogramColumnStatistics<ColumnDataType>::estimate_range(
    const AllTypeVariant& variant_minimum, const AllTypeVariant& variant_maximum) const {
  const auto minimum = type_cast<ColumnDataType>(variant_minimum);
  const auto maximum = type_cast<ColumnDataType>(variant_maximum);

  const auto can_prune = _histogram->can_prune(PredicateCondition::Between, variant_minimum, variant_maximum);
  const auto selectivity =
      can_prune ? 0.f : _histogram->estimate_selectivity(PredicateCondition::Between, minimum, maximum);

  return estimate_range(selectivity, can_prune, minimum, maximum);
}

template <typename ColumnDataType>
float HistogramColumnStatistics<ColumnDataType>::estimate_distinct_count(
    const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  const auto value = type_cast<ColumnDataType>(variant_value);

  if (predicate_type != PredicateCondition::Between) {
    return _histogram->estimate_distinct_count(predicate_type, value);
  }

  DebugAssert(static_cast<bool>(variant_value2), "Operator BETWEEN should get two parameters, second is missing!");
  const auto value2 = type_cast<ColumnDataType>(*variant_value2);
  return _histogram->estimate_distinct_count(predicate_type, value, value2);
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(HistogramColumnStatistics);

}  // namespace opossum
