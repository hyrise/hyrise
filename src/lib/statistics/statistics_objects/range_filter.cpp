#include "range_filter.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "abstract_statistics_object.hpp"
#include "all_type_variant.hpp"
#include "resolve_type.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

template <typename T>
RangeFilter<T>::RangeFilter(std::vector<std::pair<T, T>> init_ranges)
    : AbstractStatisticsObject(data_type_from_type<T>()), ranges(std::move(init_ranges)) {
  DebugAssert(!ranges.empty(), "Cannot construct empty RangeFilter.");
}

template <typename T>
Cardinality RangeFilter<T>::estimate_cardinality(const PredicateCondition /*predicate_condition*/,
                                                 const AllTypeVariant& /*variant_value*/,
                                                 const std::optional<AllTypeVariant>& /*variant_value2*/) const {
  // Theoretically, one could come up with some type of estimation (everything outside the range is 0, everything inside
  // is estimated assuming equi-distribution). For that, we would also need the cardinality of the underlying data.
  // Currently, as RangeFilters are on a per-segment basis and estimate_cardinality is called for an entire column,
  // there is no use for this.
  Fail("Currently, RangeFilters cannot be used to estimate cardinalities.");
}

template <typename T>
std::shared_ptr<const AbstractStatisticsObject> RangeFilter<T>::sliced(
    const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  if (does_not_contain(predicate_condition, variant_value, variant_value2)) {
    return nullptr;
  }

  auto sliced_ranges = std::vector<std::pair<T, T>>{};
  const auto value = boost::get<T>(variant_value);

  // If value is on range edge, we do not take the opportunity to slightly improve the new object.
  // The impact should be small.
  switch (predicate_condition) {
    case PredicateCondition::Equals:
      return std::make_shared<MinMaxFilter<T>>(value, value);
    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals: {
      const auto end_iter =
          std::lower_bound(ranges.cbegin(), ranges.cend(), value, [](const auto& range, const auto search_value) {
            return range.second < search_value;
          });

      // Copy all the ranges before the value.
      auto iter = ranges.cbegin();
      for (; iter != end_iter; ++iter) {
        sliced_ranges.emplace_back(*iter);
      }

      // If value is not in a gap, limit the last range's upper bound to value.
      if (iter != ranges.cend() && value >= iter->first) {
        sliced_ranges.emplace_back(iter->first, value);
      }
    } break;
    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals: {
      auto iter =
          std::lower_bound(ranges.cbegin(), ranges.cend(), value, [](const auto& range, const auto search_value) {
            return range.second < search_value;
          });
      DebugAssert(iter != ranges.cend(), "does_not_contain() should have caught that.");

      // If value is in a gap, use the next range, otherwise limit the next range's upper bound to value.
      if (value <= iter->first) {
        sliced_ranges.emplace_back(*iter);
      } else {
        sliced_ranges.emplace_back(value, iter->second);
      }
      ++iter;

      // Copy all following ranges.
      for (; iter != ranges.cend(); ++iter) {
        sliced_ranges.emplace_back(*iter);
      }
    } break;

    case PredicateCondition::BetweenInclusive:
    case PredicateCondition::BetweenLowerExclusive:
    case PredicateCondition::BetweenUpperExclusive:
    case PredicateCondition::BetweenExclusive: {
      DebugAssert(variant_value2, "Between needs a second value.");
      const auto lower_predicate_condition = is_lower_inclusive_between(predicate_condition)
                                                 ? PredicateCondition::GreaterThanEquals
                                                 : PredicateCondition::GreaterThan;
      const auto upper_predicate_condition = is_upper_inclusive_between(predicate_condition)
                                                 ? PredicateCondition::LessThanEquals
                                                 : PredicateCondition::LessThan;
      return sliced(lower_predicate_condition, value)->sliced(upper_predicate_condition, *variant_value2);
    }

    default:
      sliced_ranges = ranges;
  }

  DebugAssert(!sliced_ranges.empty(), "As does_not_contain() was false, the sliced_ranges should not be empty.");
  return std::make_shared<RangeFilter<T>>(sliced_ranges);
}

template <typename T>
std::shared_ptr<const AbstractStatisticsObject> RangeFilter<T>::scaled(const Selectivity /*selectivity*/) const {
  return this->shared_from_this();
}

template <typename T>
std::unique_ptr<RangeFilter<T>> RangeFilter<T>::build_filter(const pmr_vector<T>& dictionary,
                                                             uint32_t max_ranges_count) {
  // See #1536
  static_assert(std::is_arithmetic_v<T>, "Range filters are only allowed on arithmetic types.");
  DebugAssert(max_ranges_count > 0, "Number of ranges to create needs to be larger zero.");

  // Empty dictionaries will, e.g., occur in segments with only NULLs - or empty segments.
  if (dictionary.empty()) {
    return nullptr;
  }

  if (dictionary.size() == 1) {
    auto ranges = std::vector<std::pair<T, T>>{{dictionary.front(), dictionary.front()}};
    return std::make_unique<RangeFilter<T>>(std::move(ranges));
  }

  /**
   * The code to determine the range boundaries requires calculating the distances between values. We cannot express the
   * distance between -DBL_MAX and DBL_MAX using any of the standard data types. For these cases, the RangeFilter
   * effectively degrades to a MinMaxFilter (i.e., stores only a single range).
   * While being rather unlikely for doubles, it's more likely to happen when Hyrise includes tinyint etc.
   * std::make_unsigned<T>::type would be possible to use for signed int types, but not for floating types.
   * Approach: take the min and max values and simply check if the distance between both might overflow.
   */
  static_assert(std::is_signed_v<T>, "Expected a signed arithmetic type.");
  DebugAssert(std::is_sorted(dictionary.cbegin(), dictionary.cend()), "Dictionary must be sorted in ascending order.");
  const auto min = dictionary.front();
  const auto max = dictionary.back();
  // max > std::numeric_limits<T>::max() + min would be the mathematically correct assumption. However, it only works
  // for integral types. For floating-point types, the precision is not high enough to differentiate, e.g., max - 1 from
  // max. While in theory sacrificing one value, we account for this imprecision.
  if ((min < 0) && (max >= std::numeric_limits<T>::max() + min)) {
    return std::make_unique<RangeFilter<T>>(std::vector<std::pair<T, T>>{{min, max}});
  }

  // Running example:
  // Input dictionary: 2 3 5 8 9 11
  // max_ranges_count: 3
  //
  // Steps:
  // 1. Calculate the distance from each value in the (sorted) dictionary to the next value and store a pair that
  //    contains (a) the distance to the next value in the dictionary and (b) the entry's index in the dictionary:
  //    (1,0) (2,1) (3,2) (1,3) (2,3)
  // 2. Sort the vector in descending order by the size of the gaps (might be an unstable sort).
  //    (3,2) (2,1) (2,3) (1,0) (1,3)
  // 3. Shorten the vector to contain the `max_ranges_count - 1` biggest gaps.
  //    (3,2) (2,1)
  // 4. Restore the original order of the dictionary by sorting on the second field.
  //    (2,1) (3,2)
  // 5. Add the highest value in the dictionary (more correctly, its index). We will not need the distance for this
  //    entry, so it is initialized with T{}.
  //    (3,2) (2,1) (0,5)
  // 6. Construct ranges. The first range starts at the dictionary's first value and goes until the dictionary index
  //    at which the first gap begins. The next range starts at the first dictionary value after the gap (i.e., the
  //    current dictionary index plus one) and goes until the beginning of the next gap.
  //    First range:      dictionary[0]   until dictionary[1]
  //    Second range:     dictionary[1+1] until dictionary[2]
  //    Third range:      dictionary[2+1] until dictionary[5]
  //    Result as values: [2, 3] [5, 5] [8, 11]

  // 1. Calculate the distance from each value in the (sorted) dictionary to the next value.
  auto distances = std::vector<std::pair<T, size_t>>{};
  distances.reserve(dictionary.size() - 1);
  for (auto dict_it = dictionary.cbegin(); dict_it + 1 != dictionary.cend(); ++dict_it) {
    const auto dict_it_next = dict_it + 1;
    distances.emplace_back(*dict_it_next - *dict_it, std::distance(dictionary.cbegin(), dict_it));
  }

  // 2. Sort the vector in descending order by the size of the gaps.
  std::sort(distances.begin(), distances.end(), [](const auto& pair1, const auto& pair2) {
    return pair1.first > pair2.first;
  });

  // 3. Shorten the vector to containt the `max_ranges_count - 1` biggest gaps
  if ((max_ranges_count - 1) < distances.size()) {
    distances.resize(max_ranges_count - 1);
  }

  // 4. Restore the original order of the dictionary by sorting on the second field.
  std::sort(distances.begin(), distances.end(), [](const auto& pair1, const auto& pair2) {
    return pair1.second < pair2.second;
  });

  // 5. Add the highest value in the dictionary (more correctly, its index).
  distances.emplace_back(T{}, dictionary.size() - 1);

  // 6. Construct ranges.
  auto ranges = std::vector<std::pair<T, T>>{};
  ranges.reserve(distances.size());
  auto range_start_index = size_t{0};
  for (const auto& distance_index_pair : distances) {
    const auto range_end_index = distance_index_pair.second;
    ranges.emplace_back(dictionary[range_start_index], dictionary[range_end_index]);
    range_start_index = range_end_index + 1;
  }

  return std::make_unique<RangeFilter<T>>(std::move(ranges));
}

template <typename T>
bool RangeFilter<T>::does_not_contain(const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
                                      const std::optional<AllTypeVariant>& variant_value2) const {
  // Early exit for NULL-checking predicates and NULL variants. Predicates with one or more variant parameter being NULL
  // are not prunable. Malformed predicates such as can_prune(PredicateCondition::LessThan, {5}, NULL_VALUE) are not
  // pruned either. While this call might be considered nonsensical (everything compared to NULL is null), we do not
  // require callers to identify these circumstances.
  if (variant_is_null(variant_value) || (variant_value2 && variant_is_null(*variant_value2)) ||
      predicate_condition == PredicateCondition::IsNull || predicate_condition == PredicateCondition::IsNotNull) {
    return false;
  }

  // We expect the caller (e.g., the ChunkPruningRule) to handle type-safe conversions. Boost will throw an exception
  // if this was not done.
  const auto value = boost::get<T>(variant_value);

  switch (predicate_condition) {
    case PredicateCondition::GreaterThan: {
      return value >= ranges.back().second;
    }
    case PredicateCondition::GreaterThanEquals: {
      return value > ranges.back().second;
    }
    case PredicateCondition::LessThan: {
      return value <= ranges.front().first;
    }
    case PredicateCondition::LessThanEquals: {
      return value < ranges.front().first;
    }
    case PredicateCondition::Equals: {
      for (const auto& [min, max] : ranges) {
        if (value >= min && value <= max) {
          return false;
        }
      }
      return true;
    }
    case PredicateCondition::NotEquals: {
      return ranges.size() == 1 && ranges.front().first == value && ranges.front().second == value;
    }
    case PredicateCondition::BetweenInclusive:
    case PredicateCondition::BetweenLowerExclusive:
    case PredicateCondition::BetweenUpperExclusive:
    case PredicateCondition::BetweenExclusive: {
      // There are two scenarios where a between predicate can be pruned:
      //     - Both bounds are "outside" (not spanning) the segment's value range (i.e., either both are smaller than
      //       the minimum or both are larger than the maximum).
      //     - Both bounds are within the same gap.

      Assert(variant_value2, "Between predicate needs two values.");
      const auto value2 = boost::get<T>(*variant_value2);
      const auto is_lower_inclusive = is_lower_inclusive_between(predicate_condition);
      const auto is_upper_inclusive = is_upper_inclusive_between(predicate_condition);

      // "X BETWEEN Y AND Z" is equivalent to "X>=Y AND X<=Z" [SQL-92]. Thus, `a BETWEEN 5 AND 4` will always be empty.
      const auto is_inclusive = is_lower_inclusive && is_upper_inclusive;
      if (is_inclusive ? value2 < value : value2 <= value) {
        return true;
      }

      // For the following code, consider the running example of a RangeFilter with two ranges: [2, 4] [7, 10].
      // The predicate is `a BETWEEN x AND y`.
      // Case (i): y is less than the segment's minimum. If y < 2 (or <= 2 for upper exclusive predicate), the predicate
      //           cannot match.
      const auto min = ranges.front().first;
      const auto is_below_min = is_upper_inclusive_between(predicate_condition) ? (value2 < min) : (value2 <= min);
      if (is_below_min) {
        return true;
      }

      // Case (ii): x is greater than than the segment's maximum. If x > 10 (or >= 10 for lower exclusive predicate),
      //            the predicate cannot match.
      const auto max = ranges.back().second;
      const auto is_above_max = is_lower_inclusive_between(predicate_condition) ? (value > max) : (value >= max);
      if (is_above_max) {
        return true;
      }

      // Case (iii): The predicate can match the segment's values. However, the predicate values can be exactly in a gap
      //             between two ranges (e.g., a BETWEEN 5 AND 6).
      //             We know that y >(=) 2. Thus, we find the range whose minimum is >= y. If we cannot find such a
      //             range, y is greater than the segment's maximum (and thus, in a gap).
      const auto upper_bound_range =
          std::lower_bound(ranges.cbegin(), ranges.cend(), value2, [](const auto& range, const auto compare_value) {
            return range.first < compare_value;
          });

      // If we find a range, y can still be the exact range border (e.g., y = 7). For upper inclusive predicates, there
      // can be matches if this is the case.
      auto upper_bound_in_gap =
          upper_bound_range == ranges.cend() || !is_upper_inclusive || upper_bound_range->first > value2;
      if (!upper_bound_in_gap) {
        return false;
      }

      // If y is in a gap, there must be a previous range since we checked before that y is not smaller than the min.
      Assert(upper_bound_range != ranges.begin(), "Out-of-range between predicate should have been caught before.");

      // Check if x is in the same gap as y. We do this by comparing x with the upper border of the range before the
      // one we found above. If x >= 4 (or > 4 for lower inclusive), the predicate cannot match. Else, x is in the range
      // and the predicate can match.
      // Note that this check also covers the case where y is in a range. E.g., for y = 8, our search for y would point
      // to the ranges' end. We already ensured that x <= y. The previous range is [7, 10], with an upper border greater
      // than x. Thus, there can be matches.
      const auto previous_range = std::prev(upper_bound_range);
      return is_lower_inclusive ? previous_range->second < value : previous_range->second <= value;
    }
    default:
      return false;
  }
}

template class RangeFilter<int32_t>;
template class RangeFilter<int64_t>;
template class RangeFilter<float>;
template class RangeFilter<double>;

}  // namespace hyrise
