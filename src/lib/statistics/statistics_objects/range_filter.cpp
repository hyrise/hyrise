#include "range_filter.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "abstract_statistics_object.hpp"
#include "resolve_type.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
RangeFilter<T>::RangeFilter(std::vector<std::pair<T, T>> init_ranges)
    : AbstractStatisticsObject(data_type_from_type<T>()), ranges(std::move(init_ranges)) {
  DebugAssert(!ranges.empty(), "Cannot construct empty RangeFilter");
}

template <typename T>
Cardinality RangeFilter<T>::estimate_cardinality(const PredicateCondition predicate_condition,
                                                 const AllTypeVariant& variant_value,
                                                 const std::optional<AllTypeVariant>& variant_value2) const {
  // Theoretically, one could come up with some type of estimation (everything outside the range is 0, everything inside
  // is estimated assuming equi-distribution). For that, we would also need the cardinality of the underlying data.
  // Currently, as RangeFilters are on a per-segment basis and estimate_cardinality is called for an entire column,
  // there is no use for this.
  Fail("Currently, RangeFilters cannot be used to estimate cardinalities");
}

template <typename T>
std::shared_ptr<AbstractStatisticsObject> RangeFilter<T>::sliced(
    const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  if (does_not_contain(predicate_condition, variant_value, variant_value2)) {
    return nullptr;
  }

  std::vector<std::pair<T, T>> sliced_ranges;
  const auto value = boost::get<T>(variant_value);

  // If value is on range edge, we do not take the opportunity to slightly improve the new object.
  // The impact should be small.
  switch (predicate_condition) {
    case PredicateCondition::Equals:
      return std::make_shared<MinMaxFilter<T>>(value, value);
    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals: {
      auto end_it = std::lower_bound(ranges.cbegin(), ranges.cend(), value,
                                     [](const auto& a, const auto& b) { return a.second < b; });

      // Copy all the ranges before the value.
      auto it = ranges.cbegin();
      for (; it != end_it; it++) {
        sliced_ranges.emplace_back(*it);
      }

      DebugAssert(it != ranges.cend(), "does_not_contain() should have caught that.");

      // If value is not in a gap, limit the last range's upper bound to value.
      if (value >= it->first) {
        sliced_ranges.emplace_back(std::pair<T, T>{it->first, value});
      }
    } break;
    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals: {
      auto it = std::lower_bound(ranges.cbegin(), ranges.cend(), value,
                                 [](const auto& a, const auto& b) { return a.second < b; });

      DebugAssert(it != ranges.cend(), "does_not_contain() should have caught that.");

      // If value is in a gap, use the next range, otherwise limit the next range's upper bound to value.
      if (value <= it->first) {
        sliced_ranges.emplace_back(*it);
      } else {
        sliced_ranges.emplace_back(std::pair<T, T>{value, it->second});
      }
      it++;

      // Copy all following ranges.
      for (; it != ranges.cend(); it++) {
        sliced_ranges.emplace_back(*it);
      }
    } break;

    case PredicateCondition::BetweenInclusive: {
      DebugAssert(variant_value2, "Between needs a second value.");
      const auto value2 = boost::get<T>(*variant_value2);
      return sliced(PredicateCondition::GreaterThanEquals, value)->sliced(PredicateCondition::LessThanEquals, value2);
    }
    case PredicateCondition::BetweenLowerExclusive: {
      DebugAssert(variant_value2, "Between needs a second value.");
      const auto value2 = boost::get<T>(*variant_value2);
      return sliced(PredicateCondition::GreaterThanEquals, value)->sliced(PredicateCondition::LessThan, value2);
    }
    case PredicateCondition::BetweenUpperExclusive: {
      DebugAssert(variant_value2, "Between needs a second value.");
      const auto value2 = boost::get<T>(*variant_value2);
      return sliced(PredicateCondition::GreaterThan, value)->sliced(PredicateCondition::LessThanEquals, value2);
    }
    case PredicateCondition::BetweenExclusive: {
      DebugAssert(variant_value2, "Between needs a second value.");
      const auto value2 = boost::get<T>(*variant_value2);
      return sliced(PredicateCondition::GreaterThan, value)->sliced(PredicateCondition::LessThan, value2);
    }

    default:
      sliced_ranges = ranges;
  }

  DebugAssert(!sliced_ranges.empty(), "As does_not_contain was false, the sliced_ranges should not be empty");

  return std::make_shared<RangeFilter<T>>(sliced_ranges);
}

template <typename T>
std::shared_ptr<AbstractStatisticsObject> RangeFilter<T>::scaled(const Selectivity /*selectivity*/) const {
  return std::make_shared<RangeFilter<T>>(ranges);
}

template <typename T>
std::unique_ptr<RangeFilter<T>> RangeFilter<T>::build_filter(const pmr_vector<T>& dictionary,
                                                             uint32_t max_ranges_count) {
  // See #1536
  static_assert(std::is_arithmetic_v<T>, "Range filters are only allowed on arithmetic types.");

  DebugAssert(max_ranges_count > 0, "Number of ranges to create needs to be larger zero.");
  DebugAssert(std::is_sorted(dictionary.begin(), dictionary.cend()), "Dictionary must be sorted in ascending order.");

  if (dictionary.empty()) {
    // Empty dictionaries will, e.g., occur in segments with only NULLs - or empty segments.
    return nullptr;
  }

  if (dictionary.size() == 1) {
    std::vector<std::pair<T, T>> ranges;
    ranges.emplace_back(dictionary.front(), dictionary.front());
    return std::make_unique<RangeFilter<T>>(std::move(ranges));
  }

  /*
  * The code to determine the range boundaries requires calculating the distances between values. We cannot express the
  * distance between -DBL_MAX and DBL_MAX using any of the standard data types. For these cases, the RangeFilter 
  * effectively degrades to a MinMaxFilter (i.e., stores only a single range).
  * While being rather unlikely for doubles, it's more likely to happen when Hyrise includes tinyint etc.
  * std::make_unsigned<T>::type would be possible to use for signed int types, but not for floating types.
  * Approach: take the min and max values and simply check if the distance between both might overflow.
  */
  const auto min_max = std::minmax_element(dictionary.cbegin(), dictionary.cend());
  if ((*min_max.first < 0) && (*min_max.second > std::numeric_limits<T>::max() + *min_max.first)) {
    return std::make_unique<RangeFilter<T>>(std::vector<std::pair<T, T>>{{*min_max.first, *min_max.second}});
  }

  // Running example:
  // Input dictionary: 2 3 5 8 9 11
  // max_ranges_count: 3
  //
  // Steps:
  // 1. Calculate the distance from each value in the (sorted) dictionary to the next value and store a pair that
  //    contains (a) the distance to the next value in the dictionary and (b) the entry's index in the dictionary:
  //    (1,0) (2,1) (3,2) (1,3) (2,3)
  // 2. Sort the vector in descending order by the size of the gaps (might be an unstable sort):
  //    (3,2) (2,1) (2,3) (1,0) (1,3)
  // 3. Shorten the vector to containt the `max_ranges_count - 1` biggest gaps
  //    (3,2) (2,1)
  // 4. Restore the original order of the dictionary by sorting on the second field
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

  // 1. Calculate the distance from each value in the (sorted) dictionary to the next value
  std::vector<std::pair<T, size_t>> distances;
  distances.reserve(dictionary.size() - 1);
  for (auto dict_it = dictionary.cbegin(); dict_it + 1 != dictionary.cend(); ++dict_it) {
    auto dict_it_next = dict_it + 1;
    distances.emplace_back(*dict_it_next - *dict_it, std::distance(dictionary.cbegin(), dict_it));
  }

  // 2. Sort the vector in descending order by the size of the gaps
  std::sort(distances.begin(), distances.end(),
            [](const auto& pair1, const auto& pair2) { return pair1.first > pair2.first; });

  // 3. Shorten the vector to containt the `max_ranges_count - 1` biggest gaps
  if ((max_ranges_count - 1) < distances.size()) {
    distances.resize(max_ranges_count - 1);
  }

  // 4. Restore the original order of the dictionary by sorting on the second field
  std::sort(distances.begin(), distances.end(),
            [](const auto& pair1, const auto& pair2) { return pair1.second < pair2.second; });

  // 5. Add the highest value in the dictionary (more correctly, its index)
  distances.emplace_back(T{}, dictionary.size() - 1);

  // 6. Construct ranges
  std::vector<std::pair<T, T>> ranges;
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
  if (variant_is_null(variant_value) || (variant_value2.has_value() && variant_is_null(variant_value2.value())) ||
      predicate_condition == PredicateCondition::IsNull || predicate_condition == PredicateCondition::IsNotNull) {
    return false;
  }

  // We expect the caller (e.g., the ChunkPruningRule) to handle type-safe conversions. Boost will throw an exception
  // if this was not done.
  const auto value = boost::get<T>(variant_value);

  // Operators work as follows: value_from_table <operator> value
  // e.g. OpGreaterThan: value_from_table > value
  // thus we can exclude chunk if value >= _max since then no value from the table can be greater than value
  switch (predicate_condition) {
    case PredicateCondition::GreaterThan: {
      auto& max = ranges.back().second;
      return value >= max;
    }
    case PredicateCondition::GreaterThanEquals: {
      auto& max = ranges.back().second;
      return value > max;
    }
    case PredicateCondition::LessThan: {
      auto& min = ranges.front().first;
      return value <= min;
    }
    case PredicateCondition::LessThanEquals: {
      auto& min = ranges.front().first;
      return value < min;
    }
    case PredicateCondition::Equals: {
      for (const auto& bounds : ranges) {
        const auto& [min, max] = bounds;

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
      /* There are two scenarios where a between predicate can be pruned:
       *    - both bounds are "outside" (not spanning) the segment's value range (i.e., either both are smaller than
       *      the minimum or both are larger than the maximum
       *    - both bounds are within the same gap
       */

      Assert(variant_value2.has_value(), "Between operator needs two values.");
      const auto value2 = boost::get<T>(*variant_value2);

      // a BETWEEN 5 AND 4 will always be empty
      if (value2 < value) return true;

      // Smaller than the segment's minimum.
      if (does_not_contain(PredicateCondition::LessThanEquals, std::max(value, value2))) {
        return true;
      }

      // Larger than the segment's maximum.
      if (does_not_contain(PredicateCondition::GreaterThanEquals, std::min(value, value2))) {
        return true;
      }

      const auto range_comp = [](const std::pair<T, T> range, const T compare_value) {
        return range.second < compare_value;
      };
      // Get value range or next larger value range if searched value is in a gap.
      const auto start_lower = std::lower_bound(ranges.cbegin(), ranges.cend(), value, range_comp);
      const auto end_lower = std::lower_bound(ranges.cbegin(), ranges.cend(), value2, range_comp);

      const bool start_in_value_range =
          (start_lower != ranges.cend()) && (*start_lower).first <= value && value <= (*start_lower).second;
      const bool end_in_value_range =
          (end_lower != ranges.cend()) && (*end_lower).first <= value2 && value2 <= (*end_lower).second;

      return !start_in_value_range && !end_in_value_range && start_lower == end_lower;
    }
    default:
      return false;
  }
}

template class RangeFilter<int32_t>;
template class RangeFilter<int64_t>;
template class RangeFilter<float>;
template class RangeFilter<double>;

}  // namespace opossum
