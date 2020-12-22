#include "min_max_filter.hpp"

#include <memory>
#include <optional>
#include <utility>

#include "abstract_statistics_object.hpp"
#include "all_type_variant.hpp"
#include "expression/evaluation/like_matcher.hpp"
#include "lossless_cast.hpp"
#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
MinMaxFilter<T>::MinMaxFilter(T init_min, T init_max)
    : AbstractStatisticsObject(data_type_from_type<T>()), min(init_min), max(init_max) {}

template <typename T>
Cardinality MinMaxFilter<T>::estimate_cardinality(const PredicateCondition predicate_condition,
                                                  const AllTypeVariant& variant_value,
                                                  const std::optional<AllTypeVariant>& variant_value2) const {
  // Theoretically, one could come up with some type of estimation (everything outside MinMax is 0, everything inside
  // is estimated assuming equi-distribution). For that, we would also need the cardinality of the underlying data.
  // Currently, as MinMaxFilters are on a per-segment basis and estimate_cardinality is called for an entire column,
  // there is no use for this.
  Fail("Currently, MinMaxFilters cannot be used to estimate cardinalities");
}

template <typename T>
std::shared_ptr<AbstractStatisticsObject> MinMaxFilter<T>::sliced(
    const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  if (does_not_contain(predicate_condition, variant_value, variant_value2)) {
    return nullptr;
  }

  T sliced_min;
  T sliced_max;
  const auto value = boost::get<T>(variant_value);

  // If value is either sliced_min or max, we do not take the opportunity to slightly improve the new object.
  // We do not know the actual previous/next value, and for strings it's not that simple.
  // The impact should be small.
  switch (predicate_condition) {
    case PredicateCondition::Equals:
      sliced_min = value;
      sliced_max = value;
      break;

    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals:
      sliced_min = min;
      sliced_max = value;
      break;

    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals:
      sliced_min = value;
      sliced_max = max;
      break;

    case PredicateCondition::BetweenInclusive: {
      Assert(variant_value2, "Between operator needs two values.");
      const auto value2 = boost::get<T>(*variant_value2);
      return sliced(PredicateCondition::GreaterThanEquals, value)->sliced(PredicateCondition::LessThanEquals, value2);
    }

    case PredicateCondition::BetweenLowerExclusive: {
      Assert(variant_value2, "Between operator needs two values.");
      const auto value2 = boost::get<T>(*variant_value2);
      return sliced(PredicateCondition::GreaterThan, value)->sliced(PredicateCondition::LessThanEquals, value2);
    }

    case PredicateCondition::BetweenUpperExclusive: {
      Assert(variant_value2, "Between operator needs two values.");
      const auto value2 = boost::get<T>(*variant_value2);
      return sliced(PredicateCondition::GreaterThanEquals, value)->sliced(PredicateCondition::LessThan, value2);
    }

    case PredicateCondition::BetweenExclusive: {
      DebugAssert(variant_value2, "BETWEEN needs a second value.");
      const auto value2 = boost::get<T>(*variant_value2);
      return sliced(PredicateCondition::GreaterThan, value)->sliced(PredicateCondition::LessThan, value2);
    }

    default:
      sliced_min = min;
      sliced_max = max;
  }

  const auto filter = std::make_shared<MinMaxFilter<T>>(sliced_min, sliced_max);
  return filter;
}

template <typename T>
std::shared_ptr<AbstractStatisticsObject> MinMaxFilter<T>::scaled(const Selectivity /*selectivity*/) const {
  return std::make_shared<MinMaxFilter<T>>(min, max);
}

template <typename T>
bool MinMaxFilter<T>::does_not_contain(const PredicateCondition predicate_condition,
                                       const AllTypeVariant& variant_value,
                                       const std::optional<AllTypeVariant>& variant_value2) const {
  // Early exit for NULL variants.
  if (variant_is_null(variant_value)) {
    return false;
  }

  // We expect the caller (e.g., the ChunkPruningRule) to handle type-safe conversions. Boost will throw an exception
  // if this was not done.
  const auto value = boost::get<T>(variant_value);

  // Operators work as follows: value_from_table <operator> value
  // e.g. OpGreaterThan: value_from_table > value
  // thus we can exclude chunk if value >= max since then no value from the table can be greater than value
  switch (predicate_condition) {
    case PredicateCondition::GreaterThan:
      return value >= max;
    case PredicateCondition::GreaterThanEquals:
      return value > max;
    case PredicateCondition::LessThan:
      return value <= min;
    case PredicateCondition::LessThanEquals:
      return value < min;
    case PredicateCondition::Equals:
      return value < min || value > max;
    case PredicateCondition::NotEquals:
      return value == min && value == max;
    case PredicateCondition::BetweenInclusive: {
      Assert(variant_value2, "Between operator needs two values.");
      const auto value2 = boost::get<T>(*variant_value2);
      // Examples for a MinMaxFilter that holds values [7, 10]:
      // does_not_contain [5, 7] -> no condition matches as 7 is both in the MinMaxFilter and the predicate
      // does_not_contain [5, 6] -> second condition (6 < 7) matches
      // does_not_contain [10, 12] -> no condition matches as 10 overlaps
      // does_not_contain [11, 12] -> first condition matches
      return value > max || value2 < min;
    }
    case PredicateCondition::BetweenLowerExclusive: {
      Assert(variant_value2, "Between operator needs two values.");
      const auto value2 = boost::get<T>(*variant_value2);
      // Examples for a MinMaxFilter that holds values [7, 10]:
      // does_not_contain (5, 7] -> no condition matches as 7 is both in the MinMaxFilter and the predicate
      // does_not_contain (5, 6] -> second condition (6 < 7) matches
      // does_not_contain (10, 12] -> first condition matches - 10 is both the max of the filter and the lower bound
      //                              but the lower bound is excluded
      // does_not_contain (11, 12] -> first condition matches
      return value >= max || value2 < min;
    }
    case PredicateCondition::BetweenUpperExclusive: {
      Assert(variant_value2, "Between operator needs two values.");
      const auto value2 = boost::get<T>(*variant_value2);
      return value > max || value2 <= min;
    }
    case PredicateCondition::BetweenExclusive: {
      Assert(variant_value2, "Between operator needs two values.");
      const auto value2 = boost::get<T>(*variant_value2);
      return value >= max || value2 <= min;
    }
    case PredicateCondition::Like: {  // NOLINTNEXTLINE - clang-tidy doesn't like the else path of the if constexpr
      // We use the ascii collation for min/max filters. This means that lower case letters are considered larger than
      // upper case letters. This can lead to a situation, where the USA% (e.g., JOB query XXXXX) is not pruned
      // whenever just a single value starts with a lower case letter.
      // Examples for the handling of Like predicate:
      //                        | test%         | %test   | test\x7F% | test           | '' (empty string)
      // LikeMatcher::bounds()  | {test, tesu}  | nullopt | nullopt   | {test, test\0} | {'', '\0'}
      // does_not_contain(Like) | max < test or | false   | false     | max < test or  | max < '' or
      //                        | min >= tesu   |         |           | min >= test\0  | min >= '\0'
      if constexpr (std::is_same_v<T, pmr_string>) {
        const auto bounds = LikeMatcher::bounds(value);
        if (!bounds) return false;

        const auto [lower_bound, upper_bound] = *bounds;

        return max < lower_bound || upper_bound <= min;
      }

      return false;
    }
    case PredicateCondition::NotLike: {  // NOLINTNEXTLINE - clang-tidy doesn't like the else path of the if constexpr
      // Examples for the handling of NotLike predicate:
      //                          | test%           | %test   | test\x7F% | test             | '' (empty string)
      // LikeMatcher::bounds()    | {test, tesu}    | nullopt | nullopt   | {test, test\0}   | {'', '\0'}
      // does_not_contain(NotLike)| min >= test and | false   | false     | min >= test and  | min >= '\0' and
      //                          | max < tesu      |         |           | max < test\0     | max < '\0'
      if constexpr (std::is_same_v<T, pmr_string>) {
        const auto bounds = LikeMatcher::bounds(value);
        if (!bounds) return false;

        const auto [lower_bound, upper_bound] = *bounds;

        return max < upper_bound && lower_bound <= min;
      }

      return false;
    }
    default:
      return false;
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(MinMaxFilter);

}  // namespace opossum
