#include "min_max_filter.hpp"

#include <memory>
#include <optional>
#include <utility>

#include "abstract_statistics_object.hpp"
#include "all_type_variant.hpp"
#include "lossless_cast.hpp"
#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
MinMaxFilter<T>::MinMaxFilter(T min, T max) : AbstractStatisticsObject(data_type_from_type<T>()), min(min), max(max) {}

template <typename T>
Cardinality MinMaxFilter<T>::estimate_cardinality(const PredicateCondition predicate_condition,
                                                  const AllTypeVariant& variant_value,
                                                  const std::optional<AllTypeVariant>& variant_value2) const {
  return 0;
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
    default:
      return false;
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(MinMaxFilter);

}  // namespace opossum
