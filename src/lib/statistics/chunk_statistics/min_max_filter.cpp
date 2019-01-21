#include "min_max_filter.hpp"

#include <memory>
#include <optional>
#include <utility>

#include "all_type_variant.hpp"
#include "resolve_type.hpp"
#include "statistics/abstract_statistics_object.hpp"
#include "statistics/empty_statistics_object.hpp"
#include "type_cast.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
MinMaxFilter<T>::MinMaxFilter(T min, T max)
    : AbstractStatisticsObject(data_type_from_type<T>()), _min(min), _max(max) {}

template <typename T>
CardinalityEstimate MinMaxFilter<T>::estimate_cardinality(const PredicateCondition predicate_type,
                                                          const AllTypeVariant& variant_value,
                                                          const std::optional<AllTypeVariant>& variant_value2) const {
  if (_does_not_contain(predicate_type, variant_value, variant_value2)) {
    return {Cardinality{0}, EstimateType::MatchesNone};
  } else {
    return {Cardinality{0}, EstimateType::MatchesApproximately};
  }
}

template <typename T>
std::shared_ptr<AbstractStatisticsObject> MinMaxFilter<T>::sliced_with_predicate(
    const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  if (_does_not_contain(predicate_type, variant_value, variant_value2)) {
    return std::make_shared<EmptyStatisticsObject>(data_type);
  }

  T min, max;
  const auto value = type_cast_variant<T>(variant_value);

  // If value is either _min or _max, we do not take the opportunity to slightly improve the new object.
  // We do not know the actual previous/next value, and for strings it's not that simple.
  // The impact should be small.
  switch (predicate_type) {
    case PredicateCondition::Equals:
      min = value;
      max = value;
      break;
    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals:
      min = _min;
      max = value;
      break;
    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals:
      min = value;
      max = _max;
      break;
    case PredicateCondition::Between: {
      DebugAssert(variant_value2, "BETWEEN needs a second value.");
      const auto value2 = type_cast_variant<T>(*variant_value2);
      return sliced_with_predicate(PredicateCondition::GreaterThanEquals, value)
          ->sliced_with_predicate(PredicateCondition::LessThanEquals, value2);
    }
    default:
      min = _min;
      max = _max;
  }

  const auto filter = std::make_shared<MinMaxFilter<T>>(min, max);
  filter->is_derived_from_complete_chunk = is_derived_from_complete_chunk;
  return filter;
}

template <typename T>
std::shared_ptr<AbstractStatisticsObject> MinMaxFilter<T>::scaled_with_selectivity(const float /*selectivity*/) const {
  const auto filter = std::make_shared<MinMaxFilter<T>>(_min, _max);
  filter->is_derived_from_complete_chunk = is_derived_from_complete_chunk;
  return filter;
}

template <typename T>
bool MinMaxFilter<T>::_does_not_contain(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                                        const std::optional<AllTypeVariant>& variant_value2) const {
  // Early exit for NULL variants.
  if (variant_is_null(variant_value)) {
    return false;
  }

  const auto value = type_cast_variant<T>(variant_value);

  // Operators work as follows: value_from_table <operator> value
  // e.g. OpGreaterThan: value_from_table > value
  // thus we can exclude chunk if value >= _max since then no value from the table can be greater than value
  switch (predicate_type) {
    case PredicateCondition::GreaterThan:
      return value >= _max;
    case PredicateCondition::GreaterThanEquals:
      return value > _max;
    case PredicateCondition::LessThan:
      return value <= _min;
    case PredicateCondition::LessThanEquals:
      return value < _min;
    case PredicateCondition::Equals:
      return value < _min || value > _max;
    case PredicateCondition::NotEquals:
      return value == _min && value == _max;
    case PredicateCondition::Between: {
      Assert(static_cast<bool>(variant_value2), "Between operator needs two values.");
      const auto value2 = type_cast_variant<T>(*variant_value2);
      return value > _max || value2 < _min;
    }
    default:
      return false;
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(MinMaxFilter);

}  // namespace opossum
