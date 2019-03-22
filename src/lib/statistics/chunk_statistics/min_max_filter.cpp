#include "min_max_filter.hpp"

#include "boost/variant.hpp"

namespace opossum {

template <typename T>
MinMaxFilter<T>::MinMaxFilter(T min, T max) : _min(min), _max(max) {}

template <typename T>
bool MinMaxFilter<T>::can_prune(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                                const std::optional<AllTypeVariant>& variant_value2) const {
  // Early exit for NULL variants.
  if (variant_is_null(variant_value)) {
    return false;
  }

  const auto value = boost::get<T>(variant_value);

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
      const auto value2 = boost::get<T>(*variant_value2);
      return value > _max || value2 < _min;
    }
    default:
      return false;
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(MinMaxFilter);

}  // namespace opossum
