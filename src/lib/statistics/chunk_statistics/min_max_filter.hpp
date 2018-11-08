#pragma once

#include "abstract_filter.hpp"
#include "all_type_variant.hpp"
#include "type_cast.hpp"
#include "types.hpp"

namespace opossum {

/**
 *  Filter that stores a segment's minimum and maximum value
*/
template <typename T>
class MinMaxFilter : public AbstractFilter {
 public:
  explicit MinMaxFilter(T min, T max) : _min(min), _max(max) {}
  ~MinMaxFilter() override = default;

  bool can_prune(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                 const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override {
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

 protected:
  const T _min;
  const T _max;
};

}  // namespace opossum
