#pragma once

#include "all_type_variant.hpp"
#include "type_cast.hpp"
#include "types.hpp"

namespace opossum {

/**
 *  Filter that stores a columns minimum and maximum value
*/
template <typename T>
class MinMaxFilter : public AbstractFilter {
 public:
  explicit MinMaxFilter(T min, T max) : _min(min), _max(max) {}
  ~MinMaxFilter() override = default;

  bool can_prune(const AllTypeVariant& value, const PredicateCondition predicate_type) const override {
    const auto t_value = boost::get<T>(value);
    // Operators work as follows: value_from_table <operator> t_value
    // e.g. OpGreaterThan: value_from_table > t_value
    // thus we can exclude chunk if t_value >= _max since then no value from the table can be greater than t_value
    switch (predicate_type) {
      case PredicateCondition::GreaterThan:
        return t_value >= _max;
      case PredicateCondition::GreaterThanEquals:
        return t_value > _max;
      case PredicateCondition::LessThan:
        return t_value <= _min;
      case PredicateCondition::LessThanEquals:
        return t_value < _min;
      case PredicateCondition::Equals:
        return t_value < _min || t_value > _max;
      default:
        return false;
    }
  }

 protected:
  const T _min;
  const T _max;
};

}  // namespace opossum
