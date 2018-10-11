#pragma once

#include <memory>
#include <optional>
#include <utility>

#include "all_type_variant.hpp"
#include "statistics/abstract_statistics_object.hpp"
#include "type_cast.hpp"
#include "types.hpp"

namespace opossum {

/**
 *  Filter that stores a segment's minimum and maximum value
*/
template <typename T>
class MinMaxFilter : public AbstractStatisticsObject {
 public:
  explicit MinMaxFilter(T min, T max) : _min(min), _max(max) {}
  ~MinMaxFilter() override = default;

  bool does_not_contain(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                        const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const {
    const auto value = type_cast<T>(variant_value);
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
        const auto value2 = type_cast<T>(*variant_value2);
        return value > _max || value2 < _min;
      }
      default:
        return false;
    }
  }

  std::pair<float, bool> estimate_cardinality(
      const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override {
    if (does_not_contain(predicate_type, variant_value, variant_value2)) {
      return {0.f, true};
    } else {
      return {1.f, false};
    }
  }

  std::shared_ptr<AbstractStatisticsObject> slice_with_predicate(
      const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override {
    if (does_not_contain(predicate_type, variant_value, variant_value2)) {
      Fail("NYI - return empty statistics object");
    }

    T min, max;
    const auto value = boost::get<T>(variant_value);

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
        const auto value2 = boost::get<T>(*variant_value2);
        min = value;
        max = value2;
        break;
      }
      default:
        min = _min;
        max = _max;
    }

    return std::make_shared<MinMaxFilter<T>>(min, max);
  }

  std::shared_ptr<AbstractStatisticsObject> scale_with_selectivity(const float selectivity) const override {
    return std::make_shared<MinMaxFilter<T>>(_min, _max);
  }

 protected:
  const T _min;
  const T _max;
};

}  // namespace opossum
