#pragma once

#include <iostream>
#include <memory>
#include <optional>

#include "abstract_statistics_object.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

/**
 *  Filter that stores a segment's minimum and maximum value
*/
template <typename T>
class MinMaxFilter : public AbstractStatisticsObject {
 public:
  explicit MinMaxFilter(T init_min, T init_max);
  ~MinMaxFilter() override = default;

  // cppcoreguidelines-special-member-functions
  MinMaxFilter(const MinMaxFilter& other) = default;
  MinMaxFilter(MinMaxFilter&& other) = default;
  MinMaxFilter& operator=(const MinMaxFilter& other) = default;
  MinMaxFilter& operator=(MinMaxFilter&& other) = default;

  Cardinality estimate_cardinality(const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
                                   const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

  std::shared_ptr<AbstractStatisticsObject> sliced(
      const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<AbstractStatisticsObject> scaled(const Selectivity selectivity) const override;

  bool does_not_contain(const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
                        const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

  const T min;
  const T max;
};

template <typename T>
std::ostream& operator<<(std::ostream& stream, const MinMaxFilter<T>& filter) {
  stream << "{" << filter.min << " " << filter.max << "}";
  return stream;
}

}  // namespace opossum
