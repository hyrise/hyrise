#pragma once

#include <iostream>
#include <memory>
#include <optional>

#include "abstract_statistics_object.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Filter that stores a minimum and maximum value for a number of rows.
 * MinMaxFilters are typically created for a single chunk and can be used to check whether
 * a certain value exists in the segment.
*/
template <typename T>
// TODO @Bouncner: Could you clarify the "Filter" terminology? What makes a filter a filter? Histograms can be used
// for pruning, too.
// TODO @Bouncner: Why do we have MinMaxFilter/RangeFilter if we could also create a histogram? Are these cheaper to
// store or significantly cheaper to create?
class MinMaxFilter : public AbstractStatisticsObject {
 public:
  explicit MinMaxFilter(T init_min, T init_max);

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
