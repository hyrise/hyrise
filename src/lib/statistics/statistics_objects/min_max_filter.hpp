#pragma once

#include <iostream>
#include <memory>
#include <optional>

#include "abstract_statistics_object.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Filters are data structures that are primarily used for probabilistic membership queries. In Hyrise, they are
 * typically created on a single segment. They can then be used to check whether a certain value exists in the segment.
 * While histograms also support does_not_contain, their main purpose is not to answer membership queries, but to
 * provide statistics estimations.
 *
 * The MinMaxFilter is a filter that stores the minimum and maximum value for the covered values.
 *
 * MinMaxFilters could be expressed as a single-bin histogram. This is not done for two reasons: First, we like to keep
 * filters and histograms separate, as they serve different purposes. Having histograms both on a per-segment basis
 * (for membership queries) and on a per-column basis (for cardinality estimations) could lead to confusion. Second,
 * building such a histogram would be more expensive than build a MinMaxFilter, as we would have to look at each value
 * instead of only looking at the distinct values (which is significantly cheaper for dictionary encoding).
 */
template <typename T>
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
