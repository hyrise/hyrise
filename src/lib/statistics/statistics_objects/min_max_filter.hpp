#pragma once

#include <memory>
#include <optional>

#include "all_type_variant.hpp"
#include "statistics/abstract_statistics_object.hpp"
#include "types.hpp"

namespace opossum {

/**
 *  Filter that stores a segment's minimum and maximum value
*/
template <typename T>
class MinMaxFilter : public AbstractStatisticsObject {
 public:
  explicit MinMaxFilter(T min, T max);
  ~MinMaxFilter() override = default;

  CardinalityEstimate estimate_cardinality(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                                           const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

  std::shared_ptr<AbstractStatisticsObject> sliced(
      const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<AbstractStatisticsObject> scaled(const Selectivity selectivity) const override;

  bool does_not_contain(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                        const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const;

 protected:
  const T _min;
  const T _max;
};

}  // namespace opossum
