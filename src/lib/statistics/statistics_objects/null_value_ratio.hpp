#pragma once

#include "statistics/abstract_statistics_object.hpp"

namespace opossum {

// A single float value as an AbstractStatisticsObject, to keep architectures consistent....
class NullValueRatio : public AbstractStatisticsObject {
 public:
  explicit NullValueRatio(const float null_value_ratio);

  CardinalityEstimate estimate_cardinality(
      const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<AbstractStatisticsObject> sliced(
      const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<AbstractStatisticsObject> scaled(const Selectivity selectivity) const override;

  float null_value_ratio;
};

}  // namespace opossum
