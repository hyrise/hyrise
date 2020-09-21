#pragma once

#include "abstract_statistics_object.hpp"

namespace opossum {

// A single float value as an AbstractStatisticsObject, to keep architectures consistent....
class NullValueRatioStatistics : public AbstractStatisticsObject {
 public:
  explicit NullValueRatioStatistics(const float init_ratio);

  std::shared_ptr<AbstractStatisticsObject> sliced(
      const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<AbstractStatisticsObject> scaled(const Selectivity selectivity) const override;

  float ratio;
};

}  // namespace opossum
