#pragma once

#include <memory>

#include "abstract_statistics_object.hpp"
#include "types.hpp"

namespace hyrise {

// For consistency with other statistics, also single values are wrapped by an AbstractStatisticsObject.
class NullValueRatioStatistics : public AbstractStatisticsObject,
                                 public std::enable_shared_from_this<NullValueRatioStatistics> {
 public:
  explicit NullValueRatioStatistics(const Selectivity init_ratio);

  constexpr DataType data_type() const override;

  std::shared_ptr<const AbstractStatisticsObject> sliced(
      const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<const AbstractStatisticsObject> scaled(const Selectivity selectivity) const override;

  std::shared_ptr<const AbstractStatisticsObject> pruned(
      const size_t num_values_pruned, const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2) const override;

  Selectivity ratio{0};
};

}  // namespace hyrise
