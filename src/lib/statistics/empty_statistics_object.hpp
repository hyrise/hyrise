#pragma once

#include <memory>
#include <optional>
#include <utility>

#include "cardinality.hpp"

#include "all_type_variant.hpp"
#include "statistics/abstract_statistics_object.hpp"
#include "types.hpp"

namespace opossum {

class EmptyStatisticsObject : public AbstractStatisticsObject {
 public:
  explicit EmptyStatisticsObject(const DataType data_type);

  CardinalityEstimate estimate_cardinality(
      const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<AbstractStatisticsObject> slice_with_predicate(
      const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<AbstractStatisticsObject> scale_with_selectivity(const Selectivity selectivity) const override;
};

}  // namespace opossum
