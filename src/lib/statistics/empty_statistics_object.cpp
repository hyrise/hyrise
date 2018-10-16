#include "empty_statistics_object.hpp"

#include <memory>
#include <optional>
#include <utility>

#include "abstract_statistics_object.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

CardinalityEstimate EmptyStatisticsObject::estimate_cardinality(
    const PredicateCondition /*predicate_type*/, const AllTypeVariant& /*variant_value*/,
    const std::optional<AllTypeVariant>& /*variant_value2*/) const {
  return {Cardinality{0}, EstimateType::MatchesNone};
}

std::shared_ptr<AbstractStatisticsObject> EmptyStatisticsObject::slice_with_predicate(
    const PredicateCondition /*predicate_type*/, const AllTypeVariant& /*variant_value*/,
    const std::optional<AllTypeVariant>& /*variant_value2*/) const {
  return std::make_shared<EmptyStatisticsObject>();
}

std::shared_ptr<AbstractStatisticsObject> EmptyStatisticsObject::scale_with_selectivity(
    const float /*selectivity*/) const {
  return std::make_shared<EmptyStatisticsObject>();
}

}  // namespace opossum
