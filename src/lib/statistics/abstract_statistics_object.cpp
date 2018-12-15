#include "abstract_statistics_object.hpp"

namespace opossum {

bool AbstractStatisticsObject::does_not_contain(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                      const std::optional<AllTypeVariant>& variant_value2) const {
  return estimate_cardinality(predicate_type, variant_value, variant_value2).type == EstimateType::MatchesNone;
}


std::shared_ptr<AbstractStatisticsObject> AbstractStatisticsObject::_reduce_to_single_bin_histogram_impl() const {
  return nullptr;
}

}  // namespace opossum
