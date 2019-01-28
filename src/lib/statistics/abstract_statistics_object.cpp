#include "abstract_statistics_object.hpp"

namespace opossum {

CardinalityEstimate::CardinalityEstimate(const Cardinality cardinality, const EstimateType type):
  cardinality(cardinality), type(type) {

}

bool CardinalityEstimate::operator==(const CardinalityEstimate& rhs) const {
  return cardinality == rhs.cardinality && type == rhs.type;
}

AbstractStatisticsObject::AbstractStatisticsObject(const DataType data_type) : data_type(data_type) {}

bool AbstractStatisticsObject::does_not_contain(const PredicateCondition predicate_type,
                                                const AllTypeVariant& variant_value,
                                                const std::optional<AllTypeVariant>& variant_value2) const {
  return estimate_cardinality(predicate_type, variant_value, variant_value2).type == EstimateType::MatchesNone;
}

}  // namespace opossum
