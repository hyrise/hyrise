#include "cardinality_estimate.hpp"

namespace opossum {

CardinalityEstimate::CardinalityEstimate(const Cardinality cardinality, const EstimateType type)
    : cardinality(cardinality), type(type) {}

bool CardinalityEstimate::operator==(const CardinalityEstimate& rhs) const {
  return cardinality == rhs.cardinality && type == rhs.type;
}

}  // namespace opossum
