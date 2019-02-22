#include "abstract_statistics_object.hpp"

namespace opossum {

CardinalityEstimate::CardinalityEstimate(const Cardinality cardinality, const EstimateType type)
    : cardinality(cardinality), type(type) {}

bool CardinalityEstimate::operator==(const CardinalityEstimate& rhs) const {
  return cardinality == rhs.cardinality && type == rhs.type;
}

AbstractStatisticsObject::AbstractStatisticsObject(const DataType data_type) : data_type(data_type) {}

}  // namespace opossum
