#include "null_value_ratio.hpp"

#include "utils/assert.hpp"

namespace opossum {

NullValueRatio::NullValueRatio(const float null_value_ratio):
  AbstractStatisticsObject(DataType::Null), null_value_ratio(null_value_ratio) {}

CardinalityEstimate NullValueRatio::estimate_cardinality(
    const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  Fail("estimate_cardinality() should not be called on NullValueRatio");
}

std::shared_ptr<AbstractStatisticsObject> NullValueRatio::sliced(
    const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  return std::make_shared<NullValueRatio>(null_value_ratio);
}

std::shared_ptr<AbstractStatisticsObject> NullValueRatio::scaled(const Selectivity selectivity) const {
  return std::make_shared<NullValueRatio>(null_value_ratio);
}

}  // namespace opossum
