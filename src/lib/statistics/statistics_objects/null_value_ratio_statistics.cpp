#include "null_value_ratio_statistics.hpp"

#include "utils/assert.hpp"

namespace opossum {

NullValueRatioStatistics::NullValueRatioStatistics(const float init_ratio)
    : AbstractStatisticsObject(DataType::Null), ratio(init_ratio) {}

std::shared_ptr<AbstractStatisticsObject> NullValueRatioStatistics::sliced(
    const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  return std::make_shared<NullValueRatioStatistics>(ratio);
}

std::shared_ptr<AbstractStatisticsObject> NullValueRatioStatistics::scaled(const Selectivity selectivity) const {
  return std::make_shared<NullValueRatioStatistics>(ratio);
}

}  // namespace opossum
