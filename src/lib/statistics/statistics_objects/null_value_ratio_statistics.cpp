#include "null_value_ratio_statistics.hpp"

#include <memory>
#include <optional>

#include "all_type_variant.hpp"
#include "statistics/statistics_objects/abstract_statistics_object.hpp"
#include "types.hpp"

namespace hyrise {

NullValueRatioStatistics::NullValueRatioStatistics(const float init_ratio)
    : AbstractStatisticsObject(DataType::Null), ratio(init_ratio) {}

std::shared_ptr<const AbstractStatisticsObject> NullValueRatioStatistics::sliced(
    const PredicateCondition /* predicate_condition */, const AllTypeVariant& /* variant_value */,
    const std::optional<AllTypeVariant>& /* variant_value2 */) const {
  // If a scan with any predicate is performed, there cannot be NULL values in the result.
  return std::make_shared<NullValueRatioStatistics>(0);
}

std::shared_ptr<const AbstractStatisticsObject> NullValueRatioStatistics::scaled(
    const Selectivity /* selectivity */) const {
  return shared_from_this();
}

}  // namespace hyrise
