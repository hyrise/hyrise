#include "empty_statistics_object.hpp"

#include <memory>
#include <optional>
#include <utility>

#include "abstract_statistics_object.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

EmptyStatisticsObject::EmptyStatisticsObject(const DataType data_type) : AbstractStatisticsObject(data_type) {}

std::shared_ptr<AbstractStatisticsObject> EmptyStatisticsObject::sliced(
    const PredicateCondition /*predicate_type*/, const AllTypeVariant& /*variant_value*/,
    const std::optional<AllTypeVariant>& /*variant_value2*/) const {
  return std::make_shared<EmptyStatisticsObject>(data_type);
}

std::shared_ptr<AbstractStatisticsObject> EmptyStatisticsObject::scaled(
    const float /*selectivity*/) const {
  return std::make_shared<EmptyStatisticsObject>(data_type);
}

}  // namespace opossum
