#include "distinct_value_count.hpp"

#include <cstddef>
#include <memory>
#include <optional>

#include "all_type_variant.hpp"
#include "statistics/statistics_objects/abstract_statistics_object.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

DistinctValueCount::DistinctValueCount(const size_t init_count)
    : AbstractStatisticsObject(DataType::Long), count{init_count} {}

std::shared_ptr<const AbstractStatisticsObject> DistinctValueCount::sliced(
    const PredicateCondition /* predicate_condition */, const AllTypeVariant& /* variant_value */,
    const std::optional<AllTypeVariant>& /* variant_value2 */) const {
  // We do not know how the count changes given any predicate.
  Fail("Slicing is not implemented for distinct value count");
}

std::shared_ptr<const AbstractStatisticsObject> DistinctValueCount::scaled(const Selectivity /* selectivity */) const {
  return shared_from_this();
}

}  // namespace hyrise
