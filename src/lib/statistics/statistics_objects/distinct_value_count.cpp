#include "distinct_value_count.hpp"

#include <cstddef>
#include <memory>
#include <optional>

#include "all_type_variant.hpp"
#include "statistics/statistics_objects/abstract_statistics_object.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

DistinctValueCount::DistinctValueCount(const size_t init_count) : count{init_count} {}

constexpr DataType DistinctValueCount::data_type() const {
  return DataType::Long;
}

std::shared_ptr<const AbstractStatisticsObject> DistinctValueCount::sliced(
    const PredicateCondition /* predicate_condition */, const AllTypeVariant& /* variant_value */,
    const std::optional<AllTypeVariant>& /* variant_value2 */) const {
  // We do not know how the count changes given any predicate.
  Fail("Slicing is not implemented for distinct value count");
}

std::shared_ptr<const AbstractStatisticsObject> DistinctValueCount::scaled(const Selectivity /* selectivity */) const {
  return shared_from_this();
}

std::shared_ptr<const AbstractStatisticsObject> DistinctValueCount::pruned(
    const size_t /* num_values_pruned */, const PredicateCondition /* predicate_condition */,
    const AllTypeVariant& /* variant_value */, const std::optional<AllTypeVariant>& /* variant_value2 */) const {
  Fail("Pruning has not yet been implemented for the given statistics object");
}

}  // namespace hyrise
