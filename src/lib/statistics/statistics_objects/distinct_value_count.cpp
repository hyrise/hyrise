#include "distinct_value_count.hpp"

namespace hyrise {

DistinctValueCount::DistinctValueCount(const size_t init_count)
    : AbstractStatisticsObject(DataType::Long), count{init_count} {}

std::shared_ptr<AbstractStatisticsObject> DistinctValueCount::sliced(
    const PredicateCondition /* predicate_condition */, const AllTypeVariant& /* variant_value */,
    const std::optional<AllTypeVariant>& /* variant_value2 */) const {
  // We do not know how the count changes given any predicate.
  Fail("Slicing is not implemented for distinct value count");
}

std::shared_ptr<AbstractStatisticsObject> DistinctValueCount::scaled(const Selectivity /* selectivity */) const {
  return std::make_shared<DistinctValueCount>(count);
}

}  // namespace hyrise
