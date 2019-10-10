#include "base_attribute_statistics.hpp"

namespace opossum {

BaseAttributeStatistics::BaseAttributeStatistics(const DataType data_type) : data_type(data_type) {}

std::shared_ptr<BaseAttributeStatistics> BaseAttributeStatistics::pruned(
    const size_t num_values_pruned, const PredicateCondition predicate_condition, const AllTypeVariant& variant_value,
    const std::optional<AllTypeVariant>& variant_value2) const {
  Fail("Pruning has not yet been implemented for the given statistics object");
}

}  // namespace opossum
