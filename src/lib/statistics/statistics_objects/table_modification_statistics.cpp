#include "table_modification_statistics.hpp"

namespace opossum {

TableModificationStatistics::TableModificationStatistics(const size_t approx_invalid_row_count):
  approx_invalid_row_count(approx_invalid_row_count) {}

std::shared_ptr<AbstractStatisticsObject> TableModificationStatistics::sliced(
const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
const std::optional<AllTypeVariant>& variant_value2) const {
  return std::make_shared<TableModificationStatistics>(approx_invalid_row_count.load());
}

std::shared_ptr<AbstractStatisticsObject> TableModificationStatistics::scaled(const Selectivity selectivity) const {
  return std::make_shared<TableModificationStatistics>(approx_invalid_row_count.load() * selectivity);
}

}  // namespace opossum
