#pragma once

#include "statistics/abstract_statistics_object.hpp"

namespace opossum {

/**
 * A dummy class as a potential starting point to deal with statistics and Table updates. For now just stores the number
 * of invalidated/deleted row
 */
class TableModificationStatistics : public AbstractStatisticsObject {
 public:
  explicit TableModificationStatistics(const size_t approx_invalid_row_count);

  std::shared_ptr<AbstractStatisticsObject> sliced(
      const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
      const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  std::shared_ptr<AbstractStatisticsObject> scaled(const Selectivity selectivity) const override;

  // A hopefully temporary means to represent the number of rows deleted from a Table by the Delete operator.
  std::atomic<size_t> approx_invalid_row_count{0};
};

}  // namespace opossum
