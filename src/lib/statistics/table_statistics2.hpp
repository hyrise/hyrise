#pragma once

#include <memory>
#include <vector>

#include "types.hpp"
#include "all_type_variant.hpp"
#include "all_parameter_variant.hpp"

namespace opossum {

class AbstractColumnStatistics2;

/**
 * Statistics about a table, with algorithms to perform cardinality estimations.
 */
class TableStatistics2 final {
 public:
  // magic numbers below are taken from paper "Access path selection in a relational database management system",
  // P. Griffiths Selinger, 1979
  static constexpr auto DEFAULT_LIKE_SELECTIVITY = 0.1f;
  static constexpr auto DEFAULT_OPEN_ENDED_SELECTIVITY = 1.f / 3.f;

  TableStatistics2(const float row_count, const std::vector<std::shared_ptr<const AbstractColumnStatistics2>>& column_statistics);
  TableStatistics2(const TableStatistics2& table_statistics) = default;

  /**
   * @defgroup Member access
   * @{
   */
  float row_count() const;
  const std::vector<std::shared_ptr<const AbstractColumnStatistics2>>& column_statistics() const;
  /** @} */

  /**
   * @defgroup Cardinality Estimations
   * @{
   */
  TableStatistics2 estimate_predicate(
    const ColumnID column_id,
    const PredicateCondition predicate_condition,
    const AllParameterVariant& value,
    const std::optional<AllTypeVariant>& value2 = std::nullopt) const;

  TableStatistics2 estimate_cross_join(
    const TableStatistics2& right_table_statistics) const;

  TableStatistics2 estimate_predicated_join(
    const TableStatistics2& right_table_statistics,
    const JoinMode mode,
    const ColumnIDPair column_ids,
    const PredicateCondition predicate_condition) const;
  /** @} */

  std::string description() const;

 private:
  float _row_count;
  std::vector<std::shared_ptr<const AbstractColumnStatistics2>> _column_statistics;
};

}  // namespace opossum
