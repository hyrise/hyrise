#pragma once

#include <memory>
#include <vector>

#include "all_parameter_variant.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class BaseColumnStatistics;

/**
 * Statistics about a table, with algorithms to perform cardinality estimations.
 */
class TableStatistics final {
 public:
  // magic numbers below are taken from paper "Access path selection in a relational database management system",
  // P. Griffiths Selinger, 1979
  static constexpr auto DEFAULT_LIKE_SELECTIVITY = 0.1f;
  static constexpr auto DEFAULT_OPEN_ENDED_SELECTIVITY = 1.f / 3.f;
  // Made up magic number
  static constexpr auto DEFAULT_DISJUNCTION_SELECTIVITY = 0.2f;

  TableStatistics(const TableType table_type, const float row_count,
                  const std::vector<std::shared_ptr<const BaseColumnStatistics>>& column_statistics);
  TableStatistics(const TableStatistics& table_statistics) = default;

  /**
   * @defgroup Member access
   * @{
   */
  TableType table_type() const;
  float row_count() const;
  uint64_t approx_valid_row_count() const;
  const std::vector<std::shared_ptr<const BaseColumnStatistics>>& column_statistics() const;
  /** @} */

  /**
   * @defgroup Cardinality Estimations
   * @{
   */
  TableStatistics estimate_predicate(const ColumnID column_id, const PredicateCondition predicate_condition,
                                     const AllParameterVariant& value,
                                     const std::optional<AllParameterVariant>& value2 = std::nullopt) const;

  TableStatistics estimate_cross_join(const TableStatistics& right_table_statistics) const;

  TableStatistics estimate_predicated_join(const TableStatistics& right_table_statistics, const JoinMode mode,
                                           const ColumnIDPair column_ids,
                                           const PredicateCondition predicate_condition) const;
  TableStatistics estimate_disjunction(const TableStatistics& right_table_statistics) const;
  /** @} */

  // Increases the (approximate) count of invalid rows in the table (caused by deletes).
  void increase_invalid_row_count(uint64_t count);

  std::string description() const;

 private:
  TableType _table_type;
  float _row_count;
  std::vector<std::shared_ptr<const BaseColumnStatistics>> _column_statistics;

  // Stores the number of invalid (deleted) rows.
  // This is currently not an atomic due to performance considerations.
  // It is simply used as an estimate for the optimizer, and therefore does not need to be exact.
  uint64_t _approx_invalid_row_count{0};
};

}  // namespace opossum
