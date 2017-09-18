#pragma once

#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "all_parameter_variant.hpp"
#include "common.hpp"
#include "optimizer/base_column_statistics.hpp"

namespace opossum {

class Table;

/**
 * TableStatistics is the interface to the statistics component.
 *
 * It represents the expected statistics of a table.
 * This can be either a table in the StorageManager, or a result of an operator.
 *
 * TableStatistics can be chained in the same way as operators.
 * The initial table statistics is available via function table_statistics() from the corresponding table.
 *
 * TableStatistics will eventually implement a function for each operator, currently supporting only predicates
 * (via predicate_statistics()) from which the expected row count of the corresponding output table can be accessed.
 *
 * The statistics component assumes a uniform value distribution in columns. If values for predictions are missing
 * (e.g. placeholders in prepared statements), default selectivity values from below are used.
 *
 * TableStatistics store column statistics as BaseColumnStatistics, which are instances of
 * ColumnStatistics<ColumnType>
 * Public TableStatistics functions pass on the parameters to the corresponding column statistics functions.
 * These compute a new ColumnStatistics<> and the predicted selectivity of an operator.
 *
 * Find more information about table statistics in our wiki:
 * https://github.com/hyrise/zweirise/wiki/potential_statistics
 */
class TableStatistics {
 public:
  /**
   * Creates a table statistics from a table.
   * This should only be done by the storage manager when adding a table to storage manager.
   */
  explicit TableStatistics(const std::shared_ptr<Table> table);
  /**
   * Table statistics should not be copied by other actors.
   * Copy constructor not private as copy is used by make_shared.
   */
  TableStatistics(const TableStatistics &table_statistics) = default;

  /**
   * Returns the expected row_count of the output of the corresponding operator.
   * See _distinct_count declaration below or explanation of float type.
   */
  float row_count() const;

  /**
   * Get table statistics for the operator table scan table scan.
   */
  virtual std::shared_ptr<TableStatistics> predicate_statistics(const ColumnID column_id, const ScanType scan_type,
                                                                const AllParameterVariant &value,
                                                                const optional<AllTypeVariant> &value2 = nullopt);

 protected:
  std::shared_ptr<BaseColumnStatistics> column_statistics(const ColumnID column_id);

  // Only available for statistics of tables in the StorageManager.
  // This is a weak_ptr, as
  // Table --shared_ptr--> TableStatistics
  const std::weak_ptr<Table> _table;

  // row count is not an integer as it is a predicted value
  // it is multiplied with selectivity factor of a corresponding operator to predict the operator's output
  // precision is lost, if row count is rounded
  float _row_count = 0.0f;

  std::vector<std::shared_ptr<BaseColumnStatistics>> _column_statistics;

  friend std::ostream &operator<<(std::ostream &os, TableStatistics &obj);
};

inline std::ostream &operator<<(std::ostream &os, TableStatistics &obj) {
  os << "Table Stats " << std::endl;
  os << " row count: " << obj._row_count;
  for (const auto &statistics : obj._column_statistics) {
    if (statistics) os << std::endl << " " << *statistics;
  }
  return os;
}

// default selectivity factors for assumption of uniform distribution
constexpr float DEFAULT_SELECTIVITY = 0.5f;
constexpr float DEFAULT_LIKE_SELECTIVITY = 0.1f;
// values below are taken from paper "Access path selection in a relational database management system",
// P. Griffiths Selinger, 1979
constexpr float DEFAULT_OPEN_ENDED_SELECTIVITY = 1.f / 3.f;
constexpr float DEFAULT_BETWEEN_SELECTIVITY = 0.25f;

}  // namespace opossum
