#pragma once

#include <memory>
#include <string>
#include <vector>

#include "all_parameter_variant.hpp"
#include "common.hpp"
#include "optimizer/abstract_column_statistics.hpp"

namespace opossum {

class Table;

/**
 * TableStatistics represents the expected statistics of a table.
 * This can be either a table in the StorageManager, or a result of an operator.
 * TableStatistics store ColumnStatistics as pointers to AbstractColumnStatistics.
 * ColumnStatistics is typed by the ColumnType and implements the abstract methods.
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
  TableStatistics(const TableStatistics &table_statistics);

  /**
   * Returns the expected row_cunt of the output of the corresponding operator.
   */
  float row_count() const;

  /**
   * Get table statistics for the operator table scan table scan.
   */
  std::shared_ptr<TableStatistics> predicate_statistics(const std::string &column_name, const ScanType scan_type,
                                                        const AllParameterVariant &value,
                                                        const optional<AllTypeVariant> &value2 = nullopt);

 private:
  std::shared_ptr<AbstractColumnStatistics> column_statistics(const ColumnID column_id);

  // Only available for statistics of tables in the StorageManager.
  // This is a weak_ptr, as
  // Table --shared_ptr--> TableStatistics
  const std::weak_ptr<Table> _table;
  float _row_count;
  std::vector<std::shared_ptr<AbstractColumnStatistics>> _column_statistics;

  friend class Statistics;
  friend std::ostream &operator<<(std::ostream &os, TableStatistics &obj);
};

inline std::ostream &operator<<(std::ostream &os, TableStatistics &obj) {
  os << "Table Stats " << std::endl;
  os << " row count: " << obj._row_count;
  for (auto statistics : obj._column_statistics) {
    if (statistics) os << std::endl << " " << *statistics;
  }
  return os;
}

// default selectivity factors for assumption of uniform distribution
constexpr float DEFAULT_SELECTIVITY = 0.5f;
constexpr float LIKE_SELECTIVITY = 0.1f;
// below are taken from paper "Access path selection in a relational database management system",
// P. Griffiths Selinger, 1979
constexpr float OPEN_ENDED_SELECTIVITY = 1.f / 3.f;
constexpr float BETWEEN_SELECTIVITY = 0.25f;

}  // namespace opossum
