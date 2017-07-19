#pragma once

#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "all_parameter_variant.hpp"
#include "common.hpp"
#include "optimizer/abstract_column_statistics.hpp"

namespace opossum {

class Table;

/**
 * Table statistics is the interface to the statistics component.
 *
 * Table statistics represents the expected statistics of a table.
 * This can be either a table in the StorageManager, or a result of an operator.
 *
 * Table statistics can be chained in the same way as operators.
 * The initial table statisics is available via function table_statistics() from the corresponding table.
 *
 * Table statistics implements a function for each operator with the same interface as the operator.
 * Each function returns a new table statistics object
 * from which the expected row count of the corresponding output table can be accessed.
 * Currently, only predicate_statistics() for table scans is implemented.
 *
 * Statistics component assumes a uniform value distribution in columns.
 *
 * Table statistics store column statistics as pointers to AbstractColumnStatistics.
 * Column statistics is typed by ColumnType and implements the abstract methods.
 * Public table statistics functions pass on the parameters to the corresponding column statistics functions.
 * These compute a new column statistics and the predicted selectivity of an operator.
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

  // row count is not an integer as it is a predicted value
  // it is multiplied with selectivity factor of a corresponding operator to predict the operator's output
  // precision is lost, if row count is rounded
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
// values below are taken from paper "Access path selection in a relational database management system",
// P. Griffiths Selinger, 1979
constexpr float OPEN_ENDED_SELECTIVITY = 1.f / 3.f;
constexpr float BETWEEN_SELECTIVITY = 0.25f;

}  // namespace opossum
