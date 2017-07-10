#pragma once

#include <map>
#include <memory>
#include <string>

#include "all_parameter_variant.hpp"
#include "common.hpp"
#include "optimizer/abstract_column_statistics.hpp"

namespace opossum {

class Table;

/**
 * TableStatistics represents the expected statistics of a table.
 * This can be either a table in the StorageManager, or a result of an operator.
 * TableStatistics stores ColumnStatistics as pointers to AbstractColumnStatistics.
 * ColumnStatistics is typed by the ColumnType and implements the abstract methods.
 */
class TableStatistics {
  friend class Statistics;

 public:
  TableStatistics(const std::string &name, const std::weak_ptr<Table> table);
  TableStatistics(const TableStatistics &table_statistics);

  float row_count();
  std::shared_ptr<AbstractColumnStatistics> get_column_statistics(const ColumnID column_id);
  std::shared_ptr<TableStatistics> predicate_statistics(const std::string &column_name, const ScanType scan_type,
                                                        const AllParameterVariant value,
                                                        const optional<AllTypeVariant> value2 = nullopt);

  friend std::ostream &operator<<(std::ostream &os, TableStatistics &obj) {
    os << "Table Stats " << obj._name << std::endl;
    os << " row count: " << obj._row_count;
    for (auto column_statistics_pair : obj._column_statistics) {
      os << std::endl << " " << *column_statistics_pair.second;
    }
    return os;
  }

 private:
  const std::string _name;
  // Only available for statistics of tables in the StorageManager.
  // This is a weak_ptr, as
  // Table --shared_ptr--> TableStatistics
  const std::weak_ptr<Table> _table;
  float _row_count;
  std::map<ColumnID, std::shared_ptr<AbstractColumnStatistics>> _column_statistics;
};

}  // namespace opossum
