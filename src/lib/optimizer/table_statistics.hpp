#pragma once

#include <map>
#include <memory>
#include <string>

#include "all_parameter_variant.hpp"
#include "common.hpp"
#include "optimizer/abstract_column_statistics.hpp"

namespace opossum {

class Table;

class TableStatistics {
  friend class Statistics;

 public:
  // Needed for mocking in tests
  TableStatistics() = default;

  explicit TableStatistics(const std::string &name, const std::weak_ptr<Table> table);
  TableStatistics(const TableStatistics &table_statistics);

  virtual ~TableStatistics() = default;

  double row_count();
  std::shared_ptr<AbstractColumnStatistics> get_column_statistics(const ColumnID column_id);
  virtual std::shared_ptr<TableStatistics> predicate_statistics(const std::string &column_name,
                                                                const ScanType scan_type,
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

 protected:
  const std::string _name;
  /**
   * Even though _table should be a const pointer, this member cannot be defined const
   * as there are issues with Clang in that case:
   *
   * Clang will implicitly delete the default constructor if the 'const weak_ptr' is not set by the constructor.
   * For a mocked TableStatistics object this is the case, since we don't need an actual Table
   * but just try to call the default constructor of weak_ptr.
   *
   * This issue was reproducible on Jenkins and some Online Compilers with Clang 3.8, but not with Clang on Mac.
   * GCC is able to handle 'const weak_ptr' for this setting.
   */
  std::weak_ptr<Table> _table;
  double _row_count;
  std::map<ColumnID, std::shared_ptr<AbstractColumnStatistics>> _column_statistics;
};

}  // namespace opossum
