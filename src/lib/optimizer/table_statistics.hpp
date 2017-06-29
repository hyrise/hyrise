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
  TableStatistics() = default;

  explicit TableStatistics(const std::string &name, const std::weak_ptr<Table> table);
  TableStatistics(const TableStatistics &table_statistics);

  virtual ~TableStatistics() = default;

  double row_count();
  std::shared_ptr<AbstractColumnStatistics> get_column_statistics(const std::string &column_name);
  virtual std::shared_ptr<TableStatistics> predicate_statistics(const std::string &column_name, const std::string &op,
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
  const std::weak_ptr<Table> _table;
  double _row_count;
  std::map<std::string, std::shared_ptr<AbstractColumnStatistics>> _column_statistics;
};

}  // namespace opossum
