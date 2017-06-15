#pragma once

#include <map>
#include <memory>
#include <string>

#include "all_parameter_variant.hpp"
#include "optimizer/column_statistics.hpp"

namespace opossum {

class Table;

class TableStatistics {
  friend class Statistics;

 public:
  explicit TableStatistics(const std::weak_ptr<Table> table);
  TableStatistics(std::weak_ptr<Table> table, double row_count,
                  std::map<std::string, std::shared_ptr<ColumnStatistics>> column_statistics);
  double row_count();
  std::shared_ptr<ColumnStatistics> get_column_statistics(const std::string &column_name);

 private:
  std::shared_ptr<TableStatistics> shared_clone(double row_count);
  const std::weak_ptr<Table> _table;
  double _row_count;
  std::map<std::string, std::shared_ptr<ColumnStatistics>> _column_statistics;
};

}  // namespace opossum
