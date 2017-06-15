#include "table_statistics.hpp"

#include <map>
#include <memory>
#include <string>

#include "all_parameter_variant.hpp"
#include "optimizer/column_statistics.hpp"
#include "storage/table.hpp"

namespace opossum {

TableStatistics::TableStatistics(const std::weak_ptr<Table> table) : _table(table) {
  _row_count = _table.lock()->row_count();
}

TableStatistics::TableStatistics(std::weak_ptr<Table> table, double row_count,
                                 std::map<std::string, std::shared_ptr<ColumnStatistics>> column_statistics)
    : _table(table), _row_count(row_count), _column_statistics(column_statistics) {}

double TableStatistics::row_count() { return _row_count; }

std::shared_ptr<ColumnStatistics> TableStatistics::get_column_statistics(const std::string &column_name) {
  auto column_stat = _column_statistics.find(column_name);
  if (column_stat == _column_statistics.end()) {
    _column_statistics[column_name] = std::make_shared<ColumnStatistics>(_table, column_name);
  }
  return _column_statistics[column_name];
}

std::shared_ptr<TableStatistics> TableStatistics::shared_clone(double row_count) {
  return std::make_shared<TableStatistics>(_table, row_count, _column_statistics);
}

}  // namespace opossum
