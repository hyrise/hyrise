#include "table_stats.hpp"

#include <map>
#include <memory>
#include <string>

#include "all_parameter_variant.hpp"
#include "optimizer/column_stats.hpp"
#include "storage/table.hpp"

namespace opossum {

TableStats::TableStats(const std::weak_ptr<Table> table) : _table(table) { _row_count = _table.lock()->row_count(); }

TableStats::TableStats(const private_key &, std::weak_ptr<Table> table, double row_count,
                       std::map<std::string, std::shared_ptr<ColumnStats>> column_stats)
    : _table(table), _row_count(row_count), _column_stats(column_stats) {}

std::shared_ptr<TableStats> TableStats::shared_clone(double row_count) {
  return std::make_shared<TableStats>(private_key{}, _table, row_count, _column_stats);
}

double TableStats::row_count() { return _row_count; }

std::shared_ptr<ColumnStats> TableStats::get_column_stats(const std::string &column_name) {
  auto column_stat = _column_stats.find(column_name);
  if (column_stat == _column_stats.end()) {
    _column_stats[column_name] = std::make_shared<ColumnStats>(_table, column_name);
  }
  return _column_stats[column_name];
}

}  // namespace opossum
