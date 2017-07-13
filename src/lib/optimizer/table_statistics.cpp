#include "table_statistics.hpp"

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "all_parameter_variant.hpp"
#include "optimizer/abstract_column_statistics.hpp"
#include "optimizer/column_statistics.hpp"
#include "resolve_type.hpp"
#include "storage/table.hpp"

namespace opossum {

TableStatistics::TableStatistics(const std::shared_ptr<Table> table)
    : _table(table), _row_count(table->row_count()), _column_statistics(_row_count) {}

TableStatistics::TableStatistics(const TableStatistics &table_statistics)
    : _table(table_statistics._table),
      _row_count(table_statistics._row_count),
      _column_statistics(table_statistics._column_statistics) {}

float TableStatistics::row_count() const { return _row_count; }

std::shared_ptr<AbstractColumnStatistics> TableStatistics::column_statistics(const ColumnID column_id) {
  if (_column_statistics[column_id]) {
    return _column_statistics[column_id];
  }

  auto table = _table.lock();
  DebugAssert(table != nullptr, "Corresponding table of table statistics is deleted.");
  auto column_type = table->column_type(column_id);
  auto column_statistics =
      make_shared_by_column_type<AbstractColumnStatistics, ColumnStatistics>(column_type, column_id, _table);
  _column_statistics[column_id] = column_statistics;
  return _column_statistics[column_id];
}

std::shared_ptr<TableStatistics> TableStatistics::predicate_statistics(const std::string &column_name,
                                                                       const ScanType scan_type,
                                                                       const AllParameterVariant &value,
                                                                       const optional<AllTypeVariant> &value2) {
  // currently assuming all values are equally distributed

  auto _row_count = row_count();
  if (_row_count == 0) {
    auto clone = std::make_shared<TableStatistics>(*this);
    clone->_row_count = _row_count;
    return clone;
  }

  if (scan_type == ScanType::OpLike) {
    // simple heuristic:
    auto clone = std::make_shared<TableStatistics>(*this);
    clone->_row_count = _row_count * LIKE_SELECTIVITY;
    return clone;
  }

  auto table = _table.lock();
  DebugAssert(table != nullptr, "Corresponding table of table statistics is deleted.");
  const ColumnID column_id = table->column_id_by_name(column_name);

  auto old_column_statistics = column_statistics(column_id);
  auto clone = std::make_shared<TableStatistics>(*this);
  ColumnStatisticsContainer column_statistics_container{1, nullptr};

  if (value.type() == typeid(ColumnName)) {
    const ColumnID value_column_id = table->column_id_by_name(boost::get<ColumnName>(value));
    auto value_column_statistics = column_statistics(value_column_id);

    auto two_column_statistics_container =
        old_column_statistics->predicate_selectivity(scan_type, value_column_statistics, value2);

    if (two_column_statistics_container.second_column_statistics != nullptr) {
      clone->_column_statistics[value_column_id] = two_column_statistics_container.second_column_statistics;
    }
    column_statistics_container = two_column_statistics_container;

  } else if (value.type() == typeid(AllTypeVariant)) {
    auto casted_value = boost::get<AllTypeVariant>(value);

    column_statistics_container = old_column_statistics->predicate_selectivity(scan_type, casted_value, value2);

  } else if (value.type() == typeid(ValuePlaceholder)) {
    auto casted_value = boost::get<ValuePlaceholder>(value);

    column_statistics_container = old_column_statistics->predicate_selectivity(scan_type, casted_value, value2);
  }

  if (column_statistics_container.column_statistics != nullptr) {
    clone->_column_statistics[column_id] = column_statistics_container.column_statistics;
  }
  clone->_row_count *= column_statistics_container.selectivity;

  return clone;
}

}  // namespace opossum
