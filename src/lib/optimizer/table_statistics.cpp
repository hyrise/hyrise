#include "table_statistics.hpp"

#include <iostream>
#include <map>
#include <memory>
#include <string>

#include "all_parameter_variant.hpp"
#include "optimizer/abstract_column_statistics.hpp"
#include "optimizer/column_statistics.hpp"
#include "resolve_type.hpp"
#include "storage/table.hpp"

namespace opossum {

TableStatistics::TableStatistics(const std::string &name, const std::weak_ptr<Table> table)
    : _name(name), _table(table) {
  _row_count = _table.lock()->row_count();
}

TableStatistics::TableStatistics(const TableStatistics &table_statistics)
    : _name(table_statistics._name),
      _table(table_statistics._table),
      _row_count(table_statistics._row_count),
      _column_statistics(table_statistics._column_statistics) {}

float TableStatistics::row_count() { return _row_count; }

std::shared_ptr<AbstractColumnStatistics> TableStatistics::get_column_statistics(const ColumnID column_id) {
  auto table = _table.lock();
  auto column_stat = _column_statistics.find(column_id);
  if (column_stat == _column_statistics.end()) {
    auto column_type = table->column_type(column_id);
    auto column_statistics =
        make_shared_by_column_type<AbstractColumnStatistics, ColumnStatistics>(column_type, column_id, _table);
    _column_statistics[column_id] = column_statistics;
  }
  return _column_statistics[column_id];
}

std::shared_ptr<TableStatistics> TableStatistics::predicate_statistics(const std::string &column_name,
                                                                       const ScanType scan_type,
                                                                       const AllParameterVariant value,
                                                                       const optional<AllTypeVariant> value2) {
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
    clone->_row_count = _row_count * like_selectivity;
    return clone;
  }

  auto table = _table.lock();
  const ColumnID column_id = table->column_id_by_name(column_name);

  auto old_column_statistics = get_column_statistics(column_id);
  auto clone = std::make_shared<TableStatistics>(*this);
  float selectivity;
  std::shared_ptr<AbstractColumnStatistics> new_column_statistics;

  if (value.type() == typeid(ColumnName)) {
    const ColumnID value_column_id = table->column_id_by_name(boost::get<ColumnName>(value));
    auto value_column_statistics = get_column_statistics(value_column_id);
    std::shared_ptr<AbstractColumnStatistics> new_value_column_statistics;

    std::tie(selectivity, new_column_statistics, new_value_column_statistics) =
        old_column_statistics->predicate_selectivity(scan_type, value_column_statistics, value2);

    if (new_value_column_statistics != nullptr) {
      clone->_column_statistics[value_column_id] = new_value_column_statistics;
    }

  } else if (value.type() == typeid(AllTypeVariant)) {
    auto casted_value = boost::get<AllTypeVariant>(value);

    std::tie(selectivity, new_column_statistics) =
        old_column_statistics->predicate_selectivity(scan_type, casted_value, value2);

  } else if (value.type() == typeid(ValuePlaceholder)) {
    auto casted_value = boost::get<ValuePlaceholder>(value);

    std::tie(selectivity, new_column_statistics) =
        old_column_statistics->predicate_selectivity(scan_type, casted_value, value2);

  } else {
    selectivity = 1.f;
  }

  if (new_column_statistics != nullptr) {
    clone->_column_statistics[column_id] = new_column_statistics;
  }
  clone->_row_count *= selectivity;

  return clone;
}

}  // namespace opossum
