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
    : _name(name),
      _table(table),
      _column_statistics(std::make_shared<std::map<std::string, std::shared_ptr<AbstractColumnStatistics>>>()) {
  _row_count = _table.lock()->row_count();
}

TableStatistics::TableStatistics(const TableStatistics &table_statistics)
    : _name(table_statistics._name),
      _table(table_statistics._table),
      _row_count(table_statistics._row_count),
      _column_statistics(table_statistics._column_statistics) {}

double TableStatistics::row_count() { return _row_count; }

std::shared_ptr<AbstractColumnStatistics> TableStatistics::get_column_statistics(const std::string &column_name) {
  auto table = _table.lock();
  auto column_stat = _column_statistics->find(column_name);
  if (column_stat == _column_statistics->end()) {
    auto column_type = table->column_type(table->column_id_by_name(column_name));
    auto column_statistics =
        make_shared_by_column_type<AbstractColumnStatistics, ColumnStatistics>(column_type, _table, column_name);
    _column_statistics->emplace(column_name, column_statistics);
  }
  return _column_statistics->at(column_name);
}

std::shared_ptr<TableStatistics> TableStatistics::predicate_statistics(const std::string &column_name,
                                                                       const std::string &op,
                                                                       const AllParameterVariant value,
                                                                       const optional<AllTypeVariant> value2) {
  // currently assuming all values are equally distributed

  auto _row_count = row_count();
  if (_row_count == 0) {
    auto clone = std::make_shared<TableStatistics>(*this);
    clone->_row_count = _row_count;
    return clone;
  }

  if (op == "LIKE") {
    // simple heuristic:
    auto clone = std::make_shared<TableStatistics>(*this);
    clone->_row_count = _row_count / 3.;
    return clone;
  }

  auto column_statistics = get_column_statistics(column_name);
  double selectivity;
  std::shared_ptr<AbstractColumnStatistics> column_statistic;
  if (value.type() == typeid(ColumnName)) {
    ColumnName value_column_name = boost::get<ColumnName>(value);
    auto value_column_statistics = get_column_statistics(value_column_name);
    std::tie(selectivity, column_statistic) =
        column_statistics->predicate_selectivity(op, value_column_statistics, value2);
  } else {
    auto casted_value1 = boost::get<AllTypeVariant>(value);
    std::tie(selectivity, column_statistic) = column_statistics->predicate_selectivity(op, casted_value1, value2);
  }
  auto clone = std::make_shared<TableStatistics>(*this);
  if (column_statistic) {
    clone->_column_statistics->emplace(column_name, column_statistic);
  }
  clone->_row_count *= selectivity;

  return clone;
}

}  // namespace opossum
