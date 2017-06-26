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

double TableStatistics::row_count() { return _row_count; }

std::shared_ptr<AbstractColumnStatistics> TableStatistics::get_column_statistics(const std::string &column_name) {
  auto table = _table.lock();
  auto column_stat = _column_statistics.find(column_name);
  if (column_stat == _column_statistics.end()) {
    _column_statistics[column_name] = make_shared_by_column_type<AbstractColumnStatistics, ColumnStatistics>(
        table->column_type(table->column_id_by_name(column_name)), _table, column_name);
  }
  return _column_statistics[column_name];
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

  if (value.type() == typeid(ColumnName)) {
    // ColumnName column_name = boost::get<ColumnName>(value);
    // column_id2 = in_table->column_id_by_name(column_name);
    auto clone = std::make_shared<TableStatistics>(*this);
    clone->_row_count = _row_count / 3.;
    return clone;
  }

  auto casted_value1 = boost::get<AllTypeVariant>(value);

  if (op == "LIKE") {
    auto clone = std::make_shared<TableStatistics>(*this);
    clone->_row_count = _row_count / 3.;
    return clone;
  }

  auto column_statistics = get_column_statistics(column_name);  // trigger lazy initialization
  auto clone = std::make_shared<TableStatistics>(*this);
  double selectivity;
  std::shared_ptr<AbstractColumnStatistics> column_statistic;
  std::tie(selectivity, column_statistic) =
      clone->get_column_statistics(column_name)->predicate_selectivity(op, casted_value1, value2);
  if (column_statistic) {
    clone->_column_statistics[column_name] = column_statistic;
  }
  clone->_row_count *= selectivity;

  //  auto distinct_count = column_statistics->get_distinct_count();
  //  if (op == "=") {
  //    if (casted_value1 < column_statistics->get_min() || casted_value1 > column_statistics->get_max()) {
  //      clone->_row_count = 0;
  //      return clone;
  //    }
  //    clone->_row_count = _row_count / static_cast<double>(distinct_count);
  //    clone->_column_statistics[column_name] =
  //        std::make_shared<ColumnStatistics>(1, casted_value1, casted_value1, column_name);
  //  } else if (op == "!=") {
  //    // disregarding A = 5 AND A != 5
  //    // (just don't put this into a query!)
  //    clone->_row_count = _row_count - _row_count / static_cast<double>(distinct_count);
  //    clone->_column_statistics[column_name] = std::make_shared<ColumnStatistics>(
  //        distinct_count - 1, column_statistics->get_min(), column_statistics->get_max(), column_name);
  //  } else if (op == "<") {
  //    if (casted_value1 <= column_statistics->get_min()) {
  //      clone->_row_count = 0;
  //      return clone;
  //    }
  //    auto min = column_statistics->get_min();
  //    auto max = column_statistics->get_max();
  //    std::cout << "rc " << _row_count << ", casted value " << casted_value1 << std::endl;
  //    std::cout << (casted_value1 - min) << std::endl;
  //    std::cout << (max - min + 1) << std::endl;
  //    clone->_row_count = _row_count * (casted_value1 - min) / (max - min + 1);
  //    clone->_column_statistics[column_name] =
  //        std::make_shared<ColumnStatistics>(distinct_count, min, casted_value1, column_name);
  //    // } else if (op == "<=") {
  //    //   Fail(std::string("operator not yet implemented: ") + op);
  //    // } else if (op == ">") {
  //    //   Fail(std::string("operator not yet implemented: ") + op);
  //    // } else if (op == ">=") {
  //    //   Fail(std::string("operator not yet implemented: ") + op);
  //    // } else if (op == "BETWEEN") {
  //    //   Fail(std::string("operator not yet implemented: ") + op);
  //  } else {
  //    // TODO(mp): extend for other comparison operators
  //    // Brace yourselves.
  //    auto distinct_count = column_statistics->get_distinct_count();
  //    clone = std::make_shared<TableStatistics>(*this);
  //    clone->_row_count = _row_count / static_cast<double>(distinct_count);
  //    // Fail(std::string("unknown operator ") + op);
  //  }
  return clone;
}

}  // namespace opossum
