#include "table_statistics.hpp"

#include <map>
#include <memory>
#include <string>

#include "all_parameter_variant.hpp"
#include "optimizer/column_statistics.hpp"
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

std::shared_ptr<ColumnStatistics> TableStatistics::get_column_statistics(const std::string &column_name) {
  auto column_stat = _column_statistics.find(column_name);
  if (column_stat == _column_statistics.end()) {
    _column_statistics[column_name] = std::make_shared<ColumnStatistics>(_table, column_name);
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
  if (op == "=") {
    auto distinct_count = column_statistics->get_distinct_count();
    clone->_row_count = _row_count / static_cast<double>(distinct_count);
    clone->_column_statistics[column_name] =
        std::make_shared<ColumnStatistics>(1, casted_value1, casted_value1, column_name);
  } else {
    // TODO(mp): extend for other comparison operators
    // Brace yourselves.
    auto distinct_count = column_statistics->get_distinct_count();
    clone = std::make_shared<TableStatistics>(*this);
    clone->_row_count = _row_count / static_cast<double>(distinct_count);
    // Fail(std::string("unknown operator ") + op);
  }
  // else if (op == "!=") {
  // Fail(std::string("operator not yet implemented: ") + op);
  // } else if (op == "<") {
  //   Fail(std::string("operator not yet implemented: ") + op);
  // } else if (op == "<=") {
  //   Fail(std::string("operator not yet implemented: ") + op);
  // } else if (op == ">") {
  //   Fail(std::string("operator not yet implemented: ") + op);
  // } else if (op == ">=") {
  //   Fail(std::string("operator not yet implemented: ") + op);
  // } else if (op == "BETWEEN") {
  //   Fail(std::string("operator not yet implemented: ") + op);
  // } else
  return clone;
}

}  // namespace opossum
