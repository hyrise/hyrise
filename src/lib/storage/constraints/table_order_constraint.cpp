#include "table_order_constraint.hpp"

#include <boost/container_hash/hash.hpp>

namespace hyrise {

TableOrderConstraint::TableOrderConstraint(std::vector<ColumnID> columns, std::vector<ColumnID> ordered_columns)
    : AbstractTableConstraint{std::move(columns)}, _ordered_columns{std::move(ordered_columns)} {}

const std::vector<ColumnID>& TableOrderConstraint::ordered_columns() const {
  return _ordered_columns;
}

size_t TableOrderConstraint::hash() const {
  auto hash = boost::hash_value(_columns);
  boost::hash_combine(hash, _ordered_columns);
  return hash;
}

bool TableOrderConstraint::_on_equals(const AbstractTableConstraint& table_constraint) const {
  DebugAssert(dynamic_cast<const TableOrderConstraint*>(&table_constraint),
              "Different table_constraint type should have been caught by AbstractTableConstraint::operator==");
  return _ordered_columns == static_cast<const TableOrderConstraint&>(table_constraint)._ordered_columns;
}

}  // namespace hyrise

namespace std {

size_t hash<hyrise::TableOrderConstraint>::operator()(const hyrise::TableOrderConstraint& order_constraint) const {
  return order_constraint.hash();
}

}  // namespace std
