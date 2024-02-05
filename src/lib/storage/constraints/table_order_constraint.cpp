#include "table_order_constraint.hpp"

#include <algorithm>
#include <cstddef>

#include <boost/container_hash/hash.hpp>

#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

TableOrderConstraint::TableOrderConstraint(const std::vector<ColumnID>& ordering_columns,
                                           const std::vector<ColumnID>& ordered_columns)
    : _ordering_columns(ordering_columns), _ordered_columns(ordered_columns) {
  if constexpr (HYRISE_DEBUG) {
    for (const auto column : ordering_columns) {
      Assert(std::find(ordered_columns.begin(), ordered_columns.end(), column) == ordered_columns.end(),
             "Ordering and ordered columns must be disjoint.");
    }
  }
}

const std::vector<ColumnID>& TableOrderConstraint::ordering_columns() const {
  return _ordering_columns;
}

const std::vector<ColumnID>& TableOrderConstraint::ordered_columns() const {
  return _ordered_columns;
}

size_t TableOrderConstraint::hash() const {
  auto hash = boost::hash_value(_ordering_columns);
  boost::hash_combine(hash, _ordered_columns);
  return hash;
}

bool TableOrderConstraint::_on_equals(const AbstractTableConstraint& table_constraint) const {
  DebugAssert(dynamic_cast<const TableOrderConstraint*>(&table_constraint),
              "Different table_constraint type should have been caught by AbstractTableConstraint::operator==");
  const auto& rhs = static_cast<const TableOrderConstraint&>(table_constraint);
  return _ordering_columns == rhs._ordering_columns && _ordered_columns == rhs._ordered_columns;
}

}  // namespace hyrise

namespace std {

size_t hash<hyrise::TableOrderConstraint>::operator()(
    const hyrise::TableOrderConstraint& table_order_constraint) const {
  return table_order_constraint.hash();
}

}  // namespace std
