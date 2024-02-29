#include "table_key_constraint.hpp"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <set>

#include <boost/container_hash/hash.hpp>

#include "storage/constraints/abstract_table_constraint.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

TableKeyConstraint::TableKeyConstraint(const std::set<ColumnID>& columns, const KeyConstraintType key_type)
    : AbstractTableConstraint(TableConstraintType::Key), _key_type{key_type}, _columns{columns} {}

KeyConstraintType TableKeyConstraint::key_type() const {
  return _key_type;
}

const std::set<ColumnID>& TableKeyConstraint::columns() const {
  return _columns;
}

size_t TableKeyConstraint::hash() const {
  auto hash = size_t{0};
  boost::hash_combine(hash, _key_type);
  for (const auto& column : _columns) {
    boost::hash_combine(hash, column);
  }
  return hash;
}

bool TableKeyConstraint::_on_equals(const AbstractTableConstraint& table_constraint) const {
  DebugAssert(dynamic_cast<const TableKeyConstraint*>(&table_constraint),
              "Different table_constraint type should have been caught by AbstractTableConstraint::operator==");
  const auto& rhs = static_cast<const TableKeyConstraint&>(table_constraint);
  return _key_type == rhs._key_type && _columns == rhs._columns;
}

bool TableKeyConstraint::operator<(const TableKeyConstraint& rhs) const {
  // PRIMARY_KEY constraints are "smaller" than UNIQUE constraints. Thus, they are listed first when printing them.
  if (_key_type != rhs._key_type) {
    return _key_type < rhs._key_type;
  }

  // As the columns are stored in a std::set, iteration is sorted and the result is not ambiguous.
  return std::lexicographical_compare(_columns.cbegin(), _columns.cend(), rhs._columns.cbegin(), rhs._columns.cend());
}

}  // namespace hyrise

namespace std {

size_t hash<hyrise::TableKeyConstraint>::operator()(const hyrise::TableKeyConstraint& table_key_constraint) const {
  return table_key_constraint.hash();
}

}  // namespace std
