#include "table_key_constraint.hpp"

namespace hyrise {

TableKeyConstraint::TableKeyConstraint(const std::set<ColumnID>& columns, KeyConstraintType init_key_type)
    : AbstractTableConstraint{{columns.cbegin(), columns.cend()}}, _key_type{init_key_type} {}

KeyConstraintType TableKeyConstraint::key_type() const {
  return _key_type;
}

size_t TableKeyConstraint::hash() const {
  auto hash = boost::hash_value(_key_type);
  for (const auto& column : columns()) {
    boost::hash_combine(hash, column);
  }
  return hash;
}

bool TableKeyConstraint::_on_equals(const AbstractTableConstraint& table_constraint) const {
  DebugAssert(dynamic_cast<const TableKeyConstraint*>(&table_constraint),
              "Different table_constraint type should have been caught by AbstractTableConstraint::operator==");
  return key_type() == static_cast<const TableKeyConstraint&>(table_constraint).key_type();
}

bool TableKeyConstraint::operator<(const TableKeyConstraint& rhs) const {
  // PRIMARY_KEY constraints are "smaller" than UNIQUE constraints. Thus, they are listed first (e.g., for printing).
  if (_key_type < rhs.key_type()) {
    return true;
  }

  // As the columns are stored in a std::set, iteration is sorted and the result is not ambiguous.
  // We do not use the < operator for the column sets because it is deprecated in C++20.
  return std::lexicographical_compare(_columns.cbegin(), _columns.cend(), rhs._columns.cbegin(), rhs._columns.cend());
}

}  // namespace hyrise
