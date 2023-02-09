#include "table_key_constraint.hpp"

#include <boost/container_hash/hash.hpp>

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
  // PRIMARY_KEY constraints are "smaller" than UNIQUE constraints. Thus, they are listed first when printing them.
  if (_key_type != rhs.key_type()) {
    return _key_type < rhs.key_type();
  }

  // As the columns were originally stored in a std::set, iteration is sorted and the result is not ambiguous.
  return std::lexicographical_compare(_columns.cbegin(), _columns.cend(), rhs._columns.cbegin(), rhs._columns.cend());
}

}  // namespace hyrise

namespace std {

size_t hash<hyrise::TableKeyConstraint>::operator()(const hyrise::TableKeyConstraint& key_constraint) const {
  return key_constraint.hash();
}

}  // namespace std
