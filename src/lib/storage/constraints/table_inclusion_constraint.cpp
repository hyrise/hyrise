#include "table_inclusion_constraint.hpp"

namespace hyrise {

TableInclusionConstraint::TableInclusionConstraint(std::vector<ColumnID> columns,
                                                   std::vector<ColumnID> included_columns,
                                                   const std::string& included_table_name)
    : AbstractTableConstraint{std::move(columns)},
      _included_columns{std::move(included_columns)},
      _included_table_name{included_table_name} {
  Assert(_columns.size() == _included_columns.size(), "Invalid number of columns for TableInclusionConstraint");
}

const std::vector<ColumnID>& TableInclusionConstraint::included_columns() const {
  return _included_columns;
}

size_t TableInclusionConstraint::hash() const {
  auto hash = boost::hash_value(_columns.size());
  boost::hash_combine(hash, _columns);
  boost::hash_combine(hash, _included_table_name);
  boost::hash_combine(hash, _included_columns);

  return hash;
}

const std::string& TableInclusionConstraint::included_table_name() const {
  return _included_table_name;
}

bool TableInclusionConstraint::_on_equals(const AbstractTableConstraint& table_constraint) const {
  DebugAssert(dynamic_cast<const TableInclusionConstraint*>(&table_constraint),
              "Different table_constraint type should have been caught by AbstractTableConstraint::operator==");
  const auto& inclusion_constraint = static_cast<const TableInclusionConstraint&>(table_constraint);
  return _included_table_name == inclusion_constraint._included_table_name &&
         _included_columns == inclusion_constraint._included_columns;
}

}  // namespace hyrise

namespace std {

size_t hash<hyrise::TableInclusionConstraint>::operator()(
    const hyrise::TableInclusionConstraint& inclusion_constraint) const {
  return inclusion_constraint.hash();
}

}  // namespace std
