#include "table_inclusion_constraint.hpp"

namespace hyrise {

TableInclusionConstraint::TableInclusionConstraint(std::vector<ColumnID> columns,
                                                   std::vector<ColumnID> dependent_columns,
                                                   const std::string& dependent_table_name)
    : AbstractTableConstraint{std::move(columns)},
      _dependent_columns{std::move(dependent_columns)},
      _dependent_table_name{dependent_table_name} {
  Assert(_columns.size() == _dependent_columns.size(), "Invalid number of columns for TableInclusionConstraint");
}

const std::vector<ColumnID>& TableInclusionConstraint::dependent_columns() const {
  return _dependent_columns;
}

size_t TableInclusionConstraint::hash() const {
  auto hash = boost::hash_value(_columns.size());
  boost::hash_combine(hash, _columns);
  boost::hash_combine(hash, _dependent_table_name);
  boost::hash_combine(hash, _dependent_columns);

  return hash;
}

const std::string& TableInclusionConstraint::dependent_table_name() const {
  return _dependent_table_name;
}

bool TableInclusionConstraint::_on_equals(const AbstractTableConstraint& table_constraint) const {
  DebugAssert(dynamic_cast<const TableInclusionConstraint*>(&table_constraint),
              "Different table_constraint type should have been caught by AbstractTableConstraint::operator==");
  const auto& inclusion_constraint = static_cast<const TableInclusionConstraint&>(table_constraint);
  return _dependent_table_name == inclusion_constraint._dependent_table_name &&
         _dependent_columns == inclusion_constraint._dependent_columns;
}

}  // namespace hyrise
