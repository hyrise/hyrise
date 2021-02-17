#include "table_key_constraint.hpp"

namespace opossum {

TableKeyConstraint::TableKeyConstraint(std::unordered_set<ColumnID> init_columns, KeyConstraintType init_key_type, std::optional<std::shared_ptr<TableKeyConstraint>> init_foreign_table_constraint)
    : AbstractTableConstraint(std::move(init_columns)), foreign_table_constraint(init_foreign_table_constraint), _key_type(init_key_type) {
      Assert(_key_type != KeyConstraintType::FOREIGN_KEY || (foreign_table_constraint && (*foreign_table_constraint)->key_type() == KeyConstraintType::PRIMARY_KEY), "Foreign Key constraints must be provided with a valid PrimaryKey.");
    }

KeyConstraintType TableKeyConstraint::key_type() const { return _key_type; }

bool TableKeyConstraint::_on_equals(const AbstractTableConstraint& table_constraint) const {
  DebugAssert(dynamic_cast<const TableKeyConstraint*>(&table_constraint),
              "Different table_constraint type should have been caught by AbstractTableConstraint::operator==");
  return key_type() == static_cast<const TableKeyConstraint&>(table_constraint).key_type();
}

}  // namespace opossum
