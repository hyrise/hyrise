#include "abstract_dependency_validation_rule.hpp"

#include <magic_enum.hpp>

#include "hyrise.hpp"
#include "storage/constraints/foreign_key_constraint.hpp"
#include "storage/constraints/table_key_constraint.hpp"
#include "storage/constraints/table_order_constraint.hpp"

namespace hyrise {

ValidationResult::ValidationResult(const ValidationStatus init_status) : status{init_status} {}

AbstractDependencyValidationRule::AbstractDependencyValidationRule(const DependencyType init_dependency_type)
    : dependency_type(init_dependency_type) {}

ValidationResult AbstractDependencyValidationRule::validate(const AbstractDependencyCandidate& candidate) const {
  Assert(candidate.type == dependency_type, "Wrong dependency type: Expected " +
                                                std::string{magic_enum::enum_name(dependency_type)} + ", got " +
                                                std::string{magic_enum::enum_name(candidate.type)});
  const auto& constraint = _constraint_from_candidate(candidate);
  if (_is_known(candidate.table_name, constraint)) {
    return ValidationResult{ValidationStatus::AlreadyKnown};
  }

  return _on_validate(candidate);
}

bool AbstractDependencyValidationRule::_is_known(const std::string& table_name,
                                                 const std::shared_ptr<AbstractTableConstraint>& constraint) const {
  const auto& table = Hyrise::get().storage_manager.get_table(table_name);

  if (const auto& order_constraint = std::dynamic_pointer_cast<TableOrderConstraint>(constraint)) {
    const auto& current_constraints = table->soft_order_constraints();
    for (const auto& current_constraint : current_constraints) {
      if (current_constraint.columns() == order_constraint->columns() &&
          current_constraint.ordered_columns().front() == order_constraint->ordered_columns().front()) {
        return true;
      }
    }
    return false;
  }

  if (const auto& key_constraint = std::dynamic_pointer_cast<TableKeyConstraint>(constraint)) {
    const auto& current_constraints = table->soft_key_constraints();
    for (const auto& current_constraint : current_constraints) {
      if (current_constraint.columns() == key_constraint->columns()) {
        return true;
      }
    }
    return false;
  }

  if (const auto& foreign_key_constraint = std::dynamic_pointer_cast<ForeignKeyConstraint>(constraint)) {
    const auto& current_constraints = table->soft_foreign_key_constraints();
    return current_constraints.contains(*foreign_key_constraint);
  }

  Fail("Invalid table constraint.");
}

std::shared_ptr<AbstractTableConstraint> AbstractDependencyValidationRule::_constraint_from_candidate(
    const AbstractDependencyCandidate& candidate) const {
  switch (candidate.type) {
    case DependencyType::UniqueColumn: {
      const auto& ucc_candidate = static_cast<const UccCandidate&>(candidate);
      return std::make_shared<TableKeyConstraint>(std::set<ColumnID>{ucc_candidate.column_id},
                                                  KeyConstraintType::UNIQUE);
    }
    case DependencyType::Order: {
      const auto& od_candidate = static_cast<const OdCandidate&>(candidate);
      return std::make_shared<TableOrderConstraint>(std::vector<ColumnID>{od_candidate.column_id},
                                                    std::vector<ColumnID>{od_candidate.ordered_column_id});
    }
    case DependencyType::Inclusion: {
      const auto& ind_candidate = static_cast<const IndCandidate&>(candidate);
      const auto& table = Hyrise::get().storage_manager.get_table(ind_candidate.table_name);
      const auto& foreign_key_table = Hyrise::get().storage_manager.get_table(ind_candidate.foreign_key_table);
      return std::make_shared<ForeignKeyConstraint>(std::vector<ColumnID>{ind_candidate.foreign_key_column_id},
                                                    std::vector<ColumnID>{ind_candidate.column_id}, foreign_key_table,
                                                    table);
    }
  }

  Fail("Invalid dependency candidate");
}

}  // namespace hyrise
