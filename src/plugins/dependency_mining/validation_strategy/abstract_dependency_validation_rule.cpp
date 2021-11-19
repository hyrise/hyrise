#include "abstract_dependency_validation_rule.hpp"

#include <magic_enum.hpp>

#include "hyrise.hpp"
#include "storage/table_key_constraint.hpp"
#include "storage/table_order_constraint.hpp"

namespace opossum {

AbstractDependencyValidationRule::AbstractDependencyValidationRule(const DependencyType type) : dependency_type(type) {}

std::shared_ptr<ValidationResult> AbstractDependencyValidationRule::validate(
    const DependencyCandidate& candidate) const {
  Assert(candidate.type == dependency_type, "Wrong candidate type: Expected " +
                                                std::string{magic_enum::enum_name(dependency_type)} + ", got " +
                                                std::string{magic_enum::enum_name(candidate.type)});
  Assert(!candidate.determinants.empty(), "Did not expect useless candidate.");
  const auto constraint = _constraint_from_candidate(candidate);
  if (constraint.second && _is_known(constraint.first, *constraint.second)) {
    const auto result = std::make_shared<ValidationResult>(DependencyValidationStatus::Valid);
    result->constraints[constraint.first].emplace_back(constraint.second);
    return result;
  }

  return _on_validate(candidate);
}

bool AbstractDependencyValidationRule::_is_known(const std::string& table_name,
                                                 const AbstractTableConstraint& constraint) const {
  const auto& table = Hyrise::get().storage_manager.get_table(table_name);
  switch (constraint.type()) {
    case TableConstraintType::Order: {
      const auto order_constraint = dynamic_cast<const TableOrderConstraint&>(constraint);
      const auto& current_constraints = table->soft_order_constraints();
      for (const auto& current_constraint : current_constraints) {
        if (current_constraint == order_constraint) {
          return true;
        }
      }
      return false;
    }
    case TableConstraintType::Key: {
      const auto unique_constraint = dynamic_cast<const TableKeyConstraint&>(constraint);
      const auto& current_constraints = table->soft_key_constraints();
      for (const auto& current_constraint : current_constraints) {
        if (current_constraint.columns() == unique_constraint.columns()) {
          return true;
        }
      }
      return false;
    }
    case TableConstraintType::Inclusion: {
      const auto inclusion_constraint = dynamic_cast<const TableInclusionConstraint&>(constraint);
      const auto& current_constraints = table->soft_inclusion_constraints();
      for (const auto& current_constraint : current_constraints) {
        if (current_constraint == inclusion_constraint) {
          return true;
        }
      }
      return false;
    }

    default:
      Fail("Unknown TableConstraintType, this shouldn't happen.");
  }

}

std::pair<std::string, std::shared_ptr<AbstractTableConstraint>>
AbstractDependencyValidationRule::_constraint_from_candidate(const DependencyCandidate& candidate) const {
  std::unordered_set<std::string> table_names;
  std::vector<ColumnID> determinant_column_ids;
  std::for_each(candidate.determinants.cbegin(), candidate.determinants.cend(), [&](const auto& table_column_id) {
    table_names.emplace(table_column_id.table_name);
    determinant_column_ids.emplace_back(table_column_id.column_id);
  });

  switch (candidate.type) {
    case DependencyType::Order: {
      std::vector<ColumnID> dependent_column_ids;
      std::for_each(candidate.dependents.cbegin(), candidate.dependents.cend(), [&](const auto& table_column_id) {
        table_names.emplace(table_column_id.table_name);
        dependent_column_ids.emplace_back(table_column_id.column_id);
      });
      Assert(table_names.size() == 1, "Did not expect multiple tables for OD");
      return std::make_pair(*table_names.begin(),
                            std::make_shared<TableOrderConstraint>(determinant_column_ids, dependent_column_ids));
    }
    case DependencyType::Unique: {
      Assert(table_names.size() == 1, "Did not expect multiple tables for UCC");
      const auto table_key_constraint = std::make_shared<TableKeyConstraint>(
          std::unordered_set<ColumnID>(determinant_column_ids.cbegin(), determinant_column_ids.cend()),
          KeyConstraintType::UNIQUE);
      return std::make_pair(*table_names.begin(), table_key_constraint);
    }
    default:
      return std::make_pair(std::string{""}, nullptr);
  }
}

}  // namespace opossum
