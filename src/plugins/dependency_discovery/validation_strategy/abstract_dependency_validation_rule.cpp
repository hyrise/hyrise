#include "abstract_dependency_validation_rule.hpp"

#include <magic_enum.hpp>

#include "hyrise.hpp"
#include "storage/constraints/foreign_key_constraint.hpp"
#include "storage/constraints/table_key_constraint.hpp"
#include "storage/constraints/table_order_constraint.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

bool dependency_already_known(const AbstractDependencyCandidate& candidate) {
  switch (candidate.type) {
    case DependencyType::Order: {
      const auto& od_candidate = static_cast<const OdCandidate&>(candidate);
      const auto& table = Hyrise::get().storage_manager.get_table(od_candidate.table_name);
      const auto& current_constraints = table->soft_order_constraints();
      for (const auto& current_constraint : current_constraints) {
        if (current_constraint.ordering_columns().size() == 1 &&
            current_constraint.ordering_columns().front() == od_candidate.ordering_column_id &&
            current_constraint.ordered_columns().front() == od_candidate.ordered_column_id) {
          return true;
        }
      }
      return false;
    }

    case DependencyType::UniqueColumn: {
      const auto& ucc_candidate = static_cast<const UccCandidate&>(candidate);
      const auto& table = Hyrise::get().storage_manager.get_table(ucc_candidate.table_name);
      const auto& current_constraints = table->soft_key_constraints();
      for (const auto& current_constraint : current_constraints) {
        if (current_constraint.columns().size() == 1 &&
            *current_constraint.columns().cbegin() == ucc_candidate.column_id) {
          return true;
        }
      }
      return false;
    }

    case DependencyType::Inclusion: {
      const auto& ind_candidate = static_cast<const IndCandidate&>(candidate);
      const auto& table = Hyrise::get().storage_manager.get_table(ind_candidate.foreign_key_table);
      const auto& current_constraints = table->soft_foreign_key_constraints();
      const auto& primary_key_table = Hyrise::get().storage_manager.get_table(ind_candidate.primary_key_table);
      for (const auto& current_constraint : current_constraints) {
        if (current_constraint.primary_key_table() != primary_key_table) {
          continue;
        }
        const auto& primary_key_columns = current_constraint.primary_key_columns();
        const auto& foreign_key_columns = current_constraint.foreign_key_columns();
        const auto column_count = primary_key_columns.size();
        for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
          if (primary_key_columns[column_id] == ind_candidate.primary_key_column_id &&
              foreign_key_columns[column_id] == ind_candidate.foreign_key_column_id) {
            return true;
          }
        }
      }
      return false;
    }

    case DependencyType::Functional: {
      const auto& fd_candidate = static_cast<const FdCandidate&>(candidate);
      const auto& table = Hyrise::get().storage_manager.get_table(fd_candidate.table_name);
      const auto& key_constraints = table->soft_key_constraints();

      for (const auto& key_constraint : key_constraints) {
        if (key_constraint.columns().size() == 1 &&
            std::find(fd_candidate.column_ids.cbegin(), fd_candidate.column_ids.cend(),
                      *key_constraint.columns().cbegin()) != fd_candidate.column_ids.cend()) {
          return true;
        }
      }

      return false;
    }
  }

  Fail("Invalid table constraint.");
}

bool dependency_superfluous(const AbstractDependencyCandidate& candidate) {
  if (candidate.type != DependencyType::Inclusion) {
    return false;
  }

  const auto& dependents = static_cast<const IndCandidate&>(candidate).dependents;
  if (dependents.empty()) {
    return false;
  }

  return std::all_of(dependents.cbegin(), dependents.cend(),
                     [](const auto& dependent) { return dependent->status == ValidationStatus::Invalid; });
}

}  // namespace

namespace hyrise {

ValidationResult::ValidationResult(const ValidationStatus init_status) : status{init_status} {}

AbstractDependencyValidationRule::AbstractDependencyValidationRule(const DependencyType init_dependency_type)
    : dependency_type(init_dependency_type) {}

ValidationResult AbstractDependencyValidationRule::validate(const AbstractDependencyCandidate& candidate) const {
  Assert(candidate.type == dependency_type, "Wrong dependency type: Expected " +
                                                std::string{magic_enum::enum_name(dependency_type)} + ", got " +
                                                std::string{magic_enum::enum_name(candidate.type)});
  if (dependency_already_known(candidate)) {
    return ValidationResult{ValidationStatus::AlreadyKnown};
  }

  if (dependency_superfluous(candidate)) {
    return ValidationResult{ValidationStatus::Superfluous};
  }

  return _on_validate(candidate);
}

std::shared_ptr<AbstractTableConstraint> AbstractDependencyValidationRule::_constraint_from_candidate(
    const AbstractDependencyCandidate& candidate) {
  switch (candidate.type) {
    case DependencyType::UniqueColumn: {
      const auto& ucc_candidate = static_cast<const UccCandidate&>(candidate);
      return std::make_shared<TableKeyConstraint>(std::set<ColumnID>{ucc_candidate.column_id},
                                                  KeyConstraintType::UNIQUE);
    }
    case DependencyType::Order: {
      const auto& od_candidate = static_cast<const OdCandidate&>(candidate);
      return std::make_shared<TableOrderConstraint>(std::vector<ColumnID>{od_candidate.ordering_column_id},
                                                    std::vector<ColumnID>{od_candidate.ordered_column_id});
    }
    case DependencyType::Inclusion: {
      const auto& ind_candidate = static_cast<const IndCandidate&>(candidate);
      const auto& foreign_key_table = Hyrise::get().storage_manager.get_table(ind_candidate.foreign_key_table);
      const auto& primary_key_table = Hyrise::get().storage_manager.get_table(ind_candidate.primary_key_table);
      return std::make_shared<ForeignKeyConstraint>(std::vector{ind_candidate.foreign_key_column_id}, foreign_key_table,
                                                    std::vector{ind_candidate.primary_key_column_id},
                                                    primary_key_table);
    }
    case DependencyType::Functional: {
      Fail("FdCandidate cannot be translated into a table constraint");
    }
  }

  Fail("Invalid dependency candidate");
}

}  // namespace hyrise
