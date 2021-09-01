#include "abstract_dependency_validation_rule.hpp"

#include <magic_enum.hpp>

#include "hyrise.hpp"
#include "storage/table_key_constraint.hpp"
#include "storage/table_order_constraint.hpp"

namespace opossum {

AbstractDependencyValidationRule::AbstractDependencyValidationRule(
    const DependencyType type,
    tbb::concurrent_unordered_map<std::string, std::shared_ptr<std::mutex>>& table_constraint_mutexes)
    : dependency_type(type), _table_constraint_mutexes(table_constraint_mutexes) {}

DependencyValidationStatus AbstractDependencyValidationRule::is_valid(const DependencyCandidate& candidate) const {
  Assert(candidate.type == dependency_type, "Wrong candidate type: Expected " +
                                                std::string{magic_enum::enum_name(dependency_type)} + ", got " +
                                                std::string{magic_enum::enum_name(candidate.type)});
  Assert(!candidate.determinants.empty(), "Did not expect useless candidate.");
  if (_is_known(candidate)) return DependencyValidationStatus::Valid;

  const auto [status, constraints] = _on_validate(candidate);

  for (const auto& constraint : constraints) {
    // we checked if constraint is new before
    _add_constraint(constraint.first, *constraint.second);
  }
  return status;
}

bool AbstractDependencyValidationRule::_is_known(const DependencyCandidate& candidate) const {
  std::unordered_set<std::string> table_names;
  std::for_each(candidate.determinants.cbegin(), candidate.determinants.cend(),
                [&table_names](const auto& determinant) { table_names.emplace(determinant.table_name); });

  switch (candidate.type) {
    case DependencyType::Order: {
      Assert(table_names.size() == 1, "Expected OD within single table.");
      const auto& table = Hyrise::get().storage_manager.get_table(*table_names.cbegin());
      std::vector<ColumnID> determinant_column_ids;
      std::vector<ColumnID> dependent_column_ids;
      for (const auto& determinant : candidate.determinants) {
        determinant_column_ids.emplace_back(determinant.column_id);
      }
      for (const auto& dependent : candidate.dependents) {
        dependent_column_ids.emplace_back(dependent.column_id);
      }
      const auto order_constraint = TableOrderConstraint{determinant_column_ids, dependent_column_ids};

      auto& mutex = _table_constraint_mutexes[*table_names.begin()];
      if (!mutex) {
        mutex = std::make_shared<std::mutex>();
      }
      std::lock_guard<std::mutex> lock(*mutex);
      const auto& current_constraints = table->soft_order_constraints();
      for (const auto& current_constraint : current_constraints) {
        if (current_constraint == order_constraint) {
          return true;
        }
      }
    }
      return false;
    case DependencyType::Unique: {
      Assert(table_names.size() == 1, "Expected UCC within single table.");
      const auto& table = Hyrise::get().storage_manager.get_table(*table_names.begin());
      std::unordered_set<ColumnID> column_ids;
      for (const auto& determinant : candidate.determinants) {
        column_ids.emplace(determinant.column_id);
      }
      const auto unique_constraint = TableKeyConstraint{column_ids, KeyConstraintType::UNIQUE};
      auto& mutex = _table_constraint_mutexes[*table_names.begin()];
      if (!mutex) {
        mutex = std::make_shared<std::mutex>();
      }
      std::lock_guard<std::mutex> lock(*mutex);
      const auto& current_constraints = table->soft_key_constraints();
      for (const auto& current_constraint : current_constraints) {
        if (current_constraint.columns() == unique_constraint.columns()) {
          return true;
        }
      }
    }
      return false;
    default:
      return false;
  }
}

void AbstractDependencyValidationRule::_add_constraint(const std::string& table_name,
                                                       const AbstractTableConstraint& constraint) const {
  const auto& table = Hyrise::get().storage_manager.get_table(table_name);
  auto& mutex = _table_constraint_mutexes[table_name];
  std::lock_guard<std::mutex> lock(*mutex);
  switch (constraint.type()) {
    case TableConstraintType::Key:
      table->add_soft_key_constraint(dynamic_cast<const TableKeyConstraint&>(constraint));
      break;
    case TableConstraintType::Order:
      table->add_soft_order_constraint(dynamic_cast<const TableOrderConstraint&>(constraint));
      break;
  }
}

}  // namespace opossum
