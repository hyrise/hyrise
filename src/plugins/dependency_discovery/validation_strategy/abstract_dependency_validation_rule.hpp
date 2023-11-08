#pragma once

#include <unordered_map>

#include "dependency_discovery/dependency_candidates.hpp"
#include "storage/constraints/abstract_table_constraint.hpp"

namespace hyrise {

class Table;

struct ValidationResult {
 public:
  explicit ValidationResult(const ValidationStatus init_status);
  ValidationResult() = delete;

  ValidationStatus status;
  // Pointer is required for polymorphism.
  std::unordered_map<std::shared_ptr<Table>, std::shared_ptr<AbstractTableConstraint>> constraints{};
};

class AbstractDependencyValidationRule {
 public:
  explicit AbstractDependencyValidationRule(const DependencyType init_dependency_type);

  AbstractDependencyValidationRule() = delete;
  virtual ~AbstractDependencyValidationRule() = default;

  ValidationResult validate(const AbstractDependencyCandidate& candidate) const;

  const DependencyType dependency_type;

 protected:
  virtual ValidationResult _on_validate(const AbstractDependencyCandidate& candidate) const = 0;

  // Pointer is required for polymorphism.
  static std::shared_ptr<AbstractTableConstraint> _constraint_from_candidate(
      const AbstractDependencyCandidate& candidate);
};

}  // namespace hyrise
