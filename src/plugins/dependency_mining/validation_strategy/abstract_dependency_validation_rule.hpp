#pragma once

#include "dependency_mining/util.hpp"
#include "storage/abstract_table_constraint.hpp"

namespace opossum {

enum class DependencyValidationStatus { Valid, Invalid, Uncertain };

struct ValidationResult {
  explicit ValidationResult(const DependencyValidationStatus init_status) : status(init_status) {}
  DependencyValidationStatus status;
  std::unordered_map<std::string, std::vector<std::shared_ptr<AbstractTableConstraint>>> constraints;
};

static const std::shared_ptr<ValidationResult>& UNCERTAIN_VALIDATION_RESULT =
    std::make_shared<ValidationResult>(DependencyValidationStatus::Uncertain);
static const std::shared_ptr<ValidationResult>& INVALID_VALIDATION_RESULT =
    std::make_shared<ValidationResult>(DependencyValidationStatus::Invalid);

class AbstractDependencyValidationRule {
 public:
  explicit AbstractDependencyValidationRule(const DependencyType type);
  virtual ~AbstractDependencyValidationRule() = default;

  // we assume not to get candidates multiple times here
  std::shared_ptr<ValidationResult> validate(const DependencyCandidate& candidate) const;
  const DependencyType dependency_type;

 protected:
  bool _is_known(const std::string& table_name, const AbstractTableConstraint& constraint) const;
  virtual std::shared_ptr<ValidationResult> _on_validate(const DependencyCandidate& candidate) const = 0;
  std::pair<std::string, std::shared_ptr<AbstractTableConstraint>> _constraint_from_candidate(
      const DependencyCandidate& candidate) const;
};

}  // namespace opossum
