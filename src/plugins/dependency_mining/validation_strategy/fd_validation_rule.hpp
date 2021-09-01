#pragma once

#include "abstract_dependency_validation_rule.hpp"

namespace opossum {

class FDValidationRule : public AbstractDependencyValidationRule {
 public:
  FDValidationRule(tbb::concurrent_unordered_map<std::string, std::shared_ptr<std::mutex>>& table_constraint_mutexes);

 protected:
  ValidationResult _on_validate(const DependencyCandidate& candidate) const final override;
  const std::unique_ptr<AbstractDependencyValidationRule> _ucc_rule;
};

}  // namespace opossum
