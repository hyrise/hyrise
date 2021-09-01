#pragma once

#include "abstract_dependency_validation_rule.hpp"

namespace opossum {

class INDValidationRule : public AbstractDependencyValidationRule {
 public:
  INDValidationRule(tbb::concurrent_unordered_map<std::string, std::shared_ptr<std::mutex>>& table_constraint_mutexes);

 protected:
  ValidationResult _on_validate(const DependencyCandidate& candidate) const final override;
};

}  // namespace opossum
