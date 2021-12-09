#pragma once

#include "abstract_dependency_validation_rule.hpp"

namespace opossum {

class FDValidationRule : public AbstractDependencyValidationRule {
 public:
  FDValidationRule();

 protected:
  std::shared_ptr<ValidationResult> _on_validate(const DependencyCandidate& candidate) const final;
  const std::unique_ptr<AbstractDependencyValidationRule> _ucc_rule;
};

}  // namespace opossum
