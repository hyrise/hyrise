#pragma once

#include "abstract_dependency_validation_rule.hpp"

namespace opossum {

class INDValidationRuleSet : public AbstractDependencyValidationRule {
 public:
  INDValidationRuleSet();

 protected:
  std::shared_ptr<ValidationResult> _on_validate(const DependencyCandidate& candidate) const final override;
};

}  // namespace opossum
