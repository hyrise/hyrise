#pragma once

#include "abstract_dependency_validation_rule.hpp"

namespace opossum {

class UCCValidationRuleSpider : public AbstractDependencyValidationRule {
 public:
  UCCValidationRuleSpider();

 protected:
  std::shared_ptr<ValidationResult> _on_validate(const DependencyCandidate& candidate) const final;
};

}  // namespace opossum
