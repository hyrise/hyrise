#pragma once

#include "abstract_dependency_validation_rule.hpp"

namespace hyrise {

class FdValidationRule : public AbstractDependencyValidationRule {
 public:
  FdValidationRule();

 protected:
  ValidationResult _on_validate(const AbstractDependencyCandidate& candidate) const override;
};

}  // namespace hyrise
