#pragma once

#include "abstract_dependency_validation_rule.hpp"

namespace hyrise {

class Table;

class IndValidationRule : public AbstractDependencyValidationRule {
 public:
  IndValidationRule();

 protected:
  ValidationResult _on_validate(const AbstractDependencyCandidate& candidate) const override;
};

}  // namespace hyrise
