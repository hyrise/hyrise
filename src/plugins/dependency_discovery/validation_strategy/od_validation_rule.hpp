#pragma once

#include <unordered_set>

#include "abstract_dependency_validation_rule.hpp"

namespace hyrise {

class Table;

class OdValidationRule : public AbstractDependencyValidationRule {
 public:
  OdValidationRule();

 protected:
  ValidationResult _on_validate(const AbstractDependencyCandidate& candidate) const override;
};

}  // namespace hyrise
