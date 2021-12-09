#pragma once

#include "abstract_dependency_validation_rule.hpp"

namespace opossum {

class ODValidationRule : public AbstractDependencyValidationRule {
 public:
  ODValidationRule();
  constexpr static uint64_t SAMPLE_ROW_COUNT = 100;

 protected:
  std::shared_ptr<ValidationResult> _on_validate(const DependencyCandidate& candidate) const final;
};

}  // namespace opossum
