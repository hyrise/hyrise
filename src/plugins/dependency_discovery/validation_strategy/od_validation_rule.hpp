#pragma once

#include <unordered_set>

#include "abstract_dependency_validation_rule.hpp"

namespace hyrise {

class Table;

class OdValidationRule : public AbstractDependencyValidationRule {
 public:
  OdValidationRule();

  constexpr static uint64_t SAMPLE_SIZE{100};
  constexpr static uint64_t MIN_SIZE_FOR_RANDOM_SAMPLE{SAMPLE_SIZE * 2};

 protected:
  ValidationResult _on_validate(const AbstractDependencyCandidate& candidate) const override;
};

}  // namespace hyrise
