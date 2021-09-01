#pragma once

#include "abstract_dependency_validation_rule.hpp"

namespace opossum {

class ODValidationRule : public AbstractDependencyValidationRule {
 public:
  ODValidationRule(tbb::concurrent_unordered_map<std::string, std::shared_ptr<std::mutex>>& table_constraint_mutexes);
  constexpr static uint64_t SAMPLE_ROW_COUNT = 100;

 protected:
  ValidationResult _on_validate(const DependencyCandidate& candidate) const final override;
};

}  // namespace opossum
