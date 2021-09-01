#include "fd_validation_rule.hpp"

#include "ucc_validation_rule.hpp"

namespace opossum {

FDValidationRule::FDValidationRule(
    tbb::concurrent_unordered_map<std::string, std::shared_ptr<std::mutex>>& table_constraint_mutexes)
    : AbstractDependencyValidationRule(DependencyType::Functional, table_constraint_mutexes),
      _ucc_rule(std::make_unique<UCCValidationRule>(table_constraint_mutexes)) {}

ValidationResult FDValidationRule::_on_validate(const DependencyCandidate& candidate) const {
  Assert(candidate.dependents.empty(), "Invalid dependents for FD");
  bool has_ucc = false;
  for (const auto& determinant : candidate.determinants) {
    has_ucc |= _ucc_rule->is_valid({TableColumnIDs{determinant}, {}, DependencyType::Unique, 0}) ==
               DependencyValidationStatus::Valid;
  }
  // UCCs have been set by UCC rule, no need for returning constraints
  if (has_ucc) return {DependencyValidationStatus::Valid, {}};
  return UNCERTAIN_VALIDATION_RESULT;
}

}  // namespace opossum
