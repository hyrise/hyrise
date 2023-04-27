#include "fd_validation_rule.hpp"

#include "dependency_discovery/validation_strategy/ucc_validation_rule.hpp"

namespace hyrise {

FdValidationRule::FdValidationRule() : AbstractDependencyValidationRule{DependencyType::Functional} {}

ValidationResult FdValidationRule::_on_validate(const AbstractDependencyCandidate& candidate) const {
  const auto& fd_candidate = static_cast<const FdCandidate&>(candidate);

  // We do not build a lattice and check larger FDs. We just check if one of the columns is unique.
  for (const auto column_id : fd_candidate.column_ids) {
    const auto& validation_result = UccValidationRule{}.validate(UccCandidate{fd_candidate.table_name, column_id});
    if (validation_result.status != ValidationStatus::Invalid) {
      return validation_result;
    }
  }

  return ValidationResult{ValidationStatus::Invalid};
}

}  // namespace hyrise
