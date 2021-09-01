#include "fd_validation_rule.hpp"

#include "ucc_validation_rule.hpp"

namespace opossum {

FDValidationRule::FDValidationRule()
    : AbstractDependencyValidationRule(DependencyType::Functional), _ucc_rule(std::make_unique<UCCValidationRule>()) {}

std::shared_ptr<ValidationResult> FDValidationRule::_on_validate(const DependencyCandidate& candidate) const {
  Assert(candidate.dependents.empty(), "Invalid dependents for FD");
  const auto result = std::make_shared<ValidationResult>(DependencyValidationStatus::Uncertain);
  for (const auto& determinant : candidate.determinants) {
    const auto my_result = _ucc_rule->validate({TableColumnIDs{determinant}, {}, DependencyType::Unique, 0});
    if (my_result->status != DependencyValidationStatus::Valid) continue;
    result->status = DependencyValidationStatus::Valid;
    for (const auto& [table_name, constraints] : my_result->constraints) {
      auto& result_constraints = result->constraints[table_name];
      result_constraints.insert(result_constraints.end(), std::make_move_iterator(constraints.cbegin()),
                                std::make_move_iterator(constraints.cend()));
    }
  }

  return result;
}

}  // namespace opossum
