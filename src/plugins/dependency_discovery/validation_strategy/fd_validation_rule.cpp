#include "fd_validation_rule.hpp"

#include "dependency_discovery/validation_strategy/ucc_validation_rule.hpp"
#include "hyrise.hpp"
#include "resolve_type.hpp"

namespace hyrise {

FdValidationRule::FdValidationRule() : AbstractDependencyValidationRule{DependencyType::Functional} {}

ValidationResult FdValidationRule::_on_validate(const AbstractDependencyCandidate& candidate) const {
  const auto& fd_candidate = static_cast<const FdCandidate&>(candidate);
  const auto& table = Hyrise::get().storage_manager.get_table(fd_candidate.table_name);

  // Prioritize columns with integral data type. They can use more validation shortcuts and grouping with int32_t is
  // also faster in AggregateHash.
  auto ranked_column_ids = std::vector<ColumnID>{fd_candidate.column_ids.cbegin(), fd_candidate.column_ids.cend()};

  // Return true if lhs is integral and rhs is not. Else, return lhs < rhs.
  const auto compare_column_ids_by_type = [&](const auto lhs, const auto rhs) {
    auto is_smaller = false;
    resolve_data_type(table->column_data_type(lhs), [&](auto lhs_type) {
      using LhsDataType = typename decltype(lhs_type)::type;
      resolve_data_type(table->column_data_type(rhs), [&](auto rhs_type) {
        using RhsDataType = typename decltype(rhs_type)::type;

        if constexpr (std::is_integral_v<LhsDataType> && !std::is_integral_v<RhsDataType>) {
          is_smaller = true;
          return;
        }

        if constexpr (!std::is_integral_v<LhsDataType> && std::is_integral_v<RhsDataType>) {
          return;
        }

        if constexpr (std::is_arithmetic_v<LhsDataType> && !std::is_arithmetic_v<RhsDataType>) {
          is_smaller = true;
          return;
        }

        if constexpr (!std::is_arithmetic_v<LhsDataType> && std::is_arithmetic_v<RhsDataType>) {
          return;
        }

        // Both have the same type, just compare the ColumnIDs then.
        is_smaller = lhs < rhs;
      });
    });

    return is_smaller;
  };

  std::sort(ranked_column_ids.begin(), ranked_column_ids.end(), compare_column_ids_by_type);

  for (const auto column_id : ranked_column_ids) {
    const auto& validation_result = UccValidationRule{}.validate(UccCandidate{fd_candidate.table_name, column_id});
    if (validation_result.status != ValidationStatus::Invalid) {
      return validation_result;
    }
  }

  return ValidationResult{ValidationStatus::Invalid};
}

}  // namespace hyrise
