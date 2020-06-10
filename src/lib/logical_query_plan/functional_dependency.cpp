#include "functional_dependency.hpp"

namespace opossum {

FunctionalDependency::FunctionalDependency(ExpressionUnorderedSet init_determinants,
                                           ExpressionUnorderedSet init_dependents)
    : determinants(std::move(init_determinants)), dependents(std::move(init_dependents)) {
  DebugAssert(!determinants.empty() && !dependents.empty(), "FunctionalDependency cannot be empty");
}

bool FunctionalDependency::operator==(const FunctionalDependency& other) const {
  // Cannot use unordered_set::operator== because it ignores the custom equality function.
  // https://stackoverflow.com/questions/36167764/can-not-compare-stdunorded-set-with-custom-keyequal

  // Quick check for cardinality
  if (determinants.size() != other.determinants.size() || dependents.size() != other.dependents.size()) return false;

  // Compare determinants
  for (const auto& expression : other.determinants) {
    if (!determinants.contains(expression)) return false;
  }
  // Compare dependants
  for (const auto& expression : other.dependents) {
    if (!dependents.contains(expression)) return false;
  }

  return true;
}

std::ostream& operator<<(std::ostream& stream, const FunctionalDependency& expression) {
  stream << "{";
  auto determinants_vector =
      std::vector<std::shared_ptr<AbstractExpression>>{expression.determinants.begin(), expression.determinants.end()};
  stream << determinants_vector.at(0)->as_column_name();
  for (auto determinant_idx = size_t{1}; determinant_idx < determinants_vector.size(); ++determinant_idx) {
    stream << ", " << determinants_vector[determinant_idx]->as_column_name();
  }

  stream << "} => {";
  auto dependents_vector =
      std::vector<std::shared_ptr<AbstractExpression>>{expression.dependents.begin(), expression.dependents.end()};
  stream << dependents_vector.at(0)->as_column_name();
  for (auto dependent_idx = size_t{1}; dependent_idx < dependents_vector.size(); ++dependent_idx) {
    stream << ", " << dependents_vector[dependent_idx]->as_column_name();
  }
  stream << "}";

  return stream;
}

}  // namespace opossum
