#include "functional_dependency.hpp"

#include "boost/functional/hash.hpp"

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

bool FunctionalDependency::operator!=(const FunctionalDependency& other) const { return !(other == *this); }

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

std::vector<FunctionalDependency> merge_fds(const std::vector<FunctionalDependency>& fds_a,
                                            const std::vector<FunctionalDependency>& fds_b) {
  DebugAssert(!fds_a.empty() && !fds_b.empty(), "Did not expect empty FD vector(s).");

  auto fds_set = std::unordered_set<FunctionalDependency>(fds_a.begin(), fds_a.end());
  DebugAssert(fds_a.size() == fds_set.size(),
              "Did not expect input vector to contain multiple FDs with the same determinant expressions");

  for(auto& fd : fds_b) {
    auto [existing_fd, inserted] = fds_set.insert(fd);
    if(!inserted) {
      // An FD with the same determinant expressions already exists. Therefore, we only have to merge the dependent
      // expressions
      existing_fd->dependents.insert(fd.dependents.begin(), fd.dependents.end());
      // TODO Add to existing_fd.dependents – what is the problem here?
    }
  }

  return std::vector<FunctionalDependency>(fds_set.begin(), fds_set.end());
}

}  // namespace opossum

namespace std {

size_t hash<opossum::FunctionalDependency>::operator()(const opossum::FunctionalDependency& fd) const {
  return boost::hash_range(fd.determinants.cbegin(), fd.determinants.cend());
}

}  // namespace std
