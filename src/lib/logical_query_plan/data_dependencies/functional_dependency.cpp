#include "functional_dependency.hpp"

#include <boost/container_hash/hash.hpp>

namespace hyrise {

FunctionalDependency::FunctionalDependency(ExpressionUnorderedSet init_determinants,
                                           ExpressionUnorderedSet init_dependents)
    : FunctionalDependency(std::move(init_determinants), std::move(init_dependents), true) {}

FunctionalDependency::FunctionalDependency(hyrise::ExpressionUnorderedSet init_determinants,
                                           hyrise::ExpressionUnorderedSet init_dependents, bool permanent)
    : determinants(std::move(init_determinants)), dependents(std::move(init_dependents)), permanent(permanent) {
  DebugAssert(!determinants.empty() && !dependents.empty(), "FunctionalDependency cannot be empty");
}

bool FunctionalDependency::operator==(const FunctionalDependency& other) const {
  // Cannot use unordered_set::operator== because it ignores the custom equality function.
  // https://stackoverflow.com/questions/36167764/can-not-compare-stdunorded-set-with-custom-keyequal

  // Quick check for cardinality.
  if (determinants.size() != other.determinants.size() || dependents.size() != other.dependents.size()) {
    return false;
  }

  // Compare determinants.
  for (const auto& expression : other.determinants) {
    if (!determinants.contains(expression)) {
      return false;
    }
  }

  // Compare dependants.
  for (const auto& expression : other.dependents) {
    if (!dependents.contains(expression)) {
      return false;
    }
  }

  return true;
}

bool FunctionalDependency::operator!=(const FunctionalDependency& other) const {
  return !(other == *this);
}

size_t FunctionalDependency::hash() const {
  auto hash = size_t{0};
  for (const auto& expression : determinants) {
    // To make the hash independent of the expressions' order, we have to use a commutative operator like XOR.
    hash = hash ^ expression->hash();
  }

  return boost::hash_value(hash - determinants.size());
}

bool FunctionalDependency::is_permanent() const {
  return permanent;
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

FunctionalDependencies inflate_fds(const FunctionalDependencies& fds) {
  if (fds.empty()) {
    return {};
  }

  auto inflated_fds = FunctionalDependencies{fds.size()};

  for (const auto& fd : fds) {
    if (fd.dependents.size() == 1) {
      inflated_fds.insert(fd);
    } else {
      for (const auto& dependent : fd.dependents) {
        inflated_fds.emplace(fd.determinants, ExpressionUnorderedSet{dependent}, fd.is_permanent());
      }
    }
  }

  return inflated_fds;
}

FunctionalDependencies deflate_fds(const FunctionalDependencies& fds) {
  if (fds.empty()) {
    return {};
  }

  // We cannot use a set here as we want to add dependents to existing FDs and objects in sets are immutable.
  auto existing_fds = std::vector<std::tuple<ExpressionUnorderedSet, ExpressionUnorderedSet, bool>>{};
  existing_fds.reserve(fds.size());

  for (const auto& fd_to_add : fds) {
    // Check if we have seen an FD with the same determinants.
    auto existing_fd = std::find_if(existing_fds.begin(), existing_fds.end(), [&](const auto& fd) {
      // Quick check for cardinality.
      const auto& determinants = std::get<0>(fd);
      if (determinants.size() != fd_to_add.determinants.size()) {
        return false;
      }

      // Compare determinants.
      for (const auto& expression : fd_to_add.determinants) {
        if (!determinants.contains(expression)) {
          return false;
        }
      }

      return true;
    });

    // If we have found an FD with same determinants, add the dependents. Otherwise, add a new FD.
    if (existing_fd != existing_fds.end()) {
      std::get<1>(*existing_fd).insert(fd_to_add.dependents.cbegin(), fd_to_add.dependents.cend());
      if (!fd_to_add.is_permanent()) {
        std::get<2>(*existing_fd) = false;
      }
    } else {
      existing_fds.emplace_back(fd_to_add.determinants, fd_to_add.dependents, fd_to_add.is_permanent());
    }
  }

  auto deflated_fds = FunctionalDependencies(fds.size());
  for (const auto& [determinants, dependents, is_permanent] : existing_fds) {
    deflated_fds.emplace(determinants, dependents, is_permanent);
  }

  return deflated_fds;
}

FunctionalDependencies union_fds(const FunctionalDependencies& fds_a, const FunctionalDependencies& fds_b) {
  if (fds_a.empty()) {
    return fds_b;
  }

  if (fds_b.empty()) {
    return fds_a;
  }

  auto fds_unified = FunctionalDependencies{fds_a.cbegin(), fds_a.cend()};
  fds_unified.reserve(fds_a.size() + fds_b.size());
  fds_unified.insert(fds_b.begin(), fds_b.end());

  // To get rid of potential duplicates, we call deflate before returning.
  return deflate_fds(fds_unified);
}

FunctionalDependencies intersect_fds(const FunctionalDependencies& fds_a, const FunctionalDependencies& fds_b) {
  if (fds_a.empty() || fds_b.empty()) {
    return {};
  }

  const auto& inflated_fds_a = inflate_fds(fds_a);
  const auto& inflated_fds_b = inflate_fds(fds_b);

  auto intersected_fds = FunctionalDependencies();
  intersected_fds.reserve(fds_a.size());

  for (const auto& fd : inflated_fds_a) {
    if (inflated_fds_b.contains(fd)) {
      intersected_fds.emplace(fd);
    }
  }

  return deflate_fds(intersected_fds);
}

}  // namespace hyrise

namespace std {

size_t hash<hyrise::FunctionalDependency>::operator()(const hyrise::FunctionalDependency& fd) const {
  return fd.hash();
}

}  // namespace std
