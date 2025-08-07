#include "functional_dependency.hpp"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <ostream>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "utils/assert.hpp"
#include "utils/print_utils.hpp"

namespace hyrise {

FunctionalDependency::FunctionalDependency(ExpressionUnorderedSet&& init_determinants,
                                           ExpressionUnorderedSet&& init_dependents, bool is_schema_given)
    : determinants{std::move(init_determinants)},
      dependents{std::move(init_dependents)},
      _is_schema_given{is_schema_given} {
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

bool FunctionalDependency::is_schema_given() const {
  return _is_schema_given;
}

void FunctionalDependency::set_schema_given() const {
  _is_schema_given = true;
}

size_t FunctionalDependency::hash() const {
  auto hash = size_t{0};
  for (const auto& expression : determinants) {
    // To make the hash independent of the expressions' order, we have to use a commutative operator like XOR.
    hash = hash ^ expression->hash();
  }

  return std::hash<size_t>{}(hash - determinants.size());
}

std::ostream& operator<<(std::ostream& stream, const FunctionalDependency& fd) {
  stream << "{";
  print_expressions(fd.determinants, stream);
  stream << "} => {";
  print_expressions(fd.dependents, stream);
  stream << "}";

  return stream;
}

FunctionalDependencies inflate_fds(const FunctionalDependencies& fds) {
  if (fds.empty()) {
    return {};
  }

  auto inflated_fds = FunctionalDependencies{fds.size()};

  for (const auto& fd : fds) {
    for (const auto& dependent : fd.dependents) {
      auto [existing_fd, inserted] = inflated_fds.emplace(ExpressionUnorderedSet{fd.determinants},
                                                          ExpressionUnorderedSet{dependent}, fd.is_schema_given());
      if (!inserted && fd.is_schema_given() && !existing_fd->is_schema_given()) {
        existing_fd->set_schema_given();
      }
    }
  }

  return inflated_fds;
}

FunctionalDependencies deflate_fds(const FunctionalDependencies& fds) {
  if (fds.empty()) {
    return {};
  }

  using Key = std::pair<ExpressionUnorderedSet, bool>;  // Determinants and schema-given flag.

  auto hash_pair = [](const Key& key) {
    auto hash = size_t{0};
    for (const auto& expression : key.first) {
      hash ^= expression->hash();
    }

    return hash ^ static_cast<size_t>(key.second);  // Include the schema-given flag in the hash.
  };

  auto pair_equal = [](const Key& lhs, const Key& rhs) {
    if (lhs.second != rhs.second) {
      return false;  // Schema-given flags differ.
    }
    for (const auto& expression : lhs.first) {
      if (!rhs.first.contains(expression)) {
        return false;  // Determinants differ.
      }
    }
    for (const auto& expression : rhs.first) {
      if (!lhs.first.contains(expression)) {
        return false;  // Determinants differ.
      }
    }
    return true;  // Determinants are equal.
  };

  // We use this hash map to collect the dependents for each unique determinant set and its genuineness.
  auto existing_fds = std::unordered_map<Key, ExpressionUnorderedSet, decltype(hash_pair), decltype(pair_equal)>(
      fds.size(), hash_pair, pair_equal);

  for (const auto& fd_to_add : fds) {
    // Try only inserting the FD first.
    auto [existing_fd, inserted] =
        existing_fds.emplace(std::pair(fd_to_add.determinants, fd_to_add.is_schema_given()), fd_to_add.dependents);
    if (!inserted) {
      auto& dependents = existing_fd->second;
      dependents.insert(fd_to_add.dependents.cbegin(), fd_to_add.dependents.cend());
    }

    // If the FD is schema-given, we add the determinants to the non-schema-given FD with the same determinants.
    if (fd_to_add.is_schema_given()) {
      auto [existing_fd, inserted] =
          existing_fds.emplace(std::pair(fd_to_add.determinants, false), fd_to_add.dependents);
      if (!inserted) {
        auto& dependents = existing_fd->second;
        dependents.insert(fd_to_add.dependents.cbegin(), fd_to_add.dependents.cend());
      }
    }
  }

  auto deflated_fds = FunctionalDependencies{};
  deflated_fds.reserve(existing_fds.size());
  for (auto& [key, dependents] : existing_fds) {
    auto [existing_fd, inserted] =
        deflated_fds.emplace(ExpressionUnorderedSet{key.first}, std::move(dependents), key.second);

    if (!inserted && key.second && !existing_fd->is_schema_given()) {
      // If the FD was already in the set and is schema-given, we set the already existing FD to schema-given as well.
      // This is necessary because we might have added the FD with the same determinants but without the schema-given
      // flag.
      existing_fd->set_schema_given();
    }
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
