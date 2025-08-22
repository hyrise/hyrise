#include "functional_dependency.hpp"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <ostream>
#include <tuple>
#include <unordered_map>
#include <utility>

#include <boost/container_hash/hash.hpp>

#include "expression/abstract_expression.hpp"
#include "utils/assert.hpp"
#include "utils/print_utils.hpp"

namespace hyrise {

FunctionalDependency::FunctionalDependency(ExpressionUnorderedSet&& init_determinants,
                                           ExpressionUnorderedSet&& init_dependents, bool init_is_genuine)
    : determinants{std::move(init_determinants)}, dependents{std::move(init_dependents)}, is_genuine{init_is_genuine} {
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
                                                          ExpressionUnorderedSet{dependent}, fd.is_genuine);
      if (!inserted && fd.is_genuine && !existing_fd->is_genuine) {
        existing_fd->is_genuine = true;
      }
    }
  }

  return inflated_fds;
}

FunctionalDependencies deflate_fds(const FunctionalDependencies& fds) {
  if (fds.empty()) {
    return {};
  }

  const auto hash_pair = [](const auto& key) {
    auto hash = size_t{0};
    boost::hash_combine(hash, key.first.size());
    for (const auto& expression : key.first) {
      hash ^= expression->hash();
    }

    // Include the genuine flag in the hash.
    boost::hash_combine(hash, key.second);
    return hash;
  };

  const auto pair_equal = [](const auto& lhs, const auto& rhs) {
    // Early out if the genuineness or the number of determinants differ.
    if (lhs.second != rhs.second || lhs.first.size() != rhs.first.size()) {
      return false;
    }

    return std::ranges::all_of(lhs.first, [&](const auto& expression) {
      return rhs.first.contains(expression);
    });
  };

  // We use this hash map to collect the dependents for each unique determinant set and its genuineness.
  auto existing_fds = std::unordered_map<std::pair<ExpressionUnorderedSet, bool>, ExpressionUnorderedSet,
                                         decltype(hash_pair), decltype(pair_equal)>(fds.size(), hash_pair, pair_equal);

  for (const auto& fd_to_add : fds) {
    // Try only inserting the FD first.
    auto [existing_fd, inserted] =
        existing_fds.emplace(std::pair(fd_to_add.determinants, fd_to_add.is_genuine), fd_to_add.dependents);
    if (!inserted) {
      auto& dependents = existing_fd->second;
      dependents.insert(fd_to_add.dependents.cbegin(), fd_to_add.dependents.cend());
    }

    // If the FD is genuine, we add the determinants to the non-genuine FD with the same determinants.
    if (fd_to_add.is_genuine) {
      auto [existing_fd, inserted] =
          existing_fds.emplace(std::pair(fd_to_add.determinants, false), fd_to_add.dependents);
      if (!inserted) {
        auto& dependents = existing_fd->second;
        dependents.insert(fd_to_add.dependents.cbegin(), fd_to_add.dependents.cend());
      }
    }
  }

  auto deflated_fds = FunctionalDependencies{existing_fds.size()};
  for (auto& [key, dependents] : existing_fds) {
    const auto [existing_fd, inserted] =
        deflated_fds.emplace(ExpressionUnorderedSet{key.first}, std::move(dependents), key.second);

    if (!inserted && key.second) {
      // If the FD was already in the set and is genuine, we set the already existing FD to genuine as well.
      // This is necessary because we might have added the FD with the same determinants but without the genuine
      // flag.
      existing_fd->is_genuine = true;
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

  // Make sure not to prefer genuine FDs.
  for (const auto& fd : fds_b) {
    const auto [existing_fd, inserted] = fds_unified.insert(fd);
    if (!inserted && !fd.is_genuine) {
      existing_fd->is_genuine = false;
    }
  }

  // To get rid of potential duplicates, we call deflate before returning.
  return deflate_fds(fds_unified);
}

FunctionalDependencies intersect_fds(const FunctionalDependencies& fds_a, const FunctionalDependencies& fds_b) {
  if (fds_a.empty() || fds_b.empty()) {
    return {};
  }

  const auto inflated_fds_a = inflate_fds(fds_a);
  const auto inflated_fds_b = inflate_fds(fds_b);

  auto intersected_fds = FunctionalDependencies();
  intersected_fds.reserve(fds_a.size());

  // Make sure not to prefer genuine FDs.
  for (const auto& fd_a : inflated_fds_a) {
    const auto fd_b = inflated_fds_b.find(fd_a);
    if (fd_b != inflated_fds_b.end()) {
      intersected_fds.emplace(ExpressionUnorderedSet{fd_a.determinants}, ExpressionUnorderedSet{fd_a.dependents},
                              fd_a.is_genuine && fd_b->is_genuine);
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
