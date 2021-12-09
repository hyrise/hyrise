#include "order_dependency.hpp"

#include <boost/container_hash/hash.hpp>

namespace opossum {

OrderDependency::OrderDependency(ExpressionList init_determinants, ExpressionList init_dependents)
    : determinants(std::move(init_determinants)), dependents(std::move(init_dependents)) {
  DebugAssert(!determinants.empty() && !dependents.empty(), "OrderDependency cannot be empty");
}

bool OrderDependency::operator==(const OrderDependency& other) const {
  // Cannot use unordered_set::operator== because it ignores the custom equality function.
  // https://stackoverflow.com/questions/36167764/can-not-compare-stdunorded-set-with-custom-keyequal

  // Quick check for cardinality
  if (determinants.size() != other.determinants.size() || dependents.size() != other.dependents.size()) return false;

  const auto num_determinants = determinants.size();
  // Compare determinants
  for (auto it = size_t{0}; it < num_determinants; ++it) {
    if (determinants[it] != other.determinants[it]) return false;
  }

  const auto num_dependents = dependents.size();
  // Compare determinants
  for (auto it = size_t{0}; it < num_dependents; ++it) {
    if (dependents[it] != other.dependents[it]) return false;
  }

  return true;
}

bool OrderDependency::operator!=(const OrderDependency& other) const { return !(other == *this); }

size_t OrderDependency::hash() const {
  auto hash = determinants.size();
  for (const auto& expression : determinants) {
    boost::hash_combine(hash, expression->hash());
  }
  boost::hash_combine(hash, dependents.size());
  for (const auto& expression : dependents) {
    boost::hash_combine(hash, expression->hash());
  }
  return hash;
}

std::ostream& operator<<(std::ostream& stream, const OrderDependency& expression) {
  stream << "{";
  stream << expression.determinants.at(0)->as_column_name();
  for (auto determinant_idx = size_t{1}; determinant_idx < expression.determinants.size(); ++determinant_idx) {
    stream << ", " << expression.determinants[determinant_idx]->as_column_name();
  }

  stream << "} => {";
  stream << expression.dependents.at(0)->as_column_name();
  for (auto dependent_idx = size_t{1}; dependent_idx < expression.dependents.size(); ++dependent_idx) {
    stream << ", " << expression.dependents[dependent_idx]->as_column_name();
  }
  stream << "}";

  return stream;
}

std::unordered_set<OrderDependency> inflate_ods(const std::vector<OrderDependency>& ods) {
  return {};
  /*if (fds.empty()) return {};

  auto inflated_fds = std::unordered_set<OrderDependency>();
  inflated_fds.reserve(fds.size());

  for (const auto& fd : fds) {
    if (fd.dependents.size() == 1) {
      inflated_fds.insert(fd);
    } else {
      for (const auto& dependent : fd.dependents) {
        inflated_fds.emplace(fd.determinants, ExpressionUnorderedSet{dependent});
      }
    }
  }

  return inflated_fds;*/
}

std::vector<OrderDependency> deflate_ods(const std::vector<OrderDependency>& ods) {
  return {};
  /*if (fds.empty()) return {};

  auto deflated_fds = std::vector<OrderDependency>();
  deflated_fds.reserve(fds.size());

  for (const auto& fd_to_add : fds) {
    auto existing_fd = std::find_if(deflated_fds.begin(), deflated_fds.end(), [&fd_to_add](auto& fd) {
      // Cannot use unordered_set::operator== because it ignores the custom equality function.
      // https://stackoverflow.com/questions/36167764/can-not-compare-stdunorded-set-with-custom-keyequal

      // Quick check for cardinality
      if (fd.determinants.size() != fd_to_add.determinants.size()) return false;

      // Compare determinants
      for (const auto& expression : fd_to_add.determinants) {
        if (!fd.determinants.contains(expression)) return false;
      }

      return true;
    });
    if (existing_fd == deflated_fds.end()) {
      deflated_fds.push_back(fd_to_add);
    } else {
      // An FD with the same determinant expressions already exists. Therefore, we only have to add to the dependent
      // expressions set
      existing_fd->dependents.insert(fd_to_add.dependents.begin(), fd_to_add.dependents.end());
    }
  }

  return deflated_fds;*/
}

std::vector<OrderDependency> union_ods(const std::vector<OrderDependency>& ods_a,
                                       const std::vector<OrderDependency>& ods_b) {
  return {};
  /*if constexpr (HYRISE_DEBUG) {
    auto fds_a_set = std::unordered_set<OrderDependency>(fds_a.begin(), fds_a.end());
    auto fds_b_set = std::unordered_set<OrderDependency>(fds_b.begin(), fds_b.end());
    Assert(fds_a.size() == fds_a_set.size() && fds_b.size() == fds_b_set.size(),
           "Did not expect input vector to contain multiple FDs with the same determinant expressions");
  }
  if (fds_a.empty()) return fds_b;
  if (fds_b.empty()) return fds_a;

  auto fds_unified = std::vector<OrderDependency>();
  fds_unified.reserve(fds_a.size() + fds_b.size());
  fds_unified.insert(fds_unified.end(), fds_a.begin(), fds_a.end());
  fds_unified.insert(fds_unified.end(), fds_b.begin(), fds_b.end());

  // To get rid of potential duplicates, we call deflate before returning.
  return deflate_fds(fds_unified);*/
}

std::vector<OrderDependency> intersect_ods(const std::vector<OrderDependency>& ods_a,
                                           const std::vector<OrderDependency>& ods_b) {
  return {};
  /*if (fds_a.empty() || fds_b.empty()) return {};

  const auto& inflated_fds_a = inflate_fds(fds_a);
  const auto& inflated_fds_b = inflate_fds(fds_b);

  auto intersected_fds = std::vector<OrderDependency>();
  intersected_fds.reserve(fds_a.size());

  for (const auto& fd : inflated_fds_a) {
    if (inflated_fds_b.contains(fd)) {
      intersected_fds.push_back(fd);
    }
  }

  return deflate_fds(intersected_fds);*/
}

}  // namespace opossum

namespace std {

size_t hash<opossum::OrderDependency>::operator()(const opossum::OrderDependency& od) const { return od.hash(); }

}  // namespace std
