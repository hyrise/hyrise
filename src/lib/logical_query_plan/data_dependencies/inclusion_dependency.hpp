#pragma once

#include "expression/abstract_expression.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * TODO
 */
struct InclusionDependency final {
  explicit InclusionDependency(std::vector<std::shared_ptr<AbstractExpression>> init_expressions,
                               std::vector<std::shared_ptr<AbstractExpression>> init_included_expressions);

  bool operator==(const InclusionDependency& rhs) const;
  bool operator!=(const InclusionDependency& rhs) const;
  size_t hash() const;

  std::vector<std::shared_ptr<AbstractExpression>> expressions;
  std::vector<std::shared_ptr<AbstractExpression>> included_expressions;
};

std::ostream& operator<<(std::ostream& stream, const InclusionDependency& ind);

using InclusionDependencies = std::unordered_set<InclusionDependency>;

}  // namespace hyrise

namespace std {

template <>
struct hash<hyrise::InclusionDependency> {
  size_t operator()(const hyrise::InclusionDependency& ind) const;
};

}  // namespace std
