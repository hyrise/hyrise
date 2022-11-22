#pragma once

#include "expression/abstract_expression.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * Container structure to define uniqueness for subsets of LQP output expressions. Analogous to SQL's UNIQUE
 * constraint, rows containing NULL values in any of the expressions are always considered to be distinct. For
 * PRIMARY KEY semantics, check if the expressions are nullable, cf. AbstractLQPNode::is_column_nullable.
 *
 * NOTE: Unique column combinations (UCCs) are only valid for LQP nodes that contain no invalidated rows (i.e., where
 *       there has been a ValidateNode before or where MVCC is disabled).
 */
struct OrderDependency final {
  explicit OrderDependency(std::vector<std::shared_ptr<AbstractExpression>> init_dependents,
                           std::vector<std::shared_ptr<AbstractExpression>> init_determinants);

  bool operator==(const OrderDependency& rhs) const;
  bool operator!=(const OrderDependency& rhs) const;
  size_t hash() const;

  std::vector<std::shared_ptr<AbstractExpression>> determinants;
  std::vector<std::shared_ptr<AbstractExpression>> dependents;
};

std::ostream& operator<<(std::ostream& stream, const OrderDependency& ucc);

using OrderDependencies = std::vector<OrderDependency>;

}  // namespace hyrise

namespace std {

template <>
struct hash<hyrise::OrderDependency> {
  size_t operator()(const hyrise::OrderDependency& ucc) const;
};

}  // namespace std
