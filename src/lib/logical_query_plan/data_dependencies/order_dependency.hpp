#pragma once

#include "expression/abstract_expression.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * Container structure to define order dependencies for LQP nodes. An order dependency must not be mistaken for
 * sortedness. It just expresses that if we sort a table by the `expression` columns, also the `ordered_expression`
 * columns are sorted.
 *
 * NOTE: Order dependencies (ODs) are only valid for LQP nodes that contain no invalidated rows (i.e., where there has
 *       been a ValidateNode before or where MVCC is disabled).
 */
struct OrderDependency final {
  explicit OrderDependency(std::vector<std::shared_ptr<AbstractExpression>> init_expressions,
                           std::vector<std::shared_ptr<AbstractExpression>> init_ordered_expessions);

  bool operator==(const OrderDependency& rhs) const;
  bool operator!=(const OrderDependency& rhs) const;
  size_t hash() const;

  std::vector<std::shared_ptr<AbstractExpression>> expressions;
  std::vector<std::shared_ptr<AbstractExpression>> ordered_expressions;
};

std::ostream& operator<<(std::ostream& stream, const OrderDependency& od);

using OrderDependencies = std::unordered_set<OrderDependency>;

// Construct all transitive ODs. For instance, create [a] |-> [c] from [a] |-> [b] and [b] |-> [c].
void build_transitive_od_closure(OrderDependencies& order_dependencies);

}  // namespace hyrise

namespace std {

template <>
struct hash<hyrise::OrderDependency> {
  size_t operator()(const hyrise::OrderDependency& od) const;
};

}  // namespace std
