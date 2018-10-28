#pragma once

#include <unordered_map>

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/logical_expression.hpp"

namespace opossum {

class AbstractLQPNode;

/**
 * This rule tries to simpifly logical expressions. Currently, it only looks at disjunctive chains, i.e., chains of
 * ORs: `(a AND b) OR (a AND c) OR (a AND d)`. In this example, `a` can be extracted: `a AND (b OR c OR d)`.
 * Furthermore, if the expression is used by a predicate, this rule extracts `a` into its own PredicateNode, which
 * can then be independently pushed down or used for detecting joins. A good example can be seen in TPC-H query 19.
 * Without this rule, `p_partkey = l_partkey` would not be available as a join predicate and the predicates on
 * `l_shipmode` and `l_shipinstruct` could not be pulled below the join.
 * Conjunctive chains, i.e., `(a OR b) AND (a OR c) -> a OR (b AND c)` are currently ignored, as they don't allow
 * us to fully extract predicates.
 */
class LogicalReductionRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

  static std::shared_ptr<AbstractExpression> reduce_distributivity(
      const std::shared_ptr<AbstractExpression>& expression);
};

}  // namespace opossum
