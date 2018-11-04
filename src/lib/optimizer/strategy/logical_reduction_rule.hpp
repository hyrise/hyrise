#pragma once

#include <unordered_map>

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/logical_expression.hpp"

namespace opossum {

class AbstractLQPNode;

/**
 * This rule tries to simplify logical expressions, so that they can be processed faster.
 *
 * Reverse Distributivity
 *   Expressions of the form `(a AND b) OR (a AND c)` are rewritten as `a AND (b OR c)`.
 *
 * Also, this rule turns conjunctive chains (e.g. `a AND b AND c`) into multiple consecutive PredicateNodes,
 *   because other Optimizer rules can work better with simpler predicates.
 *
 * EXAMPLE: TPC-H query 19.
 *   Without this rule, `p_partkey = l_partkey` would not be available as a join predicate and the predicates on
 *   `l_shipmode` and `l_shipinstruct` could not be pulled below the join.
 *   Conjunctive chains, i.e., `(a OR b) AND (a OR c) -> a OR (b AND c)` are currently ignored, as they don't allow
 *   us to fully extract predicates.
 */
class LogicalReductionRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

  /**
   * Use the law of boolean distributivity to reduce an expression
   * `(a AND b) OR (a AND c)` becomes `a AND (b OR c)`
   */
  static std::shared_ptr<AbstractExpression> reduce_distributivity(
      const std::shared_ptr<AbstractExpression>& input_expression);
};

}  // namespace opossum
