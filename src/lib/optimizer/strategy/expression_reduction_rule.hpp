#pragma once

#include <unordered_map>

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/logical_expression.hpp"

namespace hyrise {

class AbstractLQPNode;

/**
 * This rule tries to simplify different expressions, so that they can be processed faster during operator execution or
 * be further handled by other optimizer rules.
 *
 * Reverse Distributivity
 *   Expressions of the form `(a AND b) OR (a AND c)` are rewritten as `a AND (b OR c)`.
 *   TODO(anybody): Conjunctive chains, i.e., `(a OR b) AND (a OR c) -> a OR (b AND c)` are currently ignored as it is
 *   has not been determined yet whether one or the other form executes faster.
 *
 * Constant Folding
 *   Expression involving only constants (`5 + 3 * 4`) are calculated and replaced with their result.
 *
 * LIKE to BetweenUpperExclusive
 *   `<expression> LIKE <pattern>` can be rewritten to BetweenUpperExclusive if `<pattern>` is a prefix wildcard
 *   literal.
 *   E.g., `a LIKE 'abc%'` becomes `a BetweenUpperExclusive 'abc' AND 'abd'`.
 *
 * NOT LIKE to LessThan-Or-GreaterThanEquals
 *   `<expression> NOT LIKE <pattern>` can be rewritten to a LessThan-Or-GreaterThanEquals scan if `<pattern>` is a
 *   prefix wildcard literal.
 *   E.g., `a NOT LIKE 'abc%'` becomes `a < 'abc' OR a >= 'abcd'`.
 *
 * (NOT) LIKE to Equals
 *   `<expression> (NOT) LIKE <pattern>` can be rewritten to (Not)Equals if `<pattern>` is has no wildcard.
 *   E.g., `a (NOT) LIKE 'abc'` becomes `a (Not)Equals 'abc'`.
 *
 * Duplicate Aggregate Removal
 *   The calculation of `AVG(a)` is redundant if both `SUM(a)` and `COUNT(a)` are also computed.
 *   E.g.,  `SELECT SUM(a), COUNT(a), AVG(a)` becomes `SELECT SUM(a), COUNT(a), SUM(a) / COUNT(a) AS AVG(a)`.
 *
 * IN to BinaryPredicate
 *   An InExpression with only a single element can be rewritten as BinaryPredicateExpression if the IN list has only
 *   one element. Though InExpressionRewriteRule generalizes this for multiple elements, we execute it late, whereas
 *   other optimization rules can benefit from the simpler predicate (e.g., JoinToPredicateRewriteRule).
 *   E.g., `a (NOT) IN ('abc')` becomes `a (Not)Equals 'abc'`.
 */
class ExpressionReductionRule : public AbstractRule {
 public:
  std::string name() const override;

  /**
   * Use the law of boolean distributivity to reduce an expression: `(a AND b) OR (a AND c)` becomes `a AND (b OR c)`.
   */
  static const std::shared_ptr<AbstractExpression>& reduce_distributivity(
      std::shared_ptr<AbstractExpression>& input_expression);

  /**
   * Rewrite `5 + 3` to `8`
   */
  static void reduce_constant_expression(std::shared_ptr<AbstractExpression>& input_expression);

  /**
   * Rewrite `a LIKE 'abc%'` to `a BetweenUpperExclusive 'abc' AND 'abd'`.
   * Rewrite `a NOT LIKE 'abc%'` to `a < 'abc' OR a >= 'abcd'`.
   * Rewrite `a (NOT) LIKE 'abc'` to `a (Not)Equals 'abc'`.
   */
  static void rewrite_like_prefix_wildcard(std::shared_ptr<AbstractExpression>& input_expression);

  /**
   * Rewrite `SELECT SUM(a), COUNT(a), AVG(a)` to `SELECT SUM(a), COUNT(a), SUM(a) / COUNT(a) AS AVG(a)`.
   */
  static void remove_duplicate_aggregate(std::vector<std::shared_ptr<AbstractExpression>>& input_expressions,
                                         const std::shared_ptr<AbstractLQPNode>& aggregate_node,
                                         const std::shared_ptr<AbstractLQPNode>& root_node);

  /**
   * Rewrite `a (NOT) IN ('abc')` to `a (Not)Equals 'abc'`.
   */
  static void unnest_unary_in_expression(std::shared_ptr<AbstractExpression>& input_expression);

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;
};

}  // namespace hyrise
