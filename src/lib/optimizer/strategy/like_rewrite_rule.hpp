#pragma once

#include <unordered_map>

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/logical_expression.hpp"

namespace opossum {

class AbstractLQPNode;

/**
 * This rule tries to simplify logical expressions, so that they can be processed faster during operator execution or
 *   be further handled by other Optimizer rules.
 *
 * LIKE to BetweenUpperExclusive
 *   `<expression> LIKE <pattern>` can be rewritten to BetweenUpperExclusive if `<pattern>` is a prefix wildcard
 *   literal.
 *   E.g., `a LIKE 'abc%'` becomes `a BetweenUpperExclusive 'abc' AND 'abd'`
 *
 * NOT LIKE to LessThan-Or-GreaterThanEquals
 *   `<expression> NOT LIKE <pattern>` can be rewritten to a LessThan-Or-GreaterThanEquals scan if `<pattern>` is a
 *   prefix wildcard literal.
 *   E.g., `a NOT LIKE 'abc%'` becomes `a < 'abc' OR a >= 'abcd'`
 *
 * LIKE to two LIKEs
 *   `<expression> LIKE <pattern>` can be rewritten into two more efficient LIKEs if `<pattern>` is a prefix wildcard
 *   followed by a string and another wildcard.
 *   E.g., `a LIKE 'abc%def%'` becomes `a LIKE BetweenUpperExclusive 'abc' AND 'abd'`
 */
class LikeRewriteRule : public AbstractRule {
 public:
  std::string name() const override;

  /**
   * Rewrite `a LIKE 'abc%'` to `a BetweenUpperExclusive 'abc' AND 'abd'`
   * Rewrite `a NOT LIKE 'abc%'` to `a < 'abc' OR a >= 'abcd'`
   */
  static void rewrite_like_prefix_wildcard(const std::shared_ptr<AbstractLQPNode>& sub_node,
                                           std::shared_ptr<AbstractExpression>& input_expression);

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;
};

}  // namespace opossum
