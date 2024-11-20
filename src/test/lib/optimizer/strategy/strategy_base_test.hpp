#pragma once

#include <memory>

#include "base_test.hpp"

namespace hyrise {

class AbstractLQPNode;
class AbstractRule;

class StrategyBaseTest : public BaseTest {
 protected:
  /**
   * Helper method for applying a single rule to an LQP. Creates the temporary LogicalPlanRootNode and applies the rule.
   *
   * When sharing nodes between expected and actual LQP, e.g., `_lqp = PredicateNode::make(..., node_a,);` and `const
   * auto expected_lqp = PredicateNode::make(..., node_a,);`, consider to assign a deep copy of the plan to _lqp
   * (`_lqp = PredicateNode::make(...)->deep_copy()`). Otherwise, the expected LQP also points to rewritten parts.
   *
   * When a rule should not rewrite the LQP, assign the expected LQP to a deep copy of the input LQP BEFORE applying
   * the rule. If you do it afterwards, the expected LQP will just be a copy of the optimized LQP (which is of course
   * always equal).
   *
   * Always use `EXPECT_LQP_EQ(...);` rather than `EXPECT_EQ(...);` to compare LQPs: the first macro checks that both
   * LQPs are equivalent, the second macro checks that both point to the same instance.
   */
  void _apply_rule(const std::shared_ptr<AbstractRule>& rule, std::shared_ptr<AbstractLQPNode>& input);

  /**
   * We declare a member variable of the abstract class to account for rewrites that replace nodes with nodes of a
   * different type. For example, we cannot declare `auto lqp = JoinNode::make(...);` and rewrite it to, e.g., a
   * PredicateNode.
   */
  std::shared_ptr<AbstractLQPNode> _lqp;
};

}  // namespace hyrise
