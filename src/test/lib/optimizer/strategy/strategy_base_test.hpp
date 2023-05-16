#pragma once

#include "base_test.hpp"

namespace hyrise {

class AbstractLQPNode;
class AbstractRule;

class StrategyBaseTest : public BaseTest {
 protected:
  /**
   * Helper method for applying a single rule to an LQP. Creates the temporary LogicalPlanRootNode and applies the rule.
   */
  void _apply_rule(const std::shared_ptr<AbstractRule>& rule, std::shared_ptr<AbstractLQPNode>& input);

  // We declare a member variable of the abstract class to account for rewrites that replace nodes with other nodes of
  // different types. For example, we cannot declare auto lqp = JoinNode::make(...) and rewrite it to, e.g., a
  // PredicateNode.
  std::shared_ptr<AbstractLQPNode> _lqp;
};

}  // namespace hyrise
