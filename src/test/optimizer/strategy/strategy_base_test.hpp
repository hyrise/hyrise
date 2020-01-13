#pragma once

#include <memory>

#include "base_test.hpp"

namespace opossum {

class AbstractLQPNode;
class AbstractRule;

class StrategyBaseTest : public BaseTest {
 protected:
  /**
   * Helper method for applying a single rule to an LQP. Creates the temporary LogicalPlanRootNode and returns its input
   * after applying the rule
   */
  std::shared_ptr<AbstractLQPNode> apply_rule(const std::shared_ptr<AbstractRule>& rule,
                                              const std::shared_ptr<AbstractLQPNode>& input);
};

}  // namespace opossum
