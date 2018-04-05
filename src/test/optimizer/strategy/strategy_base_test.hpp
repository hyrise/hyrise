#pragma once

#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

namespace opossum {

class AbstractLQPNode;
class AbstractRule;

class StrategyBaseTest : public BaseTest {
 protected:
  /**
   * Helper method for applying a single rule to an LQP. Creates the temporary LogicalPlanRootNode and returns its input
   * after applying the rule
   */
  AbstractLQPNodeSPtr apply_rule(const AbstractRuleSPtr& rule,
                                              const AbstractLQPNodeSPtr& input);
};

}  // namespace opossum
