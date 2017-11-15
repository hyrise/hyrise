#pragma once

#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

namespace opossum {

class AbstractLogicalQueryPlanNode;
class AbstractRule;

class StrategyBaseTest : public BaseTest {
 protected:
  void TearDown() override {}

  /**
   * Helper method for applying a single rule to an LQP. Creates the temporary LogicalPlanRootNode and returns its child
   * after applying the rule
   */
  std::shared_ptr<AbstractLogicalQueryPlanNode> apply_rule(const std::shared_ptr<AbstractRule>& rule,
                                              const std::shared_ptr<AbstractLogicalQueryPlanNode>& input);
};

}  // namespace opossum
