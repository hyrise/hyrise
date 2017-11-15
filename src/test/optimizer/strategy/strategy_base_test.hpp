#pragma once

#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

namespace opossum {

class AbstractLogicalPlanNode;
class AbstractRule;

class StrategyBaseTest : public BaseTest {
 protected:
  void TearDown() override {}

  /**
   * Helper method for applying a single rule to an AST. Creates the temporary ASTRootNode and returns its child
   * after applying the rule
   */
  std::shared_ptr<AbstractLogicalPlanNode> apply_rule(const std::shared_ptr<AbstractRule>& rule,
                                              const std::shared_ptr<AbstractLogicalPlanNode>& input);
};

}  // namespace opossum
