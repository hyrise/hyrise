#include <memory>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "expression/expression_factory.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

using namespace opossum::expression_factory;

namespace opossum {

class LimitNodeTest : public ::testing::Test {
 protected:
  void SetUp() override { _limit_node = LimitNode::make(value(10)); }

  std::shared_ptr<LimitNode> _limit_node;
};

TEST_F(LimitNodeTest, Description) { EXPECT_EQ(_limit_node->description(), "[Limit] 10"); }

TEST_F(LimitNodeTest, Equals) {
  EXPECT_TRUE(!lqp_find_subplan_mismatch(_limit_node, _limit_node));
  EXPECT_TRUE(!lqp_find_subplan_mismatch(LimitNode::make(value(10)), _limit_node));
  EXPECT_TRUE(lqp_find_subplan_mismatch(LimitNode::make(value(11)), _limit_node).has_value());
}

TEST_F(LimitNodeTest, Copy) { EXPECT_TRUE(!lqp_find_subplan_mismatch(_limit_node->deep_copy(), _limit_node)); }

}  // namespace opossum
