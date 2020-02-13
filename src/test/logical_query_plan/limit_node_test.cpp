#include <memory>

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class LimitNodeTest : public BaseTest {
 protected:
  void SetUp() override { _limit_node = LimitNode::make(value_(10)); }

  std::shared_ptr<LimitNode> _limit_node;
};

TEST_F(LimitNodeTest, Description) { EXPECT_EQ(_limit_node->description(), "[Limit] 10"); }

TEST_F(LimitNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*_limit_node, *_limit_node);
  EXPECT_EQ(*LimitNode::make(value_(10)), *_limit_node);
  EXPECT_NE(*LimitNode::make(value_(11)), *_limit_node);

  EXPECT_EQ(LimitNode::make(value_(10))->hash(), _limit_node->hash());
  EXPECT_NE(LimitNode::make(value_(11))->hash(), _limit_node->hash());
}

TEST_F(LimitNodeTest, Copy) { EXPECT_EQ(*_limit_node->deep_copy(), *_limit_node); }

TEST_F(LimitNodeTest, NodeExpressions) {
  ASSERT_EQ(_limit_node->node_expressions.size(), 1u);
  EXPECT_EQ(*_limit_node->node_expressions.at(0u), *value_(10));
}

}  // namespace opossum
