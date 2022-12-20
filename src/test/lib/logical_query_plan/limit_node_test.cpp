#include <memory>

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class LimitNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _limit_node = LimitNode::make(value_(10));
  }

  std::shared_ptr<LimitNode> _limit_node;
};

TEST_F(LimitNodeTest, Description) {
  EXPECT_EQ(_limit_node->description(), "[Limit] 10");
}

TEST_F(LimitNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*_limit_node, *_limit_node);
  EXPECT_EQ(*LimitNode::make(value_(10)), *_limit_node);
  EXPECT_NE(*LimitNode::make(value_(11)), *_limit_node);

  EXPECT_EQ(LimitNode::make(value_(10))->hash(), _limit_node->hash());
  EXPECT_NE(LimitNode::make(value_(11))->hash(), _limit_node->hash());
}

TEST_F(LimitNodeTest, Copy) {
  EXPECT_EQ(*_limit_node->deep_copy(), *_limit_node);
}

TEST_F(LimitNodeTest, NodeExpressions) {
  ASSERT_EQ(_limit_node->node_expressions.size(), 1u);
  EXPECT_EQ(*_limit_node->node_expressions.at(0u), *value_(10));
}

TEST_F(LimitNodeTest, ForwardOrderDependencies) {
  const auto mock_node = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}});
  const auto a = mock_node->get_column("a");
  const auto b = mock_node->get_column("b");
  const auto od = OrderDependency{{a}, {b}};
  mock_node->set_order_dependencies({od});
  EXPECT_EQ(mock_node->order_dependencies()->size(), 1);

  _limit_node->set_left_input(mock_node);
  const auto& order_dependencies = _limit_node->order_dependencies();
  EXPECT_EQ(order_dependencies->size(), 1);
  EXPECT_TRUE(order_dependencies->contains(od));
}

}  // namespace hyrise
