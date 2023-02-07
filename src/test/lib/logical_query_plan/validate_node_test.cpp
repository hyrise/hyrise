#include <memory>

#include "base_test.hpp"

#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/validate_node.hpp"

namespace hyrise {

class ValidateNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _validate_node = ValidateNode::make();
  }

  std::shared_ptr<ValidateNode> _validate_node;
};

TEST_F(ValidateNodeTest, Description) {
  EXPECT_EQ(_validate_node->description(), "[Validate]");
}

TEST_F(ValidateNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*_validate_node, *_validate_node);

  EXPECT_EQ(*_validate_node, *ValidateNode::make());
  EXPECT_EQ(_validate_node->hash(), ValidateNode::make()->hash());
}

TEST_F(ValidateNodeTest, Copy) {
  EXPECT_EQ(*_validate_node->deep_copy(), *_validate_node);
}

TEST_F(ValidateNodeTest, NodeExpressions) {
  ASSERT_EQ(_validate_node->node_expressions.size(), 0u);
}

TEST_F(ValidateNodeTest, ForwardOrderDependencies) {
  const auto mock_node = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}});
  const auto a = mock_node->get_column("a");
  const auto b = mock_node->get_column("b");
  const auto od = OrderDependency{{a}, {b}};
  mock_node->set_order_dependencies({od});
  EXPECT_EQ(mock_node->order_dependencies().size(), 1);

  _validate_node->set_left_input(mock_node);
  const auto& order_dependencies = _validate_node->order_dependencies();
  EXPECT_EQ(order_dependencies.size(), 1);
  EXPECT_TRUE(order_dependencies.contains(od));
}

}  // namespace hyrise
