#include <memory>

#include "base_test.hpp"

#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/validate_node.hpp"

namespace hyrise {

class ValidateNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _mock_node = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}});
    _validate_node = ValidateNode::make(_mock_node);

    _a = _mock_node->get_column("a");
    _b = _mock_node->get_column("b");
  }

  std::shared_ptr<ValidateNode> _validate_node;
  std::shared_ptr<MockNode> _mock_node;
  std::shared_ptr<LQPColumnExpression> _a, _b;
};

TEST_F(ValidateNodeTest, Description) {
  EXPECT_EQ(_validate_node->description(), "[Validate]");
}

TEST_F(ValidateNodeTest, HashingAndEqualityCheck) {
  _validate_node->set_left_input(nullptr);
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

TEST_F(ValidateNodeTest, ForwardUniqueColumnCombinations) {
  EXPECT_TRUE(_mock_node->unique_column_combinations().empty());
  EXPECT_TRUE(_validate_node->unique_column_combinations().empty());

  _mock_node->set_key_constraints({TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE}});
  EXPECT_EQ(_mock_node->unique_column_combinations().size(), 1);

  const auto& unique_column_combinations = _validate_node->unique_column_combinations();
  EXPECT_EQ(unique_column_combinations.size(), 1);
  EXPECT_TRUE(unique_column_combinations.contains(UniqueColumnCombination{{_a}}));
}

}  // namespace hyrise
