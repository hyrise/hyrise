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
    _mock_node = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}});
    _a = _mock_node->get_column("a");
    _b = _mock_node->get_column("b");

    _limit_node = LimitNode::make(value_(10), _mock_node);
  }

  std::shared_ptr<LimitNode> _limit_node;
  std::shared_ptr<MockNode> _mock_node;
  std::shared_ptr<LQPColumnExpression> _a, _b;
};

TEST_F(LimitNodeTest, Description) {
  EXPECT_EQ(_limit_node->description(), "[Limit] 10");
}

TEST_F(LimitNodeTest, HashingAndEqualityCheck) {
  // _limit_node has _mock_node as input, so it would not be equal to or have the same hash value as newly created,
  // equivalent nodes.
  _limit_node->set_left_input(nullptr);

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

TEST_F(LimitNodeTest, ForwardUniqueColumnCombinations) {
  EXPECT_TRUE(_mock_node->unique_column_combinations().empty());
  EXPECT_TRUE(_limit_node->unique_column_combinations().empty());

  const auto key_constraint_a = TableKeyConstraint{{_a->original_column_id}, KeyConstraintType::UNIQUE};
  _mock_node->set_key_constraints({key_constraint_a});
  EXPECT_EQ(_mock_node->unique_column_combinations().size(), 1);

  const auto& unique_column_combinations = _limit_node->unique_column_combinations();
  EXPECT_EQ(unique_column_combinations.size(), 1);
  EXPECT_TRUE(unique_column_combinations.contains({UniqueColumnCombination{{_a}}}));
}

}  // namespace hyrise
