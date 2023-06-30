#include <memory>

#include "base_test.hpp"

#include "logical_query_plan/data_dependencies/functional_dependency.hpp"
#include "logical_query_plan/except_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"

namespace hyrise {

class ExceptNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _mock_node1 = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "t_a");
    _mock_node2 = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "u"}, {DataType::Int, "v"}}, "t_b");
    _mock_node3 = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "x"}}, "t_v");

    _a = _mock_node1->get_column("a");
    _b = _mock_node1->get_column("b");
    _c = _mock_node1->get_column("c");

    _except_node = ExceptNode::make(SetOperationMode::Positions);
    _except_node->set_left_input(_mock_node1);
    _except_node->set_right_input(_mock_node1);

    _except_node_without_right_input = ExceptNode::make(SetOperationMode::Positions);
    _except_node_without_right_input->set_left_input(_mock_node1);
  }

  std::shared_ptr<MockNode> _mock_node1;
  std::shared_ptr<MockNode> _mock_node2;
  std::shared_ptr<MockNode> _mock_node3;
  std::shared_ptr<ExceptNode> _except_node;
  std::shared_ptr<ExceptNode> _except_node_without_right_input;
  std::shared_ptr<LQPColumnExpression> _a;
  std::shared_ptr<LQPColumnExpression> _b;
  std::shared_ptr<LQPColumnExpression> _c;
};

TEST_F(ExceptNodeTest, Description) {
  EXPECT_EQ(_except_node->description(), "[ExceptNode] Mode: Positions");
}

TEST_F(ExceptNodeTest, IsColumnNullable) {
  // Columns of a MockNode are never nullable.
  const auto column_count = _mock_node1->column_definitions().size();
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    EXPECT_FALSE(_except_node->is_column_nullable(column_id));
  }

  // Both left_input and right_input need to be set.
  EXPECT_THROW(_except_node_without_right_input->is_column_nullable(ColumnID{0}), std::logic_error);
}

TEST_F(ExceptNodeTest, NonTrivialFunctionalDependencies) {
  const auto fd_a = FunctionalDependency({_a}, {_b, _c});
  const auto fd_b_two_dependents = FunctionalDependency({_b}, {_a, _c});

  const auto non_trivial_dependencies = FunctionalDependencies{fd_a, fd_b_two_dependents};
  _mock_node1->set_non_trivial_functional_dependencies(non_trivial_dependencies);

  EXPECT_EQ(_except_node->non_trivial_functional_dependencies(), non_trivial_dependencies);
  // Also works if right_input is not set.
  EXPECT_EQ(_except_node_without_right_input->non_trivial_functional_dependencies(), non_trivial_dependencies);
}

TEST_F(ExceptNodeTest, OutputColumnExpressions) {
  EXPECT_TRUE(_except_node->output_expressions() == _mock_node1->output_expressions());
}

TEST_F(ExceptNodeTest, HashingAndEqualityCheck) {
  auto same_except_node = ExceptNode::make(SetOperationMode::Positions, _mock_node1, _mock_node1);
  auto different_except_node = ExceptNode::make(SetOperationMode::All, _mock_node1, _mock_node1);
  auto different_except_node_1 = ExceptNode::make(SetOperationMode::Positions, _mock_node1, _mock_node2);
  auto different_except_node_2 = ExceptNode::make(SetOperationMode::Positions, _mock_node2, _mock_node1);
  auto different_except_node_3 = ExceptNode::make(SetOperationMode::Positions, _mock_node2, _mock_node2);

  EXPECT_EQ(*_except_node, *same_except_node);
  EXPECT_NE(*_except_node, *different_except_node);
  EXPECT_NE(*_except_node, *different_except_node_1);
  EXPECT_NE(*_except_node, *different_except_node_2);
  EXPECT_NE(*_except_node, *different_except_node_3);
  EXPECT_NE(*_except_node, *ExceptNode::make(SetOperationMode::Positions));
  EXPECT_NE(*_except_node, *ExceptNode::make(SetOperationMode::All));

  EXPECT_EQ(_except_node->hash(), same_except_node->hash());
  EXPECT_NE(_except_node->hash(), different_except_node->hash());
  EXPECT_NE(_except_node->hash(), different_except_node_1->hash());
  EXPECT_NE(_except_node->hash(), different_except_node_2->hash());
  EXPECT_NE(_except_node->hash(), different_except_node_3->hash());
}

TEST_F(ExceptNodeTest, Copy) {
  EXPECT_EQ(*_except_node->deep_copy(), *_except_node);
}

TEST_F(ExceptNodeTest, NodeExpressions) {
  EXPECT_EQ(_except_node->node_expressions.size(), 0u);
}

TEST_F(ExceptNodeTest, ForwardUniqueColumnCombinations) {
  EXPECT_TRUE(_mock_node1->unique_column_combinations().empty());
  EXPECT_TRUE(_except_node->unique_column_combinations().empty());

  const auto key_constraint_a = TableKeyConstraint{{_a->original_column_id}, KeyConstraintType::UNIQUE};
  _mock_node1->set_key_constraints({key_constraint_a});
  EXPECT_EQ(_mock_node1->unique_column_combinations().size(), 1);

  const auto& unique_column_combinations = _except_node->unique_column_combinations();
  EXPECT_EQ(unique_column_combinations.size(), 1);
  EXPECT_TRUE(unique_column_combinations.contains({UniqueColumnCombination{{_a}}}));
}

}  // namespace hyrise
