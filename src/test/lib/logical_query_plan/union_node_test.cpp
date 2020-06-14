#include <memory>

#include "base_test.hpp"

#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/validate_node.hpp"

namespace opossum {

class UnionNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _mock_node1 = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "t_a");
    _mock_node2 = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "u"}, {DataType::Int, "v"}}, "t_b");

    _a = _mock_node1->get_column("a");
    _b = _mock_node1->get_column("b");
    _c = _mock_node1->get_column("c");

    _union_node = UnionNode::make(SetOperationMode::Positions);
    _union_node->set_left_input(_mock_node1);
    _union_node->set_right_input(_mock_node1);
  }

  std::shared_ptr<MockNode> _mock_node1, _mock_node2;
  std::shared_ptr<UnionNode> _union_node;
  std::shared_ptr<LQPColumnExpression> _a;
  std::shared_ptr<LQPColumnExpression> _b;
  std::shared_ptr<LQPColumnExpression> _c;
};

TEST_F(UnionNodeTest, Description) { EXPECT_EQ(_union_node->description(), "[UnionNode] Mode: Positions"); }

TEST_F(UnionNodeTest, OutputColumnExpressions) {
  EXPECT_EQ(*_union_node->output_expressions().at(0), *_mock_node1->output_expressions().at(0));
  EXPECT_EQ(*_union_node->output_expressions().at(1), *_mock_node1->output_expressions().at(1));
  EXPECT_EQ(*_union_node->output_expressions().at(2), *_mock_node1->output_expressions().at(2));
}

TEST_F(UnionNodeTest, HashingAndEqualityCheck) {
  auto same_union_node = UnionNode::make(SetOperationMode::Positions);
  same_union_node->set_left_input(_mock_node1);
  same_union_node->set_right_input(_mock_node1);
  auto different_union_node = UnionNode::make(SetOperationMode::All);
  different_union_node->set_left_input(_mock_node1);
  different_union_node->set_right_input(_mock_node1);
  auto different_union_node_1 = UnionNode::make(SetOperationMode::Positions);
  different_union_node_1->set_left_input(_mock_node1);
  different_union_node_1->set_right_input(_mock_node2);
  auto different_union_node_2 = UnionNode::make(SetOperationMode::Positions);
  different_union_node_2->set_left_input(_mock_node2);
  different_union_node_2->set_right_input(_mock_node1);
  auto different_union_node_3 = UnionNode::make(SetOperationMode::Positions);
  different_union_node_3->set_left_input(_mock_node2);
  different_union_node_3->set_right_input(_mock_node2);

  EXPECT_EQ(*_union_node, *same_union_node);
  EXPECT_NE(*_union_node, *different_union_node);
  EXPECT_NE(*_union_node, *different_union_node_1);
  EXPECT_NE(*_union_node, *different_union_node_2);
  EXPECT_NE(*_union_node, *different_union_node_3);
  EXPECT_NE(*_union_node, *UnionNode::make(SetOperationMode::Positions));
  EXPECT_NE(*_union_node, *UnionNode::make(SetOperationMode::All));

  EXPECT_EQ(_union_node->hash(), same_union_node->hash());
  EXPECT_NE(_union_node->hash(), different_union_node->hash());
  EXPECT_NE(_union_node->hash(), different_union_node_1->hash());
  EXPECT_NE(_union_node->hash(), different_union_node_2->hash());
  EXPECT_NE(_union_node->hash(), different_union_node_3->hash());
  EXPECT_NE(_union_node->hash(), UnionNode::make(SetOperationMode::Positions)->hash());
  EXPECT_NE(_union_node->hash(), UnionNode::make(SetOperationMode::All)->hash());
}

TEST_F(UnionNodeTest, Copy) { EXPECT_EQ(*_union_node->deep_copy(), *_union_node); }

TEST_F(UnionNodeTest, NodeExpressions) { ASSERT_EQ(_union_node->node_expressions.size(), 0u); }

TEST_F(UnionNodeTest, FunctionalDependencies) {
  // Create StoredTableNode with a single FD
  const auto table_name = "t_a";
  Hyrise::get().storage_manager.add_table(table_name, load_table("resources/test_data/tbl/int_int_float.tbl", 1));
  const auto table = Hyrise::get().storage_manager.get_table(table_name);
  table->add_soft_key_constraint({{_a->original_column_id}, KeyConstraintType::UNIQUE});
  const auto stored_table_node = StoredTableNode::make(table_name);
  EXPECT_EQ(stored_table_node->functional_dependencies().size(), 1);
  // Create ValidateNode as it is required by UnionPositions
  auto validate_node = ValidateNode::make(stored_table_node);
  EXPECT_EQ(validate_node->functional_dependencies().size(), 1);

  // Test UnionPositions (forward FDs)
  auto union_positions_node = UnionNode::make(SetOperationMode::All);
  union_positions_node->set_left_input(validate_node);
  union_positions_node->set_right_input(validate_node);

  Hyrise::get().reset();
}

TEST_F(UnionNodeTest, ConstraintsUnionPositions) {
  // Add two unique constraints to _mock_node1
  // Primary Key: a, b
  const auto table_constraint_1 = TableConstraintDefinition{{ColumnID{0}, ColumnID{1}}};
  // Unique: c
  const auto table_constraint_2 = TableConstraintDefinition{{ColumnID{2}}};
  _mock_node1->set_table_constraints(TableConstraintDefinitions{table_constraint_1, table_constraint_2});
  EXPECT_EQ(_mock_node1->constraints()->size(), 2);

  // Test constraint forwarding
  EXPECT_TRUE(_union_node->left_input() == _mock_node1 && _union_node->right_input() == _mock_node1);
  EXPECT_EQ(*_union_node->constraints(), *_mock_node1->constraints());

  // Negative test - input nodes with differing constraints should lead to failure
  auto mock_node1_changed = static_pointer_cast<MockNode>(_mock_node1->deep_copy());
  mock_node1_changed->set_table_constraints(TableConstraintDefinitions{table_constraint_1});
  _union_node->set_right_input(mock_node1_changed);
  EXPECT_THROW(_union_node->constraints(), std::logic_error);

  // TODO(Julian) Check whether something got lost while merging
}

}  // namespace opossum
