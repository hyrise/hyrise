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

TEST_F(UnionNodeTest, FunctionalDependenciesUnionAllSimple) {
  const auto trivial_fd_a = FunctionalDependency({_a}, {_b, _c});
  const auto non_trivial_fd_b = FunctionalDependency({_b}, {_a});
  const auto non_trivial_fd_c = FunctionalDependency({_c}, {_b});

  // Set FDs
  _mock_node1->set_key_constraints({{{_a->original_column_id}, KeyConstraintType::UNIQUE}});
  _mock_node1->set_non_trivial_functional_dependencies({non_trivial_fd_b, non_trivial_fd_c});
  EXPECT_EQ(_mock_node1->functional_dependencies().size(), 3);
  EXPECT_EQ(_mock_node1->functional_dependencies().at(0), non_trivial_fd_b);
  EXPECT_EQ(_mock_node1->functional_dependencies().at(1), non_trivial_fd_c);
  EXPECT_EQ(_mock_node1->functional_dependencies().at(2), trivial_fd_a);

  // Create PredicateNodes & UnionPositionsNode
  const auto& predicate_node_a = PredicateNode::make(greater_than_(_a, 5), _mock_node1);
  const auto& predicate_node_b = PredicateNode::make(greater_than_(_b, 5), _mock_node1);
  const auto& union_all_node = UnionNode::make(SetOperationMode::All);
  union_all_node->set_left_input(predicate_node_a);
  union_all_node->set_right_input(predicate_node_b);

  // We expect all FDs to be forwarded since both input nodes have the same non-trivial FDs & unique constraints.
  EXPECT_EQ(union_all_node->functional_dependencies().size(), 3);
  EXPECT_EQ(union_all_node->functional_dependencies().at(0), trivial_fd_a);
  EXPECT_EQ(union_all_node->functional_dependencies().at(1), non_trivial_fd_c);
  EXPECT_EQ(union_all_node->functional_dependencies().at(2), non_trivial_fd_b);
  // Since all unique constraints become discarded, former trivial FDs become non-trivial:
  EXPECT_EQ(union_all_node->non_trivial_functional_dependencies().size(), 3);
  EXPECT_EQ(union_all_node->non_trivial_functional_dependencies().at(0), trivial_fd_a);
  EXPECT_EQ(union_all_node->non_trivial_functional_dependencies().at(1), non_trivial_fd_c);
  EXPECT_EQ(union_all_node->non_trivial_functional_dependencies().at(2), non_trivial_fd_b);
}

TEST_F(UnionNodeTest, FunctionalDependenciesUnionAllIntersect) {
  const auto trivial_fd_a = FunctionalDependency({_a}, {_b, _c});
  const auto non_trivial_fd_b = FunctionalDependency({_b}, {_a});
  const auto non_trivial_fd_c = FunctionalDependency({_c}, {_b});

  // Set FDs
  _mock_node1->set_key_constraints({{{_a->original_column_id}, KeyConstraintType::UNIQUE}});
  _mock_node1->set_non_trivial_functional_dependencies({non_trivial_fd_b, non_trivial_fd_c});
  EXPECT_EQ(_mock_node1->functional_dependencies().size(), 3);
  EXPECT_EQ(_mock_node1->functional_dependencies().at(0), non_trivial_fd_b);
  EXPECT_EQ(_mock_node1->functional_dependencies().at(1), non_trivial_fd_c);
  EXPECT_EQ(_mock_node1->functional_dependencies().at(2), trivial_fd_a);

  // Create UnionNode
  const auto& predicate_node_a = PredicateNode::make(greater_than_(_a, 5), _mock_node1);
  EXPECT_EQ(predicate_node_a->functional_dependencies(), _mock_node1->functional_dependencies());
  // Hack:
  //  (1) Remove column, so that some unique constraints and non-trivial FDs become discarded
  const auto& projection_node_hack = ProjectionNode::make(expression_vector(_a, _b), _mock_node1);
  //  (2) Re-add column, so that we are able to perform UnionAll
  const auto& projection_node_b = ProjectionNode::make(expression_vector(_a, _b, _c), projection_node_hack);
  //  (3) We expect differing FDs and differing unique constraints:
  EXPECT_NE(*_mock_node1->unique_constraints(), *projection_node_b->unique_constraints());
  EXPECT_NE(_mock_node1->non_trivial_functional_dependencies(),
            projection_node_b->non_trivial_functional_dependencies());

  const auto& union_all_node = UnionNode::make(SetOperationMode::All);
  union_all_node->set_left_input(predicate_node_a);
  union_all_node->set_right_input(projection_node_b);

  // The input nodes have differing non-trivial FDs & unique constraints. The output is expected to be the intersect
  // of both input node's FDs –> Discard all FDs involving column expression _c
  const auto expected_fd_a = FunctionalDependency({_a}, {_b});
  EXPECT_EQ(union_all_node->functional_dependencies().size(), 2);
  EXPECT_EQ(union_all_node->functional_dependencies().at(0), non_trivial_fd_b);
  EXPECT_EQ(union_all_node->functional_dependencies().at(1), expected_fd_a);
  // Since all unique constraints become discarded, former trivial FDs become non-trivial:
  EXPECT_EQ(union_all_node->non_trivial_functional_dependencies().size(), 2);
  EXPECT_EQ(union_all_node->non_trivial_functional_dependencies().at(0), non_trivial_fd_b);
  EXPECT_EQ(union_all_node->non_trivial_functional_dependencies().at(1), expected_fd_a);
}

TEST_F(UnionNodeTest, FunctionalDependenciesUnionPositionsPositive) {
  const auto trivial_fd_a = FunctionalDependency({_a}, {_b, _c});
  const auto non_trivial_fd_b = FunctionalDependency({_b}, {_a});

  // Set FDs
  _mock_node1->set_key_constraints({{{_a->original_column_id}, KeyConstraintType::UNIQUE}});
  _mock_node1->set_non_trivial_functional_dependencies({non_trivial_fd_b});
  EXPECT_EQ(_mock_node1->functional_dependencies().size(), 2);
  EXPECT_EQ(_mock_node1->functional_dependencies().at(0), non_trivial_fd_b);
  EXPECT_EQ(_mock_node1->functional_dependencies().at(1), trivial_fd_a);

  // Create PredicateNodes & UnionPositionsNode
  const auto& predicate_node_a = PredicateNode::make(greater_than_(_a, 5), _mock_node1);
  const auto& predicate_node_b = PredicateNode::make(greater_than_(_b, 5), _mock_node1);
  const auto& union_positions_node = UnionNode::make(SetOperationMode::Positions);
  union_positions_node->set_left_input(predicate_node_a);
  union_positions_node->set_right_input(predicate_node_b);

  // Positive Tests
  EXPECT_EQ(union_positions_node->non_trivial_functional_dependencies().size(), 1);
  EXPECT_EQ(union_positions_node->non_trivial_functional_dependencies().at(0), non_trivial_fd_b);
  EXPECT_EQ(union_positions_node->functional_dependencies().size(), 2);
  EXPECT_EQ(union_positions_node->functional_dependencies().at(0), non_trivial_fd_b);
  EXPECT_EQ(union_positions_node->functional_dependencies().at(1), trivial_fd_a);
}

TEST_F(UnionNodeTest, FunctionalDependenciesUnionPositionsNegative) {
  const auto trivial_fd_a = FunctionalDependency({_a}, {_b, _c});
  const auto non_trivial_fd_b = FunctionalDependency({_b}, {_a});

  // Set FDs
  _mock_node1->set_key_constraints({{{_a->original_column_id}, KeyConstraintType::UNIQUE}});
  _mock_node1->set_non_trivial_functional_dependencies({non_trivial_fd_b});
  EXPECT_EQ(_mock_node1->functional_dependencies().size(), 2);
  EXPECT_EQ(_mock_node1->functional_dependencies().at(0), non_trivial_fd_b);
  EXPECT_EQ(_mock_node1->functional_dependencies().at(1), trivial_fd_a);

  // Create PredicateNodes & UnionPositionsNode
  const auto& predicate_node_a = PredicateNode::make(greater_than_(_a, 5), _mock_node1);
  const auto& predicate_node_b = PredicateNode::make(greater_than_(_b, 5), _mock_node2);
  const auto& union_positions_node = UnionNode::make(SetOperationMode::Positions);
  union_positions_node->set_left_input(predicate_node_a);
  union_positions_node->set_right_input(predicate_node_b);

  // Negative Tests
  EXPECT_NE(predicate_node_a->non_trivial_functional_dependencies(),
            predicate_node_b->non_trivial_functional_dependencies());
  EXPECT_THROW(union_positions_node->non_trivial_functional_dependencies(), std::logic_error);
  EXPECT_THROW(union_positions_node->functional_dependencies(), std::logic_error);
}

TEST_F(UnionNodeTest, UniqueConstraintsUnionPositions) { // TODO(Julian) make two tests: Forward & Discard
  // Add two unique constraints to _mock_node1
  const auto key_constraint_a_b = TableKeyConstraint{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::PRIMARY_KEY};
  const auto key_constraint_b = TableKeyConstraint{{ColumnID{2}}, KeyConstraintType::UNIQUE};
  _mock_node1->set_key_constraints(TableKeyConstraints{key_constraint_a_b, key_constraint_b});
  EXPECT_EQ(_mock_node1->unique_constraints()->size(), 2);

  // Check whether all unique constraints are forwarded
  EXPECT_TRUE(_union_node->left_input() == _mock_node1 && _union_node->right_input() == _mock_node1);
  EXPECT_EQ(*_union_node->unique_constraints(), *_mock_node1->unique_constraints());

  // Negative test: Input nodes with differing unique constraints should lead to failure
  auto mock_node1_changed = static_pointer_cast<MockNode>(_mock_node1->deep_copy());
  mock_node1_changed->set_key_constraints(TableKeyConstraints{key_constraint_a_b});
  _union_node->set_right_input(mock_node1_changed);
  EXPECT_THROW(_union_node->unique_constraints(), std::logic_error);
}

}  // namespace opossum
