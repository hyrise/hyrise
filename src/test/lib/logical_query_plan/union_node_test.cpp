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
    _a = _mock_node1->get_column("a");
    _b = _mock_node1->get_column("b");
    _c = _mock_node1->get_column("c");

    _union_node = UnionNode::make(SetOperationMode::Positions);
    _union_node->set_left_input(_mock_node1);
    _union_node->set_right_input(_mock_node1);

    _mock_node2 = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "u"}, {DataType::Int, "v"}}, "t_b");
    _u = _mock_node2->get_column("u");
    _v = _mock_node2->get_column("v");
  }

  std::shared_ptr<MockNode> _mock_node1, _mock_node2;
  std::shared_ptr<UnionNode> _union_node;
  std::shared_ptr<LQPColumnExpression> _a;
  std::shared_ptr<LQPColumnExpression> _b;
  std::shared_ptr<LQPColumnExpression> _c;
  std::shared_ptr<LQPColumnExpression> _u;
  std::shared_ptr<LQPColumnExpression> _v;
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
  const auto& union_node_fds = union_all_node->functional_dependencies();
  const auto& union_node_non_trivial_fds = union_all_node->non_trivial_functional_dependencies();
  // Since all unique constraints become discarded, former trivial FDs become non-trivial:
  EXPECT_EQ(union_node_fds, union_node_non_trivial_fds);

  EXPECT_EQ(union_node_fds.size(), 3);
  const auto& union_node_fds_set =
      std::unordered_set<FunctionalDependency>(union_node_fds.cbegin(), union_node_fds.cend());
  EXPECT_TRUE(union_node_fds_set.contains(trivial_fd_a));
  EXPECT_TRUE(union_node_fds_set.contains(non_trivial_fd_b));
  EXPECT_TRUE(union_node_fds_set.contains(non_trivial_fd_c));
}

TEST_F(UnionNodeTest, FunctionalDependenciesUnionAllIntersect) {
  // Create single non-trivial FD
  const auto non_trivial_fd_b = FunctionalDependency({_a}, {_b});
  _mock_node1->set_non_trivial_functional_dependencies({non_trivial_fd_b});

  /**
   * Create UnionNode
   * Hack: We use an AggregateNode with a pseudo-aggregate ANY(_c) to
   *        - receive a new unique constraint and also
   *        - a new trivial FD {_a, _b} => {_c}
   */
  const auto& projection_node_a = ProjectionNode::make(expression_vector(_a, _b, _c), _mock_node1);
  const auto& aggregate_node = AggregateNode::make(expression_vector(_a, _b), expression_vector(any_(_c)), _mock_node1);
  const auto& projection_node_b = ProjectionNode::make(expression_vector(_a, _b, _c), aggregate_node);

  const auto& union_all_node = UnionNode::make(SetOperationMode::All);
  union_all_node->set_left_input(projection_node_a);
  union_all_node->set_right_input(projection_node_b);

  // Prerequisite: Input nodes have differing FDs
  const auto& expected_fd_a_b = FunctionalDependency({_a, _b}, {_c});
  EXPECT_EQ(projection_node_a->functional_dependencies().size(), 1);
  EXPECT_EQ(projection_node_a->functional_dependencies().at(0), non_trivial_fd_b);
  EXPECT_EQ(projection_node_b->functional_dependencies().size(), 2);
  EXPECT_EQ(projection_node_b->functional_dependencies().at(0), non_trivial_fd_b);
  EXPECT_EQ(projection_node_b->functional_dependencies().at(1), expected_fd_a_b);

  // Test: We expect both input FD-sets to be intersected. Therefore, only one FD should survive.
  EXPECT_EQ(union_all_node->functional_dependencies().size(), 1);
  EXPECT_EQ(union_all_node->functional_dependencies().at(0), non_trivial_fd_b);
}

TEST_F(UnionNodeTest, FunctionalDependenciesUnionPositions) {
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

TEST_F(UnionNodeTest, FunctionalDependenciesUnionPositionsInvalidInput) {
  // This test verifies a DebugAssert condition. Therefore, we do not want this test to run in release mode.
  if constexpr (!HYRISE_DEBUG) GTEST_SKIP();

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

TEST_F(UnionNodeTest, UniqueConstraintsUnionPositions) {
  // Add two unique constraints to _mock_node1
  const auto key_constraint_a_b = TableKeyConstraint{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::PRIMARY_KEY};
  const auto key_constraint_b = TableKeyConstraint{{ColumnID{2}}, KeyConstraintType::UNIQUE};
  _mock_node1->set_key_constraints({key_constraint_a_b, key_constraint_b});
  EXPECT_EQ(_mock_node1->unique_constraints()->size(), 2);

  // Check whether all unique constraints are forwarded
  EXPECT_TRUE(_union_node->left_input() == _mock_node1 && _union_node->right_input() == _mock_node1);
  EXPECT_EQ(*_union_node->unique_constraints(), *_mock_node1->unique_constraints());
}

TEST_F(UnionNodeTest, UniqueConstraintsUnionPositionsInvalidInput) {
  const auto key_constraint_a_b = TableKeyConstraint{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::PRIMARY_KEY};
  const auto key_constraint_b = TableKeyConstraint{{ColumnID{2}}, KeyConstraintType::UNIQUE};
  _mock_node1->set_key_constraints(TableKeyConstraints{key_constraint_a_b, key_constraint_b});

  auto mock_node1_changed = static_pointer_cast<MockNode>(_mock_node1->deep_copy());
  mock_node1_changed->set_key_constraints({key_constraint_a_b});

  // Input nodes are not allowed to have differing unique constraints
  EXPECT_EQ(_mock_node1->unique_constraints()->size(), 2);
  EXPECT_EQ(mock_node1_changed->unique_constraints()->size(), 1);
  _union_node->set_right_input(mock_node1_changed);
  EXPECT_THROW(_union_node->unique_constraints(), std::logic_error);
}

}  // namespace opossum
