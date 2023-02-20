#include <memory>

#include "base_test.hpp"

#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"

namespace hyrise {

class UnionNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _mock_node1 = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "t_a");
    _a = _mock_node1->get_column("a");
    _b = _mock_node1->get_column("b");
    _c = _mock_node1->get_column("c");

    _union_node = UnionNode::make(SetOperationMode::Positions, _mock_node1, _mock_node1);

    _mock_node2 = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "u"}, {DataType::Int, "v"}}, "t_b");
    _u = _mock_node2->get_column("u");
    _v = _mock_node2->get_column("v");
  }

  std::shared_ptr<MockNode> _mock_node1, _mock_node2;
  std::shared_ptr<UnionNode> _union_node;
  std::shared_ptr<LQPColumnExpression> _a, _b, _c, _u, _v;
};

TEST_F(UnionNodeTest, Description) {
  EXPECT_EQ(_union_node->description(), "[UnionNode] Mode: Positions");
}

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

TEST_F(UnionNodeTest, Copy) {
  EXPECT_EQ(*_union_node->deep_copy(), *_union_node);
}

TEST_F(UnionNodeTest, NodeExpressions) {
  ASSERT_EQ(_union_node->node_expressions.size(), 0u);
}

TEST_F(UnionNodeTest, InvalidInputExpressions) {
  // Ensure to forbid a union of nodes with different expressions, i.e., different columns.
  {
    const auto union_node = UnionNode::make(SetOperationMode::Positions, _mock_node1, _mock_node2);
    EXPECT_THROW(union_node->output_expressions(), std::logic_error);
  }
  {
    const auto union_node = UnionNode::make(SetOperationMode::All, _mock_node1, _mock_node2);
    EXPECT_THROW(union_node->output_expressions(), std::logic_error);
  }
}

TEST_F(UnionNodeTest, FunctionalDependenciesUnionAllSimple) {
  const auto trivial_fd_a = FunctionalDependency({_a}, {_b, _c});
  const auto non_trivial_fd_b = FunctionalDependency({_b}, {_a});
  const auto non_trivial_fd_c = FunctionalDependency({_c}, {_b});

  // Set FDs
  _mock_node1->set_key_constraints({{{_a->original_column_id}, KeyConstraintType::UNIQUE}});
  _mock_node1->set_non_trivial_functional_dependencies({non_trivial_fd_b, non_trivial_fd_c});
  EXPECT_EQ(_mock_node1->functional_dependencies().size(), 3);
  EXPECT_TRUE(_mock_node1->functional_dependencies().contains(non_trivial_fd_b));
  EXPECT_TRUE(_mock_node1->functional_dependencies().contains(non_trivial_fd_c));
  EXPECT_TRUE(_mock_node1->functional_dependencies().contains(trivial_fd_a));

  // Create PredicateNodes & UnionPositionsNode
  const auto& predicate_node_a = PredicateNode::make(greater_than_(_a, 5), _mock_node1);
  const auto& predicate_node_b = PredicateNode::make(greater_than_(_b, 5), _mock_node1);
  const auto& union_all_node = UnionNode::make(SetOperationMode::All);
  union_all_node->set_left_input(predicate_node_a);
  union_all_node->set_right_input(predicate_node_b);

  // We expect all FDs to be forwarded since both input nodes have the same non-trivial FDs & UCCs.
  const auto& union_node_fds = union_all_node->functional_dependencies();
  const auto& union_node_non_trivial_fds = union_all_node->non_trivial_functional_dependencies();
  // Since all UCCs are discarded, former trivial FDs become non-trivial.
  EXPECT_EQ(union_node_fds, union_node_non_trivial_fds);

  EXPECT_EQ(union_node_fds.size(), 3);
  EXPECT_TRUE(union_node_fds.contains(trivial_fd_a));
  EXPECT_TRUE(union_node_fds.contains(non_trivial_fd_b));
  EXPECT_TRUE(union_node_fds.contains(non_trivial_fd_c));
}

TEST_F(UnionNodeTest, FunctionalDependenciesUnionAllIntersect) {
  // Create single non-trivial FD
  const auto non_trivial_fd_b = FunctionalDependency({_a}, {_b});
  _mock_node1->set_non_trivial_functional_dependencies({non_trivial_fd_b});

  /**
   * Create UnionNode
   * Hack: We use an AggregateNode with a pseudo-aggregate ANY(_c) to
   *        - receive a new UCC and also
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
  EXPECT_TRUE(projection_node_a->functional_dependencies().contains(non_trivial_fd_b));
  EXPECT_EQ(projection_node_b->functional_dependencies().size(), 2);
  EXPECT_TRUE(projection_node_b->functional_dependencies().contains(non_trivial_fd_b));
  EXPECT_TRUE(projection_node_b->functional_dependencies().contains(expected_fd_a_b));

  // Test: We expect both input FD-sets to be intersected. Therefore, only one FD should survive.
  EXPECT_EQ(union_all_node->functional_dependencies().size(), 1);
  EXPECT_TRUE(union_all_node->functional_dependencies().contains(non_trivial_fd_b));
}

TEST_F(UnionNodeTest, FunctionalDependenciesUnionPositions) {
  const auto trivial_fd_a = FunctionalDependency({_a}, {_b, _c});
  const auto non_trivial_fd_b = FunctionalDependency({_b}, {_a});

  // Set FDs
  _mock_node1->set_key_constraints({{{_a->original_column_id}, KeyConstraintType::UNIQUE}});
  _mock_node1->set_non_trivial_functional_dependencies({non_trivial_fd_b});
  EXPECT_EQ(_mock_node1->functional_dependencies().size(), 2);
  EXPECT_TRUE(_mock_node1->functional_dependencies().contains(non_trivial_fd_b));
  EXPECT_TRUE(_mock_node1->functional_dependencies().contains(trivial_fd_a));

  // Create PredicateNodes & UnionPositionsNode
  const auto& predicate_node_a = PredicateNode::make(greater_than_(_a, 5), _mock_node1);
  const auto& predicate_node_b = PredicateNode::make(greater_than_(_b, 5), _mock_node1);
  const auto& union_positions_node = UnionNode::make(SetOperationMode::Positions);
  union_positions_node->set_left_input(predicate_node_a);
  union_positions_node->set_right_input(predicate_node_b);

  // Positive Tests
  EXPECT_EQ(union_positions_node->non_trivial_functional_dependencies().size(), 1);
  EXPECT_TRUE(union_positions_node->non_trivial_functional_dependencies().contains(non_trivial_fd_b));
  EXPECT_EQ(union_positions_node->functional_dependencies().size(), 2);
  EXPECT_TRUE(union_positions_node->functional_dependencies().contains(non_trivial_fd_b));
  EXPECT_TRUE(union_positions_node->functional_dependencies().contains(trivial_fd_a));
}

TEST_F(UnionNodeTest, FunctionalDependenciesUnionPositionsInvalidInput) {
  const auto trivial_fd_a = FunctionalDependency({_a}, {_b, _c});
  const auto non_trivial_fd_b = FunctionalDependency({_b}, {_a});

  // Set FDs
  _mock_node1->set_key_constraints({{{_a->original_column_id}, KeyConstraintType::UNIQUE}});
  _mock_node1->set_non_trivial_functional_dependencies({non_trivial_fd_b});
  EXPECT_EQ(_mock_node1->functional_dependencies().size(), 2);
  EXPECT_TRUE(_mock_node1->functional_dependencies().contains(non_trivial_fd_b));
  EXPECT_TRUE(_mock_node1->functional_dependencies().contains(trivial_fd_a));

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

TEST_F(UnionNodeTest, UniqueColumnCombinationsUnionPositions) {
  // Add two UCCs to _mock_node1.
  const auto key_constraint_a_b = TableKeyConstraint{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::PRIMARY_KEY};
  const auto key_constraint_b = TableKeyConstraint{{ColumnID{2}}, KeyConstraintType::UNIQUE};
  _mock_node1->set_key_constraints({key_constraint_a_b, key_constraint_b});
  EXPECT_EQ(_mock_node1->unique_column_combinations().size(), 2);

  // Check whether all UCCs are forwarded.
  EXPECT_TRUE(_union_node->left_input() == _mock_node1 && _union_node->right_input() == _mock_node1);
  EXPECT_EQ(_union_node->unique_column_combinations(), _mock_node1->unique_column_combinations());
}

TEST_F(UnionNodeTest, UniqueColumnCombinationsUnionPositionsInvalidInput) {
  const auto key_constraint_a_b = TableKeyConstraint{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::PRIMARY_KEY};
  const auto key_constraint_b = TableKeyConstraint{{ColumnID{2}}, KeyConstraintType::UNIQUE};
  _mock_node1->set_key_constraints(TableKeyConstraints{key_constraint_a_b, key_constraint_b});

  // Input are not allowed to have differing output expressions,
  _union_node->set_right_input(_mock_node2);
  EXPECT_THROW(_union_node->unique_column_combinations(), std::logic_error);

  // Input nodes are not allowed to have differing UCCs.
  // clang-format off
  const auto projection_node =
  ProjectionNode::make(expression_vector(_a, _b, _c),
    JoinNode::make(JoinMode::Cross,
      _mock_node1,
      _mock_node1));
  // clang-format on
  _union_node->set_right_input(projection_node);
  EXPECT_THROW(_union_node->unique_column_combinations(), std::logic_error);
}

TEST_F(UnionNodeTest, UniqueColumnCombinationsUnionAll) {
  // Add two UCCs to _mock_node1.
  const auto key_constraint_a_b = TableKeyConstraint{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::PRIMARY_KEY};
  const auto key_constraint_b = TableKeyConstraint{{ColumnID{2}}, KeyConstraintType::UNIQUE};
  _mock_node1->set_key_constraints({key_constraint_a_b, key_constraint_b});
  EXPECT_EQ(_mock_node1->unique_column_combinations().size(), 2);

  const auto union_node = UnionNode::make(SetOperationMode::All, _mock_node1, _mock_node1);

  // Check that no UCCs are forwarded.
  EXPECT_TRUE(union_node->unique_column_combinations().empty());
}

TEST_F(UnionNodeTest, OrderDependenciesUnionPositions) {
  const auto od_a_to_b = OrderDependency{{_a}, {_b}};
  const auto order_constraint = TableOrderConstraint{{ColumnID{0}}, {ColumnID{1}}};
  _mock_node1->set_order_constraints({order_constraint});
  EXPECT_EQ(_mock_node1->order_dependencies().size(), 1);

  // Forward OD.
  const auto& order_dependencies = _union_node->order_dependencies();
  EXPECT_EQ(order_dependencies.size(), 1);
  EXPECT_TRUE(order_dependencies.contains(od_a_to_b));
}

TEST_F(UnionNodeTest, OrderDependenciesUnionPositionsInvalidInput) {
  const auto od_a_to_b = OrderDependency{{_a}, {_b}};
  const auto order_constraint = TableOrderConstraint{{ColumnID{0}}, {ColumnID{1}}};
  _mock_node1->set_order_constraints({order_constraint});
  EXPECT_EQ(_mock_node1->order_dependencies().size(), 1);

  // Fail if inputs have different output expressions.
  _union_node->set_right_input(_mock_node2);
  EXPECT_THROW(_union_node->order_dependencies(), std::logic_error);

  // Fail if inputs have same output expressions, but different ODs.
  // clang-format off
  const auto join_node_1 =
    JoinNode::make(JoinMode::Cross,
      _mock_node1,
      _mock_node2);
  const auto join_node_2 =
    JoinNode::make(JoinMode::Inner, equals_(_a, _u),
      _mock_node1,
      _mock_node2);
  // clang-format on

  _union_node->set_left_input(join_node_1);
  _union_node->set_right_input(join_node_2);
  EXPECT_THROW(_union_node->order_dependencies(), std::logic_error);
}

TEST_F(UnionNodeTest, OrderDependenciesUnionAll) {
  const auto od_a_to_b = OrderDependency{{_a}, {_b}};
  const auto order_constraint = TableOrderConstraint{{ColumnID{0}}, {ColumnID{1}}};
  _mock_node1->set_order_constraints({order_constraint});
  EXPECT_EQ(_mock_node1->order_dependencies().size(), 1);

  {
    // Keep OD if inputs have same output expressions.
    const auto union_node = UnionNode::make(SetOperationMode::All, _mock_node1, _mock_node1);
    const auto& order_dependencies = union_node->order_dependencies();
    EXPECT_EQ(order_dependencies.size(), 1);
    EXPECT_TRUE(order_dependencies.contains(od_a_to_b));
  }
  {
    // Fail if inputs have different output expressions.
    const auto union_node = UnionNode::make(SetOperationMode::All, _mock_node1, _mock_node2);
    EXPECT_THROW(union_node->order_dependencies(), std::logic_error);
  }
  {
    // Fail if inputs have same output expressions, but different ODs.
    // clang-format off
    const auto join_node_1 =
      JoinNode::make(JoinMode::Cross,
        _mock_node1,
        _mock_node2);
    const auto join_node_2 =
      JoinNode::make(JoinMode::Inner, equals_(_a, _u),
        _mock_node1,
        _mock_node2);
    // clang-format on

    const auto union_node = UnionNode::make(SetOperationMode::All, join_node_1, join_node_2);
    EXPECT_THROW(union_node->order_dependencies(), std::logic_error);
  }
}

TEST_F(UnionNodeTest, InclusionDependenciesUnionPositions) {
  const auto dummy_table = Table::create_dummy_table({{"a", DataType::Int, false}});
  const auto ind = InclusionDependency{{_a}, {ColumnID{0}}, dummy_table};
  const auto foreign_key_constraint = ForeignKeyConstraint{{ColumnID{0}}, {ColumnID{0}}, nullptr, dummy_table};
  _mock_node1->set_foreign_key_constraints({foreign_key_constraint});
  EXPECT_EQ(_mock_node1->inclusion_dependencies().size(), 1);

  // Forward IND.
  const auto& inclusion_dependencies = _union_node->inclusion_dependencies();
  EXPECT_EQ(inclusion_dependencies.size(), 1);
  EXPECT_TRUE(inclusion_dependencies.contains(ind));
}

TEST_F(UnionNodeTest, InclusionDependenciesUnionPositionsInvalidInput) {
  const auto dummy_table = Table::create_dummy_table({{"a", DataType::Int, false}});
  const auto foreign_key_constraint = ForeignKeyConstraint{{ColumnID{0}}, {ColumnID{0}}, nullptr, dummy_table};
  _mock_node1->set_foreign_key_constraints({foreign_key_constraint});
  EXPECT_EQ(_mock_node1->inclusion_dependencies().size(), 1);

  // Fail if inputs have different output expressions.
  _union_node->set_right_input(_mock_node2);
  EXPECT_THROW(_union_node->order_dependencies(), std::logic_error);
}

TEST_F(UnionNodeTest, InclusionDependenciesUnionAll) {
  // Forward all INDs of both inputs.
  const auto dummy_table = Table::create_dummy_table({{"a", DataType::Int, false}});
  const auto ind_a = InclusionDependency{{_a}, {ColumnID{0}}, dummy_table};
  const auto foreign_key_constraint = ForeignKeyConstraint{{ColumnID{0}}, {ColumnID{0}}, nullptr, dummy_table};
  _mock_node1->set_foreign_key_constraints({foreign_key_constraint});
  EXPECT_EQ(_mock_node1->inclusion_dependencies().size(), 1);
  const auto ind_u = InclusionDependency{{_u}, {ColumnID{0}}, dummy_table};
  _mock_node2->set_foreign_key_constraints({foreign_key_constraint});
  EXPECT_EQ(_mock_node2->inclusion_dependencies().size(), 1);

  const auto union_node = UnionNode::make(SetOperationMode::All, _mock_node1, _mock_node2);
  const auto& inclusion_dependencies = union_node->inclusion_dependencies();
  EXPECT_EQ(inclusion_dependencies.size(), 2);
  EXPECT_TRUE(inclusion_dependencies.contains(ind_a));
  EXPECT_TRUE(inclusion_dependencies.contains(ind_u));
}

}  // namespace hyrise
