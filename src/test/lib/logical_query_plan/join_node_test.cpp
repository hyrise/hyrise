#include <memory>

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "utils/data_dependency_test_utils.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class JoinNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _mock_node_a = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "t_a");
    _mock_node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "x"}, {DataType::Float, "y"}}, "t_b");

    _t_a_a = _mock_node_a->get_column("a");
    _t_a_b = _mock_node_a->get_column("b");
    _t_a_c = _mock_node_a->get_column("c");
    _t_b_x = _mock_node_b->get_column("x");
    _t_b_y = _mock_node_b->get_column("y");

    _cross_join_node = JoinNode::make(JoinMode::Cross, _mock_node_a, _mock_node_b);
    _inner_join_node = JoinNode::make(JoinMode::Inner, equals_(_t_a_a, _t_b_y), _mock_node_a, _mock_node_b);
    _semi_join_node = JoinNode::make(JoinMode::Semi, equals_(_t_a_a, _t_b_y), _mock_node_a, _mock_node_b);
    _semi_join_reduction_node = JoinNode::make(JoinMode::Semi, equals_(_t_a_a, _t_b_y), _mock_node_a, _mock_node_b);
    _semi_join_reduction_node->mark_as_semi_reduction(_inner_join_node);
    _anti_join_node = JoinNode::make(JoinMode::AntiNullAsTrue, equals_(_t_a_a, _t_b_y), _mock_node_a, _mock_node_b);

    // Prepare constraint definitions.
    _key_constraint_a = TableKeyConstraint{{_t_a_a->original_column_id}, KeyConstraintType::UNIQUE};
    _key_constraint_b_c =
        TableKeyConstraint{{_t_a_b->original_column_id, _t_a_c->original_column_id}, KeyConstraintType::UNIQUE};
    _key_constraint_x = TableKeyConstraint{{_t_b_x->original_column_id}, KeyConstraintType::UNIQUE};
    _key_constraint_y = TableKeyConstraint{{_t_b_y->original_column_id}, KeyConstraintType::UNIQUE};
  }

  std::shared_ptr<MockNode> _mock_node_a;
  std::shared_ptr<MockNode> _mock_node_b;
  std::shared_ptr<JoinNode> _inner_join_node;
  std::shared_ptr<JoinNode> _semi_join_node, _semi_join_reduction_node;
  std::shared_ptr<JoinNode> _anti_join_node;
  std::shared_ptr<JoinNode> _cross_join_node;
  std::shared_ptr<LQPColumnExpression> _t_a_a;
  std::shared_ptr<LQPColumnExpression> _t_a_b;
  std::shared_ptr<LQPColumnExpression> _t_a_c;
  std::shared_ptr<LQPColumnExpression> _t_b_x;
  std::shared_ptr<LQPColumnExpression> _t_b_y;
  std::optional<TableKeyConstraint> _key_constraint_a;
  std::optional<TableKeyConstraint> _key_constraint_b_c;
  std::optional<TableKeyConstraint> _key_constraint_x;
  std::optional<TableKeyConstraint> _key_constraint_y;
};

class JoinNodeMultiJoinModeTest : public JoinNodeTest, public ::testing::WithParamInterface<JoinMode> {};

INSTANTIATE_TEST_SUITE_P(JoinNodeMultiJoinModeTestInstance, JoinNodeMultiJoinModeTest,
                         ::testing::ValuesIn(magic_enum::enum_values<JoinMode>()), enum_formatter<JoinMode>);

TEST_F(JoinNodeTest, Description) {
  EXPECT_EQ(_cross_join_node->description(), "[Join] Mode: Cross");
}

TEST_F(JoinNodeTest, DescriptionInnerJoin) {
  EXPECT_EQ(_inner_join_node->description(), "[Join] Mode: Inner [a = y]");
}

TEST_F(JoinNodeTest, DescriptionSemiJoin) {
  EXPECT_EQ(_semi_join_node->description(), "[Join] Mode: Semi [a = y]");
}

TEST_F(JoinNodeTest, DescriptionAntiJoin) {
  EXPECT_EQ(_anti_join_node->description(), "[Join] Mode: AntiNullAsTrue [a = y]");
}

TEST_F(JoinNodeTest, OutputColumnExpressions) {
  ASSERT_EQ(_cross_join_node->output_expressions().size(), 5u);
  EXPECT_EQ(*_cross_join_node->output_expressions().at(0), *_t_a_a);
  EXPECT_EQ(*_cross_join_node->output_expressions().at(1), *_t_a_b);
  EXPECT_EQ(*_cross_join_node->output_expressions().at(2), *_t_a_c);
  EXPECT_EQ(*_cross_join_node->output_expressions().at(3), *_t_b_x);
  EXPECT_EQ(*_cross_join_node->output_expressions().at(4), *_t_b_y);

  ASSERT_EQ(_semi_join_node->output_expressions().size(), 3u);
  EXPECT_EQ(*_semi_join_node->output_expressions().at(0), *_t_a_a);
  EXPECT_EQ(*_semi_join_node->output_expressions().at(1), *_t_a_b);
  EXPECT_EQ(*_semi_join_node->output_expressions().at(2), *_t_a_c);
}

TEST_F(JoinNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*_anti_join_node, *_anti_join_node);
  EXPECT_EQ(*_cross_join_node, *_cross_join_node);
  EXPECT_EQ(*_inner_join_node, *_inner_join_node);
  EXPECT_EQ(*_semi_join_node, *_semi_join_node);
  EXPECT_EQ(*_semi_join_reduction_node, *_semi_join_reduction_node);

  const auto other_join_node_a = JoinNode::make(JoinMode::Inner, equals_(_t_a_a, _t_b_x), _mock_node_a, _mock_node_b);
  const auto other_join_node_b = JoinNode::make(JoinMode::Inner, not_like_(_t_a_a, _t_b_y), _mock_node_a, _mock_node_b);
  const auto other_join_node_c = JoinNode::make(JoinMode::Cross, _mock_node_a, _mock_node_b);
  const auto other_join_node_d = JoinNode::make(JoinMode::Inner, equals_(_t_a_a, _t_b_y), _mock_node_a, _mock_node_b);

  EXPECT_NE(*other_join_node_a, *_inner_join_node);
  EXPECT_NE(*other_join_node_b, *_inner_join_node);
  EXPECT_NE(*other_join_node_c, *_inner_join_node);
  EXPECT_EQ(*other_join_node_d, *_inner_join_node);

  EXPECT_NE(other_join_node_a->hash(), _inner_join_node->hash());
  EXPECT_NE(other_join_node_b->hash(), _inner_join_node->hash());
  EXPECT_NE(other_join_node_c->hash(), _inner_join_node->hash());
  EXPECT_EQ(other_join_node_d->hash(), _inner_join_node->hash());

  EXPECT_NE(_semi_join_node->hash(), _semi_join_reduction_node->hash());
  EXPECT_NE(*_semi_join_reduction_node, *_semi_join_node);
}

TEST_F(JoinNodeTest, Copy) {
  EXPECT_EQ(*_cross_join_node, *_cross_join_node->deep_copy());
  EXPECT_EQ(*_inner_join_node, *_inner_join_node->deep_copy());
  EXPECT_EQ(*_semi_join_node, *_semi_join_node->deep_copy());
  EXPECT_EQ(*_semi_join_reduction_node, *_semi_join_reduction_node->deep_copy());
  EXPECT_EQ(*_anti_join_node, *_anti_join_node->deep_copy());
}

TEST_F(JoinNodeTest, OutputColumnExpressionsSemiJoin) {
  ASSERT_EQ(_semi_join_node->output_expressions().size(), 3u);
  EXPECT_EQ(*_semi_join_node->output_expressions().at(0), *_t_a_a);
  EXPECT_EQ(*_semi_join_node->output_expressions().at(1), *_t_a_b);
  EXPECT_EQ(*_semi_join_node->output_expressions().at(2), *_t_a_c);
}

TEST_F(JoinNodeTest, OutputColumnExpressionsAntiJoin) {
  ASSERT_EQ(_anti_join_node->output_expressions().size(), 3u);
  EXPECT_EQ(*_anti_join_node->output_expressions().at(0), *_t_a_a);
  EXPECT_EQ(*_anti_join_node->output_expressions().at(1), *_t_a_b);
  EXPECT_EQ(*_anti_join_node->output_expressions().at(2), *_t_a_c);
}

TEST_F(JoinNodeTest, NodeExpressions) {
  ASSERT_EQ(_inner_join_node->node_expressions.size(), 1u);
  EXPECT_EQ(*_inner_join_node->node_expressions.at(0u), *equals_(_t_a_a, _t_b_y));
  ASSERT_EQ(_cross_join_node->node_expressions.size(), 0u);
}

TEST_F(JoinNodeTest, IsColumnNullableWithoutOuterJoin) {
  // Test that for LQPs without (Left,Right)Outer Joins, lqp_column_is_nullable() is equivalent to
  // expression.is_nullable()

  // clang-format off
  const auto lqp =
  JoinNode::make(JoinMode::Inner, equals_(add_(_t_a_a, null_()), _t_b_x),
    ProjectionNode::make(expression_vector(_t_a_a, _t_a_b, add_(_t_a_a, null_())),
      _mock_node_a),
    _mock_node_b);
  // clang-format on

  EXPECT_FALSE(lqp->is_column_nullable(ColumnID{0}));
  EXPECT_FALSE(lqp->is_column_nullable(ColumnID{1}));
  EXPECT_TRUE(lqp->is_column_nullable(ColumnID{2}));
  EXPECT_FALSE(lqp->is_column_nullable(ColumnID{3}));
  EXPECT_FALSE(lqp->is_column_nullable(ColumnID{4}));
}

TEST_F(JoinNodeTest, IsColumnNullableWithOuterJoin) {
  // Test that columns on the "null-supplying" side of an outer join are always nullable.
  // Test that is_null_(<nullable>) is never nullable

  // clang-format off
  const auto lqp_left_join_basic =
  JoinNode::make(JoinMode::Left, equals_(_t_a_a, _t_b_x),
    _mock_node_a,
    _mock_node_b);
  // clang-format on

  EXPECT_FALSE(lqp_left_join_basic->is_column_nullable(ColumnID{0}));
  EXPECT_FALSE(lqp_left_join_basic->is_column_nullable(ColumnID{1}));
  EXPECT_FALSE(lqp_left_join_basic->is_column_nullable(ColumnID{2}));
  EXPECT_TRUE(lqp_left_join_basic->is_column_nullable(ColumnID{3}));
  EXPECT_TRUE(lqp_left_join_basic->is_column_nullable(ColumnID{4}));

  // clang-format off
  const auto lqp_left_join =
  ProjectionNode::make(expression_vector(_t_a_a, _t_b_x, add_(_t_a_a, _t_b_x), add_(_t_a_a, 3), is_null_(add_(_t_a_a, _t_b_x))),  // NOLINT
    JoinNode::make(JoinMode::Left, equals_(_t_a_a, _t_b_x),
      _mock_node_a,
      _mock_node_b));
  // clang-format on

  EXPECT_FALSE(lqp_left_join->is_column_nullable(ColumnID{0}));
  EXPECT_TRUE(lqp_left_join->is_column_nullable(ColumnID{1}));
  EXPECT_TRUE(lqp_left_join->is_column_nullable(ColumnID{2}));
  EXPECT_FALSE(lqp_left_join->is_column_nullable(ColumnID{3}));
  EXPECT_FALSE(lqp_left_join->is_column_nullable(ColumnID{4}));

  // clang-format off
  const auto lqp_right_join =
  ProjectionNode::make(expression_vector(_t_a_a, _t_b_x, add_(_t_a_a, _t_b_x), add_(_t_a_a, 3), is_null_(add_(_t_a_a, _t_b_x))),  // NOLINT
    JoinNode::make(JoinMode::Right, equals_(_t_a_a, _t_b_x),
      _mock_node_a,
      _mock_node_b));
  // clang-format on

  EXPECT_TRUE(lqp_right_join->is_column_nullable(ColumnID{0}));
  EXPECT_FALSE(lqp_right_join->is_column_nullable(ColumnID{1}));
  EXPECT_TRUE(lqp_right_join->is_column_nullable(ColumnID{2}));
  EXPECT_TRUE(lqp_right_join->is_column_nullable(ColumnID{3}));
  EXPECT_FALSE(lqp_right_join->is_column_nullable(ColumnID{4}));

  // clang-format off
  const auto lqp_full_join =
  ProjectionNode::make(expression_vector(_t_a_a, _t_b_x, add_(_t_a_a, _t_b_x), add_(_t_a_a, 3), is_null_(add_(_t_a_a, _t_b_x))),  // NOLINT
    JoinNode::make(JoinMode::FullOuter, equals_(_t_a_a, _t_b_x),
      _mock_node_a,
      _mock_node_b));
  // clang-format on

  EXPECT_TRUE(lqp_full_join->is_column_nullable(ColumnID{0}));
  EXPECT_TRUE(lqp_full_join->is_column_nullable(ColumnID{1}));
  EXPECT_TRUE(lqp_full_join->is_column_nullable(ColumnID{2}));
  EXPECT_TRUE(lqp_full_join->is_column_nullable(ColumnID{3}));
  EXPECT_FALSE(lqp_full_join->is_column_nullable(ColumnID{4}));
}

TEST_P(JoinNodeMultiJoinModeTest, FunctionalDependenciesForwardNonTrivialLeft) {
  const auto join_mode = GetParam();
  auto join_node = std::shared_ptr<JoinNode>();
  if (join_mode == JoinMode::Cross) {
    join_node = JoinNode::make(JoinMode::Cross, _mock_node_a, _mock_node_b);
  } else {
    join_node = JoinNode::make(join_mode, equals_(_t_a_a, _t_b_x), _mock_node_a, _mock_node_b);
  }

  // Left input Node has non-trivial FDs.
  const auto fd_a = FunctionalDependency{{_t_a_a}, {_t_a_b}};
  _mock_node_a->set_non_trivial_functional_dependencies({fd_a});
  _mock_node_b->set_non_trivial_functional_dependencies({});
  EXPECT_TRUE(_mock_node_a->unique_column_combinations()->empty());
  EXPECT_TRUE(_mock_node_b->unique_column_combinations()->empty());

  if (join_mode == JoinMode::Right || join_mode == JoinMode::FullOuter) {
    EXPECT_TRUE(join_node->non_trivial_functional_dependencies().empty());
  } else {
    EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 1);
    EXPECT_TRUE(join_node->non_trivial_functional_dependencies().contains(fd_a));
  }
}

TEST_P(JoinNodeMultiJoinModeTest, FunctionalDependenciesForwardNonTrivialRight) {
  const auto join_mode = GetParam();
  auto join_node = std::shared_ptr<JoinNode>();
  if (join_mode == JoinMode::Cross) {
    join_node = JoinNode::make(JoinMode::Cross, _mock_node_a, _mock_node_b);
  } else {
    join_node = JoinNode::make(join_mode, equals_(_t_a_a, _t_b_x), _mock_node_a, _mock_node_b);
  }

  // Right input Node has non-trivial FDs.
  const auto fd_x = FunctionalDependency{{_t_b_x}, {_t_b_y}};
  _mock_node_a->set_non_trivial_functional_dependencies({});
  _mock_node_b->set_non_trivial_functional_dependencies({fd_x});
  EXPECT_TRUE(_mock_node_a->unique_column_combinations()->empty());
  EXPECT_TRUE(_mock_node_b->unique_column_combinations()->empty());

  switch (join_mode) {
    case JoinMode::Left:
    case JoinMode::FullOuter:
    case JoinMode::Semi:
    case JoinMode::AntiNullAsTrue:
    case JoinMode::AntiNullAsFalse:
      EXPECT_TRUE(join_node->non_trivial_functional_dependencies().empty());
      break;
    case JoinMode::Inner:
    case JoinMode::Right:
    case JoinMode::Cross:
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 1);
      EXPECT_TRUE(join_node->non_trivial_functional_dependencies().contains(fd_x));
  }
}

TEST_P(JoinNodeMultiJoinModeTest, FunctionalDependenciesForwardNonTrivialBoth) {
  const auto join_mode = GetParam();
  auto join_node = std::shared_ptr<JoinNode>();
  if (join_mode == JoinMode::Cross) {
    join_node = JoinNode::make(JoinMode::Cross, _mock_node_a, _mock_node_b);
  } else {
    join_node = JoinNode::make(join_mode, equals_(_t_a_a, _t_b_x), _mock_node_a, _mock_node_b);
  }

  // Both input nodes have non-trivial FDs
  const auto fd_a = FunctionalDependency{{_t_a_a}, {_t_a_b}};
  const auto fd_x = FunctionalDependency{{_t_b_x}, {_t_b_y}};
  _mock_node_a->set_non_trivial_functional_dependencies({fd_a});
  _mock_node_b->set_non_trivial_functional_dependencies({fd_x});
  EXPECT_TRUE(_mock_node_a->unique_column_combinations()->empty());
  EXPECT_TRUE(_mock_node_b->unique_column_combinations()->empty());

  switch (join_mode) {
    case JoinMode::FullOuter:
      EXPECT_TRUE(join_node->non_trivial_functional_dependencies().empty());
      break;
    case JoinMode::Left:
    case JoinMode::Semi:
    case JoinMode::AntiNullAsTrue:
    case JoinMode::AntiNullAsFalse:
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 1);
      EXPECT_TRUE(join_node->non_trivial_functional_dependencies().contains(fd_a));
      break;
    case JoinMode::Right:
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 1);
      EXPECT_TRUE(join_node->non_trivial_functional_dependencies().contains(fd_x));
      break;
    case JoinMode::Inner:
    case JoinMode::Cross:
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 2);
      EXPECT_TRUE(join_node->non_trivial_functional_dependencies().contains(fd_a));
      EXPECT_TRUE(join_node->non_trivial_functional_dependencies().contains(fd_x));
  }
}

TEST_P(JoinNodeMultiJoinModeTest, FunctionalDependenciesForwardNonTrivialBothAndDerive) {
  const auto join_mode = GetParam();
  auto join_node = std::shared_ptr<JoinNode>();
  if (join_mode == JoinMode::Cross) {
    join_node = JoinNode::make(JoinMode::Cross, _mock_node_a, _mock_node_b);
  } else {
    join_node = JoinNode::make(join_mode, equals_(_t_a_a, _t_b_x), _mock_node_a, _mock_node_b);
  }

  /**
     * Test whether non-trivial FD forwarding still works when deriving FDs from unique column combinations:
     *  - We specify UCCs for both input tables.
     *  - We enforce the dismissal of UCCs for all join modes by making none of the join columns unique.
     *    Consequently, we expect non-trivial FDs that were derived from the input nodes' unique column combinations.
     *  - Semi- and Anti-Joins preserve the UCCs, so they should not generate non-trivial FDs but keep the left input's
     *    UCCs of the left input.
     */
  const auto fd_a = FunctionalDependency{{_t_a_a}, {_t_a_b}};
  const auto fd_x = FunctionalDependency{{_t_b_x}, {_t_b_y}};
  _mock_node_a->set_non_trivial_functional_dependencies({fd_a});
  _mock_node_b->set_non_trivial_functional_dependencies({fd_x});
  _mock_node_a->set_key_constraints({*_key_constraint_b_c});
  _mock_node_b->set_key_constraints({*_key_constraint_y});
  const auto generated_fd_b_c = FunctionalDependency{{_t_a_b, _t_a_c}, {_t_a_a}};
  const auto generated_fd_y = FunctionalDependency{{_t_b_y}, {_t_b_x}};

  switch (join_mode) {
    case JoinMode::Semi:
    case JoinMode::AntiNullAsTrue:
    case JoinMode::AntiNullAsFalse:
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 1);
      EXPECT_TRUE(join_node->non_trivial_functional_dependencies().contains(fd_a));
      EXPECT_EQ(join_node->unique_column_combinations()->size(), 1);
      EXPECT_EQ(*join_node->unique_column_combinations(), *_mock_node_a->unique_column_combinations());
      break;
    case JoinMode::Left:
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 2);
      EXPECT_TRUE(join_node->non_trivial_functional_dependencies().contains(fd_a));
      EXPECT_TRUE(join_node->non_trivial_functional_dependencies().contains(generated_fd_b_c));
      break;
    case JoinMode::Right:
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 2);
      EXPECT_TRUE(join_node->non_trivial_functional_dependencies().contains(fd_x));
      EXPECT_TRUE(join_node->non_trivial_functional_dependencies().contains(generated_fd_y));
      break;
    case JoinMode::FullOuter:
      EXPECT_TRUE(join_node->non_trivial_functional_dependencies().empty());
      break;
    case JoinMode::Inner:
    case JoinMode::Cross:
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 4);
      EXPECT_TRUE(join_node->non_trivial_functional_dependencies().contains(fd_a));
      EXPECT_TRUE(join_node->non_trivial_functional_dependencies().contains(generated_fd_b_c));
      EXPECT_TRUE(join_node->non_trivial_functional_dependencies().contains(fd_x));
      EXPECT_TRUE(join_node->non_trivial_functional_dependencies().contains(generated_fd_y));
  }
}

TEST_F(JoinNodeTest, FunctionalDependenciesDeriveNone) {
  /**
   * Set UCCs for both join columns of an Inner Join, so that UCCs from both sides are forwarded. Consequently, we do
   * not expect non-trivial FDs from the left or right input nodes' UCCs to be derived.
   */
  _mock_node_a->set_key_constraints({*_key_constraint_a});
  _mock_node_b->set_key_constraints({*_key_constraint_x});

  // MockNodes with non-trivial FDs
  const auto fd_b = FunctionalDependency{{_t_a_b}, {_t_a_a}};
  const auto fd_y = FunctionalDependency{{_t_b_y}, {_t_b_x}};
  _mock_node_a->set_non_trivial_functional_dependencies({fd_b});
  _mock_node_b->set_non_trivial_functional_dependencies({fd_y});

  // clang-format off
  const auto& join_node =
  JoinNode::make(JoinMode::Inner, equals_(_t_a_a, _t_b_x),
    _mock_node_a,
    _mock_node_b);
  // clang-format on

  // Tests
  EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 2);
  EXPECT_TRUE(join_node->non_trivial_functional_dependencies().contains(fd_b));
  EXPECT_TRUE(join_node->non_trivial_functional_dependencies().contains(fd_y));

  EXPECT_EQ(join_node->functional_dependencies().size(), 4);
  EXPECT_TRUE(join_node->functional_dependencies().contains(fd_b));
  EXPECT_TRUE(join_node->functional_dependencies().contains(fd_y));
  const auto generated_fd_a = FunctionalDependency{{_t_a_a}, {_t_a_b, _t_a_c, _t_b_x, _t_b_y}};
  EXPECT_TRUE(join_node->functional_dependencies().contains(generated_fd_a));
  const auto generated_fd_x = FunctionalDependency{{_t_b_x}, {_t_b_y, _t_a_a, _t_a_b, _t_a_c}};
  EXPECT_TRUE(join_node->functional_dependencies().contains(generated_fd_x));
}

TEST_F(JoinNodeTest, FunctionalDependenciesDeriveLeftOnly) {
  /**
   * We set a UCC for the left, but not for the right join column of the Inner Join. Consequently, UCCs of the left
   * input node are discarded, whereas the UCCs of the right input node survive. Therefore, we have to check whether
   * left input node's trivial FDs are forwarded as non-trivial ones.
   */
  _mock_node_a->set_key_constraints({*_key_constraint_a});
  _mock_node_b->set_key_constraints({*_key_constraint_x});
  // clang-format off
  const auto& join_node =
  JoinNode::make(JoinMode::Inner, equals_(_t_a_a, _t_b_y),
    _mock_node_a,
    _mock_node_b);
  // clang-format on

  // Tests
  const auto generated_fd_a = FunctionalDependency{{_t_a_a}, {_t_a_b, _t_a_c}};
  EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 1);
  EXPECT_TRUE(join_node->non_trivial_functional_dependencies().contains(generated_fd_a));

  const auto generated_fd_x = FunctionalDependency{{_t_b_x}, {_t_a_a, _t_a_b, _t_a_c, _t_b_y}};
  EXPECT_EQ(join_node->functional_dependencies().size(), 2);
  EXPECT_TRUE(join_node->functional_dependencies().contains(generated_fd_a));
  EXPECT_TRUE(join_node->functional_dependencies().contains(generated_fd_x));
}

TEST_F(JoinNodeTest, FunctionalDependenciesUnify) {
  const auto key_constraint_a_b =
      TableKeyConstraint{{_t_a_a->original_column_id, _t_a_b->original_column_id}, KeyConstraintType::PRIMARY_KEY};
  const auto key_constraint_c = TableKeyConstraint{{_t_a_c->original_column_id}, KeyConstraintType::UNIQUE};
  _mock_node_a->set_key_constraints({key_constraint_a_b, key_constraint_c});
  _mock_node_b->set_key_constraints({*_key_constraint_x});

  // The following FD is trivial since it can be derived from a UCC (PRIMARY KEY across a & b). However, we define it
  // as non-trivial anyway, to verify the conflict resolution when merging FDs later on.
  const auto fd_a_b = FunctionalDependency{{_t_a_a, _t_a_b}, {_t_a_c}};
  _mock_node_a->set_non_trivial_functional_dependencies({fd_a_b});

  // Define an Inner Join, so that all UCCs survive.
  // clang-format off
  const auto& join_node =
  JoinNode::make(JoinMode::Inner, equals_(_t_a_c, _t_b_x),
    _mock_node_a,
    _mock_node_b);
  // clang-format on

  // After the join, we expect the following FDs to be returned:
  const auto expected_fd_a_b = FunctionalDependency{{_t_a_a, _t_a_b}, {_t_a_c, _t_b_x, _t_b_y}};
  const auto expected_fd_c = FunctionalDependency{{_t_a_c}, {_t_a_a, _t_a_b, _t_b_x, _t_b_y}};
  const auto expected_fd_x = FunctionalDependency{{_t_b_x}, {_t_a_a, _t_a_b, _t_a_c, _t_b_y}};

  // Prerequisites
  const auto& non_trivial_fds = join_node->non_trivial_functional_dependencies();
  EXPECT_EQ(non_trivial_fds.size(), 1);
  EXPECT_TRUE(non_trivial_fds.contains(fd_a_b));

  const auto& trivial_fds = fds_from_unique_column_combinations(join_node, join_node->unique_column_combinations());
  EXPECT_EQ(trivial_fds.size(), 3);
  EXPECT_TRUE(trivial_fds.contains(expected_fd_a_b));
  EXPECT_TRUE(trivial_fds.contains(expected_fd_c));
  EXPECT_TRUE(trivial_fds.contains(expected_fd_x));

  /**
   * After unifiying the two FD sets above, we expect three and instead of four FDs since the following FD objects
   *   {a, b} => {c} and
   *   {a, b} => {c, x, y}
   * can be merged into one.
   */
  const auto fds_unified = union_fds(non_trivial_fds, trivial_fds);
  EXPECT_EQ(fds_unified.size(), 3);
  EXPECT_TRUE(fds_unified.contains(expected_fd_a_b));
  EXPECT_TRUE(fds_unified.contains(expected_fd_c));
  EXPECT_TRUE(fds_unified.contains(expected_fd_x));
  EXPECT_EQ(fds_unified, join_node->functional_dependencies());
}

/**
 * The following four tests check the forwarding of UCCs given the uniqueness of the input nodes' join columns. See the
 * table for a brief overview of the test cases:
 *
 *                      |                               Join columns of left input is unique
 *                      |                      Yes                      |                      No
 * ---------------------------------------------------------------------------------------------------------------------
 * Join column of   Yes | UniqueColumnCombinationsBothJoinColumnsUnique | UniqueColumnCombinationsRightJoinColumnUnique
 * right input    ------------------------------------------------------------------------------------------------------
 * is unique        No  | UniqueColumnCombinationsLeftJoinColumnUnique  | UniqueColumnCombinationsNoJoinColumnUnique
 *
 */
TEST_P(JoinNodeMultiJoinModeTest, UniqueColumnCombinationsNoJoinColumnUnique) {
  const auto join_mode = GetParam();
  auto join_node = std::shared_ptr<JoinNode>{};
  if (join_mode == JoinMode::Cross) {
    join_node = JoinNode::make(join_mode, _mock_node_a, _mock_node_b);
  } else {
    // clang-format off
    join_node =
    JoinNode::make(join_mode, equals_(_t_a_a, _t_b_y),
      _mock_node_a,
      _mock_node_b);
    // clang-format on
  }

  // Case 1 – LEFT  table's join column (a) uniqueness : No
  //          RIGHT table's join column (y) uniqueness : No
  _mock_node_a->set_key_constraints({*_key_constraint_b_c});
  _mock_node_b->set_key_constraints({*_key_constraint_x});

  const auto& join_unique_column_combinations = join_node->unique_column_combinations();
  if (is_semi_or_anti_join(join_mode)) {
    // Semi- and Anti-Joins act as filters for the left input and preserve its unique column combinations.
    EXPECT_EQ(join_unique_column_combinations->size(), 1);
    EXPECT_TRUE(find_ucc_by_key_constraint(*_key_constraint_b_c, join_unique_column_combinations));
  } else {
    // Not unique join columns might lead to row-/value-duplication in the opposite relation. No UCCs remain valid.
    EXPECT_TRUE(join_unique_column_combinations->empty());
  }
}

TEST_P(JoinNodeMultiJoinModeTest, UniqueColumnCombinationsLeftJoinColumnUnique) {
  const auto join_mode = GetParam();
  auto join_node = std::shared_ptr<JoinNode>{};
  if (join_mode == JoinMode::Cross) {
    join_node = JoinNode::make(join_mode, _mock_node_a, _mock_node_b);
  } else {
    // clang-format off
    join_node =
    JoinNode::make(join_mode, equals_(_t_a_a, _t_b_y),
      _mock_node_a,
      _mock_node_b);
    // clang-format on
  }

  // Case 2 – LEFT  table's join column (a) uniqueness : Yes
  //          RIGHT table's join column (y) uniqueness : No
  _mock_node_a->set_key_constraints({*_key_constraint_a, *_key_constraint_b_c});
  _mock_node_b->set_key_constraints({*_key_constraint_x});

  const auto& join_unique_column_combinations = join_node->unique_column_combinations();
  switch (join_mode) {
    case JoinMode::Cross:
      // Cross joins should never forward UCCs.
      EXPECT_TRUE(join_unique_column_combinations->empty());
      break;
    case JoinMode::Semi:
    case JoinMode::AntiNullAsTrue:
    case JoinMode::AntiNullAsFalse:
      // Semi- and Anti-Joins act as filters for the left input and preserve its unique column combinations.
      EXPECT_EQ(join_unique_column_combinations->size(), 2);
      EXPECT_EQ(*join_unique_column_combinations, *_mock_node_a->unique_column_combinations());
      break;
    case JoinMode::Inner:
    case JoinMode::Left:
    case JoinMode::Right:
    case JoinMode::FullOuter:
      // UCCs of the right input should be forwarded. Since our current UCCimplementation is compatible with NULL
      // values, outer joins are handled like inner joins.
      EXPECT_EQ(join_unique_column_combinations->size(), 1);
      EXPECT_EQ(*join_unique_column_combinations, *_mock_node_b->unique_column_combinations());
      break;
  }
}

TEST_P(JoinNodeMultiJoinModeTest, UniqueColumnCombinationsRightJoinColumnUnique) {
  const auto join_mode = GetParam();
  auto join_node = std::shared_ptr<JoinNode>{};
  if (join_mode == JoinMode::Cross) {
    join_node = JoinNode::make(join_mode, _mock_node_a, _mock_node_b);
  } else {
    // clang-format off
    join_node =
    JoinNode::make(join_mode, equals_(_t_a_a, _t_b_y),
      _mock_node_a,
      _mock_node_b);
    // clang-format on
  }

  // Case 3 – LEFT  table's join column (a) uniqueness : No
  //          RIGHT table's join column (y) uniqueness : Yes
  _mock_node_a->set_key_constraints({*_key_constraint_b_c});
  _mock_node_b->set_key_constraints({*_key_constraint_x, *_key_constraint_y});

  const auto& join_unique_column_combinations = join_node->unique_column_combinations();
  if (join_mode == JoinMode::Cross) {
    // Cross joins should never forward UCCs.
    EXPECT_TRUE(join_unique_column_combinations->empty());
    return;
  }
  // UCCs of the left input should be forwarded.
  EXPECT_EQ(join_unique_column_combinations->size(), 1);
  EXPECT_EQ(*join_unique_column_combinations, *_mock_node_a->unique_column_combinations());
}

TEST_P(JoinNodeMultiJoinModeTest, UniqueColumnCombinationsBothJoinColumnsUnique) {
  const auto join_mode = GetParam();
  auto join_node = std::shared_ptr<JoinNode>{};
  if (join_mode == JoinMode::Cross) {
    join_node = JoinNode::make(join_mode, _mock_node_a, _mock_node_b);
  } else {
    // clang-format off
    join_node =
    JoinNode::make(join_mode, equals_(_t_a_a, _t_b_y),
      _mock_node_a,
      _mock_node_b);
    // clang-format on
  }

  // Case 4 – LEFT  table's join column (a) uniqueness : Yes
  //          RIGHT table's join column (y) uniqueness : Yes
  _mock_node_a->set_key_constraints({*_key_constraint_a, *_key_constraint_b_c});
  _mock_node_b->set_key_constraints({*_key_constraint_x, *_key_constraint_y});

  const auto& join_unique_column_combinations = join_node->unique_column_combinations();
  if (join_mode == JoinMode::Cross) {
    EXPECT_TRUE(join_unique_column_combinations->empty());
    return;
  }

  // For all other join modes, UCCs of the left input should be forwarded.
  EXPECT_TRUE(find_ucc_by_key_constraint(*_key_constraint_a, join_unique_column_combinations));
  EXPECT_TRUE(find_ucc_by_key_constraint(*_key_constraint_b_c, join_unique_column_combinations));

  if (is_semi_or_anti_join(join_mode)) {
    EXPECT_EQ(join_unique_column_combinations->size(), 2);
    return;
  }

  // Inner and outer joins should also forward the right input's UCCs.
  EXPECT_EQ(join_unique_column_combinations->size(), 4);
  EXPECT_TRUE(find_ucc_by_key_constraint(*_key_constraint_x, join_unique_column_combinations));
  EXPECT_TRUE(find_ucc_by_key_constraint(*_key_constraint_y, join_unique_column_combinations));
}

TEST_P(JoinNodeMultiJoinModeTest, UniqueColumnCombinationsNonEquiJoin) {
  // Currently, we do not support UCC forwarding for Non-Equi- or Theta-Joins. Semi-, cross-, and Anti-Joins only
  // support Equi-Joins.
  const auto join_mode = GetParam();
  if (join_mode == JoinMode::Cross || is_semi_or_anti_join(join_mode)) {
    GTEST_SKIP();
  }

  _mock_node_a->set_key_constraints({*_key_constraint_a, *_key_constraint_b_c});
  _mock_node_b->set_key_constraints({*_key_constraint_x, *_key_constraint_y});
  // clang-format off
  const auto theta_join_node =
  JoinNode::make(join_mode, greater_than_(_t_a_a, _t_b_x),
    _mock_node_a,
    _mock_node_b);
  // clang-format on

  EXPECT_TRUE(theta_join_node->unique_column_combinations()->empty());
}

TEST_P(JoinNodeMultiJoinModeTest, UniqueColumnCombinationsMultiPredicateJoin) {
  // Except for Semi- and Anti-Joins, we do not support forwarding of UCCs for multi-predicate joins.
  const auto join_mode = GetParam();
  if (join_mode == JoinMode::Cross) {
    GTEST_SKIP();
  }

  _mock_node_a->set_key_constraints({*_key_constraint_a, *_key_constraint_b_c});
  _mock_node_b->set_key_constraints({*_key_constraint_x, *_key_constraint_y});
  // clang-format off
  const auto join_node =
  JoinNode::make(join_mode, expression_vector(less_than_(_t_a_a, _t_b_x), greater_than_(_t_a_a, _t_b_y)),
    _mock_node_a,
    _mock_node_b);
  // clang-format on

  if (is_semi_or_anti_join(join_mode)) {
    EXPECT_EQ(*join_node->unique_column_combinations(), *_mock_node_a->unique_column_combinations());
  } else {
    EXPECT_TRUE(join_node->unique_column_combinations()->empty());
  }
}

TEST_F(JoinNodeTest, GetOrFindReducedJoinNode) {
  const auto join_predicate = equals_(_t_a_a, _t_b_x);
  auto semi_reduction_node = JoinNode::make(JoinMode::Semi, join_predicate, _mock_node_a, _mock_node_b);
  // clang-format off
  const auto lqp =
  JoinNode::make(JoinMode::Inner, join_predicate,
    PredicateNode::make(greater_than_(_t_a_a, _t_a_b),
      semi_reduction_node),
    _mock_node_b);
  // clang-format on
  const auto join_node = std::static_pointer_cast<JoinNode>(lqp);
  semi_reduction_node->mark_as_semi_reduction(join_node);

  // The semi join reduction node should use the stored weak pointer to create and return a shared pointer to the
  // reduced join.
  EXPECT_EQ(semi_reduction_node->get_or_find_reduced_join_node(), join_node);

  // In a deep-copied LQP, the semi reduction node's weak pointer to the reduced join is unset (lazy approach).
  // Therefore, get_or_find_reduced_join_node must utilize an LQP upwards traversal for discovering the reduced join.
  const auto copied_lqp = lqp->deep_copy();
  const auto copied_join_node = std::static_pointer_cast<JoinNode>(copied_lqp);
  const auto copied_semi_reduction_node = std::static_pointer_cast<JoinNode>(copied_lqp->left_input()->left_input());
  EXPECT_EQ(copied_semi_reduction_node->get_or_find_reduced_join_node(), copied_join_node);

  // If there is no reduced join, we expect to fail.
  EXPECT_THROW(std::static_pointer_cast<JoinNode>(semi_reduction_node->deep_copy())->get_or_find_reduced_join_node(),
               std::logic_error);
}

TEST_F(JoinNodeTest, GetOrFindReducedJoinNodeWithMultiplePredicates) {
  // In contrast to the test GetOrFindReducedJoinNode, we define a semi reduction for a join with
  // multiple predicates.

  // clang-format off
  const auto semi_reduction_predicate = equals_(_t_a_a, _t_b_x);
  auto semi_reduction_node =
  JoinNode::make(JoinMode::Semi, semi_reduction_predicate,
    _mock_node_a,
    _mock_node_b);

  const auto lqp =
  JoinNode::make(JoinMode::Inner, expression_vector(less_than_(_t_a_b, _t_b_y), semi_reduction_predicate),
    PredicateNode::make(greater_than_(_t_a_a, _t_a_b),
      semi_reduction_node),
    _mock_node_b);
  // clang-format on

  const auto join_node = std::static_pointer_cast<JoinNode>(lqp);
  semi_reduction_node->mark_as_semi_reduction(join_node);

  // The semi join reduction node should use the stored weak pointer to create and return a shared pointer to the
  // reduced join.
  EXPECT_EQ(semi_reduction_node->get_or_find_reduced_join_node(), join_node);

  // In a deep-copied LQP, the semi reduction node's weak pointer to the reduced join is unset (lazy approach).
  // Therefore, get_or_find_reduced_join_node must utilize an LQP upwards traversal for discovering the reduced join.
  const auto copied_lqp = lqp->deep_copy();
  const auto copied_join_node = std::dynamic_pointer_cast<JoinNode>(copied_lqp);
  const auto copied_semi_reduction_node = std::dynamic_pointer_cast<JoinNode>(copied_lqp->left_input()->left_input());
  EXPECT_EQ(copied_semi_reduction_node->get_or_find_reduced_join_node(), copied_join_node);
}

}  // namespace hyrise
