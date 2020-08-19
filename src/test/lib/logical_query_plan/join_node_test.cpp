#include <memory>
#include <utility>

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "utils/constraint_test_utils.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

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
    _anti_join_node = JoinNode::make(JoinMode::AntiNullAsTrue, equals_(_t_a_a, _t_b_y), _mock_node_a, _mock_node_b);

    // Prepare constraint definitions
    _key_constraint_a = TableKeyConstraint{{_t_a_a->original_column_id}, KeyConstraintType::UNIQUE};
    _key_constraint_b_c =
        TableKeyConstraint{{_t_a_b->original_column_id, _t_a_c->original_column_id}, KeyConstraintType::UNIQUE};
    _key_constraint_x = TableKeyConstraint{{_t_b_x->original_column_id}, KeyConstraintType::UNIQUE};
    _key_constraint_y = TableKeyConstraint{{_t_b_y->original_column_id}, KeyConstraintType::UNIQUE};
  }

  std::shared_ptr<MockNode> _mock_node_a;
  std::shared_ptr<MockNode> _mock_node_b;
  std::shared_ptr<JoinNode> _inner_join_node;
  std::shared_ptr<JoinNode> _semi_join_node;
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

TEST_F(JoinNodeTest, Description) { EXPECT_EQ(_cross_join_node->description(), "[Join] Mode: Cross"); }

TEST_F(JoinNodeTest, DescriptionInnerJoin) { EXPECT_EQ(_inner_join_node->description(), "[Join] Mode: Inner [a = y]"); }

TEST_F(JoinNodeTest, DescriptionSemiJoin) { EXPECT_EQ(_semi_join_node->description(), "[Join] Mode: Semi [a = y]"); }

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
}

TEST_F(JoinNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*_cross_join_node, *_cross_join_node);
  EXPECT_EQ(*_inner_join_node, *_inner_join_node);
  EXPECT_EQ(*_semi_join_node, *_semi_join_node);
  EXPECT_EQ(*_anti_join_node, *_anti_join_node);

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
}

TEST_F(JoinNodeTest, Copy) {
  EXPECT_EQ(*_cross_join_node, *_cross_join_node->deep_copy());
  EXPECT_EQ(*_inner_join_node, *_inner_join_node->deep_copy());
  EXPECT_EQ(*_semi_join_node, *_semi_join_node->deep_copy());
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

TEST_F(JoinNodeTest, FunctionalDependenciesSemiAndAntiJoins) {
  // Preparations
  const auto fd_a = FunctionalDependency{{_t_a_a}, {_t_a_b}};
  const auto fd_x = FunctionalDependency{{_t_b_x}, {_t_b_y}};
  _mock_node_a->set_non_trivial_functional_dependencies({fd_a});
  EXPECT_EQ(_mock_node_a->non_trivial_functional_dependencies().size(), 1);
  EXPECT_EQ(_mock_node_a->non_trivial_functional_dependencies().at(0), fd_a);
  _mock_node_b->set_non_trivial_functional_dependencies({fd_x});
  EXPECT_EQ(_mock_node_b->non_trivial_functional_dependencies().size(), 1);
  EXPECT_EQ(_mock_node_b->non_trivial_functional_dependencies().at(0), fd_x);

  // Tests
  for (const auto join_mode : {JoinMode::Semi, JoinMode::AntiNullAsTrue, JoinMode::AntiNullAsFalse}) {
    // clang-format off
    const auto join_node =
        JoinNode::make(join_mode, equals_(_t_a_a, _t_b_y),
                       _mock_node_a,
                       _mock_node_b);
    // clang-format on

    // We do not want JoinNode to return the FDs of the right input node
    const auto& non_trivial_fds = join_node->non_trivial_functional_dependencies();
    EXPECT_EQ(non_trivial_fds.size(), 1);
    EXPECT_EQ(non_trivial_fds.at(0), fd_a);
  }
}

TEST_F(JoinNodeTest, FunctionalDependenciesForwardNonTrivialFDsLeft) {
  const auto fd_a = FunctionalDependency{{_t_a_a}, {_t_a_b}};
  const auto join_column_a = _t_a_a;
  const auto join_column_x = _t_b_x;

  for (const auto join_mode :
       {JoinMode::Inner, JoinMode::Left, JoinMode::Right, JoinMode::FullOuter, JoinMode::Cross}) {
    auto join_node = std::shared_ptr<JoinNode>();
    if (join_mode == JoinMode::Cross) {
      join_node = JoinNode::make(JoinMode::Cross, _mock_node_a, _mock_node_b);
    } else {
      join_node = JoinNode::make(join_mode, equals_(join_column_a, join_column_x), _mock_node_a, _mock_node_b);
    }

    // Left input Node has non-trivial FDs
    _mock_node_a->set_key_constraints({});
    _mock_node_b->set_key_constraints({});
    _mock_node_a->set_non_trivial_functional_dependencies({fd_a});
    _mock_node_b->set_non_trivial_functional_dependencies({});

    if (join_mode == JoinMode::Right || join_mode == JoinMode::FullOuter) {
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 0);
    } else {
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 1);
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().at(0), fd_a);
    }
  }
}

TEST_F(JoinNodeTest, FunctionalDependenciesForwardNonTrivialFDsRight) {
  const auto fd_x = FunctionalDependency{{_t_b_x}, {_t_b_y}};
  const auto join_column_a = _t_a_a;
  const auto join_column_x = _t_b_x;

  for (const auto join_mode :
      {JoinMode::Inner, JoinMode::Left, JoinMode::Right, JoinMode::FullOuter, JoinMode::Cross}) {
    auto join_node = std::shared_ptr<JoinNode>();
    if (join_mode == JoinMode::Cross) {
      join_node = JoinNode::make(JoinMode::Cross, _mock_node_a, _mock_node_b);
    } else {
      join_node = JoinNode::make(join_mode, equals_(join_column_a, join_column_x), _mock_node_a, _mock_node_b);
    }

    // Right input Node has non-trivial FDs
    _mock_node_a->set_key_constraints({});
    _mock_node_b->set_key_constraints({});
    _mock_node_a->set_non_trivial_functional_dependencies({});
    _mock_node_b->set_non_trivial_functional_dependencies({fd_x});

    if (join_mode == JoinMode::Left || join_mode == JoinMode::FullOuter) {
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 0);
    } else {
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 1);
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().at(0), fd_x);
    }
  }
}

TEST_F(JoinNodeTest, FunctionalDependenciesForwardNonTrivialFDsBoth) {
  const auto fd_a = FunctionalDependency{{_t_a_a}, {_t_a_b}};
  const auto fd_x = FunctionalDependency{{_t_b_x}, {_t_b_y}};
  const auto join_column_a = _t_a_a;
  const auto join_column_x = _t_b_x;

  for (const auto join_mode :
      {JoinMode::Inner, JoinMode::Left, JoinMode::Right, JoinMode::FullOuter, JoinMode::Cross}) {
    auto join_node = std::shared_ptr<JoinNode>();
    if (join_mode == JoinMode::Cross) {
      join_node = JoinNode::make(JoinMode::Cross, _mock_node_a, _mock_node_b);
    } else {
      join_node = JoinNode::make(join_mode, equals_(join_column_a, join_column_x), _mock_node_a, _mock_node_b);
    }

    // Both input nodes have non-trivial FDs
    _mock_node_a->set_key_constraints({});
    _mock_node_b->set_key_constraints({});
    _mock_node_a->set_non_trivial_functional_dependencies({fd_a});
    _mock_node_b->set_non_trivial_functional_dependencies({fd_x});

    if (join_mode == JoinMode::FullOuter) {
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 0);
    } else if (join_mode == JoinMode::Left) {
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 1);
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().at(0), fd_a);
    } else if (join_mode == JoinMode::Right) {
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 1);
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().at(0), fd_x);
    } else {
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 2);
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().at(0), fd_a);
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().at(1), fd_x);
    }
  }
}

TEST_F(JoinNodeTest, FunctionalDependenciesForwardNonTrivialAndTrivialFDs) {
  const auto fd_a = FunctionalDependency{{_t_a_a}, {_t_a_b}};
  const auto fd_x = FunctionalDependency{{_t_b_x}, {_t_b_y}};
  const auto generated_fd_b_c = FunctionalDependency{{_t_a_b, _t_a_c}, {_t_a_a}};
  const auto generated_fd_y = FunctionalDependency{{_t_b_y}, {_t_b_x}};
  const auto join_column_a = _t_a_a;
  const auto join_column_x = _t_b_x;

  for (const auto join_mode :
      {JoinMode::Inner, JoinMode::Left, JoinMode::Right, JoinMode::FullOuter, JoinMode::Cross}) {
    auto join_node = std::shared_ptr<JoinNode>();
    if (join_mode == JoinMode::Cross) {
      join_node = JoinNode::make(JoinMode::Cross, _mock_node_a, _mock_node_b);
    } else {
      join_node = JoinNode::make(join_mode, equals_(join_column_a, join_column_x), _mock_node_a, _mock_node_b);
    }

    /**
     * Test, whether FD forwarding still works when FDs from unique constraints are added to the output:
     *  - We specify unique constraints for both input tables
     *  - We do not provide uniqueness guarantees for the join columns a and x to enforce the dismissal of unique
     *    constraints for all join modes. Therefore, we can always expect derived, non-trivial FDs in the output.
     */
    _mock_node_a->set_key_constraints({*_key_constraint_b_c});
    _mock_node_b->set_key_constraints({*_key_constraint_y});
    _mock_node_a->set_non_trivial_functional_dependencies({fd_a});
    _mock_node_b->set_non_trivial_functional_dependencies({fd_x});

    if (join_mode == JoinMode::FullOuter) {
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 0);
    } else if (join_mode == JoinMode::Left) {
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 2);
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().at(0), fd_a);
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().at(1), generated_fd_b_c);
    } else if (join_mode == JoinMode::Right) {
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 2);
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().at(0), fd_x);
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().at(1), generated_fd_y);
    } else {
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 4);
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().at(0), fd_a);
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().at(1), generated_fd_b_c);
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().at(2), fd_x);
      EXPECT_EQ(join_node->non_trivial_functional_dependencies().at(3), generated_fd_y);
    }
  }
}

TEST_F(JoinNodeTest, FunctionalDependenciesForwardingInnerJoin) {
  // Set unique constraints on both join columns so that unique constraints get forwarded. Consequently, derived FDs
  // remain trivial despite the join operation.
  const auto join_column_a = _t_a_a;
  _mock_node_a->set_key_constraints({*_key_constraint_a});
  const auto join_column_x = _t_b_x;
  _mock_node_b->set_key_constraints({*_key_constraint_x});

  // MockNode with non-trivial FDs
  const auto fd_b = FunctionalDependency{{_t_a_b}, {_t_a_a}};
  const auto fd_y = FunctionalDependency{{_t_b_y}, {_t_b_x}};
  _mock_node_a->set_non_trivial_functional_dependencies({fd_b});
  _mock_node_b->set_non_trivial_functional_dependencies({fd_y});

  // Inner Join
  // clang-format off
  const auto& join_node =
  JoinNode::make(JoinMode::Inner, equals_(join_column_a, join_column_x),
    _mock_node_a,
    _mock_node_b);
  // clang-format on

  // Tests
  EXPECT_EQ(join_node->non_trivial_functional_dependencies().size(), 2);
  EXPECT_EQ(join_node->non_trivial_functional_dependencies().at(0), fd_b);
  EXPECT_EQ(join_node->non_trivial_functional_dependencies().at(1), fd_y);

  EXPECT_EQ(join_node->functional_dependencies().size(), 4);
  EXPECT_EQ(join_node->functional_dependencies().at(0), fd_b);
  EXPECT_EQ(join_node->functional_dependencies().at(1), fd_y);
  const auto generated_fd_a = FunctionalDependency{{_t_a_a}, {_t_a_b, _t_a_c, _t_b_x, _t_b_y}};
  EXPECT_EQ(join_node->functional_dependencies().at(2), generated_fd_a);
  const auto generated_fd_x = FunctionalDependency{{_t_b_x}, {_t_b_y, _t_a_a, _t_a_b, _t_a_c}};
  EXPECT_EQ(join_node->functional_dependencies().at(3), generated_fd_x);
}

TEST_F(JoinNodeTest, FunctionalDependenciesMerge) {
  const auto join_column_c = _t_a_c;
  const auto join_column_x = _t_b_x;

  const auto key_constraint_a_b = TableKeyConstraint{{_t_a_a->original_column_id, _t_a_b->original_column_id},KeyConstraintType::PRIMARY_KEY};
  const auto key_constraint_c = TableKeyConstraint{{_t_a_c->original_column_id}, KeyConstraintType::UNIQUE};
  _mock_node_a->set_key_constraints({key_constraint_a_b, key_constraint_c});
  _mock_node_b->set_key_constraints({*_key_constraint_x});

  // Not really a trivial FD since it can be derived from the given key constraint across columns a & b, but for the
  // sake of the test:
  _mock_node_a->set_non_trivial_functional_dependencies({FunctionalDependency{{_t_a_a, _t_a_b}, {_t_a_c}}});

  // clang-format off
  const auto& join_node =
  JoinNode::make(JoinMode::Inner, equals_(join_column_c, join_column_x),
    _mock_node_a,
    _mock_node_b);
  // clang-format on

  const auto& fds_join = join_node->functional_dependencies();
  EXPECT_EQ(fds_join.size(), 3);
  const auto expected_fd_a_b = FunctionalDependency{{_t_a_a, _t_a_b}, {_t_a_c, _t_b_x, _t_b_y}};
  const auto expected_fd_c = FunctionalDependency{{_t_a_c}, {_t_a_a, _t_a_b, _t_b_x, _t_b_y}};
  const auto expected_fd_x = FunctionalDependency{{_t_b_x}, {_t_a_a, _t_a_b, _t_a_c, _t_b_y}};

  EXPECT_EQ(fds_join.at(0), expected_fd_a_b);
  EXPECT_EQ(fds_join.at(1), expected_fd_c);
  EXPECT_EQ(fds_join.at(2), expected_fd_x);
}

TEST_F(JoinNodeTest, UniqueConstraintsSemiAndAntiJoins) {
  _mock_node_a->set_key_constraints({*_key_constraint_a, *_key_constraint_b_c});
  _mock_node_b->set_key_constraints({*_key_constraint_x});

  for (const auto join_mode : {JoinMode::Semi, JoinMode::AntiNullAsTrue, JoinMode::AntiNullAsFalse}) {
    // clang-format off
    const auto join_node =
    JoinNode::make(join_mode, equals_(_t_a_a, _t_b_y),
      _mock_node_a,
      _mock_node_b);
    // clang-format on

    EXPECT_EQ(*join_node->unique_constraints(), *_mock_node_a->unique_constraints());
  }
}

TEST_F(JoinNodeTest, UniqueConstraintsInnerAndOuterJoins) {
  // Test the forwarding logic of all, inner and outer joins based on join column uniqueness.
  // Any join column that is not unique might lead to row-/value-duplication in the opposite table. Hence, unique
  // constraints might break and should not be forwarded.
  // Since our current unique constraints implementation is compatible with NULL values, outer joins are handled like
  // inner joins.

  for (const auto join_mode : {JoinMode::Inner, JoinMode::Left, JoinMode::Right, JoinMode::FullOuter}) {
    // clang-format off
    const auto join_node =
    JoinNode::make(join_mode, equals_(_t_a_a, _t_b_y),
      _mock_node_a,
      _mock_node_b);
    // clang-format on

    // Case 1 – LEFT  table's join column (a) uniqueness : No
    //          RIGHT table's join column (y) uniqueness : No
    _mock_node_a->set_key_constraints({*_key_constraint_b_c});
    _mock_node_b->set_key_constraints({*_key_constraint_x});
    EXPECT_TRUE(join_node->unique_constraints()->empty());

    // Case 2 – LEFT  table's join column (a) uniqueness : Yes
    //          RIGHT table's join column (y) uniqueness : No
    _mock_node_a->set_key_constraints({*_key_constraint_a, *_key_constraint_b_c});
    _mock_node_b->set_key_constraints({*_key_constraint_x});

    // Expect unique constraints of RIGHT table to be forwarded
    auto join_unique_constraints = join_node->unique_constraints();
    EXPECT_EQ(join_unique_constraints->size(), 1);
    EXPECT_TRUE(*join_unique_constraints == *_mock_node_b->unique_constraints());

    // Case 3 – LEFT  table's join column (a) uniqueness : No
    //          RIGHT table's join column (y) uniqueness : Yes
    _mock_node_a->set_key_constraints({*_key_constraint_b_c});
    _mock_node_b->set_key_constraints({*_key_constraint_x, *_key_constraint_y});

    // Expect unique constraints of LEFT table (b_c) to be forwarded
    join_unique_constraints = join_node->unique_constraints();
    EXPECT_EQ(join_unique_constraints->size(), 1);
    EXPECT_TRUE(*join_unique_constraints == *_mock_node_a->unique_constraints());

    // Case 4 – LEFT  table's join column (a) uniqueness : Yes
    //          RIGHT table's join column (y) uniqueness : Yes
    _mock_node_a->set_key_constraints({*_key_constraint_a, *_key_constraint_b_c});
    _mock_node_b->set_key_constraints({*_key_constraint_x, *_key_constraint_y});

    // Expect unique constraints of both, LEFT (a, b_c) and RIGHT (x, y) table to be forwarded
    join_unique_constraints = join_node->unique_constraints();

    // Basic check
    EXPECT_EQ(join_unique_constraints->size(), 4);
    // In-depth checks
    EXPECT_TRUE(find_unique_constraint_by_key_constraint(*_key_constraint_a, join_unique_constraints));
    EXPECT_TRUE(find_unique_constraint_by_key_constraint(*_key_constraint_b_c, join_unique_constraints));
    EXPECT_TRUE(find_unique_constraint_by_key_constraint(*_key_constraint_x, join_unique_constraints));
    EXPECT_TRUE(find_unique_constraint_by_key_constraint(*_key_constraint_y, join_unique_constraints));
  }
}

TEST_F(JoinNodeTest, UniqueConstraintsNonEquiJoin) {
  // Currently, we do not support unique constraint forwarding for Non-Equi- or Theta-Joins
  _mock_node_a->set_key_constraints({*_key_constraint_a, *_key_constraint_b_c});
  _mock_node_b->set_key_constraints({*_key_constraint_x, *_key_constraint_y});
  // clang-format off
  const auto theta_join_node =
  JoinNode::make(JoinMode::Inner, greater_than_(_t_a_a, _t_b_x),
    _mock_node_a,
    _mock_node_b);
  // clang-format on

  EXPECT_TRUE(theta_join_node->unique_constraints()->empty());
}

TEST_F(JoinNodeTest, UniqueConstraintsNonSemiNonAntiMultiPredicateJoin) {
  // Except for Semi- and Anti-Joins, we do not support forwarding of unique constraints for multi-predicate joins.
  _mock_node_a->set_key_constraints({*_key_constraint_a, *_key_constraint_b_c});
  _mock_node_b->set_key_constraints({*_key_constraint_x, *_key_constraint_y});
  // clang-format off
  const auto join_node =
  JoinNode::make(JoinMode::Inner, expression_vector(less_than_(_t_a_a, _t_b_x), greater_than_(_t_a_a, _t_b_y)),
    _mock_node_a,
    _mock_node_b);
  // clang-format on

  EXPECT_TRUE(join_node->unique_constraints()->empty());
}

TEST_F(JoinNodeTest, UniqueConstraintsCrossJoin) {
  _mock_node_a->set_key_constraints({*_key_constraint_a, *_key_constraint_b_c});
  _mock_node_b->set_key_constraints({*_key_constraint_x, *_key_constraint_y});

  EXPECT_TRUE(_cross_join_node->unique_constraints()->empty());
}

}  // namespace opossum
