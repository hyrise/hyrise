#include <memory>
#include <utility>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class JoinNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    _mock_node_a = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "t_a");
    _mock_node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "x"}, {DataType::Float, "y"}}, "t_b");

    _t_a_a = {_mock_node_a, ColumnID{0}};
    _t_a_b = {_mock_node_a, ColumnID{1}};
    _t_a_c = {_mock_node_a, ColumnID{2}};
    _t_b_x = {_mock_node_b, ColumnID{0}};
    _t_b_y = {_mock_node_b, ColumnID{1}};

    _join_node = JoinNode::make(JoinMode::Cross, _mock_node_a, _mock_node_b);
    _join_node->set_left_input(_mock_node_a);
    _join_node->set_right_input(_mock_node_b);

    _inner_join_node = JoinNode::make(JoinMode::Inner, equals_(_t_a_a, _t_b_y), _mock_node_a, _mock_node_b);
    _semi_join_node = JoinNode::make(JoinMode::Semi, equals_(_t_a_a, _t_b_y), _mock_node_a, _mock_node_b);
    _anti_join_node = JoinNode::make(JoinMode::AntiNullAsTrue, equals_(_t_a_a, _t_b_y), _mock_node_a, _mock_node_b);
  }

  std::shared_ptr<MockNode> _mock_node_a;
  std::shared_ptr<MockNode> _mock_node_b;
  std::shared_ptr<JoinNode> _inner_join_node;
  std::shared_ptr<JoinNode> _semi_join_node;
  std::shared_ptr<JoinNode> _anti_join_node;
  std::shared_ptr<JoinNode> _join_node;
  LQPColumnReference _t_a_a;
  LQPColumnReference _t_a_b;
  LQPColumnReference _t_a_c;
  LQPColumnReference _t_b_x;
  LQPColumnReference _t_b_y;
};

TEST_F(JoinNodeTest, Description) { EXPECT_EQ(_join_node->description(), "[Join] Mode: Cross"); }

TEST_F(JoinNodeTest, DescriptionInnerJoin) { EXPECT_EQ(_inner_join_node->description(), "[Join] Mode: Inner [a = y]"); }

TEST_F(JoinNodeTest, DescriptionSemiJoin) { EXPECT_EQ(_semi_join_node->description(), "[Join] Mode: Semi [a = y]"); }

TEST_F(JoinNodeTest, DescriptionAntiJoin) {
  EXPECT_EQ(_anti_join_node->description(), "[Join] Mode: AntiNullAsTrue [a = y]");
}

TEST_F(JoinNodeTest, OutputColumnExpressions) {
  ASSERT_EQ(_join_node->column_expressions().size(), 5u);
  EXPECT_EQ(*_join_node->column_expressions().at(0), *lqp_column_(_t_a_a));
  EXPECT_EQ(*_join_node->column_expressions().at(1), *lqp_column_(_t_a_b));
  EXPECT_EQ(*_join_node->column_expressions().at(2), *lqp_column_(_t_a_c));
  EXPECT_EQ(*_join_node->column_expressions().at(3), *lqp_column_(_t_b_x));
  EXPECT_EQ(*_join_node->column_expressions().at(4), *lqp_column_(_t_b_y));
}

TEST_F(JoinNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*_join_node, *_join_node);
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
  EXPECT_EQ(*_join_node, *_join_node->deep_copy());
  EXPECT_EQ(*_inner_join_node, *_inner_join_node->deep_copy());
  EXPECT_EQ(*_semi_join_node, *_semi_join_node->deep_copy());
  EXPECT_EQ(*_anti_join_node, *_anti_join_node->deep_copy());
}

TEST_F(JoinNodeTest, OutputColumnReferencesSemiJoin) {
  ASSERT_EQ(_semi_join_node->column_expressions().size(), 3u);
  EXPECT_EQ(*_semi_join_node->column_expressions().at(0), *lqp_column_(_t_a_a));
  EXPECT_EQ(*_semi_join_node->column_expressions().at(1), *lqp_column_(_t_a_b));
  EXPECT_EQ(*_semi_join_node->column_expressions().at(2), *lqp_column_(_t_a_c));
}

TEST_F(JoinNodeTest, OutputColumnReferencesAntiJoin) {
  ASSERT_EQ(_anti_join_node->column_expressions().size(), 3u);
  EXPECT_EQ(*_anti_join_node->column_expressions().at(0), *lqp_column_(_t_a_a));
  EXPECT_EQ(*_anti_join_node->column_expressions().at(1), *lqp_column_(_t_a_b));
  EXPECT_EQ(*_anti_join_node->column_expressions().at(2), *lqp_column_(_t_a_c));
}

TEST_F(JoinNodeTest, NodeExpressions) {
  ASSERT_EQ(_inner_join_node->node_expressions.size(), 1u);
  EXPECT_EQ(*_inner_join_node->node_expressions.at(0u), *equals_(_t_a_a, _t_b_y));
  ASSERT_EQ(_join_node->node_expressions.size(), 0u);
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

}  // namespace opossum
