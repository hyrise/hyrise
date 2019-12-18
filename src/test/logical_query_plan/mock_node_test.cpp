#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "statistics/generate_pruning_statistics.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class MockNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    _mock_node_a = MockNode::make(MockNode::ColumnDefinitions{
        {DataType::Int, "a"}, {DataType::Float, "b"}, {DataType::Double, "c"}, {DataType::String, "d"}});

    _mock_node_b =
        MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}}, "mock_name");
  }

  std::shared_ptr<MockNode> _mock_node_a;
  std::shared_ptr<MockNode> _mock_node_b;
};

TEST_F(MockNodeTest, Description) {
  EXPECT_EQ(_mock_node_a->description(), "[MockNode 'Unnamed'] Columns: a b c d | pruned: 0/4 columns");
  EXPECT_EQ(_mock_node_b->description(), "[MockNode 'mock_name'] Columns: a b | pruned: 0/2 columns");

  _mock_node_a->set_pruned_column_ids({ColumnID{2}});
  EXPECT_EQ(_mock_node_a->description(), "[MockNode 'Unnamed'] Columns: a b d | pruned: 1/4 columns");
}

TEST_F(MockNodeTest, OutputColumnExpression) {
  ASSERT_EQ(_mock_node_a->column_expressions().size(), 4u);
  EXPECT_EQ(*_mock_node_a->column_expressions().at(0), *lqp_column_({_mock_node_a, ColumnID{0}}));
  EXPECT_EQ(*_mock_node_a->column_expressions().at(1), *lqp_column_({_mock_node_a, ColumnID{1}}));
  EXPECT_EQ(*_mock_node_a->column_expressions().at(2), *lqp_column_({_mock_node_a, ColumnID{2}}));
  EXPECT_EQ(*_mock_node_a->column_expressions().at(3), *lqp_column_({_mock_node_a, ColumnID{3}}));

  ASSERT_EQ(_mock_node_b->column_expressions().size(), 2u);
  EXPECT_EQ(*_mock_node_b->column_expressions().at(0), *lqp_column_({_mock_node_b, ColumnID{0}}));
  EXPECT_EQ(*_mock_node_b->column_expressions().at(1), *lqp_column_({_mock_node_b, ColumnID{1}}));

  _mock_node_a->set_pruned_column_ids({ColumnID{0}, ColumnID{3}});
  EXPECT_EQ(_mock_node_a->column_expressions().size(), 2u);
  EXPECT_EQ(*_mock_node_a->column_expressions().at(0), *lqp_column_({_mock_node_a, ColumnID{1}}));
  EXPECT_EQ(*_mock_node_a->column_expressions().at(1), *lqp_column_({_mock_node_a, ColumnID{2}}));
}

TEST_F(MockNodeTest, HashingAndEqualityCheck) {
  const auto same_mock_node_b =
      MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}}, "mock_name");
  const auto different_mock_node_1 =
      MockNode::make(MockNode::ColumnDefinitions{{DataType::Long, "a"}, {DataType::String, "b"}}, "mock_name");
  const auto different_mock_node_2 =
      MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}}, "other_name");
  EXPECT_EQ(*_mock_node_b, *_mock_node_b);
  EXPECT_NE(*_mock_node_b, *different_mock_node_1);
  EXPECT_NE(*_mock_node_b, *different_mock_node_2);
  EXPECT_EQ(*_mock_node_b, *same_mock_node_b);

  EXPECT_NE(_mock_node_b->hash(), different_mock_node_1->hash());
  EXPECT_EQ(_mock_node_b->hash(), different_mock_node_2->hash());
  EXPECT_EQ(_mock_node_b->hash(), same_mock_node_b->hash());
}

TEST_F(MockNodeTest, Copy) {
  EXPECT_EQ(*_mock_node_b, *_mock_node_b->deep_copy());

  _mock_node_b->set_pruned_column_ids({ColumnID{1}});
  EXPECT_EQ(*_mock_node_b, *_mock_node_b->deep_copy());
}

TEST_F(MockNodeTest, NodeExpressions) { ASSERT_EQ(_mock_node_a->node_expressions.size(), 0u); }

TEST_F(MockNodeTest, Constraints) {
  const auto a_b_pk_constraint =
      TableConstraintDefinition{std::vector<ColumnID>{ColumnID{0}, ColumnID{1}}, IsPrimaryKey::Yes};
  const auto c_constraint = TableConstraintDefinition{std::vector<ColumnID>{ColumnID{2}}, IsPrimaryKey::No};
  // Constraints can not be added afterwards, so we simply recreate _mock_node_a
  _mock_node_a = MockNode::make(
      MockNode::ColumnDefinitions{
          {DataType::Int, "a"}, {DataType::Float, "b"}, {DataType::Double, "c"}, {DataType::String, "d"}},
      std::optional<std::string>{}, TableConstraintDefinitions{a_b_pk_constraint, c_constraint});

  // Basic checks
  const auto mock_a_constraints = _mock_node_a->get_constraints();
  EXPECT_EQ(mock_a_constraints->size(), 2);
  const auto mock_b_constraints = _mock_node_b->get_constraints();
  EXPECT_TRUE(mock_b_constraints->empty());

  // In-depth verification
  const auto lqp_constraint0 = mock_a_constraints->at(0);
  EXPECT_TRUE(lqp_constraint0.column_expressions.size() == 2 && lqp_constraint0.is_primary_key == IsPrimaryKey::Yes);
  const auto lqp_constraint0_column_expr0 =
      dynamic_pointer_cast<LQPColumnExpression>(lqp_constraint0.column_expressions[0]);
  const auto lqp_constraint0_column_expr1 =
      dynamic_pointer_cast<LQPColumnExpression>(lqp_constraint0.column_expressions[1]);
  EXPECT_TRUE(lqp_constraint0_column_expr0 && lqp_constraint0_column_expr1);
  EXPECT_TRUE(lqp_constraint0_column_expr0->column_reference.original_column_id() == ColumnID{0} &&
              lqp_constraint0_column_expr0->column_reference.original_node() == _mock_node_a);
  EXPECT_TRUE(lqp_constraint0_column_expr1->column_reference.original_column_id() == ColumnID{1} &&
              lqp_constraint0_column_expr0->column_reference.original_node() == _mock_node_a);

  const auto lqp_constraint1 = mock_a_constraints->at(1);
  EXPECT_TRUE(lqp_constraint1.column_expressions.size() == 1 && lqp_constraint1.is_primary_key == IsPrimaryKey::No);
  const auto lqp_constraint1_column_expr0 =
      dynamic_pointer_cast<LQPColumnExpression>(lqp_constraint1.column_expressions[0]);
  EXPECT_TRUE(lqp_constraint1_column_expr0);
  EXPECT_TRUE(lqp_constraint1_column_expr0->column_reference.original_column_id() == ColumnID{2} &&
              lqp_constraint1_column_expr0->column_reference.original_node() == _mock_node_a);
}

//TEST_F(MockNodeTest, ConstraintsPrunedColumns) {}

}  // namespace opossum
