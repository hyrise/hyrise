#include "gtest/gtest.h"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/union_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class LQPUtilsTest : public ::testing::Test {
 public:
  void SetUp() override {
    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}});
    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "x"}, {DataType::Int, "y"}});

    a_a = node_a->get_column("a");
    a_b = node_a->get_column("b");
    b_x = node_b->get_column("x");
    b_y = node_b->get_column("y");
  }

  std::shared_ptr<MockNode> node_a, node_b;
  LQPColumnReference a_a, a_b, b_x, b_y;
};

TEST_F(LQPUtilsTest, LQPSubplanToBooleanExpression_A) {
  // clang-format off
  const auto lqp =
  PredicateNode::make(greater_than_(a_a, 5),
      ProjectionNode::make(expression_vector(add_(a_a, a_b), a_a),
        PredicateNode::make(less_than_(a_b, 4),
          SortNode::make(expression_vector(a_b), std::vector<OrderByMode>{OrderByMode::Ascending}, node_a))));
  // clang-format on

  const auto actual_expression = lqp_subplan_to_boolean_expression(lqp);
  const auto expected_expression = and_(greater_than_(a_a, 5), less_than_(a_b, 4));

  EXPECT_EQ(*actual_expression, *expected_expression);
}

TEST_F(LQPUtilsTest, LQPSubplanToBooleanExpression_B) {
  // clang-format off
  const auto lqp =
  PredicateNode::make(greater_than_(a_a, 4),
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(greater_than_(a_a, 5),
        PredicateNode::make(less_than_(a_a, 50),
          node_a)),
      UnionNode::make(UnionMode::Positions,
        PredicateNode::make(greater_than_(a_a, 450), node_a),
        PredicateNode::make(less_than_(a_a, 500), node_a))));
  // clang-format on

  const auto actual_expression = lqp_subplan_to_boolean_expression(lqp);

  // clang-format off
  const auto expected_expression = and_(greater_than_(a_a, 4),
                                        or_(and_(greater_than_(a_a, 5),
                                                 less_than_(a_a, 50)),
                                            or_(greater_than_(a_a, 450),
                                                less_than_(a_a, 500))));
  // clang-format on

  EXPECT_EQ(*actual_expression, *expected_expression);
}

TEST_F(LQPUtilsTest, LQPSubplanToBooleanExpression_C) {
  // clang-format off
  const auto lqp =
  PredicateNode::make(greater_than_(a_a, 5),
    PredicateNode::make(less_than_(b_x, 4),
       JoinNode::make(JoinMode::Inner, equals_(a_a, b_x),
         node_a,
         node_b)));
  // clang-format on

  const auto actual_expression = lqp_subplan_to_boolean_expression(lqp);
  const auto expected_expression = and_(greater_than_(a_a, 5), less_than_(b_x, 4));

  EXPECT_EQ(*actual_expression, *expected_expression);
}

TEST_F(LQPUtilsTest, VisitLQP) {
  const auto expected_nodes = std::vector<std::shared_ptr<AbstractLQPNode>>{
      PredicateNode::make(greater_than_(a_a, 4)), UnionNode::make(UnionMode::Positions),
      PredicateNode::make(less_than_(a_a, 4)), PredicateNode::make(equals_(a_a, 4)), node_a};

  expected_nodes[0]->set_left_input(expected_nodes[1]);
  expected_nodes[1]->set_left_input(expected_nodes[2]);
  expected_nodes[1]->set_right_input(expected_nodes[3]);
  expected_nodes[2]->set_left_input(node_a);
  expected_nodes[3]->set_left_input(node_a);

  auto actual_nodes = std::vector<std::shared_ptr<AbstractLQPNode>>{};
  visit_lqp(expected_nodes[0], [&](const auto& node) {
    actual_nodes.emplace_back(node);
    return LQPVisitation::VisitInputs;
  });

  EXPECT_EQ(actual_nodes, expected_nodes);
}

}  // namespace opossum
