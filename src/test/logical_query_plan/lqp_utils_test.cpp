#include "gtest/gtest.h"

#include "base_test.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/delete_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/insert_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/union_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class LQPUtilsTest : public BaseTest {
 public:
  void SetUp() override {
    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}}, "node_a");
    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "x"}, {DataType::Int, "y"}}, "node_b");

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
  const auto expected_expression = and_(less_than_(a_b, 4), greater_than_(a_a, 5));

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
  const auto expected_expression = and_(or_(and_(less_than_(a_a, 50),
                                                 greater_than_(a_a, 5)),
                                            or_(greater_than_(a_a, 450),
                                                less_than_(a_a, 500))),
                                        greater_than_(a_a, 4));
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
  const auto expected_expression = and_(less_than_(b_x, 4), greater_than_(a_a, 5));

  EXPECT_EQ(*actual_expression, *expected_expression);
}

TEST_F(LQPUtilsTest, LQPSubplanToBooleanExpressionBeginEndNode) {
  const auto end_node = PredicateNode::make(less_than_(a_b, 4), node_a);
  const auto begin_node = PredicateNode::make(greater_than_(a_a, 5), end_node);

  const auto actual_expression = lqp_subplan_to_boolean_expression(begin_node, end_node);
  const auto expected_expression = greater_than_(a_a, 5);

  EXPECT_EQ(*actual_expression, *expected_expression);
}

TEST_F(LQPUtilsTest, VisitLQP) {
  // clang-format off
  const auto expected_nodes = std::vector<std::shared_ptr<AbstractLQPNode>>{
    PredicateNode::make(greater_than_(a_a, 4)), UnionNode::make(UnionMode::Positions),
    PredicateNode::make(less_than_(a_a, 4)), PredicateNode::make(equals_(a_a, 4)), node_a};
  // clang-format on

  expected_nodes[0]->set_left_input(expected_nodes[1]);
  expected_nodes[1]->set_left_input(expected_nodes[2]);
  expected_nodes[1]->set_right_input(expected_nodes[3]);
  expected_nodes[2]->set_left_input(node_a);
  expected_nodes[3]->set_left_input(node_a);

  {
    // Visit AbstractLQPNode
    auto actual_nodes = std::vector<std::shared_ptr<AbstractLQPNode>>{};
    visit_lqp(expected_nodes[0], [&](const auto& node) {
      actual_nodes.emplace_back(node);
      return LQPVisitation::VisitInputs;
    });

    EXPECT_EQ(actual_nodes, expected_nodes);
  }

  {
    // Visit PredicateNode
    auto actual_nodes = std::vector<std::shared_ptr<AbstractLQPNode>>{};
    visit_lqp(std::static_pointer_cast<PredicateNode>(expected_nodes[0]), [&](const auto& node) {
      actual_nodes.emplace_back(node);
      return LQPVisitation::VisitInputs;
    });

    EXPECT_EQ(actual_nodes, expected_nodes);
  }
}

TEST_F(LQPUtilsTest, VisitLQPUpwards) {
  // clang-format off
  const auto expected_nodes = std::vector<std::shared_ptr<AbstractLQPNode>>{node_a,
    PredicateNode::make(greater_than_(a_a, 4)), PredicateNode::make(less_than_(a_a, 4)),
    UnionNode::make(UnionMode::Positions), PredicateNode::make(equals_(a_a, 4))};
  // clang-format on

  expected_nodes[4]->set_left_input(expected_nodes[3]);
  expected_nodes[3]->set_left_input(expected_nodes[1]);
  expected_nodes[3]->set_right_input(expected_nodes[2]);
  expected_nodes[1]->set_left_input(node_a);
  expected_nodes[2]->set_left_input(node_a);

  {
    auto actual_nodes = std::vector<std::shared_ptr<AbstractLQPNode>>{};
    visit_lqp_upwards(node_a, [&](const auto& node) {
      actual_nodes.emplace_back(node);
      return LQPVisitation::VisitInputs;
    });

    EXPECT_EQ(actual_nodes, expected_nodes);
  }
}

TEST_F(LQPUtilsTest, LQPFindSubplanRoots) {
  // clang-format off
  const auto subquery_a_lqp = AggregateNode::make(expression_vector(b_x), expression_vector(), node_b);
  const auto subquery_a = lqp_subquery_(subquery_a_lqp);
  const auto subquery_b_lqp = ProjectionNode::make(expression_vector(subquery_a), DummyTableNode::make());
  const auto subquery_b = lqp_subquery_(subquery_b_lqp);

  const auto lqp =
  PredicateNode::make(greater_than_(a_a, 5),
    PredicateNode::make(less_than_(subquery_b, 4),
      node_a));
  // clang-format on

  const auto roots = lqp_find_subplan_roots(lqp);

  ASSERT_EQ(roots.size(), 3u);
  EXPECT_EQ(roots[0], lqp);
  EXPECT_EQ(roots[1], subquery_b_lqp);
  EXPECT_EQ(roots[2], subquery_a_lqp);
}

TEST_F(LQPUtilsTest, LQPFindModifiedTables) {
  // clang-format off
  const auto read_only_lqp =
  PredicateNode::make(greater_than_(a_a, 5),
    ProjectionNode::make(expression_vector(add_(a_a, a_b), a_a),
      PredicateNode::make(less_than_(a_b, 4),
        SortNode::make(expression_vector(a_b), std::vector<OrderByMode>{OrderByMode::Ascending},
          node_a))));
  // clang-format on

  EXPECT_EQ(lqp_find_modified_tables(read_only_lqp).size(), 0);

  // clang-format off
  const auto insert_lqp =
  InsertNode::make("insert_table_name",
    PredicateNode::make(greater_than_(a_a, 5),
      node_a));
  // clang-format on
  const auto insert_tables = lqp_find_modified_tables(insert_lqp);

  EXPECT_EQ(insert_tables.size(), 1);
  EXPECT_NE(insert_tables.find("insert_table_name"), insert_tables.end());

  const auto delete_lqp = DeleteNode::make(node_a);
  const auto delete_tables = lqp_find_modified_tables(delete_lqp);

  EXPECT_EQ(delete_tables.size(), 1);
  EXPECT_NE(delete_tables.find("node_a"), delete_tables.end());
}

}  // namespace opossum
