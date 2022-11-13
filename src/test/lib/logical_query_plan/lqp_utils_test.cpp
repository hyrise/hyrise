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

using namespace hyrise::expression_functional;  // NOLINT

namespace hyrise {

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
  std::shared_ptr<LQPColumnExpression> a_a, a_b, b_x, b_y;
};

TEST_F(LQPUtilsTest, LQPSubplanToBooleanExpression_A) {
  // clang-format off
  const auto lqp =
  PredicateNode::make(greater_than_(a_a, 5),
    ProjectionNode::make(expression_vector(add_(a_a, a_b), a_a),
      PredicateNode::make(less_than_(a_b, 4),
        SortNode::make(expression_vector(a_b), std::vector<SortMode>{SortMode::Ascending}, node_a))));
  // clang-format on

  const auto actual_expression = lqp_subplan_to_boolean_expression(lqp);
  const auto expected_expression = and_(less_than_(a_b, 4), greater_than_(a_a, 5));

  EXPECT_EQ(*actual_expression, *expected_expression);
}

TEST_F(LQPUtilsTest, LQPSubplanToBooleanExpression_B) {
  // clang-format off
  const auto lqp =
  PredicateNode::make(greater_than_(a_a, 4),
    UnionNode::make(SetOperationMode::Positions,
      PredicateNode::make(greater_than_(a_a, 5),
        PredicateNode::make(less_than_(a_a, 50),
          node_a)),
      UnionNode::make(SetOperationMode::Positions,
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
    PredicateNode::make(greater_than_(a_a, 4)), UnionNode::make(SetOperationMode::Positions),
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
    UnionNode::make(SetOperationMode::Positions), PredicateNode::make(equals_(a_a, 4))};
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
      return LQPUpwardVisitation::VisitOutputs;
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

TEST_F(LQPUtilsTest, LQPFindNodesByType) {
  auto dummy_table_node = DummyTableNode::make();
  auto literal = add_(value_(1), value_(2));
  // clang-format off
  auto lqp =
  JoinNode::make(JoinMode::Semi, equals_(b_y, literal),
    JoinNode::make(JoinMode::Inner, equals_(a_a, b_x),
      UnionNode::make(SetOperationMode::All,
        PredicateNode::make(greater_than_(a_a, 700),
          node_a),
        PredicateNode::make(less_than_(a_b, 123),
          node_a)),
      node_b),
    ProjectionNode::make(expression_vector(literal),
      dummy_table_node));

  // We do not expect duplicate nodes in the output
  const auto mock_nodes = lqp_find_nodes_by_type(lqp, LQPNodeType::Mock);
  ASSERT_EQ(mock_nodes.size(), 2);
  EXPECT_EQ(mock_nodes.at(0), node_b);
  EXPECT_EQ(mock_nodes.at(1), node_a);

  const auto dummy_table_nodes = lqp_find_nodes_by_type(lqp, LQPNodeType::DummyTable);
  ASSERT_EQ(dummy_table_nodes.size(), 1);
  EXPECT_EQ(dummy_table_nodes.at(0), dummy_table_node);

  const auto predicate_nodes = lqp_find_nodes_by_type(lqp, LQPNodeType::Predicate);
  ASSERT_EQ(predicate_nodes.size(), 2);

  const auto stored_table_nodes = lqp_find_nodes_by_type(lqp, LQPNodeType::StoredTable);
  EXPECT_TRUE(stored_table_nodes.empty());
}

TEST_F(LQPUtilsTest, LQPFindLeaves) {
  // Based on LQPFindNodesByType test
  auto dummy_table_node = DummyTableNode::make();
  auto literal = add_(value_(1), value_(2));
  // clang-format off
  auto lqp =
  JoinNode::make(JoinMode::Semi, equals_(b_y, literal),
    JoinNode::make(JoinMode::Inner, equals_(a_a, b_x),
      UnionNode::make(SetOperationMode::All,
        PredicateNode::make(greater_than_(a_a, 700),
          node_a),
        PredicateNode::make(less_than_(a_b, 123),
          node_a)),
      node_b),
    ProjectionNode::make(expression_vector(literal),
      dummy_table_node));
  // clang-format on

  const auto leaf_nodes = lqp_find_leaves(lqp);
  ASSERT_EQ(leaf_nodes.size(), 3);
  EXPECT_EQ(leaf_nodes.at(0), node_b);
  EXPECT_EQ(leaf_nodes.at(1), dummy_table_node);
  EXPECT_EQ(leaf_nodes.at(2), node_a);
}

TEST_F(LQPUtilsTest, LQPFindModifiedTables) {
  // clang-format off
  const auto read_only_lqp =
  PredicateNode::make(greater_than_(a_a, 5),
    ProjectionNode::make(expression_vector(add_(a_a, a_b), a_a),
      PredicateNode::make(less_than_(a_b, 4),
        SortNode::make(expression_vector(a_b), std::vector<SortMode>{SortMode::Ascending},
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

TEST_F(LQPUtilsTest, LQPInsertAboveNode) {
  // clang-format off
  const auto lqp =
  UnionNode::make(SetOperationMode::Positions,
    PredicateNode::make(less_than_(a_a, value_(3)),
      node_a),
    PredicateNode::make(greater_than_(a_a, value_(5)),
      node_a));

  const auto node_to_insert = ProjectionNode::make(expression_vector(a_a, a_b, add_(a_a, a_b)));
  lqp_insert_node_above(node_a, node_to_insert);

  const auto expected_common_node =
  ProjectionNode::make(expression_vector(a_a, a_b, add_(a_a, a_b)),
    node_a);

  const auto expected_lqp =
  UnionNode::make(SetOperationMode::Positions,
    PredicateNode::make(less_than_(a_a, value_(3)),
      expected_common_node),
    PredicateNode::make(greater_than_(a_a, value_(5)),
      expected_common_node));
  // clang-format on

  EXPECT_LQP_EQ(lqp, expected_lqp);
}

TEST_F(LQPUtilsTest, CollectSubqueryExpressionsByLQPNestedSubqueries) {
  // Prepare an LQP with multiple subqueries in a nested manner

  // clang-format off
  const auto nested_subquery_lqp =
  AggregateNode::make(expression_vector(), expression_vector(max_(a_a)),
    node_a);
  const auto max_a_subquery = lqp_subquery_(nested_subquery_lqp);

  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_x),
    PredicateNode::make(greater_than_(b_x, max_a_subquery),
      node_b));
  const auto x_greater_than_max_a_subquery = lqp_subquery_(subquery_lqp);

  const auto root_lqp =
  ProjectionNode::make(expression_vector(add_(a_a, a_b)),
    PredicateNode::make(in_(a_b, x_greater_than_max_a_subquery),
      node_a));
  // clang-format on

  auto subquery_expressions_by_lqp = collect_lqp_subquery_expressions_by_lqp(root_lqp);
  EXPECT_EQ(subquery_expressions_by_lqp.size(), 2);

  ASSERT_TRUE(subquery_expressions_by_lqp.contains(x_greater_than_max_a_subquery->lqp));
  EXPECT_EQ(subquery_expressions_by_lqp.find(x_greater_than_max_a_subquery->lqp)->second.size(), 1);
  EXPECT_EQ(subquery_expressions_by_lqp.find(x_greater_than_max_a_subquery->lqp)->second.at(0).lock(),
            x_greater_than_max_a_subquery);

  ASSERT_TRUE(subquery_expressions_by_lqp.contains(max_a_subquery->lqp));
  EXPECT_EQ(subquery_expressions_by_lqp.find(max_a_subquery->lqp)->second.size(), 1);
  EXPECT_EQ(subquery_expressions_by_lqp.find(max_a_subquery->lqp)->second.size(), 1);
  EXPECT_EQ(subquery_expressions_by_lqp.find(max_a_subquery->lqp)->second.at(0).lock(), max_a_subquery);
}

TEST_F(LQPUtilsTest, FindDiamondOriginNode) {
  // Test if the origin node of a simple diamond is returned.
  {
    // clang-format off
    const auto lqp =
    UnionNode::make(SetOperationMode::Positions,
      PredicateNode::make(less_than_(a_a, value_(3)),
        node_a),
      PredicateNode::make(greater_than_(a_a, value_(5)),
        node_a));
    // clang-format on
    const auto diamond_origin_node = find_diamond_origin_node(lqp);
    EXPECT_EQ(diamond_origin_node, node_a);
  }

  // Test for null pointer, because the diamond from above no longer has a common origin node.
  {
    // clang-format off
    const auto lqp =
    UnionNode::make(SetOperationMode::Positions,
      PredicateNode::make(less_than_(a_a, value_(3)),
        node_a),
      PredicateNode::make(greater_than_(a_a, value_(5)),
        node_b));
    // clang-format on
    const auto diamond_origin_node = find_diamond_origin_node(lqp);
    EXPECT_EQ(diamond_origin_node, nullptr);
  }
}

TEST_F(LQPUtilsTest, FindDiamondOriginNodeNestedUnions) {
  // Test if the diamond's origin node is returned in case of multiple UnionNodes.
  {
    // clang-format off
    const auto lqp =
    UnionNode::make(SetOperationMode::All,
      UnionNode::make(SetOperationMode::All,
        PredicateNode::make(equals_(a_a, value_(3)),
          node_a),
        PredicateNode::make(equals_(a_a, value_(5)),
          node_a)),
      PredicateNode::make(equals_(a_a, value_(7)),
        node_a));
    // clang-format on
    const auto diamond_origin_node = find_diamond_origin_node(lqp);
    EXPECT_EQ(diamond_origin_node, node_a);
  }

  // Test for null pointer, because the diamond from above no longer has a common origin node.
  {
    // clang-format off
    const auto lqp =
    UnionNode::make(SetOperationMode::All,
      UnionNode::make(SetOperationMode::All,
        PredicateNode::make(equals_(a_a, value_(3)),
          node_a),
        PredicateNode::make(equals_(a_a, value_(5)),
          node_b)),
      PredicateNode::make(equals_(a_a, value_(7)),
        node_a));
    // clang-format on
    const auto diamond_origin_node = find_diamond_origin_node(lqp);
    EXPECT_EQ(diamond_origin_node, nullptr);
  }
}

TEST_F(LQPUtilsTest, FindDiamondOriginNodeConsecutiveDiamonds) {
  // Edge case: In this test, the diamond's root node has the same number of outputs as the diamond's origin node.
  // clang-format off
  const auto bottom_diamond_root_node =
  UnionNode::make(SetOperationMode::Positions,
    PredicateNode::make(less_than_(a_a, value_(3)),
      node_a),
    PredicateNode::make(greater_than_(a_a, value_(5)),
      node_a));

  const auto top_diamond_root_node =
  UnionNode::make(SetOperationMode::Positions,
    PredicateNode::make(less_than_(a_a, value_(3)),
      bottom_diamond_root_node),
    PredicateNode::make(greater_than_(a_a, value_(5)),
      bottom_diamond_root_node));
  // clang-format on
  ASSERT_EQ(bottom_diamond_root_node->output_count(), node_a->output_count());

  const auto bottom_diamond_origin_node = find_diamond_origin_node(bottom_diamond_root_node);
  EXPECT_EQ(bottom_diamond_origin_node, node_a);

  const auto top_diamond_origin_node = find_diamond_origin_node(top_diamond_root_node);
  EXPECT_EQ(top_diamond_origin_node, bottom_diamond_root_node);
}

}  // namespace hyrise
