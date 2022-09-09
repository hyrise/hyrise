#include "strategy_base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/change_meta_table_node.hpp"
#include "logical_query_plan/delete_node.hpp"
#include "logical_query_plan/export_node.hpp"
#include "logical_query_plan/insert_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/update_node.hpp"
#include "optimizer/strategy/column_pruning_rule.hpp"
#include "optimizer/strategy/join_to_predicate_rewrite_rule.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class JoinToPredicateRewriteRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    node_a = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "a");
    node_b = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "u"}, {DataType::Int, "v"}, {DataType::Int, "w"}}, "b");

    a = node_a->get_column("a");
    b = node_a->get_column("b");
    c = node_a->get_column("c");
    u = node_b->get_column("u");
    v = node_b->get_column("v");
    w = node_b->get_column("w");

    rule = std::make_shared<JoinToPredicateRewriteRule>();
  }

  std::shared_ptr<JoinToPredicateRewriteRule> rule;
  std::shared_ptr<MockNode> node_a, node_b;
  std::shared_ptr<LQPColumnExpression> a, b, c, u, v, w;
};

TEST_F(JoinToPredicateRewriteRuleTest, SimplePredicateSemiJoin) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  auto key_constraints = std::vector<TableKeyConstraint>();

  key_constraints.push_back(TableKeyConstraint({u->original_column_id}, KeyConstraintType::UNIQUE));
  key_constraints.push_back(TableKeyConstraint({v->original_column_id}, KeyConstraintType::UNIQUE));

  node_b->set_key_constraints(key_constraints);

  // clang-format off
  lqp =
  ProjectionNode::make(expression_vector(b),
    JoinNode::make(JoinMode::Semi, equals_(a, u),
      node_a,
      PredicateNode::make(equals_(v, 0), node_b)));

  const auto annotated_lqp = apply_rule(std::make_shared<ColumnPruningRule>(), lqp);
  const auto actual_lqp = apply_rule(rule, annotated_lqp);

  const auto subquery = ProjectionNode::make(expression_vector(u),
    PredicateNode::make(equals_(v, 0), node_b));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(b),
    PredicateNode::make(equals_(a, lqp_subquery_(subquery)),
      node_a));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(JoinToPredicateRewriteRuleTest, InnerJoinRewrite) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  auto key_constraints = std::vector<TableKeyConstraint>();

  key_constraints.push_back(TableKeyConstraint({u->original_column_id}, KeyConstraintType::UNIQUE));
  key_constraints.push_back(TableKeyConstraint({v->original_column_id}, KeyConstraintType::UNIQUE));

  node_b->set_key_constraints(key_constraints);

  // clang-format off
  lqp =
  ProjectionNode::make(expression_vector(b),
    JoinNode::make(JoinMode::Inner, equals_(a, u),
      node_a,
      PredicateNode::make(equals_(v, 0), node_b)));

  const auto annotated_lqp = apply_rule(std::make_shared<ColumnPruningRule>(), lqp);
  const auto actual_lqp = apply_rule(rule, annotated_lqp);

  const auto subquery = ProjectionNode::make(expression_vector(u),
    PredicateNode::make(equals_(v, 0), node_b));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(b),
    PredicateNode::make(equals_(a, lqp_subquery_(subquery)),
      node_a));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(JoinToPredicateRewriteRuleTest, MissingPredicate) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  auto key_constraints = std::vector<TableKeyConstraint>();

  key_constraints.push_back(TableKeyConstraint({u->original_column_id}, KeyConstraintType::UNIQUE));
  key_constraints.push_back(TableKeyConstraint({v->original_column_id}, KeyConstraintType::UNIQUE));

  node_b->set_key_constraints(key_constraints);

  // clang-format off
  lqp =
  ProjectionNode::make(expression_vector(b),
    JoinNode::make(JoinMode::Semi, equals_(a, u),
      node_a, node_b));

  const auto annotated_lqp = apply_rule(std::make_shared<ColumnPruningRule>(), lqp);
  const auto actual_lqp = apply_rule(rule, annotated_lqp);

  const auto expected_lqp = ProjectionNode::make(expression_vector(b),
    JoinNode::make(JoinMode::Semi, equals_(a, u),
      node_a, node_b));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(JoinToPredicateRewriteRuleTest, MissingUCCOnPredicateColumn) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  // clang-format off
  lqp =
  ProjectionNode::make(expression_vector(b),
    JoinNode::make(JoinMode::Semi, equals_(a, u),
      node_a,
      PredicateNode::make(equals_(v, 0), node_b)));

  const auto annotated_lqp = apply_rule(std::make_shared<ColumnPruningRule>(), lqp);
  const auto actual_lqp = apply_rule(rule, annotated_lqp);

  const auto subquery = ProjectionNode::make(expression_vector(u),
    PredicateNode::make(equals_(v, 0), node_b));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(b),
    JoinNode::make(JoinMode::Semi, equals_(a, u),
      node_a,
      PredicateNode::make(equals_(v, 0), node_b)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(JoinToPredicateRewriteRuleTest, TestNoUnusedJoinSide) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  auto key_constraints = std::vector<TableKeyConstraint>();

  key_constraints.push_back(TableKeyConstraint({u->original_column_id}, KeyConstraintType::UNIQUE));
  key_constraints.push_back(TableKeyConstraint({v->original_column_id}, KeyConstraintType::UNIQUE));

  node_b->set_key_constraints(key_constraints);

  // clang-format off
  lqp =
  ProjectionNode::make(expression_vector(b, u),
    JoinNode::make(JoinMode::Inner, equals_(a, u),
      node_a,
      PredicateNode::make(equals_(v, 0), node_b)));

  const auto annotated_lqp = apply_rule(std::make_shared<ColumnPruningRule>(), lqp);
  const auto actual_lqp = apply_rule(rule, annotated_lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(b, u),
    JoinNode::make(JoinMode::Inner, equals_(a, u),
      node_a,
      PredicateNode::make(equals_(v, 0), node_b)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
