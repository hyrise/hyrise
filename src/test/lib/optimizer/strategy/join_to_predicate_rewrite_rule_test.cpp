#include "strategy_base_test.hpp"

#include "magic_enum.hpp"

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

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

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

class JoinToPredicateRewriteRuleJoinModeTest : public JoinToPredicateRewriteRuleTest,
                                               public ::testing::WithParamInterface<JoinMode> {};

INSTANTIATE_TEST_SUITE_P(JoinToPredicateRewriteRuleJoinModeTestInstance, JoinToPredicateRewriteRuleJoinModeTest,
                         ::testing::ValuesIn(magic_enum::enum_values<JoinMode>()), enum_formatter<JoinMode>);

TEST_P(JoinToPredicateRewriteRuleJoinModeTest, PerformRewrite) {
  // The rule should only rewrite inner and semi joins.
  auto key_constraints = TableKeyConstraints{};
  key_constraints.emplace(std::set<ColumnID>{u->original_column_id}, KeyConstraintType::UNIQUE);
  key_constraints.emplace(std::set<ColumnID>{v->original_column_id}, KeyConstraintType::UNIQUE);
  node_b->set_key_constraints(key_constraints);

  const auto join_node =
      GetParam() == JoinMode::Cross ? JoinNode::make(GetParam()) : JoinNode::make(GetParam(), equals_(a, u));
  join_node->set_left_input(node_a);
  join_node->set_right_input(PredicateNode::make(equals_(v, 0), node_b));
  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(b),
    join_node);

  const auto subquery = ProjectionNode::make(expression_vector(u),
    PredicateNode::make(equals_(v, 0), node_b));

  auto expected_lqp =
  ProjectionNode::make(expression_vector(b),
    PredicateNode::make(equals_(a, lqp_subquery_(subquery)),
      node_a));
  // clang-format on

  const auto annotated_lqp = apply_rule(std::make_shared<ColumnPruningRule>(), lqp->deep_copy());
  const auto actual_lqp = apply_rule(rule, annotated_lqp);
  expected_lqp = std::static_pointer_cast<ProjectionNode>(
      apply_rule(std::make_shared<ColumnPruningRule>(), expected_lqp->deep_copy()));

  if (GetParam() == JoinMode::Inner || GetParam() == JoinMode::Semi) {
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  } else {
    EXPECT_LQP_EQ(actual_lqp, annotated_lqp->deep_copy());
  }
}

TEST_F(JoinToPredicateRewriteRuleTest, MissingPredicate) {
  // Do not rewrite if there is no predicate on the column with UCC.
  auto key_constraints = TableKeyConstraints{};
  key_constraints.emplace(std::set<ColumnID>{u->original_column_id}, KeyConstraintType::UNIQUE);
  key_constraints.emplace(std::set<ColumnID>{v->original_column_id}, KeyConstraintType::UNIQUE);
  node_b->set_key_constraints(key_constraints);

  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(b),
    JoinNode::make(JoinMode::Inner, equals_(a, u),
      node_a, node_b));
  // clang-format on

  const auto annotated_lqp = apply_rule(std::make_shared<ColumnPruningRule>(), lqp);
  const auto actual_lqp = apply_rule(rule, annotated_lqp);
  EXPECT_LQP_EQ(actual_lqp, lqp->deep_copy());
}

TEST_F(JoinToPredicateRewriteRuleTest, MissingUccOnPredicateColumn) {
  // Do not rewrite if there is no UCC on the predicate column.
  auto key_constraints = TableKeyConstraints{};
  key_constraints.emplace(std::set<ColumnID>{u->original_column_id}, KeyConstraintType::UNIQUE);
  node_b->set_key_constraints(key_constraints);

  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(b),
    JoinNode::make(JoinMode::Semi, equals_(a, u),
      node_a,
      PredicateNode::make(equals_(v, 0), node_b)));
  // clang-format on

  const auto annotated_lqp = apply_rule(std::make_shared<ColumnPruningRule>(), lqp);
  const auto actual_lqp = apply_rule(rule, annotated_lqp);
  EXPECT_LQP_EQ(actual_lqp, lqp->deep_copy());
}

TEST_F(JoinToPredicateRewriteRuleTest, MissingUccOnJoinColumn) {
  // Do not rewrite if there is no UCC on the join column.
  auto key_constraints = TableKeyConstraints{};
  key_constraints.emplace(std::set<ColumnID>{v->original_column_id}, KeyConstraintType::UNIQUE);
  node_b->set_key_constraints(key_constraints);

  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(b),
    JoinNode::make(JoinMode::Inner, equals_(a, u),
      node_a,
      PredicateNode::make(equals_(v, 0), node_b)));
  // clang-format on

  const auto annotated_lqp = apply_rule(std::make_shared<ColumnPruningRule>(), lqp);
  const auto actual_lqp = apply_rule(rule, annotated_lqp);
  EXPECT_LQP_EQ(actual_lqp, lqp->deep_copy());
}

TEST_F(JoinToPredicateRewriteRuleTest, NoUnusedJoinSide) {
  // Do not rewrite if columns from b are required upwards in LQP.
  auto key_constraints = TableKeyConstraints{};
  key_constraints.emplace(std::set<ColumnID>{u->original_column_id}, KeyConstraintType::UNIQUE);
  key_constraints.emplace(std::set<ColumnID>{v->original_column_id}, KeyConstraintType::UNIQUE);
  node_b->set_key_constraints(key_constraints);

  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(b, u),
    JoinNode::make(JoinMode::Inner, equals_(a, u),
      node_a,
      PredicateNode::make(equals_(v, 0), node_b)));
  // clang-format on

  const auto annotated_lqp = apply_rule(std::make_shared<ColumnPruningRule>(), lqp);
  const auto actual_lqp = apply_rule(rule, annotated_lqp);
  EXPECT_LQP_EQ(actual_lqp, lqp->deep_copy());
}

TEST_F(JoinToPredicateRewriteRuleTest, Union) {
  // Do not rewrite if there is a union on table b.
  auto key_constraints = TableKeyConstraints{};
  key_constraints.emplace(TableKeyConstraint({u->original_column_id}, KeyConstraintType::UNIQUE));
  key_constraints.emplace(TableKeyConstraint({v->original_column_id}, KeyConstraintType::UNIQUE));
  node_b->set_key_constraints(key_constraints);

  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(b),
    JoinNode::make(JoinMode::Inner, equals_(a, u),
      node_a,
      PredicateNode::make(equals_(v, 0),
      UnionNode::make(SetOperationMode::Positions, node_b, node_b))));
  // clang-format on

  const auto annotated_lqp = apply_rule(std::make_shared<ColumnPruningRule>(), lqp);
  const auto actual_lqp = apply_rule(rule, annotated_lqp);
  EXPECT_LQP_EQ(actual_lqp, lqp->deep_copy());
}

}  // namespace hyrise
