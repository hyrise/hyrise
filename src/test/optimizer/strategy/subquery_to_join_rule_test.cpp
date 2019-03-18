#include "gtest/gtest.h"

#include "strategy_base_test.hpp"
#include "testing_assert.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/strategy/subquery_to_join_rule.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class SubqueryToJoinRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}}, "a");
    a_a = node_a->get_column("a");
    a_b = node_a->get_column("b");

    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}}, "b");
    b_a = node_b->get_column("a");
    b_b = node_b->get_column("b");

    node_c = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "c");
    c_a = node_c->get_column("a");
    c_b = node_c->get_column("b");

    node_d = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "d");
    d_a = node_d->get_column("a");
    d_b = node_d->get_column("b");
    d_c = node_d->get_column("c");

    node_e = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "e");
    e_a = node_e->get_column("a");
    e_b = node_e->get_column("b");
    e_c = node_e->get_column("c");

    _rule = std::make_shared<SubqueryToJoinRule>();
  }

  std::shared_ptr<AbstractLQPNode> apply_in_rule(const std::shared_ptr<AbstractLQPNode>& lqp) {
    auto copied_lqp = lqp->deep_copy();
    StrategyBaseTest::apply_rule(_rule, copied_lqp);

    return copied_lqp;
  }

  std::shared_ptr<SubqueryToJoinRule> _rule;

  std::shared_ptr<MockNode> node_a, node_b, node_c, node_d, node_e;
  LQPColumnReference a_a, a_b, b_a, b_b, c_a, c_b, d_a, d_b, d_c, e_a, e_b, e_c;
};

// HELPER FUNCTIONS

// REWRITE CASES

TEST_F(SubqueryToJoinRuleTest, UncorrelatedInToSemiJoin) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a FROM b)
  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a), node_b);

  const auto subquery = lqp_subquery_(subquery_lqp);

  const auto input_lqp =
  PredicateNode::make(in_(a_a, subquery), node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, equals_(a_a, b_a), node_a,
    ProjectionNode::make(expression_vector(b_a), node_b));
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, UncorrelatedInWithJoinInSubqueryToSemiJoin) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a FROM b JOIN c ON b.b = c.b)
  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    JoinNode::make(JoinMode::Inner, equals_(a_b, c_b), node_b, node_c));

  const auto subquery = lqp_subquery_(subquery_lqp);

  const auto input_lqp =
  PredicateNode::make(in_(a_a, subquery), node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, equals_(a_a, b_a), node_a,
    ProjectionNode::make(expression_vector(b_a),
      JoinNode::make(JoinMode::Inner, equals_(a_b, c_b), node_b, node_c)));

  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, SimpleCorrelatedInToSemiJoin) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a FROM b WHERE b.b = a.b)
  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);
  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    PredicateNode::make(equals_(b_b, parameter), node_b));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(in_(a_a, subquery), node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, expression_vector(equals_(a_b, b_b), equals_(a_a, b_a)), node_a,
    ProjectionNode::make(expression_vector(b_a, b_b), node_b));
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, SimpleCorrelatedInWithAdditionToSemiJoin) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a + 2 FROM b WHERE b.b = a.b)
  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);
  // clang-format off
  const auto b_a_plus_2 = add_(b_a, value_(2));
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a_plus_2),
    PredicateNode::make(equals_(b_b, parameter), node_b));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(in_(a_a, subquery), node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, expression_vector(equals_(a_b, b_b), equals_(a_a, b_a_plus_2)), node_a,
    ProjectionNode::make(expression_vector(b_a_plus_2, b_b), node_b));
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, UncorrelatedNestedInToSemiJoins) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a FROM b WHERE b.a IN (SELECT c.a FROM c))
  // clang-format off
  const auto inner_subquery_lqp =
  ProjectionNode::make(expression_vector(c_a), node_c);

  const auto inner_subquery = lqp_subquery_(inner_subquery_lqp);

  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    PredicateNode::make(in_(b_a, inner_subquery), node_b));

  const auto subquery = lqp_subquery_(subquery_lqp);

  const auto input_lqp =
  PredicateNode::make(in_(a_a, subquery), node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, equals_(a_a, b_a), node_a,
    ProjectionNode::make(expression_vector(b_a),
      JoinNode::make(JoinMode::Semi, equals_(b_a, c_a), node_b,
        ProjectionNode::make(expression_vector(c_a), node_c))));
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, UncorrelatedNotInToAntiJoin) {
  // SELECT * FROM a WHERE a.a NOT IN (SELECT b.a FROM b)
  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a), node_b);

  const auto subquery = lqp_subquery_(subquery_lqp);

  const auto input_lqp =
  PredicateNode::make(not_in_(a_a, subquery), node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::AntiDiscardNulls, equals_(a_a, b_a), node_a,
    ProjectionNode::make(expression_vector(b_a), node_b));
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, DoubleCorrelatedInToSemiJoin) {
  // SELECT * FROM d WHERE d.a IN (SELECT e.a FROM e WHERE e.b = d.b AND e.c < d.c)
  const auto parameter0 = correlated_parameter_(ParameterID{0}, d_b);
  const auto parameter1 = correlated_parameter_(ParameterID{1}, d_c);
  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(e_a),
    PredicateNode::make(equals_(e_b, parameter0),
      PredicateNode::make(less_than_(e_c, parameter1), node_e)));

  const auto subquery =
  lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, d_b), std::make_pair(ParameterID{1}, d_c));

  const auto input_lqp =
  PredicateNode::make(in_(d_a, subquery), node_d);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, expression_vector(equals_(d_b, e_b), greater_than_(d_c, e_c), equals_(d_a, e_a)), node_d,
    ProjectionNode::make(expression_vector(e_a, e_c, e_b), node_e));
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, SimpleCorrelatedAggregateToSemiJoin) {
  // SELECT * FROM a WHERE a.a IN (SELECT SUM(b.a) FROM b WHERE b.b = a.b)
  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);
  // clang-format off
  const auto subquery_lqp =
  AggregateNode::make(expression_vector(), expression_vector(sum_(b_a)),
    PredicateNode::make(equals_(b_b, parameter), node_b));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(in_(a_a, subquery), node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, expression_vector(equals_(a_b, b_b), equals_(a_a, sum_(b_a))), node_a,
    AggregateNode::make(expression_vector(b_b), expression_vector(sum_(b_a)), node_b));
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// NO REWRITE CASES

// We expect to run after the PredicateSplitUpRule. Therefore, we do not handle multiple predicates joined by AND or OR.
TEST_F(SubqueryToJoinRuleTest, NoRewriteOfAnd) {
  // SELECT * FROM d WHERE d.a IN (SELECT e.a FROM e WHERE e.b = d.b AND e.c < d.c)
  const auto parameter0 = correlated_parameter_(ParameterID{0}, d_b);
  const auto parameter1 = correlated_parameter_(ParameterID{1}, d_c);
  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(e_a),
    PredicateNode::make(and_(equals_(e_b, parameter0), less_than_(e_c, parameter1)), node_e));

  const auto subquery =
  lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, d_b), std::make_pair(ParameterID{1}, d_c));

  const auto input_lqp =
  PredicateNode::make(in_(d_a, subquery), node_d);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, NoRewriteConstantIn) {
  // SELECT * FROM a WHERE IN (1, 2, 3)
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(in_(a_a, list_(1, 2, 3)), node_a);
  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, NoRewriteUncorrelatedExists) {
  // SELECT * FROM a WHERE (NOT) EXISTS (SELECT * FROM b)
  // clang-format off
  const auto subquery =
  lqp_subquery_(node_b);

  std::vector<std::shared_ptr<ExistsExpression>> operators;
  operators.emplace_back(exists_(subquery));
  operators.emplace_back(not_exists_(subquery));

  for (const auto& operator_ : operators) {
    const auto input_lqp =
    PredicateNode::make(operator_, node_a);
    const auto expected_lqp = input_lqp->deep_copy();
    // clang-format on
    const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
}

TEST_F(SubqueryToJoinRuleTest, NoRewriteIfLeftOperandIsNotAColumn) {
  // SELECT * FROM a WHERE a.a + 2 IN (SELECT b.a FROM b)
  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a), node_b);

  const auto subquery = lqp_subquery_(subquery_lqp);

  const auto input_lqp =
  PredicateNode::make(in_(add_(a_a, 2), subquery), node_a);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, NoRewriteIfJoinUsesCorrelatedParameter) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a FROM b JOIN c ON a.b = c.b)
  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);
  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    JoinNode::make(JoinMode::Inner, equals_(parameter, c_b), node_b, node_c));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(in_(a_a, subquery), node_a);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, NoRewriteIfCorrelatedParameterIsUsedBelowJoin) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a FROM b JOIN (SELECT * FROM c WHERE a.b = c.b) AS ac ON b.b = ac.b)
  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);
  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    JoinNode::make(JoinMode::Inner, equals_(b_b, c_b), node_b,
      PredicateNode::make(equals_(parameter, c_b), node_c)));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(in_(a_a, subquery), node_a);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, NoRewriteIfCorrelatedParameterIsUsedBelowLimitNode) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a FROM b WHERE b.b = a.b LIMIT 10)
  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);
  // clang-format off
  const auto subquery_lqp =
  LimitNode::make(value_(10),
    ProjectionNode::make(expression_vector(b_a),
      PredicateNode::make(equals_(b_b, parameter), node_b)));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(in_(a_a, subquery), node_a);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, NoRewriteIfCorrelatedParameterInPredicateOtherThanEqualsBelowAggregate) {
  // SELECT * FROM a WHERE a.a IN (SELECT SUM(b.a) FROM b WHERE b.b < a.b)
  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);
  // clang-format off
  const auto subquery_lqp =
  AggregateNode::make(expression_vector(), expression_vector(sum_(b_b)),
    PredicateNode::make(less_than_(b_b, parameter), node_b));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(in_(a_a, subquery), node_a);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, NoRewriteIfCorrelatedParameterInProjection) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a + a.b FROM b)
  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);
  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(add_(b_a, parameter)), node_b);

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(in_(a_a, subquery), node_a);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, NoRewriteCorrelatedNotIn) {
  // SELECT * FROM a WHERE a.a NOT IN (SELECT b.a FROM b WHERE b.b = a.b)
  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);
  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    PredicateNode::make(equals_(b_b, parameter), node_b));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(not_in_(a_a, subquery), node_a);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, NoRewriteCorrelatedNestedIn) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a FROM b WHERE a.a IN (SELECT c.a FROM c))
  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);
  // clang-format off
  const auto inner_subquery_lqp =
  ProjectionNode::make(expression_vector(c_a), node_c);

  const auto inner_subquery = lqp_subquery_(inner_subquery_lqp);

  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    PredicateNode::make(in_(parameter, inner_subquery), node_b));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_a));

  const auto input_lqp =
  PredicateNode::make(not_in_(a_a, subquery), node_a);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinRuleTest, NoRewriteCorrelatedNestedExists) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a FROM b WHERE EXISTS (SELECT * FROM c WHERE c.a = a.a))
  const auto parameter0 = correlated_parameter_(ParameterID{0}, a_a);
  const auto parameter1 = correlated_parameter_(ParameterID{1}, a_a);
  // clang-format off
  const auto inner_subquery_lqp =
  PredicateNode::make(equals_(c_a, parameter1), node_c);

  const auto inner_subquery = lqp_subquery_(inner_subquery_lqp, std::make_pair(ParameterID{1}, parameter0));

  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    PredicateNode::make(exists_(inner_subquery), node_b));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_a));

  const auto input_lqp =
  PredicateNode::make(not_in_(a_a, subquery), node_a);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
