#include "gtest/gtest.h"

#include "strategy_base_test.hpp"
#include "testing_assert.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
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
    a_a = {node_a, ColumnID{0}};
    a_b = {node_a, ColumnID{1}};

    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}}, "b");
    b_a = {node_b, ColumnID{0}};
    b_b = {node_b, ColumnID{1}};

    node_c = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "c");
    c_a = {node_c, ColumnID{0}};
    c_b = {node_c, ColumnID{1}};

    node_d = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "d");
    d_a = {node_d, ColumnID{0}};
    d_b = {node_d, ColumnID{1}};
    d_c = {node_d, ColumnID{2}};

    node_e = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "e");
    e_a = {node_e, ColumnID{0}};
    e_b = {node_e, ColumnID{1}};
    e_c = {node_e, ColumnID{2}};

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
  JoinNode::make(JoinMode::Semi, expression_vector(equals_(a_a, b_a), equals_(a_b, b_b)), node_a, node_b);
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

TEST_F(SubqueryToJoinRuleTest, UncorrelatedNotIn) {
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

TEST_F(SubqueryToJoinRuleTest, SimpleCorrelatedNotInWithEqualityPredicate) {
  // SELECT * FROM a WHERE a.a NOT IN (SELECT b.a FROM b WHERE b.b = a.b)
  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);
  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    PredicateNode::make(equals_(b_b, parameter), node_b));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(not_in_(a_a, subquery), node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::AntiDiscardNulls, expression_vector(equals_(a_a, b_a), equals_(a_b, b_b)), node_a, node_b);
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// NO REWRITE CASES

TEST_F(SubqueryToJoinRuleTest, SimpleCorrelatedNotInWithLessThanPredicate) {
  // SELECT * FROM a WHERE a.a NOT IN (SELECT b.a FROM b WHERE b.b < a.b)
  const auto parameter = correlated_parameter_(ParameterID{0}, a_b);
  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(b_a),
    PredicateNode::make(less_than_(b_b, parameter), node_b));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, a_b));

  const auto input_lqp =
  PredicateNode::make(not_in_(a_a, subquery), node_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::AntiDiscardNulls, expression_vector(equals_(a_a, b_a), greater_than_(a_b, b_b)), node_a, node_b);
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// We will reformulate this query once the MultiPredicateJoin feature is implemented (#1482).
TEST_F(SubqueryToJoinRuleTest, NoRewriteOfDoubleCorrelatedIn) {
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
  // SELECT * FROM a WHERE EXISTS (SELECT * FROM b)
  // clang-format off
  const auto subquery =
  lqp_subquery_(node_b);

  const auto input_lqp =
  PredicateNode::make(exists_(subquery), node_a);
  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
