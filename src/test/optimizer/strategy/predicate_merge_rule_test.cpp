#include "strategy_base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/strategy/predicate_merge_rule.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class PredicateMergeRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}});
    a_a = node_a->get_column("a");
    a_b = node_a->get_column("b");

    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}}, "b");
    b_a = node_b->get_column("a");
    b_b = node_b->get_column("b");

    rule = std::make_shared<PredicateMergeRule>();

    // Reducing the minimum_union_count so that plans are merged earlier, making the test cases shorter
    rule->minimum_union_count = 1;
  }

  std::shared_ptr<MockNode> node_a, node_b;
  LQPColumnReference a_a, a_b, b_a, b_b;
  std::shared_ptr<PredicateMergeRule> rule;
};

TEST_F(PredicateMergeRuleTest, MergeUnionBelowPredicate) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(equals_(a_a, 10),
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(less_than_(a_b, 8),
        node_a),
      PredicateNode::make(greater_than_(a_b, 12),
        node_a)));

  const auto expected_lqp =
  PredicateNode::make(and_(or_(less_than_(a_b, 8), greater_than_(a_b, 12)), equals_(a_a, 10)),
    node_a);
  // clang-format on

  const auto actual_lqp = apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateMergeRuleTest, MergeUnionBelowPredicateBelowUnion) {
  // clang-format off
  const auto sub_lqp =
  PredicateNode::make(equals_(a_a, 10),
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(less_than_(a_b, 8),
        node_a),
      PredicateNode::make(greater_than_(a_b, 12),
        node_a)));

  const auto input_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(less_than_(a_b, 15),
      sub_lqp),
    PredicateNode::make(greater_than_(a_b, 5),
      sub_lqp));

  const auto expected_lqp =
  PredicateNode::make(and_(or_(less_than_(a_b, 8), greater_than_(a_b, 12)), and_(equals_(a_a, 10), or_(less_than_(a_b, 15), greater_than_(a_b, 5)))),  // NOLINT
    node_a);
  // clang-format on

  const auto actual_lqp = apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateMergeRuleTest, MergeSimpleDisjunction) {
  // clang-format off
  const auto input_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(less_than_(a_a, 3),
      node_a),
    PredicateNode::make(greater_than_equals_(a_a, 5),
      node_a));

  const auto expected_lqp =
  PredicateNode::make(or_(less_than_(a_a, 3), greater_than_equals_(a_a, 5)),
    node_a);
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateMergeRuleTest, MergeComplexDisjunction) {
  // clang-format off
  const auto input_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(equals_(a_b, 7),
      node_a),
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(less_than_(a_a, 3),
        node_a),
      UnionNode::make(UnionMode::Positions,
        PredicateNode::make(greater_than_equals_(a_a, 5),
          node_a),
        PredicateNode::make(less_than_(9, a_b),
          node_a))));

  const auto expected_lqp =
  PredicateNode::make(or_(equals_(a_b, 7), or_(less_than_(a_a, 3), or_(greater_than_equals_(a_a, 5), less_than_(9, a_b)))),  // NOLINT
    node_a);
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateMergeRuleTest, MergeBelowProjection) {
  // clang-format off
  const auto input_lqp =
  ProjectionNode::make(expression_vector(a_a),
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(less_than_(a_a, 1),
        node_a),
      PredicateNode::make(greater_than_(3, 2),
        node_a)));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(a_a),
    PredicateNode::make(or_(less_than_(a_a, 1), greater_than_(3, 2)),
      node_a));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateMergeRuleTest, MergeOnlyAboveLeftTable) {
  // clang-format off
  const auto input_lqp =
  UnionNode::make(UnionMode::Positions,
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(equals_(a_a, 47),
        node_a),
      PredicateNode::make(equals_(a_b, 11),
        node_a)),
    PredicateNode::make(equals_(b_a, 3),
      node_b));

  const auto expected_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(or_(equals_(a_a, 47), equals_(a_b, 11)),
      node_a),
    PredicateNode::make(equals_(b_a, 3),
      node_b));
  // clang-format on

  const auto actual_lqp = apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateMergeRuleTest, MergeUnionsSeparatedByProjection) {
  // clang-format off
  const auto sub_lqp =
  ProjectionNode::make(expression_vector(a_a),
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(less_than_(a_a, 1),
        node_a),
      PredicateNode::make(greater_than_(3, 2),
        node_a)));

  const auto input_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(less_than_(a_a, 10),
      sub_lqp),
    PredicateNode::make(greater_than_(30, 20),
      sub_lqp));

  const auto expected_lqp =
  PredicateNode::make(or_(less_than_(a_a, 10), greater_than_(30, 20)),
    ProjectionNode::make(expression_vector(a_a),
      PredicateNode::make(or_(less_than_(a_a, 1), greater_than_(3, 2)),
        node_a)));
  // clang-format on

  const auto actual_lqp = apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateMergeRuleTest, MergeUnionsSeparatedByJoin) {
  // clang-format off
  const auto input_lqp =
  JoinNode::make(JoinMode::Inner, equals_(a_a, b_a),
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(less_than_(a_a, 1),
        node_a),
      PredicateNode::make(greater_than_(3, 2),
        node_a)),
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(less_than_(b_a, 10),
        node_b),
      PredicateNode::make(greater_than_(30, 20),
        node_b)));

  const auto expected_lqp =
  JoinNode::make(JoinMode::Inner, equals_(a_a, b_a),
    PredicateNode::make(or_(less_than_(a_a, 1), greater_than_(3, 2)),
      node_a),
    PredicateNode::make(or_(less_than_(b_a, 10), greater_than_(30, 20)),
      node_b));
  // clang-format on

  const auto actual_lqp = apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateMergeRuleTest, HandleDiamondLQPWithCorrelatedParameters) {
  const auto parameter0 = correlated_parameter_(ParameterID{0}, b_a);
  const auto parameter1 = correlated_parameter_(ParameterID{1}, b_b);

  // clang-format off
  const auto predicate_node =
  PredicateNode::make(or_(greater_than_(a_a, parameter0), greater_than_(a_b, parameter1)),
    node_a);

  const auto union_node =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(greater_than_(a_a, parameter0),
      node_a),
    PredicateNode::make(greater_than_(a_b, parameter1),
      node_a));

  const auto input_lqp =
  JoinNode::make(JoinMode::Inner, equals_(a_a, a_b),
    ProjectionNode::make(expression_vector(a_a),
      union_node),
    ProjectionNode::make(expression_vector(a_b),
      union_node));

  const auto expected_lqp =
  JoinNode::make(JoinMode::Inner, equals_(a_a, a_b),
    ProjectionNode::make(expression_vector(a_a),
      predicate_node),
    ProjectionNode::make(expression_vector(a_b),
      predicate_node));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateMergeRuleTest, MergeSimpleNestedConjunctionsAndDisjunctions) {
  // clang-format off
  const auto lower_union_node =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(greater_than_(a_a, 10),
      node_a),
    PredicateNode::make(less_than_(a_a, 8),
      node_a));

  const auto input_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(less_than_equals_(a_b, 7),
      lower_union_node),
    PredicateNode::make(equals_(11, a_b),
      lower_union_node));

  const auto expected_lqp =
  PredicateNode::make(and_(or_(greater_than_(a_a, 10), less_than_(a_a, 8)), or_(less_than_equals_(a_b, 7), equals_(11, a_b))),  // NOLINT
    node_a);
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateMergeRuleTest, MergeComplexNestedConjunctionsAndDisjunctions) {
  // clang-format off
  const auto sub_lqp =
  ProjectionNode::make(expression_vector(a_b, a_a),
    PredicateNode::make(and_(equals_(a_a, a_b), greater_than_(a_a, 3)),
        node_a));

  const auto lower_union_node =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(greater_than_(a_a, 10),
      sub_lqp),
    PredicateNode::make(less_than_(a_a, 8),
      sub_lqp));

  const auto input_lqp =
  UnionNode::make(UnionMode::Positions,
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(less_than_equals_(a_b, 7),
        lower_union_node),
      PredicateNode::make(equals_(11, a_b),
        lower_union_node)),
    PredicateNode::make(greater_than_(a_b, 7),
      PredicateNode::make(equals_(a_a, 5),
        PredicateNode::make(equals_(13, 13),
          sub_lqp))));

  const auto expected_lqp =
  PredicateNode::make(or_(and_(or_(greater_than_(a_a, 10), less_than_(a_a, 8)), or_(less_than_equals_(a_b, 7), equals_(11, a_b))), and_(equals_(13, 13), and_(equals_(a_a, 5), greater_than_(a_b, 7)))),  // NOLINT
    ProjectionNode::make(expression_vector(a_b, a_a),
      PredicateNode::make(and_(equals_(a_a, a_b), greater_than_(a_a, 3)),
        node_a)));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateMergeRuleTest, NoRewriteSimplePredicate) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(less_than_(a_a, 10),
    node_a);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateMergeRuleTest, NoRewritePredicateChains) {
  // Pure predicate chains won't be merged since this is unlikely to improve the performance.
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(equals_(a_a, 5),
    PredicateNode::make(equals_(13, 13),
      node_a));

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  const auto actual_lqp = apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateMergeRuleTest, NoRewriteDifferentTables) {
  // clang-format off
  const auto input_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(equals_(a_a, 47),
      node_a),
    PredicateNode::make(equals_(a_b, 11),
      node_b));

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  const auto actual_lqp = apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateMergeRuleTest, NoRewriteDifferentUnionMode) {
  // clang-format off
  const auto input_lqp =
  UnionNode::make(UnionMode::All,
    PredicateNode::make(equals_(a_a, 47),
      node_a),
    PredicateNode::make(equals_(a_b, 11),
      node_a));

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  const auto actual_lqp = apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
