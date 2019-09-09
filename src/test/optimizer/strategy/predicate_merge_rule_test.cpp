#include "strategy_base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/strategy/predicate_merge_rule.hpp"
#include "testing_assert.hpp"

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

    // Build a chain of diamond structures to trigger the optimization
    // clang-format off
    const auto diamond0 =
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(value_(1),
        node_a),
      PredicateNode::make(value_(1),
        node_a));

    const auto diamond1 =
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(value_(1),
        diamond0),
      PredicateNode::make(value_(1),
        diamond0));

    union_chain =
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(value_(1),
        diamond1),
      PredicateNode::make(value_(1),
        diamond1));

    union_chain_expression =
      and_(and_(or_(value_(1), value_(1)), or_(value_(1), value_(1))), or_(value_(1), value_(1)));
    // clang-format on
  }

  std::shared_ptr<MockNode> node_a, node_b;
  LQPColumnReference a_a, a_b, b_a, b_b;
  std::shared_ptr<PredicateMergeRule> rule;
  std::shared_ptr<AbstractLQPNode> union_chain;
  std::shared_ptr<AbstractExpression> union_chain_expression;
};

TEST_F(PredicateMergeRuleTest, MergeUnionBelowPredicate) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(equals_(a_a, value_(1)),
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(value_(1),
        union_chain),
      PredicateNode::make(value_(1),
        union_chain)));

  const auto expected_lqp =
  PredicateNode::make(and_(union_chain_expression, and_(or_(value_(1), value_(1)), equals_(a_a, value_(1)))),
    node_a);
  // clang-format on

//  const auto actual_lqp = apply_rule(rule, input_lqp);

    std::cout << "INPUT\n" << *input_lqp << "\n\n";
//    std::cout << "ACTUAL\n" << *actual_lqp << "\n\n";
    std::cout << "EXPECTED\n" << *expected_lqp << "\n\n";

//  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateMergeRuleTest, MergeUnionBelowPredicateBelowUnion) {
  // clang-format off
  const auto predicate_node =
  PredicateNode::make(equals_(a_a, value_(3)),
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(value_(4),
        node_a),
      PredicateNode::make(value_(5),
        node_a)));

  const auto input_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(value_(1),
      predicate_node),
    PredicateNode::make(value_(2),
      predicate_node));

  const auto expected_lqp =
  PredicateNode::make(and_(and_(or_(value_(4), value_(5)), equals_(a_a, value_(3))), or_(value_(1), value_(2))),
    node_a);
  // clang-format on

  const auto actual_lqp = apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateMergeRuleTest, MergeSimpleDisjunction) {
  // SELECT * FROM a WHERE a < 3 OR a >= 5
  // clang-format off
  const auto input_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(less_than_(a_a, value_(3)),
      node_a),
    PredicateNode::make(greater_than_equals_(a_a, value_(5)),
      node_a));

  const auto expected_lqp =
  PredicateNode::make(or_(less_than_(a_a, value_(3)), greater_than_equals_(a_a, value_(5))),
    node_a);
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateMergeRuleTest, MergeComplexDisjunction) {
  // SELECT * FROM a WHERE b = 7 OR a < 3 OR a >= 5 OR 9 < b
  // clang-format off
  const auto input_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(equals_(a_b, value_(7)),
      node_a),
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(less_than_(a_a, value_(3)),
        node_a),
      UnionNode::make(UnionMode::Positions,
        PredicateNode::make(greater_than_equals_(a_a, value_(5)),
          node_a),
        PredicateNode::make(less_than_(9, a_b),
          node_a))));

  const auto expected_lqp =
  PredicateNode::make(or_(equals_(a_b, value_(7)), or_(less_than_(a_a, value_(3)), or_(greater_than_equals_(a_a, value_(5)), less_than_(9, a_b)))),  // NOLINT
    node_a);
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateMergeRuleTest, SelectColumn) {
  // SELECT a FROM a WHERE 1 OR 3 > 2

  // clang-format off
  const auto input_lqp =
  ProjectionNode::make(expression_vector(a_a),
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(value_(1),
        node_a),
      PredicateNode::make(greater_than_(value_(3), value_(2)),
        node_a)));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(a_a),
    PredicateNode::make(or_(value_(1), greater_than_(value_(3), value_(2))),
      node_a));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateMergeRuleTest, HandleDiamondLQPWithCorrelatedParameters) {
  // SELECT * FROM (
  //   SELECT a FROM a, b WHERE a.a > b.a OR a.b > b.b
  // ) r JOIN (
  //   SELECT b FROM a, b WHERE a.a > b.a OR a.b > b.b
  // ) s ON r.a = s.b

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
  // SELECT * FROM a WHERE (a > 10 OR a < 8) AND (b <= 7 OR 11 = b)
  // clang-format off
  const auto lower_union_node =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(greater_than_(a_a, value_(10)),
      node_a),
    PredicateNode::make(less_than_(a_a, value_(8)),
      node_a));

  const auto input_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(less_than_equals_(a_b, 7),
      lower_union_node),
    PredicateNode::make(equals_(value_(11), a_b),
      lower_union_node));

  const auto expected_lqp =
  PredicateNode::make(and_(or_(greater_than_(a_a, value_(10)), less_than_(a_a, value_(8))), or_(less_than_equals_(a_b, 7), equals_(value_(11), a_b))),  // NOLINT
    node_a);
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateMergeRuleTest, MergeComplexNestedConjunctionsAndDisjunctions) {
  // SELECT * FROM (
  //   SELECT a, b FROM a WHERE a = b AND a = 3
  // ) WHERE ((a > 10 OR a < 8) AND (b <= 7 OR 11 = b)) OR (13 = 13 AND (a = 5 AND b > 7))
  // clang-format off
  const auto subquery_lqp =
  ProjectionNode::make(expression_vector(a_b, a_a),
    PredicateNode::make(and_(equals_(a_a, a_b), greater_than_(a_a, 3)),
        node_a));

  const auto lower_union_node =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(greater_than_(a_a, value_(10)),
      subquery_lqp),
    PredicateNode::make(less_than_(a_a, value_(8)),
      subquery_lqp));

  const auto input_lqp =
  UnionNode::make(UnionMode::Positions,
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(less_than_equals_(a_b, 7),
        lower_union_node),
      PredicateNode::make(equals_(value_(11), a_b),
        lower_union_node)),
    PredicateNode::make(greater_than_(a_b, 7),
      PredicateNode::make(equals_(a_a, 5),
        PredicateNode::make(equals_(13, 13),
          subquery_lqp))));

  const auto expected_lqp =
  PredicateNode::make(or_(and_(or_(greater_than_(a_a, value_(10)), less_than_(a_a, value_(8))), or_(less_than_equals_(a_b, 7), equals_(value_(11), a_b))), and_(equals_(13, 13), and_(equals_(a_a, 5), greater_than_(a_b, 7)))),  // NOLINT
    ProjectionNode::make(expression_vector(a_b, a_a),
      PredicateNode::make(and_(equals_(a_a, a_b), greater_than_(a_a, 3)),
        node_a)));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateMergeRuleTest, NoRewriteSimplePredicate) {
  // SELECT * FROM a WHERE a = 10

  // clang-format off
  const auto input_lqp =
  PredicateNode::make(value_(10),
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
    PredicateNode::make(equals_(13, 13)),
      node_a);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  const auto actual_lqp = apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
