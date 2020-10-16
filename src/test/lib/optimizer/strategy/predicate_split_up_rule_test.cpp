#include "strategy_base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/strategy/predicate_split_up_rule.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class PredicateSplitUpRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    node_a = MockNode::make(MockNode::ColumnDefinitions{
        {DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::String, "c"}, {DataType::String, "d"}});
    a_a = node_a->get_column("a");
    a_b = node_a->get_column("b");
    a_c = node_a->get_column("c");
    a_d = node_a->get_column("d");

    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}}, "b");
    b_a = node_b->get_column("a");
    b_b = node_b->get_column("b");

    rule = std::make_shared<PredicateSplitUpRule>();
  }

  std::shared_ptr<MockNode> node_a, node_b;
  std::shared_ptr<LQPColumnExpression> a_a, a_b, a_c, a_d, b_a, b_b;
  std::shared_ptr<PredicateSplitUpRule> rule;
};

TEST_F(PredicateSplitUpRuleTest, SplitUpConjunctionInPredicateNode) {
  // SELECT * FROM (
  //   SELECT a, b FROM a WHERE a = b AND a = 3
  // ) WHERE (a = 5 AND b > 7) AND 13 = 13
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(and_(equals_(a_a, 5), greater_than_(a_b, 7)),
    PredicateNode::make(equals_(13, 13),
      ProjectionNode::make(expression_vector(a_b, a_a),
        PredicateNode::make(and_(equals_(a_a, a_b), greater_than_(a_a, 3)),
          node_a))));

  const auto expected_lqp =
  PredicateNode::make(greater_than_(a_b, 7),
    PredicateNode::make(equals_(a_a, 5),
      PredicateNode::make(equals_(13, 13),
        ProjectionNode::make(expression_vector(a_b, a_a),
          PredicateNode::make(greater_than_(a_a, 3),
            PredicateNode::make(equals_(a_a, a_b),
              node_a))))));
  // clang-format on

  const auto actual_lqp = apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateSplitUpRuleTest, SplitUpSimpleDisjunctionInPredicateNode) {
  // SELECT * FROM a WHERE a < 3 OR a > 5
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(or_(less_than_(a_a, value_(3)), greater_than_(a_a, value_(5))),
    node_a);

  const auto expected_lqp =
  UnionNode::make(SetOperationMode::Positions,
    PredicateNode::make(less_than_(a_a, value_(3)),
      node_a),
    PredicateNode::make(greater_than_(a_a, value_(5)),
      node_a));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateSplitUpRuleTest, SplitUpComplexDisjunctionInPredicateNode) {
  // SELECT * FROM a WHERE b = 7 OR a < 3 OR a >= 5 OR 9 < b
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(or_(equals_(a_b, value_(7)), or_(less_than_(a_a, value_(3)), or_(greater_than_equals_(a_a, value_(5)), less_than_(9, a_b)))),  // NOLINT
    node_a);

  const auto expected_lqp =
  UnionNode::make(SetOperationMode::Positions,
    PredicateNode::make(equals_(a_b, value_(7)),
      node_a),
    UnionNode::make(SetOperationMode::Positions,
      PredicateNode::make(less_than_(a_a, value_(3)),
        node_a),
      UnionNode::make(SetOperationMode::Positions,
        PredicateNode::make(greater_than_equals_(a_a, value_(5)),
          node_a),
        PredicateNode::make(less_than_(9, a_b),
          node_a))));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateSplitUpRuleTest, SplitUpNotLike) {
  // SELECT * FROM a WHERE a < 'foo' OR a >= 'fop' - should identify the conditions as mutually exclusive and use
  // SetOperationMode::All. The expression could have been the result of the ExpressionReductionRule being applied
  // to `a NOT LIKE 'foo%'`.

  // clang-format off
  const auto input_lqp =
  PredicateNode::make(or_(less_than_(a_c, value_("foo")), greater_than_equals_(a_c, value_("fop"))),
    node_a);

  const auto expected_lqp =
  UnionNode::make(SetOperationMode::All,
    PredicateNode::make(less_than_(a_c, value_("foo")),
      node_a),
    PredicateNode::make(greater_than_equals_(a_c, value_("fop")),
      node_a));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateSplitUpRuleTest, SplitUpNotLikeDoesNotApply1) {
  // SELECT * FROM a WHERE c < 'foo' OR c >= 'bar' - looks like SplitUpNotLike, but the optimization does not apply
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(or_(less_than_(a_c, value_("foo")), greater_than_equals_(a_c, value_("bar"))),
    node_a);

  const auto expected_lqp =
  UnionNode::make(SetOperationMode::Positions,
    PredicateNode::make(less_than_(a_c, value_("foo")),
      node_a),
    PredicateNode::make(greater_than_equals_(a_c, value_("bar")),
      node_a));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateSplitUpRuleTest, SplitUpNotLikeDoesNotApply2) {
  // SELECT * FROM a WHERE c < 'foo' OR d >= 'fop' - looks like SplitUpNotLike, but the optimization does not apply
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(or_(less_than_(a_c, value_("foo")), greater_than_equals_(a_d, value_("fop"))),
    node_a);

  const auto expected_lqp =
  UnionNode::make(SetOperationMode::Positions,
    PredicateNode::make(less_than_(a_c, value_("foo")),
      node_a),
    PredicateNode::make(greater_than_equals_(a_d, value_("fop")),
      node_a));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateSplitUpRuleTest, SplitUpMutuallyExclusiveInts) {
  // SELECT * FROM a WHERE a < 5 OR b >= 8 - should identify the conditions as mutually exclusive and use
  // SetOperationMode::All
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(or_(less_than_(a_a, value_(5)), greater_than_equals_(a_a, value_(8))),
    node_a);

  const auto expected_lqp =
  UnionNode::make(SetOperationMode::All,
    PredicateNode::make(less_than_(a_a, value_(5)),
      node_a),
    PredicateNode::make(greater_than_equals_(a_a, value_(8)),
      node_a));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateSplitUpRuleTest, SplitBelowProjection) {
  // SELECT a FROM a WHERE 1 OR 3 > 2

  // clang-format off
  const auto input_lqp =
  ProjectionNode::make(expression_vector(a_a),
    PredicateNode::make(or_(value_(1), greater_than_(value_(3), value_(2))),
      node_a));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(a_a),
    UnionNode::make(SetOperationMode::Positions,
      PredicateNode::make(value_(1),
        node_a),
      PredicateNode::make(greater_than_(value_(3), value_(2)),
        node_a)));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateSplitUpRuleTest, HandleDiamondLQPWithCorrelatedParameters) {
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

  const auto input_lqp =
  JoinNode::make(JoinMode::Inner, equals_(a_a, a_b),
    ProjectionNode::make(expression_vector(a_a),
      predicate_node),
    ProjectionNode::make(expression_vector(a_b),
      predicate_node));

  const auto union_node =
  UnionNode::make(SetOperationMode::Positions,
    PredicateNode::make(greater_than_(a_a, parameter0),
      node_a),
    PredicateNode::make(greater_than_(a_b, parameter1),
      node_a));

  const auto expected_lqp =
  JoinNode::make(JoinMode::Inner, equals_(a_a, a_b),
    ProjectionNode::make(expression_vector(a_a),
      union_node),
    ProjectionNode::make(expression_vector(a_b),
      union_node));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateSplitUpRuleTest, SplitUpSimpleNestedConjunctionsAndDisjunctions) {
  // SELECT * FROM a WHERE (a > 10 OR a < 8) AND (b <= 7 OR 11 = b)
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(and_(or_(greater_than_(a_a, value_(10)), less_than_(a_a, value_(8))), or_(less_than_equals_(a_b, 7), equals_(value_(11), a_b))),  // NOLINT
    node_a);

  const auto lower_union_node =
  UnionNode::make(SetOperationMode::Positions,
    PredicateNode::make(greater_than_(a_a, value_(10)),
      node_a),
    PredicateNode::make(less_than_(a_a, value_(8)),
      node_a));

  const auto expected_lqp =
  UnionNode::make(SetOperationMode::Positions,
    PredicateNode::make(less_than_equals_(a_b, 7),
      lower_union_node),
    PredicateNode::make(equals_(value_(11), a_b),
      lower_union_node));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateSplitUpRuleTest, SplitUpComplexNestedConjunctionsAndDisjunctions) {
  // SELECT * FROM (
  //   SELECT a, b FROM a WHERE a = b AND a = 3
  // ) WHERE ((a > 10 OR a < 8) AND (b <= 7 OR 11 = b)) OR ((a = 5 AND b > 7) AND 13 = 13)
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(or_(and_(or_(greater_than_(a_a, value_(10)), less_than_(a_a, value_(8))), or_(less_than_equals_(a_b, 7), equals_(value_(11), a_b))), and_(and_(equals_(a_a, 5), greater_than_(a_b, 7)), equals_(13, 13))),  // NOLINT
    ProjectionNode::make(expression_vector(a_b, a_a),
      PredicateNode::make(and_(equals_(a_a, a_b), greater_than_(a_a, 3)),
        node_a)));

  const auto sub_lqp =
  ProjectionNode::make(expression_vector(a_b, a_a),
    PredicateNode::make(greater_than_(a_a, 3),
      PredicateNode::make(equals_(a_a, a_b),
        node_a)));

  const auto lower_union_node =
  UnionNode::make(SetOperationMode::Positions,
    PredicateNode::make(greater_than_(a_a, value_(10)),
      sub_lqp),
    PredicateNode::make(less_than_(a_a, value_(8)),
      sub_lqp));

  const auto expected_lqp =
  UnionNode::make(SetOperationMode::Positions,
    UnionNode::make(SetOperationMode::Positions,
      PredicateNode::make(less_than_equals_(a_b, 7),
        lower_union_node),
      PredicateNode::make(equals_(value_(11), a_b),
        lower_union_node)),
    PredicateNode::make(greater_than_(a_b, 7),
      PredicateNode::make(equals_(a_a, 5),
        PredicateNode::make(equals_(13, 13),
          sub_lqp))));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateSplitUpRuleTest, NoRewriteSimplePredicate) {
  // SELECT * FROM a WHERE a < 10

  // clang-format off
  const auto input_lqp =
  PredicateNode::make(less_than_(a_a, value_(10)),
    node_a);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
