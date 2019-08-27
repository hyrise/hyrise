#include "strategy_base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "optimizer/strategy/predicate_split_up_rule.hpp"
#include "testing_assert.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class PredicateSplitUpRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}});
    a_a = node_a->get_column("a");
    a_b = node_a->get_column("b");

    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}}, "b");
    b_a = node_b->get_column("a");
    b_b = node_b->get_column("b");

    node_c = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "c");
    c_a = node_c->get_column("a");

    node_d = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "d");
    d_a = node_d->get_column("a");

    node_e = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "e");
    e_a = node_e->get_column("a");

    rule = std::make_shared<PredicateSplitUpRule>();
  }

  std::shared_ptr<MockNode> node_a, node_b, node_c, node_d, node_e;
  LQPColumnReference a_a, a_b, b_a, b_b, c_a, d_a, e_a;
  std::shared_ptr<PredicateSplitUpRule> rule;
};

TEST_F(PredicateSplitUpRuleTest, SplitUpConjunctionInPredicateNode) {
  // SELECT * FROM (
  //   SELECT a, b FROM a WHERE 13 = 13 AND (a = b AND a = 3)
  // ) WHERE a = 5 AND b = 7
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

TEST_F(PredicateSplitUpRuleTest, TwoExistsToUnion) {
  // SELECT * FROM a WHERE EXISTS (
  //   SELECT * FROM b WHERE b.a = a.a
  // ) OR EXISTS (
  //   SELECT * FROM c WHERE c.a = a.a
  // )

  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);

  // clang-format off
  const auto subquery_lqp_a =
  PredicateNode::make(equals_(b_a, parameter),
    node_b);

  const auto subquery_a = lqp_subquery_(subquery_lqp_a, std::make_pair(ParameterID{0}, a_a));

  const auto subquery_lqp_b =
  PredicateNode::make(equals_(c_a, parameter),
    node_c);

  const auto subquery_b = lqp_subquery_(subquery_lqp_b, std::make_pair(ParameterID{0}, a_a));

  const auto input_lqp =
  PredicateNode::make(or_(exists_(subquery_a), exists_(subquery_b)),
    node_a);

  const auto expected_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(exists_(subquery_a),
      node_a),
    PredicateNode::make(exists_(subquery_b),
      node_a));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateSplitUpRuleTest, FourExistsToUnion) {
  // SELECT * FROM a WHERE EXISTS (
  //   SELECT * FROM b WHERE b.a = a.a
  // ) OR EXISTS (
  //   SELECT * FROM c WHERE c.a = a.a
  // ) OR EXISTS (
  //   SELECT * FROM d WHERE d.a = a.a
  // ) OR EXISTS (
  //   SELECT * FROM e WHERE e.a = a.a
  // )

  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);

  // clang-format off
  const auto subquery_lqp_a =
  PredicateNode::make(equals_(b_a, parameter),
    node_b);

  const auto subquery_a = lqp_subquery_(subquery_lqp_a, std::make_pair(ParameterID{0}, a_a));

  const auto subquery_lqp_b =
  PredicateNode::make(equals_(c_a, parameter),
    node_c);

  const auto subquery_b = lqp_subquery_(subquery_lqp_b, std::make_pair(ParameterID{0}, a_a));

  const auto subquery_lqp_c =
  PredicateNode::make(equals_(d_a, parameter),
    node_d);

  const auto subquery_c = lqp_subquery_(subquery_lqp_c, std::make_pair(ParameterID{0}, a_a));

  const auto subquery_lqp_d =
  PredicateNode::make(equals_(e_a, parameter),
    node_e);

  const auto subquery_d = lqp_subquery_(subquery_lqp_d, std::make_pair(ParameterID{0}, a_a));

  const auto input_lqp =
  PredicateNode::make(or_(exists_(subquery_a), or_(exists_(subquery_b), or_(exists_(subquery_c), exists_(subquery_d)))),
    node_a);

  const auto expected_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(exists_(subquery_a),
      node_a),
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(exists_(subquery_b),
        node_a),
      UnionNode::make(UnionMode::Positions,
        PredicateNode::make(exists_(subquery_c),
          node_a),
        PredicateNode::make(exists_(subquery_d),
          node_a))));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateSplitUpRuleTest, SelectColumn) {
  // SELECT a FROM a WHERE 1 OR 3 > 2

  // clang-format off
  const auto input_lqp =
  ProjectionNode::make(expression_vector(a_a),
    PredicateNode::make(or_(value_(1), greater_than_(value_(3), value_(2))),
      node_a));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(a_a),
    UnionNode::make(UnionMode::Positions,
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
  UnionNode::make(UnionMode::Positions,
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
  PredicateNode::make(and_(or_(greater_than_(a_a, value_(10)), less_than_(a_a, value_(8))), or_(less_than_equals_(a_b, 7), equals_(value_(11), a_b))),
    node_a);

  const auto lower_union_node =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(greater_than_(a_a, value_(10)),
      node_a),
    PredicateNode::make(less_than_(a_a, value_(8)),
      node_a));

  const auto expected_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(less_than_equals_(a_b, 7),
      lower_union_node),
    PredicateNode::make(equals_(value_(11), a_b),
      lower_union_node));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateSplitUpRuleTest, NoRewriteSimplePredicate) {
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

}  // namespace opossum
