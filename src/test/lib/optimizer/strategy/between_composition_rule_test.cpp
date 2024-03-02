#include <memory>
#include <string>
#include <vector>

#include "expression/expression_functional.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "optimizer/strategy/between_composition_rule.hpp"
#include "statistics/table_statistics.hpp"
#include "strategy_base_test.hpp"
#include "utils/assert.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class BetweenCompositionRuleTest : public StrategyBaseTest {
 protected:
  void SetUp() override {
    _rule = std::make_shared<BetweenCompositionRule>();

    _node_a =
        MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}});

    _a_a = _node_a->get_column("a");
    _a_b = _node_a->get_column("b");
    _a_c = _node_a->get_column("c");

    _node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}});

    _b_a = _node_b->get_column("a");
  }

  std::shared_ptr<MockNode> _node_a;
  std::shared_ptr<MockNode> _node_b;
  std::shared_ptr<LQPColumnExpression> _a_a;
  std::shared_ptr<LQPColumnExpression> _a_b;
  std::shared_ptr<LQPColumnExpression> _a_c;
  std::shared_ptr<LQPColumnExpression> _b_a;
  std::shared_ptr<BetweenCompositionRule> _rule;
};

TEST_F(BetweenCompositionRuleTest, ColumnExpressionLeft) {
  // clang-format off
  _lqp =
  PredicateNode::make(greater_than_equals_(_a_a, 200),
    PredicateNode::make(less_than_equals_(_a_a, 300),
      _node_a));

  const auto expected_lqp =
  PredicateNode::make(between_inclusive_(_a_a, 200, 300),
    _node_a);
  // clang-format on

  _apply_rule(_rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(BetweenCompositionRuleTest, ColumnExpressionRight) {
  // clang-format off
  _lqp =
  PredicateNode::make(less_than_equals_(200, _a_a),
    PredicateNode::make(greater_than_equals_(300, _a_a),
      _node_a));

  const auto expected_lqp =
  PredicateNode::make(between_inclusive_(_a_a, 200, 300),
    _node_a);
  // clang-format on

  _apply_rule(_rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(BetweenCompositionRuleTest, NoColumnRange) {
  // clang-format off
  _lqp =
  PredicateNode::make(less_than_equals_(_a_b, 300),
    PredicateNode::make(greater_than_equals_(_a_a, 200),
      _node_a))->deep_copy();

  const auto expected_lqp =
  PredicateNode::make(less_than_equals_(_a_b, 300),
    PredicateNode::make(greater_than_equals_(_a_a, 200),
      _node_a));
  // clang-format on

  _apply_rule(_rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(BetweenCompositionRuleTest, EmptyColumnRange) {
  // clang-format off
  _lqp =
  PredicateNode::make(greater_than_equals_(_a_a, 300),
    PredicateNode::make(less_than_equals_(_a_a, 200),
      _node_a))->deep_copy();

  const auto expected_lqp =
  PredicateNode::make(between_inclusive_(_a_a, 300, 200),
    _node_a);
  // clang-format on

  _apply_rule(_rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(BetweenCompositionRuleTest, NoPullPastOrExpression) {
  // clang-format off
  _lqp =
  PredicateNode::make(or_(greater_than_equals_(_a_a, 200), less_than_equals_(_a_a, 300)),
    _node_a);

  const auto expected_lqp = _lqp->deep_copy();
  // clang-format on

  _apply_rule(_rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(BetweenCompositionRuleTest, PredicateChain) {
  // clang-format off
  _lqp =
  PredicateNode::make(greater_than_equals_(_a_a, 200),
    PredicateNode::make(less_than_equals_(_a_a, 300),
      _node_a))->deep_copy();

  const auto expected_lqp =
  PredicateNode::make(between_inclusive_(_a_a, 200, 300),
    _node_a);
  // clang-format on

  _apply_rule(_rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(BetweenCompositionRuleTest, LongPredicateChain) {
  // clang-format off
  _lqp =
  PredicateNode::make(greater_than_equals_(_a_a, 200),
    PredicateNode::make(less_than_equals_(_a_b, 300),
      PredicateNode::make(less_than_equals_(_a_a, 300),
        _node_a)))->deep_copy();

  const auto expected_lqp =
  PredicateNode::make(between_inclusive_(_a_a, 200, 300),
    PredicateNode::make(less_than_equals_(_a_b, 300),
      _node_a));
  // clang-format on

  _apply_rule(_rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(BetweenCompositionRuleTest, LeftExclusive) {
  // clang-format off
  _lqp =
  PredicateNode::make(greater_than_(_a_a, 200),
    PredicateNode::make(less_than_equals_(_a_a, 300),
      _node_a))->deep_copy();

  const auto expected_lqp =
  PredicateNode::make(between_lower_exclusive_(_a_a, 200, 300),
    _node_a);
  // clang-format on

  _apply_rule(_rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(BetweenCompositionRuleTest, RightExclusive) {
  // clang-format off
  _lqp =
  PredicateNode::make(greater_than_equals_(_a_a, 200),
    PredicateNode::make(less_than_(_a_a, 300),
      _node_a))->deep_copy();

  const auto expected_lqp =
  PredicateNode::make(between_upper_exclusive_(_a_a, 200, 300),
    _node_a);
  // clang-format on

  _apply_rule(_rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(BetweenCompositionRuleTest, BothExclusive) {
  // clang-format off
  _lqp =
  PredicateNode::make(greater_than_(_a_a, 200),
    PredicateNode::make(less_than_(_a_a, 300),
      _node_a))->deep_copy();

  const auto expected_lqp =
  PredicateNode::make(between_exclusive_(_a_a, 200, 300),
    _node_a);
  // clang-format on

  _apply_rule(_rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(BetweenCompositionRuleTest, TwoChains) {
  // clang-format off
  _lqp =
  PredicateNode::make(greater_than_(_a_a, 200),
    PredicateNode::make(less_than_(_a_a, 300),
      ValidateNode::make(
        PredicateNode::make(greater_than_(_a_b, 500),
          PredicateNode::make(less_than_(_a_b, 600),
            _node_a)))))->deep_copy();

  const auto expected_lqp =
  PredicateNode::make(between_exclusive_(_a_a, 200, 300),
      ValidateNode::make(
        PredicateNode::make(between_exclusive_(_a_b, 500, 600),
          _node_a)));
  // clang-format on

  _apply_rule(_rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(BetweenCompositionRuleTest, NoPullPastAggregate) {
  // clang-format off
  _lqp =
  PredicateNode::make(greater_than_equals_(_a_a, 200),
    AggregateNode::make(expression_vector(_a_a), expression_vector(),
      PredicateNode::make(less_than_equals_(_a_a, 300),
        _node_a)));
  // clang-format on

  const auto expected_lqp = _lqp->deep_copy();

  _apply_rule(_rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(BetweenCompositionRuleTest, NoPullPastJoin) {
  // clang-format off
  _lqp =
  PredicateNode::make(less_than_equals_(_a_a, _b_a),
    PredicateNode::make(greater_than_equals_(_a_a, 200),
      JoinNode::make(JoinMode::Cross,
        PredicateNode::make(less_than_equals_(_a_a, 300),
          _node_a),
        _node_b)));
  // clang-format on

  const auto expected_lqp = _lqp->deep_copy();

  _apply_rule(_rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(BetweenCompositionRuleTest, TwoChainsBeforeJoin) {
  // clang-format off
  _lqp =
  PredicateNode::make(equals_(_a_a, _b_a),
    JoinNode::make(JoinMode::Cross,
      PredicateNode::make(less_than_equals_(_a_a, 230),
        PredicateNode::make(greater_than_equals_(_a_a, 200),
          _node_a)),
      PredicateNode::make(greater_than_equals_(_b_a, 400),
        PredicateNode::make(less_than_equals_(_b_a, 500),
          _node_b))))->deep_copy();

  const auto expected_lqp =
  PredicateNode::make(equals_(_a_a, _b_a),
    JoinNode::make(JoinMode::Cross,
      PredicateNode::make(between_inclusive_(_a_a, 200, 230),
        _node_a),
      PredicateNode::make(between_inclusive_(_b_a, 400, 500),
        _node_b)));
  // clang-format on

  _apply_rule(_rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(BetweenCompositionRuleTest, TwoColumnsNoMatch) {
  // clang-format off
  _lqp =
  PredicateNode::make(less_than_equals_(_a_a, _a_b),
    _node_a);

  const auto expected_lqp = _lqp->deep_copy();
  // clang-format on

  _apply_rule(_rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(BetweenCompositionRuleTest, FindOptimalInclusiveBetween) {
  // clang-format off
  _lqp =
  PredicateNode::make(greater_than_equals_(_a_a, 100),
    PredicateNode::make(greater_than_equals_(_a_a, 200),
      PredicateNode::make(less_than_equals_(_a_a, 400),
        PredicateNode::make(less_than_equals_(_a_a, 300),
          _node_a))));

  const auto expected_lqp =
  PredicateNode::make(between_inclusive_(_a_a, 200, 300),
    _node_a);
  // clang-format on

  _apply_rule(_rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(BetweenCompositionRuleTest, FindOptimalExclusiveBetween) {
  // clang-format off
  _lqp =
  PredicateNode::make(greater_than_(_a_a, 100),
    PredicateNode::make(greater_than_(_a_a, 200),
      PredicateNode::make(less_than_(_a_a, 400),
        PredicateNode::make(less_than_(_a_a, 300),
          _node_a))));

  const auto expected_lqp =
  PredicateNode::make(between_exclusive_(_a_a, 200, 300),
    _node_a);
  // clang-format on

  _apply_rule(_rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(BetweenCompositionRuleTest, KeepRemainingPredicates) {
  // clang-format off
  _lqp =
  PredicateNode::make(greater_than_equals_(_a_a, 200),
    PredicateNode::make(less_than_equals_(_a_a, 300),
      PredicateNode::make(less_than_equals_(_a_b, 300),
        _node_a)))->deep_copy();

  const auto expected_lqp =
  PredicateNode::make(between_inclusive_(_a_a, 200, 300),
    PredicateNode::make(less_than_equals_(_a_b, 300),
      _node_a));
  // clang-format on

  _apply_rule(_rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(BetweenCompositionRuleTest, MultipleBetweensVariousLocations) {
  // clang-format off
  _lqp =
  PredicateNode::make(greater_than_equals_(_a_a, 200),
    PredicateNode::make(less_than_equals_(_a_b, 200),
      PredicateNode::make(greater_than_equals_(_a_a, 230),
        PredicateNode::make(less_than_equals_(_a_c, 200),
          PredicateNode::make(less_than_equals_(_a_a, 250),
              PredicateNode::make(less_than_equals_(_a_a, 300),
                PredicateNode::make(greater_than_equals_(_a_b, 150),
                  _node_a)))))))->deep_copy();

  const auto expected_lqp =
  PredicateNode::make(between_inclusive_(_a_a, 230, 250),
    PredicateNode::make(between_inclusive_(_a_b, 150, 200),
      PredicateNode::make(less_than_equals_(_a_c, 200),
        _node_a)));
  // clang-format on

  _apply_rule(_rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(BetweenCompositionRuleTest, NonBoundaryPredicate) {
  // clang-format off
  _lqp =
  PredicateNode::make(greater_than_(_a_a, 100),
    PredicateNode::make(in_(_a_a, list_(1, 2, 3)),
      PredicateNode::make(less_than_(_a_a, 200),
        _node_a)));

  const auto expected_lqp =
  PredicateNode::make(between_exclusive_(_a_a, 100, 200),
    PredicateNode::make(in_(_a_a, list_(1, 2, 3)),
      _node_a));
  // clang-format on

  _apply_rule(_rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(BetweenCompositionRuleTest, NoPullPastDiamondPredicate) {
  // clang-format off
  const auto predicate_node =
  PredicateNode::make(greater_than_equals_(_a_a, 200),
    _node_a);

  _lqp =
  UnionNode::make(SetOperationMode::Positions,
    PredicateNode::make(greater_than_equals_(_a_a, 300),
      predicate_node),
    PredicateNode::make(greater_than_equals_(_a_a, 400),
      predicate_node));

  // clang-format on

  const auto expected_lqp = _lqp->deep_copy();

  _apply_rule(_rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(BetweenCompositionRuleTest, HandleMultipleEqualExpressions) {
  // clang-format off
  _lqp =
  PredicateNode::make(equals_(_a_a, 100),
    PredicateNode::make(equals_(_a_b, 100),
    _node_a));

  // clang-format on

  const auto expected_lqp = _lqp->deep_copy();

  _apply_rule(_rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

}  // namespace hyrise
