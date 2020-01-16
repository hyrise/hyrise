#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "utils/assert.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "optimizer/strategy/between_composition_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "statistics/table_statistics.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class BetweenCompositionTest : public StrategyBaseTest {
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

  std::shared_ptr<MockNode> _node_a, _node_b;
  LQPColumnReference _a_a, _a_b, _a_c, _b_a;
  std::shared_ptr<BetweenCompositionRule> _rule;
};

TEST_F(BetweenCompositionTest, ColumnExpressionLeft) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_equals_(_a_a, 200),
    PredicateNode::make(less_than_equals_(_a_a, 300),
      _node_a));

  const auto expected_lqp =
  PredicateNode::make(between_inclusive_(_a_a, 200, 300),
    _node_a);
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, ColumnExpressionRight) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(less_than_equals_(200, _a_a),
    PredicateNode::make(greater_than_equals_(300, _a_a),
      _node_a));

  const auto expected_lqp =
  PredicateNode::make(between_inclusive_(_a_a, 200, 300),
    _node_a);
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, NoColumnRange) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(less_than_equals_(_a_b, 300),
    PredicateNode::make(greater_than_equals_(_a_a, 200),
      _node_a));

  const auto expected_lqp =
  PredicateNode::make(less_than_equals_(_a_b, 300),
    PredicateNode::make(greater_than_equals_(_a_a, 200),
      _node_a));
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, EmptyColumnRange) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_equals_(_a_a, 300),
    PredicateNode::make(less_than_equals_(_a_a, 200),
      _node_a));

  const auto expected_lqp =
  PredicateNode::make(between_inclusive_(_a_a, 300, 200),
    _node_a);
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, NoPullPastOrExpression) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(or_(greater_than_equals_(_a_a, 200), less_than_equals_(_a_a, 300)),
    _node_a);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, PredicateChain) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_equals_(_a_a, 200),
    PredicateNode::make(less_than_equals_(_a_a, 300),
      _node_a));

  const auto expected_lqp =
  PredicateNode::make(between_inclusive_(_a_a, 200, 300),
    _node_a);
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, LongPredicateChain) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_equals_(_a_a, 200),
    PredicateNode::make(less_than_equals_(_a_b, 300),
      PredicateNode::make(less_than_equals_(_a_a, 300),
        _node_a)));

  const auto expected_lqp =
  PredicateNode::make(less_than_equals_(_a_b, 300),
    PredicateNode::make(between_inclusive_(_a_a, 200, 300),
      _node_a));
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, LeftExclusive) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(_a_a, 200),
    PredicateNode::make(less_than_equals_(_a_a, 300),
      _node_a));

  const auto expected_lqp =
  PredicateNode::make(between_lower_exclusive_(_a_a, 200, 300),
    _node_a);
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, RightExclusive) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_equals_(_a_a, 200),
    PredicateNode::make(less_than_(_a_a, 300),
      _node_a));

  const auto expected_lqp =
  PredicateNode::make(between_upper_exclusive_(_a_a, 200, 300),
    _node_a);
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, BothExclusive) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(_a_a, 200),
    PredicateNode::make(less_than_(_a_a, 300),
      _node_a));

  const auto expected_lqp =
  PredicateNode::make(between_exclusive_(_a_a, 200, 300),
    _node_a);
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, TwoChains) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(_a_a, 200),
    PredicateNode::make(less_than_(_a_a, 300),
      ValidateNode::make(
        PredicateNode::make(greater_than_(_a_b, 500),
          PredicateNode::make(less_than_(_a_b, 600),
            _node_a)))));

  const auto expected_lqp =
  PredicateNode::make(between_exclusive_(_a_a, 200, 300),
      ValidateNode::make(
        PredicateNode::make(between_exclusive_(_a_b, 500, 600),
          _node_a)));
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, NoPullPastAggregate) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_equals_(_a_a, 200),
    AggregateNode::make(expression_vector(_a_a), expression_vector(),
      PredicateNode::make(less_than_equals_(_a_a, 300),
        _node_a)));

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, NoPullPastJoin) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(equals_(_a_a, _b_a),
    PredicateNode::make(greater_than_equals_(_a_a, 200),
      JoinNode::make(JoinMode::Cross,
        PredicateNode::make(less_than_equals_(_a_a, 300),
          _node_a),
        _node_b)));

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, TwoColumnsNoMatch) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(less_than_equals_(_a_a, _a_b),
    _node_a);

  const auto expected_lqp =
  PredicateNode::make(less_than_equals_(_a_a, _a_b),
    _node_a);
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, FindOptimalInclusiveBetween) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_equals_(_a_a, 100),
    PredicateNode::make(greater_than_equals_(_a_a, 200),
      PredicateNode::make(less_than_equals_(_a_a, 400),
        PredicateNode::make(less_than_equals_(_a_a, 300),
          _node_a))));

  const auto expected_lqp =
  PredicateNode::make(between_inclusive_(_a_a, 200, 300),
   _node_a);
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, FindOptimalExclusiveBetween) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(_a_a, 100),
    PredicateNode::make(greater_than_(_a_a, 200),
      PredicateNode::make(less_than_(_a_a, 400),
        PredicateNode::make(less_than_(_a_a, 300),
          _node_a))));

  const auto expected_lqp =
  PredicateNode::make(between_exclusive_(_a_a, 200, 300),
    _node_a);
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, KeepRemainingPredicates) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_equals_(_a_a, 200),
    PredicateNode::make(less_than_equals_(_a_a, 300),
      PredicateNode::make(less_than_equals_(_a_b, 300),
        _node_a)));

  const auto expected_lqp =
  PredicateNode::make(less_than_equals_(_a_b, 300),
    PredicateNode::make(between_inclusive_(_a_a, 200, 300),
      _node_a));
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, MultipleBetweensVariousLocations) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_equals_(_a_a, 200),
    PredicateNode::make(less_than_equals_(_a_b, 200),
      PredicateNode::make(greater_than_equals_(_a_a, 230),
        PredicateNode::make(less_than_equals_(_a_c, 200),
          PredicateNode::make(less_than_equals_(_a_a, 250),
              PredicateNode::make(less_than_equals_(_a_a, 300),
                PredicateNode::make(greater_than_equals_(_a_b, 150),
                  _node_a)))))));

  const auto expected_lqp =
  PredicateNode::make(less_than_equals_(_a_c, 200),
    PredicateNode::make(between_inclusive_(_a_a, 230, 250),
      PredicateNode::make(between_inclusive_(_a_b, 150, 200),
        _node_a)));
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, NonBoundaryPredicate) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(_a_a, 100),
    PredicateNode::make(in_(_a_a, list_(1, 2, 3)),
      PredicateNode::make(less_than_(_a_a, 200),
        _node_a)));

  const auto expected_lqp =
  PredicateNode::make(in_(_a_a, list_(1, 2, 3)),
    PredicateNode::make(between_exclusive_(_a_a, 100, 200),
      _node_a));
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, NoPullPastDiamondPredicate) {
  // clang-format off
  const auto predicate_node =
  PredicateNode::make(greater_than_equals_(_a_a, 200),
                      _node_a);

  const auto input_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(greater_than_equals_(_a_a, 300),
      predicate_node),
    PredicateNode::make(greater_than_equals_(_a_a, 400),
      predicate_node));

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, HandleMultipleEqualExpressions) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(equals_(_a_a, 100),
    PredicateNode::make(equals_(_a_b, 100),
    _node_a));

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

}  // namespace opossum
