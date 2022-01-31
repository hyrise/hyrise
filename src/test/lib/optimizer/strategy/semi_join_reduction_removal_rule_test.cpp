#include "lib/optimizer/strategy/strategy_base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "optimizer/strategy/semi_join_removal_rule.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class SemiJoinReductionRemovalRuleTest : public StrategyBaseTest {
  void SetUp() override {
    _node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}});
    _node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}});
    _a_a = _node_a->get_column("a");
    _a_b = _node_a->get_column("b");
    _b_a = _node_b->get_column("a");
    _b_b = _node_b->get_column("b");

    _join_mode = JoinMode::Inner;
    _join_predicate = equals_(_a_a, _b_a);
    _semi_join_reduction = JoinNode::make(JoinMode::Semi, _join_predicate, _node_a, _node_b);

    _rule = std::make_shared<SemiJoinRemovalRule>();
  }

 protected:
  JoinMode _join_mode;
  std::shared_ptr<JoinNode> _semi_join_reduction;
  std::shared_ptr<AbstractExpression> _join_predicate;
  std::shared_ptr<MockNode> _node_a, _node_b;
  std::shared_ptr<LQPColumnExpression> _a_a, _a_b, _b_a, _b_b;
  std::shared_ptr<SemiJoinRemovalRule> _rule;
};

TEST_F(SemiJoinReductionRemovalRuleTest, RemoveValidate) {
  // clang-format off
  const auto input_lqp =
  JoinNode::make(_join_mode, _join_predicate,
    ValidateNode::make(
      _semi_join_reduction),
    _node_b);

  _semi_join_reduction->mark_as_reducer_of(input_lqp);
  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  const auto expected_lqp =
  JoinNode::make(_join_mode, _join_predicate,
    ValidateNode::make(
      _node_a),
    _node_b);
  // clang-format off
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SemiJoinReductionRemovalRuleTest, RemoveBinaryPredicateValueBased) {
  const std::vector<std::shared_ptr<AbstractExpression>> predicates =
        {equals_(_a_a, 200), equals_(_a_a, cast_("200", DataType::Int)), greater_than_(_a_b, 50)};
  for (const auto& predicate : predicates) {
    auto semi_join_reduction = JoinNode::make(JoinMode::Semi, _join_predicate, _node_a, _node_b);
    // clang-format off
    const auto input_lqp =
    JoinNode::make(_join_mode, _join_predicate,
      ValidateNode::make(
        PredicateNode::make(predicate,
          semi_join_reduction)),
      _node_b);

    semi_join_reduction->mark_as_reducer_of(input_lqp);
    auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

    const auto expected_lqp =
    JoinNode::make(_join_mode, _join_predicate,
      ValidateNode::make(
        PredicateNode::make(predicate,
        _node_a)),
      _node_b);
    // clang-format off
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
}

TEST_F(SemiJoinReductionRemovalRuleTest, RemoveRedundantReduction) {
  // clang-format off
  const auto input_lqp =
  JoinNode::make(_join_mode, _join_predicate,
    _semi_join_reduction,
    _node_b);
  _semi_join_reduction->mark_as_reducer_of(input_lqp);

  const auto expected_lqp =
  JoinNode::make(_join_mode, _join_predicate,
    _node_a,
    _node_b);
  // clang-format off

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SemiJoinReductionRemovalRuleTest, BlockRemovalAggregate) {
  // clang-format off
  const auto input_lqp =
  JoinNode::make(_join_mode, _join_predicate,
    AggregateNode::make(expression_vector(_a_a), expression_vector(sum_(_a_b)),
      ValidateNode::make(
        PredicateNode::make(equals_(_a_a, 200),
          _semi_join_reduction))),
    _node_b);
  // clang-format off
  _semi_join_reduction->mark_as_reducer_of(input_lqp);

  const auto expected_lqp = input_lqp->deep_copy();
  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SemiJoinReductionRemovalRuleTest, BlockRemovalBinaryPredicateNonValue) {
  const std::vector<std::shared_ptr<AbstractExpression>> predicates =
      {equals_(_a_a, _a_b), greater_than_(_b_a, _a_a)};
  for (const auto& predicate : predicates) {
    auto semi_join_reduction = JoinNode::make(JoinMode::Semi, _join_predicate, _node_a, _node_b);
    // clang-format off
    const auto input_lqp =
    JoinNode::make(_join_mode, _join_predicate,
      ValidateNode::make(
        PredicateNode::make(predicate,
          semi_join_reduction)),
      _node_b);
    // clang-format off
    semi_join_reduction->mark_as_reducer_of(input_lqp);

    const auto expected_lqp = input_lqp->deep_copy();
    auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
}

TEST_F(SemiJoinReductionRemovalRuleTest, BlockRemovalBinaryPredicateLike) {
  const std::vector<std::shared_ptr<AbstractExpression>> predicates = {like_(_a_a, "%lol%"), not_like_(_a_b, "%lol%")};
  for (const auto& predicate : predicates) {
    auto semi_join_reduction = JoinNode::make(JoinMode::Semi, _join_predicate, _node_a, _node_b);
    // clang-format off
    const auto input_lqp =
    JoinNode::make(_join_mode, _join_predicate,
      ValidateNode::make(
        PredicateNode::make(predicate,
          semi_join_reduction)),
      _node_b);
    // clang-format off
    semi_join_reduction->mark_as_reducer_of(input_lqp);

    const auto expected_lqp = input_lqp->deep_copy();
    auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
}

TEST_F(SemiJoinReductionRemovalRuleTest, BlockRemovalOtherPredicates) {
  // TODO Add subquery expression
  const std::vector<std::shared_ptr<AbstractExpression>> predicates =
                {is_null_(_a_a), is_not_null_(_a_b), in_("test1", "test2"), between_inclusive_(_a_a, 100, 200)};
  for (const auto& predicate : predicates) {
    auto semi_join_reduction = JoinNode::make(JoinMode::Semi, _join_predicate, _node_a, _node_b);
    // clang-format off
    const auto input_lqp =
    JoinNode::make(_join_mode, _join_predicate,
      ValidateNode::make(
        PredicateNode::make(predicate,
          semi_join_reduction)),
      _node_b);
    // clang-format off
    semi_join_reduction->mark_as_reducer_of(input_lqp);

    const auto expected_lqp = input_lqp->deep_copy();
    auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
}

}  // namespace opossum
