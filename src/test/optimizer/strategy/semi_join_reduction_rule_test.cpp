#include "optimizer/strategy/strategy_base_test.hpp"

#include "optimizer/strategy/semi_join_reduction_rule.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class SemiJoinReductionRuleTest : public StrategyBaseTest {
 protected:
  void SetUp() override {
    {
      const auto histogram_column_a = GenericHistogram<int32_t>::with_single_bin(1, 50, 40, 40);
      const auto histogram_column_b = GenericHistogram<int32_t>::with_single_bin(10, 15, 40, 5);
      _node_a = create_mock_node_with_statistics({{DataType::Int, "a"}, {DataType::Int, "b"}}, 40,
                                                 {histogram_column_a, histogram_column_b});
      _a_a = _node_a->get_column("a");
      _a_b = _node_a->get_column("b");
    }

    {
      const auto histogram_column_a = GenericHistogram<int32_t>::with_single_bin(10, 20, 10, 10);
      const auto histogram_column_b = GenericHistogram<int32_t>::with_single_bin(40, 60, 10, 5);
      _node_b = create_mock_node_with_statistics({{DataType::Int, "a"}, {DataType::Int, "b"}}, 10,
                                                 {histogram_column_a, histogram_column_b});
      _b_a = _node_b->get_column("a");
      _b_b = _node_b->get_column("b");
    }

    {
      const auto histogram_column_a = GenericHistogram<int32_t>::with_single_bin(10, 15, 40, 5);
      _node_c = create_mock_node_with_statistics({{DataType::Int, "a"}}, 40, {histogram_column_a});
      _c_a = _node_c->get_column("a");
    }
  }

  std::shared_ptr<MockNode> _node_a, _node_b, _node_c;
  LQPColumnReference _a_a, _a_b, _b_a, _b_b, _c_a;
  std::shared_ptr<SemiJoinReductionRule> _rule{std::make_shared<SemiJoinReductionRule>()};
};

TEST_F(SemiJoinReductionRuleTest, CreateSimpleReduction) {
  // The _a_a side of the inner join has values from 1-50, the _b_a side has values from 10-20. Based on that
  // selectivity, a semi join reduction should be created.

  // clang-format off
  const auto input_lqp = JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
    _node_a,
    _node_b);

  const auto expected_lqp = JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
    JoinNode::make(JoinMode::Semi, equals_(_a_a, _b_a),
      _node_a,
      _node_b),
    _node_b);
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SemiJoinReductionRuleTest, CreateSimpleReductionRightSide) {
  // Same as CreateSimpleReduction, but with reversed sides

  // clang-format off
  const auto input_lqp = JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
    _node_b,
    _node_a);

  const auto expected_lqp = JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
    _node_b,
    JoinNode::make(JoinMode::Semi, equals_(_a_a, _b_a),
      _node_a,
      _node_b));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SemiJoinReductionRuleTest, NoReductionForOuter) {
  // Same as CreateSimpleReduction, but as tuples without a join partner survive the left outer join, do not reduce
  // the left side.

  // clang-format off
  const auto input_lqp = JoinNode::make(JoinMode::Left, less_than_(_a_a, _b_a),
    _node_a,
    _node_b);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SemiJoinReductionRuleTest, NoReductionForNonEquals) {
  // Same as CreateSimpleReduction, but as the join condition is not an equals condition, no reduction should be
  // created. Note: As non-equi joins are currently estimated with a selectivity of 100%, this test would also pass
  // if the rule was faulty and did not categorically exclude non-equi joins but would just decide not to create a
  // reduction here because of the selectivities.

  // clang-format off
  const auto input_lqp = JoinNode::make(JoinMode::Inner, less_than_(_a_a, _b_a),
    _node_a,
    _node_b);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SemiJoinReductionRuleTest, ReductionOnlyForEquals) {
  // Similar to CreateSimpleReduction, but with one equals and one non-equals expression. Only the equals expression
  // should lead to a reduction being created.

  auto predicates = std::vector<std::shared_ptr<AbstractExpression>>{less_than_(_a_a, _b_a), equals_(_b_a, _a_a)};

  // clang-format off
  const auto input_lqp = JoinNode::make(JoinMode::Inner, predicates,
    _node_a,
    _node_b);

  const auto expected_lqp = JoinNode::make(JoinMode::Inner, predicates,
    JoinNode::make(JoinMode::Semi, equals_(_b_a, _a_a),
      _node_a,
      _node_b),
    _node_b);
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SemiJoinReductionRuleTest, NoReductionForNonBeneficial) {
  // Same as CreateSimpleReduction, but with different predicates. We estimate that a semi join would not be
  // beneficial.

  // clang-format off
  const auto input_lqp = JoinNode::make(JoinMode::Inner, equals_(_a_b, _b_a),
    _node_a,
    _node_b);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SemiJoinReductionRuleTest, TraverseRightInput) {
  // On the right side of the semi join reduction, we should traverse below joins so that the cardinality is reduced

  // clang-format off
  const auto input_lqp = JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
    _node_a,
    JoinNode::make(JoinMode::Inner, equals_(_a_b, _c_a),
      _node_b,
      _node_c));

  const auto expected_lqp = JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
    JoinNode::make(JoinMode::Semi, equals_(_a_a, _b_a),
      _node_a,
      _node_b),
    JoinNode::make(JoinMode::Inner, equals_(_a_b, _c_a),
      _node_b,
      _node_c));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SemiJoinReductionRuleTest, NoReductionForAntiJoin) {
  // Same as CreateSimpleReduction, but with an anti join that must not be touched

  // clang-format off
  const auto input_lqp = JoinNode::make(JoinMode::AntiNullAsTrue, equals_(_a_a, _b_a),
    _node_a,
    _node_b);

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
