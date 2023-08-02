#include "lib/optimizer/strategy/strategy_base_test.hpp"

#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "optimizer/strategy/data_induced_predicate_rule.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class DataInducedPredicateRuleTest : public StrategyBaseTest {
 protected:
  void SetUp() override {
    {
      const auto histogram_column_a = GenericHistogram<int32_t>::with_single_bin(1, 50, 40, 40);
      const auto histogram_column_b = GenericHistogram<int32_t>::with_single_bin(10, 15, 40, 5);
      _node_a = create_mock_node_with_statistics({{DataType::Int, "a"}, {DataType::Int, "b"}}, 40,
                                                 {histogram_column_a, histogram_column_b});
      _a_a = _node_a->get_column("a");
    }

    {
      const auto histogram_column_a = GenericHistogram<int32_t>::with_single_bin(10, 20, 10, 10);
      const auto histogram_column_b = GenericHistogram<int32_t>::with_single_bin(40, 60, 10, 5);
      _node_b = create_mock_node_with_statistics({{DataType::Int, "a"}, {DataType::Int, "b"}}, 10,
                                                 {histogram_column_a, histogram_column_b});
      _b_a = _node_b->get_column("a");
    }

    {
      const auto histogram_column_a = GenericHistogram<int32_t>::with_single_bin(1, 50, 40, 40);
      const auto histogram_column_b = GenericHistogram<int32_t>::with_single_bin(10, 15, 40, 5);
      _node_c = create_mock_node_with_statistics({{DataType::Int, "a"}, {DataType::Int, "b"}}, 40,
                                                 {histogram_column_a, histogram_column_b});
      _c_a = _node_c->get_column("a");
    }
  }

  std::shared_ptr<MockNode> _node_a, _node_b, _node_c;
  std::shared_ptr<LQPColumnExpression> _a_a, _b_a, _c_a;
  std::shared_ptr<DataInducedPredicateRule> _rule{std::make_shared<DataInducedPredicateRule>()};
};

TEST_F(DataInducedPredicateRuleTest, CreateSimpleReductionOnLeftSide) {
  // The _a_a side of the join has values from 1-50, the _b_a side has values from 10-20. Based on that
  // selectivity, a data induced predicate should be created.

  const auto join_types = std::vector<JoinMode>{JoinMode::Inner, JoinMode::Semi, JoinMode::Left, JoinMode::Right};
  for (const auto& join_type : join_types) {
    const auto input_lqp = JoinNode::make(join_type, equals_(_a_a, _b_a), _node_a, _node_b);

    const auto subquery = AggregateNode::make(expression_vector(), expression_vector(min_(_b_a), max_(_b_a)), _node_b);
    const auto min = ProjectionNode::make(expression_vector(min_(_b_a)), subquery);
    const auto max = ProjectionNode::make(expression_vector(max_(_b_a)), subquery);
    const auto expected_reduction =
        PredicateNode::make(between_inclusive_(_a_a, lqp_subquery_(min), lqp_subquery_(max)), _node_a);

    const auto expected_lqp = JoinNode::make(join_type, equals_(_a_a, _b_a), expected_reduction, _node_b);

    const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
}

TEST_F(DataInducedPredicateRuleTest, CreateSimpleReductionOnRightSide) {
  // The _b_a side of the join has values from 1-50, the _a_a side has values from 10-20. Based on that
  // selectivity, a data induced predicate should be created.
  const auto join_types =
      std::vector<JoinMode>{JoinMode::Inner,           JoinMode::Semi,          JoinMode::Left, JoinMode::Right,
                            JoinMode::AntiNullAsFalse, JoinMode::AntiNullAsTrue};
  for (const auto& join_type : join_types) {
    const auto input_lqp = JoinNode::make(join_type, equals_(_a_a, _b_a), _node_b, _node_a);

    const auto subquery = AggregateNode::make(expression_vector(), expression_vector(min_(_b_a), max_(_b_a)), _node_b);
    const auto min = ProjectionNode::make(expression_vector(min_(_b_a)), subquery);
    const auto max = ProjectionNode::make(expression_vector(max_(_b_a)), subquery);
    const auto expected_reduction =
        PredicateNode::make(between_inclusive_(_a_a, lqp_subquery_(min), lqp_subquery_(max)), _node_a);

    const auto expected_lqp = JoinNode::make(join_type, equals_(_a_a, _b_a), _node_b, expected_reduction);

    const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
}

TEST_F(DataInducedPredicateRuleTest, NoReductionForNonBeneficial) {
  // The _a_a side of the join has values from 1-50, the _c_a side has the same value range as _a_a. Based on that
  // selectivity, no data induced predicate should be created.

  const auto join_types = std::vector<JoinMode>{JoinMode::Inner, JoinMode::Semi, JoinMode::Left, JoinMode::Right};
  for (const auto& join_type : join_types) {
    const auto input_lqp = JoinNode::make(join_type, equals_(_a_a, _c_a), _node_a, _node_c);

    const auto expected_lqp = JoinNode::make(join_type, equals_(_a_a, _c_a), _node_a, _node_c);

    const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
}

}  // namespace hyrise
