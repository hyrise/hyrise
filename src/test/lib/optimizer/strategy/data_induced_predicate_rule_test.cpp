#include "lib/optimizer/strategy/strategy_base_test.hpp"

#include "logical_query_plan/join_node.hpp"
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
  std::shared_ptr<LQPColumnExpression> _a_a, _a_b, _b_a, _b_b, _c_a;
  std::shared_ptr<DataInducedPredicateRule> _rule{std::make_shared<DataInducedPredicateRule>()};
};

TEST_F(DataInducedPredicateRuleTest, CreateSimpleReduction) {
  // The _a_a side of the inner join has values from 1-50, the _b_a side has values from 10-20. Based on that
  // selectivity, a semi join reduction should be created.

  // clang-format off
  const auto input_lqp =
  JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
    _node_a,
    _node_b);
  // TODO (team): Change me
  const auto expected_reduction =
  JoinNode::make(JoinMode::Semi, equals_(_a_a, _b_a),
    _node_a,
    _node_b);
  // TODO (team): Change me
  const auto expected_lqp =
  JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
    expected_reduction,
    _node_b);
  // clang-format on
  expected_reduction->mark_as_semi_reduction(expected_lqp);

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);

  // TODO (team): Change me
  // Check whether the added semi join was also marked as a semi reduction.
  auto join_node = std::static_pointer_cast<JoinNode>(actual_lqp->left_input());
  EXPECT_TRUE(join_node->is_semi_reduction());
  EXPECT_EQ(join_node->comment, _rule->name());
  EXPECT_EQ(join_node->get_or_find_reduced_join_node(), std::static_pointer_cast<JoinNode>(actual_lqp));
}

}  // namespace hyrise
