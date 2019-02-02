#include "strategy_base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/mock_node.hpp"
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

    rule = std::make_shared<PredicateSplitUpRule>();
  }

  std::shared_ptr<MockNode> node_a;
  LQPColumnReference a_a, a_b;
  std::shared_ptr<PredicateSplitUpRule> rule;
};

TEST_F(PredicateSplitUpRuleTest, SplitUpConjunctionInPredicateNode) {
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

}  // namespace opossum
