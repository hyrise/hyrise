#include "strategy_base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class PredicateSplitUpRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}});
    a_a = node_a->get_column("a");
    a_b = node_a->get_column("b");
  }

  std::shared_ptr<MockNode> node_a;
  LQPColumnReference a_a, a_b;
};

TEST_F(PredicateSplitUpRuleTest, SplitUpConjunctionInPredicateNode) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(and_(equals_(a_a, 5), greater_than(a_b, 7),
    PredicateNode::make(equals_(13, 13)),
      ProjectionNode::make(expression_vector(a_b, a_a),
        PredicateNode::make(greater_than(a_a, 3),
          node_a))));

  const auto expected_lqp =
  PredicateNode::make(equals_(a_a, 5),
  PredicateNode::make(greater_than(a_b, 7),
    PredicateNode::make(equals_(13, 13)),
      ProjectionNode::make(expression_vector(a_b, a_a),
        PredicateNode::make(equals_(a_a, a_b),
        PredicateNode::make(and_(equals_(a_a, a_b), greater_than(a_a, 3),
          node_a))));
  // clang-format on

}


}  // namespace opossum