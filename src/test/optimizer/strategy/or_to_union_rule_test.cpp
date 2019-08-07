#include "gtest/gtest.h"

#include "strategy_base_test.hpp"
#include "testing_assert.hpp"

//#include "expression/expression_functional.hpp"
//#include "expression/lqp_column_expression.hpp"
//#include "logical_query_plan/abstract_lqp_node.hpp"
//#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/union_node.hpp"
//#include "logical_query_plan/limit_node.hpp"
//#include "logical_query_plan/mock_node.hpp"
//#include "logical_query_plan/predicate_node.hpp"
//#include "logical_query_plan/projection_node.hpp"
//#include "logical_query_plan/sort_node.hpp"
//#include "logical_query_plan/stored_table_node.hpp"
//#include "logical_query_plan/union_node.hpp"
//#include "logical_query_plan/validate_node.hpp"
#include "optimizer/strategy/or_to_union_rule.hpp"

namespace opossum {

  class OrToUnionRuleTest : public StrategyBaseTest {
  public:
    void SetUp() override {
      node_a = MockNode::make(
      MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "a");
      a_a = node_a->get_column("a");
      a_b = node_a->get_column("b");
      a_c = node_a->get_column("c");

      node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}}, "b");
      b_a = node_b->get_column("a");
      b_b = node_b->get_column("b");

      node_c = MockNode::make(
      MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "c");
      c_a = node_c->get_column("a");
      c_b = node_c->get_column("b");

      _rule = std::make_shared<OrToUnionRule>();
    }

    std::shared_ptr<OrToUnionRule> _rule;

    std::shared_ptr<MockNode> node_a, node_b, node_c;
    LQPColumnReference a_a, a_b, a_c, b_a, b_b, c_a, c_b;
  };

TEST_F(OrToUnionRuleTest, TwoExistsToUnion) {
  // select * from nation where exists (select * from customer where c_nationkey = n_nationkey) or exists (select * from supplier where s_nationkey = n_nationkey);
  // SELECT * FROM a WHERE EXISTS (SELECT * FROM b WHERE b.b = a.b) OR EXISTS (SELECT * FROM c WHERE c.b = a.b)

  const auto parameter = correlated_parameter_(ParameterID{0}, a_a);

  // clang-format off
  const auto subquery_lqp_a =
  PredicateNode::make(equals_(b_b, parameter),
    node_b);

  const auto subquery_a = lqp_subquery_(subquery_lqp_a, std::make_pair(ParameterID{0}, a_b));

  const auto subquery_lqp_b =
  PredicateNode::make(equals_(c_b, parameter),
    node_c);

  const auto subquery_b = lqp_subquery_(subquery_lqp_b, std::make_pair(ParameterID{0}, a_b));

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

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  std::cout << "INPUT\n" << *input_lqp << "\n\n";
  std::cout << "ACTUAL\n" << *actual_lqp << "\n\n";
  std::cout << "EXPECTED\n" << *expected_lqp << "\n\n";

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
