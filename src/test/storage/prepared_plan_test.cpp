#include "gtest/gtest.h"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "storage/prepared_plan.hpp"
#include "testing_assert.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class PreparedPlanTest : public ::testing::Test {
 public:
  void SetUp() override {
    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}});
    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "x"}, {DataType::Int, "y"}});

    a_a = node_a->get_column("a");
    b_x = node_b->get_column("x");
  }

  std::shared_ptr<MockNode> node_a, node_b;
  LQPColumnReference a_a, b_x;
};

TEST_F(PreparedPlanTest, Instantiate) {
  // clang-format off
  const auto placeholder_parameter_a = placeholder_(ParameterID{0});
  const auto placeholder_parameter_b = placeholder_(ParameterID{2});
  const auto correlated_parameter = correlated_parameter_(ParameterID{1}, a_a);

  const auto subselect_a_lqp = PredicateNode::make(equals_(b_x, placeholder_parameter_a), node_b);
  const auto subselect_a = lqp_select_(subselect_a_lqp);

  const auto subselect_b_lqp =
  PredicateNode::make(greater_than_(subselect_a, correlated_parameter),
    DummyTableNode::make());
  const auto subselect_b = lqp_select_(subselect_b_lqp, std::make_pair(ParameterID{1}, a_a));

  const auto lqp =
  PredicateNode::make(greater_than_(a_a, placeholder_parameter_b),
    PredicateNode::make(less_than_(subselect_b, 4),
      node_a));
  // clang-format on

  const auto prepared_plan = PreparedPlan{lqp, {ParameterID{0}, ParameterID{2}}};
  const auto actual_lqp = prepared_plan.instantiate({value_(15), add_(42, 1337)});

  // clang-format off
  const auto expected_subselect_a_lqp = PredicateNode::make(equals_(b_x, 15), node_b);
  const auto expected_subselect_a = lqp_select_(expected_subselect_a_lqp);
  const auto expected_subselect_b_lqp =
  PredicateNode::make(greater_than_(expected_subselect_a, correlated_parameter),
    DummyTableNode::make());
  const auto expected_subselect_b = lqp_select_(expected_subselect_b_lqp , std::make_pair(ParameterID{1}, a_a));

  const auto expected_lqp =
  PredicateNode::make(greater_than_(a_a, add_(42, 1337)),
    PredicateNode::make(less_than_(expected_subselect_b, 4),
      node_a));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
