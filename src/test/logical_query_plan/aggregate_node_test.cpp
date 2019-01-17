#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "expression/arithmetic_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/correlated_parameter_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/evaluation/expression_result.hpp"
#include "expression/exists_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/extract_expression.hpp"
#include "expression/function_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/lqp_select_expression.hpp"
#include "expression/placeholder_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_select_expression.hpp"
#include "expression/unary_minus_expression.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class AggregateNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    _mock_node = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "t_a");

    _a = {_mock_node, ColumnID{0}};
    _b = {_mock_node, ColumnID{1}};
    _c = {_mock_node, ColumnID{2}};

    // SELECT a, c, SUM(a+b), SUM(a+c) AS some_sum [...] GROUP BY a, c
    // Columns are ordered as specified in the SELECT list
    _aggregate_node = AggregateNode::make(expression_vector(_a, _c),
                                          expression_vector(sum_(add_(_a, _b)), sum_(add_(_a, _c))), _mock_node);
  }

  std::shared_ptr<MockNode> _mock_node;
  std::shared_ptr<AggregateNode> _aggregate_node;
  LQPColumnReference _a, _b, _c;
};

TEST_F(AggregateNodeTest, OutputColumnExpressions) {
  ASSERT_EQ(_aggregate_node->column_expressions().size(), 4u);
  EXPECT_EQ(*_aggregate_node->column_expressions().at(0), *lqp_column_(_a));
  EXPECT_EQ(*_aggregate_node->column_expressions().at(1), *lqp_column_(_c));
  EXPECT_EQ(*_aggregate_node->column_expressions().at(2), *sum_(add_(_a, _b)));
  EXPECT_EQ(*_aggregate_node->column_expressions().at(3), *sum_(add_(_a, _c)));
}

TEST_F(AggregateNodeTest, NodeExpressions) {
  ASSERT_EQ(_aggregate_node->node_expressions.size(), 4u);
  EXPECT_EQ(*_aggregate_node->node_expressions.at(0), *lqp_column_(_a));
  EXPECT_EQ(*_aggregate_node->node_expressions.at(1), *lqp_column_(_c));
  EXPECT_EQ(*_aggregate_node->node_expressions.at(2), *sum_(add_(_a, _b)));
  EXPECT_EQ(*_aggregate_node->node_expressions.at(3), *sum_(add_(_a, _c)));
}

TEST_F(AggregateNodeTest, Description) {
  auto description = _aggregate_node->description();

  EXPECT_EQ(description, "[Aggregate] GroupBy: [a, c] Aggregates: [SUM(a + b), SUM(a + c)]");
}

TEST_F(AggregateNodeTest, Equals) {
  const auto same_aggregate_node = AggregateNode::make(
      expression_vector(_a, _c), expression_vector(sum_(add_(_a, _b)), sum_(add_(_a, _c))), _mock_node);

  EXPECT_EQ(*_aggregate_node, *same_aggregate_node);
  EXPECT_EQ(*same_aggregate_node, *_aggregate_node);
  EXPECT_EQ(*_aggregate_node, *_aggregate_node);

  // Build slightly different aggregate nodes
  const auto different_aggregate_node_a =
      AggregateNode::make(expression_vector(_a), expression_vector(sum_(add_(_a, _b)), sum_(add_(_a, _c))), _mock_node);
  const auto different_aggregate_node_b = AggregateNode::make(
      expression_vector(_a, _c), expression_vector(sum_(add_(_a, 2)), sum_(add_(_a, _c))), _mock_node);
  const auto different_aggregate_node_c = AggregateNode::make(
      expression_vector(_a, _c), expression_vector(sum_(add_(_a, _b)), sum_(add_(_a, _c)), min_(_a)), _mock_node);
  const auto different_aggregate_node_d = AggregateNode::make(
      expression_vector(_a, _a), expression_vector(sum_(add_(_a, _b)), sum_(add_(_a, _c))), _mock_node);

  EXPECT_NE(*_aggregate_node, *different_aggregate_node_a);
  EXPECT_NE(*_aggregate_node, *different_aggregate_node_b);
  EXPECT_NE(*_aggregate_node, *different_aggregate_node_c);
  EXPECT_NE(*_aggregate_node, *different_aggregate_node_d);
}

TEST_F(AggregateNodeTest, Copy) {
  const auto same_aggregate_node = AggregateNode::make(
      expression_vector(_a, _c), expression_vector(sum_(add_(_a, _b)), sum_(add_(_a, _c))), _mock_node);
  EXPECT_EQ(*_aggregate_node->deep_copy(), *same_aggregate_node);
}

}  // namespace opossum
