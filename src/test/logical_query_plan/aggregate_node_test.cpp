#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "expression/expression_factory.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"

using namespace opossum::expression_factory;

namespace opossum {

class AggregateNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    _mock_node = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "t_a");

    _a = {_mock_node, ColumnID{0}};
    _b = {_mock_node, ColumnID{1}};
    _c = {_mock_node, ColumnID{2}};

    // SELECT a, c, SUM(a+b), SUM(a+c) AS some_sum [...] GROUP BY a, c
    // Columns are ordered as specified in the SELECT list
    _aggregate_node = AggregateNode::make(expression_vector(_a, _c), expression_vector(sum(add(_a, _b)), sum(add(_a, _c))), _mock_node);
  }

  std::shared_ptr<MockNode> _mock_node;
  std::shared_ptr<AggregateNode> _aggregate_node;
  LQPColumnReference _a, _b, _c;
};

TEST_F(AggregateNodeTest, OutputColumnExpressions) {
  ASSERT_EQ(_aggregate_node->output_column_expressions().size(), 4u);
  EXPECT_TRUE(_aggregate_node->output_column_expressions().at(0)->deep_equals(*column(_a)));
  EXPECT_TRUE(_aggregate_node->output_column_expressions().at(1)->deep_equals(*column(_c)));
  EXPECT_TRUE(_aggregate_node->output_column_expressions().at(2)->deep_equals(*sum(add(_a, _b))));
  EXPECT_TRUE(_aggregate_node->output_column_expressions().at(3)->deep_equals(*sum(add(_a, _c))));
}

TEST_F(AggregateNodeTest, Description) {
  auto description = _aggregate_node->description();

  EXPECT_EQ(description, "[Aggregate] GroupBy: [a, c] Aggregates: [SUM(a + b), SUM(a + c)]");
}

TEST_F(AggregateNodeTest, Equals) {
  const auto same_aggregate_node = AggregateNode::make(expression_vector(_a, _c), expression_vector(sum(add(_a, _b)), sum(add(_a, _c))), _mock_node);

  EXPECT_TRUE(!lqp_find_subplan_mismatch(_aggregate_node, same_aggregate_node));
  EXPECT_TRUE(!lqp_find_subplan_mismatch(same_aggregate_node, _aggregate_node));
  EXPECT_TRUE(!lqp_find_subplan_mismatch(_aggregate_node, _aggregate_node));

  // Build slightly different aggregate nodes
  const auto different_aggregate_node_a = AggregateNode::make(expression_vector(_a), expression_vector(sum(add(_a, _b)), sum(add(_a, _c))), _mock_node);
  const auto different_aggregate_node_b = AggregateNode::make(expression_vector(_a, _c), expression_vector(sum(add(_a, 2)), sum(add(_a, _c))), _mock_node);
  const auto different_aggregate_node_c = AggregateNode::make(expression_vector(_a, _c), expression_vector(sum(add(_a, _b)), sum(add(_a, _c)), min(_a)), _mock_node);
  const auto different_aggregate_node_d = AggregateNode::make(expression_vector(_a, _a), expression_vector(sum(add(_a, _b)), sum(add(_a, _c))), _mock_node);

  EXPECT_TRUE(lqp_find_subplan_mismatch(_aggregate_node, different_aggregate_node_a).has_value());
  EXPECT_TRUE(lqp_find_subplan_mismatch(_aggregate_node, different_aggregate_node_b).has_value());
  EXPECT_TRUE(lqp_find_subplan_mismatch(_aggregate_node, different_aggregate_node_c).has_value());
  EXPECT_TRUE(lqp_find_subplan_mismatch(_aggregate_node, different_aggregate_node_d).has_value());
}

TEST_F(AggregateNodeTest, Copy) {
  const auto same_aggregate_node = AggregateNode::make(expression_vector(_a, _c), expression_vector(sum(add(_a, _b)), sum(add(_a, _c))), _mock_node);
  EXPECT_TRUE(!lqp_find_subplan_mismatch(_aggregate_node->deep_copy(), same_aggregate_node));
}

}  // namespace opossum
