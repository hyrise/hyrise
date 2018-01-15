#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/lqp_expression.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"

namespace opossum {

class AggregateNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _mock_node = std::make_shared<MockNode>(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "t_a");

    _a = {_mock_node, ColumnID{0}};
    _b = {_mock_node, ColumnID{1}};
    _c = {_mock_node, ColumnID{2}};

    const auto a_expr = LQPExpression::create_column(_a);
    const auto b_expr = LQPExpression::create_column(_b);
    const auto c_expr = LQPExpression::create_column(_c);

    // SELECT a, c, SUM(a+b), SUM(a+c) AS some_sum [...] GROUP BY a, c
    // Columns are ordered as specified in the SELECT list
    _aggregate_node = std::make_shared<AggregateNode>(
        std::vector<std::shared_ptr<LQPExpression>>{
            LQPExpression::create_aggregate_function(
                AggregateFunction::Sum,
                {LQPExpression::create_binary_operator(ExpressionType::Addition, a_expr, b_expr)}),
            LQPExpression::create_aggregate_function(
                AggregateFunction::Sum,
                {LQPExpression::create_binary_operator(ExpressionType::Addition, a_expr, c_expr)},
                {std::string("some_sum")})},
        std::vector<LQPColumnReference>{_a, _c});
    _aggregate_node->set_left_child(_mock_node);
  }

  std::shared_ptr<MockNode> _mock_node;
  std::shared_ptr<AggregateNode> _aggregate_node;
  LQPColumnReference _a;
  LQPColumnReference _b;
  LQPColumnReference _c;
};

TEST_F(AggregateNodeTest, ColumnReferenceByNamedColumnReference) {
  /**
   * Find GROUPBY columns
   */
  EXPECT_EQ(_aggregate_node->get_column({"a", std::nullopt}), _a);
  EXPECT_EQ(_aggregate_node->get_column({"a", {"t_a"}}), _a);
  EXPECT_EQ(_aggregate_node->find_column({"b", std::nullopt}), std::nullopt);
  EXPECT_EQ(_aggregate_node->find_column({"b", {"t_a"}}), std::nullopt);
  EXPECT_EQ(_aggregate_node->get_column({"c", std::nullopt}), _c);
  EXPECT_EQ(_aggregate_node->get_column({"c", {"t_a"}}), _c);

  /**
   * Find Aggregates
   */
  EXPECT_EQ(_aggregate_node->get_column({"some_sum", std::nullopt}), LQPColumnReference(_aggregate_node, ColumnID{3}));
  EXPECT_EQ(_aggregate_node->find_column({"some_sum", {"t_a"}}), std::nullopt);
}

TEST_F(AggregateNodeTest, OutputColumnReferences) {
  ASSERT_EQ(_aggregate_node->output_column_references().size(), 4u);
  EXPECT_EQ(_aggregate_node->output_column_references().at(0), _a);
  EXPECT_EQ(_aggregate_node->output_column_references().at(1), _c);
  EXPECT_EQ(_aggregate_node->output_column_references().at(2), LQPColumnReference(_aggregate_node, ColumnID{2}));
  EXPECT_EQ(_aggregate_node->output_column_references().at(3), LQPColumnReference(_aggregate_node, ColumnID{3}));
}

TEST_F(AggregateNodeTest, ExpressionToColumnID) {
  EXPECT_EQ(_aggregate_node->get_column_by_expression(LQPExpression::create_column(_a)), _a);
  EXPECT_EQ(_aggregate_node->find_column_by_expression(LQPExpression::create_column(_b)), std::nullopt);
  EXPECT_EQ(_aggregate_node->get_column_by_expression(LQPExpression::create_column(_c)), _c);

  // "a+b" is not allowed
  EXPECT_EQ(_aggregate_node->find_column_by_expression(LQPExpression::create_binary_operator(
                ExpressionType::Addition, LQPExpression::create_column(_a), LQPExpression::create_column(_b))),
            std::nullopt);

  // There is SUM(a+b)
  EXPECT_EQ(_aggregate_node->get_column_by_expression(LQPExpression::create_aggregate_function(
                AggregateFunction::Sum,
                {LQPExpression::create_binary_operator(ExpressionType::Addition, LQPExpression::create_column(_a),
                                                       LQPExpression::create_column(_b))})),
            LQPColumnReference(_aggregate_node, ColumnID{2}));

  // But there is no SUM(b+c)
  EXPECT_EQ(_aggregate_node->find_column_by_expression(LQPExpression::create_aggregate_function(
                AggregateFunction::Sum,
                {LQPExpression::create_binary_operator(ExpressionType::Addition, LQPExpression::create_column(_b),
                                                       LQPExpression::create_column(_c))})),
            std::nullopt);

  // TODO(mp): This expression is currently not found because the alias is missing.
  // This has to be fixed once expressions do not have an alias anymore.
  EXPECT_EQ(_aggregate_node->find_column_by_expression(LQPExpression::create_aggregate_function(
                AggregateFunction::Sum,
                {LQPExpression::create_binary_operator(ExpressionType::Addition, LQPExpression::create_column(_a),
                                                       LQPExpression::create_column(_c))})),
            std::nullopt);
}

TEST_F(AggregateNodeTest, Description) {
  auto description = _aggregate_node->description();

  EXPECT_EQ(description, "[Aggregate] SUM(t_a.a + t_a.b), SUM(t_a.a + t_a.c) AS \"some_sum\" GROUP BY [t_a.a, t_a.c]");
}

TEST_F(AggregateNodeTest, VerboseColumnNames) {
  EXPECT_EQ(_aggregate_node->get_verbose_column_name(ColumnID{0}), "t_a.a");
  EXPECT_EQ(_aggregate_node->get_verbose_column_name(ColumnID{1}), "t_a.c");
  EXPECT_EQ(_aggregate_node->get_verbose_column_name(ColumnID{2}), "SUM(t_a.a + t_a.b)");
  EXPECT_EQ(_aggregate_node->get_verbose_column_name(ColumnID{3}), "some_sum");
}

}  // namespace opossum
