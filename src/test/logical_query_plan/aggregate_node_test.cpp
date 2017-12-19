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
    _mock_node = std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "t_a");

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
        std::vector<ColumnOrigin>{_a, _c});
    _aggregate_node->set_left_child(_mock_node);
  }

  std::shared_ptr<MockNode> _mock_node;
  std::shared_ptr<AggregateNode> _aggregate_node;
  ColumnOrigin _a;
  ColumnOrigin _b;
  ColumnOrigin _c;
};

TEST_F(AggregateNodeTest, ColumnOriginByNamedColumnReference) {
//  /**
//   * Find GROUPBY columns
//   */
  EXPECT_EQ(_aggregate_node->get_column_origin_by_named_column_reference({"a", std::nullopt}), _a);
//  EXPECT_EQ(_aggregate_node->get_column_origin_by_named_column_reference({"a", {"t_a"}}), _a);
//  EXPECT_EQ(_aggregate_node->find_column_origin_by_named_column_reference({"b", std::nullopt}), std::nullopt);
//  EXPECT_EQ(_aggregate_node->find_column_origin_by_named_column_reference({"b", {"t_a"}}), std::nullopt);
//  EXPECT_EQ(_aggregate_node->get_column_origin_by_named_column_reference({"c", std::nullopt}), _c);
//  EXPECT_EQ(_aggregate_node->get_column_origin_by_named_column_reference({"c", {"t_a"}}), _c);
//
//  /**
//   * Find Aggregates
//   */
//  EXPECT_EQ(_aggregate_node->get_column_origin_by_named_column_reference({"some_sum", std::nullopt}), ColumnOrigin(_aggregate_node, ColumnID{3}));
//  EXPECT_EQ(_aggregate_node->find_column_origin_by_named_column_reference({"some_sum", {"t_a"}}), std::nullopt);
}
//
//TEST_F(AggregateNodeTest, ColumnOriginByOutputColumnID) {
//  EXPECT_EQ(_aggregate_node->get_column_origin_by_output_column_id(ColumnID{0}), _a);
//  EXPECT_EQ(_aggregate_node->get_column_origin_by_output_column_id(ColumnID{1}), _c);
//  EXPECT_EQ(_aggregate_node->get_column_origin_by_output_column_id(ColumnID{2}), ColumnOrigin(_aggregate_node, ColumnID{2}));
//  EXPECT_EQ(_aggregate_node->get_column_origin_by_output_column_id(ColumnID{3}), ColumnOrigin(_aggregate_node, ColumnID{3}));
//}
//
//TEST_F(AggregateNodeTest, OutputColumnIDByColumnOrigin) {
//  EXPECT_EQ(_aggregate_node->get_output_column_id_by_column_origin(_a), ColumnID{0});
//  EXPECT_EQ(_aggregate_node->find_output_column_id_by_column_origin(_b), std::nullopt);
//  EXPECT_EQ(_aggregate_node->get_output_column_id_by_column_origin(_c), ColumnID{1});
//
//  EXPECT_EQ(_aggregate_node->get_output_column_id_by_column_origin({_aggregate_node, ColumnID{2}}), ColumnID{2});
//  EXPECT_EQ(_aggregate_node->get_output_column_id_by_column_origin({_aggregate_node, ColumnID{3}}), ColumnID{3});
//}
//
//TEST_F(AggregateNodeTest, ExpressionToColumnID) {
//  EXPECT_EQ(_aggregate_node->get_column_origin_for_expression(LQPExpression::create_column(_a)), _a);
//  EXPECT_EQ(_aggregate_node->find_column_origin_for_expression(LQPExpression::create_column(_b)), std::nullopt);
//  EXPECT_EQ(_aggregate_node->get_column_origin_for_expression(LQPExpression::create_column(_c)), _c);
//
//  // "a+b" is not allowed
//  EXPECT_EQ(
//      _aggregate_node->find_column_origin_for_expression(LQPExpression::create_binary_operator(
//          ExpressionType::Addition, LQPExpression::create_column(_a), LQPExpression::create_column(_b))),
//      std::nullopt);
//
//  // There is SUM(a+b)
//  EXPECT_EQ(_aggregate_node->get_column_origin_for_expression(LQPExpression::create_aggregate_function(
//                AggregateFunction::Sum,
//                {LQPExpression::create_binary_operator(ExpressionType::Addition, LQPExpression::create_column(_a),
//                                                       LQPExpression::create_column(_b))})),
//            ColumnOrigin(_aggregate_node, ColumnID{2}));
//
//  // But there is no SUM(b+c)
//  EXPECT_EQ(_aggregate_node->find_column_origin_for_expression(LQPExpression::create_aggregate_function(
//                AggregateFunction::Sum,
//                {LQPExpression::create_binary_operator(ExpressionType::Addition, LQPExpression::create_column(_b),
//                                                       LQPExpression::create_column(_c))})),
//            std::nullopt);
//
//  // TODO(mp): This expression is currently not found because the alias is missing.
//  // This has to be fixed once expressions do not have an alias anymore.
//  EXPECT_EQ(_aggregate_node->find_column_origin_for_expression(LQPExpression::create_aggregate_function(
//                AggregateFunction::Sum,
//                {LQPExpression::create_binary_operator(ExpressionType::Addition, LQPExpression::create_column(_a),
//                                                       LQPExpression::create_column(_c))})),
//            std::nullopt);
//}
//TEST_F(AggregateNodeTest, AliasedSubqueryTest) {
//  _aggregate_node->set_alias("foo");
//
//  EXPECT_EQ(_aggregate_node->find_table_name_origin("foo"), _aggregate_node);
//  EXPECT_EQ(_aggregate_node->find_table_name_origin("t_a"), _mock_node);
//
//  EXPECT_EQ(_aggregate_node->get_column_origin_by_named_column_reference({"a"}), _a);
//  EXPECT_EQ(_aggregate_node->get_column_origin_by_named_column_reference({"a"}), _);
//  EXPECT_EQ(_aggregate_node->get_column_origin_by_named_column_reference({"a"}), _a);
//  EXPECT_EQ(aggregate_node_with_alias->get_column_id_by_named_column_reference({"a", {"foo"}}), ColumnID{0});
//  EXPECT_EQ(aggregate_node_with_alias->find_column_id_by_named_column_reference({"a", {"t_a"}}), std::nullopt);
//  EXPECT_EQ(aggregate_node_with_alias->get_column_id_by_named_column_reference({"some_sum", std::nullopt}),
//            ColumnID{3});
//  EXPECT_EQ(aggregate_node_with_alias->get_column_id_by_named_column_reference({"some_sum", {"foo"}}), ColumnID{3});
//  EXPECT_EQ(aggregate_node_with_alias->find_column_id_by_named_column_reference({"some_sum", {"t_a"}}), std::nullopt);
//}
//
//TEST_F(AggregateNodeTest, Description) {
//  auto description = _aggregate_node->description();
//
//  EXPECT_EQ(description, "[Aggregate] SUM(t_a.a + t_a.b), SUM(t_a.a + t_a.c) AS \"some_sum\" GROUP BY [t_a.a, t_a.c]");
//}
//
//TEST_F(AggregateNodeTest, VerboseColumnNames) {
//  EXPECT_EQ(_aggregate_node->get_verbose_column_name(ColumnID{0}), "t_a.a");
//  EXPECT_EQ(_aggregate_node->get_verbose_column_name(ColumnID{1}), "t_a.c");
//  EXPECT_EQ(_aggregate_node->get_verbose_column_name(ColumnID{2}), "SUM(t_a.a + t_a.b)");
//  EXPECT_EQ(_aggregate_node->get_verbose_column_name(ColumnID{3}), "some_sum");
//}
//
//TEST_F(AggregateNodeTest, ColumnInputMapping) {
//  auto column_ids = _aggregate_node->get_output_column_ids_for_table("t_a");
//
//  // we are grouping by two columns
//  EXPECT_EQ(column_ids.size(), 2u);
//
//  EXPECT_EQ(column_ids[0], ColumnID{0});
//  EXPECT_EQ(column_ids[1], ColumnID{1});
//}

}  // namespace opossum
