#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/show_columns_node.hpp"
#include "logical_query_plan/show_tables_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/aggregate.hpp"
#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/limit.hpp"
#include "operators/maintenance/show_columns.hpp"
#include "operators/maintenance/show_tables.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class LQPTranslatorTest : public BaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("table_int_float", load_table("src/test/tables/int_float.tbl", Chunk::MAX_SIZE));
    StorageManager::get().add_table("table_int_float2", load_table("src/test/tables/int_float2.tbl", Chunk::MAX_SIZE));
    StorageManager::get().add_table("table_alias_name",
                                    load_table("src/test/tables/table_alias_name.tbl", Chunk::MAX_SIZE));
  }
};

TEST_F(LQPTranslatorTest, StoredTableNode) {
  const auto node = std::make_shared<StoredTableNode>("table_int_float");
  const auto op = LQPTranslator{}.translate_node(node);

  const auto get_table_op = std::dynamic_pointer_cast<GetTable>(op);
  ASSERT_TRUE(get_table_op);
  EXPECT_EQ(get_table_op->table_name(), "table_int_float");
}

TEST_F(LQPTranslatorTest, PredicateNodeUnaryScan) {
  const auto stored_table_node = std::make_shared<StoredTableNode>("table_int_float");
  auto predicate_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::Equals, 42);
  predicate_node->set_left_child(stored_table_node);
  const auto op = LQPTranslator{}.translate_node(predicate_node);

  const auto table_scan_op = std::dynamic_pointer_cast<TableScan>(op);
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(table_scan_op->left_column_id(), ColumnID{0} /* "a" */);
  EXPECT_EQ(table_scan_op->scan_type(), ScanType::Equals);
  EXPECT_EQ(table_scan_op->right_parameter(), AllParameterVariant(42));
}

TEST_F(LQPTranslatorTest, PredicateNodeBinaryScan) {
  const auto stored_table_node = std::make_shared<StoredTableNode>("table_int_float");
  auto predicate_node =
      std::make_shared<PredicateNode>(ColumnID{0}, ScanType::Between, AllParameterVariant(42), AllTypeVariant(1337));
  predicate_node->set_left_child(stored_table_node);
  const auto op = LQPTranslator{}.translate_node(predicate_node);

  const auto table_scan_op2 = std::dynamic_pointer_cast<TableScan>(op);
  ASSERT_TRUE(table_scan_op2);
  EXPECT_EQ(table_scan_op2->left_column_id(), ColumnID{0} /* "a" */);
  EXPECT_EQ(table_scan_op2->scan_type(), ScanType::LessThanEquals);
  EXPECT_EQ(table_scan_op2->right_parameter(), AllParameterVariant(1337));

  const auto table_scan_op = std::dynamic_pointer_cast<const TableScan>(table_scan_op2->input_left());
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(table_scan_op->left_column_id(), ColumnID{0} /* "a" */);
  EXPECT_EQ(table_scan_op->scan_type(), ScanType::GreaterThanEquals);
  EXPECT_EQ(table_scan_op->right_parameter(), AllParameterVariant(42));
}

TEST_F(LQPTranslatorTest, ProjectionNode) {
  const auto stored_table_node = std::make_shared<StoredTableNode>("table_int_float");
  const auto expressions = std::vector<std::shared_ptr<Expression>>{Expression::create_column(ColumnID{0}, {"a"})};
  auto projection_node = std::make_shared<ProjectionNode>(expressions);
  projection_node->set_left_child(stored_table_node);
  const auto op = LQPTranslator{}.translate_node(projection_node);

  const auto projection_op = std::dynamic_pointer_cast<Projection>(op);
  ASSERT_TRUE(projection_op);
  EXPECT_EQ(projection_op->column_expressions().size(), 1u);
  EXPECT_EQ(projection_op->column_expressions()[0]->column_id(), ColumnID{0});
  EXPECT_EQ(*projection_op->column_expressions()[0]->alias(), "a");
}

TEST_F(LQPTranslatorTest, SortNode) {
  const auto stored_table_node = std::make_shared<StoredTableNode>("table_int_float");
  auto sort_node = std::make_shared<SortNode>(std::vector<OrderByDefinition>{{ColumnID{0}, OrderByMode::Ascending}});
  sort_node->set_left_child(stored_table_node);
  const auto op = LQPTranslator{}.translate_node(sort_node);

  const auto sort_op = std::dynamic_pointer_cast<Sort>(op);
  ASSERT_TRUE(sort_op);
  EXPECT_EQ(sort_op->column_id(), ColumnID{0});
  EXPECT_EQ(sort_op->order_by_mode(), OrderByMode::Ascending);
}

TEST_F(LQPTranslatorTest, JoinNode) {
  const auto stored_table_node_left = std::make_shared<StoredTableNode>("table_int_float");
  const auto stored_table_node_right = std::make_shared<StoredTableNode>("table_int_float2");
  auto join_node =
      std::make_shared<JoinNode>(JoinMode::Outer, std::make_pair(ColumnID{0}, ColumnID{0}), ScanType::Equals);
  join_node->set_left_child(stored_table_node_left);
  join_node->set_right_child(stored_table_node_right);
  const auto op = LQPTranslator{}.translate_node(join_node);

  const auto join_op = std::dynamic_pointer_cast<JoinSortMerge>(op);
  ASSERT_TRUE(join_op);
  EXPECT_EQ(join_op->column_ids(), join_node->join_column_ids());
  EXPECT_EQ(join_op->scan_type(), ScanType::Equals);
  EXPECT_EQ(join_op->mode(), JoinMode::Outer);
}

TEST_F(LQPTranslatorTest, ShowTablesNode) {
  const auto show_tables_node = std::make_shared<ShowTablesNode>();
  const auto op = LQPTranslator{}.translate_node(show_tables_node);

  const auto show_tables_op = std::dynamic_pointer_cast<ShowTables>(op);
  ASSERT_TRUE(show_tables_op);
  EXPECT_EQ(show_tables_op->name(), "ShowTables");
}

TEST_F(LQPTranslatorTest, ShowColumnsNode) {
  const auto show_column_node = std::make_shared<ShowColumnsNode>("table_a");
  const auto op = LQPTranslator{}.translate_node(show_column_node);

  const auto show_columns_op = std::dynamic_pointer_cast<ShowColumns>(op);
  ASSERT_TRUE(show_columns_op);
  EXPECT_EQ(show_columns_op->name(), "ShowColumns");
}

TEST_F(LQPTranslatorTest, AggregateNodeNoArithmetics) {
  const auto stored_table_node = std::make_shared<StoredTableNode>("table_int_float");

  auto sum_expression = Expression::create_aggregate_function(AggregateFunction::Sum,
                                                              {Expression::create_column(ColumnID{0})}, {"sum_of_a"});

  auto aggregate_node = std::make_shared<AggregateNode>(std::vector<std::shared_ptr<Expression>>{sum_expression},
                                                        std::vector<ColumnID>{});
  aggregate_node->set_left_child(stored_table_node);

  const auto op = LQPTranslator{}.translate_node(aggregate_node);

  const auto aggregate_op = std::dynamic_pointer_cast<Aggregate>(op);
  ASSERT_TRUE(aggregate_op);
  ASSERT_EQ(aggregate_op->aggregates().size(), 1u);
  EXPECT_EQ(aggregate_op->groupby_column_ids().size(), 0u);

  const auto aggregate_definition = aggregate_op->aggregates()[0];
  EXPECT_EQ(aggregate_definition.column_id, ColumnID{0});
  EXPECT_EQ(aggregate_definition.function, AggregateFunction::Sum);
  EXPECT_EQ(aggregate_definition.alias, std::optional<std::string>("sum_of_a"));
}

TEST_F(LQPTranslatorTest, AggregateNodeWithArithmetics) {
  const auto stored_table_node = std::make_shared<StoredTableNode>("table_int_float");

  // Create expression "b * 2".
  const auto expr_col_b = Expression::create_column(ColumnID{1});
  const auto expr_literal = Expression::create_literal(2);
  const auto expr_multiplication =
      Expression::create_binary_operator(ExpressionType::Multiplication, expr_col_b, expr_literal);

  // Create aggregate with expression "SUM(b * 2)".
  // TODO(tim): Projection cannot handle expression `$a + $b`
  // because it is not able to handle columns with different data types.
  // Create issue with failing test.
  auto sum_expression =
      Expression::create_aggregate_function(AggregateFunction::Sum, {expr_multiplication}, {"sum_of_b_times_two"});
  auto aggregate_node = std::make_shared<AggregateNode>(std::vector<std::shared_ptr<Expression>>{sum_expression},
                                                        std::vector<ColumnID>{ColumnID{0}});
  aggregate_node->set_left_child(stored_table_node);

  const auto op = LQPTranslator{}.translate_node(aggregate_node);

  // Check aggregate operator.
  const auto aggregate_op = std::dynamic_pointer_cast<Aggregate>(op);
  ASSERT_TRUE(aggregate_op);
  ASSERT_EQ(aggregate_op->aggregates().size(), 1u);

  ASSERT_EQ(aggregate_op->groupby_column_ids().size(), 1u);
  EXPECT_EQ(aggregate_op->groupby_column_ids()[0], ColumnID{0});

  const auto aggregate_definition = aggregate_op->aggregates()[0];
  EXPECT_EQ(aggregate_definition.column_id, ColumnID{1});
  EXPECT_EQ(aggregate_definition.function, AggregateFunction::Sum);
  EXPECT_EQ(aggregate_definition.alias, std::optional<std::string>("sum_of_b_times_two"));

  // Check projection operator.
  // The projection operator is required because we need the arithmetic operation to be calculated first.
  const auto left_op = aggregate_op->input_left();
  ASSERT_TRUE(left_op);

  const auto projection_op = std::dynamic_pointer_cast<const Projection>(left_op);
  ASSERT_TRUE(projection_op);

  const auto column_expressions = projection_op->column_expressions();
  ASSERT_EQ(column_expressions.size(), 2u);

  const auto column_expression0 = column_expressions[0];
  EXPECT_EQ(column_expression0->type(), ExpressionType::Column);
  EXPECT_EQ(column_expression0->column_id(), ColumnID{0});

  const auto column_expression1 = column_expressions[1];
  EXPECT_EQ(column_expression1->to_string(), "ColumnID #1 * 2");
  EXPECT_EQ(column_expression1->alias(), std::nullopt);
}

TEST_F(LQPTranslatorTest, MultipleNodesHierarchy) {
  const auto stored_table_node_left = std::make_shared<StoredTableNode>("table_int_float");
  auto predicate_node_left = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::Equals, AllParameterVariant(42));
  predicate_node_left->set_left_child(stored_table_node_left);

  const auto stored_table_node_right = std::make_shared<StoredTableNode>("table_int_float2");
  auto predicate_node_right =
      std::make_shared<PredicateNode>(ColumnID{1}, ScanType::GreaterThan, AllParameterVariant(30.0));
  predicate_node_right->set_left_child(stored_table_node_right);

  auto join_node =
      std::make_shared<JoinNode>(JoinMode::Inner, std::make_pair(ColumnID{0}, ColumnID{0}), ScanType::Equals);
  join_node->set_left_child(predicate_node_left);
  join_node->set_right_child(predicate_node_right);

  const auto op = LQPTranslator{}.translate_node(join_node);

  const auto join_op = std::dynamic_pointer_cast<const JoinHash>(op);
  ASSERT_TRUE(join_op);

  const auto predicate_op_left = std::dynamic_pointer_cast<const TableScan>(join_op->input_left());
  ASSERT_TRUE(predicate_op_left);
  EXPECT_EQ(predicate_op_left->scan_type(), ScanType::Equals);

  const auto predicate_op_right = std::dynamic_pointer_cast<const TableScan>(join_op->input_right());
  ASSERT_TRUE(predicate_op_right);
  EXPECT_EQ(predicate_op_right->scan_type(), ScanType::GreaterThan);

  const auto get_table_op_left = std::dynamic_pointer_cast<const GetTable>(predicate_op_left->input_left());
  ASSERT_TRUE(get_table_op_left);
  EXPECT_EQ(get_table_op_left->table_name(), "table_int_float");

  const auto get_table_op_right = std::dynamic_pointer_cast<const GetTable>(predicate_op_right->input_left());
  ASSERT_TRUE(get_table_op_right);
  EXPECT_EQ(get_table_op_right->table_name(), "table_int_float2");
}

TEST_F(LQPTranslatorTest, LimitNode) {
  const auto stored_table_node = std::make_shared<StoredTableNode>("table_int_float");

  const auto num_rows = 2u;
  auto limit_node = std::make_shared<LimitNode>(num_rows);
  limit_node->set_left_child(stored_table_node);

  const auto op = LQPTranslator{}.translate_node(limit_node);
  const auto limit_op = std::dynamic_pointer_cast<Limit>(op);
  ASSERT_TRUE(limit_op);
  EXPECT_EQ(limit_op->num_rows(), num_rows);
}

}  // namespace opossum
