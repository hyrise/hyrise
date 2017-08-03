#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "operators/aggregate.hpp"
#include "operators/get_table.hpp"
#include "operators/join_nested_loop_a.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "optimizer/abstract_syntax_tree/aggregate_node.hpp"
#include "optimizer/abstract_syntax_tree/ast_to_operator_translator.hpp"
#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/sort_node.hpp"
#include "optimizer/abstract_syntax_tree/stored_table_node.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class ASTToOperatorTranslatorTest : public BaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("table_int_float", load_table("src/test/tables/int_float.tbl", 2));
    StorageManager::get().add_table("table_int_float2", load_table("src/test/tables/int_float2.tbl", 2));
  }

  void TearDown() override {
    StorageManager::get().reset();
  }
};

TEST_F(ASTToOperatorTranslatorTest, StoredTableNode) {
  const auto node = std::make_shared<StoredTableNode>("table_int_float");
  const auto op = ASTToOperatorTranslator::get().translate_node(node);

  const auto get_table_op = std::dynamic_pointer_cast<GetTable>(op);
  ASSERT_TRUE(get_table_op);
  EXPECT_EQ(get_table_op->table_name(), "table_int_float");
}

TEST_F(ASTToOperatorTranslatorTest, PredicateNodeUnaryScan) {
  const auto stored_table_node = std::make_shared<StoredTableNode>("table_int_float");
  auto predicate_node = std::make_shared<PredicateNode>("a", nullptr, ScanType::OpEquals, 42);
  predicate_node->set_left_child(stored_table_node);
  const auto op = ASTToOperatorTranslator::get().translate_node(predicate_node);

  const auto table_scan_op = std::dynamic_pointer_cast<TableScan>(op);
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(table_scan_op->column_name(), "a");
  EXPECT_EQ(table_scan_op->scan_type(), ScanType::OpEquals);
  EXPECT_EQ(table_scan_op->value(), AllParameterVariant(42));
  EXPECT_FALSE(table_scan_op->value2());
}

TEST_F(ASTToOperatorTranslatorTest, PredicateNodeBinaryScan) {
  const auto stored_table_node = std::make_shared<StoredTableNode>("table_int_float");
  auto predicate_node = std::make_shared<PredicateNode>("a", nullptr, ScanType::OpBetween, 42, 1337);
  predicate_node->set_left_child(stored_table_node);
  const auto op = ASTToOperatorTranslator::get().translate_node(predicate_node);

  const auto table_scan_op = std::dynamic_pointer_cast<TableScan>(op);
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(table_scan_op->column_name(), "a");
  EXPECT_EQ(table_scan_op->scan_type(), ScanType::OpBetween);
  EXPECT_EQ(table_scan_op->value(), AllParameterVariant(42));
  EXPECT_EQ(table_scan_op->value2(), AllTypeVariant(1337));
}

TEST_F(ASTToOperatorTranslatorTest, ProjectionNode) {
  const auto stored_table_node = std::make_shared<StoredTableNode>("table_int_float");
  auto projection_node = std::make_shared<ProjectionNode>(std::vector<std::string>{"a"});
  projection_node->set_left_child(stored_table_node);
  const auto op = ASTToOperatorTranslator::get().translate_node(projection_node);

  const auto projection_op = std::dynamic_pointer_cast<Projection>(op);
  ASSERT_TRUE(projection_op);
  EXPECT_EQ(projection_op->simple_projection(), std::vector<std::string>{"a"});
}

TEST_F(ASTToOperatorTranslatorTest, SortNode) {
  const auto stored_table_node = std::make_shared<StoredTableNode>("table_int_float");
  auto sort_node = std::make_shared<SortNode>("a", true);
  sort_node->set_left_child(stored_table_node);
  const auto op = ASTToOperatorTranslator::get().translate_node(sort_node);

  const auto sort_op = std::dynamic_pointer_cast<Sort>(op);
  ASSERT_TRUE(sort_op);
  EXPECT_EQ(sort_op->sort_column_name(), "a");
  EXPECT_EQ(sort_op->ascending(), true);
}

TEST_F(ASTToOperatorTranslatorTest, JoinNode) {
  const auto stored_table_node_left = std::make_shared<StoredTableNode>("table_int_float");
  const auto stored_table_node_right = std::make_shared<StoredTableNode>("table_int_float2");
  auto join_node = std::make_shared<JoinNode>(std::make_pair("a", "a"), ScanType::OpEquals, JoinMode::Outer,
    "alpha.", "beta.");
  join_node->set_left_child(stored_table_node_left);
  join_node->set_right_child(stored_table_node_right);
  const auto op = ASTToOperatorTranslator::get().translate_node(join_node);

  const auto join_op = std::dynamic_pointer_cast<JoinNestedLoopA>(op);
  ASSERT_TRUE(join_op);
  EXPECT_EQ(join_op->column_names(), join_node->join_column_names());
  EXPECT_EQ(join_op->scan_type(), ScanType::OpEquals);
  EXPECT_EQ(join_op->mode(), JoinMode::Outer);
  EXPECT_EQ(join_op->prefix_left(), "alpha.");
  EXPECT_EQ(join_op->prefix_right(), "beta.");
}

TEST_F(ASTToOperatorTranslatorTest, AggregateNodeNoArithmetics) {
  const auto stored_table_node = std::make_shared<StoredTableNode>("table_int_float");

  auto sum_expression = ExpressionNode::create_function_reference(
    "SUM",
    {ExpressionNode::create_column_reference("table_int_float", "a", "")},
    {});
  auto aggregate_node = std::make_shared<AggregateNode>(
    std::vector<AggregateColumnDefinition>{AggregateColumnDefinition{sum_expression, "sum_of_a"}},
    std::vector<std::string>{});
  aggregate_node->set_left_child(stored_table_node);

  const auto op = ASTToOperatorTranslator::get().translate_node(aggregate_node);

  const auto aggregate_op = std::dynamic_pointer_cast<Aggregate>(op);
  ASSERT_TRUE(aggregate_op);
  ASSERT_EQ(aggregate_op->aggregates().size(), 1u);

  const auto aggregate_definition = aggregate_op->aggregates()[0];
  EXPECT_EQ(aggregate_definition.column_name, "a");
  EXPECT_EQ(aggregate_definition.function, AggregateFunction::Sum);
  EXPECT_EQ(aggregate_definition.alias, optional<std::string>("sum_of_a"));
}

//TEST_F(ASTToOperatorTranslatorTest, SelectStarAllTest) {
//  const auto query = "SELECT * FROM table_a;";
//  const auto expected_result = load_table("src/test/tables/int_float.tbl", 2);
//  execute_and_check(query, expected_result);
//}
//
//TEST_F(ASTToOperatorTranslatorTest, SelectWithAndCondition) {
//  const auto query = "SELECT * FROM table_b WHERE a = 12345 AND b > 457;";
//  const auto expected_result = load_table("src/test/tables/int_float2_filtered.tbl", 2);
//  execute_and_check(query, expected_result);
//}
//
//TEST_F(ASTToOperatorTranslatorTest, SelectWithOrderByDesc) {
//  const auto query = "SELECT * FROM table_a ORDER BY a DESC;";
//  const auto expected_result = load_table("src/test/tables/int_float_reverse.tbl", 2);
//  execute_and_check(query, expected_result, true);
//}
//
//TEST_F(ASTToOperatorTranslatorTest, SelectWithMultipleOrderByColumns) {
//  const auto query = "SELECT * FROM table_b ORDER BY a, b ASC;";
//  const auto expected_result = load_table("src/test/tables/int_float2_sorted.tbl", 2);
//  execute_and_check(query, expected_result, true);
//}
//
//TEST_F(ASTToOperatorTranslatorTest, SelectInnerJoin) {
//  const auto query = "SELECT * FROM table_a AS \"left\" INNER JOIN table_b AS \"right\" ON \"left\".a = \"right\".a;";
//  const auto expected_result = load_table("src/test/tables/joinoperators/int_inner_join.tbl", 2);
//  execute_and_check(query, expected_result);
//}
//
//// TODO(tim): Projection cannot handle expression `$a + $b`
//// because it is not able to handle columns with different data types.
//// Create issue with failing test.
//TEST_F(ASTToOperatorTranslatorTest, SelectWithAggregate) {
//  const auto query = "SELECT SUM(b + b) AS sum_b_b FROM table_a;";
//  const auto expected_result = load_table("src/test/tables/int_float_sum_b_plus_b.tbl", 2);
//  execute_and_check(query, expected_result, true);
//}

}  // namespace opossum
