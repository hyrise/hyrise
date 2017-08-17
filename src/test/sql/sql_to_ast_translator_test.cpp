#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "SQLParser.h"
#include "gtest/gtest.h"

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/aggregate_node.hpp"
#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/sort_node.hpp"
#include "sql/sql_to_ast_translator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class SQLToASTTranslatorTest : public BaseTest {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(table_a));

    std::shared_ptr<Table> table_b = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", std::move(table_b));

    std::shared_ptr<Table> table_c = load_table("src/test/tables/int_float5.tbl", 2);
    StorageManager::get().add_table("table_c", std::move(table_c));

    // TPCH
    std::shared_ptr<Table> customer = load_table("src/test/tables/tpch/customer.tbl", 1);
    StorageManager::get().add_table("customer", customer);

    std::shared_ptr<Table> orders = load_table("src/test/tables/tpch/orders.tbl", 1);
    StorageManager::get().add_table("orders", orders);

    std::shared_ptr<Table> lineitem = load_table("src/test/tables/tpch/lineitem.tbl", 1);
    StorageManager::get().add_table("lineitem", lineitem);
  }

  std::shared_ptr<AbstractASTNode> compile_query(const std::string &query) {
    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parseSQLString(query, &parse_result);

    if (!parse_result.isValid()) {
      throw std::runtime_error("Query is not valid.");
    }

    return SQLToASTTranslator::get().translate_parse_result(parse_result)[0];
  }
};

TEST_F(SQLToASTTranslatorTest, SelectStarAllTest) {
  const auto query = "SELECT * FROM table_a;";
  auto result_node = compile_query(query);

  std::vector<ColumnID> expected_columns{ColumnID{0}, ColumnID{1}};
  EXPECT_EQ(expected_columns, result_node->output_column_ids());

  EXPECT_EQ(result_node->type(), ASTNodeType::Projection);

  EXPECT_TRUE(result_node->left_child());
  EXPECT_EQ(result_node->left_child()->type(), ASTNodeType::StoredTable);

  EXPECT_FALSE(result_node->right_child());
  EXPECT_FALSE(result_node->left_child()->left_child());
}

/*
 * ExpressionNodes are able to handle this kind of expression. However, a PredicateNode needs the parsed expression as
 * input. And it does not support nested Expressions, such as '1234 + 1'.
 * This is why this test is currently not supported. It will be enabled once we are able to parse these expressions
 * in the translator.
 */
TEST_F(SQLToASTTranslatorTest, DISABLED_ExpressionTest) {
  const auto query = "SELECT * FROM table_a WHERE a = 1234 + 1";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), ASTNodeType::Projection);
  EXPECT_FALSE(result_node->right_child());

  auto ts_node_1 = result_node->left_child();
  EXPECT_EQ(ts_node_1->type(), ASTNodeType::Predicate);
  EXPECT_FALSE(ts_node_1->right_child());

  auto predicate = std::static_pointer_cast<PredicateNode>(ts_node_1)->predicate();
  EXPECT_EQ(predicate->type(), ExpressionType::Equals);
}

TEST_F(SQLToASTTranslatorTest, ExpressionStringTest) {
  const auto query = "SELECT * FROM table_a WHERE a = \"b\"";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), ASTNodeType::Projection);
  EXPECT_FALSE(result_node->right_child());

  auto ts_node_1 = result_node->left_child();
  EXPECT_EQ(ts_node_1->type(), ASTNodeType::Predicate);
  EXPECT_FALSE(ts_node_1->right_child());

  auto predicate = std::static_pointer_cast<PredicateNode>(ts_node_1)->predicate();
  EXPECT_EQ(predicate->type(), ExpressionType::Equals);
}

TEST_F(SQLToASTTranslatorTest, ExpressionStringTest2) {
  const auto query = "SELECT * FROM table_a WHERE a = 'b'";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), ASTNodeType::Projection);
  EXPECT_FALSE(result_node->right_child());

  auto ts_node_1 = result_node->left_child();
  EXPECT_EQ(ts_node_1->type(), ASTNodeType::Predicate);
  EXPECT_FALSE(ts_node_1->right_child());

  auto predicate = std::static_pointer_cast<PredicateNode>(ts_node_1)->predicate();
  EXPECT_EQ(predicate->type(), ExpressionType::Equals);
}

TEST_F(SQLToASTTranslatorTest, SelectWithAndCondition) {
  const auto query = "SELECT * FROM table_a WHERE a >= 1234 AND b < 457.9";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), ASTNodeType::Projection);
  EXPECT_FALSE(result_node->right_child());

  auto ts_node_1 = result_node->left_child();
  EXPECT_EQ(ts_node_1->type(), ASTNodeType::Predicate);
  EXPECT_FALSE(ts_node_1->right_child());

  auto ts_node_2 = ts_node_1->left_child();
  EXPECT_EQ(ts_node_2->type(), ASTNodeType::Predicate);
  EXPECT_FALSE(ts_node_2->right_child());

  auto t_node = ts_node_2->left_child();
  EXPECT_EQ(t_node->type(), ASTNodeType::StoredTable);
  EXPECT_FALSE(t_node->left_child());
  EXPECT_FALSE(t_node->right_child());
}

TEST_F(SQLToASTTranslatorTest, AggregateWithGroupBy) {
  const auto query = "SELECT a, SUM(b) AS s FROM table_a GROUP BY a;";
  const auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), ASTNodeType::Aggregate);
  EXPECT_FALSE(result_node->right_child());

  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(result_node);
  EXPECT_EQ(aggregate_node->aggregates().size(), 1u);
  const std::vector<ColumnID> groupby_columns = {ColumnID{0}};
  EXPECT_EQ(aggregate_node->groupby_columns(), groupby_columns);
  EXPECT_EQ(aggregate_node->aggregates().at(0).alias, std::string("s"));

  auto t_node_1 = result_node->left_child();
  EXPECT_EQ(t_node_1->type(), ASTNodeType::StoredTable);
  EXPECT_FALSE(t_node_1->left_child());
  EXPECT_FALSE(t_node_1->right_child());
}

TEST_F(SQLToASTTranslatorTest, AggregateWithInvalidGroupBy) {
  // Cannot select b without it being in the GROUP BY clause.
  const auto query = "SELECT b, SUM(b) AS s FROM table_a GROUP BY a;";
  EXPECT_THROW(compile_query(query), std::logic_error);
}

TEST_F(SQLToASTTranslatorTest, AggregateWithExpression) {
  const auto query = "SELECT SUM(a+b) AS s, SUM(a*b) as f FROM table_a";
  const auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), ASTNodeType::Aggregate);
  EXPECT_FALSE(result_node->right_child());

  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(result_node);
  EXPECT_EQ(aggregate_node->aggregates().size(), 2u);
  EXPECT_EQ(aggregate_node->groupby_columns().size(), 0u);
  EXPECT_EQ(aggregate_node->aggregates().at(0).alias, std::string("s"));
  EXPECT_EQ(aggregate_node->aggregates().at(1).alias, std::string("f"));

  auto t_node_1 = result_node->left_child();
  EXPECT_EQ(t_node_1->type(), ASTNodeType::StoredTable);
  EXPECT_FALSE(t_node_1->left_child());
  EXPECT_FALSE(t_node_1->right_child());
}

TEST_F(SQLToASTTranslatorTest, SelectMultipleOrderBy) {
  const auto query = "SELECT * FROM table_a ORDER BY a DESC, b ASC;";
  auto result_node = compile_query(query);

  // The first order by description is executed last (see sort operator for details).
  auto sort_node_1 = std::dynamic_pointer_cast<SortNode>(result_node);
  EXPECT_EQ(sort_node_1->type(), ASTNodeType::Sort);
  EXPECT_EQ(sort_node_1->column_id(), ColumnID{0});
  EXPECT_FALSE(sort_node_1->ascending());
  EXPECT_FALSE(sort_node_1->right_child());

  auto sort_node_2 = std::dynamic_pointer_cast<SortNode>(sort_node_1->left_child());
  EXPECT_EQ(sort_node_2->type(), ASTNodeType::Sort);
  EXPECT_EQ(sort_node_2->column_id(), ColumnID{1});
  EXPECT_TRUE(sort_node_2->ascending());
  EXPECT_FALSE(sort_node_2->right_child());
  // This node has an input node, but we don't care what kind it is in this test.
  EXPECT_TRUE(sort_node_2->left_child());
}

TEST_F(SQLToASTTranslatorTest, SelectInnerJoin) {
  const auto query = "SELECT * FROM table_a AS a INNER JOIN table_b AS b ON a.a = b.a;";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), ASTNodeType::Projection);
  auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(result_node);
  std::vector<std::string> output_columns = {"a", "b", "a", "b"};
  EXPECT_EQ(projection_node->output_column_ids().size(), 4u);
  EXPECT_EQ(projection_node->output_column_names(), output_columns);

  EXPECT_EQ(result_node->left_child()->type(), ASTNodeType::Join);
  auto join_node = std::dynamic_pointer_cast<JoinNode>(result_node->left_child());
  EXPECT_EQ(join_node->scan_type(), ScanType::OpEquals);
  EXPECT_EQ(join_node->join_mode(), JoinMode::Inner);
  EXPECT_EQ(join_node->join_column_ids()->first, ColumnID{0});
  EXPECT_EQ(join_node->join_column_ids()->second, ColumnID{0});
}

}  // namespace opossum
