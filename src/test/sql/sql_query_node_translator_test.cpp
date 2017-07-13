#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "SQLParser.h"
#include "gtest/gtest.h"

#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/sort_node.hpp"
#include "optimizer/abstract_syntax_tree/table_node.hpp"
#include "optimizer/abstract_syntax_tree/table_scan_node.hpp"
#include "sql/sql_query_node_translator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class SQLQueryNodeTranslatorTest : public BaseTest {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(table_a));

    std::shared_ptr<Table> table_b = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", std::move(table_b));
  }

  std::shared_ptr<AbstractNode> compile_query(const std::string query) {
    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parseSQLString(query, &parse_result);

    if (!parse_result.isValid()) {
      throw std::runtime_error("Query is not valid.");
    }

    return _translator.translate_parse_result(parse_result)[0];
  }

  SQLQueryNodeTranslator _translator;
};

TEST_F(SQLQueryNodeTranslatorTest, BasicSuccessTest) {
  const auto query = "SELECT * FROM table_a;";
  compile_query(query);

  const auto faulty_query = "SELECT * WHERE test;";
  EXPECT_THROW(compile_query(faulty_query), std::runtime_error);
}

TEST_F(SQLQueryNodeTranslatorTest, SelectStarAllTest) {
  const auto query = "SELECT * FROM table_a;";
  auto result_node = compile_query(query);

  std::vector<std::string> expected_columns{"a", "b"};
  EXPECT_EQ(expected_columns, result_node->output_columns());

  EXPECT_FALSE(result_node->right());
  EXPECT_FALSE(result_node->left()->left());
}

TEST_F(SQLQueryNodeTranslatorTest, SelectWithAndCondition) {
  const auto query = "SELECT * FROM table_a WHERE a >= 1234 AND b < 457.9";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), NodeType::Projection);
  EXPECT_FALSE(result_node->right());

  auto ts_node_1 = result_node->left();
  EXPECT_EQ(ts_node_1->type(), NodeType::TableScan);
  EXPECT_FALSE(ts_node_1->right());

  auto ts_node_2 = ts_node_1->left();
  EXPECT_EQ(ts_node_2->type(), NodeType::TableScan);
  EXPECT_FALSE(ts_node_2->right());

  auto t_node = ts_node_2->left();
  EXPECT_EQ(t_node->type(), NodeType::Table);
  EXPECT_FALSE(t_node->left());
  EXPECT_FALSE(t_node->right());
}

TEST_F(SQLQueryNodeTranslatorTest, SelectMultipleOrderBy) {
  const auto query = "SELECT * FROM table_a ORDER BY a DESC, b ASC;";
  auto result_node = compile_query(query);

  // The first order by description is executed last (see sort operator for details).
  auto sort_node_1 = std::dynamic_pointer_cast<SortNode>(result_node);
  EXPECT_EQ(sort_node_1->type(), NodeType::Sort);
  EXPECT_EQ(sort_node_1->column_name(), "a");
  EXPECT_FALSE(sort_node_1->asc());
  EXPECT_FALSE(sort_node_1->right());

  auto sort_node_2 = std::dynamic_pointer_cast<SortNode>(sort_node_1->left());
  EXPECT_EQ(sort_node_2->type(), NodeType::Sort);
  EXPECT_EQ(sort_node_2->column_name(), "b");
  EXPECT_TRUE(sort_node_2->asc());
  EXPECT_FALSE(sort_node_2->right());
  // This node has an input node, but we don't care what kind it is in this test.
  EXPECT_TRUE(sort_node_2->left());
}

TEST_F(SQLQueryNodeTranslatorTest, SelectInnerJoin) {
  const auto query = "SELECT * FROM table_a AS a INNER JOIN table_b AS b ON a.a = b.a;";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), NodeType::Projection);
  auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(result_node);
  std::vector<std::string> output_columns = {"a.a", "a.b", "b.a", "b.b"};
  EXPECT_EQ(projection_node->output_columns(), output_columns);

  EXPECT_EQ(result_node->left()->type(), NodeType::Join);
  auto join_node = std::dynamic_pointer_cast<JoinNode>(result_node->left());
  EXPECT_EQ(join_node->scan_type(), ScanType::OpEquals);
  EXPECT_EQ(join_node->join_mode(), JoinMode::Inner);
  EXPECT_EQ(join_node->prefix_left(), "a.");
  EXPECT_EQ(join_node->prefix_right(), "b.");
  EXPECT_EQ(join_node->join_column_names()->first, "a");
  EXPECT_EQ(join_node->join_column_names()->second, "a");
}

}  // namespace opossum
