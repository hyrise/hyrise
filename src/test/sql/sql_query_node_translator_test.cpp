#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "SQLParser.h"
#include "gtest/gtest.h"

#include "optimizer/abstract_syntax_tree/projection_node.hpp"
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

  EXPECT_FALSE(result_node->get_right());
  EXPECT_FALSE(result_node->get_left()->get_left());
}

TEST_F(SQLQueryNodeTranslatorTest, SelectWithAndCondition) {
  const auto query = "SELECT * FROM table_a WHERE a >= 1234 AND b < 457.9";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->get_type(), NodeType::Projection);
  EXPECT_FALSE(result_node->get_right());

  auto ts_node_1 = result_node->get_left();
  EXPECT_EQ(ts_node_1->get_type(), NodeType::TableScan);
  EXPECT_FALSE(ts_node_1->get_right());

  auto ts_node_2 = ts_node_1->get_left();
  EXPECT_EQ(ts_node_2->get_type(), NodeType::TableScan);
  EXPECT_FALSE(ts_node_2->get_right());

  auto t_node = ts_node_2->get_left();
  EXPECT_EQ(t_node->get_type(), NodeType::Table);
  EXPECT_FALSE(t_node->get_left());
  EXPECT_FALSE(t_node->get_right());
}

}  // namespace opossum
