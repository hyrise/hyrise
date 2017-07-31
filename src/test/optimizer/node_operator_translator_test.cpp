#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "SQLParser.h"
#include "gtest/gtest.h"

#include "operators/abstract_operator.hpp"
#include "optimizer/abstract_syntax_tree/ast_to_operator_translator.hpp"
#include "scheduler/operator_task.hpp"
#include "sql/sql_to_ast_translator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class NodeOperatorTranslatorTest : public BaseTest {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(table_a));

    std::shared_ptr<Table> table_b = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", std::move(table_b));
  }

  std::shared_ptr<AbstractOperator> translate_query_to_operator(const std::string query) {
    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parseSQLString(query, &parse_result);

    if (!parse_result.isValid()) {
      throw std::runtime_error("Query is not valid.");
    }

    auto result_node = _node_translator.translate_parse_result(parse_result)[0];
    return ASTToOperatorTranslator::get().translate_node(result_node);
  }

  std::shared_ptr<OperatorTask> schedule_query_and_return_task(const std::string query) {
    auto result_operator = translate_query_to_operator(query);
    auto tasks = OperatorTask::make_tasks_from_operator(result_operator);
    for (auto& task : tasks) {
      task->schedule();
    }
    return tasks.back();
  }

  void execute_and_check(const std::string query, std::shared_ptr<Table> expected_result,
                         bool order_sensitive = false) {
    auto result_task = schedule_query_and_return_task(query);
    EXPECT_TABLE_EQ(result_task->get_operator()->get_output(), expected_result, order_sensitive);
  }

  SQLToASTTranslator _node_translator;
};

TEST_F(NodeOperatorTranslatorTest, SelectStarAllTest) {
  const auto query = "SELECT * FROM table_a;";
  const auto expected_result = load_table("src/test/tables/int_float.tbl", 2);
  execute_and_check(query, expected_result);
}

TEST_F(NodeOperatorTranslatorTest, SelectWithAndCondition) {
  const auto query = "SELECT * FROM table_b WHERE a = 12345 AND b > 457;";
  const auto expected_result = load_table("src/test/tables/int_float2_filtered.tbl", 2);
  execute_and_check(query, expected_result);
}

TEST_F(NodeOperatorTranslatorTest, SelectWithOrderByDesc) {
  const auto query = "SELECT * FROM table_a ORDER BY a DESC;";
  const auto expected_result = load_table("src/test/tables/int_float_reverse.tbl", 2);
  execute_and_check(query, expected_result, true);
}

TEST_F(NodeOperatorTranslatorTest, SelectWithMultipleOrderByColumns) {
  const auto query = "SELECT * FROM table_b ORDER BY a, b ASC;";
  const auto expected_result = load_table("src/test/tables/int_float2_sorted.tbl", 2);
  execute_and_check(query, expected_result, true);
}

TEST_F(NodeOperatorTranslatorTest, SelectInnerJoin) {
  const auto query = "SELECT * FROM table_a AS \"left\" INNER JOIN table_b AS \"right\" ON \"left\".a = \"right\".a;";
  const auto expected_result = load_table("src/test/tables/joinoperators/int_inner_join.tbl", 2);
  execute_and_check(query, expected_result);
}

// TODO(tim): Projection cannot handle expression `$a + $b`
// because it is not able to handle columns with different data types.
// Create issue with failing test.
TEST_F(NodeOperatorTranslatorTest, SelectWithAggregate) {
  const auto query = "SELECT SUM(b + b) AS sum_b_b FROM table_a;";
  const auto expected_result = load_table("src/test/tables/int_float_sum_b_plus_b.tbl", 2);
  execute_and_check(query, expected_result, true);
}

}  // namespace opossum
