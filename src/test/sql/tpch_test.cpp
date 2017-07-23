#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "SQLParser.h"
#include "gtest/gtest.h"

#include "operators/abstract_operator.hpp"
#include "optimizer/abstract_syntax_tree/node_operator_translator.hpp"
#include "scheduler/operator_task.hpp"
#include "sql/sql_query_node_translator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class TPCHTest : public BaseTest {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> table_a = load_table("src/test/tables/tpch/lineitem.tbl", 2);
    StorageManager::get().add_table("lineitem", std::move(table_a));
  }

  std::shared_ptr<AbstractOperator> translate_query_to_operator(const std::string query) {
    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parseSQLString(query, &parse_result);

    if (!parse_result.isValid()) {
      throw std::runtime_error("Query is not valid.");
    }

    auto result_node = _node_translator.translate_parse_result(parse_result)[0];
    return NodeOperatorTranslator::get().translate_node(result_node);
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

  SQLQueryNodeTranslator _node_translator;
};

TEST_F(TPCHTest, TPCH6) {
  /**
   * Original:
   *
   * SELECT SUM(L_EXTENDEDPRICE*L_DISCOUNT) AS REVENUE
   * FROM LINEITEM
   * WHERE L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE < dateadd(yy, 1, cast('1994-01-01' as datetime))
   * AND L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01 AND L_QUANTITY < 24
   *
   * Changes:
   *  1. dates are not supported
   *    a. use strings as data type for now
   *    b. pre-calculate date operation
   *  2. arithmetic expressions with constants are not resolved automatically yet, so pre-calculate them as well
   */
  const auto query =
      "SELECT SUM(l_extendedprice*l_discount) AS REVENUE\n"
      "FROM lineitem\n"
      "WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'\n"
      "AND l_discount BETWEEN .05 AND .07 AND l_quantity < 24;";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch6.tbl", 2);
  execute_and_check(query, expected_result, true);
}

}  // namespace opossum
