#include <memory>
#include <string>
#include <tuple>
#include <utility>

#include "SQLParser.h"
#include "base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_manager.hpp"
#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "operators/validate.hpp"
#include "optimizer/abstract_syntax_tree/ast_to_operator_translator.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_planner.hpp"
#include "sql/sql_to_ast_translator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

enum class OrderSensitivity { Sensitive, Insensitive };

struct SQLTestParam {
  SQLTestParam(const std::string& query, const std::string& result_table_path,
               OrderSensitivity orderSensitivity = OrderSensitivity::Insensitive)
      : query(query), result_table_path(result_table_path), order_sensitive(orderSensitivity) {}

  const std::string query;
  const std::string result_table_path;
  const OrderSensitivity order_sensitive;
};

class SQLToResultTest : public BaseTest, public ::testing::WithParamInterface<SQLTestParam> {
 protected:
  void SetUp() override {
    // Load TPC-H tables
    StorageManager::get().add_table("customer", load_table("src/test/tables/tpch/customer.tbl", 1));
    StorageManager::get().add_table("orders", load_table("src/test/tables/tpch/orders.tbl", 1));
    StorageManager::get().add_table("lineitem", load_table("src/test/tables/tpch/lineitem.tbl", 1));
  }
};

// Generic test case that will be called with the parameters listed below.
// Compiles a query, optimizes, and executes it.
// Checks the result against a table in a file.
TEST_P(SQLToResultTest, SQLQueryTest) {
  SQLTestParam params = GetParam();

  auto expected_result = load_table(params.result_table_path, 2);

  hsql::SQLParserResult parse_result;
  hsql::SQLParser::parseSQLString(params.query, &parse_result);

  if (!parse_result.isValid()) {
    throw std::runtime_error("Query is not valid.");
  }

  auto plan = SQLPlanner::plan(parse_result);

  std::shared_ptr<AbstractOperator> result_operator;

  auto tx_context = TransactionManager::get().new_transaction_context();

  for (const auto& root : plan.tree_roots()) {
    auto tasks = OperatorTask::make_tasks_from_operator(root);

    for (auto& task : tasks) {
      task->get_operator()->set_transaction_context(tx_context);
    }

    CurrentScheduler::schedule_and_wait_for_tasks(tasks);
    result_operator = tasks.back()->get_operator();
  }

  EXPECT_TABLE_EQ(result_operator->get_output(), expected_result,
                  params.order_sensitive == OrderSensitivity::Sensitive);
}

const SQLTestParam test_queries[] = {
    {R"(SELECT customer.c_custkey, customer.c_name, COUNT(orders.o_orderkey)
        FROM customer JOIN orders ON c_custkey = o_custkey
        GROUP BY customer.c_custkey, customer.c_name
        HAVING COUNT(orders.o_orderkey) >= 100;)",
     "src/test/tables/tpch/customer_join_orders.tbl"},

    {R"(SELECT customer.c_custkey, customer.c_name, COUNT(orderitems.o_orderkey)
       FROM customer JOIN (
         SELECT * FROM orders JOIN lineitem ON o_orderkey = l_orderkey
       ) AS orderitems
       ON customer.c_custkey = orderitems.o_custkey
       GROUP BY customer.c_custkey, customer.c_name
       HAVING COUNT(orderitems.o_orderkey) >= 100;)",
     "src/test/tables/tpch/customer_join_orders_alias.tbl"},
};

INSTANTIATE_TEST_CASE_P(test_queries, SQLToResultTest, ::testing::ValuesIn(test_queries));

}  // namespace opossum
