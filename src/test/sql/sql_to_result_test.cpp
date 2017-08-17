
#include <memory>
#include <string>
#include <tuple>
#include <utility>

#include "SQLParser.h"
#include "base_test.hpp"
#include "gtest/gtest.h"

#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "optimizer/abstract_syntax_tree/ast_to_operator_translator.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_to_ast_translator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

enum class OrderSensitivity { Sensitive, Insensitive };

struct SQLTestParam {
  SQLTestParam(const std::string &query, const std::string &result_table_path,
               OrderSensitivity orderSensitivity = OrderSensitivity::Insensitive)
      : query(query), result_table_path(result_table_path), order_sensitive(orderSensitivity) {}

  const std::string query;
  const std::string result_table_path;
  const OrderSensitivity order_sensitive;
};

class SQLToResultTest : public BaseTest, public ::testing::WithParamInterface<SQLTestParam> {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(table_a));

    std::shared_ptr<Table> table_b = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", std::move(table_b));

    std::shared_ptr<Table> table_c = load_table("src/test/tables/int_float4.tbl", 2);
    StorageManager::get().add_table("table_c", std::move(table_c));

    std::shared_ptr<Table> test_table2 = load_table("src/test/tables/int_string2.tbl", 2);
    StorageManager::get().add_table("TestTable", test_table2);

    std::shared_ptr<Table> groupby_int_1gb_1agg =
        load_table("src/test/tables/aggregateoperator/groupby_int_1gb_1agg/input.tbl", 2);
    StorageManager::get().add_table("groupby_int_1gb_1agg", groupby_int_1gb_1agg);

    std::shared_ptr<Table> groupby_int_1gb_2agg =
        load_table("src/test/tables/aggregateoperator/groupby_int_1gb_2agg/input.tbl", 2);
    StorageManager::get().add_table("groupby_int_1gb_2agg", groupby_int_1gb_2agg);

    std::shared_ptr<Table> groupby_int_2gb_2agg =
        load_table("src/test/tables/aggregateoperator/groupby_int_2gb_2agg/input.tbl", 2);
    StorageManager::get().add_table("groupby_int_2gb_2agg", groupby_int_2gb_2agg);

    // Load TPC-H tables
    std::shared_ptr<Table> customer = load_table("src/test/tables/tpch/customer.tbl", 1);
    StorageManager::get().add_table("customer", customer);

    std::shared_ptr<Table> orders = load_table("src/test/tables/tpch/orders.tbl", 1);
    StorageManager::get().add_table("orders", orders);

    std::shared_ptr<Table> lineitem = load_table("src/test/tables/tpch/lineitem.tbl", 1);
    StorageManager::get().add_table("lineitem", lineitem);
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

  // Expect the query to be a single statement.
  auto result_node = SQLToASTTranslator::get().translate_parse_result(parse_result)[0];
  auto result_operator = ASTToOperatorTranslator::get().translate_node(result_node);

  auto tasks = OperatorTask::make_tasks_from_operator(result_operator);
  CurrentScheduler::schedule_and_wait_for_tasks(tasks);

  auto result_table = tasks.back()->get_operator()->get_output();

  EXPECT_TABLE_EQ(result_table, expected_result, params.order_sensitive == OrderSensitivity::Sensitive);
}

const SQLTestParam test_queries[] = {
    {"SELECT * FROM table_a;", "src/test/tables/int_float.tbl"},  // 1

    // Table Scans
    {"SELECT * FROM table_b WHERE a = 12345 AND b > 457;", "src/test/tables/int_float2_filtered.tbl"},   // 2
    {"SELECT * FROM table_a WHERE a >= 1234;", "src/test/tables/int_float_filtered2.tbl"},               // 3
    {"SELECT * FROM table_a WHERE a >= 1234 AND b < 457.9", "src/test/tables/int_float_filtered.tbl"},   // 4
    {"SELECT * FROM TestTable WHERE a BETWEEN 122 AND 124", "src/test/tables/int_string_filtered.tbl"},  // 5

    // Projection
    {"SELECT a FROM table_a;", "src/test/tables/int.tbl"},  // 5

    // ORDER BY
    {"SELECT * FROM table_a ORDER BY a DESC;", "src/test/tables/int_float_reverse.tbl",
     OrderSensitivity::Sensitive},  // 6
    {"SELECT * FROM table_b ORDER BY a, b;", "src/test/tables/int_float2_sorted.tbl",
     OrderSensitivity::Sensitive},  // 7
    {"SELECT * FROM table_b ORDER BY a, b ASC;", "src/test/tables/int_float2_sorted.tbl",
     OrderSensitivity::Sensitive},                                                                                  // 8
    {"SELECT a, b FROM table_a ORDER BY a;", "src/test/tables/int_float_sorted.tbl", OrderSensitivity::Sensitive},  // 9
    {"SELECT * FROM table_c ORDER BY a, b;", "src/test/tables/int_float2_sorted.tbl",
     OrderSensitivity::Sensitive},  // 10
    {"SELECT a FROM (SELECT a, b FROM table_a WHERE a > 1 ORDER BY b) WHERE a > 0 ORDER BY a;",
     "src/test/tables/int.tbl", OrderSensitivity::Sensitive},  // 11

    // AGGREGATE
    // TODO(tim): Projection cannot handle expression `$a + $b`
    // because it is not able to handle columns with different data types.
    // Create issue with failing test.
    {"SELECT SUM(b + b) AS sum_b_b FROM table_a;", "src/test/tables/int_float_sum_b_plus_b.tbl"},  // 12

    // JOIN
    {R"(SELECT "left".a, "left".b, "right".a, "right".b FROM table_a AS "left" JOIN table_b AS "right" ON a = a;)",
     "src/test/tables/joinoperators/int_inner_join.tbl"},  // 13
    {R"(SELECT * FROM table_a AS "left" LEFT JOIN table_b AS "right" ON a = a;)",
     "src/test/tables/joinoperators/int_left_join.tbl"},  // 14
    {R"(SELECT * FROM table_a AS "left" INNER JOIN table_b AS "right" ON "left".a = "right".a;)",
     "src/test/tables/joinoperators/int_inner_join.tbl"},  // 15

    // GROUP BY
    {"SELECT a, SUM(b) FROM groupby_int_1gb_1agg GROUP BY a;",
     "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/sum.tbl"},  // 16
    {"SELECT a, SUM(b), AVG(c) FROM groupby_int_1gb_2agg GROUP BY a;",
     "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/sum_avg.tbl"},  // 17
    {"SELECT a, b, MAX(c), AVG(d) FROM groupby_int_2gb_2agg GROUP BY a, b;",
     "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/max_avg.tbl"},  // 18

    // HAVING
    {"SELECT a, b, MAX(c), AVG(d) FROM groupby_int_2gb_2agg GROUP BY a, b HAVING MAX(c) >= 10 AND MAX(c) < 40;",
     "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/max_avg.tbl"},  // 19
    {"SELECT a, b, MAX(c), AVG(d) FROM groupby_int_2gb_2agg GROUP BY a, b HAVING MAX(c) > 10 AND MAX(c) <= 30;",
     "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/max_avg_having.tbl"},  // 20

    {"SELECT * FROM customer;", "src/test/tables/tpch/customer.tbl"},                             // 21
    {"SELECT c_custkey, c_name FROM customer;", "src/test/tables/tpch/customer_projection.tbl"},  // 22

    /**
     * TODO: Reactivate these tests once joins do not prefix their output columns anymore
     */
    {R"(SELECT customer.c_custkey, customer.c_name, COUNT(orders.o_orderkey)
        FROM customer JOIN orders ON c_custkey = o_custkey
        GROUP BY customer.c_custkey, customer.c_name
        HAVING COUNT(orders.o_orderkey) >= 100;)",
     "src/test/tables/tpch/customer_join_orders.tbl"},  // 23
    // TODO(mp): Aliases for Subselects are not supported yet
    //    {R"(SELECT customer.c_custkey, customer.c_name, COUNT(orderitems.o_orderkey)
    //        FROM customer JOIN (
    //          SELECT * FROM orders JOIN lineitem ON o_orderkey = l_orderkey
    //        ) AS orderitems
    //        ON customer.c_custkey = orders.o_custkey
    //        GROUP BY customer.c_custkey, customer.c_name
    //        HAVING COUNT(orderitems.o_orderkey) >= 100;)",
    //      "src/test/tables/tpch/customer_join_orders_alias.tbl"}, //24
};

INSTANTIATE_TEST_CASE_P(test_queries, SQLToResultTest, ::testing::ValuesIn(test_queries));

}  // namespace opossum
