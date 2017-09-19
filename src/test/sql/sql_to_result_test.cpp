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
    StorageManager::get().add_table("int_float", load_table("src/test/tables/int_float.tbl", 2));
    StorageManager::get().add_table("int_float2", load_table("src/test/tables/int_float2.tbl", 2));
    StorageManager::get().add_table("int_float4", load_table("src/test/tables/int_float4.tbl", 2));
    StorageManager::get().add_table("int_float6", load_table("src/test/tables/int_float6.tbl", 2));
    StorageManager::get().add_table("int_string2", load_table("src/test/tables/int_string2.tbl", 2));
    StorageManager::get().add_table("int_int3", load_table("src/test/tables/int_int3.tbl", 3));
    StorageManager::get().add_table("groupby_int_1gb_1agg",
                                    load_table("src/test/tables/aggregateoperator/groupby_int_1gb_1agg/input.tbl", 2));
    StorageManager::get().add_table("groupby_int_1gb_2agg",
                                    load_table("src/test/tables/aggregateoperator/groupby_int_1gb_2agg/input.tbl", 2));
    StorageManager::get().add_table("groupby_int_2gb_2agg",
                                    load_table("src/test/tables/aggregateoperator/groupby_int_2gb_2agg/input.tbl", 2));
    StorageManager::get().add_table("groupby_int_2gb_2agg_2",
                                    load_table("src/test/tables/aggregateoperator/groupby_int_2gb_2agg/input2.tbl", 2));
    StorageManager::get().add_table(
        "groupby_int_1gb_1agg_null",
        load_table("src/test/tables/aggregateoperator/groupby_int_1gb_1agg/input_null.tbl", 2));

    StorageManager::get().add_table("int_for_delete_1", load_table("src/test/tables/int.tbl", 3));

    StorageManager::get().add_table("int_int_for_insert_1", load_table("src/test/tables/int_int3_limit_1.tbl", 2));
    StorageManager::get().add_table("int_int_for_insert_2", load_table("src/test/tables/int_int3_limit_1.tbl", 2));
    StorageManager::get().add_table("int_int_for_update", load_table("src/test/tables/int_int3_updated.tbl", 2));

    std::shared_ptr<Table> groupby_int_1gb_1agg =
        load_table("src/test/tables/aggregateoperator/groupby_int_1gb_1agg/input.tbl", 2);
    StorageManager::get().add_table("groupby_int_1gb_1agg", groupby_int_1gb_1agg);

    std::shared_ptr<Table> groupby_int_1gb_2agg =
        load_table("src/test/tables/aggregateoperator/groupby_int_1gb_2agg/input.tbl", 2);
    StorageManager::get().add_table("groupby_int_1gb_2agg", groupby_int_1gb_2agg);

    std::shared_ptr<Table> groupby_int_2gb_2agg =
        load_table("src/test/tables/aggregateoperator/groupby_int_2gb_2agg/input.tbl", 2);
    StorageManager::get().add_table("groupby_int_2gb_2agg", groupby_int_2gb_2agg);

    StorageManager::get().add_table("int_int_int", load_table("src/test/tables/int_int_int.tbl", 2));

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

  for (const auto &root : plan.tree_roots()) {
    auto tasks = OperatorTask::make_tasks_from_operator(root);

    for (auto &task : tasks) {
      task->get_operator()->set_transaction_context(tx_context);
    }

    CurrentScheduler::schedule_and_wait_for_tasks(tasks);
    result_operator = tasks.back()->get_operator();
  }

  EXPECT_TABLE_EQ(result_operator->get_output(), expected_result,
                  params.order_sensitive == OrderSensitivity::Sensitive);
}

const SQLTestParam test_queries[] = {
    {"SELECT * FROM int_float;", "src/test/tables/int_float.tbl"},

    // Table Scans
    {"SELECT * FROM int_float2 WHERE a = 12345 AND b > 457;", "src/test/tables/int_float2_filtered.tbl"},
    {"SELECT * FROM int_float WHERE a >= 1234;", "src/test/tables/int_float_filtered2.tbl"},
    {"SELECT * FROM int_float WHERE 1234 <= a;", "src/test/tables/int_float_filtered2.tbl"},
    {"SELECT * FROM int_float WHERE a >= 1234 AND b < 457.9", "src/test/tables/int_float_filtered.tbl"},
    {"SELECT * FROM int_string2 WHERE a BETWEEN 122 AND 124", "src/test/tables/int_string_filtered.tbl"},
    {"SELECT * FROM int_int_int WHERE a BETWEEN b AND 10", "src/test/tables/int_int_int_between_column_literal.tbl"},

    // Projection
    {"SELECT a FROM int_float;", "src/test/tables/int.tbl"},
    {"SELECT a as b FROM int_float;", "src/test/tables/int2.tbl"},
    {"SELECT a, 4+6 as b FROM int_float;", "src/test/tables/int_int_constant.tbl"},

    // ORDER BY
    {"SELECT * FROM int_float ORDER BY a DESC;", "src/test/tables/int_float_reverse.tbl", OrderSensitivity::Sensitive},
    {"SELECT * FROM int_float4 ORDER BY a, b;", "src/test/tables/int_float2_sorted.tbl", OrderSensitivity::Sensitive},
    {"SELECT * FROM int_float4 ORDER BY a, b ASC;", "src/test/tables/int_float2_sorted.tbl",
     OrderSensitivity::Sensitive},
    {"SELECT * FROM int_float4 ORDER BY a, b DESC;", "src/test/tables/int_float2_sorted_mixed.tbl",
     OrderSensitivity::Sensitive},
    {"SELECT a, b FROM int_float ORDER BY a;", "src/test/tables/int_float_sorted.tbl", OrderSensitivity::Sensitive},
    {"SELECT * FROM int_float4 ORDER BY a, b;", "src/test/tables/int_float2_sorted.tbl", OrderSensitivity::Sensitive},
    {"SELECT a FROM (SELECT a, b FROM int_float WHERE a > 1 ORDER BY b) WHERE a > 0 ORDER BY a;",
     "src/test/tables/int.tbl", OrderSensitivity::Sensitive},

    // LIMIT
    {"SELECT * FROM int_int3 LIMIT 4;", "src/test/tables/int_int3_limit_4.tbl"},

    // PRODUCT
    {R"(SELECT "left".a, "left".b, "right".a, "right".b
        FROM int_float AS "left",  int_float2 AS "right"
        WHERE "left".a = "right".a;)",
     "src/test/tables/joinoperators/int_inner_join.tbl"},

    // JOIN
    {R"(SELECT "left".a, "left".b, "right".a, "right".b
        FROM int_float AS "left"
        JOIN int_float2 AS "right"
        ON "left".a = "right".a;)",
     "src/test/tables/joinoperators/int_inner_join.tbl"},
    {R"(SELECT *
        FROM int_float AS "left"
        LEFT JOIN int_float2 AS "right"
        ON "left".a = "right".a;)",
     "src/test/tables/joinoperators/int_left_join.tbl"},
    {R"(SELECT *
        FROM int_float AS "left"
        INNER JOIN int_float2 AS "right"
        ON "left".a = "right".a;)",
     "src/test/tables/joinoperators/int_inner_join.tbl"},

    // JOIN multiple tables
    {R"(SELECT *
        FROM int_float AS t1
        INNER JOIN int_float2 AS t2
        ON t1.a = t2.a
        INNER JOIN int_string2 AS t3
        ON t1.a = t3.a)",
     "src/test/tables/joinoperators/int_inner_join_3_tables.tbl"},

    // Make sure that name-to-id-resolving works fine.
    {R"(SELECT t1.a, t1.b, t2.b, t3.b
        FROM int_float AS t1
        INNER JOIN int_float2 AS t2
        ON t1.a = t2.a
        INNER JOIN int_string2 AS t3
        ON t1.a = t3.a)",
     "src/test/tables/joinoperators/int_inner_join_3_tables_projection.tbl"},

    // Make sure that t1.* is resolved only to columns from t1, not all columns from input node.
    {R"(SELECT t1.*, t2.b, t3.b
        FROM int_float AS t1
        INNER JOIN int_float2 AS t2
        ON t1.a = t2.a
        INNER JOIN int_string2 AS t3
        ON t1.a = t3.a)",
     "src/test/tables/joinoperators/int_inner_join_3_tables_projection.tbl"},

    {R"(SELECT t1.*, t2.a, t2.b, t3.*
        FROM int_float AS t1
        INNER JOIN int_float2 AS t2
        ON t1.a = t2.a
        INNER JOIN int_string2 AS t3
        ON t1.a = t3.a)",
     "src/test/tables/joinoperators/int_inner_join_3_tables.tbl"},

    // Join four tables, just because we can.
    {R"(SELECT t1.a, t1.b, t2.b, t3.b, t4.b
        FROM int_float AS t1
        INNER JOIN int_float2 AS t2
        ON t1.a = t2.a
        INNER JOIN int_float6 AS t3
        ON t1.a = t3.a
        INNER JOIN int_string2 AS t4
        ON t1.a = t4.a)",
     "src/test/tables/joinoperators/int_inner_join_4_tables_projection.tbl"},

    // TODO(anybody): uncomment test once filtering after joins works.
    //    {R"(SELECT *
    //        FROM int_float AS t1
    //        INNER JOIN int_float2 AS t2
    //        ON t1.a = t2.a
    //        INNER JOIN int_string2 AS t3
    //        ON t1.a = t3.a
    //        WHERE t2.b > 457.0
    //        AND t3.b = 'C')",
    //     "src/test/tables/joinoperators/int_inner_join_3_tables_filter.tbl"},

    // Aggregates
    {"SELECT SUM(b + b) AS sum_b_b FROM int_float;", "src/test/tables/int_float_sum_b_plus_b.tbl"},

    // GROUP BY
    {"SELECT a, SUM(b) FROM groupby_int_1gb_1agg GROUP BY a;",
     "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/sum.tbl"},
    {"SELECT a, SUM(b), AVG(c) FROM groupby_int_1gb_2agg GROUP BY a;",
     "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/sum_avg.tbl"},
    {"SELECT a, b, MAX(c), AVG(d) FROM groupby_int_2gb_2agg GROUP BY a, b;",
     "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/max_avg.tbl"},

    // COUNT(*)
    {"SELECT a, COUNT(*) FROM groupby_int_1gb_1agg_null GROUP BY a;",
     "src/test/tables/aggregateoperator/groupby_int_1gb_0agg/count_star.tbl"},
    {"SELECT COUNT(*), SUM(a+b) FROM int_int3;", "src/test/tables/aggregateoperator/0gb_2agg/count_sum.tbl"},
    // todo(anyone): Enable as soon as #182 is resolved
    {"SELECT COUNT(*) FROM groupby_int_1gb_1agg_null GROUP BY a;",
     "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/count_star.tbl"},

    // Aggregates with NULL
    {"SELECT a, MAX(b) FROM groupby_int_1gb_1agg_null GROUP BY a;",
     "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/max_null.tbl"},
    {"SELECT a, MIN(b) FROM groupby_int_1gb_1agg_null GROUP BY a;",
     "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/min_null.tbl"},
    {"SELECT a, SUM(b) FROM groupby_int_1gb_1agg_null GROUP BY a;",
     "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/sum_null.tbl"},
    {"SELECT a, AVG(b) FROM groupby_int_1gb_1agg_null GROUP BY a;",
     "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/avg_null.tbl"},
    {"SELECT a, COUNT(b) FROM groupby_int_1gb_1agg_null GROUP BY a;",
     "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/count_null.tbl"},

    // Checks that output of Aggregate can be worked with correctly.
    {R"(SELECT d, min_c, max_a
        FROM (
          SELECT b, d, MAX(a) AS max_a, MIN(c) AS min_c
          FROM groupby_int_2gb_2agg_2
          GROUP BY b, d
        )
        WHERE d BETWEEN 20 AND 50 AND min_c > 15;)",
     "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/max_min_filter_projection.tbl"},
    {"SELECT SUM(b) FROM groupby_int_1gb_1agg", "src/test/tables/aggregateoperator/0gb_1agg/sum.tbl"},

    // HAVING
    {"SELECT a, b, MAX(c), AVG(d) FROM groupby_int_2gb_2agg GROUP BY a, b HAVING MAX(c) >= 10 AND MAX(c) < 40;",
     "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/max_avg.tbl"},
    {"SELECT a, b, MAX(c), AVG(d) FROM groupby_int_2gb_2agg GROUP BY a, b HAVING MAX(c) > 10 AND MAX(c) <= 30;",
     "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/max_avg_having.tbl"},
    {"SELECT a, b, MAX(c), AVG(d) FROM groupby_int_2gb_2agg GROUP BY a, b HAVING b > 457 AND a = 12345;",
     "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/having_on_gb.tbl"},

    {"SELECT * FROM customer;", "src/test/tables/tpch/customer.tbl"},
    {"SELECT c_custkey, c_name FROM customer;", "src/test/tables/tpch/customer_projection.tbl"},

    // DELETE
    // TODO(MD): this will only work once SELECT automatically validates (#188)
    // {"DELETE FROM int_for_delete_1; SELECT * FROM int_for_delete_1", "src/test/tables/int_empty.tbl"},
    // {"DELETE FROM int_for_delete_2 WHERE a > 1000; SELECT * FROM int_for_delete_2",
    // "src/test/tables/int_deleted.tbl"}

    // UPDATE
    // TODO(md): see DELETE
    // {"UPDATE int_int_for_update SET a = a + 1 WHERE b > 10; SELECT * FROM int_int_for_update",
    // "src/test/tables/int_int3_updated.tbl"},

    // INSERT
    {"INSERT INTO int_int_for_insert_1 VALUES (1, 3); SELECT * FROM int_int_for_insert_1;",
     "src/test/tables/int_int3_limit_2.tbl"},
    {"INSERT INTO int_int_for_insert_1 (a, b) VALUES (1, 3); SELECT * FROM int_int_for_insert_1;",
     "src/test/tables/int_int3_limit_2.tbl"},
    {"INSERT INTO int_int_for_insert_1 (b, a) VALUES (3, 1); SELECT * FROM int_int_for_insert_1;",
     "src/test/tables/int_int3_limit_2.tbl"},

    {R"(INSERT INTO int_int_for_insert_1 VALUES (1, 3);
     INSERT INTO int_int_for_insert_1 VALUES (13, 2);
     INSERT INTO int_int_for_insert_1 VALUES (6, 9);
     SELECT * FROM int_int_for_insert_1;)",
     "src/test/tables/int_int3_limit_4.tbl"},

    // INSERT ... INTO ... (with literal projection)
    {R"(INSERT INTO int_int_for_insert_1 SELECT 1, 3 FROM int_int_for_insert_1; 
        SELECT * FROM int_int_for_insert_1;)",
     "src/test/tables/int_int3_limit_2.tbl"},
    {R"(INSERT INTO int_int_for_insert_1 (a, b) SELECT 1, 3 FROM int_int_for_insert_1; 
        SELECT * FROM int_int_for_insert_1;)",
     "src/test/tables/int_int3_limit_2.tbl"},
    {R"(INSERT INTO int_int_for_insert_1 (b, a) SELECT 3, 1 FROM int_int_for_insert_1; 
        SELECT * FROM int_int_for_insert_1;)",
     "src/test/tables/int_int3_limit_2.tbl"},

    // INSERT ... INTO ... (with regular queries)
    {R"(INSERT INTO int_int_for_insert_1 SELECT * FROM int_int3 WHERE a = 1 AND b = 3;
        INSERT INTO int_int_for_insert_1 SELECT * FROM int_int3 WHERE a = 13;
        INSERT INTO int_int_for_insert_1 (a, b) SELECT a, b FROM int_int3 WHERE a = 6;
        SELECT * FROM int_int_for_insert_1;)",
     "src/test/tables/int_int3_limit_4.tbl"},

    {R"(SELECT customer.c_custkey, customer.c_name, COUNT(orders.o_orderkey)
        FROM customer JOIN orders ON c_custkey = o_custkey
        GROUP BY customer.c_custkey, customer.c_name
        HAVING COUNT(orders.o_orderkey) >= 100;)",
     "src/test/tables/tpch/customer_join_orders.tbl"},

    // TODO(mp): Aliases for Subselects are not supported yet
    //    {R"(SELECT customer.c_custkey, customer.c_name, COUNT(orderitems.o_orderkey)
    //        FROM customer JOIN (
    //          SELECT * FROM orders JOIN lineitem ON o_orderkey = l_orderkey
    //        ) AS orderitems
    //        ON customer.c_custkey = orders.o_custkey
    //        GROUP BY customer.c_custkey, customer.c_name
    //        HAVING COUNT(orderitems.o_orderkey) >= 100;)",
    //      "src/test/tables/tpch/customer_join_orders_alias.tbl"},
};

INSTANTIATE_TEST_CASE_P(test_queries, SQLToResultTest, ::testing::ValuesIn(test_queries));

}  // namespace opossum
