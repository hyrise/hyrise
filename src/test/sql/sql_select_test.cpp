
#include <memory>
#include <string>
#include <tuple>
#include <utility>

#include "../base_test.hpp"
#include "SQLParser.h"
#include "gtest/gtest.h"

#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "optimizer/abstract_syntax_tree/ast_to_operator_translator.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_to_ast_translator.hpp"
#include "sql/sql_query_plan.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

// Query, Number of Operators, Execute Plan, Expected Result Table
typedef std::tuple<std::string, size_t, bool, std::string> SQLTestParam;

class SQLSelectTest : public BaseTest, public ::testing::WithParamInterface<SQLTestParam> {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(table_a));

    std::shared_ptr<Table> table_b = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", std::move(table_b));

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
    load_tpch_tables();
  }

  void load_tpch_tables() {
    std::shared_ptr<Table> customer = load_table("src/test/tables/tpch/customer.tbl", 1);
    StorageManager::get().add_table("customer", customer);

    std::shared_ptr<Table> orders = load_table("src/test/tables/tpch/orders.tbl", 1);
    StorageManager::get().add_table("orders", orders);

    std::shared_ptr<Table> lineitem = load_table("src/test/tables/tpch/lineitem.tbl", 1);
    StorageManager::get().add_table("lineitem", lineitem);
  }

  void compile_query(const std::string query) {
    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parseSQLString(query, &parse_result);

    ASSERT_TRUE(parse_result.isValid());

    // Compile the parse result.
    auto result_node = _translator.translate_parse_result(parse_result)[0];
    auto result_operator = ASTToOperatorTranslator::get().translate_node(result_node);

    _plan.add_tree_by_root(result_operator);
  }

  void execute_query_plan() {
    for (const auto& task : _plan.tasks()) {
      task->get_operator()->execute();
    }
  }

  std::shared_ptr<const Table> get_plan_result() { return _plan.tree_roots().back()->get_output(); }

  SQLToASTTranslator _translator;
  SQLQueryPlan _plan;
};

TEST_F(SQLSelectTest, BasicParserSuccessTest) {
  hsql::SQLParserResult parse_result;

  const std::string query = "SELECT * FROM test;";
  hsql::SQLParser::parseSQLString(query, &parse_result);
  EXPECT_TRUE(parse_result.isValid());

  const std::string faulty_query = "SELECT * WHERE test;";
  hsql::SQLParser::parseSQLString(faulty_query, &parse_result);
  EXPECT_FALSE(parse_result.isValid());
}

TEST_F(SQLSelectTest, SelectWithSchedulerTest) {
  const std::string query =
      "SELECT \"left\".a, \"left\".b, \"right\".a, \"right\".b FROM table_a AS \"left\" INNER JOIN table_b AS "
      "\"right\" ON a = a";
  auto expected_result = load_table("src/test/tables/joinoperators/int_inner_join.tbl", 1);
  // TODO(torpedro): Adding 'WHERE \"left\".a >= 0;' causes wrong data. Investigate.
  //                 Probable bug in TableScan.

  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(8, 4)));
  compile_query(query);

  for (const auto& task : _plan.tasks()) {
    task->schedule();
  }

  CurrentScheduler::get()->finish();
  CurrentScheduler::set(nullptr);

  EXPECT_TABLE_EQ(get_plan_result(), expected_result, true);
}

// Generic test case that will be called with the parameters listed below.
// Compiles a query and executes it, if specified.
// Checks the number of operators in the plan and can check the result against a table in a file.
TEST_P(SQLSelectTest, SQLQueryTest) {
  SQLTestParam param = GetParam();
  std::string query = std::get<0>(param);
  //  size_t num_operators = std::get<1>(param);
  bool should_execute = std::get<2>(param);
  std::string expected_result_file = std::get<3>(param);

  compile_query(query);
  EXPECT_EQ(1u, _plan.num_trees());
  //  EXPECT_EQ(num_operators, _plan.num_operators());

  if (should_execute) {
    execute_query_plan();

    if (!expected_result_file.empty()) {
      auto expected_result = load_table(expected_result_file, 1);
      EXPECT_TABLE_EQ(get_plan_result(), expected_result);
    }
  }
}

const SQLTestParam test_queries[] = {
    SQLTestParam{"SELECT * FROM table_a;", 1u, true, "src/test/tables/int_float.tbl"},
    // Table Scans
    SQLTestParam{"SELECT * FROM table_a WHERE a >= 1234;", 2u, true, "src/test/tables/int_float_filtered2.tbl"},
    SQLTestParam{"SELECT * FROM table_a WHERE a >= 1234 AND b < 457.9", 3u, true,
                 "src/test/tables/int_float_filtered.tbl"},
    // TODO(torpedro): Enable this test, after implementing BETWEEN support in translator.
    // SQLTestParam{"SELECT * FROM TestTable WHERE a BETWEEN 122 AND 124", 2u,
    // "src/test/tables/int_string_filtered.tbl"},
    // Projection
    SQLTestParam{"SELECT a FROM table_a;", 2u, true, "src/test/tables/int.tbl"},
    // ORDER BY
    SQLTestParam{"SELECT a, b FROM table_a ORDER BY a;", 3u, true, "src/test/tables/int_float_sorted.tbl"},
    SQLTestParam{"SELECT a FROM (SELECT a, b FROM table_a WHERE a > 1 ORDER BY b) WHERE a > 0 ORDER BY a;", 7u, true,
                 "src/test/tables/int.tbl"},
    // JOIN
    SQLTestParam{"SELECT \"left\".a, \"left\".b, \"right\".a, \"right\".b FROM table_a AS \"left\" JOIN table_b AS "
                 "\"right\" ON a = a;",
                 4u, true, "src/test/tables/joinoperators/int_inner_join.tbl"},
    SQLTestParam{"SELECT * FROM table_a AS \"left\" LEFT JOIN table_b AS \"right\" ON a = a;", 3u, true,
                 "src/test/tables/joinoperators/int_left_join.tbl"},
    // GROUP BY
    SQLTestParam{"SELECT a, SUM(b) FROM groupby_int_1gb_1agg GROUP BY a;", 3u, true,
                 "src/test/tables/aggregateoperator/groupby_int_1gb_1agg/sum.tbl"},
    SQLTestParam{"SELECT a, SUM(b), AVG(c) FROM groupby_int_1gb_2agg GROUP BY a;", 3u, true,
                 "src/test/tables/aggregateoperator/groupby_int_1gb_2agg/sum_avg.tbl"},
    SQLTestParam{"SELECT a, b, MAX(c), AVG(d) FROM groupby_int_2gb_2agg GROUP BY a, b;", 3u, true,
                 "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/max_avg.tbl"},
    SQLTestParam{
        "SELECT a, b, MAX(c), AVG(d) FROM groupby_int_2gb_2agg GROUP BY a, b HAVING MAX(c) >= 10 AND MAX(c) < 40;", 5u,
        true, "src/test/tables/aggregateoperator/groupby_int_2gb_2agg/max_avg.tbl"},

    SQLTestParam{"SELECT * FROM customer;", 1u, true, "src/test/tables/tpch/customer.tbl"},
    SQLTestParam{"SELECT c_custkey, c_name FROM customer;", 2u, true, "src/test/tables/tpch/customer_projection.tbl"},
    SQLTestParam{"SELECT customer.c_custkey, customer.c_name, COUNT(\"orders.o_orderkey\")"
                 "  FROM customer"
                 "  JOIN orders ON c_custkey = o_custkey"
                 "  GROUP BY \"customer.c_custkey\", \"customer.c_name\""
                 "  HAVING COUNT(\"orders.o_orderkey\") >= 100;",
                 6u, true, "src/test/tables/tpch/customer_join_orders.tbl"},
    SQLTestParam{"SELECT customer.c_custkey, customer.c_name, COUNT(\"orderitems.orders.o_orderkey\")"
                 "  FROM customer"
                 "  JOIN (SELECT * FROM "
                 "    orders"
                 "    JOIN lineitem ON o_orderkey = l_orderkey"
                 "  ) AS orderitems ON c_custkey = \"orders.o_custkey\""
                 "  GROUP BY customer.c_custkey, customer.c_name"
                 "  HAVING COUNT(\"orderitems.orders.o_orderkey\") >= 100;",
                 8u, true, "src/test/tables/tpch/customer_join_orders_alias.tbl"},
};

INSTANTIATE_TEST_CASE_P(test_queries, SQLSelectTest, ::testing::ValuesIn(test_queries));

}  // namespace opossum
