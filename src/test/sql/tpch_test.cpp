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

    auto result_node = SQLToASTTranslator::get().translate_parse_result(parse_result)[0];
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
};

// Enable with new Projection operator
TEST_F(TPCHTest, DISABLED_TPCH1) {
  /**
   * Original:
   *
   * SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price,
   * sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
   * avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order
   * FROM lineitem
   * WHERE l_shipdate <= date '1998-12-01' - interval '[DELTA]' day (3)
   * GROUP BY l_returnflag, l_linestatus
   * ORDER BY l_returnflag, l_linestatus
   *
   * Changes:
   *  1. dates are not supported
   *    a. use strings as data type for now
   *    b. pre-calculate date operation
   */
  const auto query =
      "SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, "
      "sum(l_extendedprice*(1-l_discount)) as sum_disc_price, "
      "sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty, "
      "avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order\n"
      "FROM lineitem\n"
      "WHERE l_shipdate <= '1998-12-01'\n"
      "GROUP BY l_returnflag, l_linestatus\n"
      "ORDER BY l_returnflag, l_linestatus;";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch1.tbl", 2);
  execute_and_check(query, expected_result, true);
}

// Enable with new Projection operator
TEST_F(TPCHTest, DISABLED_TPCH3) {
  /**
   * Original:
   *
   * SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority
   * FROM customer, orders, lineitem
   * WHERE c_mktsegment = '[SEGMENT]' AND c_custkey = o_custkey AND l_orderkey = o_orderkey
   * AND o_orderdate < date '[DATE]' AND l_shipdate > date '[DATE]'
   * GROUP BY l_orderkey, o_orderdate, o_shippriority
   * ORDER BY revenue desc, o_orderdate;
   *
   * Changes:
   *  1. Random values are hardcoded
   */
  const auto query =
      "SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority "
      "FROM customer, orders, lineitem\n"
      "WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey\n"
      "AND o_orderdate < '1995-03-15' AND l_shipdate > '1995-03-15'\n"
      "GROUP BY l_orderkey, o_orderdate, o_shippriority\n"
      "ORDER BY revenue desc, o_orderdate;";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch3.tbl", 2);
  execute_and_check(query, expected_result, true);
}

// Enable with new Projection operator
TEST_F(TPCHTest, DISABLED_TPCH5) {
  /**
   * Original:
   *
   * SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue
   * FROM customer, orders, lineitem, supplier, nation, region
   * WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
   * AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = '[REGION]' AND o_orderdate >= date
   * '[DATE]'
   * AND o_orderdate < date '[DATE]' + interval '1' year
   * GROUP BY n_name
   * ORDER BY revenue desc;
   *
   * Changes:
   *  1. Random values are hardcoded
   *  2. dates are not supported
   *    a. use strings as data type for now
   *    b. pre-calculate date operation
   */
  const auto query =
      "SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue\n"
      "FROM customer, orders, lineitem, supplier, nation, region\n"
      "WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey "
      "AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA' AND o_orderdate >= '1994-01-01'"
      "AND o_orderdate < '1995-01-01'\n "
      "GROUP BY n_name\n"
      "ORDER BY revenue desc;";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch5.tbl", 2);
  execute_and_check(query, expected_result, true);
}

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

// Enable with new Projection operator
TEST_F(TPCHTest, DISABLED_TPCH10) {
  /**
   * Original:
   *
   * SELECT c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address,
   * c_phone,
   * c_comment
   * FROM customer, orders, lineitem, nation
   * WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= date '[DATE]'
   * AND o_orderdate < date '[DATE]' + interval '3' month AND l_returnflag = 'R' AND c_nationkey = n_nationkey
   * GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
   * ORDER BY revenue desc;
   *
   * Changes:
   *  1. dates are not supported
   *    a. use strings as data type for now
   *    b. pre-calculate date operation
   */
  const auto query =
      "SELECT c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address, "
      "c_phone, c_comment\n"
      "FROM customer, orders, lineitem, nation\n"
      "WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= '1993-10-01'\n"
      "AND o_orderdate < '1994-01-01' AND l_returnflag = 'R' AND c_nationkey = n_nationkey\n"
      "GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment\n"
      "ORDER BY revenue desc;";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch10.tbl", 2);
  execute_and_check(query, expected_result, true);
}

}  // namespace opossum
