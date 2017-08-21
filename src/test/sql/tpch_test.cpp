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
      std::cout << parse_result.errorMsg() << std::endl;
      std::cout << "ErrorLine: " << parse_result.errorLine() << std::endl;
      std::cout << "ErrorColumn: " << parse_result.errorColumn() << std::endl;
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
TEST_F(TPCHTest, TPCH1) {
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
      R"(SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price,
      sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
      sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty,
      avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order
      FROM lineitem
      WHERE l_shipdate <= '1998-12-01'
      GROUP BY l_returnflag, l_linestatus
      ORDER BY l_returnflag, l_linestatus;)";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch1.tbl", 2);
  execute_and_check(query, expected_result, true);
}

// Enable once we support Subselects in WHERE condition
TEST_F(TPCHTest, TPCH2) {
  /**
   * Original:
   *
   * SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
   * FROM part, supplier, partsupp, nation, region
   * where
   *    p_partkey = ps_partkey
   *    AND s_suppkey = ps_suppkey
   *    AND p_size = [SIZE]
   *    AND p_type like '%[TYPE]'
   *    AND s_nationkey = n_nationkey
   *    AND n_regionkey = r_regionkey
   *    AND r_name = '[REGION]'
   *    AND ps_supplycost = (
   *        SELECT min(ps_supplycost)
   *        FROM partsupp, supplier, nation, region
   *        where
   *            p_partkey = ps_partkey
   *            AND s_suppkey = ps_suppkey
   *            AND s_nationkey = n_nationkey
   *            AND n_regionkey = r_regionkey
   *            AND r_name = '[REGION]'
   *        )
   * order by s_acctbal desc, n_name, s_name, p_partkey;
   *
   * Changes:
   *  1. Random values are hardcoded
   */
  const auto query =
    R"(SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
       FROM "part", supplier, partsupp, nation, region
       WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 15 AND p_type like '%BRASS' AND
       s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE' AND
       ps_supplycost = (SELECT min(ps_supplycost) FROM partsupp, supplier, nation, region
       WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey
       AND n_regionkey = r_regionkey AND r_name = 'EUROPE') order by s_acctbal desc, n_name, s_name, p_partkey;)";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch2.tbl", 2);
  execute_and_check(query, expected_result, true);
}

// Enable with new Projection operator
TEST_F(TPCHTest, TPCH3) {
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
      "SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority \n"
      "FROM customer, orders, lineitem\n"
      "WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey\n"
      "AND o_orderdate < '1995-03-15' AND l_shipdate > '1995-03-15'\n"
      "GROUP BY l_orderkey, o_orderdate, o_shippriority\n"
      "ORDER BY revenue desc, o_orderdate;";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch3.tbl", 2);
  execute_and_check(query, expected_result, true);
}

TEST_F(TPCHTest, TPCH4) {
  /**
   * Original:
   *
   * SELECT
   *    o_orderpriority,
   *    count(*) as order_count
   * FROM orders
   * where
   *    o_orderdate >= date '[DATE]'
   *    and o_orderdate < date '[DATE]' + interval '3' month
   *    and exists (
   *        SELECT *
   *        FROM lineitem
   *        where
   *            l_orderkey = o_orderkey
   *            and l_commitdate < l_receiptdate
   *        )
   * group by o_orderpriority
   * order by o_orderpriority;
   *
   * Changes:
   *  1. Random values are hardcoded
   *  2. dates are not supported
   *    a. use strings as data type for now
   *    b. pre-calculate date operation
   */
  const auto query =
    "SELECT o_orderpriority, count(*) as order_count FROM orders where o_orderdate >= '1993-07-01' and\n"
      "o_orderdate < '1993-10-01' and exists (\n"
      "SELECT *FROM lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate)\n"
      "group by o_orderpriority order by o_orderpriority;";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch4.tbl", 2);
  execute_and_check(query, expected_result, true);
}

// Enable with new Projection operator
TEST_F(TPCHTest, TPCH5) {
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
      "WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey\n"
      "AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA' AND o_orderdate >= '1994-01-01'\n"
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

TEST_F(TPCHTest, TPCH7) {
  /**
    * Original:
    *
    * SELECT supp_nation, cust_nation, l_year, sum(volume) as revenue
    * FROM (
    *   SELECT
    *       n1.n_name as supp_nation,
    *       n2.n_name as cust_nation,
    *       extract(year FROM l_shipdate) as l_year,
    *       l_extendedprice * (1 - l_discount) as volume
    *   FROM supplier, lineitem, orders, customer, nation n1, nation n2
    *   where
    *       s_suppkey = l_suppkey
    *       and o_orderkey = l_orderkey
    *       and c_custkey = o_custkey
    *       and s_nationkey = n1.n_nationkey
    *       and c_nationkey = n2.n_nationkey
    *       and (
    *           (n1.n_name = '[NATION1]' and n2.n_name = '[NATION2]')
    *           or
    *           (n1.n_name = '[NATION2]' and n2.n_name = '[NATION1]'))
    *       and l_shipdate between date '1995-01-01' and date '1996-12-31'
    *   ) as shipping
    * group by supp_nation, cust_nation, l_year
    * order by supp_nation, cust_nation, l_year;
    *
    * Changes:
    *  1. Random values are hardcoded
    *  2. dates are not supported
    *    a. use strings as data type for now
    *    b. pre-calculate date operation
    */
  const auto query =
    "SELECT supp_nation, cust_nation, l_year, sum(volume) as revenue FROM (SELECT n1.n_name as supp_nation, "
      "n2.n_name as cust_nation, extract(year FROM l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume "
      "FROM supplier, lineitem, orders, customer, nation n1, nation n2 where s_suppkey = l_suppkey and "
      "o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and "
      "c_nationkey = n2.n_nationkey and ((n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE') or "
      "(n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')) and l_shipdate between '1995-01-01' and "
      "'1996-12-31') as shipping group by supp_nation, cust_nation, l_year "
      "order by supp_nation, cust_nation, l_year;";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch7.tbl", 2);
  execute_and_check(query, expected_result, true);
}

TEST_F(TPCHTest, TPCH8) {
  /**
     * Original:
     *
     * SELECT o_year,
     *      sum(case
     *              when nation = '[NATION]'
     *              then volume
     *              else 0
     *           end) / sum(volume) as mkt_share
     * FROM (
     *      SELECT
     *          extract(year FROM o_orderdate) as o_year,
     *          l_extendedprice * (1-l_discount) as volume,
     *          n2.n_name as nation
     *      FROM
     *          part,
     *          supplier,
     *          lineitem,
     *          orders,
     *          customer,
     *          nation n1,
     *          nation n2,
     *          region
     *      where
     *          p_partkey = l_partkey
     *          and s_suppkey = l_suppkey
     *          and l_orderkey = o_orderkey
     *          and o_custkey = c_custkey
     *          and c_nationkey = n1.n_nationkey
     *          and n1.n_regionkey = r_regionkey
     *          and r_name = '[REGION]'
     *          and s_nationkey = n2.n_nationkey
     *          and o_orderdate between date '1995-01-01' and date '1996-12-31'
     *          and p_type = '[TYPE]'
     *      ) as all_nations
     * group by o_year
     * order by o_year;
     *
     * Changes:
     *  1. Random values are hardcoded
     *  2. dates are not supported
     *    a. use strings as data type for now
     */
  const auto query =
    "SELECT o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share "
      "FROM (SELECT extract(year FROM o_orderdate) as o_year, l_extendedprice * (1-l_discount) as volume, "
      "n2.n_name as nation FROM \"part\", supplier, lineitem, orders, customer, nation n1, nation n2, region "
      "where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and "
      "o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and "
      "r_name = 'AMERICA' and s_nationkey = n2.n_nationkey and o_orderdate between '1995-01-01' "
      "and '1996-12-31' and p_type = 'ECONOMY ANODIZED STEEL') as all_nations group by o_year order by o_year;";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch8.tbl", 2);
  execute_and_check(query, expected_result, true);
}

TEST_F(TPCHTest, TPCH9) {
  /**
   * Original:
   *
   * SELECT
   *    nation,
   *    o_year,
   *    sum(amount) as sum_profit
   * FROM (
   *    SELECT
   *        n_name as nation,
   *        extract(year FROM o_orderdate) as o_year,
   *        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
   *    FROM part, supplier, lineitem, partsupp, orders, nation
   *    where
   *        s_suppkey = l_suppkey
   *        and ps_suppkey = l_suppkey
   *        and ps_partkey = l_partkey
   *        and p_partkey = l_partkey
   *        and o_orderkey = l_orderkey
   *        and s_nationkey = n_nationkey
   *        and p_name like '%[COLOR]%'
   *    ) as profit
   * group by nation, o_year
   * order by nation, o_year desc;
   *
   * Changes:
   *  1. Random values are hardcoded
   */
  const auto query =
    "SELECT nation, o_year, sum(amount) as sum_profit FROM (SELECT n_name as nation, "
      "extract(year FROM o_orderdate) as o_year, "
      "l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount "
      "FROM \"part\", supplier, lineitem, partsupp, orders, nation where s_suppkey = l_suppkey "
      "and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey "
      "and s_nationkey = n_nationkey and p_name like '%green%') as profit "
      "group by nation, o_year order by nation, o_year desc;";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch9.tbl", 2);
  execute_and_check(query, expected_result, true);
}

// Enable with new Projection operator
TEST_F(TPCHTest, TPCH10) {
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
   *  1. Random values are hardcoded
   *  2. dates are not supported
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

TEST_F(TPCHTest, TPCH11) {
  /**
   * Original:
   *
   * SELECT
   *    ps_partkey,
   *    sum(ps_supplycost * ps_availqty) as value
   * FROM partsupp, supplier, nation
   * where
   *    ps_suppkey = s_suppkey
   *    and s_nationkey = n_nationkey
   *    and n_name = '[NATION]'
   * group by ps_partkey
   * having sum(ps_supplycost * ps_availqty) > (
   *    SELECT sum(ps_supplycost * ps_availqty) * [FRACTION]
   *    FROM partsupp, supplier, nation
   *    where
   *        ps_suppkey = s_suppkey
   *        and s_nationkey = n_nationkey
   *        and n_name = '[NATION]'
   *    )
   * order by value desc;
   *
   * Changes:
   *  1. Random values are hardcoded

   */
  const auto query =
    "SELECT ps_partkey, sum(ps_supplycost * ps_availqty) as value FROM partsupp, supplier, nation "
      "where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY' "
      "group by ps_partkey having sum(ps_supplycost * ps_availqty) > ("
      "SELECT sum(ps_supplycost * ps_availqty) * 0.0001 FROM partsupp, supplier, nation "
      "where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY') order by value desc;";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch11.tbl", 2);
  execute_and_check(query, expected_result, true);
}

TEST_F(TPCHTest, TPCH12) {
  /**
   * Original:
   *
   * SELECT
   *    l_shipmode,
   *    sum(case
   *            when o_orderpriority ='1-URGENT' or o_orderpriority ='2-HIGH'
   *            then 1
   *            else 0
   *        end) as high_line_count,
   *    sum(case
   *            when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH'
   *            then 1
   *            else 0
   *        end) as low_line_count
   * FROM orders, lineitem
   * where
   *    o_orderkey = l_orderkey
   *    and l_shipmode in ('[SHIPMODE1]', '[SHIPMODE2]')
   *    and l_commitdate < l_receiptdate
   *    and l_shipdate < l_commitdate
   *    and l_receiptdate >= date '[DATE]'
   *    and l_receiptdate < date '[DATE]' + interval '1' year
   * group by l_shipmode
   * order by l_shipmode;
   *
   * Changes:
   *  1. Random values are hardcoded
   *  2. dates are not supported
   *    a. use strings as data type for now
   *    b. pre-calculate date operation

   */
  const auto query =
    "SELECT l_shipmode, sum(case when o_orderpriority ='1-URGENT' or o_orderpriority ='2-HIGH' then 1 else 0 end) "
      "as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and "
      "o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count FROM orders, lineitem "
      "where o_orderkey = l_orderkey and l_shipmode in ('MAIL','SHIP') and l_commitdate < l_receiptdate "
      "and l_shipdate < l_commitdate and l_receiptdate >= '1994-01-01' and "
      "l_receiptdate < '1995-01-01' group by l_shipmode order by l_shipmode;";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch12.tbl", 2);
  execute_and_check(query, expected_result, true);
}

TEST_F(TPCHTest, TPCH13) {
  /**
   * Original:
   *
   * SELECT c_count, count(*) as custdist
   * FROM (
   *    SELECT c_custkey, count(o_orderkey)
   *    FROM customer left outer join orders
   *    on
   *        c_custkey = o_custkey
   *        and o_comment not like ‘%special%requests%’
   *    group by c_custkey
   *    ) as c_orders (c_custkey, c_count)
   * group by c_count
   * order by custdist desc, c_count desc;
   *
   * Changes:
   *  1. Random values are hardcoded

   */
  const auto query =
    "SELECT c_count, count(*) as custdist FROM (SELECT c_custkey, count(o_orderkey) "
      "FROM customer left outer join orders on c_custkey = o_custkey and o_comment not like ‘%[WORD1]%[WORD2]%’ "
      "group by c_custkey)as c_orders (c_custkey, c_count) group by c_count order by custdist desc, c_count desc;";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch13.tbl", 2);
  execute_and_check(query, expected_result, true);
}

TEST_F(TPCHTest, TPCH14) {
  /**
   * Original:
   *
   * SELECT 100.00 * sum(case
   *                        when p_type like 'PROMO%'
   *                        then l_extendedprice*(1-l_discount)
   *                        else 0
   *                     end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
   * FROM lineitem, part
   * where
   *    l_partkey = p_partkey
   *    and l_shipdate >= date '[DATE]'
   *    and l_shipdate < date '[DATE]' + interval '1' month;
   *
   * Changes:
   *  1. Random values are hardcoded
   *  2. dates are not supported
   *    a. use strings as data type for now
   *    b. pre-calculate date operation

   */
  const auto query =
    "SELECT 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice*(1-l_discount) else 0 end)"
      "/ sum(l_extendedprice * (1 - l_discount)) as promo_revenue FROM lineitem, \"part\" where l_partkey = p_partkey "
      "and l_shipdate >= '1995-09-01' and l_shipdate < 1995-10-01;";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch14.tbl", 2);
  execute_and_check(query, expected_result, true);
}

// We do not support Views yet
TEST_F(TPCHTest, DISABLED_TPCH15) {
  /**
   * Original:
   *
   * create view revenue[STREAM_ID] (supplier_no, total_revenue) as
   *    SELECT l_suppkey, sum(l_extendedprice * (1 - l_discount))
   *    FROM lineitem
   *    where
   *        l_shipdate >= date '[DATE]'
   *        and l_shipdate < date '[DATE]' + interval '3' month
   *    group by l_suppkey;
   *
   * SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
   * FROM supplier, revenue[STREAM_ID]
   * where
   *    s_suppkey = supplier_no
   *    and total_revenue = (
   *        SELECT max(total_revenue)
   *        FROM revenue[STREAM_ID]
   *    )
   * order by s_suppkey;
   *
   * drop view revenue[STREAM_ID];
   *
   * Changes:
   *  1. Random values are hardcoded
   *  2. dates are not supported
   *    a. use strings as data type for now
   *    b. pre-calculate date operation

   */
  const auto query =
    "create view revenue[STREAM_ID] (supplier_no, total_revenue) as SELECT l_suppkey, "
      "sum(l_extendedprice * (1 - l_discount)) FROM lineitem where l_shipdate >= date '[DATE]' "
      "and l_shipdate < date '[DATE]' + interval '3' month group by l_suppkey; "
      ""
      "SELECT s_suppkey, s_name, s_address, s_phone, total_revenue FROM supplier, revenue[STREAM_ID] "
      "where s_suppkey = supplier_no and total_revenue = (SELECT max(total_revenue) "
      "FROM revenue[STREAM_ID]) order by s_suppkey; "
      ""
      "drop view revenue[STREAM_ID];";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch15.tbl", 2);
  execute_and_check(query, expected_result, true);
}

TEST_F(TPCHTest, TPCH16) {
  /**
   * Original:
   *
   * SELECT p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt
   * FROM partsupp, part
   * where
   *    p_partkey = ps_partkey
   *    and p_brand <> '[BRAND]'
   *    and p_type not like '[TYPE]%'
   *    and p_size in ([SIZE1], [SIZE2], [SIZE3], [SIZE4], [SIZE5], [SIZE6], [SIZE7], [SIZE8])
   *    and ps_suppkey not in (
   *        SELECT s_suppkey
   *        FROM supplier
   *        where s_comment like '%Customer%Complaints%'
   *    )
   * group by p_brand, p_type, p_size
   * order by supplier_cnt desc, p_brand, p_type, p_size;
   *
   * Changes:
   *  1. Random values are hardcoded

   */
  const auto query =
    "SELECT p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt "
      "FROM partsupp, \"part\" where p_partkey = ps_partkey and p_brand <> 'Brand#45' and p_type not like 'MEDIUM POLISHED%'"
      "and p_size in (49, 14, 23, 45, 19, 3, 36, 9) "
      "and ps_suppkey not in (SELECT s_suppkey FROM supplier where s_comment like '%Customer%Complaints%') "
      "group by p_brand, p_type, p_size order by supplier_cnt desc, p_brand, p_type, p_size;";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch16.tbl", 2);
  execute_and_check(query, expected_result, true);
}

TEST_F(TPCHTest, TPCH17) {
  /**
   * Original:
   *
   * SELECT sum(l_extendedprice) / 7.0 as avg_yearly
   * FROM lineitem, part
   * where
   *    p_partkey = l_partkey
   *    and p_brand = '[BRAND]'
   *    and p_container = '[CONTAINER]'
   *    and l_quantity < (
   *        SELECT 0.2 * avg(l_quantity)
   *        FROM lineitem
   *        where l_partkey = p_partkey
   *    );
   *
   * Changes:
   *  1. Random values are hardcoded

   */
  const auto query =
    "SELECT sum(l_extendedprice) / 7.0 as avg_yearly FROM lineitem, \"part\" where p_partkey = l_partkey "
      "and p_brand = 'Brand#23' and p_container = 'MED BOX' and l_quantity < (SELECT 0.2 * avg(l_quantity) "
      "FROM lineitem where l_partkey = p_partkey);";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch17.tbl", 2);
  execute_and_check(query, expected_result, true);
}

TEST_F(TPCHTest, TPCH18) {
  /**
   * Original:
   *
   * SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
   * FROM customer, orders, lineitem
   * where
   *    o_orderkey in (
   *        SELECT l_orderkey
   *        FROM lineitem
   *        group by l_orderkey
   *        having sum(l_quantity) > [QUANTITY]
   *    )
   *    and c_custkey = o_custkey
   *    and o_orderkey = l_orderkey
   * group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
   * order by o_totalprice desc, o_orderdate;
   *
   * Changes:
   *  1. Random values are hardcoded

   */
  const auto query =
    "SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) "
      "FROM customer, orders, lineitem where o_orderkey in (SELECT l_orderkey FROM lineitem "
      "group by l_orderkey having sum(l_quantity) > 300) and c_custkey = o_custkey and o_orderkey = l_orderkey "
      "group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate;";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch18.tbl", 2);
  execute_and_check(query, expected_result, true);
}

TEST_F(TPCHTest, TPCH19) {
  /**
   * Original:
   *
   * SELECT sum(l_extendedprice * (1 - l_discount) ) as revenue
   * FROM lineitem, part
   * where (
   *        p_partkey = l_partkey
   *        and p_brand = ‘[BRAND1]’
   *        and p_container in ( ‘SM CASE’, ‘SM BOX’, ‘SM PACK’, ‘SM PKG’)
   *        and l_quantity >= [QUANTITY1]
   *        and l_quantity <= [QUANTITY1] + 10
   *        and p_size between 1 and 5
   *        and l_shipmode in (‘AIR’, ‘AIR REG’)
   *        and l_shipinstruct = ‘DELIVER IN PERSON’
   *    ) or (
   *        p_partkey = l_partkey
   *        and p_brand = ‘[BRAND2]’
   *        and p_container in (‘MED BAG’, ‘MED BOX’, ‘MED PKG’, ‘MED PACK’)
   *        and l_quantity >= [QUANTITY2]
   *        and l_quantity <= [QUANTITY2] + 10
   *        and p_size between 1 and 10
   *        and l_shipmode in (‘AIR’, ‘AIR REG’)
   *        and l_shipinstruct = ‘DELIVER IN PERSON’
   *    ) or (
   *        p_partkey = l_partkey
   *        and p_brand = ‘[BRAND3]’
   *        and p_container in ( ‘LG CASE’, ‘LG BOX’, ‘LG PACK’, ‘LG PKG’)
   *        and l_quantity >= [QUANTITY3]
   *        and l_quantity <= [QUANTITY3] + 10
   *        and p_size between 1 and 15
   *        and l_shipmode in (‘AIR’, ‘AIR REG’)
   *        and l_shipinstruct = ‘DELIVER IN PERSON’
   *    );
   *
   * Changes:
   *  1. Random values are hardcoded

   */
  const auto query =
    "SELECT sum(l_extendedprice * (1 - l_discount) ) as revenue FROM lineitem, \"part\" where (p_partkey = l_partkey "
      "and p_brand = ‘Brand#12’ and p_container in ( ‘SM CASE’, ‘SM BOX’, ‘SM PACK’, ‘SM PKG’) and "
      "l_quantity >= 1 and l_quantity <= 1 + 10 and p_size between 1 and 5 and l_shipmode "
      "in (‘AIR’, ‘AIR REG’) and l_shipinstruct = ‘DELIVER IN PERSON’) or (p_partkey = l_partkey "
      "and p_brand = ‘Brand#23’ and p_container in (‘MED BAG’, ‘MED BOX’, ‘MED PKG’, ‘MED PACK’) "
      "and l_quantity >= 10 and l_quantity <= 10 + 10 and p_size between 1 and 10 "
      "and l_shipmode in (‘AIR’, ‘AIR REG’) and l_shipinstruct = ‘DELIVER IN PERSON’) or "
      "(p_partkey = l_partkey and p_brand = ‘Brand#34’ and p_container in ( ‘LG CASE’, ‘LG BOX’, ‘LG PACK’, ‘LG PKG’) "
      "and l_quantity >= 20 and l_quantity <= 20 + 10 and p_size between 1 and 15 and l_shipmode in "
      "(‘AIR’, ‘AIR REG’) and l_shipinstruct = ‘DELIVER IN PERSON’);";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch19.tbl", 2);
  execute_and_check(query, expected_result, true);
}

TEST_F(TPCHTest, TPCH20) {
  /**
   * Original:
   *
   * SELECT s_name, s_address
   * FROM supplier, nation
   * where
   *    s_suppkey in (
   *        SELECT ps_suppkey
   *        FROM partsupp
   *        where ps_partkey in (
   *          SELECT p_partkey
   *          FROM part
   *          where
   *            p_name like '[COLOR]%')
   *            and ps_availqty > (
   *                SELECT 0.5 * sum(l_quantity)
   *                FROM lineitem
   *                where
   *                    l_partkey = ps_partkey
   *                    and l_suppkey = ps_suppkey
   *                    and l_shipdate >= date('[DATE]’)
   *                    and l_shipdate < date('[DATE]’) + interval ‘1’ year
   *            )
   *        )
   *    and s_nationkey = n_nationkey
   *    and n_name = '[NATION]'
   * order by s_name;
   *
   * Changes:
   *  1. Random values are hardcoded
   *  2. dates are not supported
   *    a. use strings as data type for now
   *    b. pre-calculate date operation

   */
  const auto query =
    "SELECT s_name, s_address FROM supplier, nation where s_suppkey in (SELECT ps_suppkey FROM partsupp "
      "where ps_partkey in (SELECT p_partkey FROM \"part\" where p_name like 'forest%') and ps_availqty > "
      "(SELECT 0.5 * sum(l_quantity) FROM lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and "
      "l_shipdate >= '1994-01-01' and l_shipdate < '1995-01-01')) and s_nationkey = n_nationkey "
      "and n_name = 'CANADA' order by s_name;";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch20.tbl", 2);
  execute_and_check(query, expected_result, true);
}

TEST_F(TPCHTest, TPCH21) {
  /**
   * Original:
   *
   * SELECT s_name, count(*) as numwait
   * FROM supplier, lineitem l1, orders, nation
   * where
   *    s_suppkey = l1.l_suppkey
   *    and o_orderkey = l1.l_orderkey
   *    and o_orderstatus = 'F'
   *    and l1.l_receiptdate > l1.l_commitdate
   *    and exists (
   *        SELECT *
   *        FROM lineitem l2
   *        where
   *            l2.l_orderkey = l1.l_orderkey
   *            and l2.l_suppkey <> l1.l_suppkey
   *    ) and not exists (
   *        SELECT *
   *        FROM lineitem l3
   *        where
   *            l3.l_orderkey = l1.l_orderkey
   *            and l3.l_suppkey <> l1.l_suppkey
   *            and l3.l_receiptdate > l3.l_commitdate
   *    )
   *    and s_nationkey = n_nationkey
   *    and n_name = '[NATION]'
   * group by s_name
   * order by numwait desc, s_name;
   *
   * Changes:
   *  1. Random values are hardcoded
   *  2. dates are not supported
   *    a. use strings as data type for now
   *    b. pre-calculate date operation

   */
  const auto query =
    "SELECT s_name, count(*) as numwait FROM supplier, lineitem l1, orders, nation where s_suppkey = l1.l_suppkey "
      "and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate and exists "
      "(SELECT * FROM lineitem l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey) and not exists "
      "(SELECT * FROM lineitem l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey and "
      "l3.l_receiptdate > l3.l_commitdate ) and s_nationkey = n_nationkey and n_name = 'SAUDI ARABIA' group by s_name "
      "order by numwait desc, s_name;";
  const auto expected_result = load_table("src/test/tables/tpch/results/tpch21.tbl", 2);
  execute_and_check(query, expected_result, true);
}

}  // namespace opossum
