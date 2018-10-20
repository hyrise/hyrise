#include "tpch_queries.hpp"

namespace {

/**
 * TPC-H 1
 *
 * Original:
 *
 * SELECT
 *      l_returnflag,
 *      l_linestatus,
 *      sum(l_quantity) as sum_qty,
 *      sum(l_extendedprice) as sum_base_price,
 *      sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
 *      sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
 *      avg(l_quantity) as avg_qty,
 *      avg(l_extendedprice) as avg_price,
 *      avg(l_discount) as avg_disc,
 *      count(*) as count_order
 * FROM
 *      lineitem
 * WHERE
 *      l_shipdate <= date '1998-12-01' - interval '[DELTA]' day (3)
 * GROUP BY
 *      l_returnflag, l_linestatus
 * ORDER BY
 *      l_returnflag, l_linestatus
 *
 * Changes:
 *  1. dates are not supported
 *    a. use strings as data type for now
 *    b. pre-calculate date operation
 */
const char* const tpch_query_1 =
    R"(SELECT l_returnflag, l_linestatus, SUM(l_quantity) as sum_qty, SUM(l_extendedprice) as sum_base_price,
      SUM(l_extendedprice*(1-l_discount)) as sum_disc_price,
      SUM(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, AVG(l_quantity) as avg_qty,
      AVG(l_extendedprice) as avg_price, AVG(l_discount) as avg_disc, COUNT(*) as count_order
      FROM lineitem
      WHERE l_shipdate <= '1998-09-02'
      GROUP BY l_returnflag, l_linestatus
      ORDER BY l_returnflag, l_linestatus;)";

/**
 * TPC-H 2
 *
 * Original:
 *
 * SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
 * FROM part, supplier, partsupp, nation, region
 * WHERE
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
 *        WHERE
 *            p_partkey = ps_partkey
 *            AND s_suppkey = ps_suppkey
 *            AND s_nationkey = n_nationkey
 *            AND n_regionkey = r_regionkey
 *            AND r_name = '[REGION]'
 *        )
 * ORDER BY s_acctbal DESC, n_name, s_name, p_partkey;
 *
 * Changes:
 *  1. Random values are hardcoded
 */
const char* const tpch_query_2 =
    R"(SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
       FROM part, supplier, partsupp, nation, region
       WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 15 AND p_type like '%BRASS' AND
       s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE' AND
       ps_supplycost = (SELECT min(ps_supplycost) FROM partsupp, supplier, nation, region
       WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey
       AND n_regionkey = r_regionkey AND r_name = 'EUROPE') ORDER BY s_acctbal DESC, n_name, s_name, p_partkey;)";

/**
 * TPC-H 3
 *
 * Original:
 *
 * SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority
 * FROM customer, orders, lineitem
 * WHERE c_mktsegment = '[SEGMENT]' AND c_custkey = o_custkey AND l_orderkey = o_orderkey
 * AND o_orderdate < date '[DATE]' AND l_shipdate > date '[DATE]'
 * GROUP BY l_orderkey, o_orderdate, o_shippriority
 * ORDER BY revenue DESC, o_orderdate;
 *
 * Changes:
 *  1. Random values are hardcoded
 */
const char* const tpch_query_3 =
    R"(SELECT l_orderkey, SUM(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority
      FROM customer, orders, lineitem
      WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey
      AND o_orderdate < '1995-03-15' AND l_shipdate > '1995-03-15'
      GROUP BY l_orderkey, o_orderdate, o_shippriority
      ORDER BY revenue DESC, o_orderdate;)";

/**
 * TPC-H 4
 *
 * Original:
 *
 * SELECT
 *    o_orderpriority,
 *    count(*) as order_count
 * FROM orders
 * WHERE
 *    o_orderdate >= date '[DATE]'
 *    AND o_orderdate < date '[DATE]' + interval '3' month
 *    AND exists (
 *        SELECT *
 *        FROM lineitem
 *        WHERE
 *            l_orderkey = o_orderkey
 *            AND l_commitdate < l_receiptdate
 *        )
 * GROUP BY o_orderpriority
 * ORDER BY o_orderpriority;
 *
 * Changes:
 *  1. Random values are hardcoded
 *  2. dates are not supported
 *    a. use strings as data type for now
 *    b. pre-calculate date operation
 */
const char* const tpch_query_4 =
    R"(SELECT o_orderpriority, count(*) as order_count FROM orders WHERE o_orderdate >= '1996-07-01' AND
    o_orderdate < '1996-10-01' AND exists (
    SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)
    GROUP BY o_orderpriority ORDER BY o_orderpriority;)";

/**
 * TPC-H 5
 *
 * Original:
 *
 * SELECT
 *      n_name,
 *      sum(l_extendedprice * (1 - l_discount)) as revenue
 * FROM
 *      customer,
 *      orders,
 *      lineitem,
 *      supplier,
 *      nation,
 *      region
 * WHERE
 *      c_custkey = o_custkey AND
 *      l_orderkey = o_orderkey AND
 *      l_suppkey = s_suppkey AND
 *      c_nationkey = s_nationkey AND
 *      s_nationkey = n_nationkey AND
 *      n_regionkey = r_regionkey AND
 *      r_name = '[REGION]' AND
 *      o_orderdate >= date '[DATE]' AND
 *      o_orderdate < date '[DATE]' + interval '1' year
 * GROUP BY
 *      n_name
 * ORDER BY
 *      revenue DESC;
 *
 * Changes:
 *  1. Random values are hardcoded
 *  2. dates are not supported
 *    a. use strings as data type for now
 *    b. pre-calculate date operation
 */
const char* const tpch_query_5 =
    R"(SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) as revenue
      FROM customer, orders, lineitem, supplier, nation, region
      WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
      AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'AMERICA' AND o_orderdate >= '1994-01-01'
      AND o_orderdate < '1995-01-01'
      GROUP BY n_name
      ORDER BY revenue DESC;)";

/**
 * TPC-H 6
 *
 * Original:
 *
 * SELECT sum(L_EXTENDEDPRICE*L_DISCOUNT) AS REVENUE
 * FROM LINEITEM
 * WHERE L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE < dateadd(yy, 1, cast('1994-01-01' as datetime))
 * AND L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01 AND L_QUANTITY < 24
 *
 * Changes:
 *  1. dates are not supported
 *    a. use strings as data type for now
 *    b. pre-calculate date operation
 *  2. ".06 + 0.01" is less than "0.07" in sqlite, but >= "0.07" in hyrise.
 *    a. Add a small offset ".06 + 0.01001" to include records with a l_discount of "0.07"
 */
const char* const tpch_query_6 =
    R"(SELECT sum(l_extendedprice*l_discount) AS REVENUE
      FROM lineitem
      WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
      AND l_discount BETWEEN .06 - 0.01 AND .06 + 0.01001 AND l_quantity < 24;)";

/**
 * TPC-H 7
 *
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
 *   WHERE
 *       s_suppkey = l_suppkey
 *       AND o_orderkey = l_orderkey
 *       AND c_custkey = o_custkey
 *       AND s_nationkey = n1.n_nationkey
 *       AND c_nationkey = n2.n_nationkey
 *       AND (
 *           (n1.n_name = '[NATION1]' AND n2.n_name = '[NATION2]')
 *           or
 *           (n1.n_name = '[NATION2]' AND n2.n_name = '[NATION1]'))
 *       AND l_shipdate between date '1995-01-01' AND date '1996-12-31'
 *   ) as shipping
 * GROUP BY supp_nation, cust_nation, l_year
 * ORDER BY supp_nation, cust_nation, l_year;
 *
 * Changes:
 *  1. Random values are hardcoded
 *  2. dates are not supported
 *    a. use strings as data type for now
 *    b. pre-calculate date operation
 *  3. Extract is not supported
 *    a. Use SUBSTR instead (because our date columns are strings AND SQLite doesn't support EXTRACT)
 */
const char* const tpch_query_7 =
    R"(SELECT
          supp_nation,
          cust_nation,
          l_year,
          SUM(volume) as revenue
      FROM
          (SELECT
              n1.n_name as supp_nation,
              n2.n_name as cust_nation,
              SUBSTR(l_shipdate, 0, 4) as l_year,
              l_extendedprice * (1 - l_discount) as volume
          FROM
              supplier,
              lineitem,
              orders,
              customer,
              nation n1,
              nation n2
          WHERE
              s_suppkey = l_suppkey AND
              o_orderkey = l_orderkey AND
              c_custkey = o_custkey AND
              s_nationkey = n1.n_nationkey AND
              c_nationkey = n2.n_nationkey AND
              ((n1.n_name = 'IRAN' AND n2.n_name = 'IRAQ') OR
               (n1.n_name = 'IRAQ' AND n2.n_name = 'IRAN')) AND
              l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
          ) as shipping
      GROUP BY
          supp_nation, cust_nation, l_year
      ORDER BY
          supp_nation, cust_nation, l_year;)";

/**
 * TPC-H 8
 *
 * Original:
 *
 * SELECT o_year,
 *      sum(case
 *              when nation = '[NATION]'
 *              then volume
 *              else 0
 *           end)
 *           / sum(volume) as mkt_share
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
 *      WHERE
 *          p_partkey = l_partkey
 *          AND s_suppkey = l_suppkey
 *          AND l_orderkey = o_orderkey
 *          AND o_custkey = c_custkey
 *          AND c_nationkey = n1.n_nationkey
 *          AND n1.n_regionkey = r_regionkey
 *          AND r_name = '[REGION]'
 *          AND s_nationkey = n2.n_nationkey
 *          AND o_orderdate between date '1995-01-01' AND date '1996-12-31'
 *          AND p_type = '[TYPE]'
 *      ) as all_nations
 * GROUP BY o_year
 * ORDER BY o_year;
 *
 * Changes:
 *  1. Random values are hardcoded
 *  2. dates are not supported
 *    a. use strings as data type for now
 *  3. Extract is not supported
 *    a. Use SUBSTR instead (because our date columns are strings AND SQLite doesn't support EXTRACT)
 */
const char* const tpch_query_8 =
    R"(SELECT o_year, SUM(case when nation = 'BRAZIL' then volume else 0 end) / SUM(volume) as mkt_share
     FROM (SELECT SUBSTR(o_orderdate, 0, 4) as o_year, l_extendedprice * (1-l_discount) as volume,
     n2.n_name as nation FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
     WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND
     o_custkey = c_custkey AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey AND
     r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey AND o_orderdate between '1995-01-01'
     AND '1996-12-31' AND p_type = 'ECONOMY ANODIZED STEEL') as all_nations GROUP BY o_year ORDER BY o_year;)";

/**
 * TPC-H 9
 *
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
 *    WHERE
 *        s_suppkey = l_suppkey
 *        AND ps_suppkey = l_suppkey
 *        AND ps_partkey = l_partkey
 *        AND p_partkey = l_partkey
 *        AND o_orderkey = l_orderkey
 *        AND s_nationkey = n_nationkey
 *        AND p_name like '%[COLOR]%'
 *    ) as profit
 * GROUP BY nation, o_year
 * ORDER BY nation, o_year DESC;
 *
 * Changes:
 *  1. Random values are hardcoded
 *  2. Extract is not supported
 *    a. Use SUBSTR instead
 */
const char* const tpch_query_9 =
    R"(SELECT nation, o_year, SUM(amount) as sum_profit FROM (SELECT n_name as nation, SUBSTR(o_orderdate, 0, 4) as o_year,
      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
      FROM part, supplier, lineitem, partsupp, orders, nation WHERE s_suppkey = l_suppkey
      AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey AND p_partkey = l_partkey AND o_orderkey = l_orderkey
      AND s_nationkey = n_nationkey AND p_name like '%green%') as profit
      GROUP BY nation, o_year ORDER BY nation, o_year DESC;)";

/**
 * TPC-H 10
 *
 * Original:
 *
 * SELECT
 *      c_custkey,
 *      c_name,
 *      sum(l_extendedprice * (1 - l_discount)) as revenue,
 *      c_acctbal,
 *      n_name,
 *      c_address,
 *      c_phone,
 *      c_comment
 * FROM
 *      customer,
 *      orders,
 *      lineitem,
 *      nation
 * WHERE
 *      c_custkey = o_custkey AND
 *      l_orderkey = o_orderkey AND
 *      o_orderdate >= date '[DATE]' AND
 *      o_orderdate < date '[DATE]' + interval '3' month AND
 *      l_returnflag = 'R' AND
 *      c_nationkey = n_nationkey
 * GROUP BY
 *      c_custkey,
 *      c_name,
 *      c_acctbal,
 *      c_phone,
 *      n_name,
 *      c_address,
 *      c_comment
 * ORDER BY
 *      revenue DESC;
 *
 * Changes:
 *  1. Random values are hardcoded
 *  2. dates are not supported
 *    a. use strings as data type for now
 *    b. pre-calculate date operation
 */
const char* const tpch_query_10 =
    R"(SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address,
      c_phone, c_comment
      FROM customer, orders, lineitem, nation
      WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= '1993-10-01'
      AND o_orderdate < '1994-01-01' AND l_returnflag = 'R' AND c_nationkey = n_nationkey
      GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
      ORDER BY revenue DESC;)";

/**
 * TPC-H 11
 *
 * Original:
 *
 * SELECT
 *    ps_partkey,
 *    sum(ps_supplycost * ps_availqty) as value
 * FROM partsupp, supplier, nation
 * WHERE
 *    ps_suppkey = s_suppkey
 *    AND s_nationkey = n_nationkey
 *    AND n_name = '[NATION]'
 * GROUP BY ps_partkey
 * having sum(ps_supplycost * ps_availqty) > (
 *    SELECT sum(ps_supplycost * ps_availqty) * [FRACTION]
 *    FROM partsupp, supplier, nation
 *    WHERE
 *        ps_suppkey = s_suppkey
 *        AND s_nationkey = n_nationkey
 *        AND n_name = '[NATION]'
 *    )
 * ORDER BY value DESC;
 *
 * Changes:
 *  1. Random values are hardcoded

 */
const char* const tpch_query_11 =
    R"(SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) as value FROM partsupp, supplier, nation
      WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'
      GROUP BY ps_partkey having SUM(ps_supplycost * ps_availqty) > (
      SELECT SUM(ps_supplycost * ps_availqty) * 0.0001 FROM partsupp, supplier, nation
      WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY') ORDER BY value DESC;)";

/**
 * TPC-H 12
 *
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
 *            when o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH'
 *            then 1
 *            else 0
 *        end) as low_line_count
 * FROM orders, lineitem
 * WHERE
 *    o_orderkey = l_orderkey
 *    AND l_shipmode in ('[SHIPMODE1]', '[SHIPMODE2]')
 *    AND l_commitdate < l_receiptdate
 *    AND l_shipdate < l_commitdate
 *    AND l_receiptdate >= date '[DATE]'
 *    AND l_receiptdate < date '[DATE]' + interval '1' year
 * GROUP BY l_shipmode
 * ORDER BY l_shipmode;
 *
 * Changes:
 *  1. Random values are hardcoded
 *  2. dates are not supported
 *    a. use strings as data type for now
 *    b. pre-calculate date operation
 */
const char* const tpch_query_12 =
    R"(SELECT l_shipmode, SUM(case when o_orderpriority ='1-URGENT' or o_orderpriority ='2-HIGH' then 1 else 0 end)
      as high_line_count, SUM(case when o_orderpriority <> '1-URGENT' AND
      o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count FROM orders, lineitem
      WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL','SHIP') AND l_commitdate < l_receiptdate
      AND l_shipdate < l_commitdate AND l_receiptdate >= '1994-01-01' AND
      l_receiptdate < '1995-01-01' GROUP BY l_shipmode ORDER BY l_shipmode;)";

/**
 * TPC-H 13
 *
 * Original:
 *
 * SELECT c_count, count(*) as custdist
 * FROM (
 *    SELECT c_custkey, count(o_orderkey)
 *    FROM customer left outer join orders
 *    on
 *        c_custkey = o_custkey
 *        AND o_comment not like '%special%requests%'
 *    GROUP BY c_custkey
 *    ) as c_orders (c_custkey, c_count)
 * GROUP BY c_count
 * ORDER BY custdist DESC, c_count DESC;
 *
 * Changes:
 *  1. Random values are hardcoded
 *  2. Subselect column aliases are moved into subselect because SQLite does not support aliases at the original position
 */
const char* const tpch_query_13 =
    R"(SELECT c_count, count(*) as custdist FROM (SELECT c_custkey, count(o_orderkey) AS c_count
      FROM customer left outer join orders on c_custkey = o_custkey AND o_comment not like '%special%request%'
      GROUP BY c_custkey) as c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC;)";

/**
 * TPC-H 14
 *
 * Original:
 *
 * SELECT 100.00 * sum(case
 *                        when p_type like 'PROMO%'
 *                        then l_extendedprice*(1-l_discount)
 *                        else 0
 *                     end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
 * FROM lineitem, part
 * WHERE
 *    l_partkey = p_partkey
 *    AND l_shipdate >= date '[DATE]'
 *    AND l_shipdate < date '[DATE]' + interval '1' month;
 *
 * Changes:
 *  1. Random values are hardcoded
 *  2. dates are not supported
 *    a. use strings as data type for now
 *    b. pre-calculate date operation
 */
const char* const tpch_query_14 =
    R"(SELECT 100.00 * SUM(case when p_type like 'PROMO%' then l_extendedprice*(1-l_discount) else 0 end)
      / SUM(l_extendedprice * (1 - l_discount)) as promo_revenue FROM lineitem, part WHERE l_partkey = p_partkey
      AND l_shipdate >= '1995-09-01' AND l_shipdate < '1995-10-01';)";

/**
 * TPC-H 15
 *
 * Original:
 *
 * create view revenue[STREAM_ID] (supplier_no, total_revenue) as
 *    SELECT l_suppkey, sum(l_extendedprice * (1 - l_discount))
 *    FROM lineitem
 *    WHERE
 *        l_shipdate >= date '[DATE]'
 *        AND l_shipdate < date '[DATE]' + interval '3' month
 *    GROUP BY l_suppkey;
 *
 * SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
 * FROM supplier, revenue[STREAM_ID]
 * WHERE
 *    s_suppkey = supplier_no
 *    AND total_revenue = (
 *        SELECT max(total_revenue)
 *        FROM revenue[STREAM_ID]
 *    )
 * ORDER BY s_suppkey;
 *
 * drop view revenue[STREAM_ID];
 *
 * Changes:
 *  1. Random values are hardcoded
 *  2. "revenue[STREAM_ID]" renamed to "revenue"
 *  2. dates are not supported
 *    a. use strings as data type for now
 *    b. pre-calculate date operation
 */
const char* const tpch_query_15 =
    R"(create view revenue (supplier_no, total_revenue) as SELECT l_suppkey,
      SUM(l_extendedprice * (1 - l_discount)) FROM lineitem WHERE l_shipdate >= '1993-05-13'
      AND l_shipdate < '1993-08-13' GROUP BY l_suppkey;

      SELECT s_suppkey, s_name, s_address, s_phone, total_revenue FROM supplier, revenue
      WHERE s_suppkey = supplier_no AND total_revenue = (SELECT max(total_revenue)
      FROM revenue) ORDER BY s_suppkey;

      drop view revenue;)";

/**
 * TPC-H 16
 *
 * Original:
 *
 * SELECT p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt
 * FROM partsupp, part
 * WHERE
 *    p_partkey = ps_partkey
 *    AND p_brand <> '[BRAND]'
 *    AND p_type not like '[TYPE]%'
 *    AND p_size in ([SIZE1], [SIZE2], [SIZE3], [SIZE4], [SIZE5], [SIZE6], [SIZE7], [SIZE8])
 *    AND ps_suppkey not in (
 *        SELECT s_suppkey
 *        FROM supplier
 *        WHERE s_comment like '%Customer%Complaints%'
 *    )
 * GROUP BY p_brand, p_type, p_size
 * ORDER BY supplier_cnt DESC, p_brand, p_type, p_size;
 *
 * Changes:
 *  1. Random values are hardcoded

 */
const char* const tpch_query_16 =
    R"(SELECT p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt
      FROM partsupp, part WHERE p_partkey = ps_partkey AND p_brand <> 'Brand#45'
      AND p_type not like 'MEDIUM POLISHED%' AND p_size in (49, 14, 23, 45, 19, 3, 36, 9)
      AND ps_suppkey not in (SELECT s_suppkey FROM supplier WHERE s_comment like '%Customer%Complaints%')
      GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size;)";

/**
 * TPC-H 17
 *
 * Original:
 *
 * SELECT sum(l_extendedprice) / 7.0 as avg_yearly
 * FROM lineitem, part
 * WHERE
 *    p_partkey = l_partkey
 *    AND p_brand = '[BRAND]'
 *    AND p_container = '[CONTAINER]'
 *    AND l_quantity < (
 *        SELECT 0.2 * avg(l_quantity)
 *        FROM lineitem
 *        WHERE l_partkey = p_partkey
 *    );
 *
 * Changes:
 *  1. Random values are hardcoded

 */
const char* const tpch_query_17 =
    R"(SELECT SUM(l_extendedprice) / 7.0 as avg_yearly FROM lineitem, part WHERE p_partkey = l_partkey
      AND p_brand = 'Brand#23' AND p_container = 'MED BOX' AND l_quantity < (SELECT 0.2 * avg(l_quantity)
      FROM lineitem WHERE l_partkey = p_partkey);)";

/**
 * TPC-H 18
 *
 * Original:
 *
 * SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
 * FROM customer, orders, lineitem
 * WHERE
 *    o_orderkey in (
 *        SELECT l_orderkey
 *        FROM lineitem
 *        GROUP BY l_orderkey
 *        having sum(l_quantity) > [QUANTITY]
 *    )
 *    AND c_custkey = o_custkey
 *    AND o_orderkey = l_orderkey
 * GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
 * ORDER BY o_totalprice DESC, o_orderdate;
 *
 * Changes:
 *  1. Random values are hardcoded

 */
const char* const tpch_query_18 =
    R"(SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity)
      FROM customer, orders, lineitem WHERE o_orderkey in (SELECT l_orderkey FROM lineitem
      GROUP BY l_orderkey having SUM(l_quantity) > 300) AND c_custkey = o_custkey AND o_orderkey = l_orderkey
      GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice ORDER BY o_totalprice DESC, o_orderdate;)";

/**
 * TPC-H 19
 *
 * Original:
 *
 * SELECT sum(l_extendedprice * (1 - l_discount) ) as revenue
 * FROM lineitem, part
 * WHERE (
 *        p_partkey = l_partkey
 *        AND p_brand = '[BRAND1]'
 *        AND p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
 *        AND l_quantity >= [QUANTITY1]
 *        AND l_quantity <= [QUANTITY1] + 10
 *        AND p_size between 1 AND 5
 *        AND l_shipmode in ('AIR', 'AIR REG')
 *        AND l_shipinstruct = 'DELIVER IN PERSON'
 *    ) or (
 *        p_partkey = l_partkey
 *        AND p_brand = '[BRAND2]'
 *        AND p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
 *        AND l_quantity >= [QUANTITY2]
 *        AND l_quantity <= [QUANTITY2] + 10
 *        AND p_size between 1 AND 10
 *        AND l_shipmode in ('AIR', 'AIR REG')
 *        AND l_shipinstruct = 'DELIVER IN PERSON'
 *    ) or (
 *        p_partkey = l_partkey
 *        AND p_brand = '[BRAND3]'
 *        AND p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
 *        AND l_quantity >= [QUANTITY3]
 *        AND l_quantity <= [QUANTITY3] + 10
 *        AND p_size between 1 AND 15
 *        AND l_shipmode in ('AIR', 'AIR REG')
 *        AND l_shipinstruct = 'DELIVER IN PERSON'
 *    );
 *
 * Changes:
 *  1. Random values are hardcoded
 *  2. implicit type conversions for arithmetic operations are not supported
 *    a. changed 1 to 1.0 explicitly
 *  3. Extracted "p_partkey = l_partkey" to make JoinDetectionRule work
 */
const char* const tpch_query_19 =
    R"(SELECT SUM(l_extendedprice * (1 - l_discount) ) as revenue FROM lineitem, part WHERE p_partkey = l_partkey AND ((
      p_brand = 'Brand#12' AND p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND
      l_quantity >= 1 AND l_quantity <= 1 + 10 AND p_size between 1 AND 5 AND l_shipmode
      in ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') or (p_brand = 'Brand#23' AND p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
      AND l_quantity >= 10 AND l_quantity <= 10 + 10 AND p_size between 1 AND 10
      AND l_shipmode in ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') or
      (p_brand = 'Brand#34' AND p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
      AND l_quantity >= 20 AND l_quantity <= 20 + 10 AND p_size between 1 AND 15 AND l_shipmode in
      ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON'));)";

/**
 * TPC-H 20
 *
 * Original:
 *
 * SELECT s_name, s_address
 * FROM supplier, nation
 * WHERE
 *    s_suppkey in (
 *        SELECT ps_suppkey
 *        FROM partsupp
 *        WHERE ps_partkey in (
 *          SELECT p_partkey
 *          FROM part
 *          WHERE
 *            p_name like '[COLOR]%')
 *            AND ps_availqty > (
 *                SELECT 0.5 * sum(l_quantity)
 *                FROM lineitem
 *                WHERE
 *                    l_partkey = ps_partkey
 *                    AND l_suppkey = ps_suppkey
 *                    AND l_shipdate >= date('[DATE]')
 *                    AND l_shipdate < date('[DATE]') + interval '1' year
 *            )
 *        )
 *    AND s_nationkey = n_nationkey
 *    AND n_name = '[NATION]'
 * ORDER BY s_name;
 *
 * Changes:
 *  1. Random values are hardcoded
 *  2. dates are not supported
 *    a. use strings as data type for now
 *    b. pre-calculate date operation
 */
const char* const tpch_query_20 =
    R"(SELECT s_name, s_address FROM supplier, nation WHERE s_suppkey in (SELECT ps_suppkey FROM partsupp
      WHERE ps_partkey in (SELECT p_partkey FROM part WHERE p_name like 'forest%') AND ps_availqty >
      (SELECT 0.5 * SUM(l_quantity) FROM lineitem WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey AND
      l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01')) AND s_nationkey = n_nationkey
      AND n_name = 'CANADA' ORDER BY s_name;)";

/**
 * TPC-H 21
 *
 * Original:
 *
 * SELECT s_name, count(*) as numwait
 * FROM supplier, lineitem l1, orders, nation
 * WHERE
 *    s_suppkey = l1.l_suppkey
 *    AND o_orderkey = l1.l_orderkey
 *    AND o_orderstatus = 'F'
 *    AND l1.l_receiptdate > l1.l_commitdate
 *    AND exists (
 *        SELECT *
 *        FROM lineitem l2
 *        WHERE
 *            l2.l_orderkey = l1.l_orderkey
 *            AND l2.l_suppkey <> l1.l_suppkey
 *    ) AND not exists (
 *        SELECT *
 *        FROM lineitem l3
 *        WHERE
 *            l3.l_orderkey = l1.l_orderkey
 *            AND l3.l_suppkey <> l1.l_suppkey
 *            AND l3.l_receiptdate > l3.l_commitdate
 *    )
 *    AND s_nationkey = n_nationkey
 *    AND n_name = '[NATION]'
 * GROUP BY s_name
 * ORDER BY numwait DESC, s_name;
 *
 * Changes:
 *  1. Random values are hardcoded
 *  2. dates are not supported
 *    a. use strings as data type for now
 *    b. pre-calculate date operation

 */
const char* const tpch_query_21 =
    R"(SELECT s_name, count(*) as numwait FROM supplier, lineitem l1, orders, nation WHERE s_suppkey = l1.l_suppkey
      AND o_orderkey = l1.l_orderkey AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate AND exists
      (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey) AND not exists
      (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND
      l3.l_receiptdate > l3.l_commitdate ) AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA' GROUP BY s_name
      ORDER BY numwait DESC, s_name;)";

/**
 * TPC-H 22
 *
 * Original:
 *
 * SELECT
 *     CNTRYCODE,
 *     COUNT(*) AS NUMCUST,
 *     SUM(c_acctbal) AS TOTACCTBAL
 * FROM (
 *     SELECT
 *         SUBSTRING(c_phone,1,2) AS CNTRYCODE,
 *         c_acctbal
 *     FROM
 *         customer
 *    WHERE
 *         SUBSTRING(c_phone,1,2) IN ('13', '31', '23', '29', '30', '18', '17')
 *         AND c_acctbal > (
 *             SELECT
 *                 AVG(c_acctbal)
 *             FROM
 *                 customer
 *             WHERE
 *                 c_acctbal > 0.00
 *                 AND SUBSTRING(c_phone,1,2) IN ('13', '31', '23', '29', '30', '18', '17')
 *         )
 *         AND NOT EXISTS (
 *             SELECT
 *                 *
 *             FROM
 *                 orders
 *             WHERE
 *                 o_custkey = c_custkey
 *         )
 * ) AS CUSTSALE
 * GROUP BY
 *     CNTRYCODE
 * ORDER BY
 *     CNTRYCODE
 *
 * Changes:
 *  1. Renamed SUBSTRING to SUBSTR because SQLite does not support the former
 */
const char* const tpch_query_22 =
    R"(SELECT
         CNTRYCODE, COUNT(*) AS NUMCUST, SUM(c_acctbal) AS TOTACCTBAL
       FROM
         (SELECT
            SUBSTR(c_phone,1,2) AS CNTRYCODE, c_acctbal
          FROM
            customer
          WHERE
            SUBSTR(c_phone,1,2) IN ('13', '31', '23', '29', '30', '18', '17') AND
            c_acctbal > (SELECT
                           AVG(c_acctbal)
                         FROM
                           customer
                         WHERE
                          c_acctbal > 0.00 AND
                          SUBSTR(c_phone,1,2) IN ('13', '31', '23', '29', '30', '18', '17')) AND
            NOT EXISTS ( SELECT * FROM orders WHERE o_custkey = c_custkey)
        ) AS CUSTSALE
       GROUP BY CNTRYCODE
       ORDER BY CNTRYCODE;)";

}  // namespace

namespace opossum {

const std::map<size_t, const char*> tpch_queries = {
    {1, tpch_query_1},   {2, tpch_query_2},   {3, tpch_query_3},   {4, tpch_query_4},   {5, tpch_query_5},
    {6, tpch_query_6},   {7, tpch_query_7},   {8, tpch_query_8},   {9, tpch_query_9},   {10, tpch_query_10},
    {11, tpch_query_11}, {12, tpch_query_12}, {13, tpch_query_13}, {14, tpch_query_14}, {15, tpch_query_15},
    {16, tpch_query_16}, {17, tpch_query_17}, {18, tpch_query_18}, {19, tpch_query_19}, {20, tpch_query_20},
    {21, tpch_query_21}, {22, tpch_query_22}};

}  // namespace opossum
