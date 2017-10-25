SELECT * FROM int_float;
SELECT * FROM int_float_null;

-- Table Scans
SELECT * FROM int_float2 WHERE a = 12345 AND b > 457;
SELECT * FROM int_float WHERE a >= 1234;
SELECT * FROM int_float WHERE 1234 <= a;
SELECT * FROM int_float WHERE a >= 1234 AND b < 457.9
SELECT * FROM int_string2 WHERE a BETWEEN 122 AND 124
SELECT * FROM int_int_int WHERE a BETWEEN b AND 10

-- Projection
SELECT a FROM int_float;
SELECT a as b FROM int_float;
SELECT a, 4+6 as b FROM int_float;

-- ORDER BY
SELECT * FROM int_float ORDER BY a DESC;
SELECT * FROM int_float4 ORDER BY a, b;
SELECT * FROM int_float4 ORDER BY a, b ASC;
SELECT * FROM int_float4 ORDER BY a, b DESC;
SELECT a, b FROM int_float ORDER BY a;
SELECT * FROM int_float4 ORDER BY a, b;
SELECT a FROM (SELECT a, b FROM int_float WHERE a > 1 ORDER BY b) AS sub WHERE sub.a > 0 ORDER BY a;

-- LIMIT
SELECT * FROM int_int3 LIMIT 4;

-- PRODUCT
SELECT "left".a, "left".b, "right".a, "right".b FROM int_float AS "left",  int_float2 AS "right" WHERE "left".a = "right".a;

-- JOIN
SELECT "left".a, "left".b, "right".a, "right".b FROM int_float AS "left" JOIN int_float2 AS "right" ON "left".a = "right".a;
-- SELECT * FROM int_float AS "left" LEFT JOIN int_float2 AS "right" ON "left".a = "right".a;
SELECT * FROM int_float AS "left" INNER JOIN int_float2 AS "right" ON "left".a = "right".a;

-- JOIN multiple tables
SELECT * FROM int_float AS t1 INNER JOIN int_float2 AS t2 ON t1.a = t2.a INNER JOIN int_string2 AS t3 ON t1.a = t3.a;

-- Make sure that name-to-id-resolving works fine.
SELECT t1.a, t1.b, t2.b, t3.b FROM int_float AS t1 INNER JOIN int_float2 AS t2 ON t1.a = t2.a INNER JOIN int_string2 AS t3 ON t1.a = t3.a;

-- Make sure that t1.* is resolved only to columns from t1, not all columns from input node.
SELECT t1.*, t2.b, t3.b FROM int_float AS t1 INNER JOIN int_float2 AS t2 ON t1.a = t2.a INNER JOIN int_string2 AS t3 ON t1.a = t3.a;
SELECT t1.*, t2.a, t2.b, t3.* FROM int_float AS t1 INNER JOIN int_float2 AS t2 ON t1.a = t2.a INNER JOIN int_string2 AS t3 ON t1.a = t3.a;

-- Join four tables, just because we can.
SELECT t1.a, t1.b, t2.b, t3.b, t4.b FROM int_float AS t1 INNER JOIN int_float2 AS t2 ON t1.a = t2.a INNER JOIN int_float6 AS t3 ON t1.a = t3.a INNER JOIN int_string2 AS t4 ON t1.a = t4.a;

-- Join three tables and perform a scan
SELECT * FROM int_float AS t1 INNER JOIN int_float2 AS t2 ON t1.a = t2.a INNER JOIN int_string2 AS t3 ON t1.a = t3.a WHERE t2.b > 457.0 AND t3.b = 'C';

-- Aggregates
SELECT SUM(b + b) AS sum_b_b FROM int_float;

-- GROUP BY
SELECT a, SUM(b) FROM groupby_int_1gb_1agg GROUP BY a;
SELECT a, SUM(b), AVG(c) FROM groupby_int_1gb_2agg GROUP BY a;
SELECT a, b, MAX(c), AVG(d) FROM groupby_int_2gb_2agg GROUP BY a, b;

-- Join, GROUP BY, Having, ...
SELECT c_custkey, c_name, COUNT(o_orderkey) FROM customer JOIN orders ON c_custkey = o_custkey GROUP BY c_custkey, c_name HAVING COUNT(orders.o_orderkey) >= 1;
SELECT c_custkey, c_name, COUNT(o_orderkey) FROM customer JOIN ( SELECT * FROM orders JOIN lineitem ON o_orderkey = l_orderkey ) AS orderitems ON customer.c_custkey = orderitems.o_custkey GROUP BY c_custkey, c_name HAVING COUNT(orderitems.o_orderkey) >= 1;

-- COUNT(*)
SELECT a, COUNT(*) FROM groupby_int_1gb_1agg_null GROUP BY a;
SELECT COUNT(*), SUM(a + b) FROM int_int3;
SELECT COUNT(*) FROM groupby_int_1gb_1agg_null GROUP BY a;

-- COUNT(DISTINCT)
SELECT a, COUNT(DISTINCT b) FROM groupby_int_1gb_1agg_null GROUP BY a;

-- Case insensitivity
SELECT Sum(b + b) AS sum_b_b FROM int_float;

-- Aggregates with NULL
SELECT a, MAX(b) FROM groupby_int_1gb_1agg_null GROUP BY a;
SELECT a, MIN(b) FROM groupby_int_1gb_1agg_null GROUP BY a;
SELECT a, SUM(b) FROM groupby_int_1gb_1agg_null GROUP BY a;
SELECT a, AVG(b) FROM groupby_int_1gb_1agg_null GROUP BY a;
SELECT a, COUNT(b) FROM groupby_int_1gb_1agg_null GROUP BY a;

-- Checks that output of Aggregate can be worked with correctly.
SELECT d, sub.min_c, max_a FROM ( SELECT b, d, MAX(a) AS max_a, MIN(c) AS min_c FROM groupby_int_2gb_2agg_2 GROUP BY b, d ) AS sub WHERE d BETWEEN 20 AND 50 AND min_c > 15;

-- HAVING
SELECT a, b, MAX(c), AVG(d) FROM groupby_int_2gb_2agg GROUP BY a, b HAVING MAX(c) >= 10 AND MAX(c) < 40;
SELECT a, b, MAX(c), AVG(d) FROM groupby_int_2gb_2agg GROUP BY a, b HAVING MAX(c) > 10 AND MAX(c) <= 30;
SELECT a, b, MAX(c), AVG(d) FROM groupby_int_2gb_2agg GROUP BY a, b HAVING b > 457 AND a = 12345;

-- HAVING w/o mentioning in the SELECT list
SELECT a, b, AVG(d) FROM groupby_int_2gb_2agg GROUP BY a, b HAVING MAX(c) > 10 AND MAX(c) <= 30;
SELECT * FROM customer;
SELECT c_custkey, c_name FROM customer;

-- DELETE
-- TODO(MD): this will only work once SELECT automatically validates (#188)
-- DELETE FROM int_for_delete_1; SELECT * FROM int_for_delete_1;
-- DELETE FROM int_for_delete_2 WHERE a > 1000; SELECT * FROM int_for_delete_2;

-- Update
-- TODO(md): see DELETE
-- UPDATE int_int_for_update SET a = a + 1 WHERE b > 10; SELECT * FROM int_int_for_update;

-- INSERT
INSERT INTO int_int_for_insert_1 VALUES (1, 3); SELECT * FROM int_int_for_insert_1;
INSERT INTO int_int_for_insert_1 (a, b) VALUES (1, 3); SELECT * FROM int_int_for_insert_1;
INSERT INTO int_int_for_insert_1 (b, a) VALUES (3, 1); SELECT * FROM int_int_for_insert_1;

INSERT INTO int_int_for_insert_1 VALUES (1, 3); INSERT INTO int_int_for_insert_1 VALUES (13, 2); INSERT INTO int_int_for_insert_1 VALUES (6, 9); SELECT * FROM int_int_for_insert_1;

-- INSERT ... INTO ... (with literal projection)
INSERT INTO int_int_for_insert_1 SELECT 1, 3 FROM int_int_for_insert_1; SELECT * FROM int_int_for_insert_1;
INSERT INTO int_int_for_insert_1 (a, b) SELECT 1, 3 FROM int_int_for_insert_1; SELECT * FROM int_int_for_insert_1;
INSERT INTO int_int_for_insert_1 (b, a) SELECT 3, 1 FROM int_int_for_insert_1; SELECT * FROM int_int_for_insert_1;

-- INSERT ... INTO ... (with regular queries)
INSERT INTO int_int_for_insert_1 SELECT * FROM int_int3 WHERE a = 1 AND b = 3; INSERT INTO int_int_for_insert_1 SELECT * FROM int_int3 WHERE a = 13; INSERT INTO int_int_for_insert_1 (a, b) SELECT a, b FROM int_int3 WHERE a = 6; SELECT * FROM int_int_for_insert_1;
