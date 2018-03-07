SELECT * FROM mixed;
SELECT * FROM mixed_null;

-- Table Scans
SELECT * FROM mixed WHERE b = 10;
SELECT * FROM mixed WHERE a = 'a' AND c < 65.31;
SELECT * FROM mixed WHERE a = 'a' AND c <= 65.31;
SELECT * FROM mixed WHERE 40 >= b;
SELECT * FROM mixed WHERE b >= 21 AND c < 72.76;
SELECT * FROM mixed WHERE b BETWEEN 20 AND 45;
SELECT * FROM mixed WHERE b BETWEEN c AND 45;
SELECT * FROM mixed WHERE b >= 21 OR c < 72.76;
SELECT * FROM mixed WHERE b >= 21 OR (b <= 30 AND c > 50.0);
SELECT * FROM mixed WHERE b >= 21 OR c < 72.76 OR (b <= 30 AND c > 50.0);

SELECT * FROM mixed_null WHERE b = 12;

-- Projection
SELECT a FROM mixed;
SELECT a as b FROM mixed;
SELECT b, 4+6 as c, b+4 AS d, 5.0+c AS e FROM mixed_null;
SELECT int_a*int_b/int_c AS calc FROM int_int_int;
SELECT b*b AS calc FROM mixed;
SELECT int_a, int_b, int_a+int_b AS e, int_a+int_b+NULL AS f FROM int_int_int;
SELECT a, b, b+b AS e, b+b+NULL AS f FROM mixed;
SELECT a, b, b+b AS e, b+b+NULL AS f FROM mixed_null;

-- ORDER BY
SELECT * FROM mixed ORDER BY a;
SELECT * FROM mixed ORDER BY a, b;
SELECT * FROM mixed ORDER BY a, b ASC;
SELECT * FROM mixed ORDER BY a, b DESC;
SELECT * FROM mixed ORDER BY b, a, c;
SELECT * FROM mixed ORDER BY b, a DESC, c;
SELECT sub.a, sub.b FROM (SELECT a, b FROM mixed WHERE a = 'a' ORDER BY b) AS sub WHERE sub.b > 10 ORDER BY b;
-- (#577) SELECT * FROM mixed_null ORDER BY b;

-- LIMIT
SELECT * FROM mixed LIMIT 77;
SELECT b FROM mixed LIMIT 10;

-- PRODUCT
SELECT "left".a, "left".b, "right".a, "right".b FROM mixed AS "left", mixed_null AS "right" WHERE "left".a = "right".a;
-- (#580) SELECT * FROM mixed AS "left", mixed_null AS "right" WHERE "left".a = "right".d;

-- JOIN
SELECT "left".a, "left".b, "right".a, "right".b FROM mixed AS "left" JOIN mixed_null AS "right" ON "left".b = "right".b;
SELECT * FROM mixed AS "left" LEFT JOIN mixed_null AS "right" ON "left".b = "right".b;
SELECT * FROM mixed AS "left" INNER JOIN mixed_null AS "right" ON "left".b = "right".b;
SELECT * FROM mixed NATURAL JOIN int_int_int;
SELECT * FROM mixed NATURAL JOIN (SELECT int_c, id FROM int_int_int) AS T2;
SELECT * FROM (SELECT "right".a a, "left".b b FROM mixed AS "left" LEFT JOIN mixed AS "right" ON "left".a = "right".a) t where t.a > 0;
-- (#511) SELECT * FROM int_float4 NATURAL JOIN (SELECT b, a FROM int_float6) AS T2;

-- JOIN multiple tables
SELECT * FROM mixed_null AS t1 INNER JOIN int_int_int AS t2 ON t1.b = t2.int_a INNER JOIN mixed AS t3 ON t1.b = t3.b;

-- Make sure that name-to-id-resolving works fine.
SELECT t1.a, t1.b, t2.b, t3.int_a FROM mixed AS t1 INNER JOIN mixed_null AS t2 ON t1.b = t2.b INNER JOIN int_int_int AS t3 ON t1.b = t3.int_a;

-- Make sure that t1.* is resolved only to columns from t1, not all columns from input node.
SELECT t1.*, t2.b, t3.int_a FROM mixed AS t1 INNER JOIN mixed_null AS t2 ON t1.b = t2.b INNER JOIN int_int_int AS t3 ON t1.b = t3.int_a;
SELECT t1.*, t2.a, t2.b, t3.* FROM mixed AS t1 INNER JOIN mixed_null AS t2 ON t1.b = t2.b INNER JOIN int_int_int AS t3 ON t1.b = t3.int_a;

-- Join four tables, just because we can.
SELECT t1.id, t1.a, t2.b, t3.int_b, t4.c_name FROM mixed AS t1 INNER JOIN mixed_null AS t2 ON t1.id = t2.b INNER JOIN int_int_int AS t3 ON t1.id = t3.int_b INNER JOIN tpch_customer AS t4 ON t1.id = t4.c_custkey;

-- Join three tables and perform a scan
SELECT * FROM mixed AS t1 INNER JOIN mixed_null AS t2 ON t1.b = t2.b INNER JOIN int_int_int AS t3 ON t1.b = t3.int_a WHERE t1.c > 23.0 AND t2.a = 'c';

-- Aggregates
SELECT SUM(b + b) AS sum_b_b FROM mixed;

-- GROUP BY
SELECT a, SUM(b) FROM mixed GROUP BY a;
SELECT a, SUM(b), AVG(c) FROM mixed GROUP BY a;
SELECT a, b, MAX(c), AVG(b) FROM mixed GROUP BY a, b;
-- (#578) SELECT a AS whatever, SUM(b) FROM mixed GROUP BY whatever;

-- Join, GROUP BY, Having, ...
SELECT c_custkey, c_name, COUNT(int_a) FROM tpch_customer JOIN int_int_int ON c_custkey = int_a GROUP BY c_custkey, c_name HAVING COUNT(int_a) >= 2;
SELECT c_custkey, c_name, COUNT(int_a) FROM tpch_customer JOIN ( SELECT * FROM int_int_int JOIN mixed ON int_a = mixed.id ) AS sub ON tpch_customer.c_custkey = sub.int_a GROUP BY c_custkey, c_name HAVING COUNT(sub.int_a) >= 2;

-- COUNT(*)
SELECT COUNT(*) FROM mixed GROUP BY a;
SELECT a, COUNT(*) FROM mixed GROUP BY a;
SELECT COUNT(*), SUM(int_a + int_b) FROM int_int_int;

-- COUNT(DISTINCT)
SELECT a, COUNT(DISTINCT b) FROM mixed GROUP BY a;

-- Case insensitivity
sELEcT Sum(b + b) AS sum_b_b from mixed;

-- Aggregates with NULL
SELECT a, MAX(b) FROM mixed_null GROUP BY a;
SELECT a, MIN(b) FROM mixed_null GROUP BY a;
SELECT a, SUM(b) FROM mixed_null GROUP BY a;
SELECT a, AVG(b) FROM mixed_null GROUP BY a;
SELECT a, COUNT(b) FROM mixed_null GROUP BY a;

-- Checks that output of Aggregate can be worked with correctly.
SELECT b, sub.min_c, max_b FROM (SELECT a, b, MAX(b) AS max_b, MIN(c) AS min_c FROM mixed GROUP BY a, b) as sub WHERE b BETWEEN 20 AND 50 AND min_c > 15;

-- HAVING
SELECT a, b, MAX(b), AVG(c) FROM mixed GROUP BY a, b HAVING MAX(b) >= 10 AND MAX(b) < 40;
SELECT a, b, MAX(b), AVG(c) FROM mixed GROUP BY a, b HAVING MAX(b) > 10 AND MAX(b) <= 30;
SELECT a, b, MAX(b), AVG(c) FROM mixed GROUP BY a, b HAVING b > 33 AND AVG(c) > 50;
-- (#579) SELECT a, b, MAX(b), AVG(c) FROM mixed GROUP BY a, b HAVING b > 33 OR b = 1 OR b = 17;

-- HAVING w/o mentioning in the SELECT list
SELECT a, b, AVG(b) FROM mixed GROUP BY a, b HAVING MAX(c) > 10 AND MAX(c) <= 30;

-- DELETE
DELETE FROM int_int_int; INSERT INTO int_int_int VALUES (1, 2, 3, 4); SELECT * FROM int_int_int;
DELETE FROM int_int_int WHERE id > 75; SELECT * FROM int_int_int;

-- Update
UPDATE int_int_int SET int_a = int_a + 1 WHERE id > 10; SELECT * FROM int_int_int;

-- INSERT
INSERT INTO int_int_int VALUES (100, 1, 2, 3); SELECT * FROM int_int_int;
INSERT INTO int_int_int (id, int_a, int_b, int_c) VALUES (100, 1, 2, 3); SELECT * FROM int_int_int;
INSERT INTO int_int_int (id, int_c, int_b, int_a) VALUES (100, 3, 2, 1); SELECT * FROM int_int_int;

INSERT INTO int_int_int VALUES (100, 1, 2, 3); INSERT INTO int_int_int VALUES (101, 3, 2, 1); INSERT INTO int_int_int VALUES (102, 42, 77992, 1000000); SELECT * FROM int_int_int;

-- INSERT with mixed types
INSERT INTO mixed VALUES (100, 'x', 42, 123.456, 'xkcd'); SELECT * FROM mixed;

-- INSERT ... INTO ... (with literal projection)
INSERT INTO int_int_int SELECT 100, 1, 2, 3 FROM int_int_int; SELECT * FROM int_int_int;
INSERT INTO int_int_int (id, int_a, int_b, int_c) SELECT 100, 1, 2, 3 FROM int_int_int; SELECT * FROM int_int_int;
INSERT INTO int_int_int (int_b, id, int_c, int_a) SELECT 2, 100, 3, 1 FROM int_int_int; SELECT * FROM int_int_int;

-- INSERT ... INTO ... (with regular queries)
INSERT INTO mixed_null SELECT a, b, c, d FROM mixed WHERE a = 'c' AND b > 15; INSERT INTO mixed_null SELECT a, b, c, d FROM mixed WHERE d = 'caoe'; INSERT INTO mixed_null (b, c, a, d) SELECT b, c, a, d FROM mixed WHERE id < 13; SELECT * FROM mixed_null;


-- VIEWS disabled because of #367
-- CREATE VIEW count_view1 AS SELECT a, COUNT(DISTINCT b) FROM groupby_int_1gb_1agg_null GROUP BY a; SELECT * FROM count_view;
-- CREATE VIEW count_view2 AS SELECT a, COUNT(DISTINCT b) FROM groupby_int_1gb_1agg_null GROUP BY a; SELECT * FROM count_view WHERE a > 10;
-- CREATE VIEW count_view3 (foo, bar) AS SELECT a, COUNT(DISTINCT b) FROM groupby_int_1gb_1agg_null GROUP BY a; SELECT * FROM count_view WHERE a > 10;

-- NULL Semantics
SELECT * FROM mixed WHERE b IS NOT NULL;
SELECT * FROM mixed_null WHERE b IS NULL;
SELECT * FROM mixed_null WHERE b IS NOT NULL;

-- Subqueries in SELECT statement
SELECT int_a, (SELECT MAX(b) FROM mixed) AS foo FROM int_int_int;

-- Subqueries in WHERE statement
SELECT int_a FROM int_int_int WHERE int_a > (SELECT MIN(b) FROM mixed)
SELECT * FROM int_int_int WHERE int_a > (SELECT MIN(b) FROM mixed)
SELECT int_a, int_b FROM int_int_int WHERE int_a > (SELECT MIN(b) FROM mixed)
SELECT * FROM int_int_int WHERE int_a IN (SELECT b FROM mixed)
SELECT int_a FROM int_int_int WHERE int_a IN (SELECT b FROM mixed)
SELECT int_a, int_b FROM int_int_int WHERE int_a IN (SELECT b FROM mixed)

-- cannot test these because we cannot handle empty query results here
-- SELECT * FROM mixed WHERE b IS NULL;
-- SELECT * FROM mixed WHERE b = NULL;
-- SELECT * FROM mixed WHERE b > NULL;
-- SELECT * FROM mixed WHERE b < NULL;
-- SELECT * FROM mixed WHERE b <> NULL;
-- SELECT * FROM mixed WHERE b BETWEEN NULL AND NULL;
-- SELECT * FROM mixed_null WHERE b = NULL;
-- SELECT * FROM mixed_null WHERE b > NULL;
-- SELECT * FROM mixed_null WHERE b < NULL;
-- SELECT * FROM mixed_null WHERE b <> NULL;
-- SELECT * FROM mixed_null WHERE b BETWEEN NULL AND NULL;
