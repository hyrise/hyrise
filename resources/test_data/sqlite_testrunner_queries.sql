-- Select entire table
SELECT * FROM mixed;
SELECT * FROM mixed_null;

-- No FROM clause
SELECT 1;
SELECT -1;
SELECT (1 + 3.0) * 13.0 as some_arithmetics;
SELECT 22 / 5 AS col;

-- Table Scans
SELECT * FROM mixed WHERE b = 10;
SELECT * FROM mixed WHERE a > d AND b > 1;
SELECT * FROM mixed WHERE a = d;
SELECT * FROM mixed WHERE a = 'a' AND c < 65.31;
SELECT * FROM mixed WHERE a = 'a' AND c <= 65.31;
SELECT * FROM mixed WHERE 40 >= b;
SELECT * FROM mixed WHERE b >= 21 AND c < 72.76;
SELECT * FROM mixed WHERE b BETWEEN 0 AND 99999;
SELECT * FROM mixed WHERE b BETWEEN 20 AND 45;
SELECT * FROM mixed WHERE b BETWEEN 20 AND 45.5;
SELECT * FROM mixed WHERE b = 10 OR b BETWEEN 45 AND 20; -- valid SQL with expected empty result
SELECT * FROM mixed WHERE b BETWEEN c AND 45;
SELECT * FROM mixed WHERE b >= 21 OR c < 72.76;
SELECT * FROM mixed WHERE b >= 21 OR (b <= 30 AND c > 50.0);
SELECT * FROM mixed WHERE b >= 21 OR c < 72.76 OR (b <= 30 AND c > 50.0);
SELECT * FROM mixed WHERE b + c < c * b - 100;
SELECT * FROM mixed WHERE id > b;
SELECT * FROM mixed WHERE id = b;
SELECT * FROM mixed WHERE id IN (SELECT 14) AND b > (SELECT 15) AND b < (SELECT 98);
SELECT * FROM mixed WHERE id IN (SELECT 14 + 1) AND b > (SELECT 15 + 2) AND b < (SELECT 98 + 3)
SELECT * FROM mixed WHERE id >= 5.5;
SELECT * FROM mixed WHERE id BETWEEN 5.5 AND 8;
SELECT * FROM mixed WHERE id < 5.5;
SELECT * FROM mixed WHERE d > 'c';
SELECT * FROM mixed WHERE d >= 'jp';
SELECT * FROM mixed_null WHERE b IS NULL;
SELECT * FROM mixed_null WHERE b*c IS NOT NULL;
SELECT * FROM mixed_null WHERE b = 12;
SELECT * FROM mixed_null WHERE NOT (b = 12);
SELECT * FROM mixed_null WHERE NOT (b IN (12, 13, 14));

-- Scans should behave correctly if there is a comparison with NULL - wrap in NOT EXISTS to produce non-empty result
-- table
SELECT * FROM mixed WHERE NOT EXISTS (SELECT * FROM mixed WHERE id > NULL);
SELECT * FROM mixed WHERE NOT EXISTS (SELECT * FROM mixed WHERE a <= NULL);
SELECT * FROM mixed WHERE NOT EXISTS (SELECT * FROM mixed WHERE a > NULL);
SELECT * FROM mixed WHERE NOT EXISTS (SELECT * FROM mixed WHERE b != NULL);
SELECT * FROM mixed WHERE NOT EXISTS (SELECT * FROM mixed WHERE d = NULL);
SELECT * FROM mixed WHERE NOT EXISTS (SELECT * FROM mixed WHERE d < NULL);
SELECT * FROM mixed WHERE NOT EXISTS (SELECT * FROM mixed WHERE d BETWEEN NULL AND NULL);
SELECT * FROM mixed WHERE NOT EXISTS (SELECT * FROM mixed WHERE d BETWEEN '' AND NULL);
SELECT * FROM mixed WHERE NOT EXISTS (SELECT * FROM mixed WHERE d BETWEEN NULL AND '');

-- Scans that use values out of the range of the column type
SELECT * FROM mixed WHERE b > -3000000000 AND b < 3000000000;
SELECT * FROM mixed WHERE (b < -3000000000 AND b > 3000000000) OR id = 1;
SELECT * FROM mixed WHERE b BETWEEN -3123456789 AND 3123456789;
-- Scientific notation not supported, so we have to write ~1e40, which is > FLOAT_MAX, out
SELECT * FROM mixed WHERE c < 1234567890123456789012345678901234567890.0;

-- Scans with predicates that do not reference columns
SELECT * FROM mixed_null WHERE 4 > 3;
SELECT * FROM mixed_null WHERE (4 > 3 AND 2 < 1) OR 4 < 5;
SELECT * FROM mixed_null WHERE 50 IN (51, 52, 50);
SELECT * FROM mixed_null WHERE 50 IN (SELECT id FROM mixed);

-- Scans with potential for BETWEEN rewrite
SELECT * FROM id_int_int_int_100 WHERE a >= 20 AND a <= 90;
SELECT * FROM id_int_int_int_100 WHERE a > 20 AND a <= 91;
SELECT * FROM id_int_int_int_100 WHERE a >= 20 AND a < 91;
SELECT * FROM id_int_int_int_100 WHERE a > 20 AND a < 91;
SELECT * FROM id_int_int_int_100 WHERE a = 20 AND b = 93; -- negative example

SELECT * FROM id_int_int_int_100 WHERE 90 >= a AND 20 <= a;
SELECT * FROM id_int_int_int_100 WHERE 91 > a AND 20 <= a;
SELECT * FROM id_int_int_int_100 WHERE 91 >= a AND 20 < a;
SELECT * FROM id_int_int_int_100 WHERE 91 > a AND 20 < a;

-- Scans with potential for predicate pruning
SELECT * FROM id_int_int_int_100 WHERE a >= 20 AND a <= 40 OR b >= 50 AND b <= 95;
SELECT * FROM id_int_int_int_100 WHERE a >= 20 AND a <= 40 AND c <= 35 AND b >= 49 AND a >= 21 AND b <= 95 AND c <= 40 AND c >= 23;

-- Scans with potential for predicate split-up and predicate merge
SELECT a FROM mixed WHERE 1 OR 3 > 2;
SELECT * FROM mixed AS a WHERE EXISTS (SELECT * FROM id_int_int_int_50 AS b WHERE b.b = a.b) OR EXISTS (SELECT * FROM id_int_int_int_100 AS c WHERE c.b = a.b)
SELECT * FROM mixed AS a WHERE EXISTS (SELECT * FROM id_int_int_int_50 AS b WHERE b.b = a.b) OR EXISTS (SELECT * FROM id_int_int_int_50 AS c WHERE c.b + 1 = a.b) OR EXISTS (SELECT * FROM id_int_int_int_50 AS d WHERE d.b + 2 = a.b) OR EXISTS (SELECT * FROM id_int_int_int_50 AS e WHERE e.b + 3 = a.b)
SELECT * FROM (SELECT a.a FROM id_int_int_int_100 AS a, mixed AS b WHERE a.a = b.b OR a.b = b.c) r JOIN (SELECT a.b FROM id_int_int_int_100 AS a, mixed AS b WHERE a.a = b.b OR a.b = b.c) s ON r.a = s.b
SELECT * FROM mixed WHERE (b > 10 OR b < 8) AND (c <= 7 OR 11 = c)
SELECT * FROM (SELECT b, c FROM mixed WHERE b < c AND b = 19) r WHERE (b > 10 OR b < 8) AND (c <= 60 OR 11 = c) OR ((b = 5 AND c = 7) AND 13 = 13)
SELECT b FROM mixed WHERE (2 > 1 OR 2 > 0) AND b = 1;
SELECT * FROM (SELECT a FROM id_int_int_int_50 WHERE a < 1 OR 3 > 2) AS S WHERE a < 10 OR 30 > 20;
SELECT * FROM (SELECT * FROM id_int_int_int_50 WHERE a < 1 OR 3 > 2) AS L JOIN (SELECT * FROM id_int_int_int_50 WHERE a < 10 OR 30 > 20) AS R ON L.a = R.a;

-- Projection
SELECT a FROM mixed;
SELECT -b as neg_b FROM mixed;
SELECT b + b FROM mixed;
SELECT b + c FROM mixed;
SELECT (b * c) / b + (c * c) / b - b / b as x FROM mixed;
SELECT a as b FROM mixed;
SELECT b, 4+6 as c, b+4 AS d, 5.0+c AS e FROM mixed_null;
SELECT a*b/c AS calc FROM id_int_int_int_100;
SELECT b*b AS calc FROM mixed;
SELECT a, b, a+b AS e, a+b+NULL AS f FROM id_int_int_int_100;
SELECT a, b, b+b AS e, b+b+NULL AS f FROM mixed;
SELECT a, b, b+b AS e, b+b+NULL AS f FROM mixed_null;
SELECT 1 + 5.6 > 7 OR 2 > 1 AS i FROM mixed;
SELECT 2 / 0, b / 0, 50 / id FROM mixed;
SELECT 2 % 0, b % 0, 50 % id FROM mixed;

-- Aliases
SELECT R.a, S.a FROM mixed AS R, mixed AS S;
SELECT a AS x FROM mixed WHERE a > 10;
SELECT a AS x, SUM(b) FROM mixed GROUP BY x;
SELECT a AS x, SUM(b) FROM mixed GROUP BY a;
SELECT a AS x, SUM(b) FROM mixed GROUP BY x HAVING a > 10;
SELECT a AS x, SUM(b) FROM mixed GROUP BY x HAVING x > 10;
SELECT a AS x, SUM(b) FROM mixed GROUP BY x HAVING x > 10;
SELECT a AS a1, a AS a2 FROM mixed;
SELECT a AS a1, b AS b2, b AS b3, a AS a3, b AS b1, a AS a2 FROM mixed;
SELECT COUNT(*) AS cnt1, COUNT(*) AS cnt2, COUNT(*) AS cnt3 FROM mixed;
SELECT COUNT(*) AS cnt1, COUNT(*) AS cnt2, COUNT(*) AS cnt3 FROM mixed GROUP BY a;
SELECT a1, b2, a3 FROM (SELECT a AS a1, b AS b2, b AS b3, a AS a3, b AS b1, a AS a2 FROM mixed) AS R;
SELECT * FROM (SELECT COUNT(*) AS cnt1, COUNT(*) AS cnt2, COUNT(*) AS cnt3 FROM mixed) AS R;
SELECT b AS b1, b AS b2 FROM id_int_int_int_100 WHERE a < (SELECT MAX(b) FROM mixed WHERE mixed.b > b1);
SELECT * FROM (SELECT COUNT(*) AS cnt1 FROM id_int_int_int_50) AS s1, (SELECT COUNT(*) AS cnt2 FROM id_int_int_int_100) AS s2;
SELECT * FROM (SELECT COUNT(a) AS cnt1 FROM id_int_int_int_50) AS s1, (SELECT COUNT(a) AS cnt2 FROM id_int_int_int_100) AS s2;
SELECT * FROM (SELECT COUNT(*) FROM mixed AS L, mixed AS R WHERE L.a = R.a) AS S1, (SELECT COUNT(*) FROM mixed AS L, mixed AS R WHERE L.b = R.b) AS S2;
SELECT * FROM (SELECT COUNT(*) FROM mixed AS L, mixed AS R WHERE L.a = R.a) AS S1, (SELECT COUNT(*) FROM id_int_int_int_50 AS L, id_int_int_int_50 AS R WHERE L.a = R.a) AS S2;

-- ORDER BY
SELECT * FROM mixed ORDER BY a;
SELECT a AS x, b AS y FROM mixed ORDER BY a, b;
SELECT a AS x, b AS y FROM mixed ORDER BY x, y;
SELECT b + 13 AS t FROM mixed ORDER BY a, b ASC;
SELECT * FROM mixed ORDER BY a, b DESC;
SELECT * FROM mixed ORDER BY b, a, c;
SELECT * FROM mixed ORDER BY b, a DESC, c;
SELECT sub.a, sub.b FROM (SELECT a, b FROM mixed WHERE a = 'a' ORDER BY b) AS sub WHERE sub.b > 10 ORDER BY b;
SELECT * FROM mixed_null ORDER BY b;

-- LIMIT
SELECT * FROM mixed LIMIT 77;
SELECT b FROM mixed LIMIT 10;

-- PRODUCT
SELECT "right".b FROM mixed AS "left", mixed_null AS "right" WHERE "left".a = "right".a AND "left".b = 2;
SELECT * FROM mixed AS "left", mixed_null AS "right" WHERE "left".a = "right".d;

-- JOIN
SELECT "left".a, "left".b, "right".a, "right".b FROM mixed AS "left" JOIN mixed_null AS "right" ON "left".b = "right".b;
SELECT "left".a1, "left".a2, "right".a1 FROM (SELECT a AS a1, a AS a2 FROM mixed) AS "left" JOIN (SELECT a AS a1, a AS a2 FROM mixed_null) AS "right" ON "left".a1 = "right".a2;
SELECT "left".e, "left".f, "right".a, "right".b FROM (SELECT a AS e, b as f FROM mixed) AS "left" JOIN mixed_null AS "right" ON "left".f = "right".b;
SELECT * FROM mixed AS "left" LEFT JOIN mixed_null AS "right" ON "left".b = "right".b;
SELECT b.*, a.* FROM mixed AS a JOIN mixed AS b ON a.id = b.id WHERE a.id > 50;
SELECT * FROM mixed AS "left" INNER JOIN mixed_null AS "right" ON "left".b = "right".b;
SELECT * FROM mixed NATURAL JOIN (SELECT id FROM id_int_int_int_100) AS T2;
SELECT * FROM mixed NATURAL JOIN (SELECT c AS foo, id FROM id_int_int_int_100) AS T2;
SELECT * FROM (SELECT "right".a a, "left".b b FROM mixed AS "left" LEFT JOIN mixed AS "right" ON "left".a = "right".a) t where t.a > 0;
SELECT * FROM mixed AS m1 JOIN mixed AS m2 ON m1.id * 3 = m2.id - 5;
SELECT l.new_id, r.id + 10 AS a FROM (SELECT id + 5 AS new_id FROM mixed WHERE new_id > 90) AS l LEFT JOIN mixed AS r ON l.new_id = r.id
SELECT (SELECT r.id AS a FROM (SELECT id + 5 AS id FROM mixed) AS l LEFT JOIN mixed AS r ON l.id = r.id WHERE l.id >= 100 LIMIT 1) + 5 AS a
SELECT * FROM id_int_int_int_100 AS t1 LEFT JOIN id_int_int_int_100 AS t2 ON t1.a < t2.a;
SELECT * FROM id_int_int_int_100 AS t1 LEFT JOIN id_int_int_int_100 AS t2 ON t1.a > t2.a;
SELECT * FROM id_int_int_int_100 AS t1 LEFT JOIN id_int_int_int_100 AS t2 ON t1.a <= t2.a;
SELECT * FROM id_int_int_int_100 AS t1 LEFT JOIN id_int_int_int_100 AS t2 ON t1.a >= t2.a;
SELECT * FROM mixed AS t1 LEFT JOIN mixed AS t2 ON t1.id >= t2.b WHERE t1.id > 90;

-- Join multiple predicates
SELECT * FROM mixed AS t1 JOIN mixed_null AS t2 ON t1.a = t2.a AND t1.b = t2.b;
SELECT * FROM mixed AS t1 JOIN mixed_null AS t2 ON t1.a <= t2.a AND t1.b = t2.b AND t1.c > t2.c;
SELECT * FROM mixed AS t1 JOIN mixed_null AS t2 ON t1.a >= t2.a AND t1.b = t2.b AND t1.c < t2.c;
SELECT * FROM mixed AS t1 LEFT JOIN mixed_null AS t2 ON t1.a = t2.a AND t1.b = t2.b;
--SELECT * FROM mixed AS t1 LEFT JOIN mixed_null AS t2 ON t1.a = t2.a AND t1.b < t2.b;
SELECT * FROM mixed AS t1 LEFT JOIN mixed_null AS t2 ON t1.a = t2.a AND t1.b <= t2.b;
SELECT * FROM mixed AS t1 LEFT JOIN mixed_null AS t2 ON t1.a = t2.a AND t1.b > t2.b;
SELECT * FROM mixed AS t1 LEFT JOIN mixed_null AS t2 ON t1.a = t2.a AND t1.b >= t2.b;
SELECT * FROM id_int_int_int_100 AS t1 LEFT JOIN id_int_int_int_100 AS t2 ON t1.a = t2.a;
SELECT * FROM id_int_int_int_100 AS t1 LEFT JOIN id_int_int_int_100 AS t2 ON t1.a > t2.a AND t1.b >= t2.b AND t1.c < t2.c;
SELECT * FROM id_int_int_int_100 AS t1 LEFT JOIN id_int_int_int_100 AS t2 ON t1.a <= t2.a AND t1.b > t2.b AND t1.c < t2.c;
SELECT * FROM id_int_int_int_100 AS t1 LEFT JOIN id_int_int_int_100 AS t2 ON t1.a <= t2.a AND t1.b <= t2.b AND t1.c <= t2.c;
SELECT * FROM id_int_int_int_100 AS t1 LEFT JOIN id_int_int_int_100 AS t2 ON t1.a >= t2.a AND t1.b >= t2.b AND t1.c >= t2.c;
SELECT * FROM id_int_int_int_100 AS t1 LEFT JOIN id_int_int_int_100 AS t2 ON t1.a <= t2.a AND t1.b > t2.b AND t1.c < t2.c AND t1.b > t2.c AND t1.a = t2.c;

SELECT * FROM id_int_int_int_100 AS t1 LEFT JOIN id_int_int_int_100 AS t2 ON t1.a = t2.a;
SELECT * FROM id_int_int_int_100 AS t1 LEFT JOIN id_int_int_int_100 AS t2 ON t1.a < t2.a;
SELECT * FROM id_int_int_int_100 AS t1 LEFT JOIN id_int_int_int_100 AS t2 ON t1.a > t2.a;
SELECT * FROM id_int_int_int_100 AS t1 LEFT JOIN id_int_int_int_100 AS t2 ON t1.a <= t2.a;
SELECT * FROM id_int_int_int_100 AS t1 LEFT JOIN id_int_int_int_100 AS t2 ON t1.a >= t2.a;

-- JOIN on string column
SELECT * FROM mixed AS m1 JOIN mixed AS m2 ON m1.a = m2.a AND m1.d > m2.d WHERE m1.id > 50
SELECT * FROM mixed AS m1 JOIN mixed AS m2 ON m1.a != m2.a AND m1.d > m2.d WHERE m1.id > 50
SELECT * FROM mixed AS m1 JOIN mixed AS m2 ON m1.a < m2.a AND m1.d > m2.d WHERE m1.id > 50
SELECT * FROM mixed AS m1 LEFT JOIN mixed AS m2 ON m1.a = m2.a WHERE m2.a NOT IN ('a', 'b')

SELECT * FROM mixed AS m1 JOIN mixed AS m2 ON m1.id * 3 = m2.id - 5 OR m1.id > 20;
-- (#511) SELECT * FROM int_float4 NATURAL JOIN (SELECT b, a FROM int_float6) AS T2;

-- JOIN multiple tables
SELECT * FROM mixed_null AS t1 INNER JOIN id_int_int_int_100 AS t2 ON t1.b = t2.a INNER JOIN mixed AS t3 ON t1.b = t3.b;

-- Make sure that name-to-id-resolving works fine.
SELECT t1.a, t1.b, t2.b, t3.a FROM mixed AS t1 INNER JOIN mixed_null AS t2 ON t1.b = t2.b INNER JOIN id_int_int_int_100 AS t3 ON t1.b = t3.a;

-- Make sure that t1.* is resolved only to columns from t1, not all columns from input node.
SELECT t1.*, t2.b, t3.a FROM mixed AS t1 INNER JOIN mixed_null AS t2 ON t1.b = t2.b INNER JOIN id_int_int_int_100 AS t3 ON t1.b = t3.a;
SELECT t1.*, t2.a, t2.b, t3.* FROM mixed AS t1 INNER JOIN mixed_null AS t2 ON t1.b = t2.b INNER JOIN id_int_int_int_100 AS t3 ON t1.b = t3.a;

-- Join four tables, just because we can.
SELECT t1.id, t1.a, t2.b, t3.b, t4.c_name FROM mixed AS t1 INNER JOIN mixed_null AS t2 ON t1.id = t2.b INNER JOIN id_int_int_int_100 AS t3 ON t1.id = t3.b INNER JOIN tpch_customer AS t4 ON t1.id = t4.c_custkey;

-- Join three tables and perform a scan
SELECT * FROM mixed AS t1 INNER JOIN mixed_null AS t2 ON t1.b = t2.b INNER JOIN id_int_int_int_100 AS t3 ON t1.b = t3.a WHERE t1.c > 23.0 AND t2.a = 'c';

-- Join three, complex join predicate,
SELECT t1.b FROM mixed AS t1, mixed_null AS t2, id_int_int_int_100 AS t3 WHERE t1.id + t2.b = t3.a - 5 AND (5 > 3 OR 3 > 2) AND t1.b = 19 AND t1.b + 40 = t3.b;

-- (not) exists to semi(/anti) join reformulation
SELECT * FROM id_int_int_int_100 WHERE EXISTS (SELECT * FROM int_date WHERE id_int_int_int_100.id = int_date.a)
SELECT * FROM id_int_int_int_100 WHERE NOT EXISTS (SELECT * FROM int_date WHERE id_int_int_int_100.id = int_date.a)
-- exists to semi join reformulation: query not rewriteable
SELECT * FROM id_int_int_int_100 WHERE EXISTS (SELECT * FROM int_date WHERE id_int_int_int_100.id = int_date.a) OR id < 20

-- Aggregates
SELECT SUM(b + b) AS sum_b_b FROM mixed;
SELECT SUM(b) + AVG(c) AS x FROM mixed GROUP BY id + 5;
SELECT SUM(b) + AVG(c) AS x, AVG(c)*3 AS y FROM mixed GROUP BY id + 5;
SELECT MIN(id) FROM mixed GROUP BY d, c;
SELECT * FROM id_int_int_int_100 AS r WHERE a < (SELECT MAX(s.a) FROM id_int_int_int_50 AS s)
SELECT * FROM id_int_int_int_100 AS r WHERE a >= (SELECT MIN(s.a) FROM id_int_int_int_50 AS s)
SELECT * FROM id_int_int_int_100 AS r WHERE a >= (SELECT MIN(s.a) FROM id_int_int_int_50 AS s WHERE s.b = r.b)
SELECT * FROM id_int_int_int_100 AS r WHERE a < (SELECT SUM(min_a) FROM (SELECT MIN(s.a) AS min_a FROM id_int_int_int_50 AS s WHERE s.b = r.b GROUP BY s.c) min_a_per_c)

-- GROUP BY
SELECT SUM(b) FROM mixed GROUP BY a;
SELECT a, SUM(b) FROM mixed GROUP BY a;
SELECT a, SUM(b), AVG(c) FROM mixed GROUP BY a;
SELECT a, b, MAX(c), AVG(b) FROM mixed GROUP BY a, b;

-- DISTINCT
SELECT DISTINCT a FROM mixed;
SELECT DISTINCT a FROM mixed GROUP BY a;
SELECT DISTINCT a, b FROM mixed;
SELECT DISTINCT * FROM mixed;
SELECT DISTINCT a, MIN(b) FROM mixed GROUP BY a;
SELECT DISTINCT MIN(b) FROM mixed GROUP BY a;

-- Join, GROUP BY, Having, ...
SELECT c_custkey, c_name, COUNT(a) FROM tpch_customer JOIN id_int_int_int_100 ON c_custkey = a GROUP BY c_custkey, c_name HAVING COUNT(a) >= 2;
SELECT c_custkey, c_name, COUNT(a) FROM tpch_customer JOIN ( SELECT id_int_int_int_100.* FROM id_int_int_int_100 JOIN mixed ON id_int_int_int_100.a = mixed.id ) AS sub ON tpch_customer.c_custkey = sub.a GROUP BY c_custkey, c_name HAVING COUNT(sub.a) >= 2;

-- COUNT(*)
SELECT COUNT(*) FROM mixed;
SELECT COUNT(*) FROM mixed GROUP BY a;
SELECT a, COUNT(*) FROM mixed GROUP BY a;
SELECT COUNT(*), SUM(a + b) FROM id_int_int_int_100;
SELECT COUNT(*) FROM mixed AS L, mixed AS R WHERE L.a = R.a;
SELECT COUNT(*) FROM id_int_int_int_50, id_int_int_int_100;
SELECT COUNT(*) FROM (SELECT 1) t;
SELECT COUNT(*) FROM mixed, id_int_int_int_100;

-- COUNT(DISTINCT)
SELECT a, COUNT(DISTINCT b) as d FROM mixed GROUP BY a;

-- Case insensitivity
sELEcT Sum(b + b) AS sum_b_b from mixed;

-- Aggregates with NULL
SELECT a, MAX(b) FROM mixed_null GROUP BY a;
SELECT a, MIN(b) FROM mixed_null GROUP BY a;
SELECT a, SUM(b) FROM mixed_null GROUP BY a;
SELECT a, AVG(b) FROM mixed_null GROUP BY a;
SELECT a, COUNT(b) FROM mixed_null GROUP BY a;
SELECT a, COUNT(*) FROM mixed_null GROUP BY a;

-- Checks that output of Aggregate can be worked with correctly.
SELECT b, sub.min_c, max_b FROM (SELECT a, b, MAX(b) AS max_b, MIN(c) AS min_c FROM mixed GROUP BY a, b) as sub WHERE b BETWEEN 20 AND 50 AND min_c > 15;

-- HAVING
SELECT a, b, MAX(b), AVG(c) FROM mixed GROUP BY a, b HAVING MAX(b) >= 10 AND MAX(b) < 40;
SELECT a, b, MAX(b), AVG(c) FROM mixed GROUP BY a, b HAVING MAX(b) >= 10 AND MAX(b*0.8+c*0.01) < 40;
SELECT a, b, MAX(b), AVG(c) FROM mixed GROUP BY a, b HAVING MAX(b) > 10 AND MAX(b) <= 30;
SELECT a, b, MAX(b), AVG(c) FROM mixed GROUP BY a, b HAVING b > 33 AND AVG(c) > 50;
SELECT a, b, MAX(b), AVG(c) FROM mixed GROUP BY a, b HAVING b > 33 OR b = 1 OR b = 17;

-- HAVING w/o mentioning in the SELECT list
SELECT a, b, AVG(b) FROM mixed GROUP BY a, b HAVING MAX(c) > 10 AND MAX(c) <= 30;

-- DELETE
DELETE FROM id_int_int_int_100; INSERT INTO id_int_int_int_100 VALUES (1, 2, 3, 4); SELECT * FROM id_int_int_int_100;
DELETE FROM id_int_int_int_100 WHERE id > 75; SELECT * FROM id_int_int_int_100;
DELETE FROM id_int_int_int_100 WHERE a > 40 OR b < 20; SELECT * FROM id_int_int_int_100;
DELETE FROM id_int_int_int_100 WHERE a > 40 OR b < 20; SELECT * FROM id_int_int_int_100;
DELETE FROM id_int_int_int_100 WHERE a > 40; DELETE FROM id_int_int_int_100 WHERE b < 20; SELECT * FROM id_int_int_int_100;
DELETE FROM id_int_int_int_100 WHERE a > 9000; SELECT * FROM id_int_int_int_100;
DELETE FROM id_int_int_int_100 WHERE a = 5 OR b = 6 OR (a > 2 AND b > 80) OR (a = (SELECT MIN(a) FROM id_int_int_int_100)); SELECT * FROM id_int_int_int_100;

-- Update
UPDATE id_int_int_int_100 SET a = a + 1 WHERE id > 10; SELECT * FROM id_int_int_int_100;
UPDATE id_int_int_int_100 SET a = a + 1; SELECT * FROM id_int_int_int_100;
UPDATE id_int_int_int_100 SET a = b + c + 3 WHERE id > 10 * 5; SELECT * FROM id_int_int_int_100;
UPDATE id_int_int_int_100 SET a = b + c + 3 WHERE id > 1000 * 1000; SELECT * FROM id_int_int_int_100;
UPDATE id_int_int_int_100 SET id = 0 WHERE id > 20; SELECT * FROM id_int_int_int_100;
UPDATE id_int_int_int_100 SET id = a, a = b, b = c, c = id WHERE id > 20; SELECT * FROM id_int_int_int_100;
UPDATE id_int_int_int_100 SET id = a - 1, a = b, b = c, c = id + 1 WHERE id > 20; SELECT * FROM id_int_int_int_100;

-- INSERT
INSERT INTO id_int_int_int_100 VALUES (100, 1, 2, 3); SELECT * FROM id_int_int_int_100;
INSERT INTO mixed_null VALUES ('Hello', NULL, 3.3, 'World'); SELECT * FROM id_int_int_int_100;
INSERT INTO mixed_null VALUES ('Hello', NULL, 3.3, NULL); SELECT * FROM id_int_int_int_100;
INSERT INTO id_int_int_int_100 (id, a, b, c) VALUES (100, 1, 2, 3); SELECT * FROM id_int_int_int_100;
INSERT INTO id_int_int_int_100 (id, c, b, a) VALUES (100, 3, 2, 1); SELECT * FROM id_int_int_int_100;
INSERT INTO id_int_int_int_100 VALUES (100, 1, 2, 3); INSERT INTO id_int_int_int_100 VALUES (101, 3, 2, 1); INSERT INTO id_int_int_int_100 VALUES (102, 42, 77992, 1000000); SELECT * FROM id_int_int_int_100;

-- INSERT with mixed types
INSERT INTO mixed VALUES (100, 'x', 42, 123.456, 'xkcd'); SELECT * FROM mixed;

-- INSERT ... INTO ... (with literal projection)
INSERT INTO id_int_int_int_100 SELECT 100, 1, 2, 3 FROM id_int_int_int_100; SELECT * FROM id_int_int_int_100;
INSERT INTO id_int_int_int_100 (id, a, b, c) SELECT 100, 1, 2, 3 FROM id_int_int_int_100; SELECT * FROM id_int_int_int_100;
INSERT INTO id_int_int_int_100 (b, id, c, a) SELECT 2, 100, 3, 1 FROM id_int_int_int_100; SELECT * FROM id_int_int_int_100;

-- INSERT ... INTO ... (with regular queries)
INSERT INTO mixed_null SELECT a, b, c, d FROM mixed WHERE a = 'c' AND b > 15; SELECT * FROM mixed_null;
INSERT INTO mixed_null SELECT a, b, c, d FROM mixed WHERE d = 'caoe'; SELECT * FROM mixed_null;
INSERT INTO mixed_null (b, c, a, d) SELECT b, c, a, d FROM mixed WHERE id < 13; SELECT * FROM mixed_null;

-- VIEWS
CREATE VIEW count_view1 AS SELECT a, COUNT(DISTINCT b) AS cd FROM id_int_int_int_100 GROUP BY a; SELECT * FROM count_view1;
CREATE VIEW count_view2 AS SELECT a, COUNT(DISTINCT b) AS cd FROM id_int_int_int_100 GROUP BY a; SELECT * FROM count_view2 WHERE a > 10;
CREATE VIEW count_view3 (foo, bar) AS SELECT a, COUNT(DISTINCT b) AS cd FROM id_int_int_int_100 GROUP BY a; SELECT * FROM count_view3 WHERE foo > 10;
CREATE VIEW alias_view AS SELECT a AS a1, a AS a2 FROM id_int_int_int_100 WHERE a > 10; SELECT a1, a2 FROM alias_view;

-- NULL Semantics
SELECT * FROM mixed WHERE b IS NOT NULL;
SELECT * FROM mixed_null WHERE b IS NULL;
SELECT * FROM mixed_null WHERE b IS NOT NULL;

-- Subqueries in SELECT statement
SELECT a, (SELECT MAX(b) FROM mixed) AS foo FROM id_int_int_int_100;
SELECT (SELECT MAX(b) + id_int_int_int_100.a FROM mixed) AS foo FROM id_int_int_int_100;
SELECT (SELECT MAX(b) + id_int_int_int_100.a + id_int_int_int_100.b FROM mixed) AS foo FROM id_int_int_int_100;
SELECT (SELECT MIN(1 + 2) FROM mixed) AS foos FROM id_int_int_int_100;

-- Subqueries in WHERE statement
SELECT a FROM id_int_int_int_100 AS r WHERE id + 1 = (SELECT MIN(b) + r.id FROM mixed)
SELECT * FROM id_int_int_int_100 WHERE a = (SELECT MAX(b) FROM id_int_int_int_100)
SELECT a FROM id_int_int_int_100 WHERE a > (SELECT MIN(b) FROM mixed)
SELECT * FROM id_int_int_int_100 WHERE a > (SELECT MIN(b) FROM mixed)
SELECT a, b FROM id_int_int_int_100 WHERE a > (SELECT MIN(b) FROM mixed)
SELECT * FROM id_int_int_int_100 WHERE a IN (SELECT b FROM mixed)
SELECT * FROM id_int_int_int_100 WHERE a * 10 IN (SELECT b FROM mixed)
SELECT * FROM id_int_int_int_100 WHERE a * 10 NOT IN (SELECT b FROM mixed)
SELECT a FROM id_int_int_int_100 WHERE a IN (SELECT b FROM mixed)
SELECT a, b FROM id_int_int_int_100 WHERE a IN (SELECT b FROM mixed)
SELECT a FROM id_int_int_int_100 WHERE a IN (SELECT 14) AND b > (SELECT 15);
SELECT a FROM id_int_int_int_100 WHERE a IN (SELECT 11) AND b > (SELECT 11);
SELECT a FROM id_int_int_int_100 WHERE a IN (SELECT 9 + 2) AND b > (SELECT 9 + 2);
SELECT a FROM id_int_int_int_100 WHERE a IN (SELECT MAX(id) / 9 FROM mixed) AND b > (SELECT MAX(id) / 9 FROM mixed);

-- Correlated parameter in WHERE statement
SELECT * FROM id_int_int_int_100 WHERE a < (SELECT MAX(b) FROM mixed WHERE mixed.b > id_int_int_int_100.b)
SELECT * FROM id_int_int_int_100 t1 WHERE (SELECT MIN(t2.id + 10) FROM id_int_int_int_100 t2 WHERE t2.id = t1.id) > 20;

-- Subqueries in FROM statement
SELECT * FROM (SELECT t1.id FROM id_int_int_int_100 t1 JOIN id_int_int_int_100 t2 ON t1.id + 1 = t2.id) AS s1, id_int_int_int_100 t3 WHERE s1.id + 5 = t3.id;
SELECT * FROM id_int_int_int_100 t1 WHERE id < 9 AND (SELECT MIN(t2.id + 10) FROM (SELECT * FROM id_int_int_int_100 t3 WHERE t3.id > t1.id + 90) AS s1, id_int_int_int_100 t2 WHERE t2.id = t1.id + 90) > 5;

-- cannot test these because we cannot handle empty query results here
---- SELECT * FROM mixed WHERE b IS NULL;
---- SELECT * FROM mixed WHERE b = NULL;
---- SELECT * FROM mixed WHERE b > NULL;
---- SELECT * FROM mixed WHERE b < NULL;
---- SELECT * FROM mixed WHERE b <> NULL;
---- SELECT * FROM mixed WHERE b BETWEEN NULL AND NULL;
---- SELECT * FROM mixed_null WHERE b = NULL;
---- SELECT * FROM mixed_null WHERE b > NULL;
---- SELECT * FROM mixed_null WHERE b < NULL;
---- SELECT * FROM mixed_null WHERE b <> NULL;
---- SELECT * FROM mixed_null WHERE b BETWEEN NULL AND NULL;

-- CASE
SELECT CASE WHEN id < 50 THEN 'Hello' WHEN id < 70 THEN 'World' ELSE 'Ciao' END AS case_column FROM mixed;
SELECT CASE WHEN id + 3.4 < 50 THEN 'Hello' WHEN id < 70 THEN 'World' ELSE 'Ciao' END AS case_column FROM mixed;
SELECT CASE id + 10 WHEN 15 THEN a WHEN 26 THEN 'World' ELSE d END AS case_column FROM mixed;
SELECT a, CASE WHEN a IS NULL THEN 1 ELSE 2 END FROM mixed_null GROUP BY a

-- IN
SELECT * FROM id_int_int_int_100 WHERE a IN (24, 55, 78)
SELECT * FROM id_int_int_int_100 WHERE a IN (b - 48, b + 1)
SELECT a + c FROM id_int_int_int_100 WHERE a + c IN (110, 9, 'Hello', 13.345)
SELECT id FROM mixed WHERE d IN ('hamqiv', 9, 'Hello', 13.345, 'xfkk', 13*13)
SELECT * FROM id_int_int_int_100 WHERE a NOT IN (SELECT a FROM id_int_int_int_50)
SELECT * FROM id_int_int_int_100 AS r WHERE a IN (SELECT s.a FROM id_int_int_int_50 AS s WHERE s.b = r.b)
SELECT * FROM id_int_int_int_100 AS r WHERE a IN (SELECT s.a FROM id_int_int_int_50 AS s WHERE s.b <> r.b)
SELECT * FROM id_int_int_int_100 AS r WHERE a IN (SELECT s.a FROM id_int_int_int_50 AS s WHERE s.b < r.b)
SELECT a FROM id_int_int_int_100 AS r WHERE a IN (SELECT s.a FROM id_int_int_int_50 AS s WHERE s.b < r.b)
SELECT * FROM id_int_int_int_100 AS r WHERE a IN (SELECT s.a + 2 FROM id_int_int_int_50 AS s WHERE s.b < r.b)
SELECT * FROM id_int_int_int_100 AS r WHERE a NOT IN (SELECT a FROM id_int_int_int_50 AS s WHERE s.b = r.b)
SELECT * FROM id_int_int_int_100 AS r WHERE a NOT IN (SELECT a FROM id_int_int_int_50 AS s WHERE s.b < r.b)
SELECT * FROM id_int_int_int_100 WHERE a IN (SELECT a FROM id_int_int_int_50 WHERE a IN (SELECT b FROM mixed))
SELECT * FROM id_int_int_int_100 AS r WHERE a IN (SELECT s.a FROM id_int_int_int_50 AS s WHERE s.b = r.b AND s.c < r.c)

-- SUBSTR
SELECT SUBSTR('HELLO', 2, 3) AS s;
SELECT SUBSTR('HELLO', -4, 3) AS s;
SELECT SUBSTR('HELLO', -4, 0) AS s;
SELECT SUBSTR('migz', -18, 19) AS s;
SELECT SUBSTR('HELLO', 5000, 20) AS s;
SELECT SUBSTR(d, id - 10, b) AS s FROM mixed ORDER BY id;
SELECT SUBSTR(d, b / 10, b / 20) AS s FROM mixed_null;

-- LIKE
SELECT * FROM mixed WHERE d LIKE '%a%b%';
SELECT * FROM mixed WHERE d NOT LIKE 'ldggoca';
SELECT * FROM mixed WHERE d LIKE '%y__%g_%';
SELECT * FROM mixed WHERE d LIKE '%y__%g_%' OR (id > 50 AND a LIKE '%a%');
SELECT CASE WHEN d LIKE '%ab%' THEN 'contains AB' WHEN d NOT LIKE '%x%' THEN 'doesnt contain x' ELSE a END AS c FROM mixed;
SELECT * FROM mixed WHERE d LIKE 'a%';
SELECT * FROM mixed WHERE d NOT LIKE 'a%';
SELECT * FROM mixed WHERE d LIKE '%';

-- EXISTS
SELECT EXISTS(SELECT 1) AS some_exists;
SELECT EXISTS(SELECT * FROM id_int_int_int_100) AS some_exists;
SELECT NOT EXISTS(SELECT * FROM id_int_int_int_100) AS some_exists;
SELECT * FROM mixed AS outer_mixed WHERE EXISTS(SELECT * FROM mixed AS inner_mixed WHERE inner_mixed.id = outer_mixed.id * 10);
SELECT * FROM mixed WHERE EXISTS (SELECT id_int_int_int_100.a FROM id_int_int_int_100 WHERE id_int_int_int_100.b = mixed.b);
SELECT * FROM mixed WHERE NOT EXISTS (SELECT id_int_int_int_100.a FROM id_int_int_int_100 WHERE id_int_int_int_100.b = mixed.b);
SELECT * FROM mixed_null WHERE EXISTS(SELECT 0) OR b = 42;
SELECT * FROM mixed_null WHERE EXISTS(SELECT 1);
SELECT * FROM mixed_null WHERE NOT EXISTS(SELECT * FROM mixed WHERE b > 1000);
SELECT * FROM id_int_int_int_100 WHERE EXISTS (SELECT a FROM id_int_int_int_50)
SELECT * FROM id_int_int_int_100 AS r WHERE EXISTS (SELECT s.a FROM id_int_int_int_50 AS s WHERE s.b = r.b)
SELECT * FROM id_int_int_int_100 AS r WHERE EXISTS (SELECT s.a FROM id_int_int_int_50 AS s WHERE s.b <> r.b)
SELECT * FROM id_int_int_int_100 AS r WHERE EXISTS (SELECT s.a FROM id_int_int_int_50 AS s WHERE s.b < r.b)
SELECT a FROM id_int_int_int_100 AS r WHERE EXISTS (SELECT s.a FROM id_int_int_int_50 AS s WHERE s.b < r.b)
SELECT * FROM id_int_int_int_100 AS r WHERE NOT EXISTS (SELECT a FROM id_int_int_int_50 AS s WHERE s.b = r.b)
SELECT * FROM id_int_int_int_100 AS r WHERE NOT EXISTS (SELECT a FROM id_int_int_int_50 AS s WHERE s.b < r.b)
SELECT * FROM id_int_int_int_100 WHERE EXISTS (SELECT a FROM id_int_int_int_50 WHERE EXISTS (SELECT b FROM mixed))
SELECT * FROM id_int_int_int_100 AS r WHERE EXISTS (SELECT s.a FROM id_int_int_int_50 AS s WHERE s.b = r.b AND s.c < r.c)

-- Cannot test the following (expressions), because sqlite doesn't support them:
--  * EXTRACT
--  * CONCAT
--  * PREPARE/EXECUTE
--  rename columns in FROM clause
