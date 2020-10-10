-- https://db.in.tum.de/research/projects/CHbenCHmark/?lang=en


-- Query 1
--
-- Original:
--
-- select ol_number,
--     sum(ol_quantity) as sum_qty,
--     sum(ol_amount) as sum_amount,
--     avg(ol_quantity) as avg_qty,
--     avg(ol_amount) as avg_amount,
--     count(*) as count_order
-- from orderline
-- where ol_delivery_d > '2007-01-02 00:00:00.000000'
-- group by ol_number order by ol_number
--
-- Changes:
-- (i) Capitalization.
-- (ii) Converted '2007-01-02' to unix timestamp 1170288000.
--
SELECT OL_NUMBER, SUM(OL_QUANTITY) AS SUM_QTY, SUM(OL_AMOUNT) AS SUM_AMOUNT, AVG(OL_QUANTITY) AS AVG_QTY, AVG(OL_AMOUNT) AS AVG_AMOUNT, COUNT(*) AS COUNT_ORDER FROM ORDER_LINE WHERE OL_DELIVERY_D > 1170288000 GROUP BY OL_NUMBER ORDER BY OL_NUMBER;


-- Query 2
--
-- Original:
--
-- select su_suppkey, su_name, n_name, i_id, i_name, su_address, su_phone, su_comment
-- from item, supplier, stock, nation, region,
--     (select s_i_id as m_i_id,
-- 	    min(s_quantity) as m_s_quantity
--     from stock, supplier, nation, region
--     where mod((s_w_id*s_i_id),10000)=su_suppkey
--         and su_nationkey=n_nationkey
--         and n_regionkey=r_regionkey
--         and r_name like 'Europ%'
--     group by s_i_id) m
-- where i_id = s_i_id
--     and mod((s_w_id * s_i_id), 10000) = su_suppkey
--     and su_nationkey = n_nationkey
--     and n_regionkey = r_regionkey
--     and i_data like '%b'
--     and r_name like 'Europ%'
--     and i_id=m_i_id
--     and s_quantity = m_s_quantity
-- order by n_name, su_name, i_id
--
-- Changes:
-- (i) Capitalization.
-- (ii) removed use of MOD()
-- (iii) Changed 'Europ%' to 'EUROP%' as it is generated like that by the TPC-H data generator.
--
-- Note: supplier, nation, and region are from the TPC-H data set 
--
SELECT s_suppkey, s_name, n_name, I_ID, I_NAME, s_address, s_phone, s_comment FROM ITEM, supplier, STOCK, nation, region, (SELECT STOCK.S_I_ID AS M_I_ID, MIN(S_QUANTITY) AS M_S_QUANTITY FROM STOCK, supplier, nation, region WHERE STOCK.S_W_ID*STOCK.S_I_ID=s_suppkey AND s_nationkey=n_nationkey AND n_regionkey=r_regionkey AND r_name LIKE 'EUROP%' GROUP BY STOCK.S_I_ID) M WHERE I_ID = STOCK.S_I_ID AND S_W_ID * STOCK.S_I_ID = s_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND I_DATA LIKE '%b' AND r_name LIKE 'EUROP%' AND I_ID=M_I_ID AND S_QUANTITY = M_S_QUANTITY ORDER BY n_name, s_name, I_ID;


-- Query 3
--
-- Original:
--
-- select   ol_o_id, ol_w_id, ol_d_id,
-- 	 sum(ol_amount) as revenue, o_entry_d
-- from 	 customer, neworder, orders, orderline
-- where 	 c_state like 'A%'
-- 	 and c_id = o_c_id
-- 	 and c_w_id = o_w_id
-- 	 and c_d_id = o_d_id
-- 	 and no_w_id = o_w_id
-- 	 and no_d_id = o_d_id
-- 	 and no_o_id = o_id
-- 	 and ol_w_id = o_w_id
-- 	 and ol_d_id = o_d_id
-- 	 and ol_o_id = o_id
-- 	 and o_entry_d > '2007-01-02 00:00:00.000000'
-- group by ol_o_id, ol_w_id, ol_d_id, o_entry_d
-- order by revenue desc, o_entry_d
--
-- Changes:
-- (i) Capitalization.
-- (ii) removed use of MOD()
-- (iii) Changed 'A%' to 'a%' as it is generated like that by the TPC-C data generator.
--
SELECT OL_O_ID, OL_W_ID, OL_D_ID, SUM(OL_AMOUNT) AS REVENUE, O_ENTRY_D FROM  CUSTOMER, NEW_ORDER, "ORDER", ORDER_LINE WHERE C_STATE LIKE 'a%' AND C_ID = O_C_ID AND C_W_ID = O_W_ID AND C_D_ID = O_D_ID AND NO_W_ID = O_W_ID AND NO_D_ID = O_D_ID AND NO_O_ID = O_ID AND OL_W_ID = O_W_ID AND OL_D_ID = O_D_ID AND OL_O_ID = O_ID AND O_ENTRY_D > 1170288000 GROUP BY OL_O_ID, OL_W_ID, OL_D_ID, O_ENTRY_D ORDER BY REVENUE DESC, O_ENTRY_D;


-- Query 4
--
-- Original:
--
-- select	o_ol_cnt, count(*) as order_count
-- from	orders
-- where	o_entry_d >= '2007-01-02 00:00:00.000000'
-- 	and o_entry_d < '2012-01-02 00:00:00.000000'
-- 	and exists (select *
-- 		    from orderline
-- 		    where o_id = ol_o_id
-- 		    and o_w_id = ol_w_id
-- 		    and o_d_id = ol_d_id
-- 		    and ol_delivery_d >= o_entry_d)
-- group	by o_ol_cnt
-- order	by o_ol_cnt
--
-- Changes:
-- (i) Capitalization.
-- (ii) removed use of MOD()
-- (iii) "ORDER" instead of orders
-- (iv) adaption of timestamps to use unix timestamps
--
SELECT O_OL_CNT, COUNT(*) AS ORDER_COUNT FROM "ORDER" WHERE O_ENTRY_D >= 1170288000 AND O_ENTRY_D < 1328054400 AND EXISTS (SELECT * FROM ORDER_LINE WHERE O_ID = OL_O_ID AND O_W_ID = OL_W_ID AND O_D_ID = OL_D_ID AND OL_DELIVERY_D >= O_ENTRY_D) GROUP BY O_OL_CNT ORDER BY O_OL_CNT;


-- Query 5
--
-- Original:
--
-- select	o_ol_cnt, count(*) as order_count
-- from	orders
-- where	o_entry_d >= '2007-01-02 00:00:00.000000'
-- 	and o_entry_d < '2012-01-02 00:00:00.000000'
-- 	and exists (select *
-- 		    from orderline
-- 		    where o_id = ol_o_id
-- 		    and o_w_id = ol_w_id
-- 		    and o_d_id = ol_d_id
-- 		    and ol_delivery_d >= o_entry_d)
-- group	by o_ol_cnt
-- order	by o_ol_cnt
--
-- Changes:
-- (i) Capitalization.
-- (ii) removed use of MOD()
-- (iii) "ORDER" instead of orders
-- (iv) adaption of timestamps to use unix timestamps
-- 
-----------------------------------------------------------               CAUTION: defect due to lack of ASCII()
--
-- SELECT n_name, SUM(OL_AMOUNT) AS REVENUE FROM CUSTOMER, "ORDER", ORDER_LINE, STOCK, supplier, nation, region WHERE C_ID = O_C_ID AND C_W_ID = O_W_ID AND C_D_ID = O_D_ID AND OL_O_ID = O_ID AND OL_W_ID = O_W_ID AND OL_D_ID=O_D_ID AND OL_W_ID = S_W_ID AND OL_I_ID = S_I_ID AND S_W_ID * S_I_ID = s_suppkey AND SUBSTR(C_STATE,1,1) = s_nationkey AND s_nationkey = n_nat AND n_regionkey = r_regionkey AND R_NAME = 'Europe' AND O_ENTRY_D >= 1170288000 GROUP BY N_NAME ORDER BY REVENUE DESC;


-- Query 6
--
-- Original:
--
-- select	sum(ol_amount) as revenue
-- from	orderline
-- where	ol_delivery_d >= '1999-01-01 00:00:00.000000'
-- 	and ol_delivery_d < '2020-01-01 00:00:00.000000'
-- 	and ol_quantity between 1 and 100000
--
-- Changes:
-- (i) Capitalization.
-- (ii) adaption of timestamps to use unix timestamps
-- 
SELECT SUM(OL_AMOUNT) AS REVENUE FROM ORDER_LINE WHERE OL_DELIVERY_D >= 915148800 AND OL_DELIVERY_D < 1577836800 AND OL_QUANTITY BETWEEN 1 AND 100000;


-- Query 7 uses ASCII()
-- Query 8 uses ASCII()
-- Query 9 uses EXTRACT(year) which we "hack" in TPC-H by substringing the dates, but TPC-C's dates are integers (unix timestamps)
-- Query 10 uses ASCII()

-- Query 11
-- 
-- Original:
--
-- select	 s_i_id, sum(s_order_cnt) as ordercount
-- from	 stock, supplier, nation
-- where	 mod((s_w_id * s_i_id),10000) = su_suppkey
-- 	 and su_nationkey = n_nationkey
-- 	 and n_name = 'Germany'
-- group by s_i_id
-- having   sum(s_order_cnt) >
-- 		(select sum(s_order_cnt) * .005
-- 		from stock, supplier, nation
-- 		where mod((s_w_id * s_i_id),10000) = su_suppkey
-- 		and su_nationkey = n_nationkey
-- 		and n_name = 'Germany')
-- order by ordercount desc
--
-- Changes:
-- (i) Capitalization.
-- (ii) removed use of MOD()
-- (iii) Use of TPC-H tables supplier and nation
-- (iv) Changed 'Germany' to 'GERMANY' as it is generated like that by the TPC-H data generator.
-- 
SELECT S_I_ID, SUM(S_ORDER_CNT) AS ORDERCOUNT FROM STOCK, supplier, nation WHERE S_W_ID * S_I_ID = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY' GROUP BY S_I_ID HAVING SUM(S_ORDER_CNT) > (SELECT SUM(S_ORDER_CNT) * .005 FROM STOCK, supplier, nation WHERE S_W_ID * S_I_ID = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY') ORDER BY ORDERCOUNT DESC;


-- Query 12
-- 
-- Original:
--
-- select	 o_ol_cnt,
-- 	 sum(case when o_carrier_id = 1 or o_carrier_id = 2 then 1 else 0 end) as high_line_count,
-- 	 sum(case when o_carrier_id <> 1 and o_carrier_id <> 2 then 1 else 0 end) as low_line_count
-- from	 orders, orderline
-- where	 ol_w_id = o_w_id
-- 	 and ol_d_id = o_d_id
-- 	 and ol_o_id = o_id
-- 	 and o_entry_d <= ol_delivery_d
-- 	 and ol_delivery_d < '2020-01-01 00:00:00.000000'
-- group by o_ol_cnt
-- order by o_ol_cnt
--
-- Changes:
-- (i) Capitalization.
-- (ii) Converted '2020-01-01' to unix timestamp 1577836800.
-- (iii) "ORDER" instead of orders
-- 
SELECT O_OL_CNT, SUM(CASE WHEN O_CARRIER_ID = 1 OR O_CARRIER_ID = 2 THEN 1 ELSE 0 END) AS HIGH_LINE_COUNT, SUM(CASE WHEN O_CARRIER_ID <> 1 AND O_CARRIER_ID <> 2 THEN 1 ELSE 0 END) AS LOW_LINE_COUNT FROM "ORDER", ORDER_LINE WHERE OL_W_ID = O_W_ID AND OL_D_ID = O_D_ID AND OL_O_ID = O_ID AND O_ENTRY_D <= OL_DELIVERY_D AND OL_DELIVERY_D < 1577836800 GROUP BY O_OL_CNT ORDER BY O_OL_CNT;


-- Query 13
-- 
-- Original:
--
-- select	 c_count, count(*) as custdist
-- from	 (select c_id, count(o_id)
-- 	 from customer left outer join orders on (
-- 		c_w_id = o_w_id
-- 		and c_d_id = o_d_id
-- 		and c_id = o_c_id
-- 		and o_carrier_id > 8)
-- 	 group by c_id) as c_orders (c_id, c_count)
-- group by c_count
-- order by custdist desc, c_count desc
--
-- Changes:
-- (i) Capitalization.
-- (ii) "ORDER" instead of orders
-- 
SELECT C_COUNT, COUNT(*) AS CUSTDIST FROM (SELECT C_ID, COUNT(O_ID) FROM CUSTOMER LEFT OUTER JOIN "ORDER" ON ( C_W_ID = O_W_ID AND C_D_ID = O_D_ID AND C_ID = O_C_ID AND O_CARRIER_ID > 8) GROUP BY C_ID) AS C_ORDERS (C_ID, C_COUNT) GROUP BY C_COUNT ORDER BY CUSTDIST DESC, C_COUNT DESC;


-- Query 14
-- 
-- Original:
--
-- select	100.00 * sum(case when i_data like 'PR%' then ol_amount else 0 end) / (1+sum(ol_amount)) as promo_revenue
-- from	order_line, item
-- where	ol_i_id = i_id and ol_delivery_d >= '2007-01-02 00:00:00.000000'
-- 	and ol_delivery_d < '2020-01-02 00:00:00.000000'
--
-- Changes:
-- (i) Capitalization.
-- (ii) Converted timestamps to unix timestamps.
-- 
SELECT 100.00 * SUM(CASE WHEN I_DATA LIKE 'PR%' THEN OL_AMOUNT ELSE 0 END) / (1+SUM(OL_AMOUNT)) AS PROMO_REVENUE FROM ORDER_LINE, ITEM WHERE OL_I_ID = I_ID AND OL_DELIVERY_D >= 1170288000 AND OL_DELIVERY_D < 1577836800;


-- Query 15
-- 
-- Original:
--
-- with	 revenue (supplier_no, total_revenue) as (
-- 	 select	mod((s_w_id * s_i_id),10000) as supplier_no,
-- 		sum(ol_amount) as total_revenue
-- 	 from	orderline, stock
-- 		where ol_i_id = s_i_id and ol_supply_w_id = s_w_id
-- 		and ol_delivery_d >= '2007-01-02 00:00:00.000000'
-- 	 group by mod((s_w_id * s_i_id),10000))
-- select	 su_suppkey, su_name, su_address, su_phone, total_revenue
-- from	 supplier, revenue
-- where	 su_suppkey = supplier_no
-- 	 and total_revenue = (select max(total_revenue) from revenue)
-- order by su_suppkey
--
-- Changes:
-- (i) Capitalization.
-- (ii) Converted timestamps to unix timestamps.
-- (iii) Discarding of MOD().
-- (iv) Use of views.
-- 
CREATE VIEW REVENUE (SUPPLIER_NO, TOTAL_REVENUE) AS SELECT S_W_ID * S_I_ID AS SUPPLIER_NO, SUM(OL_AMOUNT) AS TOTAL_REVENUE FROM ORDER_LINE, STOCK WHERE OL_I_ID = S_I_ID AND OL_SUPPLY_W_ID = S_W_ID AND OL_DELIVERY_D >= 1170288000 GROUP BY S_W_ID * S_I_ID;
SELECT s_suppkey, s_name, s_address, s_phone, TOTAL_REVENUE FROM supplier, REVENUE WHERE s_suppkey = SUPPLIER_NO AND TOTAL_REVENUE = (SELECT MAX(TOTAL_REVENUE) FROM REVENUE) ORDER BY s_suppkey;
DROP VIEW REVENUE;


-- Query 15
-- 
-- Original:
--
-- select	 i_name,
-- 	 substr(i_data, 1, 3) as brand,
-- 	 i_price,
-- 	 count(distinct (mod((s_w_id * s_i_id),10000))) as supplier_cnt
-- from	 stock, item
-- where	 i_id = s_i_id
-- 	 and i_data not like 'zz%'
-- 	 and (mod((s_w_id * s_i_id),10000) not in
-- 		(select su_suppkey
-- 		 from supplier
-- 		 where su_comment like '%bad%'))
-- group by i_name, substr(i_data, 1, 3), i_price
-- order by supplier_cnt desc
--
-- Changes:
-- (i) Capitalization.
-- (ii) Discarding of MOD().
--
SELECT I_NAME, SUBSTR(I_DATA, 1, 3) AS BRAND, I_PRICE, COUNT(DISTINCT (S_W_ID * S_I_ID)) AS SUPPLIER_CNT FROM STOCK, ITEM WHERE I_ID = S_I_ID AND I_DATA NOT LIKE 'ZZ%' AND (S_W_ID * S_I_ID NOT IN (SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%BAD%')) GROUP BY I_NAME, SUBSTR(I_DATA, 1, 3), I_PRICE ORDER BY SUPPLIER_CNT DESC;
