-- start query 16 in stream 0 using template query16.tpl
SELECT
         Count(DISTINCT cs_order_number) AS order_count ,
         Sum(cs_ext_ship_cost)           AS total_shipping_cost ,
         Sum(cs_net_profit)              AS total_net_profit
FROM     catalog_sales cs1 ,
         date_dim ,
         customer_address ,
         call_center
WHERE    d_date BETWEEN '2002-03-01' AND '2002-04-30'
AND      cs1.cs_ship_date_sk = d_date_sk
AND      cs1.cs_ship_addr_sk = ca_address_sk
AND      ca_state = 'IA'
AND      cs1.cs_call_center_sk = cc_call_center_sk
AND      cc_county IN ('Williamson County',
                       'Williamson County',
                       'Williamson County',
                       'Williamson County',
                       'Williamson County' )
AND      EXISTS
         (
                SELECT *
                FROM   catalog_sales cs2
                WHERE  cs1.cs_order_number = cs2.cs_order_number
                AND    cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
AND      NOT EXISTS
         (
                SELECT *
                FROM   catalog_returns cr1
                WHERE  cs1.cs_order_number = cr1.cr_order_number)
ORDER BY count(DISTINCT cs_order_number)
LIMIT 100;

