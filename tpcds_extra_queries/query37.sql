-- start query 37 in stream 0 using template query37.tpl
-- empty?
SELECT
         i_item_id ,
         i_item_desc ,
         i_current_price
FROM     item,
         inventory,
         date_dim,
         catalog_sales
WHERE    i_current_price BETWEEN 20 AND      20 + 30
AND      inv_item_sk = i_item_sk
AND      d_date_sk=inv_date_sk
AND      d_date BETWEEN '1999-03-06' AND '1999-05-05'
AND      i_manufact_id IN (843,815,850,840)
AND      inv_quantity_on_hand BETWEEN 100 AND      500
AND      cs_item_sk = i_item_sk
GROUP BY i_item_id,
         i_item_desc,
         i_current_price
ORDER BY i_item_id
LIMIT 100;

