-- start query 82 in stream 0 using template query82.tpl
SELECT
         i_item_id ,
         i_item_desc ,
         i_current_price
FROM     item,
         inventory,
         date_dim,
         store_sales
WHERE    i_current_price BETWEEN 63 AND      63+30
AND      inv_item_sk = i_item_sk
AND      d_date_sk=inv_date_sk
AND      d_date BETWEEN '1998-04-27' AND '1998-06-26'
AND      i_manufact_id IN (57,293,427,320)
AND      inv_quantity_on_hand BETWEEN 100 AND      500
AND      ss_item_sk = i_item_sk
GROUP BY i_item_id,
         i_item_desc,
         i_current_price
ORDER BY i_item_id
LIMIT 100;

