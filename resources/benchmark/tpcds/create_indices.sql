-- store sales
---- primary
create index ss_item_sk on store_sales(ss_item_sk);
create index ss_ticket_number on store_sales(ss_ticket_number);
---- foreign
create index ss_sold_date_sk on store_sales(ss_sold_date_sk);
create index ss_sold_time_sk on store_sales(ss_sold_time_sk);
create index ss_customer_sk on store_sales(ss_customer_sk);
create index ss_cdemo_sk on store_sales(ss_cdemo_sk);
create index ss_hdemo_sk on store_sales(ss_hdemo_sk);
create index ss_addr_sk on store_sales(ss_addr_sk);
create index ss_store_sk on store_sales(ss_store_sk);
create index ss_promo_sk on store_sales(ss_promo_sk);

-- store returns
---- primary
create index sr_item_sk on store_returns(sr_item_sk);	
create index sr_ticket_number on store_returns(sr_ticket_number);	
---- foreign
create index sr_returned_date_sk on store_returns(sr_returned_date_sk);
create index sr_return_time_sk on store_returns(sr_return_time_sk);
create index sr_customer_sk on store_returns(sr_customer_sk);
create index sr_cdemo_sk on store_returns(sr_cdemo_sk);
create index sr_hdemo_sk on store_returns(sr_hdemo_sk);
create index sr_addr_sk on store_returns(sr_addr_sk);
create index sr_store_sk on store_returns(sr_store_sk);
create index sr_reason_sk on store_returns(sr_reason_sk);

-- catalog sales
---- primary
create index cs_item_sk on catalog_sales(cs_item_sk);
create index cs_order_number on catalog_sales(cs_order_number);	
---- foreign
create index cs_sold_date_sk on catalog_sales(cs_sold_date_sk);
create index cs_sold_time_sk on catalog_sales(cs_sold_time_sk);
create index cs_ship_date_sk on catalog_sales(cs_ship_date_sk);
create index cs_bill_customer_sk on catalog_sales(cs_bill_customer_sk);
create index cs_bill_cdemo_sk on catalog_sales(cs_bill_cdemo_sk);
create index cs_bill_hdemo_sk on catalog_sales(cs_bill_hdemo_sk);
create index cs_bill_addr_sk on catalog_sales(cs_bill_addr_sk);
create index cs_ship_customer_sk on catalog_sales(cs_ship_customer_sk);
create index cs_ship_cdemo_sk on catalog_sales(cs_ship_cdemo_sk);
create index cs_ship_hdemo_sk on catalog_sales(cs_ship_hdemo_sk);
create index cs_ship_addr_sk on catalog_sales(cs_ship_addr_sk);
create index cs_call_center_sk on catalog_sales(cs_call_center_sk);
create index cs_catalog_page_sk on catalog_sales(cs_catalog_page_sk);
create index cs_ship_mode_sk on catalog_sales(cs_ship_mode_sk);
create index cs_warehouse_sk on catalog_sales(cs_warehouse_sk);
create index cs_promo_sk on catalog_sales(cs_promo_sk);

-- catalog returns
---- primary
create index cr_item_sk on catalog_returns(cr_item_sk);	
create index cr_order_number on catalog_returns(cr_order_number);
---- foreign
create index cr_returned_date_sk on catalog_returns(cr_returned_date_sk);
create index cr_returned_time_sk on catalog_returns(cr_returned_time_sk);
create index cr_refunded_customer_sk on catalog_returns(cr_refunded_customer_sk);
create index cr_refunded_cdemo_sk on catalog_returns(cr_refunded_cdemo_sk);
create index cr_refunded_hdemo_sk on catalog_returns(cr_refunded_hdemo_sk);
create index cr_refunded_addr_sk on catalog_returns(cr_refunded_addr_sk);
create index cr_returning_customer_sk on catalog_returns(cr_returning_customer_sk);
create index cr_returning_cdemo_sk on catalog_returns(cr_returning_cdemo_sk);
create index cr_returning_hdemo_sk on catalog_returns(cr_returning_hdemo_sk);
create index cr_returning_addr_sk on catalog_returns(cr_returning_addr_sk);
create index cr_call_center_sk on catalog_returns(cr_call_center_sk);
create index cr_catalog_page_sk on catalog_returns(cr_catalog_page_sk);
create index cr_ship_mode_sk on catalog_returns(cr_ship_mode_sk);
create index cr_warehouse_sk on catalog_returns(cr_warehouse_sk);
create index cr_reason_sk on catalog_returns(cr_reason_sk);
create index cr_return_quantity on catalog_returns(cr_return_quantity);

-- web sales
---- primary
create index ws_item_sk on web_sales(ws_item_sk);	
create index ws_order_number on web_sales(ws_order_number);	
---- foreign
create index ws_sold_date_sk on web_sales(ws_sold_date_sk);
create index ws_sold_time_sk on web_sales(ws_sold_time_sk);
create index ws_ship_date_sk on web_sales(ws_ship_date_sk);
create index ws_bill_customer_sk on web_sales(ws_bill_customer_sk);
create index ws_bill_cdemo_sk on web_sales(ws_bill_cdemo_sk);
create index ws_bill_hdemo_sk on web_sales(ws_bill_hdemo_sk);
create index ws_bill_addr_sk on web_sales(ws_bill_addr_sk);
create index ws_ship_customer_sk on web_sales(ws_ship_customer_sk);
create index ws_ship_cdemo_sk on web_sales(ws_ship_cdemo_sk);
create index ws_ship_hdemo_sk on web_sales(ws_ship_hdemo_sk);
create index ws_ship_addr_sk on web_sales(ws_ship_addr_sk);
create index ws_web_page_sk on web_sales(ws_web_page_sk);
create index ws_web_site_sk on web_sales(ws_web_site_sk);
create index ws_ship_mode_sk on web_sales(ws_ship_mode_sk);
create index ws_warehouse_sk on web_sales(ws_warehouse_sk);
create index ws_promo_sk on web_sales(ws_promo_sk);

-- web returns
---- primary
create index wr_item_sk on web_returns(wr_item_sk);
create index wr_order_number on web_returns(wr_order_number);	
---- foreign
create index wr_returned_date_sk on web_returns(wr_returned_date_sk);
create index wr_returned_time_sk on web_returns(wr_returned_time_sk);
create index wr_refunded_customer_sk on web_returns(wr_refunded_customer_sk);
create index wr_refunded_cdemo_sk on web_returns(wr_refunded_cdemo_sk);
create index wr_refunded_hdemo_sk on web_returns(wr_refunded_hdemo_sk);
create index wr_refunded_addr_sk on web_returns(wr_refunded_addr_sk);
create index wr_returning_customer_sk on web_returns(wr_returning_customer_sk);
create index wr_returning_cdemo_sk on web_returns(wr_returning_cdemo_sk);
create index wr_returning_hdemo_sk on web_returns(wr_returning_hdemo_sk);
create index wr_returning_addr_sk on web_returns(wr_returning_addr_sk);
create index wr_web_page_sk on web_returns(wr_web_page_sk);
create index wr_reason_sk on web_returns(wr_reason_sk);

-- inventory
---- primary & foreign
create index inv_date_sk on inventory(inv_date_sk);	
create index inv_item_sk on inventory(inv_item_sk);
create index inv_warehouse_sk on inventory(inv_warehouse_sk);	

-- store
---- primary
create index s_store_sk on store(s_store_sk);
---- foreign
create index s_closed_date_sk on store(s_closed_date_sk);

-- call center
---- primary
create index cc_call_center_sk on call_center(cc_call_center_sk);	
---- foreign
create index cc_closed_date_sk on call_center(cc_closed_date_sk);
create index cc_open_date_sk on call_center(cc_open_date_sk);

-- catalog page
---- primary
create index cp_catalog_page_sk on catalog_page(cp_catalog_page_sk);	
---- foreign
create index cp_start_date_sk on catalog_page(cp_start_date_sk);	
create index cp_end_date_sk on catalog_page(cp_end_date_sk);	

-- web site
---- primary
create index web_site_sk on web_site(web_site_sk);
---- foreign
create index web_open_date_sk on web_site(web_open_date_sk);
create index web_close_date_sk on web_site(web_close_date_sk);

-- web page
---- primary
create index wp_web_page_sk on web_page(wp_web_page_sk);	
---- foreign
create index wp_creation_date_sk on web_page(wp_creation_date_sk);
create index wp_access_date_sk on web_page(wp_access_date_sk);
create index wp_customer_sk on web_page(wp_customer_sk);

-- warehouse
---- primary
create index w_warehouse_sk on warehouse(w_warehouse_sk);	
---- foreign (none)

-- customer
---- primary
create index c_customer_sk on customer(c_customer_sk);	
---- foreign
create index c_current_cdemo_sk on customer(c_current_cdemo_sk);	
create index c_current_hdemo_sk on customer(c_current_hdemo_sk);	
create index c_current_addr_sk on customer(c_current_addr_sk);	
create index c_first_shipto_date_sk on customer(c_first_shipto_date_sk);	
create index c_first_sales_date_sk on customer(c_first_sales_date_sk);	
create index c_last_review_date on customer(c_last_review_date);	

-- customer address
---- primary
create index ca_address_sk on customer_address(ca_address_sk);
---- foreign (none)

-- customer demopgaphics
---- primary
create index cd_demo_sk on customer_demographics(cd_demo_sk);	
---- foreign (none)

-- date dim
---- primary
create index d_date_sk on date_dim(d_date_sk);	
---- foreign (none)

-- household demogrphics
---- primary
create index hd_demo_sk on household_demographics(hd_demo_sk);	
---- foreign
create index hd_income_band_sk on household_demographics(hd_income_band_sk);	

-- item
---- primary
create index i_item_sk on item(i_item_sk);
---- foreign (none)

-- income band
---- primary
create index ib_income_band_sk on income_band(ib_income_band_sk);	
---- foreign (none)

-- promotion
---- primary
create index p_promo_sk on promotion(p_promo_sk);	
---- foreign
create index p_start_date_sk on promotion(p_start_date_sk);	
create index p_end_date_sk on promotion(p_end_date_sk);	
create index p_item_sk on promotion(p_item_sk);	

-- reason
---- primary
create index r_reason_sk on reason(r_reason_sk);
---- foreign (none)

-- ship mode
---- primary
create index sm_ship_mode_sk on ship_mode(sm_ship_mode_sk);
---- foreign (none)

-- time dim
---- primary
create index t_time_sk on time_dim(t_time_sk);
---- foreign (none)
