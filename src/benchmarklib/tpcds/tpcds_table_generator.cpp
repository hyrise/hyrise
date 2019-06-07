#include "tpcds_table_generator.hpp"

extern "C" {
#include <../tpcds-dbgen/config.h>  // must be included before porting.h, otherwise HUGE_TYPE is not found
#include <porting.h>                // must be included before dbgen_version, otherwise ds_key_t is not found

#include <dbgen_version.h>
#include <decimal.h>
#include <genrand.h>
#include <parallel.h>
#include <r_params.h>
#include <tables.h>
#include <tdefs.h>

#include <w_call_center.h>
#include <w_catalog_page.h>
#include <w_catalog_returns.h>
#include <w_catalog_sales.h>
#include <w_customer.h>
#include <w_customer_address.h>
#include <w_customer_demographics.h>
#include <w_datetbl.h>
#include <w_household_demographics.h>
#include <w_income_band.h>
#include <w_inventory.h>
#include <w_item.h>
#include <w_promotion.h>
#include <w_reason.h>
#include <w_ship_mode.h>
#include <w_store.h>
#include <w_store_returns.h>
#include <w_store_sales.h>
#include <w_timetbl.h>
#include <w_warehouse.h>
#include <w_web_page.h>
#include <w_web_returns.h>
#include <w_web_sales.h>
#include <w_web_site.h>
}

#include <benchmark_config.hpp>
#include <table_builder.hpp>

namespace {

using namespace opossum;  // NOLINT

void set_rng_seed(int rng_seed) {
  auto rng_seed_string = std::string{"RNGSEED"};
  auto rng_seed_value_string = std::to_string(rng_seed);
  set_int(rng_seed_string.data(), rng_seed_value_string.data());
  init_rand();
}

void set_scale_factor(uint32_t scale_factor) {
  auto scale_factor_string = std::string{"SCALE"};
  auto scale_factor_value_string = std::to_string(scale_factor);
  set_int(scale_factor_string.data(), scale_factor_value_string.data());
}

// TpcdsRow is the type of a database row, e.g. CALL_CENTER_TBL
template <class TpcdsRow, int builder(void*, ds_key_t), int table_id>
TpcdsRow call_dbgen_mk(ds_key_t index) {
  auto tpcds_row = TpcdsRow{};
  builder(&tpcds_row, index);
  tpcds_row_stop(table_id);
  return tpcds_row;
}

std::pair<ds_key_t, ds_key_t> prepare_for_table(int table_id) {
  auto k_row_count = ds_key_t{};
  auto k_first_row = ds_key_t{};

  split_work(table_id, &k_first_row, &k_row_count);

  const auto& tdefs = *getSimpleTdefsByNumber(table_id);

  if (k_first_row != 1) {
    row_skip(table_id, static_cast<int>(k_first_row - 1));
    if (tdefs.flags & FL_PARENT) {
      row_skip(tdefs.nParam, static_cast<int>(k_first_row - 1));
    }
  }

  if (tdefs.flags & FL_SMALL) {
    resetCountCount();
  }

  return {k_first_row, k_row_count};
}

float decimal_to_float(decimal_t decimal) {
  auto result = 0.0;
  dectof(&result, &decimal);
  return static_cast<float>(result / 10);
}

pmr_string resolve_date_id(ds_key_t date_id) {
  if (date_id <= 0) {
    return pmr_string{};
  }

  auto date = date_t{};
  jtodt(&date, static_cast<int>(date_id));
  return pmr_string{dttostr(&date)};
}

pmr_string convert_boolean(bool boolean) { return pmr_string(1, boolean ? 'Y' : 'N'); }

std::optional<int64_t> convert_key(ds_key_t key) {
  return key == -1 ? std::nullopt : std::optional<int64_t>{static_cast<int64_t>(key)};
}

// mapping types used by tpcds-dbgen as follows:
// ds_key_t -> int64_t
// int -> int32_t
// char* and char[] -> pmr_string
// decimal -> float (using decimal_to_float function)
// ds_addr_t -> corresponding types for types in struct ds_addr_t, see address.h
// date_t / ds_key_t (as date id) -> pmr_string (using resolve_date_id function)

// the types are derived from print functions (eg. pr_w_call_center in w_call_center.c), because these are used by the
// dsdgen command line tool to create the *.dat files

// clang-format off
const auto call_center_column_types = boost::hana::tuple<std::optional<int64_t>, pmr_string, pmr_string, pmr_string, std::optional<int64_t>, std::optional<int64_t>, pmr_string, pmr_string, int32_t, int32_t, pmr_string, pmr_string, int32_t, pmr_string, pmr_string, pmr_string, int32_t, pmr_string, int32_t, pmr_string, int32_t, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, int32_t, pmr_string, int32_t, float>(); // NOLINT
const auto call_center_column_names = boost::hana::make_tuple("cc_call_center_sk", "cc_call_center_id", "cc_rec_start_date", "cc_rec_end_date", "cc_closed_date_sk", "cc_open_date_sk", "cc_name", "cc_class", "cc_employees", "cc_sq_ft", "cc_hours", "cc_manager", "cc_mkt_id", "cc_mkt_class", "cc_mkt_desc", "cc_market_manager", "cc_division", "cc_division_name", "cc_company", "cc_company_name", "cc_street_number", "cc_street_name", "cc_street_type", "cc_suite_number", "cc_city", "cc_county", "cc_state", "cc_zip", "cc_country", "cc_gmt_offset", "cc_tax_percentage"); // NOLINT

const auto catalog_page_column_types = boost::hana::tuple<std::optional<int64_t>, pmr_string, std::optional<int64_t>, std::optional<int64_t>, pmr_string, int32_t, int32_t, pmr_string, pmr_string>(); // NOLINT
const auto catalog_page_column_names = boost::hana::make_tuple("cp_catalog_page_sk", "cp_catalog_page_id", "cp_start_date_sk", "cp_end_date_sk", "cp_department", "cp_catalog_number", "cp_catalog_page_number", "cp_description", "cp_type"); // NOLINT

const auto catalog_returns_column_types = boost::hana::tuple<std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, int32_t, float, float, float, float, float, float, float, float, float>(); // NOLINT
const auto catalog_returns_column_names = boost::hana::make_tuple("cr_returned_date_sk", "cr_returned_time_sk", "cr_item_sk", "cr_refunded_customer_sk", "cr_refunded_cdemo_sk", "cr_refunded_hdemo_sk", "cr_refunded_addr_sk", "cr_returning_customer_sk", "cr_returning_cdemo_sk", "cr_returning_hdemo_sk", "cr_returning_addr_sk", "cr_call_center_sk", "cr_catalog_page_sk", "cr_ship_mode_sk", "cr_warehouse_sk", "cr_reason_sk", "cr_order_number", "cr_return_quantity", "cr_return_amount", "cr_return_tax", "cr_return_amt_inc_tax", "cr_fee", "cr_return_ship_cost", "cr_refunded_cash", "cr_reversed_charge", "cr_store_credit", "cr_net_loss"); // NOLINT

const auto catalog_sales_column_types = boost::hana::tuple<std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, int32_t, float, float, float, float, float, float, float, float, float, float, float, float, float, float, float>(); // NOLINT
const auto catalog_sales_column_names = boost::hana::make_tuple("cs_sold_date_sk", "cs_sold_time_sk", "cs_ship_date_sk", "cs_bill_customer_sk", "cs_bill_cdemo_sk", "cs_bill_hdemo_sk", "cs_bill_addr_sk", "cs_ship_customer_sk", "cs_ship_cdemo_sk", "cs_ship_hdemo_sk", "cs_ship_addr_sk", "cs_call_center_sk", "cs_catalog_page_sk", "cs_ship_mode_sk", "cs_warehouse_sk", "cs_item_sk", "cs_promo_sk", "cs_order_number", "cs_quantity", "cs_wholesale_cost", "cs_list_price", "cs_sales_price", "cs_ext_discount_amt", "cs_ext_sales_price", "cs_ext_wholesale_cost", "cs_ext_list_price", "cs_ext_tax", "cs_coupon_amt", "cs_ext_ship_cost", "cs_net_paid", "cs_net_paid_inc_tax", "cs_net_paid_inc_ship", "cs_net_paid_inc_ship_tax", "cs_net_profit"); // NOLINT

const auto customer_column_types = boost::hana::tuple<std::optional<int64_t>, pmr_string, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, int32_t, int32_t, pmr_string, pmr_string, pmr_string, pmr_string, int32_t, int32_t, int32_t, pmr_string, pmr_string, pmr_string, int32_t>(); // NOLINT
const auto customer_column_names = boost::hana::make_tuple("c_customer_sk", "c_customer_id", "c_current_cdemo_sk", "c_current_hdemo_sk", "c_current_addr_sk", "c_first_shipto_date_sk", "c_first_sales_date_sk", "c_salutation", "c_first_name", "c_last_name", "c_preferred_cust_flag", "c_birth_day", "c_birth_month", "c_birth_year", "c_birth_country", "c_login", "c_email_address", "c_last_review_date"); // NOLINT

const auto customer_address_column_types = boost::hana::tuple<std::optional<int64_t>, pmr_string, int32_t, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, int32_t, pmr_string, int32_t, pmr_string>(); // NOLINT
const auto customer_address_column_names = boost::hana::make_tuple("ca_address_sk", "ca_address_id", "ca_street_number", "ca_street_name", "ca_street_type", "ca_suite_number", "ca_city", "ca_county", "ca_state", "ca_zip", "ca_country", "ca_gmt_offset", "ca_location_type"); // NOLINT

const auto customer_demographics_column_types = boost::hana::tuple<std::optional<int64_t>, pmr_string, pmr_string, pmr_string, int32_t, pmr_string, int32_t, int32_t, int32_t>(); // NOLINT
const auto customer_demographics_column_names = boost::hana::make_tuple("cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status", "cd_purchase_estimate", "cd_credit_rating", "cd_dep_count", "cd_dep_employed_count", "cd_dep_college_count"); // NOLINT

const auto date_column_types = boost::hana::tuple<std::optional<int64_t>, pmr_string, pmr_string, int32_t, int32_t, int32_t, int32_t, int32_t, int32_t, int32_t, int32_t, int32_t, int32_t, int32_t, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, int32_t, int32_t, int32_t, int32_t, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string>(); // NOLINT
const auto date_column_names = boost::hana::make_tuple("d_date_sk", "d_date_id", "d_date", "d_month_seq", "d_week_seq", "d_quarter_seq", "d_year", "d_dow", "d_moy", "d_dom", "d_qoy", "d_fy_year", "d_fy_quarter_seq", "d_fy_week_seq", "d_day_name", "d_quarter_name", "d_holiday", "d_weekend", "d_following_holiday", "d_first_dom", "d_last_dom", "d_same_day_ly", "d_same_day_lq", "d_current_day", "d_current_week", "d_current_month", "d_current_quarter", "d_current_year"); // NOLINT

const auto household_demographics_column_types = boost::hana::tuple<std::optional<int64_t>, std::optional<int64_t>, pmr_string, int32_t, int32_t>(); // NOLINT
const auto household_demographics_column_names = boost::hana::make_tuple("hd_demo_sk", "hd_income_band_sk", "hd_buy_potential", "hd_dep_count", "hd_vehicle_count"); // NOLINT

const auto income_band_column_types = boost::hana::tuple<int32_t, int32_t, int32_t>(); // NOLINT
const auto income_band_column_names = boost::hana::make_tuple("ib_income_band_sk", "ib_lower_bound", "ib_upper_bound"); // NOLINT

const auto inventory_column_types = boost::hana::tuple<std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, int32_t>(); // NOLINT
const auto inventory_column_names = boost::hana::make_tuple("inv_date_sk", "inv_item_sk", "inv_warehouse_sk", "inv_quantity_on_hand"); // NOLINT

const auto item_column_types = boost::hana::tuple<std::optional<int64_t>, pmr_string, pmr_string, pmr_string, pmr_string, float, float, std::optional<int64_t>, pmr_string, std::optional<int64_t>, pmr_string, std::optional<int64_t>, pmr_string, std::optional<int64_t>, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, std::optional<int64_t>, pmr_string>(); // NOLINT
const auto item_column_names = boost::hana::make_tuple("i_item_sk", "i_item_id", "i_rec_start_date", "i_rec_end_date", "i_item_desc", "i_current_price", "i_wholesale_cost", "i_brand_id", "i_brand", "i_class_id", "i_class", "i_category_id", "i_category", "i_manufact_id", "i_manufact", "i_size", "i_formulation", "i_color", "i_units", "i_container", "i_manager_id", "i_product_name"); // NOLINT

const auto promotion_column_types = boost::hana::tuple<std::optional<int64_t>, pmr_string, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, float, int32_t, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string>(); // NOLINT
const auto promotion_column_names = boost::hana::make_tuple("p_promo_sk", "p_promo_id", "p_start_date_sk", "p_end_date_sk", "p_item_sk", "p_cost", "p_response_target", "p_promo_name", "p_channel_dmail", "p_channel_email", "p_channel_catalog", "p_channel_tv", "p_channel_radio", "p_channel_press", "p_channel_event", "p_channel_demo", "p_channel_details", "p_purpose", "p_discount_active"); // NOLINT

const auto reason_column_types = boost::hana::tuple<std::optional<int64_t>, pmr_string, pmr_string>(); // NOLINT
const auto reason_column_names = boost::hana::make_tuple("r_reason_sk", "r_reason_id", "r_reason_desc"); // NOLINT

const auto ship_mode_column_types = boost::hana::tuple<std::optional<int64_t>, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string>(); // NOLINT
const auto ship_mode_column_names = boost::hana::make_tuple("sm_ship_mode_sk", "sm_ship_mode_id", "sm_type", "sm_code", "sm_carrier", "sm_contract"); // NOLINT

const auto store_column_types = boost::hana::tuple<std::optional<int64_t>, pmr_string, pmr_string, pmr_string, std::optional<int64_t>, pmr_string, int32_t, int32_t, pmr_string, pmr_string, int32_t, pmr_string, pmr_string, pmr_string, std::optional<int64_t>, pmr_string, std::optional<int64_t>, pmr_string, int32_t, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, int32_t, pmr_string, int32_t, float>(); // NOLINT
const auto store_column_names = boost::hana::make_tuple("s_store_sk", "s_store_id", "s_rec_start_date", "s_rec_end_date", "s_closed_date_sk", "s_store_name", "s_number_employees", "s_floor_space", "s_hours", "s_manager", "s_market_id", "s_geography_class", "s_market_desc", "s_market_manager", "s_division_id", "s_division_name", "s_company_id", "s_company_name", "s_street_number", "s_street_name", "s_street_type", "s_suite_number", "s_city", "s_county", "s_state", "s_zip", "s_country", "s_gmt_offset", "s_tax_precentage"); // NOLINT

const auto store_returns_column_types = boost::hana::tuple<std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, int32_t, float, float, float, float, float, float, float, float, float>(); // NOLINT
const auto store_returns_column_names = boost::hana::make_tuple("sr_returned_date_sk", "sr_return_time_sk", "sr_item_sk", "sr_customer_sk", "sr_cdemo_sk", "sr_hdemo_sk", "sr_addr_sk", "sr_store_sk", "sr_reason_sk", "sr_ticket_number", "sr_return_quantity", "sr_return_amt", "sr_return_tax", "sr_return_amt_inc_tax", "sr_fee", "sr_return_ship_cost", "sr_refunded_cash", "sr_reversed_charge", "sr_store_credit", "sr_net_loss"); // NOLINT

const auto store_sales_column_types = boost::hana::tuple<std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, int32_t, float, float, float, float, float, float, float, float, float, float, float, float>(); // NOLINT
const auto store_sales_column_names = boost::hana::make_tuple("ss_sold_date_sk", "ss_sold_time_sk", "ss_item_sk", "ss_customer_sk", "ss_cdemo_sk", "ss_hdemo_sk", "ss_addr_sk", "ss_store_sk", "ss_promo_sk", "ss_ticket_number", "ss_quantity", "ss_wholesale_cost", "ss_list_price", "ss_sales_price", "ss_ext_discount_amt", "ss_ext_sales_price", "ss_ext_wholesale_cost", "ss_ext_list_price", "ss_ext_tax", "ss_coupon_amt", "ss_net_paid", "ss_net_paid_inc_tax", "ss_net_profit"); // NOLINT

const auto time_column_types = boost::hana::tuple<std::optional<int64_t>, pmr_string, int32_t, int32_t, int32_t, int32_t, pmr_string, pmr_string, pmr_string, pmr_string>(); // NOLINT
const auto time_column_names = boost::hana::make_tuple("t_time_sk", "t_time_id", "t_time", "t_hour", "t_minute", "t_second", "t_am_pm", "t_shift", "t_sub_shift", "t_meal_time"); // NOLINT

const auto warehouse_column_types = boost::hana::tuple<std::optional<int64_t>, pmr_string, pmr_string, int32_t, int32_t, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, int32_t, pmr_string, int32_t>(); // NOLINT
const auto warehouse_column_names = boost::hana::make_tuple("w_warehouse_sk", "w_warehouse_id", "w_warehouse_name", "w_warehouse_sq_ft", "w_street_number", "w_street_name", "w_street_type", "w_suite_number", "w_city", "w_county", "w_state", "w_zip", "w_country", "w_gmt_offset"); // NOLINT

const auto web_page_column_types = boost::hana::tuple<std::optional<int64_t>, pmr_string, pmr_string, pmr_string, std::optional<int64_t>, std::optional<int64_t>, pmr_string, std::optional<int64_t>, pmr_string, pmr_string, int32_t, int32_t, int32_t, int32_t>(); // NOLINT
const auto web_page_column_names = boost::hana::make_tuple("wp_web_page_sk", "wp_web_page_id", "wp_rec_start_date", "wp_rec_end_date", "wp_creation_date_sk", "wp_access_date_sk", "wp_autogen_flag", "wp_customer_sk", "wp_url", "wp_type", "wp_char_count", "wp_link_count", "wp_image_count", "wp_max_ad_count"); // NOLINT

const auto web_returns_column_types = boost::hana::tuple<std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, int32_t, float, float, float, float, float, float, float, float, float>(); // NOLINT
const auto web_returns_column_names = boost::hana::make_tuple("wr_returned_date_sk", "wr_returned_time_sk", "wr_item_sk", "wr_refunded_customer_sk", "wr_refunded_cdemo_sk", "wr_refunded_hdemo_sk", "wr_refunded_addr_sk", "wr_returning_customer_sk", "wr_returning_cdemo_sk", "wr_returning_hdemo_sk", "wr_returning_addr_sk", "wr_web_page_sk", "wr_reason_sk", "wr_order_number", "wr_return_quantity", "wr_return_amt", "wr_return_tax", "wr_return_amt_inc_tax", "wr_fee", "wr_return_ship_cost", "wr_refunded_cash", "wr_reversed_charge", "wr_account_credit", "wr_net_loss"); // NOLINT

const auto web_sales_column_types = boost::hana::tuple<std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, std::optional<int64_t>, int32_t, float, float, float, float, float, float, float, float, float, float, float, float, float, float, float>(); // NOLINT
const auto web_sales_column_names = boost::hana::make_tuple("ws_sold_date_sk", "ws_sold_time_sk", "ws_ship_date_sk", "ws_item_sk", "ws_bill_customer_sk", "ws_bill_cdemo_sk", "ws_bill_hdemo_sk", "ws_bill_addr_sk", "ws_ship_customer_sk", "ws_ship_cdemo_sk", "ws_ship_hdemo_sk", "ws_ship_addr_sk", "ws_web_page_sk", "ws_web_site_sk", "ws_ship_mode_sk", "ws_warehouse_sk", "ws_promo_sk", "ws_order_number", "ws_quantity", "ws_wholesale_cost", "ws_list_price", "ws_sales_price", "ws_ext_discount_amt", "ws_ext_sales_price", "ws_ext_wholesale_cost", "ws_ext_list_price", "ws_ext_tax", "ws_coupon_amt", "ws_ext_ship_cost", "ws_net_paid", "ws_net_paid_inc_tax", "ws_net_paid_inc_ship", "ws_net_paid_inc_ship_tax", "ws_net_profit"); // NOLINT

const auto web_site_column_types = boost::hana::tuple<std::optional<int64_t>, pmr_string, pmr_string, pmr_string, pmr_string, std::optional<int64_t>, std::optional<int64_t>, pmr_string, pmr_string, int32_t, pmr_string, pmr_string, pmr_string, int32_t, pmr_string, int32_t, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, int32_t, float>(); // NOLINT
const auto web_site_column_names = boost::hana::make_tuple("web_site_sk", "web_site_id", "web_rec_start_date", "web_rec_end_date", "web_name", "web_open_date_sk", "web_close_date_sk", "web_class", "web_manager", "web_mkt_id", "web_mkt_class", "web_mkt_desc", "web_market_manager", "web_company_id", "web_company_name", "web_street_number", "web_street_name", "web_street_type", "web_suite_number", "web_city", "web_county", "web_state", "web_zip", "web_country", "web_gmt_offset", "web_tax_percentage"); // NOLINT

const auto dbgen_version_column_types = boost::hana::tuple<pmr_string, pmr_string, pmr_string, pmr_string>(); // NOLINT
const auto dbgen_version_column_names = boost::hana::make_tuple("dv_version", "dv_create_date", "dv_create_time", "dv_cmdline_args"); // NOLINT
// clang-format on
}  // namespace

namespace opossum {

// TODO(anyone): allow arbitrary scale factors. dsdgen only supports 9 scale factors, see scaling files like scaling.dst

TpcdsTableGenerator::TpcdsTableGenerator(uint32_t scale_factor, uint32_t chunk_size)
    : AbstractTableGenerator(create_benchmark_config_with_chunk_size(chunk_size)), _scale_factor(scale_factor) {}

TpcdsTableGenerator::TpcdsTableGenerator(uint32_t scale_factor,
                                         const std::shared_ptr<BenchmarkConfig>& benchmark_config)
    : AbstractTableGenerator(benchmark_config), _scale_factor(scale_factor) {}

std::unordered_map<std::string, BenchmarkTableInfo> TpcdsTableGenerator::generate() {
  set_scale_factor(_scale_factor);
  set_rng_seed(0);
  auto table_info_by_name = std::unordered_map<std::string, BenchmarkTableInfo>();

  // call center
  {
    const auto [call_center_first, call_center_count] = prepare_for_table(CALL_CENTER);

    auto call_center_builder =
        TableBuilder{_benchmark_config->chunk_size, call_center_column_types, call_center_column_names, 
                     static_cast<size_t>(call_center_count)};

    auto call_center = CALL_CENTER_TBL{};
    for (auto i = ds_key_t{0}; i < call_center_count; i++) {
      // mk_w_call_center needs a pointer to the previous result of mk_w_call_center to add "update entries" for the
      // same call center
      mk_w_call_center(&call_center, call_center_first + i);
      tpcds_row_stop(CALL_CENTER);

      auto street_name = pmr_string{call_center.cc_address.street_name1};
      if (call_center.cc_address.street_name2 != nullptr) {
        street_name += pmr_string{" "} + call_center.cc_address.street_name2;
      }

      call_center_builder.append_row(
          convert_key(call_center.cc_call_center_sk), call_center.cc_call_center_id,
          resolve_date_id(call_center.cc_rec_start_date_id), resolve_date_id(call_center.cc_rec_end_date_id),
          convert_key(call_center.cc_closed_date_id), convert_key(call_center.cc_open_date_id), call_center.cc_name,
          call_center.cc_class, call_center.cc_employees, call_center.cc_sq_ft, call_center.cc_hours,
          call_center.cc_manager, call_center.cc_market_id, call_center.cc_market_class, call_center.cc_market_desc,
          call_center.cc_market_manager, call_center.cc_division_id, call_center.cc_division_name,
          call_center.cc_company, call_center.cc_company_name, call_center.cc_address.street_num,
          std::move(street_name), call_center.cc_address.street_type, call_center.cc_address.suite_num,
          call_center.cc_address.city, call_center.cc_address.county, call_center.cc_address.state,
          call_center.cc_address.zip, call_center.cc_address.country, call_center.cc_address.gmt_offset,
          decimal_to_float(call_center.cc_tax_percentage));
    }

    table_info_by_name["call_center"].table = call_center_builder.finish_table();
    std::cout << "call_center table generated" << std::endl;
  }

  // catalog page
  {
    const auto [catalog_page_first, catalog_page_count] = prepare_for_table(CATALOG_PAGE);

    auto catalog_page_builder =
        TableBuilder{_benchmark_config->chunk_size, catalog_page_column_types, catalog_page_column_names, 
                     static_cast<size_t>(catalog_page_count)};

    auto catalog_page = CATALOG_PAGE_TBL{};
    for (auto i = ds_key_t{0}; i < catalog_page_count; i++) {
      // need a pointer to the previous result of mk_w_catalog_page, because cp_department is only set once
      mk_w_catalog_page(&catalog_page, catalog_page_first + i);
      tpcds_row_stop(CATALOG_PAGE);

      catalog_page_builder.append_row(convert_key(catalog_page.cp_catalog_page_sk), catalog_page.cp_catalog_page_id,
                                      convert_key(catalog_page.cp_start_date_id),
                                      convert_key(catalog_page.cp_end_date_id), catalog_page.cp_department,
                                      catalog_page.cp_catalog_number, catalog_page.cp_catalog_page_number,
                                      catalog_page.cp_description, catalog_page.cp_type);
    }

    table_info_by_name["catalog_page"].table = catalog_page_builder.finish_table();
    std::cout << "catalog_page table generated" << std::endl;
  }

  // catalog returns
  {
    DebugAssert(prepare_for_table(CATALOG_RETURNS).second <= 0,
                "catalog returns are only created together with catalog sales");
  }

  // catalog sales
  {
    const auto [catalog_sales_first, catalog_sales_count] = prepare_for_table(CATALOG_SALES);

    auto catalog_sales_builder =
        TableBuilder{_benchmark_config->chunk_size, catalog_sales_column_types, catalog_sales_column_names,
                      static_cast<size_t>(catalog_sales_count)};

    auto catalog_returns_builder = TableBuilder{_benchmark_config->chunk_size, catalog_returns_column_types,
                                                catalog_returns_column_names};

    for (auto i = ds_key_t{0}; i < catalog_sales_count; i++) {
      auto catalog_sales = W_CATALOG_SALES_TBL{};
      auto catalog_returns = W_CATALOG_RETURNS_TBL{};
      int was_returned = 0;

      mk_w_catalog_sales(&catalog_sales, catalog_sales_first + i, &catalog_returns, &was_returned);
      tpcds_row_stop(CATALOG_SALES);

      catalog_sales_builder.append_row(
          convert_key(catalog_sales.cs_sold_date_sk), convert_key(catalog_sales.cs_sold_time_sk),
          convert_key(catalog_sales.cs_ship_date_sk), convert_key(catalog_sales.cs_bill_customer_sk),
          convert_key(catalog_sales.cs_bill_cdemo_sk), convert_key(catalog_sales.cs_bill_hdemo_sk),
          convert_key(catalog_sales.cs_bill_addr_sk), convert_key(catalog_sales.cs_ship_customer_sk),
          convert_key(catalog_sales.cs_ship_cdemo_sk), convert_key(catalog_sales.cs_ship_hdemo_sk),
          convert_key(catalog_sales.cs_ship_addr_sk), convert_key(catalog_sales.cs_call_center_sk),
          convert_key(catalog_sales.cs_catalog_page_sk), convert_key(catalog_sales.cs_ship_mode_sk),
          convert_key(catalog_sales.cs_warehouse_sk), convert_key(catalog_sales.cs_sold_item_sk),
          convert_key(catalog_sales.cs_promo_sk), convert_key(catalog_sales.cs_order_number),
          catalog_sales.cs_pricing.quantity, decimal_to_float(catalog_sales.cs_pricing.wholesale_cost),
          decimal_to_float(catalog_sales.cs_pricing.list_price), decimal_to_float(catalog_sales.cs_pricing.sales_price),
          decimal_to_float(catalog_sales.cs_pricing.ext_discount_amt),
          decimal_to_float(catalog_sales.cs_pricing.ext_sales_price),
          decimal_to_float(catalog_sales.cs_pricing.ext_wholesale_cost),
          decimal_to_float(catalog_sales.cs_pricing.ext_list_price), decimal_to_float(catalog_sales.cs_pricing.ext_tax),
          decimal_to_float(catalog_sales.cs_pricing.coupon_amt),
          decimal_to_float(catalog_sales.cs_pricing.ext_ship_cost), decimal_to_float(catalog_sales.cs_pricing.net_paid),
          decimal_to_float(catalog_sales.cs_pricing.net_paid_inc_tax),
          decimal_to_float(catalog_sales.cs_pricing.net_paid_inc_ship),
          decimal_to_float(catalog_sales.cs_pricing.net_paid_inc_ship_tax),
          decimal_to_float(catalog_sales.cs_pricing.net_profit));

      if (was_returned != 0) {
        catalog_returns_builder.append_row(
            convert_key(catalog_returns.cr_returned_date_sk), convert_key(catalog_returns.cr_returned_time_sk),
            convert_key(catalog_returns.cr_item_sk), convert_key(catalog_returns.cr_refunded_customer_sk),
            convert_key(catalog_returns.cr_refunded_cdemo_sk), convert_key(catalog_returns.cr_refunded_hdemo_sk),
            convert_key(catalog_returns.cr_refunded_addr_sk), convert_key(catalog_returns.cr_returning_customer_sk),
            convert_key(catalog_returns.cr_returning_cdemo_sk), convert_key(catalog_returns.cr_returning_hdemo_sk),
            convert_key(catalog_returns.cr_returning_addr_sk), convert_key(catalog_returns.cr_call_center_sk),
            convert_key(catalog_returns.cr_catalog_page_sk), convert_key(catalog_returns.cr_ship_mode_sk),
            convert_key(catalog_returns.cr_warehouse_sk), convert_key(catalog_returns.cr_reason_sk),
            convert_key(catalog_returns.cr_order_number), catalog_returns.cr_pricing.quantity,
            decimal_to_float(catalog_returns.cr_pricing.net_paid), decimal_to_float(catalog_returns.cr_pricing.ext_tax),
            decimal_to_float(catalog_returns.cr_pricing.net_paid_inc_tax),
            decimal_to_float(catalog_returns.cr_pricing.fee),
            decimal_to_float(catalog_returns.cr_pricing.ext_ship_cost),
            decimal_to_float(catalog_returns.cr_pricing.refunded_cash),
            decimal_to_float(catalog_returns.cr_pricing.reversed_charge),
            decimal_to_float(catalog_returns.cr_pricing.store_credit),
            decimal_to_float(catalog_returns.cr_pricing.net_loss));
      }
    }

    table_info_by_name["catalog_sales"].table = catalog_sales_builder.finish_table();
    std::cout << "catalog_sales table generated" << std::endl;

    table_info_by_name["catalog_returns"].table = catalog_returns_builder.finish_table();
    std::cout << "catalog_returns table generated" << std::endl;
  }

  // customer
  {
    const auto [customer_first, customer_count] = prepare_for_table(CUSTOMER);

    auto customer_builder = TableBuilder{_benchmark_config->chunk_size, customer_column_types, customer_column_names,
                                          static_cast<size_t>(customer_count)};

    for (auto i = ds_key_t{0}; i < customer_count; i++) {
      const auto customer = call_dbgen_mk<W_CUSTOMER_TBL, &mk_w_customer, CUSTOMER>(customer_first + i);

      customer_builder.append_row(
          convert_key(customer.c_customer_sk), customer.c_customer_id, convert_key(customer.c_current_cdemo_sk),
          convert_key(customer.c_current_hdemo_sk), convert_key(customer.c_current_addr_sk),
          customer.c_first_shipto_date_id, customer.c_first_sales_date_id, customer.c_salutation, customer.c_first_name,
          customer.c_last_name, convert_boolean(customer.c_preferred_cust_flag), customer.c_birth_day,
          customer.c_birth_month, customer.c_birth_year, customer.c_birth_country, customer.c_login,
          customer.c_email_address, customer.c_last_review_date);
    }

    table_info_by_name["customer"].table = customer_builder.finish_table();
    std::cout << "customer table generated" << std::endl;
  }

  // customer address
  {
    const auto [customer_address_first, customer_address_count] = prepare_for_table(CUSTOMER_ADDRESS);

    auto customer_address_builder =
        TableBuilder{_benchmark_config->chunk_size, customer_address_column_types, customer_address_column_names,
                      static_cast<size_t>(customer_address_count)};

    for (auto i = ds_key_t{0}; i < customer_address_count; i++) {
      const auto customer_address =
          call_dbgen_mk<W_CUSTOMER_ADDRESS_TBL, &mk_w_customer_address, CUSTOMER_ADDRESS>(customer_address_first + i);

      auto street_name = pmr_string{customer_address.ca_address.street_name1};
      if (customer_address.ca_address.street_name2 != nullptr) {
        street_name += pmr_string{" "} + customer_address.ca_address.street_name2;
      }

      customer_address_builder.append_row(
          convert_key(customer_address.ca_addr_sk), customer_address.ca_addr_id, customer_address.ca_address.street_num,
          std::move(street_name), customer_address.ca_address.street_type, customer_address.ca_address.suite_num,
          customer_address.ca_address.city, customer_address.ca_address.county, customer_address.ca_address.state,
          customer_address.ca_address.zip, customer_address.ca_address.country, customer_address.ca_address.gmt_offset,
          customer_address.ca_location_type);
    }

    table_info_by_name["customer_address"].table = customer_address_builder.finish_table();
    std::cout << "customer_address table generated" << std::endl;
  }

  // customer demographics
  {
    const auto [customer_demographics_first, customer_demographics_count] = prepare_for_table(CUSTOMER_DEMOGRAPHICS);

    auto customer_demographics_builder = TableBuilder{_benchmark_config->chunk_size, customer_demographics_column_types,
                                                      customer_demographics_column_names, 
                                                      static_cast<size_t>(customer_demographics_count)};

    for (auto i = ds_key_t{0}; i < customer_demographics_count; i++) {
      const auto customer_demographics =
          call_dbgen_mk<W_CUSTOMER_DEMOGRAPHICS_TBL, &mk_w_customer_demographics, CUSTOMER_DEMOGRAPHICS>(
              customer_demographics_first + i);

      customer_demographics_builder.append_row(
          convert_key(customer_demographics.cd_demo_sk), customer_demographics.cd_gender,
          customer_demographics.cd_marital_status, customer_demographics.cd_education_status,
          customer_demographics.cd_purchase_estimate, customer_demographics.cd_credit_rating,
          customer_demographics.cd_dep_count, customer_demographics.cd_dep_employed_count,
          customer_demographics.cd_dep_college_count);
    }

    table_info_by_name["customer_demographics"].table = customer_demographics_builder.finish_table();
    std::cout << "customer_demographics table generated" << std::endl;
  }

  // date
  {
    const auto [date_first, date_count] = prepare_for_table(DATE);

    auto date_builder = TableBuilder{_benchmark_config->chunk_size, date_column_types, date_column_names, 
                                     static_cast<size_t>(date_count)};

    for (auto i = ds_key_t{0}; i < date_count; i++) {
      const auto date = call_dbgen_mk<W_DATE_TBL, &mk_w_date, DATE>(date_first + i);

      auto quarter_name = pmr_string{std::to_string(date.d_year)};
      quarter_name += "Q" + std::to_string(date.d_qoy);

      date_builder.append_row(convert_key(date.d_date_sk), date.d_date_id, resolve_date_id(date.d_date_sk),
                              date.d_month_seq, date.d_week_seq, date.d_quarter_seq, date.d_year, date.d_dow,
                              date.d_moy, date.d_dom, date.d_qoy, date.d_fy_year, date.d_fy_quarter_seq,
                              date.d_fy_week_seq, date.d_day_name, std::move(quarter_name),
                              convert_boolean(date.d_holiday), convert_boolean(date.d_weekend),
                              convert_boolean(date.d_following_holiday), date.d_first_dom, date.d_last_dom,
                              date.d_same_day_ly, date.d_same_day_lq, convert_boolean(date.d_current_day),
                              convert_boolean(date.d_current_week), convert_boolean(date.d_current_month),
                              convert_boolean(date.d_current_quarter), convert_boolean(date.d_current_year));
    }

    table_info_by_name["date"].table = date_builder.finish_table();
    std::cout << "date table generated" << std::endl;
  }

  // household demographics
  {
    const auto [household_demographics_first, household_demographics_count] = prepare_for_table(HOUSEHOLD_DEMOGRAPHICS);

    auto household_demographics_builder = TableBuilder{
        _benchmark_config->chunk_size, household_demographics_column_types, household_demographics_column_names,
         static_cast<size_t>(household_demographics_count)};

    for (auto i = ds_key_t{0}; i < household_demographics_count; i++) {
      const auto household_demographics =
          call_dbgen_mk<W_HOUSEHOLD_DEMOGRAPHICS_TBL, &mk_w_household_demographics, HOUSEHOLD_DEMOGRAPHICS>(
              household_demographics_first + i);

      household_demographics_builder.append_row(
          convert_key(household_demographics.hd_demo_sk), convert_key(household_demographics.hd_income_band_id),
          household_demographics.hd_buy_potential, household_demographics.hd_dep_count,
          household_demographics.hd_vehicle_count);
    }

    table_info_by_name["household_demographics"].table = household_demographics_builder.finish_table();
    std::cout << "household_demographics table generated" << std::endl;
  }

  // income band
  {
    const auto [income_band_first, income_band_count] = prepare_for_table(INCOME_BAND);

    auto income_band_builder =
        TableBuilder{_benchmark_config->chunk_size, income_band_column_types, income_band_column_names, 
                     static_cast<size_t>(income_band_count)};

    for (auto i = ds_key_t{0}; i < income_band_count; i++) {
      const auto income_band = call_dbgen_mk<W_INCOME_BAND_TBL, &mk_w_income_band, INCOME_BAND>(income_band_first + i);

      income_band_builder.append_row(income_band.ib_income_band_id, income_band.ib_lower_bound,
                                     income_band.ib_upper_bound);
    }

    table_info_by_name["income_band"].table = income_band_builder.finish_table();
    std::cout << "income_band table generated" << std::endl;
  }

  // inventory
  {
    const auto [inventory_first, inventory_count] = prepare_for_table(INVENTORY);

    auto inventory_builder = TableBuilder{_benchmark_config->chunk_size, inventory_column_types, inventory_column_names,
                                           static_cast<size_t>(inventory_count)};

    for (auto i = ds_key_t{0}; i < inventory_count; i++) {
      const auto inventory = call_dbgen_mk<W_INVENTORY_TBL, &mk_w_inventory, INVENTORY>(inventory_first + i);

      inventory_builder.append_row(convert_key(inventory.inv_date_sk), convert_key(inventory.inv_item_sk),
                                   convert_key(inventory.inv_warehouse_sk), inventory.inv_quantity_on_hand);
    }

    table_info_by_name["inventory"].table = inventory_builder.finish_table();
    std::cout << "inventory table generated" << std::endl;
  }

  // item
  {
    const auto [item_first, item_count] = prepare_for_table(ITEM);

    auto item_builder = TableBuilder{_benchmark_config->chunk_size, item_column_types, item_column_names, 
                                     static_cast<size_t>(item_count)};

    for (auto i = ds_key_t{0}; i < item_count; i++) {
      const auto item = call_dbgen_mk<W_ITEM_TBL, &mk_w_item, ITEM>(item_first + i);

      item_builder.append_row(convert_key(item.i_item_sk), item.i_item_id, resolve_date_id(item.i_rec_start_date_id),
                              resolve_date_id(item.i_rec_end_date_id), item.i_item_desc,
                              decimal_to_float(item.i_current_price), decimal_to_float(item.i_wholesale_cost),
                              convert_key(item.i_brand_id), item.i_brand, convert_key(item.i_class_id), item.i_class,
                              convert_key(item.i_category_id), item.i_category, convert_key(item.i_manufact_id),
                              item.i_manufact, item.i_size, item.i_formulation, item.i_color, item.i_units,
                              item.i_container, convert_key(item.i_manager_id), item.i_product_name);
    }

    table_info_by_name["item"].table = item_builder.finish_table();
    std::cout << "item table generated" << std::endl;
  }

  // promotion
  {
    const auto [promotion_first, promotion_count] = prepare_for_table(PROMOTION);

    auto promotion_builder = TableBuilder{_benchmark_config->chunk_size, promotion_column_types, promotion_column_names,
                                           static_cast<size_t>(promotion_count)};

    for (auto i = ds_key_t{0}; i < promotion_count; i++) {
      const auto promotion = call_dbgen_mk<W_PROMOTION_TBL, &mk_w_promotion, PROMOTION>(promotion_first + i);

      promotion_builder.append_row(
          convert_key(promotion.p_promo_sk), promotion.p_promo_id, convert_key(promotion.p_start_date_id),
          convert_key(promotion.p_end_date_id), convert_key(promotion.p_item_sk), decimal_to_float(promotion.p_cost),
          promotion.p_response_target, promotion.p_promo_name, convert_boolean(promotion.p_channel_dmail),
          convert_boolean(promotion.p_channel_email), convert_boolean(promotion.p_channel_catalog),
          convert_boolean(promotion.p_channel_tv), convert_boolean(promotion.p_channel_radio),
          convert_boolean(promotion.p_channel_press), convert_boolean(promotion.p_channel_event),
          convert_boolean(promotion.p_channel_demo), promotion.p_channel_details, promotion.p_purpose,
          convert_boolean(promotion.p_discount_active));
    }

    table_info_by_name["promotion"].table = promotion_builder.finish_table();
    std::cout << "promotion table generated" << std::endl;
  }

  // reason
  {
    const auto [reason_first, reason_count] = prepare_for_table(REASON);

    auto reason_builder = TableBuilder{_benchmark_config->chunk_size, reason_column_types, reason_column_names,
                                        static_cast<size_t>(reason_count)};

    for (auto i = ds_key_t{0}; i < reason_count; i++) {
      const auto reason = call_dbgen_mk<W_REASON_TBL, &mk_w_reason, REASON>(reason_first + i);

      reason_builder.append_row(convert_key(reason.r_reason_sk), reason.r_reason_id, reason.r_reason_description);
    }

    table_info_by_name["reason"].table = reason_builder.finish_table();
    std::cout << "reason table generated" << std::endl;
  }

  // ship mode
  {
    const auto [ship_mode_first, ship_mode_count] = prepare_for_table(SHIP_MODE);

    auto ship_mode_builder = TableBuilder{_benchmark_config->chunk_size, ship_mode_column_types, ship_mode_column_names,
                                           static_cast<size_t>(ship_mode_count)};

    for (auto i = ds_key_t{0}; i < ship_mode_count; i++) {
      const auto ship_mode = call_dbgen_mk<W_SHIP_MODE_TBL, &mk_w_ship_mode, SHIP_MODE>(ship_mode_first + i);

      ship_mode_builder.append_row(convert_key(ship_mode.sm_ship_mode_sk), ship_mode.sm_ship_mode_id, ship_mode.sm_type,
                                   ship_mode.sm_code, ship_mode.sm_carrier, ship_mode.sm_contract);
    }

    table_info_by_name["ship_mode"].table = ship_mode_builder.finish_table();
    std::cout << "ship_mode table generated" << std::endl;
  }

  // store
  {
    const auto [store_first, store_count] = prepare_for_table(STORE);

    auto store_builder = TableBuilder{_benchmark_config->chunk_size, store_column_types, store_column_names,
                                       static_cast<size_t>(store_count)};

    for (auto i = ds_key_t{0}; i < store_count; i++) {
      const auto store = call_dbgen_mk<W_STORE_TBL, &mk_w_store, STORE>(store_first + i);

      auto street_name = pmr_string{store.address.street_name1};
      if (store.address.street_name2 != nullptr) {
        street_name += pmr_string{" "} + store.address.street_name2;
      }

      store_builder.append_row(convert_key(store.store_sk), store.store_id, resolve_date_id(store.rec_start_date_id),
                               resolve_date_id(store.rec_end_date_id), convert_key(store.closed_date_id),
                               store.store_name, store.employees, store.floor_space, store.hours, store.store_manager,
                               store.market_id, store.geography_class, store.market_desc, store.market_manager,
                               convert_key(store.division_id), store.division_name, convert_key(store.company_id),
                               store.company_name, store.address.street_num, std::move(street_name),
                               store.address.street_type, store.address.suite_num, store.address.city,
                               store.address.county, store.address.state, store.address.zip, store.address.country,
                               store.address.gmt_offset, decimal_to_float(store.dTaxPercentage));
    }

    table_info_by_name["store"].table = store_builder.finish_table();
    std::cout << "store table generated" << std::endl;
  }

  // store returns
  {
    DebugAssert(prepare_for_table(STORE_RETURNS).second <= 0,
                "store returns are only created together with store sales");
  }

  // store sales
  {
    const auto [store_sales_first, store_sales_count] = prepare_for_table(STORE_SALES);

    auto store_sales_builder =
        TableBuilder{_benchmark_config->chunk_size, store_sales_column_types, store_sales_column_names, 
                     static_cast<size_t>(store_sales_count)};
    auto store_returns_builder = TableBuilder{_benchmark_config->chunk_size, store_returns_column_types,
                                              store_returns_column_names};

    for (auto i = ds_key_t{0}; i < store_sales_count; i++) {
      auto store_sales = W_STORE_SALES_TBL{};
      auto store_returns = W_STORE_RETURNS_TBL{};
      int was_returned = 0;

      mk_w_store_sales(&store_sales, store_sales_first + i, &store_returns, &was_returned);
      tpcds_row_stop(STORE_SALES);

      store_sales_builder.append_row(
          convert_key(store_sales.ss_sold_date_sk), convert_key(store_sales.ss_sold_time_sk),
          convert_key(store_sales.ss_sold_item_sk), convert_key(store_sales.ss_sold_customer_sk),
          convert_key(store_sales.ss_sold_cdemo_sk), convert_key(store_sales.ss_sold_hdemo_sk),
          convert_key(store_sales.ss_sold_addr_sk), convert_key(store_sales.ss_sold_store_sk),
          convert_key(store_sales.ss_sold_promo_sk), convert_key(store_sales.ss_ticket_number),
          store_sales.ss_pricing.quantity, decimal_to_float(store_sales.ss_pricing.wholesale_cost),
          decimal_to_float(store_sales.ss_pricing.list_price), decimal_to_float(store_sales.ss_pricing.sales_price),
          decimal_to_float(store_sales.ss_pricing.coupon_amt), decimal_to_float(store_sales.ss_pricing.ext_sales_price),
          decimal_to_float(store_sales.ss_pricing.ext_wholesale_cost),
          decimal_to_float(store_sales.ss_pricing.ext_list_price), decimal_to_float(store_sales.ss_pricing.ext_tax),
          decimal_to_float(store_sales.ss_pricing.coupon_amt), decimal_to_float(store_sales.ss_pricing.net_paid),
          decimal_to_float(store_sales.ss_pricing.net_paid_inc_tax),
          decimal_to_float(store_sales.ss_pricing.net_profit));

      if (was_returned != 0) {
        store_returns_builder.append_row(
            convert_key(store_returns.sr_returned_date_sk), convert_key(store_returns.sr_returned_time_sk),
            convert_key(store_returns.sr_item_sk), convert_key(store_returns.sr_customer_sk),
            convert_key(store_returns.sr_cdemo_sk), convert_key(store_returns.sr_hdemo_sk),
            convert_key(store_returns.sr_addr_sk), convert_key(store_returns.sr_store_sk),
            convert_key(store_returns.sr_reason_sk), convert_key(store_returns.sr_ticket_number),
            store_returns.sr_pricing.quantity, decimal_to_float(store_returns.sr_pricing.net_paid),
            decimal_to_float(store_returns.sr_pricing.ext_tax),
            decimal_to_float(store_returns.sr_pricing.net_paid_inc_tax), decimal_to_float(store_returns.sr_pricing.fee),
            decimal_to_float(store_returns.sr_pricing.ext_ship_cost),
            decimal_to_float(store_returns.sr_pricing.refunded_cash),
            decimal_to_float(store_returns.sr_pricing.reversed_charge),
            decimal_to_float(store_returns.sr_pricing.store_credit),
            decimal_to_float(store_returns.sr_pricing.net_loss));
      }
    }

    table_info_by_name["store_sales"].table = store_sales_builder.finish_table();
    std::cout << "store_sales table generated" << std::endl;

    table_info_by_name["store_returns"].table = store_returns_builder.finish_table();
    std::cout << "store_returns table generated" << std::endl;
  }

  // time
  {
    const auto [time_first, time_count] = prepare_for_table(TIME);

    auto time_builder = TableBuilder{_benchmark_config->chunk_size, time_column_types, time_column_names, 
                                     static_cast<size_t>(time_count)};

    for (auto i = ds_key_t{0}; i < time_count; i++) {
      const auto time = call_dbgen_mk<W_TIME_TBL, &mk_w_time, TIME>(time_first + i);

      time_builder.append_row(convert_key(time.t_time_sk), time.t_time_id, time.t_time, time.t_hour, time.t_minute,
                              time.t_second, time.t_am_pm, time.t_shift, time.t_sub_shift, time.t_meal_time);
    }

    table_info_by_name["time"].table = time_builder.finish_table();
    std::cout << "time table generated" << std::endl;
  }

  // warehouse
  {
    const auto [warehouse_first, warehouse_count] = prepare_for_table(WAREHOUSE);

    auto warehouse_builder = TableBuilder{_benchmark_config->chunk_size, warehouse_column_types, warehouse_column_names,
                                           static_cast<size_t>(warehouse_count)};

    for (auto i = ds_key_t{0}; i < warehouse_count; i++) {
      const auto warehouse = call_dbgen_mk<W_WAREHOUSE_TBL, &mk_w_warehouse, WAREHOUSE>(warehouse_first + i);

      auto street_name = pmr_string{warehouse.w_address.street_name1};
      if (warehouse.w_address.street_name2 != nullptr) {
        street_name += pmr_string{" "} + warehouse.w_address.street_name2;
      }

      warehouse_builder.append_row(
          convert_key(warehouse.w_warehouse_sk), warehouse.w_warehouse_id, warehouse.w_warehouse_name,
          warehouse.w_warehouse_sq_ft, warehouse.w_address.street_num, std::move(street_name),
          warehouse.w_address.street_type, warehouse.w_address.suite_num, warehouse.w_address.city,
          warehouse.w_address.county, warehouse.w_address.state, warehouse.w_address.zip, warehouse.w_address.country,
          warehouse.w_address.gmt_offset);
    }

    table_info_by_name["warehouse"].table = warehouse_builder.finish_table();
    std::cout << "warehouse table generated" << std::endl;
  }

  // web page
  {
    const auto [web_page_first, web_page_count] = prepare_for_table(WEB_PAGE);

    auto web_page_builder = TableBuilder{_benchmark_config->chunk_size, web_page_column_types, web_page_column_names,
                                          static_cast<size_t>(web_page_count)};

    for (auto i = ds_key_t{0}; i < web_page_count; i++) {
      const auto web_page = call_dbgen_mk<W_WEB_PAGE_TBL, &mk_w_web_page, WEB_PAGE>(web_page_first + i);

      web_page_builder.append_row(
          convert_key(web_page.wp_page_sk), web_page.wp_page_id, resolve_date_id(web_page.wp_rec_start_date_id),
          resolve_date_id(web_page.wp_rec_end_date_id), convert_key(web_page.wp_creation_date_sk),
          convert_key(web_page.wp_access_date_sk), convert_boolean(web_page.wp_autogen_flag),
          convert_key(web_page.wp_customer_sk), web_page.wp_url, web_page.wp_type, web_page.wp_char_count,
          web_page.wp_link_count, web_page.wp_image_count, web_page.wp_max_ad_count);
    }

    table_info_by_name["web_page"].table = web_page_builder.finish_table();
    std::cout << "web_page table generated" << std::endl;
  }

  // web returns
  { DebugAssert(prepare_for_table(WEB_RETURNS).second <= 0, "web returns are only created together with web sales"); }

  // web sales
  {
    const auto [web_sales_first, web_sales_count] = prepare_for_table(WEB_SALES);

    auto web_sales_builder = TableBuilder{_benchmark_config->chunk_size, web_sales_column_types, web_sales_column_names,
                                           static_cast<size_t>(web_sales_count)};
    auto web_returns_builder =
        TableBuilder{_benchmark_config->chunk_size, web_returns_column_types, web_returns_column_names};

    for (auto i = ds_key_t{0}; i < web_sales_count; i++) {
      auto web_sales = W_WEB_SALES_TBL{};
      auto web_returns = W_WEB_RETURNS_TBL{};
      int was_returned = 0;

      mk_w_web_sales(&web_sales, web_sales_first + i, &web_returns, &was_returned);
      tpcds_row_stop(WEB_SALES);

      web_sales_builder.append_row(
          convert_key(web_sales.ws_sold_date_sk), convert_key(web_sales.ws_sold_time_sk),
          convert_key(web_sales.ws_ship_date_sk), convert_key(web_sales.ws_item_sk),
          convert_key(web_sales.ws_bill_customer_sk), convert_key(web_sales.ws_bill_cdemo_sk),
          convert_key(web_sales.ws_bill_hdemo_sk), convert_key(web_sales.ws_bill_addr_sk),
          convert_key(web_sales.ws_ship_customer_sk), convert_key(web_sales.ws_ship_cdemo_sk),
          convert_key(web_sales.ws_ship_hdemo_sk), convert_key(web_sales.ws_ship_addr_sk),
          convert_key(web_sales.ws_web_page_sk), convert_key(web_sales.ws_web_site_sk),
          convert_key(web_sales.ws_ship_mode_sk), convert_key(web_sales.ws_warehouse_sk),
          convert_key(web_sales.ws_promo_sk), convert_key(web_sales.ws_order_number), web_sales.ws_pricing.quantity,
          decimal_to_float(web_sales.ws_pricing.wholesale_cost), decimal_to_float(web_sales.ws_pricing.list_price),
          decimal_to_float(web_sales.ws_pricing.sales_price), decimal_to_float(web_sales.ws_pricing.ext_discount_amt),
          decimal_to_float(web_sales.ws_pricing.ext_sales_price),
          decimal_to_float(web_sales.ws_pricing.ext_wholesale_cost),
          decimal_to_float(web_sales.ws_pricing.ext_list_price), decimal_to_float(web_sales.ws_pricing.ext_tax),
          decimal_to_float(web_sales.ws_pricing.coupon_amt), decimal_to_float(web_sales.ws_pricing.ext_ship_cost),
          decimal_to_float(web_sales.ws_pricing.net_paid), decimal_to_float(web_sales.ws_pricing.net_paid_inc_tax),
          decimal_to_float(web_sales.ws_pricing.net_paid_inc_ship),
          decimal_to_float(web_sales.ws_pricing.net_paid_inc_ship_tax),
          decimal_to_float(web_sales.ws_pricing.net_profit));

      if (was_returned != 0) {
        web_returns_builder.append_row(
            convert_key(web_returns.wr_returned_date_sk), convert_key(web_returns.wr_returned_time_sk),
            convert_key(web_returns.wr_item_sk), convert_key(web_returns.wr_refunded_customer_sk),
            convert_key(web_returns.wr_refunded_cdemo_sk), convert_key(web_returns.wr_refunded_hdemo_sk),
            convert_key(web_returns.wr_refunded_addr_sk), convert_key(web_returns.wr_returning_customer_sk),
            convert_key(web_returns.wr_returning_cdemo_sk), convert_key(web_returns.wr_returning_hdemo_sk),
            convert_key(web_returns.wr_returning_addr_sk), convert_key(web_returns.wr_web_page_sk),
            convert_key(web_returns.wr_reason_sk), convert_key(web_returns.wr_order_number),
            web_returns.wr_pricing.quantity, decimal_to_float(web_returns.wr_pricing.net_paid),
            decimal_to_float(web_returns.wr_pricing.ext_tax), decimal_to_float(web_returns.wr_pricing.net_paid_inc_tax),
            decimal_to_float(web_returns.wr_pricing.fee), decimal_to_float(web_returns.wr_pricing.ext_ship_cost),
            decimal_to_float(web_returns.wr_pricing.refunded_cash),
            decimal_to_float(web_returns.wr_pricing.reversed_charge),
            decimal_to_float(web_returns.wr_pricing.store_credit), decimal_to_float(web_returns.wr_pricing.net_loss));
      }
    }

    table_info_by_name["web_sales"].table = web_sales_builder.finish_table();
    std::cout << "web_sales table generated" << std::endl;

    table_info_by_name["web_returns"].table = web_returns_builder.finish_table();
    std::cout << "web_returns table generated" << std::endl;
  }

  // web site
  {
    const auto [web_site_first, web_site_count] = prepare_for_table(WEB_SITE);

    auto web_site_builder = TableBuilder{_benchmark_config->chunk_size, web_site_column_types, web_site_column_names,
                                          static_cast<size_t>(web_site_count)};

    for (auto i = ds_key_t{0}; i < web_site_count; i++) {
      const auto web_site = call_dbgen_mk<W_WEB_SITE_TBL, &mk_w_web_site, WEB_SITE>(web_site_first + i);

      auto street_name = pmr_string{web_site.web_address.street_name1};
      if (web_site.web_address.street_name2 != nullptr) {
        street_name += pmr_string{" "} + web_site.web_address.street_name2;
      }

      web_site_builder.append_row(
          convert_key(web_site.web_site_sk), web_site.web_site_id, resolve_date_id(web_site.web_rec_start_date_id),
          resolve_date_id(web_site.web_rec_end_date_id), web_site.web_name, convert_key(web_site.web_open_date),
          convert_key(web_site.web_close_date), web_site.web_class, web_site.web_manager, web_site.web_market_id,
          web_site.web_market_class, web_site.web_market_desc, web_site.web_market_manager, web_site.web_company_id,
          web_site.web_company_name, web_site.web_address.street_num, std::move(street_name),
          web_site.web_address.street_type, web_site.web_address.suite_num, web_site.web_address.city,
          web_site.web_address.county, web_site.web_address.state, std::move(street_name), web_site.web_address.country,
          web_site.web_address.gmt_offset, decimal_to_float(web_site.web_tax_percentage));
    }

    table_info_by_name["web_site"].table = web_site_builder.finish_table();
    std::cout << "web_site table generated" << std::endl;
  }

  // dbgen version
  {
    const auto [dbgen_version_first, dbgen_version_count] = prepare_for_table(DBGEN_VERSION);

    auto dbgen_version_builder =
        TableBuilder{_benchmark_config->chunk_size, dbgen_version_column_types, dbgen_version_column_names,
                      static_cast<size_t>(dbgen_version_count)};

    for (auto i = ds_key_t{0}; i < dbgen_version_count; i++) {
      auto dbgen_version = call_dbgen_mk<DBGEN_VERSION_TBL, &mk_dbgen_version, DBGEN_VERSION>(dbgen_version_first + i);

      dbgen_version_builder.append_row(dbgen_version.szVersion, dbgen_version.szDate, dbgen_version.szTime,
                                       dbgen_version.szCmdLineArgs);
    }

    table_info_by_name["dbgen_version"].table = dbgen_version_builder.finish_table();
    std::cout << "dbgen_version table generated" << std::endl;
  }

  // TODO(pascal): dbgen cleanup?

  return table_info_by_name;
}

}  // namespace opossum
