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

#include <table_builder.hpp>

namespace {

using namespace opossum;  // NOLINT

void set_rng_seed(int rng_seed) {
  auto rng_seed_string = std::string("RNGSEED");
  auto rng_seed_value_string = std::to_string(rng_seed);
  set_int(rng_seed_string.data(), rng_seed_value_string.data());
  init_rand();
}

// TpcdsRow is the type of a database row, e.g. CALL_CENTER_TBL
template <class TpcdsRow>
TpcdsRow call_dbgen_mk(table_func_t tdef_functions, ds_key_t index, int table_id) {
  auto tpcds_row = TpcdsRow{};
  tdef_functions.builder(&tpcds_row, index);
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

float decimal_to_float(decimal_t& decimal) {
  double result;
  dectof(&result, &decimal);
  return static_cast<float>(result);
}

pmr_string resolve_date_id(ds_key_t date_id) {
  auto date = date_t{};
  jtodt(&date, static_cast<int>(date_id));

  return pmr_string{dttostr(&date)};
}

// clang-format off

// mapping types used by C-library dsdgen as follows:
// ds_key_t -> int64_t
// int -> int32_t
// char* and char[] -> pmr_string
// decimal -> float
// ds_addr_t -> corresponding types for types in struct ds_addr_t, see address.h

// TODO(pascal): change some int64_t to int32_t, align types with names

const auto call_center_column_types = boost::hana::tuple<      int64_t,             pmr_string,          pmr_string,          pmr_string,        int64_t,             int64_t,           pmr_string, pmr_string, int32_t,        int32_t,    pmr_string, pmr_string,   int32_t,     pmr_string,     pmr_string,    pmr_string,          int32_t,       pmr_string,         int32_t,      pmr_string,        int32_t,            pmr_string,       pmr_string,       pmr_string,        pmr_string, pmr_string,  pmr_string, int32_t,    pmr_string,   int32_t,         float>();  // NOLINT
const auto call_center_column_names = boost::hana::make_tuple("cc_call_center_sk", "cc_call_center_id", "cc_rec_start_date", "cc_rec_end_date", "cc_closed_date_sk", "cc_open_date_sk", "cc_name",  "cc_class", "cc_employees", "cc_sq_ft", "cc_hours", "cc_manager", "cc_mkt_id", "cc_mkt_class", "cc_mkt_desc", "cc_market_manager", "cc_division", "cc_division_name", "cc_company", "cc_company_name", "cc_street_number", "cc_street_name", "cc_street_type", "cc_suite_number", "cc_city",  "cc_county", "cc_state", "cc_zip",   "cc_country", "cc_gmt_offset", "cc_tax_percentage"); // NOLINT

const auto catalog_page_column_types = boost::hana::tuple<      int64_t,              pmr_string,           int64_t,            int64_t,          pmr_string,      int32_t,             int32_t,                  pmr_string,       pmr_string>();  // NOLINT
const auto catalog_page_column_names = boost::hana::make_tuple("cp_catalog_page_sk", "cp_catalog_page_id", "cp_start_date_sk", "cp_end_date_sk", "cp_department", "cp_catalog_number", "cp_catalog_page_number", "cp_description", "cp_type"); // NOLINT

const auto catalog_returns_column_types = boost::hana::tuple<      int64_t,               int64_t,               int64_t,      int64_t,                   int64_t,                int64_t,                int64_t,               int64_t,                    int64_t,                 int64_t,                 int64_t,                int64_t,             int64_t,              int64_t,           int64_t,           int64_t,        int64_t,           int32_t,              float,              float,           float,                   float,    float,                 float,              float,                float,             float>();  // NOLINT
const auto catalog_returns_column_names = boost::hana::make_tuple("cr_returned_date_sk", "cr_returned_time_sk", "cr_item_sk", "cr_refunded_customer_sk", "cr_refunded_cdemo_sk", "cr_refunded_hdemo_sk", "cr_refunded_addr_sk", "cr_returning_customer_sk", "cr_returning_cdemo_sk", "cr_returning_hdemo_sk", "cr_returning_addr_sk", "cr_call_center_sk", "cr_catalog_page_sk", "cr_ship_mode_sk", "cr_warehouse_sk", "cr_reason_sk", "cr_order_number", "cr_return_quantity", "cr_return_amount", "cr_return_tax", "cr_return_amt_inc_tax", "cr_fee", "cr_return_ship_cost", "cr_refunded_cash", "cr_reversed_charge", "cr_store_credit", "cr_net_loss"); // NOLINT

const auto catalog_sales_column_types = boost::hana::tuple<      int64_t,           int64_t,           int64_t,           int64_t,               int64_t,            int64_t,            int64_t,           int64_t,               int64_t,            int64_t,            int64_t,           int64_t,             int64_t,              int64_t,           int64_t,           int64_t,      int64_t,       int64_t,           int32_t,       float,               float,           float,            float,                 float,                float,                   float,               float,        float,           float,              float,         float,                 float,                  float,                      float>();  // NOLINT
const auto catalog_sales_column_names = boost::hana::make_tuple("cs_sold_date_sk", "cs_sold_time_sk", "cs_ship_date_sk", "cs_bill_customer_sk", "cs_bill_cdemo_sk", "cs_bill_hdemo_sk", "cs_bill_addr_sk", "cs_ship_customer_sk", "cs_ship_cdemo_sk", "cs_ship_hdemo_sk", "cs_ship_addr_sk", "cs_call_center_sk", "cs_catalog_page_sk", "cs_ship_mode_sk", "cs_warehouse_sk", "cs_item_sk", "cs_promo_sk", "cs_order_number", "cs_quantity", "cs_wholesale_cost", "cs_list_price", "cs_sales_price", "cs_ext_discount_amt", "cs_ext_sales_price", "cs_ext_wholesale_cost", "cs_ext_list_price", "cs_ext_tax", "cs_coupon_amt", "cs_ext_ship_cost", "cs_net_paid", "cs_net_paid_inc_tax", "cs_net_paid_inc_ship", "cs_net_paid_inc_ship_tax", "cs_net_profit"); // NOLINT

const auto customer_column_types = boost::hana::tuple<      int64_t,         pmr_string,      int64_t,              int64_t,              int64_t,             int32_t,                  int32_t,                 pmr_string,     pmr_string,     pmr_string,    pmr_string,              int32_t,       int32_t,         int32_t,        pmr_string,        pmr_string, pmr_string,        int32_t>();  // NOLINT
const auto customer_column_names = boost::hana::make_tuple("c_customer_sk", "c_customer_id", "c_current_cdemo_sk", "c_current_hdemo_sk", "c_current_addr_sk", "c_first_shipto_date_sk", "c_first_sales_date_sk", "c_salutation", "c_first_name", "c_last_name", "c_preferred_cust_flag", "c_birth_day", "c_birth_month", "c_birth_year", "c_birth_country", "c_login",  "c_email_address", "c_last_review_date"); // NOLINT

const auto customer_address_column_types = boost::hana::tuple<      int64_t,         pmr_string,      int32_t,            pmr_string,       pmr_string,       pmr_string,        pmr_string, pmr_string,  pmr_string, int32_t,    pmr_string,   int32_t,         pmr_string>();  // NOLINT
const auto customer_address_column_names = boost::hana::make_tuple("ca_address_sk", "ca_address_id", "ca_street_number", "ca_street_name", "ca_street_type", "ca_suite_number", "ca_city",  "ca_county", "ca_state", "ca_zip",   "ca_country", "ca_gmt_offset", "ca_location_type"); // NOLINT

const auto customer_demographics_column_types = boost::hana::tuple<      int64_t,      pmr_string,  pmr_string,          pmr_string,            int32_t,                pmr_string,         int32_t,        int32_t,                 int32_t>();  // NOLINT
const auto customer_demographics_column_names = boost::hana::make_tuple("cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status", "cd_purchase_estimate", "cd_credit_rating", "cd_dep_count", "cd_dep_employed_count", "cd_dep_college_count"); // NOLINT

const auto date_column_types = boost::hana::tuple<      int64_t,     pmr_string,  pmr_string, int32_t,       int32_t,      int32_t,         int32_t,  int32_t, int32_t, int32_t, int32_t, int32_t,     int32_t,            int32_t,         pmr_string,   pmr_string,       pmr_string,  pmr_string,  pmr_string,            int32_t,       int32_t,      int32_t,         int32_t,         pmr_string,      pmr_string,       pmr_string,        pmr_string,          pmr_string>();  // NOLINT
const auto date_column_names = boost::hana::make_tuple("d_date_sk", "d_date_id", "d_date",   "d_month_seq", "d_week_seq", "d_quarter_seq", "d_year", "d_dow", "d_moy", "d_dom", "d_qoy", "d_fy_year", "d_fy_quarter_seq", "d_fy_week_seq", "d_day_name", "d_quarter_name", "d_holiday", "d_weekend", "d_following_holiday", "d_first_dom", "d_last_dom", "d_same_day_ly", "d_same_day_lq", "d_current_day", "d_current_week", "d_current_month", "d_current_quarter", "d_current_year"); // NOLINT

const auto dbgen_version_column_types = boost::hana::tuple<pmr_string, pmr_string, pmr_string, pmr_string>();  // NOLINT
const auto dbgen_version_column_names = boost::hana::make_tuple("dv_version", "dv_create_date", "dv_create_time", "dv_cmdline_args"); // NOLINT

const auto household_demographics_column_types = boost::hana::tuple<int64_t, int64_t, pmr_string, int64_t, int64_t>();  // NOLINT
const auto household_demographics_column_names = boost::hana::make_tuple("hd_demo_sk", "hd_income_band_sk", "hd_buy_potential", "hd_dep_count", "hd_vehicle_count"); // NOLINT

const auto income_band_column_types = boost::hana::tuple<int64_t, int64_t, int64_t>();  // NOLINT
const auto income_band_column_names = boost::hana::make_tuple("ib_income_band_sk", "ib_lower_bound", "ib_upper_bound"); // NOLINT

const auto inventory_column_types = boost::hana::tuple<int64_t, int64_t, int64_t, int64_t>();  // NOLINT
const auto inventory_column_names = boost::hana::make_tuple("inv_date_sk", "inv_item_sk", "inv_warehouse_sk", "inv_quantity_on_hand"); // NOLINT

const auto item_column_types = boost::hana::tuple<int64_t, pmr_string, pmr_string, pmr_string, pmr_string, float, float, int64_t, pmr_string, int64_t, pmr_string, int64_t, pmr_string, int64_t, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, int64_t, pmr_string>();  // NOLINT
const auto item_column_names = boost::hana::make_tuple("i_item_sk", "i_item_id", "i_rec_start_date", "i_rec_end_date", "i_item_desc", "i_current_price", "i_wholesale_cost", "i_brand_id", "i_brand", "i_class_id", "i_class", "i_category_id", "i_category", "i_manufact_id", "i_manufact", "i_size", "i_formulation", "i_color", "i_units", "i_container", "i_manager_id", "i_product_name"); // NOLINT

const auto promotion_column_types = boost::hana::tuple<int64_t, pmr_string, int64_t, int64_t, int64_t, float, int64_t, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string>();  // NOLINT
const auto promotion_column_names = boost::hana::make_tuple("p_promo_sk", "p_promo_id", "p_start_date_sk", "p_end_date_sk", "p_item_sk", "p_cost", "p_response_target", "p_promo_name", "p_channel_dmail", "p_channel_email", "p_channel_catalog", "p_channel_tv", "p_channel_radio", "p_channel_press", "p_channel_event", "p_channel_demo", "p_channel_details", "p_purpose", "p_discount_active"); // NOLINT

const auto reason_column_types = boost::hana::tuple<int64_t, pmr_string, pmr_string>();  // NOLINT
const auto reason_column_names = boost::hana::make_tuple("r_reason_sk", "r_reason_id", "r_reason_desc"); // NOLINT

const auto ship_mode_column_types = boost::hana::tuple<int64_t, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string>();  // NOLINT
const auto ship_mode_column_names = boost::hana::make_tuple("sm_ship_mode_sk", "sm_ship_mode_id", "sm_type", "sm_code", "sm_carrier", "sm_contract"); // NOLINT

const auto store_column_types = boost::hana::tuple<int64_t, pmr_string, pmr_string, pmr_string, int64_t, pmr_string, int64_t, int64_t, pmr_string, pmr_string, int64_t, pmr_string, pmr_string, pmr_string, int64_t, pmr_string, int64_t, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, float, float>();  // NOLINT
const auto store_column_names = boost::hana::make_tuple("s_store_sk", "s_store_id", "s_rec_start_date", "s_rec_end_date", "s_closed_date_sk", "s_store_name", "s_number_employees", "s_floor_space", "s_hours", "s_manager", "s_market_id", "s_geography_class", "s_market_desc", "s_market_manager", "s_division_id", "s_division_name", "s_company_id", "s_company_name", "s_street_number", "s_street_name", "s_street_type", "s_suite_number", "s_city", "s_county", "s_state", "s_zip", "s_country", "s_gmt_offset", "s_tax_precentage"); // NOLINT

const auto store_returns_column_types = boost::hana::tuple<int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, float, float, float, float, float, float, float, float, float>();  // NOLINT
const auto store_returns_column_names = boost::hana::make_tuple("sr_returned_date_sk", "sr_return_time_sk", "sr_item_sk", "sr_customer_sk", "sr_cdemo_sk", "sr_hdemo_sk", "sr_addr_sk", "sr_store_sk", "sr_reason_sk", "sr_ticket_number", "sr_return_quantity", "sr_return_amt", "sr_return_tax", "sr_return_amt_inc_tax", "sr_fee", "sr_return_ship_cost", "sr_refunded_cash", "sr_reversed_charge", "sr_store_credit", "sr_net_loss"); // NOLINT

const auto store_sales_column_types = boost::hana::tuple<int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, float, float, float, float, float, float, float, float, float, float, float, float>();  // NOLINT
const auto store_sales_column_names = boost::hana::make_tuple("ss_sold_date_sk", "ss_sold_time_sk", "ss_item_sk", "ss_customer_sk", "ss_cdemo_sk", "ss_hdemo_sk", "ss_addr_sk", "ss_store_sk", "ss_promo_sk", "ss_ticket_number", "ss_quantity", "ss_wholesale_cost", "ss_list_price", "ss_sales_price", "ss_ext_discount_amt", "ss_ext_sales_price", "ss_ext_wholesale_cost", "ss_ext_list_price", "ss_ext_tax", "ss_coupon_amt", "ss_net_paid", "ss_net_paid_inc_tax", "ss_net_profit"); // NOLINT

const auto time_dim_column_types = boost::hana::tuple<int64_t, pmr_string, int64_t, int64_t, int64_t, int64_t, pmr_string, pmr_string, pmr_string, pmr_string>();  // NOLINT
const auto time_dim_column_names = boost::hana::make_tuple("t_time_sk", "t_time_id", "t_time", "t_hour", "t_minute", "t_second", "t_am_pm", "t_shift", "t_sub_shift", "t_meal_time"); // NOLINT

const auto warehouse_column_types = boost::hana::tuple<int64_t, pmr_string, pmr_string, int64_t, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, float>();  // NOLINT
const auto warehouse_column_names = boost::hana::make_tuple("w_warehouse_sk", "w_warehouse_id", "w_warehouse_name", "w_warehouse_sq_ft", "w_street_number", "w_street_name", "w_street_type", "w_suite_number", "w_city", "w_county", "w_state", "w_zip", "w_country", "w_gmt_offset"); // NOLINT

const auto web_page_column_types = boost::hana::tuple<int64_t, pmr_string, pmr_string, pmr_string, int64_t, int64_t, pmr_string, int64_t, pmr_string, pmr_string, int64_t, int64_t, int64_t, int64_t>();  // NOLINT
const auto web_page_column_names = boost::hana::make_tuple("wp_web_page_sk", "wp_web_page_id", "wp_rec_start_date", "wp_rec_end_date", "wp_creation_date_sk", "wp_access_date_sk", "wp_autogen_flag", "wp_customer_sk", "wp_url", "wp_type", "wp_char_count", "wp_link_count", "wp_image_count", "wp_max_ad_count"); // NOLINT

const auto web_returns_column_types = boost::hana::tuple<int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, float, float, float, float, float, float, float, float, float>();  // NOLINT
const auto web_returns_column_names = boost::hana::make_tuple("wr_returned_date_sk", "wr_returned_time_sk", "wr_item_sk", "wr_refunded_customer_sk", "wr_refunded_cdemo_sk", "wr_refunded_hdemo_sk", "wr_refunded_addr_sk", "wr_returning_customer_sk", "wr_returning_cdemo_sk", "wr_returning_hdemo_sk", "wr_returning_addr_sk", "wr_web_page_sk", "wr_reason_sk", "wr_order_number", "wr_return_quantity", "wr_return_amt", "wr_return_tax", "wr_return_amt_inc_tax", "wr_fee", "wr_return_ship_cost", "wr_refunded_cash", "wr_reversed_charge", "wr_account_credit", "wr_net_loss"); // NOLINT

const auto web_sales_column_types = boost::hana::tuple<int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, float, float, float, float, float, float, float, float, float, float, float, float, float, float, float>();  // NOLINT
const auto web_sales_column_names = boost::hana::make_tuple("ws_sold_date_sk", "ws_sold_time_sk", "ws_ship_date_sk", "ws_item_sk", "ws_bill_customer_sk", "ws_bill_cdemo_sk", "ws_bill_hdemo_sk", "ws_bill_addr_sk", "ws_ship_customer_sk", "ws_ship_cdemo_sk", "ws_ship_hdemo_sk", "ws_ship_addr_sk", "ws_web_page_sk", "ws_web_site_sk", "ws_ship_mode_sk", "ws_warehouse_sk", "ws_promo_sk", "ws_order_number", "ws_quantity", "ws_wholesale_cost", "ws_list_price", "ws_sales_price", "ws_ext_discount_amt", "ws_ext_sales_price", "ws_ext_wholesale_cost", "ws_ext_list_price", "ws_ext_tax", "ws_coupon_amt", "ws_ext_ship_cost", "ws_net_paid", "ws_net_paid_inc_tax", "ws_net_paid_inc_ship", "ws_net_paid_inc_ship_tax", "ws_net_profit"); // NOLINT

const auto web_site_column_types = boost::hana::tuple<int64_t, pmr_string, pmr_string, pmr_string, pmr_string, int64_t, int64_t, pmr_string, pmr_string, int64_t, pmr_string, pmr_string, pmr_string, int64_t, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, pmr_string, float, float>();  // NOLINT
const auto web_site_column_names = boost::hana::make_tuple("web_site_sk", "web_site_id", "web_rec_start_date", "web_rec_end_date", "web_name", "web_open_date_sk", "web_close_date_sk", "web_class", "web_manager", "web_mkt_id", "web_mkt_class", "web_mkt_desc", "web_market_manager", "web_company_id", "web_company_name", "web_street_number", "web_street_name", "web_street_type", "web_suite_number", "web_city", "web_county", "web_state", "web_zip", "web_country", "web_gmt_offset", "web_tax_percentage"); // NOLINT

// clang-format on
}  // namespace

namespace opossum {

TpcdsTableGenerator::TpcdsTableGenerator(float scale_factor, uint32_t chunk_size)
    : AbstractTableGenerator(create_benchmark_config_with_chunk_size(chunk_size)), _scale_factor(scale_factor) {}

TpcdsTableGenerator::TpcdsTableGenerator(float scale_factor, const std::shared_ptr<BenchmarkConfig>& benchmark_config)
    : AbstractTableGenerator(benchmark_config), _scale_factor(scale_factor) {}

std::unordered_map<std::string, BenchmarkTableInfo> TpcdsTableGenerator::generate() {
  set_rng_seed(0);
  auto table_info_by_name = std::unordered_map<std::string, BenchmarkTableInfo>();

  // call center
  {
    const auto [call_center_first, call_center_count] = prepare_for_table(CALL_CENTER);

    auto call_center_builder =
        TableBuilder{_benchmark_config->chunk_size, call_center_column_types, call_center_column_names, UseMvcc::Yes,
                     static_cast<size_t>(call_center_count)};

    for (auto i = ds_key_t{0}; i < call_center_count; i++) {
      const auto call_center_functions = getTdefFunctionsByNumber(CALL_CENTER);
      auto call_center = call_dbgen_mk<CALL_CENTER_TBL>(*call_center_functions, call_center_first + i, CALL_CENTER);

      if (call_center.cc_address.street_name1 == nullptr) {
        // TODO(pascal): this should never happen...
        mk_address(&call_center.cc_address, CC_ADDRESS);
      }

      auto street_name = pmr_string{call_center.cc_address.street_name1};
      if (call_center.cc_address.street_name2 != nullptr) {
        street_name += pmr_string{" "} + call_center.cc_address.street_name2;
      }

      // TODO(pascal): handle ints properly (move vs. int{} vs. static_cast)
      call_center_builder.append_row(
          int64_t{call_center.cc_call_center_sk}, call_center.cc_call_center_id,
          resolve_date_id(call_center.cc_rec_start_date_id), resolve_date_id(call_center.cc_rec_end_date_id),
          int64_t{call_center.cc_closed_date_id}, int64_t{call_center.cc_open_date_id}, call_center.cc_name,
          call_center.cc_class, int32_t{call_center.cc_employees}, int32_t{call_center.cc_sq_ft}, call_center.cc_hours,
          call_center.cc_manager, int32_t{call_center.cc_market_id}, call_center.cc_market_class,
          call_center.cc_market_desc, call_center.cc_market_manager, int32_t{call_center.cc_division_id},
          call_center.cc_division_name, int32_t{call_center.cc_company}, call_center.cc_company_name,
          int32_t{call_center.cc_address.street_num}, std::move(street_name), call_center.cc_address.street_type,
          call_center.cc_address.suite_num, call_center.cc_address.city, call_center.cc_address.county,
          call_center.cc_address.state, int32_t{call_center.cc_address.zip}, call_center.cc_address.country,
          int32_t{call_center.cc_address.gmt_offset}, decimal_to_float(call_center.cc_tax_percentage));
    }

    table_info_by_name["call_center"].table = call_center_builder.finish_table();
  }

  // catalog page
  {
    const auto [catalog_page_first, catalog_page_count] = prepare_for_table(CATALOG_PAGE);

    auto catalog_page_builder =
        TableBuilder{_benchmark_config->chunk_size, catalog_page_column_types, catalog_page_column_names, UseMvcc::Yes,
                     static_cast<size_t>(catalog_page_count)};

    for (auto i = ds_key_t{0}; i < catalog_page_count; i++) {
      const auto catalog_page_functions = getTdefFunctionsByNumber(CATALOG_PAGE);
      auto catalog_page =
          call_dbgen_mk<CATALOG_PAGE_TBL>(*catalog_page_functions, catalog_page_first + i, CATALOG_PAGE);

      catalog_page_builder.append_row(int64_t{catalog_page.cp_catalog_page_sk}, catalog_page.cp_catalog_page_id,
                                      int64_t{catalog_page.cp_start_date_id}, int64_t{catalog_page.cp_end_date_id},
                                      catalog_page.cp_department, int32_t{catalog_page.cp_catalog_number},
                                      int32_t{catalog_page.cp_catalog_page_number}, catalog_page.cp_description,
                                      catalog_page.cp_type);
    }

    table_info_by_name["catalog_page"].table = catalog_page_builder.finish_table();
  }

  // catalog returns
  {
    const auto [catalog_returns_first, catalog_returns_count] = prepare_for_table(CATALOG_RETURNS);

    auto catalog_returns_builder =
        TableBuilder{_benchmark_config->chunk_size, catalog_returns_column_types, catalog_returns_column_names,
                     UseMvcc::Yes, static_cast<size_t>(catalog_returns_count)};

    for (auto i = ds_key_t{0}; i < catalog_returns_count; i++) {
      const auto catalog_returns_functions = getTdefFunctionsByNumber(CATALOG_RETURNS);
      auto catalog_returns =
          call_dbgen_mk<W_CATALOG_RETURNS_TBL>(*catalog_returns_functions, catalog_returns_first + i, CATALOG_RETURNS);

      catalog_returns_builder.append_row(
          int64_t{catalog_returns.cr_returned_date_sk}, int64_t{catalog_returns.cr_returned_time_sk},
          int64_t{catalog_returns.cr_item_sk}, int64_t{catalog_returns.cr_refunded_customer_sk},
          int64_t{catalog_returns.cr_refunded_cdemo_sk}, int64_t{catalog_returns.cr_refunded_hdemo_sk},
          int64_t{catalog_returns.cr_refunded_addr_sk}, int64_t{catalog_returns.cr_returning_customer_sk},
          int64_t{catalog_returns.cr_returning_cdemo_sk}, int64_t{catalog_returns.cr_returning_hdemo_sk},
          int64_t{catalog_returns.cr_returning_addr_sk}, int64_t{catalog_returns.cr_call_center_sk},
          int64_t{catalog_returns.cr_catalog_page_sk}, int64_t{catalog_returns.cr_ship_mode_sk},
          int64_t{catalog_returns.cr_warehouse_sk}, int64_t{catalog_returns.cr_reason_sk},
          int64_t{catalog_returns.cr_order_number}, int32_t{catalog_returns.cr_pricing.quantity},
          decimal_to_float(catalog_returns.cr_pricing.net_paid), decimal_to_float(catalog_returns.cr_pricing.ext_tax),
          decimal_to_float(catalog_returns.cr_pricing.net_paid_inc_tax),
          decimal_to_float(catalog_returns.cr_pricing.fee), decimal_to_float(catalog_returns.cr_pricing.ext_ship_cost),
          decimal_to_float(catalog_returns.cr_pricing.refunded_cash),
          decimal_to_float(catalog_returns.cr_pricing.reversed_charge),
          decimal_to_float(catalog_returns.cr_pricing.store_credit),
          decimal_to_float(catalog_returns.cr_pricing.net_loss));
    }

    table_info_by_name["catalog_returns"].table = catalog_returns_builder.finish_table();
  }

  // catalog sales
  {
    const auto [catalog_sales_first, catalog_sales_count] = prepare_for_table(CATALOG_SALES);

    auto catalog_sales_builder =
        TableBuilder{_benchmark_config->chunk_size, catalog_sales_column_types, catalog_sales_column_names,
                     UseMvcc::Yes, static_cast<size_t>(catalog_sales_count)};

    for (auto i = ds_key_t{0}; i < catalog_sales_count; i++) {
      const auto catalog_sales_functions = getTdefFunctionsByNumber(CATALOG_SALES);
      auto catalog_sales =
          call_dbgen_mk<W_CATALOG_SALES_TBL>(*catalog_sales_functions, catalog_sales_first + i, CATALOG_SALES);

      catalog_sales_builder.append_row(
          int64_t{catalog_sales.cs_sold_date_sk}, int64_t{catalog_sales.cs_sold_time_sk},
          int64_t{catalog_sales.cs_ship_date_sk}, int64_t{catalog_sales.cs_bill_customer_sk},
          int64_t{catalog_sales.cs_bill_cdemo_sk}, int64_t{catalog_sales.cs_bill_hdemo_sk},
          int64_t{catalog_sales.cs_bill_addr_sk}, int64_t{catalog_sales.cs_ship_customer_sk},
          int64_t{catalog_sales.cs_ship_cdemo_sk}, int64_t{catalog_sales.cs_ship_hdemo_sk},
          int64_t{catalog_sales.cs_ship_addr_sk}, int64_t{catalog_sales.cs_call_center_sk},
          int64_t{catalog_sales.cs_catalog_page_sk}, int64_t{catalog_sales.cs_ship_mode_sk},
          int64_t{catalog_sales.cs_warehouse_sk}, int64_t{catalog_sales.cs_sold_item_sk},
          int64_t{catalog_sales.cs_promo_sk}, int64_t{catalog_sales.cs_order_number},
          int32_t{catalog_sales.cs_pricing.quantity}, decimal_to_float(catalog_sales.cs_pricing.wholesale_cost),
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
    }

    table_info_by_name["catalog_sales"].table = catalog_sales_builder.finish_table();
  }

  // customer
  {
    const auto [customer_first, customer_count] = prepare_for_table(CUSTOMER);

    auto customer_builder = TableBuilder{_benchmark_config->chunk_size, customer_column_types, customer_column_names,
                                         UseMvcc::Yes, static_cast<size_t>(customer_count)};

    for (auto i = ds_key_t{0}; i < customer_count; i++) {
      const auto customer_functions = getTdefFunctionsByNumber(CUSTOMER);
      auto customer = call_dbgen_mk<W_CUSTOMER_TBL>(*customer_functions, customer_first + i, CUSTOMER);

      customer_builder.append_row(
          int64_t{customer.c_customer_sk}, customer.c_customer_id, int64_t{customer.c_current_cdemo_sk},
          int64_t{customer.c_current_hdemo_sk}, int64_t{customer.c_current_addr_sk},
          int32_t{customer.c_first_shipto_date_id}, int32_t{customer.c_first_sales_date_id}, customer.c_salutation,
          customer.c_first_name, customer.c_last_name, customer.c_preferred_cust_flag ? "Y" : "N",
          int32_t{customer.c_birth_day}, int32_t{customer.c_birth_month}, int32_t{customer.c_birth_year},
          customer.c_birth_country, customer.c_login, customer.c_email_address, int32_t{customer.c_last_review_date});
    }

    table_info_by_name["customer"].table = customer_builder.finish_table();
  }

  // customer address
  {
    const auto [customer_address_first, customer_address_count] = prepare_for_table(CUSTOMER_ADDRESS);

    auto customer_address_builder =
        TableBuilder{_benchmark_config->chunk_size, customer_address_column_types, customer_address_column_names,
                     UseMvcc::Yes, static_cast<size_t>(customer_address_count)};

    for (auto i = ds_key_t{0}; i < customer_address_count; i++) {
      const auto customer_address_functions = getTdefFunctionsByNumber(CUSTOMER_ADDRESS);
      auto customer_address = call_dbgen_mk<W_CUSTOMER_ADDRESS_TBL>(*customer_address_functions,
                                                                    customer_address_first + i, CUSTOMER_ADDRESS);

      auto street_name = pmr_string{customer_address.ca_address.street_name1};
      if (customer_address.ca_address.street_name2 != nullptr) {
        street_name += pmr_string{" "} + customer_address.ca_address.street_name2;
      }

      customer_address_builder.append_row(
          int64_t{customer_address.ca_addr_sk}, customer_address.ca_addr_id,
          int32_t{customer_address.ca_address.street_num}, std::move(street_name),
          customer_address.ca_address.street_type, customer_address.ca_address.suite_num,
          customer_address.ca_address.city, customer_address.ca_address.county, customer_address.ca_address.state,
          int32_t{customer_address.ca_address.zip}, customer_address.ca_address.country,
          int32_t{customer_address.ca_address.gmt_offset}, customer_address.ca_location_type);
    }

    table_info_by_name["customer_address"].table = customer_address_builder.finish_table();
  }

  // customer demographics
  {
    const auto [customer_demographics_first, customer_demographics_count] = prepare_for_table(CUSTOMER_DEMOGRAPHICS);

    auto customer_demographics_builder = TableBuilder{_benchmark_config->chunk_size, customer_demographics_column_types,
                                                      customer_demographics_column_names, UseMvcc::Yes,
                                                      static_cast<size_t>(customer_demographics_count)};

    for (auto i = ds_key_t{0}; i < customer_demographics_count; i++) {
      const auto customer_demographics_functions = getTdefFunctionsByNumber(CUSTOMER_DEMOGRAPHICS);
      auto customer_demographics = call_dbgen_mk<W_CUSTOMER_DEMOGRAPHICS_TBL>(
          *customer_demographics_functions, customer_demographics_first + i, CUSTOMER_DEMOGRAPHICS);

      customer_demographics_builder.append_row(
          int64_t{customer_demographics.cd_demo_sk}, customer_demographics.cd_gender,
          customer_demographics.cd_marital_status, customer_demographics.cd_education_status,
          int32_t{customer_demographics.cd_purchase_estimate}, customer_demographics.cd_credit_rating,
          int32_t{customer_demographics.cd_dep_count}, int32_t{customer_demographics.cd_dep_employed_count},
          int32_t{customer_demographics.cd_dep_college_count});
    }

    table_info_by_name["customer_demographics"].table = customer_demographics_builder.finish_table();
  }

  // date
  {
    const auto [date_first, date_count] = prepare_for_table(DATE);

    auto date_builder = TableBuilder{_benchmark_config->chunk_size, date_column_types, date_column_names, UseMvcc::Yes,
                                     static_cast<size_t>(date_count)};

    for (auto i = ds_key_t{0}; i < date_count; i++) {
      const auto date_functions = getTdefFunctionsByNumber(DATE);
      auto date = call_dbgen_mk<W_DATE_TBL>(*date_functions, date_first + i, DATE);

      auto quarter_name = pmr_string{std::to_string(date.d_year)};
      quarter_name += "Q" + std::to_string(date.d_qoy);

      date_builder.append_row(
          int64_t{date.d_date_sk}, date.d_date_id, resolve_date_id(date.d_date_sk), int32_t{date.d_month_seq},
          int32_t{date.d_week_seq}, int32_t{date.d_quarter_seq}, int32_t{date.d_year}, int32_t{date.d_dow},
          int32_t{date.d_moy}, int32_t{date.d_dom}, int32_t{date.d_qoy}, int32_t{date.d_fy_year},
          int32_t{date.d_fy_quarter_seq}, int32_t{date.d_fy_week_seq}, date.d_day_name, std::move(quarter_name),
          date.d_holiday ? "Y" : "N", date.d_weekend ? "Y" : "N", date.d_following_holiday ? "Y" : "N",
          int32_t{date.d_first_dom}, int32_t{date.d_last_dom}, int32_t{date.d_same_day_ly}, int32_t{date.d_same_day_lq},
          date.d_current_day ? "Y" : "N", date.d_current_week ? "Y" : "N", date.d_current_month ? "Y" : "N",
          date.d_current_quarter ? "Y" : "N", date.d_current_year ? "Y" : "N");
    }

    table_info_by_name["date"].table = date_builder.finish_table();
  }

  //#define HOUSEHOLD_DEMOGRAPHICS	8
  //#define INCOME_BAND	9
  //#define INVENTORY	10
  //#define ITEM	11
  //#define PROMOTION	12
  //#define REASON	13
  //#define SHIP_MODE	14
  //#define STORE	15
  //#define STORE_RETURNS	16
  //#define STORE_SALES	17
  //#define TIME	18
  //#define WAREHOUSE	19
  //#define WEB_PAGE	20
  //#define WEB_RETURNS	21
  //#define WEB_SALES	22
  //#define WEB_SITE	23
  //#define DBGEN_VERSION	24

  // TODO(pascal): dbgen cleanup?

  return table_info_by_name;
}

}  // namespace opossum
