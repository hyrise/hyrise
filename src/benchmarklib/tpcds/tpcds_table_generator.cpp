#include "tpcds_table_generator.hpp"

extern "C" {
#include <tpcds-kit/tools/dbgen_version.h>
#include <tpcds-kit/tools/decimal.h>
#include <tpcds-kit/tools/genrand.h>
#include <tpcds-kit/tools/nulls.h>
#include <tpcds-kit/tools/parallel.h>
#include <tpcds-kit/tools/r_params.h>
#include <tpcds-kit/tools/tables.h>
#include <tpcds-kit/tools/tdefs.h>

#include <tpcds-kit/tools/w_call_center.h>
#include <tpcds-kit/tools/w_catalog_page.h>
#include <tpcds-kit/tools/w_catalog_returns.h>
#include <tpcds-kit/tools/w_catalog_sales.h>
#include <tpcds-kit/tools/w_customer.h>
#include <tpcds-kit/tools/w_customer_address.h>
#include <tpcds-kit/tools/w_customer_demographics.h>
#include <tpcds-kit/tools/w_datetbl.h>
#include <tpcds-kit/tools/w_household_demographics.h>
#include <tpcds-kit/tools/w_income_band.h>
#include <tpcds-kit/tools/w_inventory.h>
#include <tpcds-kit/tools/w_item.h>
#include <tpcds-kit/tools/w_promotion.h>
#include <tpcds-kit/tools/w_reason.h>
#include <tpcds-kit/tools/w_ship_mode.h>
#include <tpcds-kit/tools/w_store.h>
#include <tpcds-kit/tools/w_store_returns.h>
#include <tpcds-kit/tools/w_store_sales.h>
#include <tpcds-kit/tools/w_timetbl.h>
#include <tpcds-kit/tools/w_warehouse.h>
#include <tpcds-kit/tools/w_web_page.h>
#include <tpcds-kit/tools/w_web_returns.h>
#include <tpcds-kit/tools/w_web_sales.h>
#include <tpcds-kit/tools/w_web_site.h>
}

#include "benchmark_config.hpp"
#include "operators/import_binary.hpp"
#include "table_builder.hpp"
#include "utils/timer.hpp"

namespace {
using namespace opossum;  // NOLINT

using tpcds_key_t = int32_t;

void init_tpcds_tools(uint32_t scale_factor, int rng_seed) {
  // setting some values that were intended by dsdgen to be passed via command line

  auto scale_factor_string = std::string{"SCALE"};
  auto scale_factor_value_string = std::to_string(scale_factor);
  set_int(scale_factor_string.data(), scale_factor_value_string.data());

  auto rng_seed_string = std::string{"RNGSEED"};
  auto rng_seed_value_string = std::to_string(rng_seed);
  set_int(rng_seed_string.data(), rng_seed_value_string.data());

  // init_rand from genrand.c, adapted
  {
    auto n_seed = get_int(rng_seed_string.data());
    auto skip = INT_MAX / MAX_COLUMN;
    for (auto i = 0; i < MAX_COLUMN; i++) {
      Streams[i].nInitialSeed = n_seed + skip * i;
      Streams[i].nSeed = n_seed + skip * i;
      Streams[i].nUsed = 0;
    }
  }

  mk_w_store_sales_master(nullptr, 0, 1);
  mk_w_web_sales_master(nullptr, 0, 1);
  mk_w_web_sales_detail(nullptr, 0, nullptr, nullptr, 1);
  mk_w_catalog_sales_master(nullptr, 0, 1);

  auto distributions_string = std::string{"DISTRIBUTIONS"};
  auto distributions_value = std::string{"resources/benchmark/tpcds/tpcds.idx"};
  set_str(distributions_string.data(), distributions_value.data());

  for (auto table_id = 0; table_id <= MAX_TABLE; table_id++) {
    resetSeeds(table_id);
    RNGReset(table_id);
  }
}

template <class TpcdsRow, int builder(void*, ds_key_t), int table_id>
TpcdsRow call_dbgen_mk(ds_key_t index) {
  auto tpcds_row = TpcdsRow{};
  builder(&tpcds_row, index);
  tpcds_row_stop(table_id);
  return tpcds_row;
}

// get starting index and row count for a table, see third_party/tpcds-kit/tools/driver.c:549
std::pair<ds_key_t, ds_key_t> prepare_for_table(int table_id) {
  auto k_row_count = ds_key_t{};
  auto k_first_row = ds_key_t{};

  split_work(table_id, &k_first_row, &k_row_count);

  const auto& tdefs = *getSimpleTdefsByNumber(table_id);

  if (k_first_row != 1) {
    row_skip(table_id, static_cast<int>(k_first_row - 1));
    if (tdefs.flags & FL_PARENT) {  // NOLINT
      row_skip(tdefs.nParam, static_cast<int>(k_first_row - 1));
    }
  }

  if (tdefs.flags & FL_SMALL) {  // NOLINT
    resetCountCount();
  }

  Assert(k_row_count <= std::numeric_limits<tpcds_key_t>::max(),
         "tpcds_key_t is too small for this scale factor, "
         "consider using tpcds_key_t = int64_t;");
  return {k_first_row, k_row_count};
}

pmr_string boolean_to_string(bool boolean) { return pmr_string(1, boolean ? 'Y' : 'N'); }

pmr_string zip_to_string(int32_t zip) {
  auto result = pmr_string(5, '?');
  std::snprintf(result.data(), result.size() + 1, "%05d", zip);
  return result;
}

// dsdgen deliberately creates NULL values if nullCheck(column_id) is true, resolve functions mimic that
std::optional<pmr_string> resolve_date_id(int column_id, ds_key_t date_id) {
  if (nullCheck(column_id) || date_id <= 0) {
    return std::nullopt;
  }

  auto date = date_t{};
  jtodt(&date, static_cast<int>(date_id));

  auto result = pmr_string(10, '?');
  std::snprintf(result.data(), result.size() + 1, "%4d-%02d-%02d", date.year, date.month, date.day);

  return result;
}

std::optional<tpcds_key_t> resolve_key(int column_id, ds_key_t key) {
  return nullCheck(column_id) || key == -1 ? std::nullopt : std::optional{static_cast<tpcds_key_t>(key)};
}

std::optional<pmr_string> resolve_string(int column_id, pmr_string string) {
  return nullCheck(column_id) || string.empty() ? std::nullopt : std::optional{std::move(string)};
}

std::optional<int32_t> resolve_integer(int column_id, int value) {
  return nullCheck(column_id) ? std::nullopt : std::optional{int32_t{value}};
}

std::optional<float> resolve_decimal(int column_id, decimal_t decimal) {
  auto result = 0.0;
  dectof(&result, &decimal);
  // we have to divide by 10 after dectof to get the expected result
  return nullCheck(column_id) ? std::nullopt : std::optional{static_cast<float>(result / 10)};
}

std::optional<float> resolve_gmt_offset(int column_id, int32_t gmt_offset) {
  return nullCheck(column_id) ? std::nullopt : std::optional{static_cast<float>(gmt_offset)};
}

std::optional<pmr_string> resolve_street_name(int column_id, const ds_addr_t& address) {
  return nullCheck(column_id) ? std::nullopt
                              : address.street_name2 == nullptr
                                    ? std::optional{pmr_string{address.street_name1}}
                                    : std::optional{pmr_string{address.street_name1} + " " + address.street_name2};
}

// mapping types used by tpcds-dbgen as follows (according to create table statements in tpcds.sql):
// ds_key_t -> tpcds_key_t
// int -> int32_t
// char*, char[], bool, date (ds_key_t as date_id), time (ds_key_t as time_id) -> pmr_string
// decimal, float -> float
// ds_addr_t -> corresponding types for types in struct ds_addr_t, see address.h

// in tpcds most columns are nullable, so we pass std::optional<?> as type

// clang-format off
const auto call_center_column_types = boost::hana::tuple<     tpcds_key_t         , pmr_string          , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<int32_t> , std::optional<int32_t> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<int32_t> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<int32_t> , std::optional<pmr_string> , std::optional<int32_t> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<float> , std::optional<float>>(); // NOLINT
const auto call_center_column_names = boost::hana::make_tuple("cc_call_center_sk" , "cc_call_center_id" , "cc_rec_start_date"       , "cc_rec_end_date"         , "cc_closed_date_sk"        , "cc_open_date_sk"          , "cc_name"                 , "cc_class"                , "cc_employees"         , "cc_sq_ft"             , "cc_hours"                , "cc_manager"              , "cc_mkt_id"            , "cc_mkt_class"            , "cc_mkt_desc"             , "cc_market_manager"       , "cc_division"          , "cc_division_name"        , "cc_company"           , "cc_company_name"         , "cc_street_number"        , "cc_street_name"          , "cc_street_type"          , "cc_suite_number"         , "cc_city"                 , "cc_county"               , "cc_state"                , "cc_zip"                  , "cc_country"              , "cc_gmt_offset"      , "cc_tax_percentage"); // NOLINT

const auto catalog_page_column_types = boost::hana::tuple<     tpcds_key_t          , pmr_string           , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<pmr_string> , std::optional<int32_t> , std::optional<int32_t>   , std::optional<pmr_string> , std::optional<pmr_string>>(); // NOLINT
const auto catalog_page_column_names = boost::hana::make_tuple("cp_catalog_page_sk" , "cp_catalog_page_id" , "cp_start_date_sk"         , "cp_end_date_sk"           , "cp_department"           , "cp_catalog_number"    , "cp_catalog_page_number" , "cp_description"          , "cp_type"); // NOLINT

const auto catalog_returns_column_types = boost::hana::tuple<     std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , tpcds_key_t  , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , tpcds_key_t       , std::optional<int32_t> , std::optional<float> , std::optional<float> , std::optional<float>    , std::optional<float> , std::optional<float>  , std::optional<float> , std::optional<float> , std::optional<float> , std::optional<float>>(); // NOLINT
const auto catalog_returns_column_names = boost::hana::make_tuple("cr_returned_date_sk"      , "cr_returned_time_sk"      , "cr_item_sk" , "cr_refunded_customer_sk"  , "cr_refunded_cdemo_sk"     , "cr_refunded_hdemo_sk"     , "cr_refunded_addr_sk"      , "cr_returning_customer_sk" , "cr_returning_cdemo_sk"    , "cr_returning_hdemo_sk"    , "cr_returning_addr_sk"     , "cr_call_center_sk"        , "cr_catalog_page_sk"       , "cr_ship_mode_sk"          , "cr_warehouse_sk"          , "cr_reason_sk"             , "cr_order_number" , "cr_return_quantity"   , "cr_return_amount"   , "cr_return_tax"      , "cr_return_amt_inc_tax" , "cr_fee"             , "cr_return_ship_cost" , "cr_refunded_cash"   , "cr_reversed_charge" , "cr_store_credit"    , "cr_net_loss"); // NOLINT

const auto catalog_sales_column_types = boost::hana::tuple<     std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , tpcds_key_t  , std::optional<tpcds_key_t> , tpcds_key_t       , std::optional<int32_t> , std::optional<float> , std::optional<float> , std::optional<float> , std::optional<float>  , std::optional<float> , std::optional<float>    , std::optional<float> , std::optional<float> , std::optional<float> , std::optional<float> , std::optional<float> , std::optional<float>  , std::optional<float>   , std::optional<float>       , std::optional<float>>(); // NOLINT
const auto catalog_sales_column_names = boost::hana::make_tuple("cs_sold_date_sk"          , "cs_sold_time_sk"          , "cs_ship_date_sk"          , "cs_bill_customer_sk"      , "cs_bill_cdemo_sk"         , "cs_bill_hdemo_sk"         , "cs_bill_addr_sk"          , "cs_ship_customer_sk"      , "cs_ship_cdemo_sk"         , "cs_ship_hdemo_sk"         , "cs_ship_addr_sk"          , "cs_call_center_sk"        , "cs_catalog_page_sk"       , "cs_ship_mode_sk"          , "cs_warehouse_sk"          , "cs_item_sk" , "cs_promo_sk"              , "cs_order_number" , "cs_quantity"          , "cs_wholesale_cost"  , "cs_list_price"      , "cs_sales_price"     , "cs_ext_discount_amt" , "cs_ext_sales_price" , "cs_ext_wholesale_cost" , "cs_ext_list_price"  , "cs_ext_tax"         , "cs_coupon_amt"      , "cs_ext_ship_cost"   , "cs_net_paid"        , "cs_net_paid_inc_tax" , "cs_net_paid_inc_ship" , "cs_net_paid_inc_ship_tax" , "cs_net_profit"); // NOLINT

const auto customer_column_types = boost::hana::tuple<     tpcds_key_t     , pmr_string      , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<int32_t>   , std::optional<int32_t>  , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<int32_t> , std::optional<int32_t> , std::optional<int32_t> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<int32_t>>(); // NOLINT
const auto customer_column_names = boost::hana::make_tuple("c_customer_sk" , "c_customer_id" , "c_current_cdemo_sk"       , "c_current_hdemo_sk"       , "c_current_addr_sk"        , "c_first_shipto_date_sk" , "c_first_sales_date_sk" , "c_salutation"            , "c_first_name"            , "c_last_name"             , "c_preferred_cust_flag"   , "c_birth_day"          , "c_birth_month"        , "c_birth_year"         , "c_birth_country"         , "c_login"                 , "c_email_address"         , "c_last_review_date"); // NOLINT

const auto customer_address_column_types = boost::hana::tuple<     tpcds_key_t     , pmr_string      , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<float> , std::optional<pmr_string>>(); // NOLINT
const auto customer_address_column_names = boost::hana::make_tuple("ca_address_sk" , "ca_address_id" , "ca_street_number"        , "ca_street_name"          , "ca_street_type"          , "ca_suite_number"         , "ca_city"                 , "ca_county"               , "ca_state"                , "ca_zip"                  , "ca_country"              , "ca_gmt_offset"      , "ca_location_type"); // NOLINT

const auto customer_demographics_column_types = boost::hana::tuple<     tpcds_key_t  , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<int32_t> , std::optional<pmr_string> , std::optional<int32_t> , std::optional<int32_t>  , std::optional<int32_t>>(); // NOLINT
const auto customer_demographics_column_names = boost::hana::make_tuple("cd_demo_sk" , "cd_gender"               , "cd_marital_status"       , "cd_education_status"     , "cd_purchase_estimate" , "cd_credit_rating"        , "cd_dep_count"         , "cd_dep_employed_count" , "cd_dep_college_count"); // NOLINT

const auto date_column_types = boost::hana::tuple<     tpcds_key_t , pmr_string  , std::optional<pmr_string> , std::optional<int32_t> , std::optional<int32_t> , std::optional<int32_t> , std::optional<int32_t> , std::optional<int32_t> , std::optional<int32_t> , std::optional<int32_t> , std::optional<int32_t> , std::optional<int32_t> , std::optional<int32_t> , std::optional<int32_t> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<int32_t> , std::optional<int32_t> , std::optional<int32_t> , std::optional<int32_t> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string>>(); // NOLINT
const auto date_column_names = boost::hana::make_tuple("d_date_sk" , "d_date_id" , "d_date"                  , "d_month_seq"          , "d_week_seq"           , "d_quarter_seq"        , "d_year"               , "d_dow"                , "d_moy"                , "d_dom"                , "d_qoy"                , "d_fy_year"            , "d_fy_quarter_seq"     , "d_fy_week_seq"        , "d_day_name"              , "d_quarter_name"          , "d_holiday"               , "d_weekend"               , "d_following_holiday"     , "d_first_dom"          , "d_last_dom"           , "d_same_day_ly"        , "d_same_day_lq"        , "d_current_day"           , "d_current_week"          , "d_current_month"         , "d_current_quarter"       , "d_current_year"); // NOLINT

const auto household_demographics_column_types = boost::hana::tuple<     tpcds_key_t  , std::optional<tpcds_key_t> , std::optional<pmr_string> , std::optional<int32_t> , std::optional<int32_t>>(); // NOLINT
const auto household_demographics_column_names = boost::hana::make_tuple("hd_demo_sk" , "hd_income_band_sk"        , "hd_buy_potential"        , "hd_dep_count"         , "hd_vehicle_count"); // NOLINT

const auto income_band_column_types = boost::hana::tuple<     int32_t             , std::optional<int32_t> , std::optional<int32_t>>(); // NOLINT
const auto income_band_column_names = boost::hana::make_tuple("ib_income_band_sk" , "ib_lower_bound"       , "ib_upper_bound"); // NOLINT

const auto inventory_column_types = boost::hana::tuple<     tpcds_key_t   , tpcds_key_t   , tpcds_key_t        , std::optional<int32_t>>(); // NOLINT
const auto inventory_column_names = boost::hana::make_tuple("inv_date_sk" , "inv_item_sk" , "inv_warehouse_sk" , "inv_quantity_on_hand"); // NOLINT

const auto item_column_types = boost::hana::tuple<     tpcds_key_t , pmr_string  , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<float> , std::optional<float> , std::optional<tpcds_key_t> , std::optional<pmr_string> , std::optional<tpcds_key_t> , std::optional<pmr_string> , std::optional<tpcds_key_t> , std::optional<pmr_string> , std::optional<tpcds_key_t> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<tpcds_key_t> , std::optional<pmr_string>>(); // NOLINT
const auto item_column_names = boost::hana::make_tuple("i_item_sk" , "i_item_id" , "i_rec_start_date"        , "i_rec_end_date"          , "i_item_desc"             , "i_current_price"    , "i_wholesale_cost"   , "i_brand_id"               , "i_brand"                 , "i_class_id"               , "i_class"                 , "i_category_id"            , "i_category"              , "i_manufact_id"            , "i_manufact"              , "i_size"                  , "i_formulation"           , "i_color"                 , "i_units"                 , "i_container"             , "i_manager_id"             , "i_product_name"); // NOLINT

const auto promotion_column_types = boost::hana::tuple<     tpcds_key_t  , pmr_string   , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<float> , std::optional<int32_t> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string>>(); // NOLINT
const auto promotion_column_names = boost::hana::make_tuple("p_promo_sk" , "p_promo_id" , "p_start_date_sk"          , "p_end_date_sk"            , "p_item_sk"                , "p_cost"             , "p_response_target"    , "p_promo_name"            , "p_channel_dmail"         , "p_channel_email"         , "p_channel_catalog"       , "p_channel_tv"            , "p_channel_radio"         , "p_channel_press"         , "p_channel_event"         , "p_channel_demo"          , "p_channel_details"       , "p_purpose"               , "p_discount_active"); // NOLINT

const auto reason_column_types = boost::hana::tuple<     tpcds_key_t   , pmr_string    , std::optional<pmr_string>>(); // NOLINT
const auto reason_column_names = boost::hana::make_tuple("r_reason_sk" , "r_reason_id" , "r_reason_desc"); // NOLINT

const auto ship_mode_column_types = boost::hana::tuple<     tpcds_key_t       , pmr_string        , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string>>(); // NOLINT
const auto ship_mode_column_names = boost::hana::make_tuple("sm_ship_mode_sk" , "sm_ship_mode_id" , "sm_type"                 , "sm_code"                 , "sm_carrier"              , "sm_contract"); // NOLINT

const auto store_column_types = boost::hana::tuple<     tpcds_key_t  , pmr_string   , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<tpcds_key_t> , std::optional<pmr_string> , std::optional<int32_t> , std::optional<int32_t> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<int32_t> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<tpcds_key_t> , std::optional<pmr_string> , std::optional<tpcds_key_t> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<float> , std::optional<float>>(); // NOLINT
const auto store_column_names = boost::hana::make_tuple("s_store_sk" , "s_store_id" , "s_rec_start_date"        , "s_rec_end_date"          , "s_closed_date_sk"         , "s_store_name"            , "s_number_employees"   , "s_floor_space"        , "s_hours"                 , "s_manager"               , "s_market_id"          , "s_geography_class"       , "s_market_desc"           , "s_market_manager"        , "s_division_id"            , "s_division_name"         , "s_company_id"             , "s_company_name"          , "s_street_number"         , "s_street_name"           , "s_street_type"           , "s_suite_number"          , "s_city"                  , "s_county"                , "s_state"                 , "s_zip"                   , "s_country"               , "s_gmt_offset"       , "s_tax_precentage"); // NOLINT

const auto store_returns_column_types = boost::hana::tuple<     std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , tpcds_key_t  , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , tpcds_key_t        , std::optional<int32_t> , std::optional<float> , std::optional<float> , std::optional<float>    , std::optional<float> , std::optional<float>  , std::optional<float> , std::optional<float> , std::optional<float> , std::optional<float>>(); // NOLINT
const auto store_returns_column_names = boost::hana::make_tuple("sr_returned_date_sk"      , "sr_return_time_sk"        , "sr_item_sk" , "sr_customer_sk"           , "sr_cdemo_sk"              , "sr_hdemo_sk"              , "sr_addr_sk"               , "sr_store_sk"              , "sr_reason_sk"             , "sr_ticket_number" , "sr_return_quantity"   , "sr_return_amt"      , "sr_return_tax"      , "sr_return_amt_inc_tax" , "sr_fee"             , "sr_return_ship_cost" , "sr_refunded_cash"   , "sr_reversed_charge" , "sr_store_credit"    , "sr_net_loss"); // NOLINT

const auto store_sales_column_types = boost::hana::tuple<     std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , tpcds_key_t  , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , tpcds_key_t        , std::optional<int32_t> , std::optional<float> , std::optional<float> , std::optional<float> , std::optional<float>  , std::optional<float> , std::optional<float>    , std::optional<float> , std::optional<float> , std::optional<float> , std::optional<float> , std::optional<float>  , std::optional<float>>(); // NOLINT
const auto store_sales_column_names = boost::hana::make_tuple("ss_sold_date_sk"          , "ss_sold_time_sk"          , "ss_item_sk" , "ss_customer_sk"           , "ss_cdemo_sk"              , "ss_hdemo_sk"              , "ss_addr_sk"               , "ss_store_sk"              , "ss_promo_sk"              , "ss_ticket_number" , "ss_quantity"          , "ss_wholesale_cost"  , "ss_list_price"      , "ss_sales_price"     , "ss_ext_discount_amt" , "ss_ext_sales_price" , "ss_ext_wholesale_cost" , "ss_ext_list_price"  , "ss_ext_tax"         , "ss_coupon_amt"      , "ss_net_paid"        , "ss_net_paid_inc_tax" , "ss_net_profit"); // NOLINT

const auto time_column_types = boost::hana::tuple<     tpcds_key_t      , pmr_string  , std::optional<int32_t> , std::optional<int32_t> , std::optional<int32_t> , std::optional<int32_t> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string>>(); // NOLINT
const auto time_column_names = boost::hana::make_tuple("t_time_sk"      , "t_time_id" , "t_time"               , "t_hour"               , "t_minute"             , "t_second"             , "t_am_pm"                 , "t_shift"                 , "t_sub_shift"             , "t_meal_time"); // NOLINT

const auto warehouse_column_types = boost::hana::tuple<     tpcds_key_t      , pmr_string       , std::optional<pmr_string> , std::optional<int32_t> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<float>>(); // NOLINT
const auto warehouse_column_names = boost::hana::make_tuple("w_warehouse_sk" , "w_warehouse_id" , "w_warehouse_name"        , "w_warehouse_sq_ft"    , "w_street_number"         , "w_street_name"           , "w_street_type"           , "w_suite_number"          , "w_city"                  , "w_county"                , "w_state"                 , "w_zip"                   , "w_country"               , "w_gmt_offset"); // NOLINT

const auto web_page_column_types = boost::hana::tuple<     tpcds_key_t      , pmr_string       , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<pmr_string> , std::optional<tpcds_key_t> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<int32_t> , std::optional<int32_t> , std::optional<int32_t> , std::optional<int32_t>>(); // NOLINT
const auto web_page_column_names = boost::hana::make_tuple("wp_web_page_sk" , "wp_web_page_id" , "wp_rec_start_date"       , "wp_rec_end_date"         , "wp_creation_date_sk"      , "wp_access_date_sk"        , "wp_autogen_flag"         , "wp_customer_sk"           , "wp_url"                  , "wp_type"                 , "wp_char_count"        , "wp_link_count"        , "wp_image_count"       , "wp_max_ad_count"); // NOLINT

const auto web_returns_column_types = boost::hana::tuple<     std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , tpcds_key_t  , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , tpcds_key_t       , std::optional<int32_t> , std::optional<float> , std::optional<float> , std::optional<float>    , std::optional<float> , std::optional<float>  , std::optional<float> , std::optional<float> , std::optional<float> , std::optional<float>>(); // NOLINT
const auto web_returns_column_names = boost::hana::make_tuple("wr_returned_date_sk"      , "wr_returned_time_sk"      , "wr_item_sk" , "wr_refunded_customer_sk"  , "wr_refunded_cdemo_sk"     , "wr_refunded_hdemo_sk"     , "wr_refunded_addr_sk"      , "wr_returning_customer_sk" , "wr_returning_cdemo_sk"    , "wr_returning_hdemo_sk"    , "wr_returning_addr_sk"     , "wr_web_page_sk"           , "wr_reason_sk"             , "wr_order_number" , "wr_return_quantity"   , "wr_return_amt"      , "wr_return_tax"      , "wr_return_amt_inc_tax" , "wr_fee"             , "wr_return_ship_cost" , "wr_refunded_cash"   , "wr_reversed_charge" , "wr_account_credit"  , "wr_net_loss"); // NOLINT

const auto web_sales_column_types = boost::hana::tuple<     std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , tpcds_key_t  , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , tpcds_key_t       , std::optional<int32_t> , std::optional<float> , std::optional<float> , std::optional<float> , std::optional<float>  , std::optional<float> , std::optional<float>    , std::optional<float> , std::optional<float> , std::optional<float> , std::optional<float> , std::optional<float> , std::optional<float>  , std::optional<float>   , std::optional<float>       , std::optional<float>>(); // NOLINT
const auto web_sales_column_names = boost::hana::make_tuple("ws_sold_date_sk"          , "ws_sold_time_sk"          , "ws_ship_date_sk"          , "ws_item_sk" , "ws_bill_customer_sk"      , "ws_bill_cdemo_sk"         , "ws_bill_hdemo_sk"         , "ws_bill_addr_sk"          , "ws_ship_customer_sk"      , "ws_ship_cdemo_sk"         , "ws_ship_hdemo_sk"         , "ws_ship_addr_sk"          , "ws_web_page_sk"           , "ws_web_site_sk"           , "ws_ship_mode_sk"          , "ws_warehouse_sk"          , "ws_promo_sk"              , "ws_order_number" , "ws_quantity"          , "ws_wholesale_cost"  , "ws_list_price"      , "ws_sales_price"     , "ws_ext_discount_amt" , "ws_ext_sales_price" , "ws_ext_wholesale_cost" , "ws_ext_list_price"  , "ws_ext_tax"         , "ws_coupon_amt"      , "ws_ext_ship_cost"   , "ws_net_paid"        , "ws_net_paid_inc_tax" , "ws_net_paid_inc_ship" , "ws_net_paid_inc_ship_tax" , "ws_net_profit"); // NOLINT

const auto web_site_column_types = boost::hana::tuple<     tpcds_key_t   , pmr_string    , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<tpcds_key_t> , std::optional<tpcds_key_t> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<int32_t> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<int32_t> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<pmr_string> , std::optional<float> , std::optional<float>>(); // NOLINT
const auto web_site_column_names = boost::hana::make_tuple("web_site_sk" , "web_site_id" , "web_rec_start_date"      , "web_rec_end_date"        , "web_name"                , "web_open_date_sk"         , "web_close_date_sk"        , "web_class"               , "web_manager"             , "web_mkt_id"           , "web_mkt_class"           , "web_mkt_desc"            , "web_market_manager"      , "web_company_id"       , "web_company_name"        , "web_street_number"       , "web_street_name"         , "web_street_type"         , "web_suite_number"        , "web_city"                , "web_county"              , "web_state"               , "web_zip"                 , "web_country"             , "web_gmt_offset"     , "web_tax_percentage"); // NOLINT
// clang-format on
}  // namespace

namespace opossum {

TpcdsTableGenerator::TpcdsTableGenerator(uint32_t scale_factor, ChunkOffset chunk_size, int rng_seed)
    : AbstractTableGenerator(create_benchmark_config_with_chunk_size(chunk_size)), _scale_factor{scale_factor} {
  init_tpcds_tools(scale_factor, rng_seed);
}

TpcdsTableGenerator::TpcdsTableGenerator(uint32_t scale_factor,
                                         const std::shared_ptr<BenchmarkConfig>& benchmark_config, int rng_seed)
    : AbstractTableGenerator(benchmark_config), _scale_factor{scale_factor} {
  init_tpcds_tools(scale_factor, rng_seed);
}

std::unordered_map<std::string, BenchmarkTableInfo> TpcdsTableGenerator::generate() {
  auto table_info_by_name = std::unordered_map<std::string, BenchmarkTableInfo>{};

  // try to load cached tables
  const auto cache_directory = "tpcds_cached_tables/sf-" + std::to_string(_scale_factor);  // NOLINT
  if (_benchmark_config->cache_binary_tables && std::filesystem::is_directory(cache_directory)) {
    for (const auto& table_file : std::filesystem::recursive_directory_iterator(cache_directory)) {
      const auto table_name = table_file.path().stem();
      auto timer = Timer{};
      std::cout << "-  Loading table " << table_name << " from cached binary " << table_file.path().relative_path();

      table_info_by_name[table_name].table = ImportBinary::read_binary(table_file.path());
      table_info_by_name[table_name].loaded_from_binary = true;

      std::cout << " (" << timer.lap_formatted() << ")" << std::endl;
    }

    return table_info_by_name;
  }

  for (const auto& table_name : {"call_center", "catalog_page", "customer_address", "customer", "customer_demographics",
                                 "date_dim", "household_demographics", "income_band", "inventory", "item", "promotion",
                                 "reason", "ship_mode", "store", "time_dim", "warehouse", "web_page", "web_site"}) {
    table_info_by_name[table_name].table = _generate_table(table_name);
  }

  for (const auto& [sales_table_name, returns_table_name] : std::vector<std::pair<std::string, std::string>>{
           {"catalog_sales", "catalog_returns"}, {"store_sales", "store_returns"}, {"web_sales", "web_returns"}}) {
    auto catalog_sales_and_returns = _generate_sales_and_returns_tables(sales_table_name);
    table_info_by_name[sales_table_name].table = catalog_sales_and_returns.first;
    table_info_by_name[returns_table_name].table = catalog_sales_and_returns.second;
  }

  if (_benchmark_config->cache_binary_tables) {
    std::filesystem::create_directories(cache_directory);
    for (auto& [table_name, table_info] : table_info_by_name) {
      table_info.binary_file_path = cache_directory + "/" + table_name + ".bin";  // NOLINT
    }
  }

  return table_info_by_name;
}

std::shared_ptr<Table> TpcdsTableGenerator::_generate_table(const std::string& table_name) const {
  if (table_name == "call_center") {
    return generate_call_center();
  } else if (table_name == "catalog_page") {
    return generate_catalog_page();
  } else if (table_name == "customer_address") {
    return generate_customer_address();
  } else if (table_name == "customer") {
    return generate_customer();
  } else if (table_name == "customer_demographics") {
    return generate_customer_demographics();
  } else if (table_name == "date_dim") {
    return generate_date_dim();
  } else if (table_name == "household_demographics") {
    return generate_household_demographics();
  } else if (table_name == "income_band") {
    return generate_income_band();
  } else if (table_name == "inventory") {
    return generate_inventory();
  } else if (table_name == "item") {
    return generate_item();
  } else if (table_name == "promotion") {
    return generate_promotion();
  } else if (table_name == "reason") {
    return generate_reason();
  } else if (table_name == "ship_mode") {
    return generate_ship_mode();
  } else if (table_name == "store") {
    return generate_store();
  } else if (table_name == "time_dim") {
    return generate_time_dim();
  } else if (table_name == "warehouse") {
    return generate_warehouse();
  } else if (table_name == "web_page") {
    return generate_web_page();
  } else if (table_name == "web_site") {
    return generate_web_site();
  } else {
    Fail("Unexpected table name: " + table_name);
  }
}

std::pair<std::shared_ptr<Table>, std::shared_ptr<Table>> TpcdsTableGenerator::_generate_sales_and_returns_tables(
    const std::string& sales_table_name) const {
  if (sales_table_name == "catalog_sales") {
    return generate_catalog_sales_and_returns();
  } else if (sales_table_name == "store_sales") {
    return generate_store_sales_and_returns();
  } else if (sales_table_name == "web_sales") {
    return generate_web_sales_and_returns();
  } else {
    Fail("Unexpected sales table name: " + sales_table_name);
  }
}

std::shared_ptr<Table> TpcdsTableGenerator::generate_call_center(ds_key_t max_rows) const {
  auto [call_center_first, call_center_count] = prepare_for_table(CALL_CENTER);
  call_center_count = std::min(call_center_count, max_rows);

  auto call_center_builder = TableBuilder{_benchmark_config->chunk_size, call_center_column_types,
                                          call_center_column_names, static_cast<ChunkOffset>(call_center_count)};

  auto call_center = CALL_CENTER_TBL{};
  call_center.cc_closed_date_id = ds_key_t{-1};
  for (auto i = ds_key_t{0}; i < call_center_count; i++) {
    // mk_w_call_center needs a pointer to the previous result of mk_w_call_center to add "update entries" for the
    // same call center
    mk_w_call_center(&call_center, call_center_first + i);
    tpcds_row_stop(CALL_CENTER);

    call_center_builder.append_row(
        call_center.cc_call_center_sk, call_center.cc_call_center_id,
        resolve_date_id(CC_REC_START_DATE_ID, call_center.cc_rec_start_date_id),
        resolve_date_id(CC_REC_END_DATE_ID, call_center.cc_rec_end_date_id),
        resolve_key(CC_CLOSED_DATE_ID, call_center.cc_closed_date_id),
        resolve_key(CC_OPEN_DATE_ID, call_center.cc_open_date_id), resolve_string(CC_NAME, call_center.cc_name),
        resolve_string(CC_CLASS, call_center.cc_class), resolve_integer(CC_EMPLOYEES, call_center.cc_employees),
        resolve_integer(CC_SQ_FT, call_center.cc_sq_ft), resolve_string(CC_HOURS, call_center.cc_hours),
        resolve_string(CC_MANAGER, call_center.cc_manager), resolve_integer(CC_MARKET_ID, call_center.cc_market_id),
        resolve_string(CC_MARKET_CLASS, call_center.cc_market_class),
        resolve_string(CC_MARKET_DESC, call_center.cc_market_desc),
        resolve_string(CC_MARKET_MANAGER, call_center.cc_market_manager),
        resolve_integer(CC_DIVISION, call_center.cc_division_id),
        resolve_string(CC_DIVISION_NAME, call_center.cc_division_name),
        resolve_integer(CC_COMPANY, call_center.cc_company),
        resolve_string(CC_COMPANY_NAME, call_center.cc_company_name),
        resolve_string(CC_ADDRESS, pmr_string{std::to_string(call_center.cc_address.street_num)}),
        resolve_street_name(CC_ADDRESS, call_center.cc_address),
        resolve_string(CC_ADDRESS, call_center.cc_address.street_type),
        resolve_string(CC_ADDRESS, call_center.cc_address.suite_num),
        resolve_string(CC_ADDRESS, call_center.cc_address.city),
        resolve_string(CC_ADDRESS, call_center.cc_address.county),
        resolve_string(CC_ADDRESS, call_center.cc_address.state),
        resolve_string(CC_ADDRESS, zip_to_string(call_center.cc_address.zip)),
        resolve_string(CC_ADDRESS, call_center.cc_address.country),
        resolve_gmt_offset(CC_ADDRESS, call_center.cc_address.gmt_offset),
        resolve_decimal(CC_TAX_PERCENTAGE, call_center.cc_tax_percentage));
  }

  return call_center_builder.finish_table();
}

std::shared_ptr<Table> TpcdsTableGenerator::generate_catalog_page(ds_key_t max_rows) const {
  auto [catalog_page_first, catalog_page_count] = prepare_for_table(CATALOG_PAGE);
  catalog_page_count = std::min(catalog_page_count, max_rows);

  auto catalog_page_builder = TableBuilder{_benchmark_config->chunk_size, catalog_page_column_types,
                                           catalog_page_column_names, static_cast<ChunkOffset>(catalog_page_count)};

  auto catalog_page = CATALOG_PAGE_TBL{};
  std::snprintf(catalog_page.cp_department, sizeof(catalog_page.cp_department), "%s", "DEPARTMENT");
  for (auto i = ds_key_t{0}; i < catalog_page_count; i++) {
    // need a pointer to the previous result of mk_w_catalog_page, because cp_department is only set once
    mk_w_catalog_page(&catalog_page, catalog_page_first + i);
    tpcds_row_stop(CATALOG_PAGE);

    catalog_page_builder.append_row(catalog_page.cp_catalog_page_sk, catalog_page.cp_catalog_page_id,
                                    resolve_key(CP_START_DATE_ID, catalog_page.cp_start_date_id),
                                    resolve_key(CP_END_DATE_ID, catalog_page.cp_end_date_id),
                                    resolve_string(CP_DEPARTMENT, catalog_page.cp_department),
                                    resolve_integer(CP_CATALOG_NUMBER, catalog_page.cp_catalog_number),
                                    resolve_integer(CP_CATALOG_PAGE_NUMBER, catalog_page.cp_catalog_page_number),
                                    resolve_string(CP_DESCRIPTION, catalog_page.cp_description),
                                    resolve_string(CP_TYPE, catalog_page.cp_type));
  }

  return catalog_page_builder.finish_table();
}

std::pair<std::shared_ptr<Table>, std::shared_ptr<Table>> TpcdsTableGenerator::generate_catalog_sales_and_returns(
    ds_key_t max_rows) const {
  auto [catalog_sales_first, catalog_sales_count] = prepare_for_table(CATALOG_SALES);
  // catalog_sales_count is NOT the actual number of catalog sales created, for each of these "master" catalog_sales
  // multiple "detail" catalog sales are created and possibly returned

  auto catalog_sales_builder = TableBuilder{_benchmark_config->chunk_size, catalog_sales_column_types,
                                            catalog_sales_column_names, static_cast<ChunkOffset>(catalog_sales_count)};

  auto catalog_returns_builder =
      TableBuilder{_benchmark_config->chunk_size, catalog_returns_column_types, catalog_returns_column_names};

  for (auto i = ds_key_t{0}; i < catalog_sales_count; i++) {
    auto catalog_sales = W_CATALOG_SALES_TBL{};
    auto catalog_returns = W_CATALOG_RETURNS_TBL{};

    // modified call to mk_w_catalog_sales(&catalog_sales, catalog_sales_first + i, &catalog_returns, &was_returned);
    {
      mk_w_catalog_sales_master(&catalog_sales, catalog_sales_first + i, 0);
      int n_lineitems;
      genrand_integer(&n_lineitems, DIST_UNIFORM, 4, 14, 0, CS_ORDER_NUMBER);
      for (auto j = 1; j <= n_lineitems; j++) {
        int was_returned = 0;
        mk_w_catalog_sales_detail(&catalog_sales, 0, &catalog_returns, &was_returned);

        if (catalog_sales_builder.row_count() < static_cast<size_t>(max_rows)) {
          catalog_sales_builder.append_row(
              resolve_key(CS_SOLD_DATE_SK, catalog_sales.cs_sold_date_sk),
              resolve_key(CS_SOLD_TIME_SK, catalog_sales.cs_sold_time_sk),
              resolve_key(CS_SHIP_DATE_SK, catalog_sales.cs_ship_date_sk),
              resolve_key(CS_BILL_CUSTOMER_SK, catalog_sales.cs_bill_customer_sk),
              resolve_key(CS_BILL_CDEMO_SK, catalog_sales.cs_bill_cdemo_sk),
              resolve_key(CS_BILL_HDEMO_SK, catalog_sales.cs_bill_hdemo_sk),
              resolve_key(CS_BILL_ADDR_SK, catalog_sales.cs_bill_addr_sk),
              resolve_key(CS_SHIP_CUSTOMER_SK, catalog_sales.cs_ship_customer_sk),
              resolve_key(CS_SHIP_CDEMO_SK, catalog_sales.cs_ship_cdemo_sk),
              resolve_key(CS_SHIP_HDEMO_SK, catalog_sales.cs_ship_hdemo_sk),
              resolve_key(CS_SHIP_ADDR_SK, catalog_sales.cs_ship_addr_sk),
              resolve_key(CS_CALL_CENTER_SK, catalog_sales.cs_call_center_sk),
              resolve_key(CS_CATALOG_PAGE_SK, catalog_sales.cs_catalog_page_sk),
              resolve_key(CS_SHIP_MODE_SK, catalog_sales.cs_ship_mode_sk),
              resolve_key(CS_WAREHOUSE_SK, catalog_sales.cs_warehouse_sk), catalog_sales.cs_sold_item_sk,
              resolve_key(CS_PROMO_SK, catalog_sales.cs_promo_sk), catalog_sales.cs_order_number,
              resolve_integer(CS_PRICING_QUANTITY, catalog_sales.cs_pricing.quantity),
              resolve_decimal(CS_PRICING_WHOLESALE_COST, catalog_sales.cs_pricing.wholesale_cost),
              resolve_decimal(CS_PRICING_LIST_PRICE, catalog_sales.cs_pricing.list_price),
              resolve_decimal(CS_PRICING_SALES_PRICE, catalog_sales.cs_pricing.sales_price),
              resolve_decimal(CS_PRICING_EXT_DISCOUNT_AMOUNT, catalog_sales.cs_pricing.ext_discount_amt),
              resolve_decimal(CS_PRICING_EXT_SALES_PRICE, catalog_sales.cs_pricing.ext_sales_price),
              resolve_decimal(CS_PRICING_EXT_WHOLESALE_COST, catalog_sales.cs_pricing.ext_wholesale_cost),
              resolve_decimal(CS_PRICING_EXT_LIST_PRICE, catalog_sales.cs_pricing.ext_list_price),
              resolve_decimal(CS_PRICING_EXT_TAX, catalog_sales.cs_pricing.ext_tax),
              resolve_decimal(CS_PRICING_COUPON_AMT, catalog_sales.cs_pricing.coupon_amt),
              resolve_decimal(CS_PRICING_EXT_SHIP_COST, catalog_sales.cs_pricing.ext_ship_cost),
              resolve_decimal(CS_PRICING_NET_PAID, catalog_sales.cs_pricing.net_paid),
              resolve_decimal(CS_PRICING_NET_PAID_INC_TAX, catalog_sales.cs_pricing.net_paid_inc_tax),
              resolve_decimal(CS_PRICING_NET_PAID_INC_SHIP, catalog_sales.cs_pricing.net_paid_inc_ship),
              resolve_decimal(CS_PRICING_NET_PAID_INC_SHIP_TAX, catalog_sales.cs_pricing.net_paid_inc_ship_tax),
              resolve_decimal(CS_PRICING_NET_PROFIT, catalog_sales.cs_pricing.net_profit));
        }

        if (was_returned != 0) {
          catalog_returns_builder.append_row(
              resolve_key(CR_RETURNED_DATE_SK, catalog_returns.cr_returned_date_sk),
              resolve_key(CR_RETURNED_TIME_SK, catalog_returns.cr_returned_time_sk), catalog_returns.cr_item_sk,
              resolve_key(CR_REFUNDED_CUSTOMER_SK, catalog_returns.cr_refunded_customer_sk),
              resolve_key(CR_REFUNDED_CDEMO_SK, catalog_returns.cr_refunded_cdemo_sk),
              resolve_key(CR_REFUNDED_HDEMO_SK, catalog_returns.cr_refunded_hdemo_sk),
              resolve_key(CR_REFUNDED_ADDR_SK, catalog_returns.cr_refunded_addr_sk),
              resolve_key(CR_RETURNING_CUSTOMER_SK, catalog_returns.cr_returning_customer_sk),
              resolve_key(CR_RETURNING_CDEMO_SK, catalog_returns.cr_returning_cdemo_sk),
              resolve_key(CR_RETURNING_HDEMO_SK, catalog_returns.cr_returning_hdemo_sk),
              resolve_key(CR_RETURNING_ADDR_SK, catalog_returns.cr_returning_addr_sk),
              resolve_key(CR_CALL_CENTER_SK, catalog_returns.cr_call_center_sk),
              resolve_key(CR_CATALOG_PAGE_SK, catalog_returns.cr_catalog_page_sk),
              resolve_key(CR_SHIP_MODE_SK, catalog_returns.cr_ship_mode_sk),
              resolve_key(CR_WAREHOUSE_SK, catalog_returns.cr_warehouse_sk),
              resolve_key(CR_REASON_SK, catalog_returns.cr_reason_sk), catalog_returns.cr_order_number,
              resolve_integer(CR_PRICING_QUANTITY, catalog_returns.cr_pricing.quantity),
              resolve_decimal(CR_PRICING_NET_PAID, catalog_returns.cr_pricing.net_paid),
              resolve_decimal(CR_PRICING_EXT_TAX, catalog_returns.cr_pricing.ext_tax),
              resolve_decimal(CR_PRICING_NET_PAID_INC_TAX, catalog_returns.cr_pricing.net_paid_inc_tax),
              resolve_decimal(CR_PRICING_FEE, catalog_returns.cr_pricing.fee),
              resolve_decimal(CR_PRICING_EXT_SHIP_COST, catalog_returns.cr_pricing.ext_ship_cost),
              resolve_decimal(CR_PRICING_REFUNDED_CASH, catalog_returns.cr_pricing.refunded_cash),
              resolve_decimal(CR_PRICING_REVERSED_CHARGE, catalog_returns.cr_pricing.reversed_charge),
              resolve_decimal(CR_PRICING_STORE_CREDIT, catalog_returns.cr_pricing.store_credit),
              resolve_decimal(CR_PRICING_NET_LOSS, catalog_returns.cr_pricing.net_loss));

          if (catalog_returns_builder.row_count() == static_cast<size_t>(max_rows)) {
            break;
          }
        }
      }
    }
    tpcds_row_stop(CATALOG_SALES);
    if (catalog_returns_builder.row_count() == static_cast<size_t>(max_rows)) {
      break;
    }
  }

  return {catalog_sales_builder.finish_table(), catalog_returns_builder.finish_table()};
}

std::shared_ptr<Table> TpcdsTableGenerator::generate_customer_address(ds_key_t max_rows) const {
  auto [customer_address_first, customer_address_count] = prepare_for_table(CUSTOMER_ADDRESS);
  customer_address_count = std::min(customer_address_count, max_rows);

  auto customer_address_builder =
      TableBuilder{_benchmark_config->chunk_size, customer_address_column_types, customer_address_column_names,
                   static_cast<ChunkOffset>(customer_address_count)};

  for (auto i = ds_key_t{0}; i < customer_address_count; i++) {
    const auto customer_address =
        call_dbgen_mk<W_CUSTOMER_ADDRESS_TBL, &mk_w_customer_address, CUSTOMER_ADDRESS>(customer_address_first + i);

    customer_address_builder.append_row(
        customer_address.ca_addr_sk, customer_address.ca_addr_id,
        resolve_string(CA_ADDRESS_STREET_NUM, pmr_string{std::to_string(customer_address.ca_address.street_num)}),
        resolve_street_name(CA_ADDRESS_STREET_NAME1, customer_address.ca_address),
        resolve_string(CA_ADDRESS_STREET_TYPE, customer_address.ca_address.street_type),
        resolve_string(CA_ADDRESS_SUITE_NUM, customer_address.ca_address.suite_num),
        resolve_string(CA_ADDRESS_CITY, customer_address.ca_address.city),
        resolve_string(CA_ADDRESS_COUNTY, customer_address.ca_address.county),
        resolve_string(CA_ADDRESS_STATE, customer_address.ca_address.state),
        resolve_string(CA_ADDRESS_ZIP, zip_to_string(customer_address.ca_address.zip)),
        resolve_string(CA_ADDRESS_COUNTRY, customer_address.ca_address.country),
        resolve_gmt_offset(CA_ADDRESS_GMT_OFFSET, customer_address.ca_address.gmt_offset),
        resolve_string(CA_LOCATION_TYPE, customer_address.ca_location_type));
  }

  return customer_address_builder.finish_table();
}

std::shared_ptr<Table> TpcdsTableGenerator::generate_customer(ds_key_t max_rows) const {
  auto [customer_first, customer_count] = prepare_for_table(CUSTOMER);
  customer_count = std::min(customer_count, max_rows);

  auto customer_builder = TableBuilder{_benchmark_config->chunk_size, customer_column_types, customer_column_names,
                                       static_cast<ChunkOffset>(customer_count)};

  for (auto i = ds_key_t{0}; i < customer_count; i++) {
    const auto customer = call_dbgen_mk<W_CUSTOMER_TBL, &mk_w_customer, CUSTOMER>(customer_first + i);

    customer_builder.append_row(
        customer.c_customer_sk, customer.c_customer_id, resolve_key(C_CURRENT_CDEMO_SK, customer.c_current_cdemo_sk),
        resolve_key(C_CURRENT_HDEMO_SK, customer.c_current_hdemo_sk),
        resolve_key(C_CURRENT_ADDR_SK, customer.c_current_addr_sk),
        resolve_integer(C_FIRST_SHIPTO_DATE_ID, customer.c_first_shipto_date_id),
        resolve_integer(C_FIRST_SALES_DATE_ID, customer.c_first_sales_date_id),
        resolve_string(C_SALUTATION, customer.c_salutation), resolve_string(C_FIRST_NAME, customer.c_first_name),
        resolve_string(C_LAST_NAME, customer.c_last_name),
        resolve_string(C_PREFERRED_CUST_FLAG, boolean_to_string(customer.c_preferred_cust_flag)),
        resolve_integer(C_BIRTH_DAY, customer.c_birth_day), resolve_integer(C_BIRTH_MONTH, customer.c_birth_month),
        resolve_integer(C_BIRTH_YEAR, customer.c_birth_year), resolve_string(C_BIRTH_COUNTRY, customer.c_birth_country),
        resolve_string(C_LOGIN, customer.c_login), resolve_string(C_EMAIL_ADDRESS, customer.c_email_address),
        resolve_integer(C_LAST_REVIEW_DATE, customer.c_last_review_date));
  }

  return customer_builder.finish_table();
}

std::shared_ptr<Table> TpcdsTableGenerator::generate_customer_demographics(ds_key_t max_rows) const {
  auto [customer_demographics_first, customer_demographics_count] = prepare_for_table(CUSTOMER_DEMOGRAPHICS);
  customer_demographics_count = std::min(customer_demographics_count, max_rows);

  auto customer_demographics_builder =
      TableBuilder{_benchmark_config->chunk_size, customer_demographics_column_types,
                   customer_demographics_column_names, static_cast<ChunkOffset>(customer_demographics_count)};

  for (auto i = ds_key_t{0}; i < customer_demographics_count; i++) {
    const auto customer_demographics =
        call_dbgen_mk<W_CUSTOMER_DEMOGRAPHICS_TBL, &mk_w_customer_demographics, CUSTOMER_DEMOGRAPHICS>(
            customer_demographics_first + i);

    customer_demographics_builder.append_row(
        customer_demographics.cd_demo_sk, resolve_string(CD_GENDER, customer_demographics.cd_gender),
        resolve_string(CD_MARITAL_STATUS, customer_demographics.cd_marital_status),
        resolve_string(CD_EDUCATION_STATUS, customer_demographics.cd_education_status),
        resolve_integer(CD_PURCHASE_ESTIMATE, customer_demographics.cd_purchase_estimate),
        resolve_string(CD_CREDIT_RATING, customer_demographics.cd_credit_rating),
        resolve_integer(CD_DEP_COUNT, customer_demographics.cd_dep_count),
        resolve_integer(CD_DEP_EMPLOYED_COUNT, customer_demographics.cd_dep_employed_count),
        resolve_integer(CD_DEP_COLLEGE_COUNT, customer_demographics.cd_dep_college_count));
  }

  return customer_demographics_builder.finish_table();
}

std::shared_ptr<Table> TpcdsTableGenerator::generate_date_dim(ds_key_t max_rows) const {
  auto [date_first, date_count] = prepare_for_table(DATE);
  date_count = std::min(date_count, max_rows);

  auto date_builder = TableBuilder{_benchmark_config->chunk_size, date_column_types, date_column_names,
                                   static_cast<ChunkOffset>(date_count)};

  for (auto i = ds_key_t{0}; i < date_count; i++) {
    const auto date = call_dbgen_mk<W_DATE_TBL, &mk_w_date, DATE>(date_first + i);

    auto quarter_name = pmr_string{std::to_string(date.d_year) + "Q" + std::to_string(date.d_qoy)};

    date_builder.append_row(
        date.d_date_sk, date.d_date_id,

        resolve_date_id(D_DATE_SK, date.d_date_sk), resolve_integer(D_MONTH_SEQ, date.d_month_seq),
        resolve_integer(D_WEEK_SEQ, date.d_week_seq), resolve_integer(D_QUARTER_SEQ, date.d_quarter_seq),
        resolve_integer(D_YEAR, date.d_year), resolve_integer(D_DOW, date.d_dow), resolve_integer(D_MOY, date.d_moy),
        resolve_integer(D_DOM, date.d_dom), resolve_integer(D_QOY, date.d_qoy),
        resolve_integer(D_FY_YEAR, date.d_fy_year), resolve_integer(D_FY_QUARTER_SEQ, date.d_fy_quarter_seq),
        resolve_integer(D_FY_WEEK_SEQ, date.d_fy_week_seq), resolve_string(D_DAY_NAME, date.d_day_name),
        resolve_string(D_QUARTER_NAME, std::move(quarter_name)),
        resolve_string(D_HOLIDAY, boolean_to_string(date.d_holiday)),
        resolve_string(D_WEEKEND, boolean_to_string(date.d_weekend)),
        resolve_string(D_FOLLOWING_HOLIDAY, boolean_to_string(date.d_following_holiday)),
        resolve_integer(D_FIRST_DOM, date.d_first_dom), resolve_integer(D_LAST_DOM, date.d_last_dom),
        resolve_integer(D_SAME_DAY_LY, date.d_same_day_ly), resolve_integer(D_SAME_DAY_LQ, date.d_same_day_lq),
        resolve_string(D_CURRENT_DAY, boolean_to_string(date.d_current_day)),
        resolve_string(D_CURRENT_WEEK, boolean_to_string(date.d_current_week)),
        resolve_string(D_CURRENT_MONTH, boolean_to_string(date.d_current_month)),
        resolve_string(D_CURRENT_QUARTER, boolean_to_string(date.d_current_quarter)),
        resolve_string(D_CURRENT_YEAR, boolean_to_string(date.d_current_year)));
  }

  return date_builder.finish_table();
}

std::shared_ptr<Table> TpcdsTableGenerator::generate_household_demographics(ds_key_t max_rows) const {
  auto [household_demographics_first, household_demographics_count] = prepare_for_table(HOUSEHOLD_DEMOGRAPHICS);
  household_demographics_count = std::min(household_demographics_count, max_rows);

  auto household_demographics_builder =
      TableBuilder{_benchmark_config->chunk_size, household_demographics_column_types,
                   household_demographics_column_names, static_cast<ChunkOffset>(household_demographics_count)};

  for (auto i = ds_key_t{0}; i < household_demographics_count; i++) {
    const auto household_demographics =
        call_dbgen_mk<W_HOUSEHOLD_DEMOGRAPHICS_TBL, &mk_w_household_demographics, HOUSEHOLD_DEMOGRAPHICS>(
            household_demographics_first + i);

    household_demographics_builder.append_row(
        household_demographics.hd_demo_sk,

        resolve_key(HD_INCOME_BAND_ID, household_demographics.hd_income_band_id),
        resolve_string(HD_BUY_POTENTIAL, household_demographics.hd_buy_potential),
        resolve_integer(HD_DEP_COUNT, household_demographics.hd_dep_count),
        resolve_integer(HD_VEHICLE_COUNT, household_demographics.hd_vehicle_count));
  }

  return household_demographics_builder.finish_table();
}

std::shared_ptr<Table> TpcdsTableGenerator::generate_income_band(ds_key_t max_rows) const {
  auto [income_band_first, income_band_count] = prepare_for_table(INCOME_BAND);
  income_band_count = std::min(income_band_count, max_rows);

  auto income_band_builder = TableBuilder{_benchmark_config->chunk_size, income_band_column_types,
                                          income_band_column_names, static_cast<ChunkOffset>(income_band_count)};

  for (auto i = ds_key_t{0}; i < income_band_count; i++) {
    const auto income_band = call_dbgen_mk<W_INCOME_BAND_TBL, &mk_w_income_band, INCOME_BAND>(income_band_first + i);

    income_band_builder.append_row(income_band.ib_income_band_id,
                                   resolve_integer(IB_LOWER_BOUND, income_band.ib_lower_bound),
                                   resolve_integer(IB_UPPER_BOUND, income_band.ib_upper_bound));
  }

  return income_band_builder.finish_table();
}

std::shared_ptr<Table> TpcdsTableGenerator::generate_inventory(ds_key_t max_rows) const {
  auto [inventory_first, inventory_count] = prepare_for_table(INVENTORY);
  inventory_count = std::min(inventory_count, max_rows);

  auto inventory_builder = TableBuilder{_benchmark_config->chunk_size, inventory_column_types, inventory_column_names,
                                        static_cast<ChunkOffset>(inventory_count)};

  for (auto i = ds_key_t{0}; i < inventory_count; i++) {
    const auto inventory = call_dbgen_mk<W_INVENTORY_TBL, &mk_w_inventory, INVENTORY>(inventory_first + i);

    inventory_builder.append_row(inventory.inv_date_sk, inventory.inv_item_sk, inventory.inv_warehouse_sk,
                                 resolve_integer(INV_QUANTITY_ON_HAND, inventory.inv_quantity_on_hand));
  }

  return inventory_builder.finish_table();
}

std::shared_ptr<Table> TpcdsTableGenerator::generate_item(ds_key_t max_rows) const {
  auto [item_first, item_count] = prepare_for_table(ITEM);
  item_count = std::min(item_count, max_rows);

  auto item_builder = TableBuilder{_benchmark_config->chunk_size, item_column_types, item_column_names,
                                   static_cast<ChunkOffset>(item_count)};

  for (auto i = ds_key_t{0}; i < item_count; i++) {
    const auto item = call_dbgen_mk<W_ITEM_TBL, &mk_w_item, ITEM>(item_first + i);

    item_builder.append_row(
        item.i_item_sk, item.i_item_id, resolve_date_id(I_REC_START_DATE_ID, item.i_rec_start_date_id),
        resolve_date_id(I_REC_END_DATE_ID, item.i_rec_end_date_id), resolve_string(I_ITEM_DESC, item.i_item_desc),
        resolve_decimal(I_CURRENT_PRICE, item.i_current_price),
        resolve_decimal(I_WHOLESALE_COST, item.i_wholesale_cost), resolve_key(I_BRAND_ID, item.i_brand_id),
        resolve_string(I_BRAND, item.i_brand), resolve_key(I_CLASS_ID, item.i_class_id),
        resolve_string(I_CLASS, item.i_class), resolve_key(I_CATEGORY_ID, item.i_category_id),
        resolve_string(I_CATEGORY, item.i_category), resolve_key(I_MANUFACT_ID, item.i_manufact_id),
        resolve_string(I_MANUFACT, item.i_manufact), resolve_string(I_SIZE, item.i_size),
        resolve_string(I_FORMULATION, item.i_formulation), resolve_string(I_COLOR, item.i_color),
        resolve_string(I_UNITS, item.i_units), resolve_string(I_CONTAINER, item.i_container),
        resolve_key(I_MANAGER_ID, item.i_manager_id), resolve_string(I_PRODUCT_NAME, item.i_product_name));
  }

  return item_builder.finish_table();
}

std::shared_ptr<Table> TpcdsTableGenerator::generate_promotion(ds_key_t max_rows) const {
  auto [promotion_first, promotion_count] = prepare_for_table(PROMOTION);
  promotion_count = std::min(promotion_count, max_rows);

  auto promotion_builder = TableBuilder{_benchmark_config->chunk_size, promotion_column_types, promotion_column_names,
                                        static_cast<ChunkOffset>(promotion_count)};

  for (auto i = ds_key_t{0}; i < promotion_count; i++) {
    const auto promotion = call_dbgen_mk<W_PROMOTION_TBL, &mk_w_promotion, PROMOTION>(promotion_first + i);

    promotion_builder.append_row(
        promotion.p_promo_sk, promotion.p_promo_id, resolve_key(P_START_DATE_ID, promotion.p_start_date_id),
        resolve_key(P_END_DATE_ID, promotion.p_end_date_id), resolve_key(P_ITEM_SK, promotion.p_item_sk),
        resolve_decimal(P_COST, promotion.p_cost), resolve_integer(P_RESPONSE_TARGET, promotion.p_response_target),
        resolve_string(P_PROMO_NAME, promotion.p_promo_name),
        resolve_string(P_CHANNEL_DMAIL, boolean_to_string(promotion.p_channel_dmail)),
        resolve_string(P_CHANNEL_EMAIL, boolean_to_string(promotion.p_channel_email)),
        resolve_string(P_CHANNEL_CATALOG, boolean_to_string(promotion.p_channel_catalog)),
        resolve_string(P_CHANNEL_TV, boolean_to_string(promotion.p_channel_tv)),
        resolve_string(P_CHANNEL_RADIO, boolean_to_string(promotion.p_channel_radio)),
        resolve_string(P_CHANNEL_PRESS, boolean_to_string(promotion.p_channel_press)),
        resolve_string(P_CHANNEL_EVENT, boolean_to_string(promotion.p_channel_event)),
        resolve_string(P_CHANNEL_DEMO, boolean_to_string(promotion.p_channel_demo)),
        resolve_string(P_CHANNEL_DETAILS, promotion.p_channel_details), resolve_string(P_PURPOSE, promotion.p_purpose),
        resolve_string(P_DISCOUNT_ACTIVE, boolean_to_string(promotion.p_discount_active)));
  }

  return promotion_builder.finish_table();
}

std::shared_ptr<Table> TpcdsTableGenerator::generate_reason(ds_key_t max_rows) const {
  auto [reason_first, reason_count] = prepare_for_table(REASON);
  reason_count = std::min(reason_count, max_rows);

  auto reason_builder = TableBuilder{_benchmark_config->chunk_size, reason_column_types, reason_column_names,
                                     static_cast<ChunkOffset>(reason_count)};

  for (auto i = ds_key_t{0}; i < reason_count; i++) {
    const auto reason = call_dbgen_mk<W_REASON_TBL, &mk_w_reason, REASON>(reason_first + i);

    reason_builder.append_row(reason.r_reason_sk, reason.r_reason_id,
                              resolve_string(R_REASON_DESCRIPTION, reason.r_reason_description));
  }

  return reason_builder.finish_table();
}

std::shared_ptr<Table> TpcdsTableGenerator::generate_ship_mode(ds_key_t max_rows) const {
  auto [ship_mode_first, ship_mode_count] = prepare_for_table(SHIP_MODE);
  ship_mode_count = std::min(ship_mode_count, max_rows);

  auto ship_mode_builder = TableBuilder{_benchmark_config->chunk_size, ship_mode_column_types, ship_mode_column_names,
                                        static_cast<ChunkOffset>(ship_mode_count)};

  for (auto i = ds_key_t{0}; i < ship_mode_count; i++) {
    const auto ship_mode = call_dbgen_mk<W_SHIP_MODE_TBL, &mk_w_ship_mode, SHIP_MODE>(ship_mode_first + i);

    ship_mode_builder.append_row(ship_mode.sm_ship_mode_sk, ship_mode.sm_ship_mode_id,

                                 resolve_string(SM_TYPE, ship_mode.sm_type), resolve_string(SM_CODE, ship_mode.sm_code),
                                 resolve_string(SM_CARRIER, ship_mode.sm_carrier),
                                 resolve_string(SM_CONTRACT, ship_mode.sm_contract));
  }

  return ship_mode_builder.finish_table();
}

std::shared_ptr<Table> TpcdsTableGenerator::generate_store(ds_key_t max_rows) const {
  auto [store_first, store_count] = prepare_for_table(STORE);
  store_count = std::min(store_count, max_rows);

  auto store_builder = TableBuilder{_benchmark_config->chunk_size, store_column_types, store_column_names,
                                    static_cast<ChunkOffset>(store_count)};

  for (auto i = ds_key_t{0}; i < store_count; i++) {
    const auto store = call_dbgen_mk<W_STORE_TBL, &mk_w_store, STORE>(store_first + i);

    store_builder.append_row(
        store.store_sk, store.store_id,

        resolve_date_id(W_STORE_REC_START_DATE_ID, store.rec_start_date_id),
        resolve_date_id(W_STORE_REC_END_DATE_ID, store.rec_end_date_id),
        resolve_key(W_STORE_CLOSED_DATE_ID, store.closed_date_id), resolve_string(W_STORE_NAME, store.store_name),
        resolve_integer(W_STORE_EMPLOYEES, store.employees), resolve_integer(W_STORE_FLOOR_SPACE, store.floor_space),
        resolve_string(W_STORE_HOURS, store.hours), resolve_string(W_STORE_MANAGER, store.store_manager),
        resolve_integer(W_STORE_MARKET_ID, store.market_id),
        resolve_string(W_STORE_GEOGRAPHY_CLASS, store.geography_class),
        resolve_string(W_STORE_MARKET_DESC, store.market_desc),
        resolve_string(W_STORE_MARKET_MANAGER, store.market_manager),
        resolve_key(W_STORE_DIVISION_ID, store.division_id), resolve_string(W_STORE_DIVISION_NAME, store.division_name),
        resolve_key(W_STORE_COMPANY_ID, store.company_id), resolve_string(W_STORE_COMPANY_NAME, store.company_name),
        resolve_string(W_STORE_ADDRESS_STREET_NUM, pmr_string{std::to_string(store.address.street_num)}),
        resolve_street_name(W_STORE_ADDRESS_STREET_NAME1, store.address),
        resolve_string(W_STORE_ADDRESS_STREET_TYPE, store.address.street_type),
        resolve_string(W_STORE_ADDRESS_SUITE_NUM, store.address.suite_num),
        resolve_string(W_STORE_ADDRESS_CITY, store.address.city),
        resolve_string(W_STORE_ADDRESS_COUNTY, store.address.county),
        resolve_string(W_STORE_ADDRESS_STATE, store.address.state),
        resolve_string(W_STORE_ADDRESS_ZIP, zip_to_string(store.address.zip)),
        resolve_string(W_STORE_ADDRESS_COUNTRY, store.address.country),
        resolve_gmt_offset(W_STORE_ADDRESS_GMT_OFFSET, store.address.gmt_offset),
        resolve_decimal(W_STORE_TAX_PERCENTAGE, store.dTaxPercentage));
  }

  return store_builder.finish_table();
}

std::pair<std::shared_ptr<Table>, std::shared_ptr<Table>> TpcdsTableGenerator::generate_store_sales_and_returns(
    ds_key_t max_rows) const {
  auto [store_sales_first, store_sales_count] = prepare_for_table(STORE_SALES);

  auto store_sales_builder = TableBuilder{_benchmark_config->chunk_size, store_sales_column_types,
                                          store_sales_column_names, static_cast<ChunkOffset>(store_sales_count)};
  auto store_returns_builder =
      TableBuilder{_benchmark_config->chunk_size, store_returns_column_types, store_returns_column_names};

  for (auto i = ds_key_t{0}; i < store_sales_count; i++) {
    auto store_sales = W_STORE_SALES_TBL{};
    auto store_returns = W_STORE_RETURNS_TBL{};

    // modified call to mk_w_store_sales(&store_sales, store_sales_first + i, &store_returns, &was_returned)
    {
      mk_w_store_sales_master(&store_sales, store_sales_first + i, 0);

      int n_lineitems;
      genrand_integer(&n_lineitems, DIST_UNIFORM, 8, 16, 0, SS_TICKET_NUMBER);
      for (auto j = 1; j <= n_lineitems; j++) {
        int was_returned = 0;
        mk_w_store_sales_detail(&store_sales, 0, &store_returns, &was_returned);

        if (store_sales_builder.row_count() < static_cast<size_t>(max_rows)) {
          store_sales_builder.append_row(
              resolve_key(SS_SOLD_DATE_SK, store_sales.ss_sold_date_sk),
              resolve_key(SS_SOLD_TIME_SK, store_sales.ss_sold_time_sk), store_sales.ss_sold_item_sk,
              resolve_key(SS_SOLD_CUSTOMER_SK, store_sales.ss_sold_customer_sk),
              resolve_key(SS_SOLD_CDEMO_SK, store_sales.ss_sold_cdemo_sk),
              resolve_key(SS_SOLD_HDEMO_SK, store_sales.ss_sold_hdemo_sk),
              resolve_key(SS_SOLD_ADDR_SK, store_sales.ss_sold_addr_sk),
              resolve_key(SS_SOLD_STORE_SK, store_sales.ss_sold_store_sk),
              resolve_key(SS_SOLD_PROMO_SK, store_sales.ss_sold_promo_sk), store_sales.ss_ticket_number,
              resolve_integer(SS_PRICING_QUANTITY, store_sales.ss_pricing.quantity),
              resolve_decimal(SS_PRICING_WHOLESALE_COST, store_sales.ss_pricing.wholesale_cost),
              resolve_decimal(SS_PRICING_LIST_PRICE, store_sales.ss_pricing.list_price),
              resolve_decimal(SS_PRICING_SALES_PRICE, store_sales.ss_pricing.sales_price),
              resolve_decimal(SS_PRICING_COUPON_AMT, store_sales.ss_pricing.coupon_amt),
              resolve_decimal(SS_PRICING_EXT_SALES_PRICE, store_sales.ss_pricing.ext_sales_price),
              resolve_decimal(SS_PRICING_EXT_WHOLESALE_COST, store_sales.ss_pricing.ext_wholesale_cost),
              resolve_decimal(SS_PRICING_EXT_LIST_PRICE, store_sales.ss_pricing.ext_list_price),
              resolve_decimal(SS_PRICING_EXT_TAX, store_sales.ss_pricing.ext_tax),
              resolve_decimal(SS_PRICING_COUPON_AMT, store_sales.ss_pricing.coupon_amt),
              resolve_decimal(SS_PRICING_NET_PAID, store_sales.ss_pricing.net_paid),
              resolve_decimal(SS_PRICING_NET_PAID_INC_TAX, store_sales.ss_pricing.net_paid_inc_tax),
              resolve_decimal(SS_PRICING_NET_PROFIT, store_sales.ss_pricing.net_profit));
          // dsdgen prints coupon_amt twice, so we do too...
        }

        if (was_returned != 0) {
          store_returns_builder.append_row(
              resolve_key(SR_RETURNED_DATE_SK, store_returns.sr_returned_date_sk),
              resolve_key(SR_RETURNED_TIME_SK, store_returns.sr_returned_time_sk), store_returns.sr_item_sk,
              resolve_key(SR_CUSTOMER_SK, store_returns.sr_customer_sk),
              resolve_key(SR_CDEMO_SK, store_returns.sr_cdemo_sk), resolve_key(SR_HDEMO_SK, store_returns.sr_hdemo_sk),
              resolve_key(SR_ADDR_SK, store_returns.sr_addr_sk), resolve_key(SR_STORE_SK, store_returns.sr_store_sk),
              resolve_key(SR_REASON_SK, store_returns.sr_reason_sk), store_returns.sr_ticket_number,
              resolve_integer(SR_PRICING_QUANTITY, store_returns.sr_pricing.quantity),
              resolve_decimal(SR_PRICING_NET_PAID, store_returns.sr_pricing.net_paid),
              resolve_decimal(SR_PRICING_EXT_TAX, store_returns.sr_pricing.ext_tax),
              resolve_decimal(SR_PRICING_NET_PAID_INC_TAX, store_returns.sr_pricing.net_paid_inc_tax),
              resolve_decimal(SR_PRICING_FEE, store_returns.sr_pricing.fee),
              resolve_decimal(SR_PRICING_EXT_SHIP_COST, store_returns.sr_pricing.ext_ship_cost),
              resolve_decimal(SR_PRICING_REFUNDED_CASH, store_returns.sr_pricing.refunded_cash),
              resolve_decimal(SR_PRICING_REVERSED_CHARGE, store_returns.sr_pricing.reversed_charge),
              resolve_decimal(SR_PRICING_STORE_CREDIT, store_returns.sr_pricing.store_credit),
              resolve_decimal(SR_PRICING_NET_LOSS, store_returns.sr_pricing.net_loss));
          if (store_returns_builder.row_count() == static_cast<size_t>(max_rows)) {
            break;
          }
        }
      }
    }
    tpcds_row_stop(STORE_SALES);
    if (store_returns_builder.row_count() == static_cast<size_t>(max_rows)) {
      break;
    }
  }

  return {store_sales_builder.finish_table(), store_returns_builder.finish_table()};
}

std::shared_ptr<Table> TpcdsTableGenerator::generate_time_dim(ds_key_t max_rows) const {
  auto [time_first, time_count] = prepare_for_table(TIME);
  time_count = std::min(time_count, max_rows);

  auto time_builder = TableBuilder{_benchmark_config->chunk_size, time_column_types, time_column_names,
                                   static_cast<ChunkOffset>(time_count)};

  for (auto i = ds_key_t{0}; i < time_count; i++) {
    const auto time = call_dbgen_mk<W_TIME_TBL, &mk_w_time, TIME>(time_first + i);

    time_builder.append_row(time.t_time_sk, time.t_time_id, resolve_integer(T_TIME, time.t_time),
                            resolve_integer(T_HOUR, time.t_hour), resolve_integer(T_MINUTE, time.t_minute),
                            resolve_integer(T_SECOND, time.t_second), resolve_string(T_AM_PM, time.t_am_pm),
                            resolve_string(T_SHIFT, time.t_shift), resolve_string(T_SUB_SHIFT, time.t_sub_shift),
                            resolve_string(T_MEAL_TIME, time.t_meal_time));
  }

  return time_builder.finish_table();
}

std::shared_ptr<Table> TpcdsTableGenerator::generate_warehouse(ds_key_t max_rows) const {
  auto [warehouse_first, warehouse_count] = prepare_for_table(WAREHOUSE);
  warehouse_count = std::min(warehouse_count, max_rows);

  auto warehouse_builder = TableBuilder{_benchmark_config->chunk_size, warehouse_column_types, warehouse_column_names,
                                        static_cast<ChunkOffset>(warehouse_count)};

  for (auto i = ds_key_t{0}; i < warehouse_count; i++) {
    const auto warehouse = call_dbgen_mk<W_WAREHOUSE_TBL, &mk_w_warehouse, WAREHOUSE>(warehouse_first + i);

    warehouse_builder.append_row(
        warehouse.w_warehouse_sk, warehouse.w_warehouse_id,

        resolve_string(W_WAREHOUSE_NAME, warehouse.w_warehouse_name),
        resolve_integer(W_WAREHOUSE_SQ_FT, warehouse.w_warehouse_sq_ft),
        resolve_string(W_ADDRESS_STREET_NUM, pmr_string{std::to_string(warehouse.w_address.street_num)}),
        resolve_street_name(W_ADDRESS_STREET_NAME1, warehouse.w_address),
        resolve_string(W_ADDRESS_STREET_TYPE, warehouse.w_address.street_type),
        resolve_string(W_ADDRESS_SUITE_NUM, warehouse.w_address.suite_num),
        resolve_string(W_ADDRESS_CITY, warehouse.w_address.city),
        resolve_string(W_ADDRESS_COUNTY, warehouse.w_address.county),
        resolve_string(W_ADDRESS_STATE, warehouse.w_address.state),
        resolve_string(W_ADDRESS_ZIP, zip_to_string(warehouse.w_address.zip)),
        resolve_string(W_ADDRESS_COUNTRY, warehouse.w_address.country),
        resolve_gmt_offset(W_ADDRESS_GMT_OFFSET, warehouse.w_address.gmt_offset));
  }

  return warehouse_builder.finish_table();
}

std::shared_ptr<Table> TpcdsTableGenerator::generate_web_page(ds_key_t max_rows) const {
  auto [web_page_first, web_page_count] = prepare_for_table(WEB_PAGE);
  web_page_count = std::min(web_page_count, max_rows);

  auto web_page_builder = TableBuilder{_benchmark_config->chunk_size, web_page_column_types, web_page_column_names,
                                       static_cast<ChunkOffset>(web_page_count)};

  for (auto i = ds_key_t{0}; i < web_page_count; i++) {
    const auto web_page = call_dbgen_mk<W_WEB_PAGE_TBL, &mk_w_web_page, WEB_PAGE>(web_page_first + i);

    web_page_builder.append_row(
        web_page.wp_page_sk, web_page.wp_page_id, resolve_date_id(WP_REC_START_DATE_ID, web_page.wp_rec_start_date_id),
        resolve_date_id(WP_REC_END_DATE_ID, web_page.wp_rec_end_date_id),
        resolve_key(WP_CREATION_DATE_SK, web_page.wp_creation_date_sk),
        resolve_key(WP_ACCESS_DATE_SK, web_page.wp_access_date_sk),
        resolve_string(WP_AUTOGEN_FLAG, boolean_to_string(web_page.wp_autogen_flag)),
        resolve_key(WP_CUSTOMER_SK, web_page.wp_customer_sk), resolve_string(WP_URL, web_page.wp_url),
        resolve_string(WP_TYPE, web_page.wp_type), resolve_integer(WP_CHAR_COUNT, web_page.wp_char_count),
        resolve_integer(WP_LINK_COUNT, web_page.wp_link_count),
        resolve_integer(WP_IMAGE_COUNT, web_page.wp_image_count),
        resolve_integer(WP_MAX_AD_COUNT, web_page.wp_max_ad_count));
  }

  return web_page_builder.finish_table();
}

std::pair<std::shared_ptr<Table>, std::shared_ptr<Table>> TpcdsTableGenerator::generate_web_sales_and_returns(
    ds_key_t max_rows) const {
  auto [web_sales_first, web_sales_count] = prepare_for_table(WEB_SALES);

  auto web_sales_builder = TableBuilder{_benchmark_config->chunk_size, web_sales_column_types, web_sales_column_names,
                                        static_cast<ChunkOffset>(web_sales_count)};
  auto web_returns_builder =
      TableBuilder{_benchmark_config->chunk_size, web_returns_column_types, web_returns_column_names};

  for (auto i = ds_key_t{0}; i < web_sales_count; i++) {
    auto web_sales = W_WEB_SALES_TBL{};
    auto web_returns = W_WEB_RETURNS_TBL{};

    // modified call to mk_w_web_sales(&web_sales, web_sales_first + i, &web_returns, &was_returned);
    {
      mk_w_web_sales_master(&web_sales, web_sales_first + i, 0);

      int n_lineitems;
      genrand_integer(&n_lineitems, DIST_UNIFORM, 8, 16, 9, WS_ORDER_NUMBER);
      for (auto j = 1; j <= n_lineitems; j++) {
        int was_returned = 0;
        mk_w_web_sales_detail(&web_sales, 0, &web_returns, &was_returned, 0);

        if (web_sales_builder.row_count() < static_cast<size_t>(max_rows)) {
          web_sales_builder.append_row(
              resolve_key(WS_SOLD_DATE_SK, web_sales.ws_sold_date_sk),
              resolve_key(WS_SOLD_TIME_SK, web_sales.ws_sold_time_sk),
              resolve_key(WS_SHIP_DATE_SK, web_sales.ws_ship_date_sk), web_sales.ws_item_sk,
              resolve_key(WS_BILL_CUSTOMER_SK, web_sales.ws_bill_customer_sk),
              resolve_key(WS_BILL_CDEMO_SK, web_sales.ws_bill_cdemo_sk),
              resolve_key(WS_BILL_HDEMO_SK, web_sales.ws_bill_hdemo_sk),
              resolve_key(WS_BILL_ADDR_SK, web_sales.ws_bill_addr_sk),
              resolve_key(WS_SHIP_CUSTOMER_SK, web_sales.ws_ship_customer_sk),
              resolve_key(WS_SHIP_CDEMO_SK, web_sales.ws_ship_cdemo_sk),
              resolve_key(WS_SHIP_HDEMO_SK, web_sales.ws_ship_hdemo_sk),
              resolve_key(WS_SHIP_ADDR_SK, web_sales.ws_ship_addr_sk),
              resolve_key(WS_WEB_PAGE_SK, web_sales.ws_web_page_sk),
              resolve_key(WS_WEB_SITE_SK, web_sales.ws_web_site_sk),
              resolve_key(WS_SHIP_MODE_SK, web_sales.ws_ship_mode_sk),
              resolve_key(WS_WAREHOUSE_SK, web_sales.ws_warehouse_sk), resolve_key(WS_PROMO_SK, web_sales.ws_promo_sk),
              web_sales.ws_order_number, resolve_integer(WS_PRICING_QUANTITY, web_sales.ws_pricing.quantity),
              resolve_decimal(WS_PRICING_WHOLESALE_COST, web_sales.ws_pricing.wholesale_cost),
              resolve_decimal(WS_PRICING_LIST_PRICE, web_sales.ws_pricing.list_price),
              resolve_decimal(WS_PRICING_SALES_PRICE, web_sales.ws_pricing.sales_price),
              resolve_decimal(WS_PRICING_EXT_DISCOUNT_AMT, web_sales.ws_pricing.ext_discount_amt),
              resolve_decimal(WS_PRICING_EXT_SALES_PRICE, web_sales.ws_pricing.ext_sales_price),
              resolve_decimal(WS_PRICING_EXT_WHOLESALE_COST, web_sales.ws_pricing.ext_wholesale_cost),
              resolve_decimal(WS_PRICING_EXT_LIST_PRICE, web_sales.ws_pricing.ext_list_price),
              resolve_decimal(WS_PRICING_EXT_TAX, web_sales.ws_pricing.ext_tax),
              resolve_decimal(WS_PRICING_COUPON_AMT, web_sales.ws_pricing.coupon_amt),
              resolve_decimal(WS_PRICING_EXT_SHIP_COST, web_sales.ws_pricing.ext_ship_cost),
              resolve_decimal(WS_PRICING_NET_PAID, web_sales.ws_pricing.net_paid),
              resolve_decimal(WS_PRICING_NET_PAID_INC_TAX, web_sales.ws_pricing.net_paid_inc_tax),
              resolve_decimal(WS_PRICING_NET_PAID_INC_SHIP, web_sales.ws_pricing.net_paid_inc_ship),
              resolve_decimal(WS_PRICING_NET_PAID_INC_SHIP_TAX, web_sales.ws_pricing.net_paid_inc_ship_tax),
              resolve_decimal(WS_PRICING_NET_PROFIT, web_sales.ws_pricing.net_profit));
        }

        if (was_returned != 0) {
          web_returns_builder.append_row(
              resolve_key(WR_RETURNED_DATE_SK, web_returns.wr_returned_date_sk),
              resolve_key(WR_RETURNED_TIME_SK, web_returns.wr_returned_time_sk), web_returns.wr_item_sk,
              resolve_key(WR_REFUNDED_CUSTOMER_SK, web_returns.wr_refunded_customer_sk),
              resolve_key(WR_REFUNDED_CDEMO_SK, web_returns.wr_refunded_cdemo_sk),
              resolve_key(WR_REFUNDED_HDEMO_SK, web_returns.wr_refunded_hdemo_sk),
              resolve_key(WR_REFUNDED_ADDR_SK, web_returns.wr_refunded_addr_sk),
              resolve_key(WR_RETURNING_CUSTOMER_SK, web_returns.wr_returning_customer_sk),
              resolve_key(WR_RETURNING_CDEMO_SK, web_returns.wr_returning_cdemo_sk),
              resolve_key(WR_RETURNING_HDEMO_SK, web_returns.wr_returning_hdemo_sk),
              resolve_key(WR_RETURNING_ADDR_SK, web_returns.wr_returning_addr_sk),
              resolve_key(WR_WEB_PAGE_SK, web_returns.wr_web_page_sk),
              resolve_key(WR_REASON_SK, web_returns.wr_reason_sk), web_returns.wr_order_number,
              resolve_integer(WR_PRICING_QUANTITY, web_returns.wr_pricing.quantity),
              resolve_decimal(WR_PRICING_NET_PAID, web_returns.wr_pricing.net_paid),
              resolve_decimal(WR_PRICING_EXT_TAX, web_returns.wr_pricing.ext_tax),
              resolve_decimal(WR_PRICING_NET_PAID_INC_TAX, web_returns.wr_pricing.net_paid_inc_tax),
              resolve_decimal(WR_PRICING_FEE, web_returns.wr_pricing.fee),
              resolve_decimal(WR_PRICING_EXT_SHIP_COST, web_returns.wr_pricing.ext_ship_cost),
              resolve_decimal(WR_PRICING_REFUNDED_CASH, web_returns.wr_pricing.refunded_cash),
              resolve_decimal(WR_PRICING_REVERSED_CHARGE, web_returns.wr_pricing.reversed_charge),
              resolve_decimal(WR_PRICING_STORE_CREDIT, web_returns.wr_pricing.store_credit),
              resolve_decimal(WR_PRICING_NET_LOSS, web_returns.wr_pricing.net_loss));
          if (web_returns_builder.row_count() == static_cast<size_t>(max_rows)) {
            break;
          }
        }
      }
    }
    tpcds_row_stop(WEB_SALES);

    if (web_returns_builder.row_count() == static_cast<size_t>(max_rows)) {
      break;
    }
  }

  return {web_sales_builder.finish_table(), web_returns_builder.finish_table()};
}

std::shared_ptr<Table> TpcdsTableGenerator::generate_web_site(ds_key_t max_rows) const {
  auto [web_site_first, web_site_count] = prepare_for_table(WEB_SITE);
  web_site_count = std::min(web_site_count, max_rows);

  auto web_site_builder = TableBuilder{_benchmark_config->chunk_size, web_site_column_types, web_site_column_names,
                                       static_cast<ChunkOffset>(web_site_count)};

  auto web_site = W_WEB_SITE_TBL{};
  static_assert(sizeof(web_site.web_class) == 51);
  std::snprintf(web_site.web_class, sizeof(web_site.web_class), "%s", "Unknown");
  for (auto i = ds_key_t{0}; i < web_site_count; i++) {
    // mk_w_web_site needs a pointer to the previous result because it expects values set previously to still be there
    mk_w_web_site(&web_site, web_site_first + i);
    tpcds_row_stop(WEB_SITE);

    web_site_builder.append_row(
        web_site.web_site_sk, web_site.web_site_id,
        resolve_date_id(WEB_REC_START_DATE_ID, web_site.web_rec_start_date_id),
        resolve_date_id(WEB_REC_END_DATE_ID, web_site.web_rec_end_date_id), resolve_string(WEB_NAME, web_site.web_name),
        resolve_key(WEB_OPEN_DATE, web_site.web_open_date), resolve_key(WEB_CLOSE_DATE, web_site.web_close_date),
        resolve_string(WEB_CLASS, web_site.web_class), resolve_string(WEB_MANAGER, web_site.web_manager),
        resolve_integer(WEB_MARKET_ID, web_site.web_market_id),
        resolve_string(WEB_MARKET_CLASS, web_site.web_market_class),
        resolve_string(WEB_MARKET_DESC, web_site.web_market_desc),
        resolve_string(WEB_MARKET_MANAGER, web_site.web_market_manager),
        resolve_integer(WEB_COMPANY_ID, web_site.web_company_id),
        resolve_string(WEB_COMPANY_NAME, web_site.web_company_name),
        resolve_string(WEB_ADDRESS_STREET_NUM, pmr_string{std::to_string(web_site.web_address.street_num)}),
        resolve_street_name(WEB_ADDRESS_STREET_NAME1, web_site.web_address),
        resolve_string(WEB_ADDRESS_STREET_TYPE, web_site.web_address.street_type),
        resolve_string(WEB_ADDRESS_SUITE_NUM, web_site.web_address.suite_num),
        resolve_string(WEB_ADDRESS_CITY, web_site.web_address.city),
        resolve_string(WEB_ADDRESS_COUNTY, web_site.web_address.county),
        resolve_string(WEB_ADDRESS_STATE, web_site.web_address.state),
        resolve_string(WEB_ADDRESS_ZIP, zip_to_string(web_site.web_address.zip)),
        resolve_string(WEB_ADDRESS_COUNTRY, web_site.web_address.country),
        resolve_gmt_offset(WEB_ADDRESS_GMT_OFFSET, web_site.web_address.gmt_offset),
        resolve_decimal(WEB_TAX_PERCENTAGE, web_site.web_tax_percentage));
  }

  return web_site_builder.finish_table();
}

}  // namespace opossum
