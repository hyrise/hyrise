#include "tpcds_table_generator.hpp"

extern "C" {
#include <genrand.h>
#include <parallel.h>
#include <r_params.h>
#include <tables.h>
#include <tdefs.h>
#include <w_call_center.h>
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
auto call_dbgen_mk(table_func_t tdef_functions, ds_key_t index, int table_id) {
  auto tpcds_row = TpcdsRow{};
  tdef_functions.builder(&tpcds_row, index);
  tpcds_row_stop(table_id);
  return tpcds_row;
}

auto prepare_for_table(int table_id) {
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

  return std::pair{k_first_row, k_row_count};
}

// clang-format off
// TODO(pascal): add cc_address(ds_addr_t) and cc_tax_percentage(decimal_t)
// corresponding column types in struct CALL_CENTER_TBL:      ds_key_t,            char [RS_BKEY + 1],  ds_key_t,               ds_key_t,             ds_key_t,            ds_key_t,          char [RS_CC_NAME + 1], char *,     int,            int,         char *,     char [RS_CC_MANAGER + 1], int,            char [RS_CC_MARKET_CLASS + 1], char [RS_CC_MARKET_DESC + 1], char [RS_CC_MARKET_MANAGER + 1], int,              char [RS_CC_DIVISION_NAME + 1], int,                char [RS_CC_COMPANY_NAME + 1], ds_addr_t,    decimal_t             // NOLINT
const auto call_center_column_types = boost::hana::tuple<     int64_t,             pmr_string,          int64_t,                int64_t,              int64_t,             int64_t,           pmr_string,            pmr_string, int32_t,        int32_t,     pmr_string, pmr_string,               int32_t,        pmr_string,                    pmr_string,                   pmr_string,                      int32_t,          pmr_string,                     int32_t,            pmr_string>();  // NOLINT      ?,            ?>();                 // NOLINT
const auto call_center_column_names = boost::hana::make_tuple("cc_call_center_sk", "cc_call_center_id", "cc_rec_start_date_id", "cc_rec_end_date_id", "cc_closed_date_id", "cc_open_date_id", "cc_name",             "cc_class", "cc_employees", "cc_sq_ft",  "cc_hours", "cc_manager",             "cc_market_id", "cc_market_class",             "cc_market_desc",             "cc_market_manager",             "cc_division_id", "cc_division_name",             "cc_company",       "cc_company_name");  //        "cc_address", "cc_tax_percentage"); // NOLINT
// clang-format on
}  // namespace

namespace opossum {

TpcdsTableGenerator::TpcdsTableGenerator(float scale_factor, uint32_t chunk_size)
    : AbstractTableGenerator(create_benchmark_config_with_chunk_size(chunk_size)), _scale_factor(scale_factor) {}

TpcdsTableGenerator::TpcdsTableGenerator(float scale_factor, const std::shared_ptr<BenchmarkConfig>& benchmark_config)
    : AbstractTableGenerator(benchmark_config), _scale_factor(scale_factor) {}

std::unordered_map<std::string, BenchmarkTableInfo> TpcdsTableGenerator::generate() {
  set_rng_seed(0);

  // call center

  const auto [call_center_first, call_center_count] = prepare_for_table(CALL_CENTER);

  auto call_center_builder =
      TableBuilder{_benchmark_config->chunk_size, call_center_column_types, call_center_column_names, UseMvcc::Yes,
                   static_cast<size_t>(call_center_count)};

  for (auto i = ds_key_t{0}; i < call_center_count; i++) {
    const auto tdef_functions = getTdefFunctionsByNumber(CALL_CENTER);
    auto call_center = call_dbgen_mk<CALL_CENTER_TBL>(*tdef_functions, call_center_first + i, CALL_CENTER);
    call_center_builder.append_row(
        static_cast<int64_t>(call_center.cc_call_center_sk), call_center.cc_call_center_id,
        static_cast<int64_t>(call_center.cc_rec_start_date_id), static_cast<int64_t>(call_center.cc_rec_end_date_id),
        static_cast<int64_t>(call_center.cc_closed_date_id), static_cast<int64_t>(call_center.cc_open_date_id),
        call_center.cc_name, call_center.cc_class, static_cast<int64_t>(call_center.cc_employees),
        static_cast<int64_t>(call_center.cc_sq_ft), call_center.cc_hours, call_center.cc_manager,
        static_cast<int64_t>(call_center.cc_market_id), call_center.cc_market_class, call_center.cc_market_desc,
        call_center.cc_market_manager, static_cast<int64_t>(call_center.cc_division_id), call_center.cc_division_name,
        static_cast<int64_t>(call_center.cc_company), call_center.cc_company_name);
  }

  // end - call center

  // TODO(pascal): dbgen cleanup?

  auto table_info_by_name = std::unordered_map<std::string, BenchmarkTableInfo>();

  table_info_by_name["callcenter"].table = call_center_builder.finish_table();

  return table_info_by_name;
}

}  // namespace opossum
