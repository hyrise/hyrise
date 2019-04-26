#include "tpcds_table_generator.hpp"

#include <genrand.h>
#include <parallel.h>
#include <r_params.h>
#include <tables.h>
#include <tdefs.h>

namespace {

void set_rng_seed(int rng_seed) {
  auto rng_seed_string = std::string("RNGSEED");
  auto rng_seed_value_string = std::to_string(rng_seed);
  set_int(rng_seed_string.data(), rng_seed_value_string.data());
  init_rand();
}

void generate_tables() {
  // CALL_CENTER is the first table - iterate over all tables
  for (auto table_id = CALL_CENTER; table_id < MAX_TABLE; table_id++) {
    auto k_row_count = ds_key_t{};
    auto k_first_row = ds_key_t{};

    split_work(table_id, &k_first_row, &k_row_count);

    // gen_tbl (int table_id, ds_key_t kFirstRow, ds_key_t kRowCount)
    {
      auto& tdef_functions = *getTdefFunctionsByNumber(table_id); //  see tdef_functions.c

      if (getSimpleTdefsByNumber(table_id)->flags & FL_SMALL){
        resetCountCount();
      }

      for (auto i = k_first_row; i < k_first_row + k_row_count; i++) {
        tdef_functions.builder(nullptr, i);
        // TODO: get generated row from global or pass in pointer of correct type, depending on table
        row_stop(table_id); //  TODO: name collision with tpch, make sure the tpcds version is called!
      }
    }
  }
}

}

namespace opossum {

TpcdsTableGenerator::TpcdsTableGenerator(float scale_factor, uint32_t chunk_size)
  : AbstractTableGenerator(create_benchmark_config_with_chunk_size(chunk_size))
  , _scale_factor(scale_factor)
{}

TpcdsTableGenerator::TpcdsTableGenerator(float scale_factor, const std::shared_ptr<BenchmarkConfig> &benchmark_config)
    : AbstractTableGenerator(benchmark_config)
    , _scale_factor(scale_factor)
{}

std::unordered_map<std::string, BenchmarkTableInfo> TpcdsTableGenerator::generate() {
  set_rng_seed(0);

  generate_tables();

  return std::unordered_map<std::string, BenchmarkTableInfo>(); //  TODO
}

}