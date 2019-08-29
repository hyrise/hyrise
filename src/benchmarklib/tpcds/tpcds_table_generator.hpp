#pragma once

extern "C" {
#include <tpcds-kit/tools/config.h>
#include <tpcds-kit/tools/porting.h>
}

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "abstract_table_generator.hpp"
#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Wrapper around the official tpcds-dbgen tool, making it directly generate opossum::Table instances without having
 * to generate and then load .tbl files.
 *
 * NOT thread safe because the underlying dsdgen is probably not (assuming it has the same issues as the tpch dbgen).
 * There is a tpcds_cleanup() function in third_party/tpcds-kit/r_params.c, but after calling tpcds_cleanup, TPC-DS 
 * data can no longer be generated. We decided that being able to generate tpcds data multiple times in for example a
 * hyriseConsole session is more important than fixing these small memory leaks (<1MB for 1GB of generated data).
 */
class TpcdsTableGenerator final : public AbstractTableGenerator {
 public:
  // rng seed 19620718 is the same dsdgen uses as default
  explicit TpcdsTableGenerator(uint32_t scale_factor, ChunkOffset chunk_size = Chunk::DEFAULT_SIZE,
                               int rng_seed = 19620718);
  TpcdsTableGenerator(uint32_t scale_factor, const std::shared_ptr<BenchmarkConfig>& benchmark_config,
                      int rng_seed = 19620718);

  std::unordered_map<std::string, BenchmarkTableInfo> generate() override;

  // max_rows is used to limit the number of rows generated in tests
  std::shared_ptr<Table> generate_call_center(ds_key_t max_rows = _ds_key_max) const;
  std::shared_ptr<Table> generate_catalog_page(ds_key_t max_rows = _ds_key_max) const;
  std::pair<std::shared_ptr<Table>, std::shared_ptr<Table>> generate_catalog_sales_and_returns(
      ds_key_t max_rows = _ds_key_max) const;
  std::shared_ptr<Table> generate_customer_address(ds_key_t max_rows = _ds_key_max) const;
  std::shared_ptr<Table> generate_customer(ds_key_t max_rows = _ds_key_max) const;
  std::shared_ptr<Table> generate_customer_demographics(ds_key_t max_rows = _ds_key_max) const;
  std::shared_ptr<Table> generate_date_dim(ds_key_t max_rows = _ds_key_max) const;
  std::shared_ptr<Table> generate_household_demographics(ds_key_t max_rows = _ds_key_max) const;
  std::shared_ptr<Table> generate_income_band(ds_key_t max_rows = _ds_key_max) const;
  std::shared_ptr<Table> generate_inventory(ds_key_t max_rows = _ds_key_max) const;
  std::shared_ptr<Table> generate_item(ds_key_t max_rows = _ds_key_max) const;
  std::shared_ptr<Table> generate_promotion(ds_key_t max_rows = _ds_key_max) const;
  std::shared_ptr<Table> generate_reason(ds_key_t max_rows = _ds_key_max) const;
  std::shared_ptr<Table> generate_ship_mode(ds_key_t max_rows = _ds_key_max) const;
  std::shared_ptr<Table> generate_store(ds_key_t max_rows = _ds_key_max) const;
  std::pair<std::shared_ptr<Table>, std::shared_ptr<Table>> generate_store_sales_and_returns(
      ds_key_t max_rows = _ds_key_max) const;
  std::shared_ptr<Table> generate_time_dim(ds_key_t max_rows = _ds_key_max) const;
  std::shared_ptr<Table> generate_warehouse(ds_key_t max_rows = _ds_key_max) const;
  std::shared_ptr<Table> generate_web_page(ds_key_t max_rows = _ds_key_max) const;
  std::pair<std::shared_ptr<Table>, std::shared_ptr<Table>> generate_web_sales_and_returns(
      ds_key_t max_rows = _ds_key_max) const;
  std::shared_ptr<Table> generate_web_site(ds_key_t max_rows = _ds_key_max) const;

 private:
  static constexpr auto _ds_key_max = std::numeric_limits<ds_key_t>::max();
  uint32_t _scale_factor;

  std::shared_ptr<Table> _generate_table(const std::string& table_name);
  std::pair<std::shared_ptr<Table>, std::shared_ptr<Table>> _generate_sales_and_returns_tables(
      const std::string& sales_table_name);
};

}  // namespace opossum
