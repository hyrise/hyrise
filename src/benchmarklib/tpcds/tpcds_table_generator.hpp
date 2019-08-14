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
 */
class TpcdsTableGenerator final : public AbstractTableGenerator {
 public:
  explicit TpcdsTableGenerator(uint32_t scale_factor, ChunkOffset chunk_size = Chunk::DEFAULT_SIZE,
                               int rng_seed = 19620718, bool cleanup_after_generate = true);
  TpcdsTableGenerator(uint32_t scale_factor, const std::shared_ptr<BenchmarkConfig>& benchmark_config,
                      std::optional<std::filesystem::path> path_to_cache = {}, int rng_seed = 19620718,
                      bool cleanup_after_generate = true);

  std::unordered_map<std::string, BenchmarkTableInfo> generate() override;

  // max_rows is used to limit the number of rows generated in tests
  std::shared_ptr<Table> generate_call_center(ds_key_t max_rows = std::numeric_limits<ds_key_t>::max()) const;
  std::shared_ptr<Table> generate_catalog_page(ds_key_t max_rows = std::numeric_limits<ds_key_t>::max()) const;
  std::pair<std::shared_ptr<Table>, std::shared_ptr<Table>> generate_catalog_sales_and_returns(
      ds_key_t max_rows = std::numeric_limits<ds_key_t>::max()) const;
  std::shared_ptr<Table> generate_customer_address(ds_key_t max_rows = std::numeric_limits<ds_key_t>::max()) const;
  std::shared_ptr<Table> generate_customer(ds_key_t max_rows = std::numeric_limits<ds_key_t>::max()) const;
  std::shared_ptr<Table> generate_customer_demographics(ds_key_t max_rows = std::numeric_limits<ds_key_t>::max()) const;
  std::shared_ptr<Table> generate_date_dim(ds_key_t max_rows = std::numeric_limits<ds_key_t>::max()) const;
  std::shared_ptr<Table> generate_household_demographics(
      ds_key_t max_rows = std::numeric_limits<ds_key_t>::max()) const;
  std::shared_ptr<Table> generate_income_band(ds_key_t max_rows = std::numeric_limits<ds_key_t>::max()) const;
  std::shared_ptr<Table> generate_inventory(ds_key_t max_rows = std::numeric_limits<ds_key_t>::max()) const;
  std::shared_ptr<Table> generate_item(ds_key_t max_rows = std::numeric_limits<ds_key_t>::max()) const;
  std::shared_ptr<Table> generate_promotion(ds_key_t max_rows = std::numeric_limits<ds_key_t>::max()) const;
  std::shared_ptr<Table> generate_reason(ds_key_t max_rows = std::numeric_limits<ds_key_t>::max()) const;
  std::shared_ptr<Table> generate_ship_mode(ds_key_t max_rows = std::numeric_limits<ds_key_t>::max()) const;
  std::shared_ptr<Table> generate_store(ds_key_t max_rows = std::numeric_limits<ds_key_t>::max()) const;
  std::pair<std::shared_ptr<Table>, std::shared_ptr<Table>> generate_store_sales_and_returns(
      ds_key_t max_rows = std::numeric_limits<ds_key_t>::max()) const;
  std::shared_ptr<Table> generate_time_dim(ds_key_t max_rows = std::numeric_limits<ds_key_t>::max()) const;
  std::shared_ptr<Table> generate_warehouse(ds_key_t max_rows = std::numeric_limits<ds_key_t>::max()) const;
  std::shared_ptr<Table> generate_web_page(ds_key_t max_rows = std::numeric_limits<ds_key_t>::max()) const;
  std::pair<std::shared_ptr<Table>, std::shared_ptr<Table>> generate_web_sales_and_returns(
      ds_key_t max_rows = std::numeric_limits<ds_key_t>::max()) const;
  std::shared_ptr<Table> generate_web_site(ds_key_t max_rows = std::numeric_limits<ds_key_t>::max()) const;

 private:
  bool cleanup_after_generate;
  std::optional<std::filesystem::path> path_to_cache;

  std::shared_ptr<Table> _generate_table(const std::string& table_name);
};

}  // namespace opossum
