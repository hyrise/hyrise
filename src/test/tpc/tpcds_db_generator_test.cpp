#include "gtest/gtest.h"

#include "hyrise.hpp"
#include "import_export/csv_parser.hpp"
#include "testing_assert.hpp"
#include "tpcds/tpcds_table_generator.hpp"
#include "utils/load_table.hpp"

using namespace opossum;  // NOLINT

namespace {
std::shared_ptr<Table> load_csv(const std::string& file_name) {
  return CsvParser{}.parse(
      "resources/test_data/csv/tpcds/" + file_name,
      process_csv_meta_file("resources/benchmark/tpcds/tables/" + file_name + CsvMeta::META_FILE_EXTENSION),
      Chunk::DEFAULT_SIZE);
}
}  // namespace

namespace opossum {

TEST(TpcdsTableGeneratorTest, TableContentsFirstRows) {
  /**
   * Check whether the data that TpcdsTableGenerator generates is the exact same that dsdgen generates.
   * Since dsdgen does not support very small scale factors only generate and check first rows for each table.
   */
  const auto rows_to_check = ds_key_t{50};

  // Initialize with different params to check whether global state is correctly reset.
  TpcdsTableGenerator(10, 2, 42);

  // Run generation twice to make sure no global state (of which tpcds_dbgen has plenty :( ) from the
  //  first generation process carried over into the second
  for (auto i = 1; i <= 2; i++) {
    SCOPED_TRACE("FirstRowsAndRowCounts iteration " + std::to_string(i));
    const auto table_generator = TpcdsTableGenerator(1, Chunk::DEFAULT_SIZE, 305);  // seed 305 includes Mrs. Null
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_call_center(rows_to_check), load_csv("call_center.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_catalog_page(rows_to_check), load_csv("catalog_page.csv"));
    const auto [catalog_sales_table, catalog_returns_table] =
        table_generator.generate_catalog_sales_and_returns(rows_to_check);
    EXPECT_TABLE_EQ_ORDERED(catalog_sales_table, load_csv("catalog_sales.csv"));
    EXPECT_TABLE_EQ_ORDERED(catalog_returns_table, load_csv("catalog_returns.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_customer_address(rows_to_check), load_csv("customer_address.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_customer(rows_to_check), load_csv("customer.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_customer_demographics(rows_to_check),
                            load_csv("customer_demographics.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_date_dim(rows_to_check), load_csv("date_dim.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_household_demographics(rows_to_check),
                            load_csv("household_demographics.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_income_band(rows_to_check), load_csv("income_band.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_inventory(rows_to_check), load_csv("inventory.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_item(rows_to_check), load_csv("item.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_promotion(rows_to_check), load_csv("promotion.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_reason(rows_to_check), load_csv("reason.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_ship_mode(rows_to_check), load_csv("ship_mode.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_store(rows_to_check), load_csv("store.csv"));
    const auto [store_sales_table, store_returns_table] =
        table_generator.generate_store_sales_and_returns(rows_to_check);
    EXPECT_TABLE_EQ_ORDERED(store_sales_table, load_csv("store_sales.csv"));
    EXPECT_TABLE_EQ_ORDERED(store_returns_table, load_csv("store_returns.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_time_dim(rows_to_check), load_csv("time_dim.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_warehouse(rows_to_check), load_csv("warehouse.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_web_page(rows_to_check), load_csv("web_page.csv"));
    const auto [web_sales_table, web_returns_table] = table_generator.generate_web_sales_and_returns(rows_to_check);
    EXPECT_TABLE_EQ_ORDERED(web_sales_table, load_csv("web_sales.csv"));
    EXPECT_TABLE_EQ_ORDERED(web_returns_table, load_csv("web_returns.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_web_site(rows_to_check), load_csv("web_site.csv"));
  }
}

TEST(TpcdsTableGeneratorTest, GenerateAndStoreRowCounts) {
  /**
 * Check whether all TPC-DS tables are created by the TpcdsTableGenerator and added to the StorageManager.
 * Then check whether the row count is correct for all tables.
 */

  const auto expected_sizes = std::map<std::string, uint64_t>{{"call_center", 6},
                                                              {"catalog_page", 11718},
                                                              {"catalog_returns", 144201},
                                                              {"catalog_sales", 1440060},
                                                              {"customer", 100000},
                                                              {"customer_address", 50000},
                                                              {"customer_demographics", 1920800},
                                                              {"date_dim", 73049},
                                                              {"household_demographics", 7200},
                                                              {"income_band", 20},
                                                              {"inventory", 11745000},
                                                              {"item", 18000},
                                                              {"promotion", 300},
                                                              {"reason", 35},
                                                              {"ship_mode", 20},
                                                              {"store", 12},
                                                              {"store_returns", 288324},
                                                              {"store_sales", 2879434},
                                                              {"time_dim", 86400},
                                                              {"warehouse", 5},
                                                              {"web_page", 60},
                                                              {"web_returns", 71746},
                                                              {"web_sales", 719620},
                                                              {"web_site", 30}};

  EXPECT_EQ(Hyrise::get().storage_manager.tables().size(), 0);

  TpcdsTableGenerator(1, Chunk::DEFAULT_SIZE, 0).generate_and_store();

  for (const auto& [name, size] : expected_sizes) {
    SCOPED_TRACE("checking table " + std::string{name});
    EXPECT_EQ(Hyrise::get().storage_manager.get_table(name)->row_count(), size);
  }

  Hyrise::reset();
}

}  // namespace opossum
