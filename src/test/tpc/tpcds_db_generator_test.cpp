#include "gtest/gtest.h"

#include "import_export/csv_parser.hpp"
#include "storage/storage_manager.hpp"
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

TEST(TpcdsTableGeneratorTest, GenerateAndStoreRowCounts) {
  /**
 * Check whether all TPC-DS tables are created by the TpcdsTableGenerator and added to the StorageManager.
 * Then check whether the row count is correct for all tables.
 */

  EXPECT_FALSE(StorageManager::get().has_table("call_center"));
  EXPECT_FALSE(StorageManager::get().has_table("catalog_page"));
  EXPECT_FALSE(StorageManager::get().has_table("catalog_returns"));
  EXPECT_FALSE(StorageManager::get().has_table("catalog_sales"));
  EXPECT_FALSE(StorageManager::get().has_table("customer"));
  EXPECT_FALSE(StorageManager::get().has_table("customer_address"));
  EXPECT_FALSE(StorageManager::get().has_table("customer_demographics"));
  EXPECT_FALSE(StorageManager::get().has_table("date"));
  EXPECT_FALSE(StorageManager::get().has_table("household_demographics"));
  EXPECT_FALSE(StorageManager::get().has_table("income_band"));
  EXPECT_FALSE(StorageManager::get().has_table("inventory"));
  EXPECT_FALSE(StorageManager::get().has_table("item"));
  EXPECT_FALSE(StorageManager::get().has_table("promotion"));
  EXPECT_FALSE(StorageManager::get().has_table("reason"));
  EXPECT_FALSE(StorageManager::get().has_table("ship_mode"));
  EXPECT_FALSE(StorageManager::get().has_table("store"));
  EXPECT_FALSE(StorageManager::get().has_table("store_returns"));
  EXPECT_FALSE(StorageManager::get().has_table("store_sales"));
  EXPECT_FALSE(StorageManager::get().has_table("time"));
  EXPECT_FALSE(StorageManager::get().has_table("warehouse"));
  EXPECT_FALSE(StorageManager::get().has_table("web_page"));
  EXPECT_FALSE(StorageManager::get().has_table("web_returns"));
  EXPECT_FALSE(StorageManager::get().has_table("web_sales"));
  EXPECT_FALSE(StorageManager::get().has_table("web_site"));

  TpcdsTableGenerator(1, Chunk::DEFAULT_SIZE, 0).generate_and_store();

  EXPECT_TRUE(StorageManager::get().has_table("call_center"));
  EXPECT_TRUE(StorageManager::get().has_table("catalog_page"));
  EXPECT_TRUE(StorageManager::get().has_table("catalog_returns"));
  EXPECT_TRUE(StorageManager::get().has_table("catalog_sales"));
  EXPECT_TRUE(StorageManager::get().has_table("customer"));
  EXPECT_TRUE(StorageManager::get().has_table("customer_address"));
  EXPECT_TRUE(StorageManager::get().has_table("customer_demographics"));
  EXPECT_TRUE(StorageManager::get().has_table("date"));
  EXPECT_TRUE(StorageManager::get().has_table("household_demographics"));
  EXPECT_TRUE(StorageManager::get().has_table("income_band"));
  EXPECT_TRUE(StorageManager::get().has_table("inventory"));
  EXPECT_TRUE(StorageManager::get().has_table("item"));
  EXPECT_TRUE(StorageManager::get().has_table("promotion"));
  EXPECT_TRUE(StorageManager::get().has_table("reason"));
  EXPECT_TRUE(StorageManager::get().has_table("ship_mode"));
  EXPECT_TRUE(StorageManager::get().has_table("store"));
  EXPECT_TRUE(StorageManager::get().has_table("store_returns"));
  EXPECT_TRUE(StorageManager::get().has_table("store_sales"));
  EXPECT_TRUE(StorageManager::get().has_table("time"));
  EXPECT_TRUE(StorageManager::get().has_table("warehouse"));
  EXPECT_TRUE(StorageManager::get().has_table("web_page"));
  EXPECT_TRUE(StorageManager::get().has_table("web_returns"));
  EXPECT_TRUE(StorageManager::get().has_table("web_sales"));
  EXPECT_TRUE(StorageManager::get().has_table("web_site"));

  EXPECT_EQ(StorageManager::get().get_table("call_center")->row_count(), 6);
  EXPECT_EQ(StorageManager::get().get_table("catalog_page")->row_count(), 11718);
  EXPECT_EQ(StorageManager::get().get_table("catalog_returns")->row_count(), 144201);
  EXPECT_EQ(StorageManager::get().get_table("catalog_sales")->row_count(), 1440060);
  EXPECT_EQ(StorageManager::get().get_table("customer")->row_count(), 100000);
  EXPECT_EQ(StorageManager::get().get_table("customer_address")->row_count(), 50000);
  EXPECT_EQ(StorageManager::get().get_table("customer_demographics")->row_count(), 1920800);
  EXPECT_EQ(StorageManager::get().get_table("date")->row_count(), 73049);
  EXPECT_EQ(StorageManager::get().get_table("household_demographics")->row_count(), 7200);
  EXPECT_EQ(StorageManager::get().get_table("income_band")->row_count(), 20);
  EXPECT_EQ(StorageManager::get().get_table("inventory")->row_count(), 11745000);
  EXPECT_EQ(StorageManager::get().get_table("item")->row_count(), 18000);
  EXPECT_EQ(StorageManager::get().get_table("promotion")->row_count(), 300);
  EXPECT_EQ(StorageManager::get().get_table("reason")->row_count(), 35);
  EXPECT_EQ(StorageManager::get().get_table("ship_mode")->row_count(), 20);
  EXPECT_EQ(StorageManager::get().get_table("store")->row_count(), 12);
  EXPECT_EQ(StorageManager::get().get_table("store_returns")->row_count(), 288324);
  EXPECT_EQ(StorageManager::get().get_table("store_sales")->row_count(), 2879434);
  EXPECT_EQ(StorageManager::get().get_table("time")->row_count(), 86400);
  EXPECT_EQ(StorageManager::get().get_table("warehouse")->row_count(), 5);
  EXPECT_EQ(StorageManager::get().get_table("web_page")->row_count(), 60);
  EXPECT_EQ(StorageManager::get().get_table("web_returns")->row_count(), 71746);
  EXPECT_EQ(StorageManager::get().get_table("web_sales")->row_count(), 719620);
  EXPECT_EQ(StorageManager::get().get_table("web_site")->row_count(), 30);

  StorageManager::reset();
}

TEST(TpcdsTableGeneratorTest, TableContentsFirstRows) {
  /**
   * Check whether the data that TpcdsTableGenerator generates is the exact same that dsdgen generates.
   * Since dsdgen does not support very small scale factors only generate and check first rows for each table.
   */
  const auto rows_to_check = ds_key_t{50};

  // Run generation twice to make sure no global state (of which tpcds_dbgen has plenty :( ) from the
  //  first generation process carried over into the second
  for (auto i = 1; i <= 2; i++) {
    std::cout << "TableContentsFirstRows pass " << i << std::endl;
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
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_date(rows_to_check), load_csv("date_dim.csv"));
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
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_time(rows_to_check), load_csv("time_dim.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_warehouse(rows_to_check), load_csv("warehouse.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_web_page(rows_to_check), load_csv("web_page.csv"));
    const auto [web_sales_table, web_returns_table] = table_generator.generate_web_sales_and_returns(rows_to_check);
    EXPECT_TABLE_EQ_ORDERED(web_sales_table, load_csv("web_sales.csv"));
    EXPECT_TABLE_EQ_ORDERED(web_returns_table, load_csv("web_returns.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_web_site(rows_to_check), load_csv("web_site.csv"));
  }
}

}  // namespace opossum
