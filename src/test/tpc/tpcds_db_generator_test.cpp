#include "gtest/gtest.h"

#include "import_export/csv_parser.hpp"
#include "storage/storage_manager.hpp"
#include "testing_assert.hpp"
#include "tpcds/tpcds_table_generator.hpp"
#include "utils/load_table.hpp"

using namespace opossum;  // NOLINT

namespace {
  std::shared_ptr<Table> load_csv(const std::string& file_name) {
    return CsvParser{}.parse("resources/test_data/csv/tpcds/" + file_name,
      process_csv_meta_file("resources/benchmark/tpcds/tables/" + file_name + CsvMeta::META_FILE_EXTENSION),
      Chunk::DEFAULT_SIZE);
  }
}

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
  EXPECT_FALSE(StorageManager::get().has_table("dbgen_version"));
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

  TpcdsTableGenerator(1, Chunk::DEFAULT_SIZE).generate_and_store();

  EXPECT_TRUE(StorageManager::get().has_table("call_center"));
  EXPECT_TRUE(StorageManager::get().has_table("catalog_page"));
  EXPECT_TRUE(StorageManager::get().has_table("catalog_returns"));
  EXPECT_TRUE(StorageManager::get().has_table("catalog_sales"));
  EXPECT_TRUE(StorageManager::get().has_table("customer"));
  EXPECT_TRUE(StorageManager::get().has_table("customer_address"));
  EXPECT_TRUE(StorageManager::get().has_table("customer_demographics"));
  EXPECT_TRUE(StorageManager::get().has_table("date"));
  EXPECT_TRUE(StorageManager::get().has_table("dbgen_version"));
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
  EXPECT_EQ(StorageManager::get().get_table("dbgen_version")->row_count(), 1);
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

  const auto table_generator = TpcdsTableGenerator(1, Chunk::DEFAULT_SIZE, 0);

  const auto table_a = table_generator.generate_call_center(50);
  const auto table_b = load_csv("call_center.csv");

  EXPECT_TABLE_EQ_ORDERED(table_a, table_b);

  // TODO: Run generation a second time to make sure no global state (of which tpcds_dbgen has plenty :( ) from the
  //  first generation process carried over into the second
}

}  // namespace opossum
