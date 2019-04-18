#include "gtest/gtest.h"

#include "storage/storage_manager.hpp"
#include "testing_assert.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "utils/load_table.hpp"

namespace opossum {

TEST(TpchTableGeneratorTest, RowCountsSmallScaleFactor) {
  /**
   * Mostly intended to generate coverage and trigger potential leaks in third_party/tpch_dbgen
   */
  const auto scale_factor = 0.0015f;
  const auto table_info_by_name = TpchTableGenerator(scale_factor, 100).generate();

  EXPECT_EQ(table_info_by_name.at("part").table->row_count(), std::floor(200'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("supplier").table->row_count(), std::floor(10'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("partsupp").table->row_count(), std::floor(800'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("customer").table->row_count(), std::floor(150'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("orders").table->row_count(), std::floor(1'500'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("nation").table->row_count(), std::floor(25));
  EXPECT_EQ(table_info_by_name.at("region").table->row_count(), std::floor(5));
}

TEST(TpchTableGeneratorTest, RowCountsMediumScaleFactor) {
  /**
   * Mostly intended to generate coverage and trigger potential leaks in third_party/tpch_dbgen
   */
  const auto scale_factor = 1.0f;
  const auto table_info_by_name = TpchTableGenerator(scale_factor, 100).generate();

  EXPECT_EQ(table_info_by_name.at("part").table->row_count(), std::floor(200'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("supplier").table->row_count(), std::floor(10'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("partsupp").table->row_count(), std::floor(800'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("customer").table->row_count(), std::floor(150'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("orders").table->row_count(), std::floor(1'500'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("nation").table->row_count(), std::floor(25));
  EXPECT_EQ(table_info_by_name.at("region").table->row_count(), std::floor(5));
}

TEST(TpchTableGeneratorTest, TableContents) {
  /**
   * Check whether that data TpchTableGenerator generates with a scale factor of 0.001 is the exact same that dbgen
   *     generates
   */

  const auto dir_001 = std::string{"resources/test_data/tbl/tpch/sf-0.001/"};

  const auto chunk_size = 1000;
  auto table_info_by_name = TpchTableGenerator(0.001f, chunk_size).generate();

  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("part").table, load_table(dir_001 + "part.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("supplier").table, load_table(dir_001 + "supplier.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("partsupp").table, load_table(dir_001 + "partsupp.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("customer").table, load_table(dir_001 + "customer.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("orders").table, load_table(dir_001 + "orders.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("nation").table, load_table(dir_001 + "nation.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("region").table, load_table(dir_001 + "region.tbl", chunk_size));

  // Run generation a second time to make sure no global state (of which tpch_dbgen has plenty :( ) from the first
  // generation process carried over into the second

  const auto dir_002 = std::string{"resources/test_data/tbl/tpch/sf-0.002/"};

  table_info_by_name = TpchTableGenerator(0.002f, chunk_size).generate();

  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("part").table, load_table(dir_002 + "part.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("supplier").table, load_table(dir_002 + "supplier.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("partsupp").table, load_table(dir_002 + "partsupp.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("customer").table, load_table(dir_002 + "customer.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("orders").table, load_table(dir_002 + "orders.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("nation").table, load_table(dir_002 + "nation.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("region").table, load_table(dir_002 + "region.tbl", chunk_size));
}

TEST(TpchTableGeneratorTest, GenerateAndStore) {
  EXPECT_FALSE(StorageManager::get().has_table("part"));
  EXPECT_FALSE(StorageManager::get().has_table("supplier"));
  EXPECT_FALSE(StorageManager::get().has_table("partsupp"));
  EXPECT_FALSE(StorageManager::get().has_table("customer"));
  EXPECT_FALSE(StorageManager::get().has_table("orders"));
  EXPECT_FALSE(StorageManager::get().has_table("nation"));
  EXPECT_FALSE(StorageManager::get().has_table("region"));

  // Small scale factor
  TpchTableGenerator(0.01f, Chunk::DEFAULT_SIZE).generate_and_store();

  EXPECT_TRUE(StorageManager::get().has_table("part"));
  EXPECT_TRUE(StorageManager::get().has_table("supplier"));
  EXPECT_TRUE(StorageManager::get().has_table("partsupp"));
  EXPECT_TRUE(StorageManager::get().has_table("customer"));
  EXPECT_TRUE(StorageManager::get().has_table("orders"));
  EXPECT_TRUE(StorageManager::get().has_table("nation"));
  EXPECT_TRUE(StorageManager::get().has_table("region"));

  StorageManager::reset();
}
}  // namespace opossum
