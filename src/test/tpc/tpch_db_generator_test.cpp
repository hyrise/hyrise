#include "gtest/gtest.h"

#include "storage/storage_manager.hpp"
#include "testing_assert.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "utils/load_table.hpp"

namespace opossum {

TEST(TpchDbGeneratorTest, RowCounts) {
  /**
   * Mostly intended to generate coverage and trigger potential leaks in third_party/tpch_dbgen
   */
  const auto scale_factor = 0.001f;
  const auto table_info_by_name = TpchTableGenerator(scale_factor, 100).generate();

  EXPECT_EQ(table_info_by_name.at("part").table->row_count(), std::floor(200'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("supplier").table->row_count(), std::floor(10'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("partsupp").table->row_count(), std::floor(800'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("customer").table->row_count(), std::floor(150'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("orders").table->row_count(), std::floor(1'500'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("nation").table->row_count(), std::floor(25));
  EXPECT_EQ(table_info_by_name.at("region").table->row_count(), std::floor(5));
}

TEST(TpchDbGeneratorTest, TableContents) {
  /**
   * Check whether that data TpchTableGenerator generates with a scale factor of 0.001 is the exact same that dbgen
   *     generates
   */
  const auto scale_factor = 0.001f;
  const auto chunk_size = 1000;
  const auto table_info_by_name = TpchTableGenerator(scale_factor, chunk_size).generate();

  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("part").table,
                          load_table("resources/test_data/tbl/tpch/sf-0.001/part.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("supplier").table,
                          load_table("resources/test_data/tbl/tpch/sf-0.001/supplier.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("partsupp").table,
                          load_table("resources/test_data/tbl/tpch/sf-0.001/partsupp.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("customer").table,
                          load_table("resources/test_data/tbl/tpch/sf-0.001/customer.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("orders").table,
                          load_table("resources/test_data/tbl/tpch/sf-0.001/orders.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("nation").table,
                          load_table("resources/test_data/tbl/tpch/sf-0.001/nation.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("region").table,
                          load_table("resources/test_data/tbl/tpch/sf-0.001/region.tbl", chunk_size));
}

TEST(TpchDbGeneratorTest, GenerateAndStore) {
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
