#include "gtest/gtest.h"

#include "testing_assert.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "utils/load_table.hpp"

namespace opossum {

TEST(TpchDbGeneratorTest, RowCounts) {
  /**
   * Mostly intended to generate coverage and trigger potential leaks in third_party/tpch_dbgen
   */
  const auto scale_factor = 0.001f;
  const auto tables = TpchDbGenerator(scale_factor, 100).generate();

  EXPECT_EQ(tables.at(TpchTable::Part)->row_count(), std::floor(200'000 * scale_factor));
  EXPECT_EQ(tables.at(TpchTable::Supplier)->row_count(), std::floor(10'000 * scale_factor));
  EXPECT_EQ(tables.at(TpchTable::PartSupp)->row_count(), std::floor(800'000 * scale_factor));
  EXPECT_EQ(tables.at(TpchTable::Customer)->row_count(), std::floor(150'000 * scale_factor));
  EXPECT_EQ(tables.at(TpchTable::Orders)->row_count(), std::floor(1'500'000 * scale_factor));
  EXPECT_EQ(tables.at(TpchTable::Nation)->row_count(), std::floor(25));
  EXPECT_EQ(tables.at(TpchTable::Region)->row_count(), std::floor(5));
}

TEST(TpchDbGeneratorTest, TableContents) {
  /**
   * Check whether that data TpchDbGenerator generates with a scale factor of 0.001 is the exact same that dbgen
   *     generates
   */
  const auto scale_factor = 0.001f;
  const auto chunk_size = 1000;
  const auto tables = TpchDbGenerator(scale_factor, chunk_size).generate();

  EXPECT_TABLE_EQ_ORDERED(tables.at(TpchTable::Part), load_table("src/test/tables/tpch/sf-0.001/part.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(tables.at(TpchTable::Supplier),
                          load_table("src/test/tables/tpch/sf-0.001/supplier.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(tables.at(TpchTable::PartSupp),
                          load_table("src/test/tables/tpch/sf-0.001/partsupp.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(tables.at(TpchTable::Customer),
                          load_table("src/test/tables/tpch/sf-0.001/customer.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(tables.at(TpchTable::Orders),
                          load_table("src/test/tables/tpch/sf-0.001/orders.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(tables.at(TpchTable::Nation),
                          load_table("src/test/tables/tpch/sf-0.001/nation.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(tables.at(TpchTable::Region),
                          load_table("src/test/tables/tpch/sf-0.001/region.tbl", chunk_size));
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
  TpchDbGenerator(0.01f, Chunk::MAX_SIZE).generate_and_store();

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
