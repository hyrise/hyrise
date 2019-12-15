#include "gtest/gtest.h"

#include "hyrise.hpp"
#include "testing_assert.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "utils/load_table.hpp"

namespace opossum {

TEST(TPCHTableGeneratorTest, SmallScaleFactor) {
  /**
   * Check whether the data that TPCHTableGenerator generates with a scale factor of 0.01 is the exact same that dbgen
   *     generates
   */

  const auto dir_001 = std::string{"resources/test_data/tbl/tpch/sf-0.01/"};

  const auto chunk_size = 1000;
  auto table_info_by_name = TPCHTableGenerator(0.01f, chunk_size).generate();

  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("part").table, load_table(dir_001 + "part.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("supplier").table, load_table(dir_001 + "supplier.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("partsupp").table, load_table(dir_001 + "partsupp.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("customer").table, load_table(dir_001 + "customer.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("orders").table, load_table(dir_001 + "orders.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("nation").table, load_table(dir_001 + "nation.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("region").table, load_table(dir_001 + "region.tbl", chunk_size));

#if defined(__has_feature)
#if (__has_feature(thread_sanitizer) || __has_feature(address_sanitizer))
  // We verified thread and address safety above. As this is quite expensive to sanitize, don't perform the following
  // check - double parantheses mark the code as explicitly dead.
  if ((true)) return;
#endif
#endif

  // Run generation a second time to make sure no global state (of which tpch_dbgen has plenty :( ) from the first
  // generation process carried over into the second

  const auto dir_002 = std::string{"resources/test_data/tbl/tpch/sf-0.02/"};

  table_info_by_name = TPCHTableGenerator(0.02f, chunk_size).generate();

  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("part").table, load_table(dir_002 + "part.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("supplier").table, load_table(dir_002 + "supplier.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("partsupp").table, load_table(dir_002 + "partsupp.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("customer").table, load_table(dir_002 + "customer.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("orders").table, load_table(dir_002 + "orders.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("nation").table, load_table(dir_002 + "nation.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("region").table, load_table(dir_002 + "region.tbl", chunk_size));
}

TEST(TPCHTableGeneratorTest, RowCountsMediumScaleFactor) {
  /**
   * Mostly intended to generate coverage and trigger potential leaks in third_party/tpch_dbgen
   */
  const auto scale_factor = 1.0f;
  const auto table_info_by_name = TPCHTableGenerator(scale_factor, 100).generate();

  EXPECT_EQ(table_info_by_name.at("part").table->row_count(), std::floor(200'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("supplier").table->row_count(), std::floor(10'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("partsupp").table->row_count(), std::floor(800'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("customer").table->row_count(), std::floor(150'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("orders").table->row_count(), std::floor(1'500'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("nation").table->row_count(), std::floor(25));
  EXPECT_EQ(table_info_by_name.at("region").table->row_count(), std::floor(5));
}

TEST(TpchTableGeneratorTest, GenerateAndStore) {
  EXPECT_FALSE(Hyrise::get().storage_manager.has_table("part"));
  EXPECT_FALSE(Hyrise::get().storage_manager.has_table("supplier"));
  EXPECT_FALSE(Hyrise::get().storage_manager.has_table("partsupp"));
  EXPECT_FALSE(Hyrise::get().storage_manager.has_table("customer"));
  EXPECT_FALSE(Hyrise::get().storage_manager.has_table("orders"));
  EXPECT_FALSE(Hyrise::get().storage_manager.has_table("nation"));
  EXPECT_FALSE(Hyrise::get().storage_manager.has_table("region"));

  // Small scale factor
  TPCHTableGenerator(0.01f, Chunk::DEFAULT_SIZE).generate_and_store();

  EXPECT_TRUE(Hyrise::get().storage_manager.has_table("part"));
  EXPECT_TRUE(Hyrise::get().storage_manager.has_table("supplier"));
  EXPECT_TRUE(Hyrise::get().storage_manager.has_table("partsupp"));
  EXPECT_TRUE(Hyrise::get().storage_manager.has_table("customer"));
  EXPECT_TRUE(Hyrise::get().storage_manager.has_table("orders"));
  EXPECT_TRUE(Hyrise::get().storage_manager.has_table("nation"));
  EXPECT_TRUE(Hyrise::get().storage_manager.has_table("region"));

  Hyrise::reset();
}
}  // namespace opossum
