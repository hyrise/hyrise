#include "gtest/gtest.h"

#include "tpch/tpch_db_generator.hpp"
#include "testing_assert.hpp"
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
   * Mostly intended to generate coverage and trigger potential leaks in third_party/tpch_dbgen
   */
  const auto scale_factor = 0.001f;
  const auto chunk_size = 1000;
  const auto tables = TpchDbGenerator(scale_factor, chunk_size).generate();

  EXPECT_TABLE_EQ_ORDERED(tables.at(TpchTable::Part), load_table("src/test/tables/tpch-sf-0001/part.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(tables.at(TpchTable::Supplier), load_table("src/test/tables/tpch-sf-0001/supplier.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(tables.at(TpchTable::PartSupp), load_table("src/test/tables/tpch-sf-0001/partsupp.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(tables.at(TpchTable::Customer), load_table("src/test/tables/tpch-sf-0001/customer.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(tables.at(TpchTable::Orders), load_table("src/test/tables/tpch-sf-0001/orders.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(tables.at(TpchTable::Nation), load_table("src/test/tables/tpch-sf-0001/nation.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(tables.at(TpchTable::Region), load_table("src/test/tables/tpch-sf-0001/region.tbl", chunk_size));
}
}  // namespace opossum