#include "gtest/gtest.h"

#include "tpch/tpch_db_generator.hpp"

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
}