#include "gtest/gtest.h"

#include "tpch/tpch_db_generator.hpp"

namespace opossum {

TEST(TpchDbGeneratorTest, RowCounts) {
  const auto scale_factor = 0.001f;

  /**
   * Mostly intended to generate coverage and trigger potential leaks in third_party/tpch_dbgen
   */
  const auto tables = TpchDbGenerator(scale_factor, 100).generate();

  EXPECT_EQ(tables.at(TpchTable::Part)->row_count(), static_cast<size_t>(200'000 * scale_factor));
}

}