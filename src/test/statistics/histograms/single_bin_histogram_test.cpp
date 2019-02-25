#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/histograms/histogram_utils.hpp"
#include "statistics/histograms/single_bin_histogram.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class SingleBinHistogramTest : public BaseTest {};

TEST_F(SingleBinHistogramTest, FromSegment) {
  const auto table = load_table("resources/test_data/tbl/int_int4.tbl");
  ASSERT_EQ(table->chunk_count(), 1u);

  const auto distribution =
      histogram::value_distribution_from_segment<int32_t>(*table->get_chunk(ChunkID{0})->get_segment(ColumnID{0}));
  const auto hist = SingleBinHistogram<int32_t>::from_distribution(distribution);
  EXPECT_EQ(hist->bin_count(), 1u);
  EXPECT_EQ(hist->bin_minimum(BinID{0}), 0);
  EXPECT_EQ(hist->bin_maximum(hist->bin_count() - 1), 18);
  EXPECT_EQ(hist->total_count(), 11u);
  EXPECT_EQ(hist->total_distinct_count(), 7u);
}

}  // namespace opossum
