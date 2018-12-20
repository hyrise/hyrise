#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/chunk_statistics/histograms/single_bin_histogram.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class SingleBinHistogramTest : public BaseTest {
  void SetUp() override {
    _int_histogram = std::make_shared<SingleBinHistogram<int32_t>>(2, 100, 87, 35);
    _double_histogram = std::make_shared<SingleBinHistogram<double>>(2., 100., 87, 35);
    _string_histogram =
        std::make_shared<SingleBinHistogram<std::string>>("aa", "dr", 87, 35, "abcdefghijklmnopqrstuvwxyz", 2u);
  }

 protected:
  std::shared_ptr<SingleBinHistogram<int32_t>> _int_histogram;
  std::shared_ptr<SingleBinHistogram<double>> _double_histogram;
  std::shared_ptr<SingleBinHistogram<std::string>> _string_histogram;
};

TEST_F(SingleBinHistogramTest, Basic) {
  {
    const auto estimate = _int_histogram->estimate_cardinality(PredicateCondition::Equals, 1);
    EXPECT_FLOAT_EQ(estimate.cardinality, 0.f);
    EXPECT_EQ(estimate.type, EstimateType::MatchesNone);
  }
  {
    const auto estimate = _double_histogram->estimate_cardinality(PredicateCondition::Equals, 1.);
    EXPECT_FLOAT_EQ(estimate.cardinality, 0.f);
    EXPECT_EQ(estimate.type, EstimateType::MatchesNone);
  }
  {
    const auto estimate = _string_histogram->estimate_cardinality(PredicateCondition::Equals, "a");
    EXPECT_FLOAT_EQ(estimate.cardinality, 0.f);
    EXPECT_EQ(estimate.type, EstimateType::MatchesNone);
  }

  {
    const auto estimate = _int_histogram->estimate_cardinality(PredicateCondition::Equals, 38);
    EXPECT_FLOAT_EQ(estimate.cardinality, 87.f / 35);
    EXPECT_EQ(estimate.type, EstimateType::MatchesApproximately);
  }
  {
    const auto estimate = _double_histogram->estimate_cardinality(PredicateCondition::Equals, 38.);
    EXPECT_FLOAT_EQ(estimate.cardinality, 87.f / 35);
    EXPECT_EQ(estimate.type, EstimateType::MatchesApproximately);
  }
  {
    const auto estimate = _string_histogram->estimate_cardinality(PredicateCondition::Equals, "bj");
    EXPECT_FLOAT_EQ(estimate.cardinality, 87.f / 35);
    EXPECT_EQ(estimate.type, EstimateType::MatchesApproximately);
  }

  {
    const auto estimate = _int_histogram->estimate_cardinality(PredicateCondition::LessThan, 38);
    EXPECT_FLOAT_EQ(estimate.cardinality, (38 - 2) / (100 - 2 + 1.f) * 87);
    EXPECT_EQ(estimate.type, EstimateType::MatchesApproximately);
  }
  {
    const auto estimate = _double_histogram->estimate_cardinality(PredicateCondition::LessThan, 38.);
    EXPECT_FLOAT_EQ(estimate.cardinality,
                    (38. - 2.) / std::nextafter(100. - 2., std::numeric_limits<double>::infinity()) * 87.);
    EXPECT_EQ(estimate.type, EstimateType::MatchesApproximately);
  }
  {
    const auto estimate = _string_histogram->estimate_cardinality(PredicateCondition::LessThan, "bj");
    EXPECT_FLOAT_EQ(estimate.cardinality, (38 - 2) / (100 - 2 + 1.f) * 87);
    EXPECT_EQ(estimate.type, EstimateType::MatchesApproximately);
  }

  {
    const auto estimate = _int_histogram->estimate_cardinality(PredicateCondition::Equals, 101);
    EXPECT_FLOAT_EQ(estimate.cardinality, 0.f);
    EXPECT_EQ(estimate.type, EstimateType::MatchesNone);
  }
  {
    const auto estimate = _double_histogram->estimate_cardinality(PredicateCondition::Equals, 101.);
    EXPECT_FLOAT_EQ(estimate.cardinality, 0.f);
    EXPECT_EQ(estimate.type, EstimateType::MatchesNone);
  }
  {
    const auto estimate = _string_histogram->estimate_cardinality(PredicateCondition::Equals, "dra");
    EXPECT_FLOAT_EQ(estimate.cardinality, 0.f);
    EXPECT_EQ(estimate.type, EstimateType::MatchesNone);
  }
}

TEST_F(SingleBinHistogramTest, FromSegment) {
  const auto table = load_table("resources/test_data/tbl/int_int4.tbl");
  ASSERT_EQ(table->chunk_count(), 1u);

  const auto hist = SingleBinHistogram<int32_t>::from_segment(table->get_chunk(ChunkID{0})->get_segment(ColumnID{0}));
  EXPECT_EQ(hist->bin_count(), 1u);
  EXPECT_EQ(hist->minimum(), 0);
  EXPECT_EQ(hist->maximum(), 18);
  EXPECT_EQ(hist->total_count(), 11u);
  EXPECT_EQ(hist->total_distinct_count(), 7u);
}

}  // namespace opossum
