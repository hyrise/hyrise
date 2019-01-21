#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/chunk_statistics/histograms/generic_histogram.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class GenericHistogramTest : public BaseTest {
  void SetUp() override {
    // clang-format off
    _int_histogram = std::make_shared<GenericHistogram<int32_t>>(
            std::vector<int32_t>{2,  21, 37},
            std::vector<int32_t>{20, 25, 100},
            std::vector<HistogramCountType>{17, 30, 40},
            std::vector<HistogramCountType>{5,  3,  27});
    _double_histogram = std::make_shared<GenericHistogram<double>>(
            std::vector<double>{2.,  21., 37.},
            std::vector<double>{20., 25., 100.},
            std::vector<HistogramCountType>{17, 30, 40},
            std::vector<HistogramCountType>{5,  3,  27});
    _string_histogram = std::make_shared<GenericHistogram<std::string>>(
            std::vector<std::string>{"aa", "at", "bi"},
            std::vector<std::string>{"as", "ax", "dr"},
            std::vector<HistogramCountType>{17, 30, 40},
            std::vector<HistogramCountType>{5,  3,  27},
            "abcdefghijklmnopqrstuvwxyz", 2u);
    // clang-format on
  }

 protected:
  std::shared_ptr<GenericHistogram<int32_t>> _int_histogram;
  std::shared_ptr<GenericHistogram<double>> _double_histogram;
  std::shared_ptr<GenericHistogram<std::string>> _string_histogram;
};

TEST_F(GenericHistogramTest, Basic) {
  CardinalityEstimate estimate;

  estimate = _int_histogram->estimate_cardinality(PredicateCondition::Equals, 1);
  EXPECT_FLOAT_EQ(estimate.cardinality, 0.f);
  EXPECT_EQ(estimate.type, EstimateType::MatchesNone);

  estimate = _double_histogram->estimate_cardinality(PredicateCondition::Equals, 1.);
  EXPECT_FLOAT_EQ(estimate.cardinality, 0.f);
  EXPECT_EQ(estimate.type, EstimateType::MatchesNone);

  estimate = _string_histogram->estimate_cardinality(PredicateCondition::Equals, "a");
  EXPECT_FLOAT_EQ(estimate.cardinality, 0.f);
  EXPECT_EQ(estimate.type, EstimateType::MatchesNone);

  estimate = _int_histogram->estimate_cardinality(PredicateCondition::Equals, 3);
  EXPECT_FLOAT_EQ(estimate.cardinality, 17.f / 5);
  EXPECT_EQ(estimate.type, EstimateType::MatchesApproximately);

  estimate = _double_histogram->estimate_cardinality(PredicateCondition::Equals, 3.);
  EXPECT_FLOAT_EQ(estimate.cardinality, 17.f / 5);
  EXPECT_EQ(estimate.type, EstimateType::MatchesApproximately);

  estimate = _string_histogram->estimate_cardinality(PredicateCondition::Equals, "ab");
  EXPECT_FLOAT_EQ(estimate.cardinality, 17.f / 5);
  EXPECT_EQ(estimate.type, EstimateType::MatchesApproximately);

  estimate = _int_histogram->estimate_cardinality(PredicateCondition::Equals, 26);
  EXPECT_FLOAT_EQ(estimate.cardinality, 0.f);
  EXPECT_EQ(estimate.type, EstimateType::MatchesNone);

  estimate = _double_histogram->estimate_cardinality(PredicateCondition::Equals, 26.);
  EXPECT_FLOAT_EQ(estimate.cardinality, 0.f);
  EXPECT_EQ(estimate.type, EstimateType::MatchesNone);

  estimate = _string_histogram->estimate_cardinality(PredicateCondition::Equals, "ay");
  EXPECT_FLOAT_EQ(estimate.cardinality, 0.f);
  EXPECT_EQ(estimate.type, EstimateType::MatchesNone);
}

TEST_F(GenericHistogramTest, SliceWithPredicate) {
  // clang-format off
  const auto hist = std::make_shared<GenericHistogram<int32_t>>(
          std::vector<int32_t>{1,  30, 60, 80},
          std::vector<int32_t>{25, 50, 75, 100},
          std::vector<HistogramCountType>{40, 30, 20, 10},
          std::vector<HistogramCountType>{10, 20, 15,  5});
  // clang-format on
  auto new_hist = std::shared_ptr<GenericHistogram<int32_t>>{};

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1).type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThanEquals, 1).type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::GreaterThanEquals, 100).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::GreaterThan, 100).type, EstimateType::MatchesNone);

  new_hist =
      std::static_pointer_cast<GenericHistogram<int32_t>>(hist->sliced_with_predicate(PredicateCondition::Equals, 15));
  // New histogram should have 15 as min and max.
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThan, 15).type, EstimateType::MatchesNone);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThanEquals, 15).type, EstimateType::MatchesAll);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThanEquals, 15).type, EstimateType::MatchesAll);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThan, 15).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 15).cardinality, 40.f / 10);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->sliced_with_predicate(PredicateCondition::NotEquals, 15));
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThan, 1).type, EstimateType::MatchesNone);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThanEquals, 1).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThanEquals, 100).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThan, 100).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 23).cardinality, 36.f / 9);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->sliced_with_predicate(PredicateCondition::LessThanEquals, 15));
  // New bin should start at same value as before and end at 15.
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThan, 1).type, EstimateType::MatchesNone);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThanEquals, 1).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThanEquals, 15).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThan, 15).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 10).cardinality, 24.f / 6);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->sliced_with_predicate(PredicateCondition::LessThanEquals, 27));
  // New bin should start at same value as before and end before first gap (because 27 is in that first gap).
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThan, 1).type, EstimateType::MatchesNone);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThanEquals, 1).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThanEquals, 25).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThan, 25).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 10).cardinality, 40.f / 10);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->sliced_with_predicate(PredicateCondition::GreaterThanEquals, 15));
  // New bin should start at 15 and end at same value as before.
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThan, 15).type, EstimateType::MatchesNone);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThanEquals, 15).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThanEquals, 100).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThan, 100).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 18).cardinality, 18.f / 5);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->sliced_with_predicate(PredicateCondition::GreaterThanEquals, 27));
  // New bin should start after the first gap (because 27 is in that first gap) and end at same value as before.
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThan, 30).type, EstimateType::MatchesNone);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThanEquals, 30).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThanEquals, 100).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThan, 100).type, EstimateType::MatchesNone);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::Between, 51, 59).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 35).cardinality, 30.f / 20);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->sliced_with_predicate(PredicateCondition::Between, 0, 17));
  // New bin should start at same value as before (because 0 is smaller than the min of the histogram) and end at 17.
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThan, 1).type, EstimateType::MatchesNone);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThanEquals, 1).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThanEquals, 17).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThan, 17).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 15).cardinality, 40.f / 10);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->sliced_with_predicate(PredicateCondition::Between, 15, 77));
  // New bin should start at 15 and end right before the second gap (because 77 is in that gap).
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThan, 15).type, EstimateType::MatchesNone);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThanEquals, 15).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::Between, 51, 59).type, EstimateType::MatchesNone);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThanEquals, 75).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThan, 75).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 18).cardinality, 18.f / 5);
}

TEST_F(GenericHistogramTest, SplitAtBinBounds) {
  // clang-format off
    const auto hist = std::make_shared<GenericHistogram<int32_t>>(
            std::vector<int32_t>{1,  30, 60, 80},
            std::vector<int32_t>{25, 50, 75, 100},
            std::vector<HistogramCountType>{40, 30, 20, 10},
            std::vector<HistogramCountType>{10, 20, 15, 5});
  // clang-format on

  const auto expected_minima = std::vector<int32_t>{1, 10, 16, 30, 36, 60, 80};
  const auto expected_maxima = std::vector<int32_t>{9, 15, 25, 35, 50, 75, 100};
  const auto expected_heights = std::vector<HistogramCountType>{15, 10, 16, 9, 22, 20, 10};
  const auto expected_distinct_counts = std::vector<HistogramCountType>{4, 3, 4, 6, 15, 15, 5};

  const auto new_hist = hist->split_at_bin_bounds(std::vector<std::pair<int32_t, int32_t>>{{10, 15}, {28, 35}});
  EXPECT_EQ(new_hist->bin_count(), expected_minima.size());

  for (auto bin_id = BinID{0}; bin_id < expected_minima.size(); bin_id++) {
    EXPECT_EQ(new_hist->bin_minimum(bin_id), expected_minima[bin_id]);
    EXPECT_EQ(new_hist->bin_maximum(bin_id), expected_maxima[bin_id]);
    EXPECT_EQ(new_hist->bin_height(bin_id), expected_heights[bin_id]);
    EXPECT_EQ(new_hist->bin_distinct_count(bin_id), expected_distinct_counts[bin_id]);
  }
}

TEST_F(GenericHistogramTest, SplitAtBinBoundssTwoHistograms) {
  // clang-format off
  const auto hist_1 = std::make_shared<GenericHistogram<int32_t>>(
          std::vector<int32_t>{0,  5, 15, 20, 35, 45, 50},
          std::vector<int32_t>{4, 10, 18, 29, 40, 48, 51},

          // We only care about the bin edges in this test.
          std::vector<HistogramCountType>{1, 1, 1, 1, 1, 1, 1},
          std::vector<HistogramCountType>{1, 1, 1, 1, 1, 1, 1});

  const auto hist_2 = std::make_shared<GenericHistogram<int32_t>>(
          std::vector<int32_t>{2, 12, 40, 45, 50},
          std::vector<int32_t>{7, 25, 42, 48, 52},

          // We only care about the bin edges in this test.
          std::vector<HistogramCountType>{1, 1, 1, 1, 1},
          std::vector<HistogramCountType>{1, 1, 1, 1, 1});

  // Even though the histograms are supposed to have the same bin edges, they do not exactly match.
  // The reason is that bins which do not contain any values are not created,
  // so some bins are missing in one histogram, and some are missing in the other.
  const auto hist_1_expected_minima = std::vector<int32_t>{0, 2, 5,  8,     15,     20, 26, 35, 40,     45, 50};
  const auto hist_2_expected_minima = std::vector<int32_t>{   2, 5,     12, 15, 19, 20,         40, 41, 45, 50, 52};
  const auto hist_1_expected_maxima = std::vector<int32_t>{1, 4, 7, 10,     18,     25, 29, 39, 40,     48, 51};
  const auto hist_2_expected_maxima = std::vector<int32_t>{   4, 7,     14, 18, 19, 25,         40, 42, 48, 51, 52};
  // clang-format on

  const auto new_hist_1 = hist_1->split_at_bin_bounds(hist_2->bin_bounds());
  const auto new_hist_2 = hist_2->split_at_bin_bounds(hist_1->bin_bounds());
  EXPECT_EQ(new_hist_1->bin_count(), hist_1_expected_minima.size());
  EXPECT_EQ(new_hist_2->bin_count(), hist_2_expected_minima.size());

  for (auto bin_id = BinID{0}; bin_id < hist_1_expected_minima.size(); bin_id++) {
    EXPECT_EQ(new_hist_1->bin_minimum(bin_id), hist_1_expected_minima[bin_id]);
    EXPECT_EQ(new_hist_1->bin_maximum(bin_id), hist_1_expected_maxima[bin_id]);
  }

  for (auto bin_id = BinID{0}; bin_id < hist_2_expected_minima.size(); bin_id++) {
    EXPECT_EQ(new_hist_2->bin_minimum(bin_id), hist_2_expected_minima[bin_id]);
    EXPECT_EQ(new_hist_2->bin_maximum(bin_id), hist_2_expected_maxima[bin_id]);
  }
}

}  // namespace opossum
