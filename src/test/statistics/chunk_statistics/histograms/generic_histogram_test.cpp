#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/chunk_statistics/histograms/generic_histogram.hpp"
#include "statistics/chunk_statistics/histograms/histogram_utils.hpp"
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
  std::pair<float, bool> estimate_pair;

  estimate_pair = _int_histogram->estimate_cardinality(PredicateCondition::Equals, 1);
  EXPECT_FLOAT_EQ(estimate_pair.first, 0.f);
  EXPECT_TRUE(estimate_pair.second);
  estimate_pair = _double_histogram->estimate_cardinality(PredicateCondition::Equals, 1.);
  EXPECT_FLOAT_EQ(estimate_pair.first, 0.f);
  EXPECT_TRUE(estimate_pair.second);
  estimate_pair = _string_histogram->estimate_cardinality(PredicateCondition::Equals, "a");
  EXPECT_FLOAT_EQ(estimate_pair.first, 0.f);
  EXPECT_TRUE(estimate_pair.second);

  estimate_pair = _int_histogram->estimate_cardinality(PredicateCondition::Equals, 3);
  EXPECT_FLOAT_EQ(estimate_pair.first, 17.f / 5);
  EXPECT_FALSE(estimate_pair.second);
  estimate_pair = _double_histogram->estimate_cardinality(PredicateCondition::Equals, 3.);
  EXPECT_FLOAT_EQ(estimate_pair.first, 17.f / 5);
  EXPECT_FALSE(estimate_pair.second);
  estimate_pair = _string_histogram->estimate_cardinality(PredicateCondition::Equals, "ab");
  EXPECT_FLOAT_EQ(estimate_pair.first, 17.f / 5);
  EXPECT_FALSE(estimate_pair.second);

  estimate_pair = _int_histogram->estimate_cardinality(PredicateCondition::Equals, 26);
  EXPECT_FLOAT_EQ(estimate_pair.first, 0.f);
  EXPECT_TRUE(estimate_pair.second);
  estimate_pair = _double_histogram->estimate_cardinality(PredicateCondition::Equals, 26.);
  EXPECT_FLOAT_EQ(estimate_pair.first, 0.f);
  EXPECT_TRUE(estimate_pair.second);
  estimate_pair = _string_histogram->estimate_cardinality(PredicateCondition::Equals, "ay");
  EXPECT_FLOAT_EQ(estimate_pair.first, 0.f);
  EXPECT_TRUE(estimate_pair.second);
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

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::LessThan, 1));
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThanEquals, 1));
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::GreaterThanEquals, 100));
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::GreaterThan, 100));

  new_hist =
      std::static_pointer_cast<GenericHistogram<int32_t>>(hist->slice_with_predicate(PredicateCondition::Equals, 15));
  // New histogram should have 15 as min and max.
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::LessThan, 15));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::LessThanEquals, 15));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::GreaterThanEquals, 15));
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::GreaterThan, 15));
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 15).first, 40.f / 10);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->slice_with_predicate(PredicateCondition::NotEquals, 15));
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::LessThan, 1));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::LessThanEquals, 1));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::GreaterThanEquals, 100));
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::GreaterThan, 100));
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 23).first, 36.f / 9);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->slice_with_predicate(PredicateCondition::LessThanEquals, 15));
  // New bin should start at same value as before and end at 15.
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::LessThan, 1));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::LessThanEquals, 1));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::GreaterThanEquals, 15));
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::GreaterThan, 15));
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 10).first, 24.f / 6);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->slice_with_predicate(PredicateCondition::LessThanEquals, 27));
  // New bin should start at same value as before and end before first gap (because 27 is in that first gap).
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::LessThan, 1));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::LessThanEquals, 1));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::GreaterThanEquals, 25));
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::GreaterThan, 25));
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 10).first, 40.f / 10);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->slice_with_predicate(PredicateCondition::GreaterThanEquals, 15));
  // New bin should start at 15 and end at same value as before.
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::LessThan, 15));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::LessThanEquals, 15));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::GreaterThanEquals, 100));
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::GreaterThan, 100));
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 18).first, 18.f / 5);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->slice_with_predicate(PredicateCondition::GreaterThanEquals, 27));
  // New bin should start after the first gap (because 27 is in that first gap) and end at same value as before.
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::LessThan, 30));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::LessThanEquals, 30));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::GreaterThanEquals, 100));
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::GreaterThan, 100));
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::Between, 51, 59));
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 35).first, 30.f / 20);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->slice_with_predicate(PredicateCondition::Between, 0, 17));
  // New bin should start at same value as before (because 0 is smaller than the min of the histogram) and end at 17.
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::LessThan, 1));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::LessThanEquals, 1));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::GreaterThanEquals, 17));
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::GreaterThan, 17));
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 15).first, 40.f / 10);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->slice_with_predicate(PredicateCondition::Between, 15, 77));
  // New bin should start at 15 and end right before the second gap (because 77 is in that gap).
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::LessThan, 15));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::LessThanEquals, 15));
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::Between, 51, 59));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::GreaterThanEquals, 75));
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::GreaterThan, 75));
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 18).first, 18.f / 5);
}

}  // namespace opossum
