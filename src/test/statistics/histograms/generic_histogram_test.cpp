#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/histograms/generic_histogram.hpp"
#include "statistics/histograms/histogram_utils.hpp"
#include "statistics/empty_statistics_object.hpp"
#include "constant_mappings.hpp"
#include "utils/load_table.hpp"

/**
 * As well as testing GenericHistogram, we also test a lot of the functionally implemented in AbstractHistogram here.
 * We do this here since GenericHistogram allows us to easily construct interesting edge cases.
 */

namespace opossum {

class GenericHistogramTest : public BaseTest {
 public:
  template<typename T>
  void test_slicing(const AbstractHistogram<T>& input_histogram, const PredicateCondition predicate_condition, const AllTypeVariant& value, const GenericHistogram<T>& expected_histogram) {
    test_slicing(input_histogram, predicate_condition, value, std::nullopt, expected_histogram);
  }

  // Test that a predicate produces a specified slice of a histogram
  template<typename T>
  void test_slicing(const AbstractHistogram<T>& input_histogram, const PredicateCondition predicate_condition, const AllTypeVariant& value, const std::optional<AllTypeVariant>& value2, const GenericHistogram<T>& expected_histogram) {
    std::ostringstream stream;
    stream << "sliced_with_predicate(" << predicate_condition_to_string.left.at(predicate_condition) << ", " << value;
    if (value2) {
      stream << ", " << *value2;
    }
    stream << ")";

    SCOPED_TRACE(stream.str());

    const auto actual_statistics_object = input_histogram.sliced_with_predicate(predicate_condition, value, value2);
    const auto actual_histogram = std::dynamic_pointer_cast<GenericHistogram<T>>(actual_statistics_object);

    ASSERT_TRUE(actual_histogram);
    EXPECT_EQ(*actual_histogram, expected_histogram);
  }

  // Tests that two predicates result in the same slice of a histogram
  template<typename T>
  void test_slicing_has_equal_result(const AbstractHistogram<T>& input_histogram, const PredicateCondition predicate_condition_a, const AllTypeVariant& value_a, const PredicateCondition predicate_condition_b, const AllTypeVariant& value_b) {
    std::ostringstream stream;
    stream << "sliced_with_predicate(" << predicate_condition_to_string.left.at(predicate_condition_a) << ", " << value_a;
    stream << ") == slice_with_predicate(" << predicate_condition_to_string.left.at(predicate_condition_b) << ", " << value_b << ")";

    SCOPED_TRACE(stream.str());

    const auto actual_statistics_object_a = input_histogram.sliced_with_predicate(predicate_condition_a, value_a);
    const auto actual_statistics_object_b = input_histogram.sliced_with_predicate(predicate_condition_b, value_b);

    if (std::dynamic_pointer_cast<EmptyStatisticsObject>(actual_statistics_object_a) && std::dynamic_pointer_cast<EmptyStatisticsObject>(actual_statistics_object_b)) {
      return;
    }

    const auto actual_histogram_a = std::dynamic_pointer_cast<GenericHistogram<T>>(actual_statistics_object_a);
    const auto actual_histogram_b = std::dynamic_pointer_cast<GenericHistogram<T>>(actual_statistics_object_b);

    ASSERT_TRUE(actual_histogram_a);
    ASSERT_TRUE(actual_histogram_b);
    EXPECT_EQ(*actual_histogram_a, *actual_histogram_b);
  }

 protected:
  std::shared_ptr<GenericHistogram<std::string>> histogram;
};

TEST_F(GenericHistogramTest, EstimateCardinalityInt) {
  // clang-format off
  const auto histogram = GenericHistogram<int32_t>(
    {2,  21, 37,  101, 105},
    {20, 25, 100, 103, 105},
    {17, 30, 40,  1,     5},
    { 5,  3, 27,  1,     1});
  // clang-format on

  const auto total_count = histogram.total_count();

  // clang-format off
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 1).cardinality, 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 3).cardinality, 17.0f / 5.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 26).cardinality, 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 105).cardinality, 5.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 200).cardinality, 0.0f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotEquals, 1).cardinality, total_count);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotEquals, 21).cardinality, total_count - 10);  // NOLINT

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, -10).cardinality, 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 2).cardinality, 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 20).cardinality, 17.0f - 17.0f / 19.0f);  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 21).cardinality, 17.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 40).cardinality, 17.0f + 30 + 3 * (40.0f / 64.0f));  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 105).cardinality, total_count - 5);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThan, 1000).cardinality, total_count);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, -10).cardinality, 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, 2).cardinality, 17.0f / 19.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, 3).cardinality, 2 * (17.0f / 19.0f));  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, 20).cardinality, 17.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, 21).cardinality, 17.0f + (30.0f / 5.0f));  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, 40).cardinality, 17.0f + 30 + 4 * (40.0f / 64.0f));  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, 105).cardinality, total_count);  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::LessThanEquals, 1000).cardinality, total_count);  // NOLINT

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThan, -10).cardinality, total_count);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThan, 1).cardinality, total_count);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThan, 2).cardinality, total_count - (17.0f / 19.0f));  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThan, 20).cardinality, 76.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThan, 21).cardinality, 76.0f - (30.0f / 5.0f));  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThan, 105).cardinality, 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThan, 1000).cardinality, 0.0f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThanEquals, -10).cardinality, total_count);  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThanEquals, 1).cardinality, total_count);  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThanEquals, 2).cardinality, total_count);  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThanEquals, 20).cardinality, 76.0f + 17.0f / 19.0f);  // NOLINT
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThanEquals, 21).cardinality, 76.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThanEquals, 105).cardinality, 5.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::GreaterThanEquals, 1000).cardinality, 0.0f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Between, 2, 20).cardinality, 17.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Between, 2, 25).cardinality, 47.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Between, 26, 27).cardinality, 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Between, 105, 105).cardinality, 5);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Between, 105, 106).cardinality, 5);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Between, 107, 107).cardinality, 0.0f);
  // clang-format on
}

TEST_F(GenericHistogramTest, EstimateCardinalityFloat) {
  // clang-format off
  const auto histogram = std::make_shared<GenericHistogram<float>>(
    std::vector<float>             {2.0f,  23.0f, next_value(25.0f),            31.0f,  32.0f},
    std::vector<float>             {22.0f, 25.0f,            30.0f,  next_value(31.0f), 32.0f},
    std::vector<HistogramCountType>{17,    30,               20,                 7,      3},
    std::vector<HistogramCountType>{ 5,     3,                5,                 2,      1});
  // clang-format on

  const auto total_count = histogram->total_count();

  // clang-format off
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, 1.0f).cardinality, 0.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, 3.0f).cardinality, 17.f / 5.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, 22.5f).cardinality, 0.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, 31.0f).cardinality, 7.0f / 2.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, next_value(31.0f)).cardinality, 7.0f / 2.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, 32.0f).cardinality, 3.0f);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotEquals, 1.0f).cardinality, total_count);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotEquals, 2.0f).cardinality, total_count - 17.0f / 5.0f);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotEquals, 31.0f).cardinality, total_count - 7.0f / 2.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotEquals, next_value(31.0f)).cardinality, total_count - 7.0f / 2.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotEquals, 32.0f).cardinality, 74);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, 2.0f).cardinality, 0.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, next_value(2.0f)).cardinality, 17.0f * ((next_value(2.0f) - 2.0f) / 20.0f));  // NOLINT
  // Floating point quirk: These go to exactly 67, should be slightly below
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, 24.5f).cardinality, 17.0f + 30 * (1.5f / 2.0f)); // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, 30.0f).cardinality, 67.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, next_value(30.0f)).cardinality, 67.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, 150.0f).cardinality, total_count);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, 31.0f).cardinality, 67.0f);
  // Floating point quirk: The bin [31, next_value(31.0f)] is too small to be split at `< next_value(31.0f)`
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, next_value(31.0f)).cardinality, 74.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, 32.0f).cardinality, 74.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, next_value(32.0f)).cardinality, 77.0f);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, 2.0f).cardinality, 17.0f * ((next_value(2.0f) - 2.0f) / 20.0f));  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, previous_value(24.0f)).cardinality, 17.0f + 30.0f * 0.5f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, 30.0f).cardinality, 67.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, 150.0f).cardinality, total_count);  // NOLINT
  // Floating point quirk: `<= 31.0f` is `< next_value(31.0f)`, which in turn covers the entire bin.
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, 31.0f).cardinality, 74.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, next_value(31.0f)).cardinality, 74.0f);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, 32.0f).cardinality, total_count);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, next_value(32.0f)).cardinality, total_count);  // NOLINT

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, 2.0f).cardinality, total_count - 17.0f * ((next_value(2.0f) - 2.0f) / 20.0f));  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, 30.0f).cardinality, 10.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, 150.0f).cardinality, 0.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, 31.0f).cardinality, 3.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, next_value(31.0f)).cardinality, 3.0f);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, 32.0f).cardinality, 0.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, previous_value(32.0f)).cardinality, 3);  // NOLINT

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, 2.0f).cardinality, total_count);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, 24.0f).cardinality, 45.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, 30.0f).cardinality, 10.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, 150.0f).cardinality, 0.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, 31.0f).cardinality, 10.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, next_value(31.0f)).cardinality, 3.0f);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, 32.0f).cardinality, 3.0f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, previous_value(32.0f)).cardinality, 3);  // NOLINT

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 2.0f, 3.0f).cardinality, 17.0f * ((next_value(3.0f) - 2.0f) / 20.0f));  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 2.0f, next_value(2.0f)).cardinality, ((next_value(next_value(2.0f)) - 2.0f) / 20.0f) * 17.0f);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 2.0f, 22.5f).cardinality, 17.0f);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 2.0f, 30.0f).cardinality, 67.0f);  // NOLINT
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, previous_value(2.0f), 2.0f).cardinality, 17.0f * ((next_value(2.0f) - 2.0f) / 20.0f));  // NOLINT
  // clang-format on
}

TEST_F(GenericHistogramTest, EstimateCardinalityString) {
  CardinalityEstimate estimate;
  
  // clang-format off
  const auto histogram = std::make_shared<GenericHistogram<std::string>>(
  std::vector<std::string>       {"aa", "at", "bi"},
  std::vector<std::string>       {"as", "ax", "dr"},
  std::vector<HistogramCountType>{  17,   30,   40},
  std::vector<HistogramCountType>{   5,    3,   27},
  "abcdefghijklmnopqrstuvwxyz", 2u);
  // clang-format on

  estimate = histogram->estimate_cardinality(PredicateCondition::Equals, "a");
  EXPECT_FLOAT_EQ(estimate.cardinality, 0.f);
  EXPECT_EQ(estimate.type, EstimateType::MatchesNone);

  estimate = histogram->estimate_cardinality(PredicateCondition::Equals, "ab");
  EXPECT_FLOAT_EQ(estimate.cardinality, 17.f / 5);
  EXPECT_EQ(estimate.type, EstimateType::MatchesApproximately);


  estimate = histogram->estimate_cardinality(PredicateCondition::Equals, "ay");
  EXPECT_FLOAT_EQ(estimate.cardinality, 0.f);
  EXPECT_EQ(estimate.type, EstimateType::MatchesNone);
}

TEST_F(GenericHistogramTest, SlicedWithPredicateInt) {
  // clang-format off
  const auto histogram = GenericHistogram<int32_t>{{ 1, 30, 60,  80},
                                                   {25, 50, 60, 100},
                                                   {40, 30,  5,  10},
                                                   {10, 20,  1,   1}};
  const auto single_bin_single_value_histogram = GenericHistogram<int32_t>{{5}, {5}, {12}, {1}};
  // clang-format on

  // clang-format off
  test_slicing(histogram, PredicateCondition::Equals, 15, GenericHistogram<int32_t>{{15}, {15}, {4}, {1}});
  test_slicing(histogram, PredicateCondition::Equals, 60, GenericHistogram<int32_t>{{60}, {60}, {5}, {1}});
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(histogram.sliced_with_predicate(PredicateCondition::Equals, 61)));  // NOLINT
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(histogram.sliced_with_predicate(PredicateCondition::Equals, -50)));  // NOLINT
  test_slicing(single_bin_single_value_histogram, PredicateCondition::Equals, 5, single_bin_single_value_histogram);
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(single_bin_single_value_histogram.sliced_with_predicate(PredicateCondition::Equals, 61)));  // NOLINT

  test_slicing(histogram, PredicateCondition::NotEquals, 0, histogram);
  test_slicing(histogram, PredicateCondition::NotEquals, 26, histogram);
  test_slicing(histogram, PredicateCondition::NotEquals, 26, histogram);
  // The second bin could be shrunken to [30, 50], but original bounds are kept to keep implementation simple
  test_slicing(histogram, PredicateCondition::NotEquals, 30, GenericHistogram<int32_t>{{1, 31, 60, 80}, {25, 50, 60, 100}, {40, 29, 5, 10}, {10, 19, 1, 1}});  // NOLINT
  // The second bin could be split into two bins, but original bounds are kept to keep implementation simple
  test_slicing(histogram, PredicateCondition::NotEquals, 36, GenericHistogram<int32_t>{{1, 30, 60, 80}, {25, 50, 60, 100}, {40, 29, 5, 10}, {10, 19, 1, 1}});  // NOLINT
  test_slicing(single_bin_single_value_histogram, PredicateCondition::NotEquals, 101, single_bin_single_value_histogram);
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(single_bin_single_value_histogram.sliced_with_predicate(PredicateCondition::GreaterThan, 5)));

  test_slicing(histogram, PredicateCondition::GreaterThan, 60, GenericHistogram<int32_t>{{80}, {100}, {10}, {1}});  // NOLINT
  test_slicing(histogram, PredicateCondition::GreaterThan, 80, GenericHistogram<int32_t>{{81}, {100}, {10}, {1}});  // NOLINT
  test_slicing(histogram, PredicateCondition::GreaterThan, 99, GenericHistogram<int32_t>{{100}, {100}, {1}, {1}});  // NOLINT
  test_slicing(histogram, PredicateCondition::GreaterThan, 59, GenericHistogram<int32_t>{{60, 80}, {60, 100}, {5, 10}, {1, 1}});  // NOLINT
  test_slicing(histogram, PredicateCondition::GreaterThan, 0, histogram);
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(histogram.sliced_with_predicate(PredicateCondition::GreaterThan, 100)));  // NOLINT

  test_slicing(histogram, PredicateCondition::GreaterThanEquals, 60, GenericHistogram<int32_t>{{60, 80}, {60, 100}, {5, 10}, {1, 1}});  // NOLINT
  test_slicing(histogram, PredicateCondition::GreaterThanEquals, 99, GenericHistogram<int32_t>{{99}, {100}, {1}, {1}});  // NOLINT
  test_slicing(histogram, PredicateCondition::GreaterThanEquals, 100, GenericHistogram<int32_t>{{100}, {100}, {1}, {1}});  // NOLINT
  test_slicing(histogram, PredicateCondition::GreaterThanEquals, 1, histogram);
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(histogram.sliced_with_predicate(PredicateCondition::GreaterThanEquals, 101)));  // NOLINT

  test_slicing(histogram, PredicateCondition::LessThan, 50, GenericHistogram<int32_t>{{1, 30}, {25, 49}, {40, 29}, {10, 20}});  // NOLINT
  test_slicing(histogram, PredicateCondition::LessThan, 60, GenericHistogram<int32_t>{{1, 30}, {25, 50}, {40, 30}, {10, 20}});  // NOLINT
  test_slicing(histogram, PredicateCondition::LessThan, 61, GenericHistogram<int32_t>{{1, 30, 60}, {25, 50, 60}, {40, 30, 5}, {10, 20, 1}});  // NOLINT
  test_slicing(histogram, PredicateCondition::LessThan, 99, GenericHistogram<int32_t>{{1, 30, 60, 80}, {25, 50, 60, 98}, {40, 30, 5, 10}, {10, 20, 1, 1}});  // NOLINT
  test_slicing(histogram, PredicateCondition::LessThan, 1000, histogram);  // NOLINT
  test_slicing(histogram, PredicateCondition::LessThan, 2, GenericHistogram<int32_t>{{1}, {1}, {2}, {1}});
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(histogram.sliced_with_predicate(PredicateCondition::LessThan, 1)));  // NOLINT

  test_slicing(histogram, PredicateCondition::LessThanEquals, 24, GenericHistogram<int32_t>{{1}, {24}, {39}, {10}});  // NOLINT
  test_slicing(histogram, PredicateCondition::LessThanEquals, 25, GenericHistogram<int32_t>{{1}, {25}, {40}, {10}});  // NOLINT
  test_slicing(histogram, PredicateCondition::LessThanEquals, 60, GenericHistogram<int32_t>{{1, 30, 60}, {25, 50, 60}, {40, 30, 5}, {10, 20, 1}});  // NOLINT
  test_slicing(histogram, PredicateCondition::LessThanEquals, 97, GenericHistogram<int32_t>{{1, 30, 60, 80}, {25, 50, 60, 97}, {40, 30, 5, 9}, {10, 20, 1, 1}});  // NOLINT
  test_slicing(histogram, PredicateCondition::LessThanEquals, 100, histogram);  // NOLINT
  test_slicing(histogram, PredicateCondition::LessThanEquals, 2, GenericHistogram<int32_t>{{1}, {2}, {4}, {1}});
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(histogram.sliced_with_predicate(PredicateCondition::LessThanEquals, 0)));  // NOLINT

  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(histogram.sliced_with_predicate(PredicateCondition::Between, 0, 0)));  // NOLINT
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(histogram.sliced_with_predicate(PredicateCondition::Between, 26, 29)));  // NOLINT
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(histogram.sliced_with_predicate(PredicateCondition::Between, 101, 1000)));  // NOLINT
  test_slicing(histogram, PredicateCondition::Between, 1, 50, GenericHistogram<int32_t>{{1, 30}, {25, 50}, {40, 30}, {10, 20}});
  test_slicing(histogram, PredicateCondition::Between, 60, 60, GenericHistogram<int32_t>{{60}, {60}, {5}, {1}});
  test_slicing(histogram, PredicateCondition::Between, 1, 12, GenericHistogram<int32_t>{{1}, {12}, {20}, {5}});  // NOLINT
  test_slicing(histogram, PredicateCondition::Between, 21, 60, GenericHistogram<int32_t>{{21, 30, 60}, {25, 50, 60}, {8, 30, 5}, {2, 20, 1}});
  test_slicing(histogram, PredicateCondition::Between, 31, 99, GenericHistogram<int32_t>{{31, 60, 80}, {50, 60, 99}, {29, 5, 10}, {20, 1, 1}});
  // clang-format on
}

TEST_F(GenericHistogramTest, SlicedWithPredicateFloat) {
  // clang-format off
  const auto histogram = GenericHistogram<float>{{1.0f,            2.0f,   3.0f, 12.25f,       100.0f},
                                                 {1.0f, next_value(2.0f), 11.0f, 17.25f, 1'000'100.0f},
                                                 {5,              10,     32,    70,             1},
                                                 {1,               5,      4,    70,             1}};
  const auto single_bin_single_value_histogram = GenericHistogram<float>{{5.0f}, {5.0f}, {12}, {1}};
  // clang-format on

  // clang-format off
  test_slicing(histogram, PredicateCondition::Equals, 1.0f, GenericHistogram<float>{{1.0f}, {1.0f}, {5}, {1}});
  test_slicing(histogram, PredicateCondition::Equals, 12.25f, GenericHistogram<float>{{12.25f}, {12.25f}, {1}, {1}});
  test_slicing(histogram, PredicateCondition::Equals, 2.0f, GenericHistogram<float>{{2.0f}, {2.0f}, {2}, {1}});
  test_slicing(histogram, PredicateCondition::Equals, next_value(2.0f), GenericHistogram<float>{{next_value(2.0f)}, {next_value(2.0f)}, {2}, {1}});
  test_slicing(histogram, PredicateCondition::Equals, 4.2f, GenericHistogram<float>{{4.2f}, {4.2f}, {8}, {1}});
  test_slicing(histogram, PredicateCondition::Equals, 5'000.0f, GenericHistogram<float>{{5'000.0f}, {5'000.0f}, {1}, {1}});
  test_slicing(single_bin_single_value_histogram, PredicateCondition::Equals, 5.0f, single_bin_single_value_histogram);
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(histogram.sliced_with_predicate(PredicateCondition::Equals, 0.0f)));  // NOLINT
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(histogram.sliced_with_predicate(PredicateCondition::Equals, 11.1f)));  // NOLINT

  test_slicing(histogram, PredicateCondition::NotEquals, 0.0f, histogram);
  test_slicing(histogram, PredicateCondition::NotEquals, 2.0f, GenericHistogram<float>{{1.0f, next_value(2.0f), 3.0f, 12.25f, 100.0f}, {1.0f, next_value(2.0f), 11.0f, 17.25,  1'000'100.0f}, {5, 2, 32, 70, 1}, {1, 1, 4, 70, 1}});
  test_slicing(histogram, PredicateCondition::NotEquals, next_value(2.0f), GenericHistogram<float>{{1.0f, 2.0f, 3.0f, 12.25f, 100.0f}, {1.0f, 2.0f, 11.0f, 17.25,  1'000'100.0f}, {5, 2, 32, 70, 1}, {1, 1, 4, 70, 1}});
  test_slicing(histogram, PredicateCondition::NotEquals, 1.0f, GenericHistogram<float>{{2.0f, 3.0f, 12.25f, 100.0f}, {next_value(2.0f), 11.0f, 17.25,  1'000'100.0f}, {10, 32, 70, 1}, {5, 4, 70, 1}});
  test_slicing(histogram, PredicateCondition::NotEquals, 3.5f, GenericHistogram<float>{{1.0f, 2.0f, 3.0f, 12.25f, 100.0f}, {1.0f, next_value(2.0f), 11.0f, 17.25,  1'000'100.0f}, {5, 10, 24, 70, 1}, {1, 5, 3, 70, 1}});
  test_slicing(histogram, PredicateCondition::NotEquals, 100'000'000.5f, histogram);
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(single_bin_single_value_histogram.sliced_with_predicate(PredicateCondition::NotEquals, 5.0f)));  // NOLINT

  test_slicing(histogram, PredicateCondition::LessThan, 100'000'000.5f, histogram);
  test_slicing(single_bin_single_value_histogram, PredicateCondition::LessThan, 6.0f, single_bin_single_value_histogram);
  test_slicing(histogram, PredicateCondition::LessThan, next_value(1.0f), GenericHistogram<float>{{1.0f}, {1.0f}, {5}, {1}});
  test_slicing(histogram, PredicateCondition::LessThan, 2.0f, GenericHistogram<float>{{1.0f}, {1.0f}, {5}, {1}});
  test_slicing(histogram, PredicateCondition::LessThan, next_value(2.0f), GenericHistogram<float>{{1.0f, 2.0f}, {1.0f, 2.0f}, {5, 10}, {1, 1}});
  test_slicing(histogram, PredicateCondition::LessThan, next_value(3.0f), GenericHistogram<float>{{1.0f, 2.0f, 3.0f}, {1.0f, next_value(2.0f), 3.0f}, {5, 10, 1}, {1, 5, 1}});
  test_slicing(histogram, PredicateCondition::LessThan, 11.0f, GenericHistogram<float>{{1.0f, 2.0f, 3.0f}, {1.0f, next_value(2.0f), previous_value(11.0f)}, {5, 10, 32}, {1, 5, 4}});
  test_slicing(histogram, PredicateCondition::LessThan, 7.0f, GenericHistogram<float>{{1.0f, 2.0f, 3.0f}, {1.0f, next_value(2.0f), previous_value(7.0f)}, {5, 10, 16}, {1, 5, 2}});
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(histogram.sliced_with_predicate(PredicateCondition::LessThan, 1.0f)));  // NOLINT
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(histogram.sliced_with_predicate(PredicateCondition::LessThan, 0.09f)));  // NOLINT
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(single_bin_single_value_histogram.sliced_with_predicate(PredicateCondition::LessThan, 5.0f)));  // NOLINT

  // Testing LessThan above, we can assume it works. Now verify LessThanEquals using LessThan to keep the test simple(ish)
  test_slicing_has_equal_result(histogram, PredicateCondition::LessThan, 0.0f, PredicateCondition::LessThanEquals, previous_value(0.0f));
  test_slicing_has_equal_result(histogram, PredicateCondition::LessThan, 1.0f, PredicateCondition::LessThanEquals, previous_value(1.0f));
  test_slicing_has_equal_result(histogram, PredicateCondition::LessThan, next_value(1.0f), PredicateCondition::LessThanEquals, 1.0f);
  test_slicing_has_equal_result(histogram, PredicateCondition::LessThan, 2.0f, PredicateCondition::LessThanEquals, previous_value(2.0f));
  test_slicing_has_equal_result(histogram, PredicateCondition::LessThan, next_value(2.0f), PredicateCondition::LessThanEquals, 2.0f);
  test_slicing_has_equal_result(histogram, PredicateCondition::LessThan, 6.0f, PredicateCondition::LessThanEquals, previous_value(6.0f));
  test_slicing_has_equal_result(histogram, PredicateCondition::LessThan, 11.0f, PredicateCondition::LessThanEquals, previous_value(11.0f));
  test_slicing_has_equal_result(histogram, PredicateCondition::LessThan, 12.0f, PredicateCondition::LessThanEquals, previous_value(12.0f));
  test_slicing_has_equal_result(histogram, PredicateCondition::LessThan, next_value(12.25f), PredicateCondition::LessThanEquals, 12.25f);
  test_slicing_has_equal_result(histogram, PredicateCondition::LessThan, next_value(200.0f), PredicateCondition::LessThanEquals, 200.0f);
  test_slicing_has_equal_result(histogram, PredicateCondition::LessThan, 1'000'100.0f, PredicateCondition::LessThanEquals, previous_value(1'000'100.0f));
  test_slicing_has_equal_result(histogram, PredicateCondition::LessThan, next_value(1'000'100.0f), PredicateCondition::LessThanEquals, 1'000'100.0f);

  test_slicing(histogram, PredicateCondition::GreaterThan, 0.0f, histogram);
  test_slicing(histogram, PredicateCondition::GreaterThan, 1.0f, GenericHistogram<float>{{2.0f, 3.0f, 12.25f, 100.0f}, {next_value(2.0f), 11.0f, 17.25,  1'000'100.0f}, {10, 32, 70, 1}, {5, 4, 70, 1}});
  test_slicing(histogram, PredicateCondition::GreaterThan, 1.5f, GenericHistogram<float>{{2.0f, 3.0f, 12.25f, 100.0f}, {next_value(2.0f), 11.0f, 17.25,  1'000'100.0f}, {10, 32, 70, 1}, {5, 4, 70, 1}});
  test_slicing(histogram, PredicateCondition::GreaterThan, 2.0f, GenericHistogram<float>{{next_value(2.0f), 3.0f, 12.25f, 100.0f}, {next_value(2.0f), 11.0f, 17.25,  1'000'100.0f}, {1, 32, 70, 1}, {1, 4, 70, 1}});
  test_slicing(histogram, PredicateCondition::GreaterThan, 10.0f, GenericHistogram<float>{{next_value(10.0f), 12.25f, 100.0f}, {11.0f, 17.25,  1'000'100.0f}, {4, 70, 1}, {1, 70, 1}});
  test_slicing(histogram, PredicateCondition::GreaterThan, 10.1f, GenericHistogram<float>{{next_value(10.1f), 12.25f, 100.0f}, {11.0f, 17.25,  1'000'100.0f}, {4, 70, 1}, {1, 70, 1}});
  test_slicing(histogram, PredicateCondition::GreaterThan, 11.0f, GenericHistogram<float>{{12.25f, 100.0f}, {17.25,  1'000'100.0f}, {70, 1}, {70, 1}});
  test_slicing(histogram, PredicateCondition::GreaterThan, previous_value(1'000'100.0f), GenericHistogram<float>{{1'000'100.0f}, {1'000'100.0f}, {1}, {1}});
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(histogram.sliced_with_predicate(PredicateCondition::GreaterThan, 1'000'100.0f)));  // NOLINT
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(single_bin_single_value_histogram.sliced_with_predicate(PredicateCondition::GreaterThan, 6.0f)));  // NOLINT

  // Testing GreaterThan above, we can assume it works. Now verify GreaterThanEquals using LessThan to keep the test simple(ish)
  test_slicing_has_equal_result(histogram, PredicateCondition::GreaterThan, 0.0f, PredicateCondition::GreaterThanEquals, next_value(0.0f));
  test_slicing_has_equal_result(histogram, PredicateCondition::GreaterThan, 1.0f, PredicateCondition::GreaterThanEquals, next_value(1.0f));
  test_slicing_has_equal_result(histogram, PredicateCondition::GreaterThan, previous_value(1.0f), PredicateCondition::GreaterThanEquals, 1.0f);
  test_slicing_has_equal_result(histogram, PredicateCondition::GreaterThan, 2.0f, PredicateCondition::GreaterThanEquals, next_value(2.0f));
  test_slicing_has_equal_result(histogram, PredicateCondition::GreaterThan, previous_value(2.0f), PredicateCondition::GreaterThanEquals, 2.0f);
  test_slicing_has_equal_result(histogram, PredicateCondition::GreaterThan, 6.0f, PredicateCondition::GreaterThanEquals, next_value(6.0f));
  test_slicing_has_equal_result(histogram, PredicateCondition::GreaterThan, 11.0f, PredicateCondition::GreaterThanEquals, next_value(11.0f));
  test_slicing_has_equal_result(histogram, PredicateCondition::GreaterThan, 12.0f, PredicateCondition::GreaterThanEquals, next_value(12.0f));
  test_slicing_has_equal_result(histogram, PredicateCondition::GreaterThan, previous_value(12.25f), PredicateCondition::GreaterThanEquals, 12.25f);
  test_slicing_has_equal_result(histogram, PredicateCondition::GreaterThan, previous_value(200.0f), PredicateCondition::GreaterThanEquals, 200.0f);
  test_slicing_has_equal_result(histogram, PredicateCondition::GreaterThan, 1'000'100.0f, PredicateCondition::GreaterThanEquals, next_value(1'000'100.0f));
  test_slicing_has_equal_result(histogram, PredicateCondition::GreaterThan, previous_value(1'000'100.0f), PredicateCondition::GreaterThanEquals, 1'000'100.0f);
  // clang-format on
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

TEST_F(GenericHistogramTest, SplitAtBinBoundsTwoHistograms) {
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
