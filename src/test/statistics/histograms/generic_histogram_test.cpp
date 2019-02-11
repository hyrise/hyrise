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

namespace {

using namespace opossum;  // NOLINT

struct Predicate {
  PredicateCondition predicate_condition;
  AllTypeVariant value;
  std::optional<AllTypeVariant> value2;
};

}  // namespace

namespace opossum {


class GenericHistogramTest : public BaseTest {
 public:
  std::string predicate_to_string(const Predicate& predicate) {
    std::ostringstream stream;
    stream << predicate_condition_to_string.left.at(predicate.predicate_condition) << " " << predicate.value;
    if (predicate.value2) {
      stream << " " << *predicate.value2;
    }

    return stream.str();
  }

  template<typename T>
  void test_sliced_with_predicates(const std::vector<std::shared_ptr<AbstractHistogram<T>>>& histograms, const std::vector<Predicate>& predicates) {
    for (const auto& predicate : predicates) {
      SCOPED_TRACE(predicate_to_string(predicate));

      for (const auto& histogram : histograms) {
        SCOPED_TRACE(histogram->description(true));

        const auto sliced_statistics_object = histogram->sliced(predicate.predicate_condition,
                                                                               predicate.value, predicate.value2);
        const auto cardinality = histogram->estimate_cardinality(predicate.predicate_condition, predicate.value,
                                                                 predicate.value2).cardinality;

        if (const auto sliced_histogram = std::dynamic_pointer_cast<AbstractHistogram<T>>(
        sliced_statistics_object)) {
          SCOPED_TRACE(sliced_histogram->description(true));
          EXPECT_NEAR(sliced_histogram->total_count(), cardinality, 0.005f);
        } else {
          EXPECT_EQ(cardinality, 0.0f);
        }
      }
    }
  }
};

TEST_F(GenericHistogramTest, EstimateCardinalityInt) {
  // clang-format off
  const auto histogram = GenericHistogram<int32_t>(
    {2,  21, 37,  101, 105},
    {20, 25, 100, 103, 105},
    {17, 30, 40,  1,     5},
    { 5,  3, 27,  1,     1});

  const auto histogram_zeros = GenericHistogram<int32_t>(
    {2,    21,     37},
    {20,   25,    100},
    {0.0f,  6.0f,   0.0f},
    {5.0f,  0.0f,   0.0f});
  // clang-format on

  const auto total_count = histogram.total_count();

  // clang-format off
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 1).cardinality, 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 3).cardinality, 17.0f / 5.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 26).cardinality, 0.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 105).cardinality, 5.0f);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::Equals, 200).cardinality, 0.0f);
  EXPECT_EQ(histogram_zeros.estimate_cardinality(PredicateCondition::Equals, 2).cardinality, 0.0f);
  EXPECT_EQ(histogram_zeros.estimate_cardinality(PredicateCondition::Equals, 21).cardinality, 0.0f);
  EXPECT_EQ(histogram_zeros.estimate_cardinality(PredicateCondition::Equals, 37).cardinality, 0.0f);

  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotEquals, 1).cardinality, total_count);
  EXPECT_EQ(histogram.estimate_cardinality(PredicateCondition::NotEquals, 21).cardinality, total_count - 10);
  EXPECT_EQ(histogram_zeros.estimate_cardinality(PredicateCondition::NotEquals, 2).cardinality, 6.0f);
  EXPECT_EQ(histogram_zeros.estimate_cardinality(PredicateCondition::NotEquals, 21).cardinality, 6.0f);
  EXPECT_EQ(histogram_zeros.estimate_cardinality(PredicateCondition::NotEquals, 37).cardinality, 6.0f);

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
  StringHistogramDomain{"abcdefghijklmnopqrstuvwxyz", 2u});
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

TEST_F(GenericHistogramTest, SlicedInt) {
  // clang-format off

  // Normal histogram, no special cases here
  const auto histogram_a = std::make_shared<GenericHistogram<int32_t>>(
    std::vector<int32_t>            { 1, 31, 60, 80},
    std::vector<int32_t>            {25, 50, 60, 99},
    std::vector<HistogramCountType> {40, 30, 5,  10},
    std::vector<HistogramCountType> {10, 20, 1,  1}
  );

  // Histogram with zeros in bin height/distinct_count
  const auto histogram_b = std::make_shared<GenericHistogram<int32_t>>(
    std::vector<int32_t>            { 0,  6, 30, 51},
    std::vector<int32_t>            { 5, 20, 50, 52},
    std::vector<HistogramCountType> { 4,  0,  0,  0.000001f},
    std::vector<HistogramCountType> { 0, 20,  0,  0.000001f}
  );

  // Histogram with a single bin, which in turn has a single value
  const auto histogram_c = std::make_shared<GenericHistogram<int32_t>>(
    std::vector<int32_t>            { 5},
    std::vector<int32_t>            {15},
    std::vector<HistogramCountType> { 1},
    std::vector<HistogramCountType> { 1}
  );

  // Empty histogram
  const auto histogram_d = std::make_shared<GenericHistogram<int32_t>>(
    std::vector<int32_t>            {},
    std::vector<int32_t>            {},
    std::vector<HistogramCountType> {},
    std::vector<HistogramCountType> {}
  );
  // clang-format on

  std::vector<Predicate> predicates{
    Predicate{PredicateCondition::Equals, -50, std::nullopt},
    Predicate{PredicateCondition::Equals, 5, std::nullopt},
    Predicate{PredicateCondition::Equals, 15, std::nullopt},
    Predicate{PredicateCondition::Equals, 60, std::nullopt},
    Predicate{PredicateCondition::Equals, 61, std::nullopt},
    Predicate{PredicateCondition::Equals, 150, std::nullopt},
    Predicate{PredicateCondition::NotEquals, 0, std::nullopt},
    Predicate{PredicateCondition::NotEquals, 26, std::nullopt},
    Predicate{PredicateCondition::NotEquals, 31, std::nullopt},
    Predicate{PredicateCondition::NotEquals, 36, std::nullopt},
    Predicate{PredicateCondition::NotEquals, 101, std::nullopt},
    Predicate{PredicateCondition::GreaterThan, 0, std::nullopt},
    Predicate{PredicateCondition::GreaterThan, 59, std::nullopt},
    Predicate{PredicateCondition::GreaterThan, 60, std::nullopt},
    Predicate{PredicateCondition::GreaterThan, 81, std::nullopt},
    Predicate{PredicateCondition::GreaterThan, 98, std::nullopt},
    Predicate{PredicateCondition::GreaterThanEquals, 1, std::nullopt},
    Predicate{PredicateCondition::GreaterThanEquals, 5, std::nullopt},
    Predicate{PredicateCondition::GreaterThanEquals, 60, std::nullopt},
    Predicate{PredicateCondition::GreaterThanEquals, 99, std::nullopt},
    Predicate{PredicateCondition::LessThan, 1, std::nullopt},
    Predicate{PredicateCondition::LessThan, 6, std::nullopt},
    Predicate{PredicateCondition::LessThan, 50, std::nullopt},
    Predicate{PredicateCondition::LessThan, 60, std::nullopt},
    Predicate{PredicateCondition::LessThan, 61, std::nullopt},
    Predicate{PredicateCondition::LessThan, 99, std::nullopt},
    Predicate{PredicateCondition::LessThan, 1000, std::nullopt},
    Predicate{PredicateCondition::LessThanEquals, 0, std::nullopt},
    Predicate{PredicateCondition::LessThanEquals, 2, std::nullopt},
    Predicate{PredicateCondition::LessThanEquals, 24, std::nullopt},
    Predicate{PredicateCondition::LessThanEquals, 25, std::nullopt},
    Predicate{PredicateCondition::LessThanEquals, 60, std::nullopt},
    Predicate{PredicateCondition::LessThanEquals, 97, std::nullopt},
    Predicate{PredicateCondition::LessThanEquals, 99, std::nullopt},
    Predicate{PredicateCondition::Between, 0, 0},
    Predicate{PredicateCondition::Between, 26, 29},
    Predicate{PredicateCondition::Between, 100, 1000},
    Predicate{PredicateCondition::Between, 1, 20},
    Predicate{PredicateCondition::Between, 1, 50},
    Predicate{PredicateCondition::Between, 21, 60},
    Predicate{PredicateCondition::Between, 60, 60},
    Predicate{PredicateCondition::Between, 32, 99},
  };

  std::vector<std::shared_ptr<AbstractHistogram<int32_t>>>histograms{
    histogram_a, histogram_b, histogram_c, histogram_d
  };

  test_sliced_with_predicates(histograms, predicates);
}

TEST_F(GenericHistogramTest, SlicedFloat) {
  // clang-format off
  const auto histogram_a = std::make_shared<GenericHistogram<float>>(
    std::vector<float>             {1.0f,            2.0f,   3.0f, 12.25f,       100.0f},
    std::vector<float>             {1.0f, next_value(2.0f), 11.0f, 17.25f, 1'000'100.0f},
    std::vector<HistogramCountType>{5,              10,     32,    70,             1},
    std::vector<HistogramCountType>{1,               5,      4,    70,             1}
  );

  // Histogram with zeros in bin height/distinct_count
  const auto histogram_b = std::make_shared<GenericHistogram<float>>(
  std::vector<float>              { 0,  6, 30, 51},
  std::vector<float>              { 5, 20, 50, 52},
  std::vector<HistogramCountType> { 4,  0,  0,  0.000001f},
  std::vector<HistogramCountType> { 0, 20,  0,  0.000001f}
  );

  const auto histogram_c = std::make_shared<GenericHistogram<float>>(
    std::vector<float>             {5.0f},
    std::vector<float>             {5.0f},
    std::vector<HistogramCountType>{12},
    std::vector<HistogramCountType>{1}
  );

  const auto histogram_d = std::make_shared<GenericHistogram<float>>(
    std::vector<float>             {},
    std::vector<float>             {},
    std::vector<HistogramCountType>{},
    std::vector<HistogramCountType>{}
  );
  // clang-format on

  std::vector<std::shared_ptr<AbstractHistogram<float>>> histograms{histogram_a, histogram_b, histogram_c, histogram_d};

  std::vector<Predicate> predicates{
    Predicate{PredicateCondition::Equals, 1.0f, std::nullopt},
    Predicate{PredicateCondition::Equals, 2.0f, std::nullopt},
    Predicate{PredicateCondition::Equals, next_value(2.0f), std::nullopt},
    Predicate{PredicateCondition::Equals, 4.2f, std::nullopt},
    Predicate{PredicateCondition::Equals, 5.0f, std::nullopt},
    Predicate{PredicateCondition::Equals, 11.1f, std::nullopt},
    Predicate{PredicateCondition::Equals, 5'000.0f, std::nullopt},
    Predicate{PredicateCondition::NotEquals, 0.0f, std::nullopt},
    Predicate{PredicateCondition::NotEquals, 1.0f, std::nullopt},
    Predicate{PredicateCondition::NotEquals, 2.0f, std::nullopt},
    Predicate{PredicateCondition::NotEquals, next_value(2.0f), std::nullopt},
    Predicate{PredicateCondition::NotEquals, 3.5f, std::nullopt},
    Predicate{PredicateCondition::NotEquals, 5.0f, std::nullopt},
    Predicate{PredicateCondition::NotEquals, 100'000'000.5f, std::nullopt},
    Predicate{PredicateCondition::LessThan, 1.0f, std::nullopt},
    Predicate{PredicateCondition::LessThan, next_value(1.0f), std::nullopt},
    Predicate{PredicateCondition::LessThan, 2.0f, std::nullopt},
    Predicate{PredicateCondition::LessThan, next_value(2.0f), std::nullopt},
    Predicate{PredicateCondition::LessThan, next_value(3.0f), std::nullopt},
    Predicate{PredicateCondition::LessThan, 5.0f, std::nullopt},
    Predicate{PredicateCondition::LessThan, 6.0f, std::nullopt},
    Predicate{PredicateCondition::LessThan, 7.0f, std::nullopt},
    Predicate{PredicateCondition::LessThanEquals, previous_value(0.0f), std::nullopt},
    Predicate{PredicateCondition::LessThanEquals, previous_value(1.0f), std::nullopt},
    Predicate{PredicateCondition::LessThanEquals, 1.0f, std::nullopt},
    Predicate{PredicateCondition::LessThanEquals, previous_value(2.0f), std::nullopt},
    Predicate{PredicateCondition::LessThanEquals, 2.0f, std::nullopt},
    Predicate{PredicateCondition::LessThanEquals, previous_value(6.0f), std::nullopt},
    Predicate{PredicateCondition::LessThanEquals, previous_value(11.0f), std::nullopt},
    Predicate{PredicateCondition::LessThanEquals, previous_value(12.25f), std::nullopt},
    Predicate{PredicateCondition::LessThanEquals, 12.25, std::nullopt},
    Predicate{PredicateCondition::LessThanEquals, 200.0f, std::nullopt},
    Predicate{PredicateCondition::LessThanEquals, 1'000'100.0f, std::nullopt},
    Predicate{PredicateCondition::GreaterThan, 0.0f, std::nullopt},
    Predicate{PredicateCondition::GreaterThan, 1.0f, std::nullopt},
    Predicate{PredicateCondition::GreaterThan, 1.5f, std::nullopt},
    Predicate{PredicateCondition::GreaterThan, 2.0f, std::nullopt},
    Predicate{PredicateCondition::GreaterThan, 6.0f, std::nullopt},
    Predicate{PredicateCondition::GreaterThan, 10.0f, std::nullopt},
    Predicate{PredicateCondition::GreaterThan, 10.1f, std::nullopt},
    Predicate{PredicateCondition::GreaterThan, 11.0f, std::nullopt},
    Predicate{PredicateCondition::GreaterThan, previous_value(1'000'100.0f), std::nullopt},
    Predicate{PredicateCondition::GreaterThanEquals, 0.0f, std::nullopt},
    Predicate{PredicateCondition::GreaterThanEquals, 1.0f, std::nullopt},
    Predicate{PredicateCondition::GreaterThanEquals, previous_value(1.0f), std::nullopt},
    Predicate{PredicateCondition::GreaterThanEquals, previous_value(2.0f), std::nullopt},
    Predicate{PredicateCondition::GreaterThanEquals, 11.0f, std::nullopt},
    Predicate{PredicateCondition::GreaterThanEquals, 200.0f, std::nullopt},
    Predicate{PredicateCondition::GreaterThanEquals, previous_value(1'000'100.0f), std::nullopt},
    Predicate{PredicateCondition::GreaterThanEquals, 1'000'100.0f, std::nullopt},
    Predicate{PredicateCondition::Between, 0.0f, 1'000'100.0f},
    Predicate{PredicateCondition::Between, 1.0f, 1'000'100.0f},
    Predicate{PredicateCondition::Between, 2.0f, 1'000'100.0f},
    Predicate{PredicateCondition::Between, 2.0f, 50.0f},
    Predicate{PredicateCondition::Between, 3.0f, 11.00f},
    Predicate{PredicateCondition::Between, 10.0f, 50.0f},
    Predicate{PredicateCondition::Between, 20.0f, 50.0f},
  };

  test_sliced_with_predicates(histograms, predicates);

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
  const auto expected_heights = std::vector<HistogramCountType>{14.4f, 9.6f, 16.0f, 8.57143f, 21.42857f, 20.0f, 10};
  const auto expected_distinct_counts = std::vector<HistogramCountType>{3.6f, 2.4f, 4.0f, 5.7142859f, 14.285714f, 15, 5};

  const auto new_hist = hist->split_at_bin_bounds(std::vector<std::pair<int32_t, int32_t>>{{10, 15}, {28, 35}});

  EXPECT_EQ(new_hist->bin_count(), expected_minima.size());

  for (auto bin_id = BinID{0}; bin_id < expected_minima.size(); bin_id++) {
    EXPECT_EQ(new_hist->bin_minimum(bin_id), expected_minima[bin_id]);
    EXPECT_EQ(new_hist->bin_maximum(bin_id), expected_maxima[bin_id]);
    EXPECT_FLOAT_EQ(new_hist->bin_height(bin_id), expected_heights[bin_id]);
    EXPECT_FLOAT_EQ(new_hist->bin_distinct_count(bin_id), expected_distinct_counts[bin_id]);
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
