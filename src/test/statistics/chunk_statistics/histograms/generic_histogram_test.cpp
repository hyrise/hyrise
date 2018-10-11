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

}  // namespace opossum
