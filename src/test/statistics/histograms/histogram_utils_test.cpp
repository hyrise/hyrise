#include <string>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/histograms/generic_histogram.hpp"
#include "statistics/histograms/histogram_utils.hpp"

using namespace opossum::histogram;  // NOLINT

namespace opossum {

class HistogramUtilsTest : public BaseTest {
 public:
  void SetUp() override {
    int_histograms.emplace_back(std::make_shared<GenericHistogram<int32_t>>(
      std::vector<int32_t>           {1, 6, 11},
      std::vector<int32_t>           {3, 6, 18},
      std::vector<HistogramCountType>{3, 4, 30},
      std::vector<HistogramCountType>{2, 1,  5}
    ));

    int_histograms.emplace_back(std::make_shared<GenericHistogram<int32_t>>(
      std::vector<int32_t>           { 1,   5,  8, 11, 16, 18, 22},
      std::vector<int32_t>           { 3,   6,  9, 13, 16, 20, 22},
      std::vector<HistogramCountType>{10,  20, 20, 40, 10, 60, 10},
      std::vector<HistogramCountType>{ 3,   2,  2,  2,  1,  2,  1}
    ));

    int_histograms.emplace_back(std::make_shared<GenericHistogram<int32_t>>(
      std::vector<int32_t>           {1, 2, 3, 6, 8},
      std::vector<int32_t>           {1, 2, 5, 7, 9},
      std::vector<HistogramCountType>{2, 1, 2, 0, 0},
      std::vector<HistogramCountType>{1, 1, 0, 5, 0}
    ));

    int_histograms.emplace_back(std::make_shared<GenericHistogram<int32_t>>(
      std::vector<int32_t>           {2, 3, 4},
      std::vector<int32_t>           {2, 3, 9},
      std::vector<HistogramCountType>{1, 2, 2},
      std::vector<HistogramCountType>{1, 1, 0}
    ));


    float_histograms.emplace_back(std::make_shared<GenericHistogram<float>>(
      std::vector<float>             {0.0f, 5.5f, 14.5f},
      std::vector<float>             {3.5f, 12.0f, 16.0f},
      std::vector<HistogramCountType>{14, 52, 3},
      std::vector<HistogramCountType>{14, 52, 3}
    ));

    float_histograms.emplace_back(std::make_shared<GenericHistogram<float>>(
      std::vector<float>             {0.5f, 7.0f, next_value(9.5f)},
      std::vector<float>             {4.0, 9.5f, 11.5f},
      std::vector<HistogramCountType>{21, 7, 20},
      std::vector<HistogramCountType>{21, 7, 20}
    ));

    float_histograms.emplace_back(std::make_shared<GenericHistogram<float>>(
      std::vector<float>             {2.0f,  5.0f},
      std::vector<float>             {3.5f, 10.0f},
      std::vector<HistogramCountType>{4,    41},
      std::vector<HistogramCountType>{4,    41}
    ));

    float_histograms.emplace_back(std::make_shared<GenericHistogram<float>>(
      std::vector<float>             {3.5f,                5.5f, next_value(6.0f)},
      std::vector<float>             {4.0f, previous_value(6.0f),           7.0f},
      std::vector<HistogramCountType>{5,                  25,              15},
      std::vector<HistogramCountType>{5,                  25,              15}
    ));
  }

  std::vector<std::shared_ptr<AbstractHistogram<int32_t>>> int_histograms;
  std::vector<std::shared_ptr<AbstractHistogram<float>>> float_histograms;
};

TEST_F(HistogramUtilsTest, MergeHistogramsInt) {
  for (const auto& histogram_l : int_histograms) {
    SCOPED_TRACE(histogram_l->description(true));

    for (const auto& histogram_r : int_histograms) {
      SCOPED_TRACE(histogram_r->description(true));

      const auto merged_histogram = merge_histograms(*histogram_l, *histogram_r);

      SCOPED_TRACE(merged_histogram->description(true));

      EXPECT_FLOAT_EQ(merged_histogram->total_count(), histogram_l->total_count() + histogram_r->total_count());
    }
  }
}

TEST_F(HistogramUtilsTest, MergeHistogramsFloat) {
  for (const auto& histogram_l : float_histograms) {
    SCOPED_TRACE(histogram_l->description(true));

    for (const auto& histogram_r : float_histograms) {
      SCOPED_TRACE(histogram_r->description(true));

      const auto merged_histogram = merge_histograms(*histogram_l, *histogram_r);
      SCOPED_TRACE(merged_histogram->description(true));

      EXPECT_FLOAT_EQ(merged_histogram->total_count(), histogram_l->total_count() + histogram_r->total_count());
    }
  }
}

TEST_F(HistogramUtilsTest, ReduceHistogramInt) {
  for (const auto target_bin_count : {1, 2, 3, 6, 20}) {
    SCOPED_TRACE(target_bin_count);
    for (const auto& histogram : int_histograms) {
      SCOPED_TRACE(histogram->description(true));

      const auto reduced_histogram = reduce_histogram(*histogram, target_bin_count);
      SCOPED_TRACE(reduced_histogram->description(true));

      EXPECT_LE(reduced_histogram->bin_count(), target_bin_count);
      EXPECT_FLOAT_EQ(reduced_histogram->total_count(), histogram->total_count());
    }
  }
}

TEST_F(HistogramUtilsTest, ReduceHistogramFloat) {
  for (const auto target_bin_count : {1, 2, 3, 6, 20}) {
    SCOPED_TRACE(target_bin_count);
    for (const auto& histogram : float_histograms) {
      SCOPED_TRACE(histogram->description(true));

      const auto reduced_histogram = reduce_histogram(*histogram, target_bin_count);
      SCOPED_TRACE(reduced_histogram->description(true));

      EXPECT_LE(reduced_histogram->bin_count(), target_bin_count);
      EXPECT_FLOAT_EQ(reduced_histogram->total_count(), histogram->total_count());
    }
  }
}

}  // namespace opossum
