#include <memory>
#include <string>

#include "all_type_variant.hpp"
#include "base_test.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"
#include "statistics/statistics_objects/abstract_statistics_object.hpp"
#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"
#include "statistics/statistics_objects/generic_histogram.hpp"
#include "statistics/statistics_objects/histogram_domain.hpp"
#include "statistics/statistics_objects/scaled_histogram.hpp"
#include "types.hpp"

namespace hyrise {

class ScaledHistogramTest : public BaseTestWithParam<DataType> {
 public:
  void SetUp() override {
    auto bin_minima = std::vector<int32_t>{10, 30, 60};
    auto bin_maxima = std::vector<int32_t>{20, 50, 100};
    auto bin_heights = std::vector<HistogramCountType>{13, 12, 65};
    auto bin_distinct_counts = std::vector<HistogramCountType>{3, 4, 5};
    _referenced_histogram = std::make_shared<GenericHistogram<int32_t>>(
        std::move(bin_minima), std::move(bin_maxima), std::move(bin_heights), std::move(bin_distinct_counts));
  }

  template <typename T>
  static std::shared_ptr<const AbstractHistogram<T>> referenced_histogram(const ScaledHistogram<T>& scaled_histogram) {
    return scaled_histogram._referenced_histogram;
  }

  template <typename T>
  static Selectivity selectivity(const ScaledHistogram<T>& scaled_histogram) {
    return scaled_histogram._selectivity;
  }

 protected:
  std::shared_ptr<GenericHistogram<int32_t>> _referenced_histogram;
};

INSTANTIATE_TEST_SUITE_P(ScaledHistogramTestDataType, ScaledHistogramTest,
                         testing::Values(DataType::Int, DataType::Long, DataType::Float, DataType::Double,
                                         DataType::String),
                         enum_formatter<DataType>);

TEST_F(ScaledHistogramTest, Name) {
  const auto histogram = ScaledHistogram<int32_t>{*_referenced_histogram, 0.5f};
  EXPECT_EQ(histogram.name(), "Scaled");
}

TEST_F(ScaledHistogramTest, FromReferencedHistogram) {
  const auto scaled_histogram = ScaledHistogram<int32_t>::from_referenced_histogram(*_referenced_histogram, 0.2f);

  const auto bin_count = scaled_histogram->bin_count();
  EXPECT_EQ(bin_count, _referenced_histogram->bin_count());
  EXPECT_FLOAT_EQ(scaled_histogram->total_count(), _referenced_histogram->total_count() * 0.2f);

  auto expected_distinct_count = HistogramCountType{0};
  for (auto bin_id = BinID{0}; bin_id < bin_count; ++bin_id) {
    EXPECT_EQ(scaled_histogram->bin_minimum(bin_id), _referenced_histogram->bin_minimum(bin_id));
    EXPECT_EQ(scaled_histogram->bin_maximum(bin_id), _referenced_histogram->bin_maximum(bin_id));

    const auto bin_height = scaled_histogram->bin_height(bin_id);
    EXPECT_FLOAT_EQ(bin_height, _referenced_histogram->bin_height(bin_id) * 0.2f);

    const auto expected_bin_distinct_count = std::min(_referenced_histogram->bin_distinct_count(bin_id), bin_height);
    expected_distinct_count += expected_bin_distinct_count;
    EXPECT_FLOAT_EQ(scaled_histogram->bin_distinct_count(bin_id), expected_bin_distinct_count);
    EXPECT_LE(scaled_histogram->bin_distinct_count(bin_id), bin_height);
  }

  for (const auto value : std::vector<int32_t>{0, 10, 15, 20, 25, 30, 40, 55, 65, 105}) {
    EXPECT_EQ(scaled_histogram->bin_for_value(value), _referenced_histogram->bin_for_value(value));
    EXPECT_EQ(scaled_histogram->next_bin_for_value(value), _referenced_histogram->next_bin_for_value(value));
  }

  EXPECT_FLOAT_EQ(scaled_histogram->total_distinct_count(), expected_distinct_count);
}

TEST_F(ScaledHistogramTest, FromScaling) {
  const auto scaled_histogram =
      std::dynamic_pointer_cast<const ScaledHistogram<int32_t>>(_referenced_histogram->scaled(0.5f));
  ASSERT_TRUE(scaled_histogram);
  EXPECT_EQ(referenced_histogram(*scaled_histogram), _referenced_histogram);
  EXPECT_FLOAT_EQ(selectivity(*scaled_histogram), 0.5f);
}

TEST_F(ScaledHistogramTest, SingleIndirection) {
  const auto scaled_histogram_a =
      std::dynamic_pointer_cast<const AbstractHistogram<int32_t>>(_referenced_histogram->scaled(0.4f));
  const auto scaled_histogram_b = ScaledHistogram<int32_t>::from_referenced_histogram(*scaled_histogram_a, 0.25f);

  EXPECT_EQ(referenced_histogram(*scaled_histogram_b), _referenced_histogram);
  EXPECT_FLOAT_EQ(selectivity(*scaled_histogram_b), 0.1f);
}

TEST_F(ScaledHistogramTest, CopiesDomain) {
  const auto domain = StringHistogramDomain{'a', 'y', 3};
  const auto generic_histogram = GenericHistogram<pmr_string>::with_single_bin("giraffe", "platypus", 10, 5, domain);
  const auto scaled_histogram = ScaledHistogram<pmr_string>::from_referenced_histogram(*generic_histogram, 0.5f);

  const auto actual_domain = static_cast<const StringHistogramDomain&>(scaled_histogram->domain());
  const auto expected_domain = static_cast<const StringHistogramDomain&>(generic_histogram->domain());

  EXPECT_EQ(actual_domain, expected_domain);
}

TEST_F(ScaledHistogramTest, BinHeightAndDistinctCount) {
  // The distinct count of a bin can usually not be adjusted by scaling and remains the same. However, it must not be
  // higher than the bin height, i.e., the total number of elements in the bin.
  const auto scaled_histogram = ScaledHistogram<int32_t>::from_referenced_histogram(*_referenced_histogram, 0.1f);

  //   BinID                |  0   |  1   |  2
  //  ----------------------+------+------+------
  //   original height      | 13   | 12   | 65
  //   original dist. count |  3   |  4   |  5
  //   scaled height        |  1.3 |  1.2 |  6.5
  //   scaled dist. count   |  1.3 |  1.2 |  5

  EXPECT_FLOAT_EQ(scaled_histogram->bin_height(BinID{0}), 1.3f);
  EXPECT_FLOAT_EQ(scaled_histogram->bin_distinct_count(BinID{0}), 1.3f);
  EXPECT_FLOAT_EQ(scaled_histogram->bin_height(BinID{1}), 1.2f);
  EXPECT_FLOAT_EQ(scaled_histogram->bin_distinct_count(BinID{1}), 1.2f);
  EXPECT_FLOAT_EQ(scaled_histogram->bin_height(BinID{2}), 6.5f);
  EXPECT_FLOAT_EQ(scaled_histogram->bin_distinct_count(BinID{2}), 5.0f);
}

TEST_P(ScaledHistogramTest, ReferencesGenericHistogram) {
  resolve_data_type(GetParam(), [&](auto type) {
    using HistogramDataType = typename decltype(type)::type;

    const auto min = HistogramDataType{70};
    const auto max = HistogramDataType{80};
    const auto generic_histogram = GenericHistogram<HistogramDataType>::with_single_bin(min, max, 10, 5);

    const auto scaled_histogram = ScaledHistogram<HistogramDataType>{*generic_histogram, 0.5f};
    EXPECT_EQ(referenced_histogram(scaled_histogram), generic_histogram);
    EXPECT_EQ(selectivity(scaled_histogram), 0.5f);
  });
}

TEST_P(ScaledHistogramTest, ReferencesEqualDistinctCountHistogram) {
  resolve_data_type(GetParam(), [&](auto type) {
    using HistogramDataType = typename decltype(type)::type;

    auto bin_minima = std::vector{HistogramDataType{70}};
    auto bin_maxima = std::vector{HistogramDataType{80}};
    auto bin_heights = std::vector{HistogramCountType{5}};
    const auto equal_distinct_count_histogram = std::make_shared<EqualDistinctCountHistogram<HistogramDataType>>(
        std::move(bin_minima), std::move(bin_maxima), std::move(bin_heights), 5, BinID{0});

    const auto scaled_histogram = ScaledHistogram<HistogramDataType>{*equal_distinct_count_histogram, 0.5f};
    EXPECT_EQ(referenced_histogram(scaled_histogram), equal_distinct_count_histogram);
    EXPECT_EQ(selectivity(scaled_histogram), 0.5f);
  });
}

}  // namespace hyrise
