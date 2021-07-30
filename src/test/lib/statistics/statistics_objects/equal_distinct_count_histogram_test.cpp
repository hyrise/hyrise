#include <limits>
#include <memory>
#include <string>

#include "base_test.hpp"

#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"
#include "statistics/statistics_objects/generic_histogram.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class EqualDistinctCountHistogramTest : public BaseTest {
  void SetUp() override {
    _int_float4 = load_table("resources/test_data/tbl/int_float4.tbl");
    _int_float4_multichunk = load_table("resources/test_data/tbl/int_float4.tbl", 5);
    _int_float_multichunk_large = load_table("resources/test_data/tbl/int_float_merging_hist.tbl", 6);
    _int_float_multichunk_large_small_chunks = load_table("resources/test_data/tbl/int_float_merging_hist.tbl", 4);
    _float2 = load_table("resources/test_data/tbl/float2.tbl");
    _string2 = load_table("resources/test_data/tbl/string2.tbl");
  }

 protected:
  std::shared_ptr<Table> _int_float4;
  std::shared_ptr<Table> _int_float4_multichunk;
  std::shared_ptr<Table> _int_float_multichunk_large;
  std::shared_ptr<Table> _int_float_multichunk_large_small_chunks;
  std::shared_ptr<Table> _float2;
  std::shared_ptr<Table> _string2;
};

TEST_F(EqualDistinctCountHistogramTest, FromColumnString) {
  StringHistogramDomain default_domain;
  const auto default_domain_histogram =
      EqualDistinctCountHistogram<pmr_string>::from_column(*_string2, ColumnID{0}, 4u, default_domain);

  ASSERT_EQ(default_domain_histogram->bin_count(), 4u);
  EXPECT_EQ(default_domain_histogram->bin(BinID{0}), HistogramBin<pmr_string>("aa", "birne", 3, 3));
  EXPECT_EQ(default_domain_histogram->bin(BinID{1}), HistogramBin<pmr_string>("bla", "ttt", 4, 3));
  EXPECT_EQ(default_domain_histogram->bin(BinID{2}), HistogramBin<pmr_string>("uuu", "xxx", 4, 3));

  StringHistogramDomain reduced_histogram{'a', 'c', 9};
  const auto reduced_domain_histogram =
      EqualDistinctCountHistogram<pmr_string>::from_column(*_string2, ColumnID{0}, 4u, reduced_histogram);

  ASSERT_EQ(reduced_domain_histogram->bin_count(), 4u);
  EXPECT_EQ(reduced_domain_histogram->bin(BinID{0}), HistogramBin<pmr_string>("aa", "b", 2, 2));
  EXPECT_EQ(reduced_domain_histogram->bin(BinID{1}), HistogramBin<pmr_string>("bca", "bccc", 2, 2));
  EXPECT_EQ(reduced_domain_histogram->bin(BinID{2}), HistogramBin<pmr_string>("bcccc", "bcccc", 1, 1));
  EXPECT_EQ(reduced_domain_histogram->bin(BinID{3}), HistogramBin<pmr_string>("ccc", "ccc", 10, 1));
}

TEST_F(EqualDistinctCountHistogramTest, FromColumnInt) {
  const auto hist = EqualDistinctCountHistogram<int32_t>::from_column(*_int_float4, ColumnID{0}, 2u);

  ASSERT_EQ(hist->bin_count(), 2u);
  EXPECT_EQ(hist->bin(BinID{0}), HistogramBin<int32_t>(12, 123, 2, 2));
  EXPECT_EQ(hist->bin(BinID{1}), HistogramBin<int32_t>(12345, 123456, 5, 2));
}

TEST_F(EqualDistinctCountHistogramTest, FromColumnFloat) {
  auto hist = EqualDistinctCountHistogram<float>::from_column(*_float2, ColumnID{0}, 3u);

  ASSERT_EQ(hist->bin_count(), 3u);
  EXPECT_EQ(hist->bin(BinID{0}), HistogramBin<float>(0.5f, 2.2f, 4, 4));
  EXPECT_EQ(hist->bin(BinID{1}), HistogramBin<float>(2.5f, 3.3f, 6, 3));
  EXPECT_EQ(hist->bin(BinID{2}), HistogramBin<float>(3.6f, 6.1f, 4, 3));
}

TEST_F(EqualDistinctCountHistogramTest, FromSegmentString) {
  StringHistogramDomain default_domain;
  const auto default_domain_histogram =
      EqualDistinctCountHistogram<pmr_string>::from_segment(*_string2, ColumnID{0}, ChunkID{0}, 4u, default_domain);

  ASSERT_EQ(default_domain_histogram->bin_count(), 4u);
  EXPECT_EQ(default_domain_histogram->bin(BinID{0}), HistogramBin<pmr_string>("aa", "birne", 3, 3));
  EXPECT_EQ(default_domain_histogram->bin(BinID{1}), HistogramBin<pmr_string>("bla", "ttt", 4, 3));
  EXPECT_EQ(default_domain_histogram->bin(BinID{2}), HistogramBin<pmr_string>("uuu", "xxx", 4, 3));
  EXPECT_EQ(default_domain_histogram->bin(BinID{3}), HistogramBin<pmr_string>("yyy", "zzz", 4, 2));

  StringHistogramDomain reduced_histogram{'a', 'c', 9};
  const auto reduced_domain_histogram =
      EqualDistinctCountHistogram<pmr_string>::from_segment(*_string2, ColumnID{0}, ChunkID{0}, 4u, reduced_histogram);

  ASSERT_EQ(reduced_domain_histogram->bin_count(), 4u);
  EXPECT_EQ(reduced_domain_histogram->bin(BinID{0}), HistogramBin<pmr_string>("aa", "b", 2, 2));
  EXPECT_EQ(reduced_domain_histogram->bin(BinID{1}), HistogramBin<pmr_string>("bca", "bccc", 2, 2));
  EXPECT_EQ(reduced_domain_histogram->bin(BinID{2}), HistogramBin<pmr_string>("bcccc", "bcccc", 1, 1));
  EXPECT_EQ(reduced_domain_histogram->bin(BinID{3}), HistogramBin<pmr_string>("ccc", "ccc", 10, 1));
}

TEST_F(EqualDistinctCountHistogramTest, FromSegmentInt) {
  const auto hist = EqualDistinctCountHistogram<int32_t>::from_segment(*_int_float4, ColumnID{0}, ChunkID{0}, 2u);

  ASSERT_EQ(hist->bin_count(), 2u);
  EXPECT_EQ(hist->bin(BinID{0}), HistogramBin<int32_t>(12, 123, 2, 2));
  EXPECT_EQ(hist->bin(BinID{1}), HistogramBin<int32_t>(12345, 123456, 5, 2));
}

TEST_F(EqualDistinctCountHistogramTest, FromSegmentFloat) {
  auto hist = EqualDistinctCountHistogram<float>::from_segment(*_float2, ColumnID{0}, ChunkID{0}, 3u);

  ASSERT_EQ(hist->bin_count(), 3u);
  EXPECT_EQ(hist->bin(BinID{0}), HistogramBin<float>(0.5f, 2.2f, 4, 4));
  EXPECT_EQ(hist->bin(BinID{1}), HistogramBin<float>(2.5f, 3.3f, 6, 3));
  EXPECT_EQ(hist->bin(BinID{2}), HistogramBin<float>(3.6f, 6.1f, 4, 3));
}

TEST_F(EqualDistinctCountHistogramTest, FromSegmentIntMultipleChunks) {
  const auto hist =
      EqualDistinctCountHistogram<int32_t>::from_segment(*_int_float4_multichunk, ColumnID{0}, ChunkID{0}, 2u);

  ASSERT_EQ(hist->bin_count(), 2u);
  EXPECT_EQ(hist->bin(BinID{0}), HistogramBin<int32_t>(123, 12345, 3, 2));
  EXPECT_EQ(hist->bin(BinID{1}), HistogramBin<int32_t>(123456, 123456, 2, 1));

  const auto hist2 =
      EqualDistinctCountHistogram<int32_t>::from_segment(*_int_float4_multichunk, ColumnID{0}, ChunkID{1}, 1u);

  ASSERT_EQ(hist2->bin_count(), 1u);
  EXPECT_EQ(hist2->bin(BinID{0}), HistogramBin<int32_t>(12, 123456, 2, 2));
}

TEST_F(EqualDistinctCountHistogramTest, IntHistogramMerging) {
  using HistogramDataType = int32_t;
  using HistogramType = EqualDistinctCountHistogram<HistogramDataType>;
  using HistogramPtrType = std::shared_ptr<EqualDistinctCountHistogram<HistogramDataType>>;

  const auto max_bin_count = 6u;
  const auto column = ColumnID{0};
  const auto table = _int_float_multichunk_large;
  const auto chunk_count = table->chunk_count();
  auto histograms = std::vector<HistogramPtrType>();

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; chunk_id++) {
    const auto hist = HistogramType::from_segment(*table, column, chunk_id, max_bin_count);
    histograms.push_back(hist);
  }

  const auto [merged_hist, max_estimation_error] = HistogramType::merge(histograms, max_bin_count);

  ASSERT_EQ(merged_hist->bin_count(), max_bin_count);
  EXPECT_EQ(max_estimation_error, 0.0);
  EXPECT_EQ(merged_hist->bin(BinID{0}), HistogramBin<HistogramDataType>(1, 2, 2, 2));
  EXPECT_EQ(merged_hist->bin(BinID{1}), HistogramBin<HistogramDataType>(3, 4, 2, 2));
  EXPECT_EQ(merged_hist->bin(BinID{2}), HistogramBin<HistogramDataType>(5, 6, 2, 2));
  EXPECT_EQ(merged_hist->bin(BinID{3}), HistogramBin<HistogramDataType>(7, 8, 2, 2));
  EXPECT_EQ(merged_hist->bin(BinID{4}), HistogramBin<HistogramDataType>(9, 10, 2, 2));
  EXPECT_EQ(merged_hist->bin(BinID{5}), HistogramBin<HistogramDataType>(11, 12, 2, 2));
}

TEST_F(EqualDistinctCountHistogramTest, FloatSingleBinHistogramMerging) {
  using HistogramDataType = float;
  using HistogramType = EqualDistinctCountHistogram<HistogramDataType>;
  using HistogramPtrType = std::shared_ptr<EqualDistinctCountHistogram<HistogramDataType>>;

  const auto max_bin_count = 3u;
  const auto column = ColumnID{2};
  const auto table = _int_float_multichunk_large;
  const auto chunk_count = table->chunk_count();
  auto histograms = std::vector<HistogramPtrType>();

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; chunk_id++) {
    const auto hist = HistogramType::from_segment(*table, column, chunk_id, max_bin_count);
    histograms.push_back(hist);
  }

  const auto [merged_hist, max_estimation_error] = HistogramType::merge(histograms, max_bin_count);

  ASSERT_EQ(merged_hist->bin_count(), max_bin_count);
  EXPECT_EQ(max_estimation_error, 0.0);
  EXPECT_EQ(merged_hist->bin(BinID{0}), HistogramBin<HistogramDataType>(1, 1, 6, 1));
  EXPECT_EQ(merged_hist->bin(BinID{1}), HistogramBin<HistogramDataType>(2, 2, 4, 1));
  EXPECT_EQ(merged_hist->bin(BinID{2}), HistogramBin<HistogramDataType>(3, 3, 2, 1));
}

TEST_F(EqualDistinctCountHistogramTest, IntSingleBinHistogramMerging) {
  using HistogramDataType = int32_t;
  using HistogramType = EqualDistinctCountHistogram<HistogramDataType>;
  using HistogramPtrType = std::shared_ptr<EqualDistinctCountHistogram<HistogramDataType>>;

  const auto max_bin_count = 5u;
  const auto column = ColumnID{3};
  const auto table = _int_float_multichunk_large_small_chunks;
  const auto chunk_count = table->chunk_count();
  auto histograms = std::vector<HistogramPtrType>();

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; chunk_id++) {
    const auto hist = HistogramType::from_segment(*table, column, chunk_id, max_bin_count);
    histograms.push_back(hist);
  }

  const auto [merged_hist, max_estimation_error] = HistogramType::merge(histograms, max_bin_count);

  ASSERT_EQ(merged_hist->bin_count(), max_bin_count);
  EXPECT_EQ(merged_hist->total_distinct_count(), 5.0);
  EXPECT_EQ(max_estimation_error, 0.0);
  EXPECT_EQ(merged_hist->bin(BinID{0}), HistogramBin<HistogramDataType>(1, 1, 3, 1));
  EXPECT_EQ(merged_hist->bin(BinID{1}), HistogramBin<HistogramDataType>(40, 40, 2, 1));
  EXPECT_EQ(merged_hist->bin(BinID{2}), HistogramBin<HistogramDataType>(41, 41, 2, 1));
  EXPECT_EQ(merged_hist->bin(BinID{3}), HistogramBin<HistogramDataType>(42, 42, 2, 1));
  EXPECT_EQ(merged_hist->bin(BinID{4}), HistogramBin<HistogramDataType>(100, 100, 3, 1));
}

TEST_F(EqualDistinctCountHistogramTest, IntLargeBinHistogramMerging) {
  using HistogramDataType = int32_t;
  using HistogramType = EqualDistinctCountHistogram<HistogramDataType>;
  using HistogramPtrType = std::shared_ptr<EqualDistinctCountHistogram<HistogramDataType>>;

  const auto max_bin_count = 1u;
  const auto column = ColumnID{3};
  const auto table = _int_float_multichunk_large_small_chunks;
  const auto chunk_count = table->chunk_count();
  auto histograms = std::vector<HistogramPtrType>();

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; chunk_id++) {
    const auto hist = HistogramType::from_segment(*table, column, chunk_id, max_bin_count);
    histograms.push_back(hist);
  }

  const auto [merged_hist, max_estimation_error] = HistogramType::merge(histograms, max_bin_count);

  ASSERT_EQ(merged_hist->bin_count(), max_bin_count);
  EXPECT_EQ(merged_hist->total_distinct_count(), 12);
  EXPECT_EQ(max_estimation_error, 8);
  EXPECT_EQ(merged_hist->bin(BinID{0}), HistogramBin<HistogramDataType>(1, 100, 12, 12));
}

}  // namespace opossum
