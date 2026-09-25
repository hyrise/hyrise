#include <cstdint>
#include <memory>
#include <string>

#include "base_test.hpp"
#include "hyrise.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"
#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

namespace hyrise {

class EqualDistinctCountHistogramTest : public BaseTest {
  void SetUp() override {
    _int_float4 = load_table("resources/test_data/tbl/int_float4.tbl");
    _float2 = load_table("resources/test_data/tbl/float2.tbl");
    _string2 = load_table("resources/test_data/tbl/string2.tbl");
  }

 protected:
  std::shared_ptr<Table> _int_float4;
  std::shared_ptr<Table> _float2;
  std::shared_ptr<Table> _string2;
};

TEST_F(EqualDistinctCountHistogramTest, Name) {
  const auto histogram = EqualDistinctCountHistogram<int32_t>{{1}, {100}, {50}, 10, 0};
  EXPECT_EQ(histogram.name(), "EqualDistinctCount");
}

TEST_F(EqualDistinctCountHistogramTest, FromColumnString) {
  const auto default_domain = StringHistogramDomain{};
  const auto default_domain_histogram =
      EqualDistinctCountHistogram<pmr_string>::from_column(*_string2, ColumnID{0}, 4, default_domain);

  ASSERT_EQ(default_domain_histogram->bin_count(), 4);
  EXPECT_EQ(default_domain_histogram->bin(BinID{0}), HistogramBin<pmr_string>("aa", "birne", 3, 3));
  EXPECT_EQ(default_domain_histogram->bin(BinID{1}), HistogramBin<pmr_string>("bla", "ttt", 4, 3));
  EXPECT_EQ(default_domain_histogram->bin(BinID{2}), HistogramBin<pmr_string>("uuu", "xxx", 4, 3));

  const auto reduced_histogram = StringHistogramDomain{'a', 'c', 9};
  const auto reduced_domain_histogram =
      EqualDistinctCountHistogram<pmr_string>::from_column(*_string2, ColumnID{0}, 4, reduced_histogram);

  ASSERT_EQ(default_domain_histogram->bin_count(), 4);
  EXPECT_EQ(default_domain_histogram->bin(BinID{0}), HistogramBin<pmr_string>("aa", "birne", 3, 3));
  EXPECT_EQ(default_domain_histogram->bin(BinID{1}), HistogramBin<pmr_string>("bla", "ttt", 4, 3));
  EXPECT_EQ(default_domain_histogram->bin(BinID{2}), HistogramBin<pmr_string>("uuu", "xxx", 4, 3));
}

TEST_F(EqualDistinctCountHistogramTest, FromColumnInt) {
  const auto hist = EqualDistinctCountHistogram<int32_t>::from_column(*_int_float4, ColumnID{0}, 2);

  ASSERT_EQ(hist->bin_count(), 2);
  EXPECT_EQ(hist->bin(BinID{0}), HistogramBin<int32_t>(12, 123, 2, 2));
  EXPECT_EQ(hist->bin(BinID{1}), HistogramBin<int32_t>(12345, 123456, 5, 2));
}

TEST_F(EqualDistinctCountHistogramTest, FromColumnFloat) {
  const auto hist = EqualDistinctCountHistogram<float>::from_column(*_float2, ColumnID{0}, 3);

  ASSERT_EQ(hist->bin_count(), 3);
  EXPECT_EQ(hist->bin(BinID{0}), HistogramBin<float>(0.5f, 2.2f, 4, 4));
  EXPECT_EQ(hist->bin(BinID{1}), HistogramBin<float>(2.5f, 3.3f, 6, 3));
  EXPECT_EQ(hist->bin(BinID{2}), HistogramBin<float>(3.6f, 6.1f, 4, 3));
}

TEST_F(EqualDistinctCountHistogramTest, AllNullValues) {
  auto column_definitions = TableColumnDefinitions{};
  column_definitions.emplace_back("a", DataType::Int, true);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{10});

  for (auto index = size_t{0}; index < 129; ++index) {
    table->append({NULL_VALUE});
  }

  const auto hist = EqualDistinctCountHistogram<int32_t>::from_column(*table, ColumnID{0}, 16);
  ASSERT_FALSE(hist);
}

TEST_F(EqualDistinctCountHistogramTest, FromTPCHlineitem) {
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  const auto lineitem = load_table("resources/test_data/tbl/tpch/sf-0.02/lineitem.tbl", ChunkOffset{1'000});
  ASSERT_EQ(lineitem->chunk_count(), 121);

  // For each column, we test the first, middle, and last bin.
  const auto orderkey_histogram = EqualDistinctCountHistogram<int32_t>::from_column(*lineitem, ColumnID{0}, 16);
  ASSERT_TRUE(orderkey_histogram);
  ASSERT_EQ(orderkey_histogram->bin_count(), 16);
  EXPECT_EQ(orderkey_histogram->total_count(), lineitem->row_count());
  EXPECT_EQ(orderkey_histogram->bin(BinID{0}), HistogramBin<int32_t>(1, 7491, 7501, 1875));
  EXPECT_EQ(orderkey_histogram->bin(BinID{8}), HistogramBin<int32_t>(60'001, 67'491, 7467, 1875));
  EXPECT_EQ(orderkey_histogram->bin(BinID{15}), HistogramBin<int32_t>(112'486, 120'000, 7499, 1875));

  const auto quantity_histogram = EqualDistinctCountHistogram<float>::from_column(*lineitem, ColumnID{4}, 64);
  ASSERT_TRUE(quantity_histogram);
  ASSERT_EQ(quantity_histogram->bin_count(), 50);
  EXPECT_EQ(quantity_histogram->total_count(), lineitem->row_count());
  EXPECT_EQ(quantity_histogram->bin(BinID{0}), HistogramBin<float>(1.0f, 1.0f, 2414, 1));
  EXPECT_EQ(quantity_histogram->bin(BinID{25}), HistogramBin<float>(26.0f, 26.0f, 2518, 1));
  EXPECT_EQ(quantity_histogram->bin(BinID{49}), HistogramBin<float>(50.0f, 50.0f, 2426, 1));

  const auto comment_histogram = EqualDistinctCountHistogram<pmr_string>::from_column(*lineitem, ColumnID{15}, 256);
  ASSERT_TRUE(comment_histogram);
  ASSERT_EQ(comment_histogram->bin_count(), 256);
  EXPECT_EQ(comment_histogram->total_count(), lineitem->row_count());
  EXPECT_EQ(comment_histogram->bin(BinID{0}),
            HistogramBin<pmr_string>(" Tiresias ", " accounts cajole furiously f", 512, 451));
  EXPECT_EQ(comment_histogram->bin(BinID{128}),
            HistogramBin<pmr_string>("ironic requests. blithely ironic pl", "ithely after the furiously silent pack",
                                     458, 451));
  EXPECT_EQ(comment_histogram->bin(BinID{255}),
            HistogramBin<pmr_string>("yly ironic instructions. regular foxes w", "zzle: pending i", 465, 450));
}

}  // namespace hyrise
