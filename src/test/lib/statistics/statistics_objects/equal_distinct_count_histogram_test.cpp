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

TEST_F(EqualDistinctCountHistogramTest, FromTPCHOrders) {
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  const auto orders = load_table("resources/test_data/tbl/tpch/sf-0.02/orders.tbl", ChunkOffset{900});
  ASSERT_EQ(orders->row_count(), 30'000);
  ASSERT_EQ(orders->chunk_count(), 34);

  // For each column, we test the first, middle, and last bin.
  const auto orderkey_histogram = EqualDistinctCountHistogram<int32_t>::from_column(*orders, ColumnID{0}, 16);
  ASSERT_TRUE(orderkey_histogram);
  ASSERT_EQ(orderkey_histogram->bin_count(), 16);
  EXPECT_EQ(orderkey_histogram->total_count(), orders->row_count());
  EXPECT_EQ(orderkey_histogram->bin(BinID{0}), HistogramBin<int32_t>(1, 7491, 1875, 1875));
  EXPECT_EQ(orderkey_histogram->bin(BinID{8}), HistogramBin<int32_t>(60'001, 67'491, 1875, 1875));
  EXPECT_EQ(orderkey_histogram->bin(BinID{15}), HistogramBin<int32_t>(112'486, 120'000, 1875, 1875));

  const auto totalprice_histogram = EqualDistinctCountHistogram<float>::from_column(*orders, ColumnID{3}, 64);
  ASSERT_TRUE(totalprice_histogram);
  ASSERT_EQ(totalprice_histogram->bin_count(), 64);
  EXPECT_EQ(totalprice_histogram->total_count(), orders->row_count());
  EXPECT_EQ(totalprice_histogram->bin(BinID{0}), HistogramBin<float>(913.01f, 8181.45f, 469, 469));
  EXPECT_EQ(totalprice_histogram->bin(BinID{32}), HistogramBin<float>(135'630.52f, 139'741.90f, 470, 468));
  EXPECT_EQ(totalprice_histogram->bin(BinID{63}), HistogramBin<float>(326'850.39f, 451'578.10f, 468, 468));

  // More bins than values in o_orderpriority.
  const auto orderpriority_histogram = EqualDistinctCountHistogram<pmr_string>::from_column(*orders, ColumnID{5}, 64);
  ASSERT_TRUE(orderpriority_histogram);
  ASSERT_EQ(orderpriority_histogram->bin_count(), 5);
  EXPECT_EQ(orderpriority_histogram->total_count(), orders->row_count());
  EXPECT_EQ(orderpriority_histogram->bin(BinID{0}), HistogramBin<pmr_string>("1-URGENT", "1-URGENT", 5985, 1));
  EXPECT_EQ(orderpriority_histogram->bin(BinID{1}), HistogramBin<pmr_string>("2-HIGH", "2-HIGH", 6062, 1));
  EXPECT_EQ(orderpriority_histogram->bin(BinID{2}), HistogramBin<pmr_string>("3-MEDIUM", "3-MEDIUM", 5902, 1));
  EXPECT_EQ(orderpriority_histogram->bin(BinID{3}),
            HistogramBin<pmr_string>("4-NOT SPECIFIED", "4-NOT SPECIFIED", 6010, 1));
  EXPECT_EQ(orderpriority_histogram->bin(BinID{4}), HistogramBin<pmr_string>("5-LOW", "5-LOW", 6041, 1));

  const auto comment_histogram = EqualDistinctCountHistogram<pmr_string>::from_column(*orders, ColumnID{8}, 256);
  ASSERT_TRUE(comment_histogram);
  ASSERT_EQ(comment_histogram->bin_count(), 256);
  EXPECT_EQ(comment_histogram->total_count(), orders->row_count());
  EXPECT_EQ(comment_histogram->bin(BinID{0}), HistogramBin<pmr_string>(" about the accounts. slyly express accounts wa",
                                                                       " accounts nag blithel", 118, 118));
  EXPECT_EQ(comment_histogram->bin(BinID{128}),
            HistogramBin<pmr_string>("ironic requests. packages cajole according",
                                     "ithely blithe deposits sleep beyond the", 117, 117));
  EXPECT_EQ(
      comment_histogram->bin(BinID{255}),
      HistogramBin<pmr_string>("yly regular packages ar", "zzle. carefully enticing deposits nag furio", 117, 117));
}

}  // namespace hyrise
