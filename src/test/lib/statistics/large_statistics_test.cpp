#include <array>
#include <cmath>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "cost_estimation/cost_estimator_logical.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/mvcc_data.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class LargeStatisticsTest : public BaseTest {
 public:
  // The resolution of float is larger than 1 for values greater than 16777216.0f, i.e., incrementing by 1 does not
  // increase the value anymore. This behavior can cause problems in statistics generation or cardinality/cost
  // estimation.
  //
  // To understand this behavior, let us dig a bit into floating-points as defined by IEEE 754 (see
  // https://en.wikipedia.org/wiki/IEEE_754). C++'s float is a single-precision/binary32 type. This means, floats are
  // encoded as follows:
  //     sign s (1 bit) | exponent e (8 bits) | mantissa m (23 bits) = 32 bit together.
  //
  // Each float value f can be computed as f = (-1)^s * (1 + (m / 2^23)) * 2^e. However, this means that not all
  // integers larger than 2^23 can be represented anymore.
  // E.g., with e = 23 and m = 0, f = (1 + 0 / 2^23) * 2^23 = 8'388'608. The next representable number with m = 1 is
  // f = (1 + 1 / 2^23) * 2^23 = 8'388'609, the next (m = 2) is f = (1 + 2 / 2^23) * 2^23 = 8'388'610, etc.
  // The difference between two values is exactly 1.0.
  // With the next higher exponent e = 24, f = (1 + 0 / 2^23) * 2^24 = 16'777'216 for m = 0, and for m = 1,
  // f = (1 + 1 / 2^22) * 2^24 = 16'777'218. Here, the difference is exactly 2.0 (and thus, incrementing by 1.0 does not
  // make a difference anymore).
  //
  // To test these scenarios, we create a table with a single column that has 30 million rows, but only three unique
  // values in the ratio 27:2:1. Thus, we can be sure we hit a platform-independent spot where floats are not precise
  // enough anymore.
  //
  // (In case you wondered: C++ doubles are binary64 types with an exponent of 11 bits and a mantissa of 52 bits. Their
  // precision is at 1.0 for values larger than 2^52 > 4 * 10^15.)
  static constexpr auto VALUE_RATIO = std::array<uint32_t, 3>{27, 2, 1};
  static constexpr auto VALUES_PER_SHARE = 1'000'000;
  static inline std::shared_ptr<Table> table;

  static void SetUpTestSuite() {
    // Make sure that we actually hit a point for the most frequent value where the precision of float is larger than 1.
    // To do so, we calculate the difference of that value's frequency and the next smaller float that can be
    // represented. This difference must be larger than 1.0, hence `value + 1` cannot be represented and returns
    // `value`.
    const auto most_frequent_row_count = VALUE_RATIO[0] * VALUES_PER_SHARE;
    ASSERT_GT(float{most_frequent_row_count} - std::nextafter(float{most_frequent_row_count}, 0.0f), 1.0f);

    // Build a simple table of ValueSegments with the desired characteristics.
    const auto chunk_size = ChunkOffset{100'000};
    table = std::make_shared<Table>(TableColumnDefinitions{{"a_a", DataType::Int, false}}, TableType::Data, chunk_size,
                                    UseMvcc::Yes);

    auto values_1 = pmr_vector<int32_t>(chunk_size, 1);
    const auto value_segment_1 = std::make_shared<ValueSegment<int32_t>>(std::move(values_1));
    const auto most_frequent_chunk_count = most_frequent_row_count / chunk_size;
    const auto mvcc_data = std::make_shared<MvccData>(chunk_size, CommitID{0});
    for (auto chunk_id = ChunkID{0}; chunk_id < most_frequent_chunk_count; ++chunk_id) {
      table->append_chunk({value_segment_1}, mvcc_data);
    }

    auto values_2 = pmr_vector<int32_t>(chunk_size, 2);
    const auto value_segment_2 = std::make_shared<ValueSegment<int32_t>>(std::move(values_2));
    const auto second_most_frequent_chunk_count = (VALUE_RATIO[1] * VALUES_PER_SHARE) / chunk_size;
    for (auto chunk_id = ChunkID{0}; chunk_id < second_most_frequent_chunk_count; ++chunk_id) {
      table->append_chunk({value_segment_2}, mvcc_data);
    }

    auto values_3 = pmr_vector<int32_t>(chunk_size, 3);
    const auto value_segment_3 = std::make_shared<ValueSegment<int32_t>>(std::move(values_3));
    const auto remaining_chunk_count = (VALUE_RATIO[2] * VALUES_PER_SHARE) / chunk_size;
    for (auto chunk_id = ChunkID{0}; chunk_id < remaining_chunk_count; ++chunk_id) {
      table->append_chunk({value_segment_3}, mvcc_data);
    }

    // Generate table statistics, which are used in the test cases.
    table->set_table_statistics(TableStatistics::from_table(*table));
  }

  static void TearDownTestSuite() {
    table = nullptr;
  }
};

// The low precision of floats can cause incorrect histogram bin heights (see #2676). Make sure that this is not a
// problem.
TEST_F(LargeStatisticsTest, CorrectHistogramBins) {
  const auto table_statistics = table->table_statistics();
  ASSERT_TRUE(table_statistics);
  const auto& column_statistics = table_statistics->column_statistics;
  ASSERT_EQ(column_statistics.size(), 1);
  const auto attribute_statistics = std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(column_statistics[0]);
  ASSERT_TRUE(attribute_statistics);
  const auto histogram = attribute_statistics->histogram;
  ASSERT_TRUE(histogram);

  const auto total_count =
      (std::get<0>(VALUE_RATIO) + std::get<1>(VALUE_RATIO) + std::get<2>(VALUE_RATIO)) * VALUES_PER_SHARE;
  EXPECT_EQ(histogram->total_count(), total_count);
  EXPECT_EQ(histogram->total_count(), 30'000'000);
  ASSERT_EQ(histogram->bin_count(), 3);

  EXPECT_EQ(histogram->bin(BinID{0}), HistogramBin<int32_t>(1, 1, std::get<0>(VALUE_RATIO) * VALUES_PER_SHARE, 1));
  EXPECT_EQ(histogram->bin(BinID{1}), HistogramBin<int32_t>(2, 2, std::get<1>(VALUE_RATIO) * VALUES_PER_SHARE, 1));
  EXPECT_EQ(histogram->bin(BinID{2}), HistogramBin<int32_t>(3, 3, std::get<2>(VALUE_RATIO) * VALUES_PER_SHARE, 1));
}

TEST_F(LargeStatisticsTest, CostAndCardinalityEstimation) {
  Hyrise::get().storage_manager.add_table("a", table);
  const auto a = StoredTableNode::make("a");

  // clang-format off
  const auto lqp =
  UnionNode::make(SetOperationMode::All,
    a,
    SortNode::make(expression_vector(a->get_column("a_a")), std::vector<SortMode>{SortMode::AscendingNullsFirst},
      LimitNode::make(value_(1),
        a)));
  // clang-format on

  const auto cardinality_estimator = std::make_shared<CardinalityEstimator>();
  auto cost_estimator = CostEstimatorLogical{cardinality_estimator};

  const auto cardinality_a = cardinality_estimator->estimate_cardinality(a);
  EXPECT_EQ(cardinality_a, 30'000'000);
  EXPECT_EQ(cardinality_estimator->estimate_cardinality(lqp->left_input()), 30'000'000);
  EXPECT_EQ(cardinality_estimator->estimate_cardinality(lqp->right_input()), 1);
  EXPECT_EQ(cardinality_estimator->estimate_cardinality(lqp->right_input()->left_input()), 1);
  EXPECT_EQ(cardinality_estimator->estimate_cardinality(lqp), 30'000'001);

  // The cost of the entire plan sums all individual costs. Thus, the overall cost should be higher than the cost of a
  // single node.
  EXPECT_GT(cost_estimator.estimate_plan_cost(lqp), cost_estimator.estimate_plan_cost(a));
}

}  // namespace hyrise
