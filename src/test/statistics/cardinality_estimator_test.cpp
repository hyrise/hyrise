#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "statistics/chunk_statistics/histograms/equal_distinct_count_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_width_histogram.hpp"
#include "statistics/chunk_statistics/histograms/generic_histogram.hpp"
#include "statistics/chunk_statistics2.hpp"
#include "statistics/segment_statistics2.hpp"
#include "statistics/table_statistics2.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class CardinalityEstimatorTest : public ::testing::Test {
 public:
  void SetUp() override {
    const auto segment_statistics_a_0_a = std::make_shared<SegmentStatistics2<int32_t>>();
    const auto segment_statistics_a_0_b = std::make_shared<SegmentStatistics2<int32_t>>();

    // clang-format off
    const auto histogram_a_0_a = std::make_shared<EqualDistinctCountHistogram<int32_t>>(
      std::vector<int32_t>{1,  26, 51, 76},
      std::vector<int32_t>{25, 50, 75, 100},
      std::vector<HistogramCountType>{40, 30, 20, 10},
      10, 0);

    const auto histogram_a_0_b = std::make_shared<EqualWidthHistogram<int32_t>>(
      10, 129, std::vector<HistogramCountType>{15, 25, 35}, std::vector<HistogramCountType>{10, 20, 25}, 0);
    // clang-format on

    segment_statistics_a_0_a->equal_distinct_count_histogram = histogram_a_0_a;
    segment_statistics_a_0_b->equal_width_histogram = histogram_a_0_b;

    const auto chunk_statistics_a_0 = std::make_shared<ChunkStatistics2>(100);
    chunk_statistics_a_0->segment_statistics.emplace_back(segment_statistics_a_0_a);
    chunk_statistics_a_0->segment_statistics.emplace_back(segment_statistics_a_0_b);

    const auto table_statistics_a = std::make_shared<TableStatistics2>();
    table_statistics_a->chunk_statistics.emplace_back(chunk_statistics_a_0);

    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}});
    node_a->set_table_statistics2(table_statistics_a);

    a_a = node_a->get_column("a");
    a_b = node_a->get_column("b");
  }

  CardinalityEstimator estimator;
  LQPColumnReference a_a, a_b;
  std::shared_ptr<MockNode> node_a;
};

TEST_F(CardinalityEstimatorTest, SinglePredicate) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(a_a, 50),
    node_a);
  // clang-format on

  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp), 30.0f);

  /**
   * Test LQP output statistics objects
   */
  const auto plan_output_statistics = estimator.estimate_statistics(input_lqp);
  EXPECT_FLOAT_EQ(plan_output_statistics->row_count(), 30.0f);  // Same as above
  ASSERT_EQ(plan_output_statistics->chunk_statistics.size(), 1u);

  const auto plan_output_statistics_0 = plan_output_statistics->chunk_statistics.at(0);
  ASSERT_EQ(plan_output_statistics_0->segment_statistics.size(), 2u);

  const auto plan_output_statistics_0_a =
      std::dynamic_pointer_cast<SegmentStatistics2<int32_t>>(plan_output_statistics_0->segment_statistics.at(0));
  const auto plan_output_statistics_0_b =
      std::dynamic_pointer_cast<SegmentStatistics2<int32_t>>(plan_output_statistics_0->segment_statistics.at(1));
  ASSERT_TRUE(plan_output_statistics_0_a);
  ASSERT_TRUE(plan_output_statistics_0_b);

  ASSERT_TRUE(plan_output_statistics_0_a->generic_histogram);
  ASSERT_TRUE(plan_output_statistics_0_b->equal_width_histogram);

  EXPECT_EQ(plan_output_statistics_0_a->generic_histogram->estimate_cardinality(PredicateCondition::LessThan, 50).type,
            EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(
      plan_output_statistics_0_a->generic_histogram->estimate_cardinality(PredicateCondition::GreaterThan, 75)
          .cardinality,
      10.f);
  EXPECT_FLOAT_EQ(
      plan_output_statistics_0_b->equal_width_histogram->estimate_cardinality(PredicateCondition::LessThan, 50)
          .cardinality,
      5.f);
}

TEST_F(CardinalityEstimatorTest, TwoPredicates) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(a_a, 50),
    PredicateNode::make(less_than_equals_(a_a, 75),
      node_a));
  // clang-format on

  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp), 20);
}

TEST_F(CardinalityEstimatorTest, EstimateCardinalityOfInnerJoinWithNumericHistograms) {
  const auto histogram_left = std::make_shared<GenericHistogram<int32_t>>(
      std::vector<int32_t>{0, 10, 20, 30, 40, 50, 60}, std::vector<int32_t>{9, 19, 29, 39, 49, 59, 69},
      std::vector<HistogramCountType>{10, 15, 10, 20, 5, 15, 5}, std::vector<HistogramCountType>{1, 1, 3, 8, 1, 5, 1});

  const auto histogram_right = std::make_shared<GenericHistogram<int32_t>>(
      std::vector<int32_t>{20, 30, 50}, std::vector<int32_t>{29, 39, 59}, std::vector<HistogramCountType>{10, 5, 10},
      std::vector<HistogramCountType>{7, 2, 10});

  EXPECT_FLOAT_EQ(CardinalityEstimator::estimate_cardinality_of_inner_equi_join_with_numeric_histograms<int32_t>(
                      histogram_left, histogram_right),
                  (10.f * 10.f * (1.f / 7.f)) + (20.f * 5.f * (1.f / 8.f)) + (15.f * 10.f * (1.f / 10.f)));
}

}  // namespace opossum
