#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/alias_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "statistics/cardinality_estimation/cardinality_estimation_join.hpp"
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
    /**
     * node_a
     */
    segment_statistics_a_0_a = std::make_shared<SegmentStatistics2<int32_t>>();
    segment_statistics_a_0_b = std::make_shared<SegmentStatistics2<int32_t>>();

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
    chunk_statistics_a_0->approx_invalid_row_count = 5;
    chunk_statistics_a_0->segment_statistics.emplace_back(segment_statistics_a_0_a);
    chunk_statistics_a_0->segment_statistics.emplace_back(segment_statistics_a_0_b);

    table_statistics_a = std::make_shared<TableStatistics2>();
    table_statistics_a->chunk_statistics.emplace_back(chunk_statistics_a_0);

    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}});
    node_a->set_table_statistics2(table_statistics_a);

    a_a = node_a->get_column("a");
    a_b = node_a->get_column("b");

    /**
     * node_b
     *  Uses the same ChunkStatistics (chunk_statistics_b) for all three Chunks
     */
    const auto chunk_statistics_b = std::make_shared<ChunkStatistics2>(32);
    const auto segment_statistics_b_a = std::make_shared<SegmentStatistics2<int32_t>>();
    const auto segment_statistics_b_b = std::make_shared<SegmentStatistics2<int32_t>>();

    // clang-format off
    segment_statistics_b_a->generic_histogram = std::make_shared<GenericHistogram<int32_t>>(
      std::vector<int32_t>{0, 5, 10}, std::vector<int32_t>{4, 9, 15},
      std::vector<HistogramCountType>{10, 10, 12}, std::vector<HistogramCountType>{5, 5, 6});

    segment_statistics_b_b->generic_histogram = std::make_shared<GenericHistogram<int32_t>>(
      std::vector<int32_t>{0}, std::vector<int32_t>{9},
      std::vector<HistogramCountType>{32}, std::vector<HistogramCountType>{10});
    // clang-format on

    chunk_statistics_b->segment_statistics.emplace_back(segment_statistics_b_a);
    chunk_statistics_b->segment_statistics.emplace_back(segment_statistics_b_b);

    const auto table_statistics_b = std::make_shared<TableStatistics2>();
    table_statistics_b->chunk_statistics.emplace_back(chunk_statistics_b);
    table_statistics_b->chunk_statistics.emplace_back(chunk_statistics_b);
    table_statistics_b->chunk_statistics.emplace_back(chunk_statistics_b);

    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}});
    node_b->set_table_statistics2(table_statistics_b);

    b_a = node_b->get_column("a");

    /**
     * node_c
     *  Uses the same ChunkStatistics (chunk_statistics_c) for both Chunks
     */
    const auto chunk_statistics_c = std::make_shared<ChunkStatistics2>(64);
    const auto segment_statistics_c_x = std::make_shared<SegmentStatistics2<int32_t>>();
    const auto segment_statistics_c_y = std::make_shared<SegmentStatistics2<int32_t>>();

    // clang-format off
    segment_statistics_c_x->equal_distinct_count_histogram = std::make_shared<EqualDistinctCountHistogram<int32_t>>(
      std::vector<int32_t>{0, 8}, std::vector<int32_t>{7, 15},
      std::vector<HistogramCountType>{32, 32}, 8, 0);

    segment_statistics_c_y->equal_width_histogram = std::make_shared<EqualWidthHistogram<int32_t>>(
      0, 9, std::vector<HistogramCountType>{64}, std::vector<HistogramCountType>{10}, 0);
    // clang-format on

    chunk_statistics_c->segment_statistics.emplace_back(segment_statistics_c_x);
    chunk_statistics_c->segment_statistics.emplace_back(segment_statistics_c_y);

    const auto table_statistics_c = std::make_shared<TableStatistics2>();
    table_statistics_c->chunk_statistics.emplace_back(chunk_statistics_c);
    table_statistics_c->chunk_statistics.emplace_back(chunk_statistics_c);

    node_c = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "x"}, {DataType::Int, "y"}});
    node_c->set_table_statistics2(table_statistics_c);

    c_x = node_c->get_column("x");
  }

  CardinalityEstimator estimator;
  LQPColumnReference a_a, a_b, b_a, c_x;
  std::shared_ptr<SegmentStatistics2<int32_t>> segment_statistics_a_0_a, segment_statistics_a_0_b;
  std::shared_ptr<MockNode> node_a, node_b, node_c;
  std::shared_ptr<TableStatistics2> table_statistics_a;
};

TEST_F(CardinalityEstimatorTest, Alias) {
  // clang-format off
  const auto input_lqp =
  AliasNode::make(expression_vector(a_b, a_a), std::vector<std::string>{"x", "y"},
    node_a);
  // clang-format on

  const auto table_statistics = estimator.estimate_statistics(input_lqp);

  ASSERT_EQ(table_statistics->chunk_statistics.size(), 1u);

  const auto chunk_statistics = table_statistics->chunk_statistics.at(0);

  EXPECT_EQ(chunk_statistics->row_count, 100u);
  ASSERT_EQ(chunk_statistics->segment_statistics.size(), 2u);
  EXPECT_EQ(chunk_statistics->segment_statistics.at(0), segment_statistics_a_0_b);
  EXPECT_EQ(chunk_statistics->segment_statistics.at(1), segment_statistics_a_0_a);
}

TEST_F(CardinalityEstimatorTest, Projection) {
  // clang-format off
  const auto input_lqp =
  ProjectionNode::make(expression_vector(a_b, add_(a_b, a_a), a_a),
    node_a);
  // clang-format on

  const auto table_statistics = estimator.estimate_statistics(input_lqp);

  ASSERT_EQ(table_statistics->chunk_statistics.size(), 1u);

  const auto chunk_statistics = table_statistics->chunk_statistics.at(0);

  EXPECT_EQ(chunk_statistics->row_count, 100u);
  ASSERT_EQ(chunk_statistics->segment_statistics.size(), 3u);
  EXPECT_EQ(chunk_statistics->segment_statistics.at(0), segment_statistics_a_0_b);
  EXPECT_TRUE(chunk_statistics->segment_statistics.at(1));
  EXPECT_EQ(chunk_statistics->segment_statistics.at(2), segment_statistics_a_0_a);
}

TEST_F(CardinalityEstimatorTest, Aggregate) {
  // clang-format off
  const auto input_lqp =
  AggregateNode::make(expression_vector(a_b, add_(a_b, a_a)), expression_vector(sum_(a_a)),
    node_a);
  // clang-format on

  const auto table_statistics = estimator.estimate_statistics(input_lqp);

  ASSERT_EQ(table_statistics->chunk_statistics.size(), 1u);

  const auto chunk_statistics = table_statistics->chunk_statistics.at(0);

  EXPECT_EQ(chunk_statistics->row_count, 100u);
  ASSERT_EQ(chunk_statistics->segment_statistics.size(), 3u);
  EXPECT_EQ(chunk_statistics->segment_statistics.at(0), segment_statistics_a_0_b);
  EXPECT_TRUE(chunk_statistics->segment_statistics.at(1));
  EXPECT_TRUE(chunk_statistics->segment_statistics.at(2));
}

TEST_F(CardinalityEstimatorTest, Validate) {
  // clang-format off
  const auto input_lqp =
  ValidateNode::make(
    node_a);
  // clang-format on

  const auto table_statistics = estimator.estimate_statistics(input_lqp);

  ASSERT_EQ(table_statistics->chunk_statistics.size(), 1u);

  const auto chunk_statistics = table_statistics->chunk_statistics.at(0);

  EXPECT_EQ(chunk_statistics->row_count, 100u - 5u);
  ASSERT_EQ(chunk_statistics->segment_statistics.size(), 2u);

  const auto segment_statistics_a =
      std::dynamic_pointer_cast<SegmentStatistics2<int32_t>>(chunk_statistics->segment_statistics.at(0));
  ASSERT_TRUE(segment_statistics_a->generic_histogram);
  EXPECT_EQ(segment_statistics_a->generic_histogram->total_count(), 100u - 4u);

  const auto segment_statistics_b =
      std::dynamic_pointer_cast<SegmentStatistics2<int32_t>>(chunk_statistics->segment_statistics.at(1));
  ASSERT_TRUE(segment_statistics_b->equal_width_histogram);
  EXPECT_EQ(segment_statistics_b->equal_width_histogram->total_count(), 75u - 2u);
}

TEST_F(CardinalityEstimatorTest, Sort) {
  // clang-format off
  const auto input_lqp =
  SortNode::make(expression_vector(a_b), std::vector<OrderByMode>{OrderByMode::Ascending},
    node_a);
  // clang-format on

  const auto table_statistics = estimator.estimate_statistics(input_lqp);
  EXPECT_EQ(table_statistics, table_statistics_a);
}

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

TEST_F(CardinalityEstimatorTest, TwoPredicatesSameColumn) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(a_a, 50),
    PredicateNode::make(less_than_equals_(a_a, 75),
      node_a));
  // clang-format on

  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp), 20);
}

TEST_F(CardinalityEstimatorTest, TwoPredicatesDifferentColumn) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(a_a, 50),
    PredicateNode::make(less_than_equals_(a_b, 75),
      node_a));
  // clang-format on

  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp), 13.257575f);
}

TEST_F(CardinalityEstimatorTest, ArithmeticEquiInnerJoin) {
  // clang-format off
  const auto input_lqp =
  JoinNode::make(JoinMode::Inner, equals_(b_a, c_x),
    node_b,
    node_c);
  // clang-format on

  const auto result_statistics = estimator.estimate_statistics(input_lqp);

  ASSERT_EQ(result_statistics->chunk_statistics.size(), 6u);
  EXPECT_EQ(result_statistics->row_count(), 6u * 128u);

  for (auto& chunk_statistics : result_statistics->chunk_statistics) {
    ASSERT_EQ(chunk_statistics->segment_statistics.size(), 4u);

    const auto segment_statistics_b_a =
        std::dynamic_pointer_cast<SegmentStatistics2<int32_t>>(chunk_statistics->segment_statistics[0]);
    const auto join_histogram_b_a = segment_statistics_b_a->generic_histogram;
    EXPECT_EQ(join_histogram_b_a->bin_count(), 4u);

    const auto segment_statistics_b_b =
        std::dynamic_pointer_cast<SegmentStatistics2<int32_t>>(chunk_statistics->segment_statistics[1]);
    const auto scaled_histogram_b_b = segment_statistics_b_b->generic_histogram;
    EXPECT_EQ(scaled_histogram_b_b->total_count(), 32 * 4);

    const auto segment_statistics_c_x =
        std::dynamic_pointer_cast<SegmentStatistics2<int32_t>>(chunk_statistics->segment_statistics[2]);
    const auto join_histogram_c_x = segment_statistics_c_x->generic_histogram;
    EXPECT_EQ(join_histogram_c_x->bin_count(), 4u);

    const auto segment_statistics_c_y =
        std::dynamic_pointer_cast<SegmentStatistics2<int32_t>>(chunk_statistics->segment_statistics[3]);
    const auto scaled_histogram_c_y = segment_statistics_c_y->equal_width_histogram;
    EXPECT_EQ(scaled_histogram_c_y->total_count(), 64 * 2);
  }
}

TEST_F(CardinalityEstimatorTest, CrossJoin) {
  // clang-format off
  const auto input_lqp =
  JoinNode::make(JoinMode::Cross,
    node_b,
    node_c);
  // clang-format on

  const auto result_statistics = estimator.estimate_statistics(input_lqp);

  ASSERT_EQ(result_statistics->chunk_statistics.size(), 6u);
  ASSERT_EQ(result_statistics->row_count(), (32u * 64u) * 6u);

  for (auto& chunk_statistics : result_statistics->chunk_statistics) {
    ASSERT_EQ(chunk_statistics->segment_statistics.size(), 4u);

    const auto segment_statistics_b_a =
        std::dynamic_pointer_cast<SegmentStatistics2<int32_t>>(chunk_statistics->segment_statistics[0]);
    EXPECT_EQ(segment_statistics_b_a->generic_histogram->total_count(), 32u * 64u);

    const auto segment_statistics_b_b =
        std::dynamic_pointer_cast<SegmentStatistics2<int32_t>>(chunk_statistics->segment_statistics[1]);
    EXPECT_EQ(segment_statistics_b_b->generic_histogram->total_count(), 32u * 64u);

    const auto segment_statistics_c_x =
        std::dynamic_pointer_cast<SegmentStatistics2<int32_t>>(chunk_statistics->segment_statistics[2]);
    EXPECT_EQ(segment_statistics_c_x->generic_histogram->total_count(), 32u * 64u);

    const auto segment_statistics_c_y =
        std::dynamic_pointer_cast<SegmentStatistics2<int32_t>>(chunk_statistics->segment_statistics[3]);
    EXPECT_EQ(segment_statistics_c_y->equal_width_histogram->total_count(), 32u * 64u);
  }
}



TEST_F(CardinalityEstimatorTest, EstimateHistogramOfInnerEquiJoinWithBinAdjustedHistograms) {
  const auto histogram_left = std::make_shared<GenericHistogram<int32_t>>(
      std::vector<int32_t>{0, 10, 20, 30, 40, 50, 60}, std::vector<int32_t>{9, 19, 29, 39, 49, 59, 69},
      std::vector<HistogramCountType>{10, 15, 10, 20, 5, 15, 5}, std::vector<HistogramCountType>{1, 1, 3, 8, 1, 5, 1});

  const auto histogram_right = std::make_shared<GenericHistogram<int32_t>>(
      std::vector<int32_t>{20, 30, 50}, std::vector<int32_t>{29, 39, 59}, std::vector<HistogramCountType>{10, 5, 10},
      std::vector<HistogramCountType>{7, 2, 10});

  const auto join_histogram =
      estimate_histogram_of_inner_equi_join_with_bin_adjusted_histograms<int32_t>(histogram_left, histogram_right);

  ASSERT_EQ(join_histogram->bin_count(), 3u);

  EXPECT_EQ(join_histogram->bin_minimum(0), 20);
  EXPECT_EQ(join_histogram->bin_maximum(0), 29);
  EXPECT_EQ(join_histogram->bin_height(0), std::ceil(10.f * 10.f * (1.f / 7.f)));
  EXPECT_EQ(join_histogram->bin_distinct_count(0), 3u);

  EXPECT_EQ(join_histogram->bin_minimum(1), 30);
  EXPECT_EQ(join_histogram->bin_maximum(1), 39);
  EXPECT_EQ(join_histogram->bin_height(1), std::ceil(20.f * 5.f * (1.f / 8.f)));
  EXPECT_EQ(join_histogram->bin_distinct_count(1), 2u);

  EXPECT_EQ(join_histogram->bin_minimum(2), 50);
  EXPECT_EQ(join_histogram->bin_maximum(2), 59);
  EXPECT_EQ(join_histogram->bin_height(2), std::ceil(15.f * 10.f * (1.f / 10.f)));
  EXPECT_EQ(join_histogram->bin_distinct_count(2), 5u);
}

}  // namespace opossum
