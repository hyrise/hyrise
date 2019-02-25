#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "base_test.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/alias_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "statistics/cardinality_estimation/cardinality_estimation_join.hpp"
#include "statistics/cardinality_estimation/cardinality_estimation_scan.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "statistics/histograms/equal_distinct_count_histogram.hpp"
#include "statistics/histograms/generic_histogram.hpp"
#include "statistics/horizontal_statistics_slice.hpp"
#include "statistics/table_cardinality_estimation_statistics.hpp"
#include "statistics/vertical_statistics_slice.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class CardinalityEstimatorTest : public BaseTest {
 public:
  void SetUp() override {
    /**
     * node_a
     */
    vertical_slices_a_0_a = std::make_shared<VerticalStatisticsSlice<int32_t>>();
    vertical_slices_a_0_b = std::make_shared<VerticalStatisticsSlice<int32_t>>();

    // clang-format off
    const auto histogram_a_0_a = std::make_shared<EqualDistinctCountHistogram<int32_t>>(
      std::vector<int32_t>{1,  26, 51, 76},
      std::vector<int32_t>{25, 50, 75, 100},
      std::vector<HistogramCountType>{40, 30, 20, 10},
      10, 0);

    const auto histogram_a_0_b = std::make_shared<GenericHistogram<int32_t>>(
      std::vector<int32_t>{10, 50, 90},
      std::vector<int32_t>{49, 89, 129},
      std::vector<HistogramCountType>{15, 25, 35},
      std::vector<HistogramCountType>{10, 20, 25});
    // clang-format on

    vertical_slices_a_0_a->histogram = histogram_a_0_a;
    vertical_slices_a_0_b->histogram = histogram_a_0_b;

    const auto statistics_slice_a_0 = std::make_shared<HorizontalStatisticsSlice>(100);
    statistics_slice_a_0->vertical_slices.emplace_back(vertical_slices_a_0_a);
    statistics_slice_a_0->vertical_slices.emplace_back(vertical_slices_a_0_b);

    table_statistics_a =
        std::make_shared<TableCardinalityEstimationStatistics>(std::vector<DataType>{DataType::Int, DataType::Int});
    table_statistics_a->approx_invalid_row_count = 5;
    table_statistics_a->horizontal_slices.emplace_back(statistics_slice_a_0);

    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}});
    node_a->set_cardinality_estimation_statistics(table_statistics_a);

    a_a = node_a->get_column("a");
    a_b = node_a->get_column("b");

    /**
     * node_b
     *  Uses the same ChunkStatistics (statistics_slice_b) for all three Chunks
     */
    const auto statistics_slice_b = std::make_shared<HorizontalStatisticsSlice>(32);
    const auto vertical_slices_b_a = std::make_shared<VerticalStatisticsSlice<int32_t>>();
    const auto vertical_slices_b_b = std::make_shared<VerticalStatisticsSlice<int32_t>>();

    // clang-format off
    vertical_slices_b_a->histogram = std::make_shared<GenericHistogram<int32_t>>(
      std::vector<int32_t>{0, 5, 10}, std::vector<int32_t>{4, 9, 15},
      std::vector<HistogramCountType>{10, 10, 12}, std::vector<HistogramCountType>{5, 5, 6});

    vertical_slices_b_b->histogram = std::make_shared<GenericHistogram<int32_t>>(
      std::vector<int32_t>{0}, std::vector<int32_t>{9},
      std::vector<HistogramCountType>{32}, std::vector<HistogramCountType>{10});
    // clang-format on

    statistics_slice_b->vertical_slices.emplace_back(vertical_slices_b_a);
    statistics_slice_b->vertical_slices.emplace_back(vertical_slices_b_b);

    const auto table_statistics_b =
        std::make_shared<TableCardinalityEstimationStatistics>(std::vector<DataType>{DataType::Int, DataType::Int});
    table_statistics_b->horizontal_slices.emplace_back(statistics_slice_b);
    table_statistics_b->horizontal_slices.emplace_back(statistics_slice_b);
    table_statistics_b->horizontal_slices.emplace_back(statistics_slice_b);

    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}});
    node_b->set_cardinality_estimation_statistics(table_statistics_b);

    b_a = node_b->get_column("a");

    /**
     * node_c
     *  Uses the same ChunkStatistics (statistics_slice_c) for both Chunks
     */
    const auto statistics_slice_c = std::make_shared<HorizontalStatisticsSlice>(64);
    const auto vertical_slices_c_x = std::make_shared<VerticalStatisticsSlice<int32_t>>();
    const auto vertical_slices_c_y = std::make_shared<VerticalStatisticsSlice<int32_t>>();

    // clang-format off
    vertical_slices_c_x->histogram = std::make_shared<EqualDistinctCountHistogram<int32_t>>(
      std::vector<int32_t>{0, 8}, std::vector<int32_t>{7, 15},
      std::vector<HistogramCountType>{32, 32}, 8, 0);

    vertical_slices_c_y->histogram = std::make_shared<GenericHistogram<int32_t>>(
      std::vector<int32_t>{0},
      std::vector<int32_t>{9},
      std::vector<HistogramCountType>{64},
      std::vector<HistogramCountType>{10});
    // clang-format on

    statistics_slice_c->vertical_slices.emplace_back(vertical_slices_c_x);
    statistics_slice_c->vertical_slices.emplace_back(vertical_slices_c_y);

    const auto table_statistics_c =
        std::make_shared<TableCardinalityEstimationStatistics>(std::vector<DataType>{DataType::Int, DataType::Int});
    table_statistics_c->horizontal_slices.emplace_back(statistics_slice_c);
    table_statistics_c->horizontal_slices.emplace_back(statistics_slice_c);

    node_c = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "x"}, {DataType::Int, "y"}});
    node_c->set_cardinality_estimation_statistics(table_statistics_c);

    c_x = node_c->get_column("x");

    /**
     * node_d
     */
    node_d = create_mock_node_with_statistics(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, 100,
        {std::make_shared<SingleBinHistogram<int32_t>>(10, 100, 100, 20),
         std::make_shared<SingleBinHistogram<int32_t>>(50, 60, 100, 5),
         std::make_shared<SingleBinHistogram<int32_t>>(110, 1100, 100, 2)});

    d_a = LQPColumnReference{node_d, ColumnID{0}};
    d_b = LQPColumnReference{node_d, ColumnID{1}};
    d_c = LQPColumnReference{node_d, ColumnID{2}};
  }

  CardinalityEstimator estimator;
  LQPColumnReference a_a, a_b, b_a, c_x, d_a, d_b, d_c;
  std::shared_ptr<VerticalStatisticsSlice<int32_t>> vertical_slices_a_0_a, vertical_slices_a_0_b;
  std::shared_ptr<MockNode> node_a, node_b, node_c, node_d;
  std::shared_ptr<TableCardinalityEstimationStatistics> table_statistics_a;
};

TEST_F(CardinalityEstimatorTest, Alias) {
  // clang-format off
  const auto input_lqp =
  AliasNode::make(expression_vector(a_b, a_a), std::vector<std::string>{"x", "y"},
    node_a);
  // clang-format on

  const auto table_statistics = estimator.estimate_statistics(input_lqp);

  ASSERT_EQ(table_statistics->horizontal_slices.size(), 1u);

  const auto statistics_slice = table_statistics->horizontal_slices.at(0);

  EXPECT_EQ(statistics_slice->row_count, 100u);
  ASSERT_EQ(statistics_slice->vertical_slices.size(), 2u);
  EXPECT_EQ(statistics_slice->vertical_slices.at(0), vertical_slices_a_0_b);
  EXPECT_EQ(statistics_slice->vertical_slices.at(1), vertical_slices_a_0_a);
}

TEST_F(CardinalityEstimatorTest, Projection) {
  // clang-format off
  const auto input_lqp =
  ProjectionNode::make(expression_vector(a_b, add_(a_b, a_a), a_a),
    node_a);
  // clang-format on

  const auto table_statistics = estimator.estimate_statistics(input_lqp);

  ASSERT_EQ(table_statistics->horizontal_slices.size(), 1u);

  const auto statistics_slice = table_statistics->horizontal_slices.at(0);

  EXPECT_EQ(statistics_slice->row_count, 100u);
  ASSERT_EQ(statistics_slice->vertical_slices.size(), 3u);
  EXPECT_EQ(statistics_slice->vertical_slices.at(0), vertical_slices_a_0_b);
  EXPECT_TRUE(statistics_slice->vertical_slices.at(1));
  EXPECT_EQ(statistics_slice->vertical_slices.at(2), vertical_slices_a_0_a);
}

TEST_F(CardinalityEstimatorTest, Aggregate) {
  // clang-format off
  const auto input_lqp =
  AggregateNode::make(expression_vector(a_b, add_(a_b, a_a)), expression_vector(sum_(a_a)),
    node_a);
  // clang-format on

  const auto table_statistics = estimator.estimate_statistics(input_lqp);

  ASSERT_EQ(table_statistics->horizontal_slices.size(), 1u);

  const auto statistics_slice = table_statistics->horizontal_slices.at(0);

  EXPECT_EQ(statistics_slice->row_count, 100u);
  ASSERT_EQ(statistics_slice->vertical_slices.size(), 3u);
  EXPECT_EQ(statistics_slice->vertical_slices.at(0), vertical_slices_a_0_b);
  EXPECT_TRUE(statistics_slice->vertical_slices.at(1));
  EXPECT_TRUE(statistics_slice->vertical_slices.at(2));
}

TEST_F(CardinalityEstimatorTest, Validate) {
  // clang-format off
  const auto input_lqp =
  ValidateNode::make(
    node_a);
  // clang-format on

  const auto table_statistics = estimator.estimate_statistics(input_lqp);

  std::cout << *estimator.estimate_statistics(input_lqp->left_input()) << std::endl;
  std::cout << *table_statistics << std::endl;

  ASSERT_EQ(table_statistics->horizontal_slices.size(), 1u);

  const auto statistics_slice = table_statistics->horizontal_slices.at(0);

  EXPECT_EQ(statistics_slice->row_count, 100u - 5u);
  ASSERT_EQ(statistics_slice->vertical_slices.size(), 2u);

  const auto vertical_slices_a =
      std::dynamic_pointer_cast<VerticalStatisticsSlice<int32_t>>(statistics_slice->vertical_slices.at(0));
  ASSERT_TRUE(vertical_slices_a->histogram);
  EXPECT_EQ(vertical_slices_a->histogram->total_count(), 100u - 5u);

  const auto vertical_slices_b =
      std::dynamic_pointer_cast<VerticalStatisticsSlice<int32_t>>(statistics_slice->vertical_slices.at(1));
  ASSERT_TRUE(vertical_slices_b->histogram);
  EXPECT_EQ(vertical_slices_b->histogram->total_count(), 75u - 3.75f);
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
  ASSERT_EQ(plan_output_statistics->horizontal_slices.size(), 1u);

  const auto plan_output_statistics_0 = plan_output_statistics->horizontal_slices.at(0);
  ASSERT_EQ(plan_output_statistics_0->vertical_slices.size(), 2u);

  const auto plan_output_statistics_0_a =
      std::dynamic_pointer_cast<VerticalStatisticsSlice<int32_t>>(plan_output_statistics_0->vertical_slices.at(0));
  const auto plan_output_statistics_0_b =
      std::dynamic_pointer_cast<VerticalStatisticsSlice<int32_t>>(plan_output_statistics_0->vertical_slices.at(1));
  ASSERT_TRUE(plan_output_statistics_0_a);
  ASSERT_TRUE(plan_output_statistics_0_b);

  ASSERT_TRUE(plan_output_statistics_0_a->histogram);
  ASSERT_TRUE(plan_output_statistics_0_b->histogram);

  EXPECT_EQ(plan_output_statistics_0_a->histogram->estimate_cardinality(PredicateCondition::LessThan, 50).type,
            EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(
      plan_output_statistics_0_a->histogram->estimate_cardinality(PredicateCondition::GreaterThan, 75).cardinality,
      10.f);

  EXPECT_FLOAT_EQ(
      plan_output_statistics_0_b->histogram->estimate_cardinality(PredicateCondition::LessThan, 50).cardinality, 4.5f);
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

  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp), 12.5f);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp->left_input()), 41.66666f);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp->left_input()->left_input()), 100.0f);
}

TEST_F(CardinalityEstimatorTest, MultiplePredicates) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(equals_(d_a, 95),
    PredicateNode::make(greater_than_(d_b, 55),
      PredicateNode::make(greater_than_(d_b, 40),
        PredicateNode::make(greater_than_equals_(d_a, 90),
          PredicateNode::make(less_than_(d_c, 500),
            node_d)))));
  // clang-format on

  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp), 1.0f);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp->left_input()), 2.1623178f);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp->left_input()->left_input()), 4.7571f);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp->left_input()->left_input()->left_input()), 4.7571f);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp->left_input()->left_input()->left_input()->left_input()),
                  39.3542f);
}

TEST_F(CardinalityEstimatorTest, PredicateWithValuePlaceholder) {
  // 20 distinct values in column d_a and 100 values total. So == is assumed to have a selectivity of 5%, != of 95%, and
  // everything else is assumed to hit 50%

  const auto lqp_a = PredicateNode::make(equals_(d_a, placeholder_(ParameterID{0})), node_d);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(lqp_a), 5.0f);

  const auto lqp_b = PredicateNode::make(not_equals_(d_a, placeholder_(ParameterID{0})), node_d);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(lqp_b), 95.0f);

  const auto lqp_c = PredicateNode::make(less_than_(d_a, placeholder_(ParameterID{0})), node_d);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(lqp_c), 50.0f);

  const auto lqp_d = PredicateNode::make(greater_than_(d_a, placeholder_(ParameterID{0})), node_d);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(lqp_d), 50.0f);

  // BETWEEN is split up into (a >= ? AND a <= ?), so it ends up with a selecitivity of 25%
  const auto lqp_e =
      PredicateNode::make(between_(d_a, placeholder_(ParameterID{0}), placeholder_(ParameterID{1})), node_d);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(lqp_e), 25.0f);
}

TEST_F(CardinalityEstimatorTest, ArithmeticEquiInnerJoin) {
  // clang-format off
  const auto input_lqp =
  JoinNode::make(JoinMode::Inner, equals_(b_a, c_x),
    node_b,
    node_c);
  // clang-format on

  const auto result_statistics = estimator.estimate_statistics(input_lqp);

  ASSERT_EQ(result_statistics->horizontal_slices.size(), 6u);
  EXPECT_EQ(result_statistics->row_count(), 6u * 128u);

  for (auto& statistics_slice : result_statistics->horizontal_slices) {
    ASSERT_EQ(statistics_slice->vertical_slices.size(), 4u);

    const auto vertical_slices_b_a =
        std::dynamic_pointer_cast<VerticalStatisticsSlice<int32_t>>(statistics_slice->vertical_slices[0]);
    const auto join_histogram_b_a = vertical_slices_b_a->histogram;
    EXPECT_EQ(join_histogram_b_a->bin_count(), 4u);

    const auto vertical_slices_b_b =
        std::dynamic_pointer_cast<VerticalStatisticsSlice<int32_t>>(statistics_slice->vertical_slices[1]);
    const auto scaled_histogram_b_b = vertical_slices_b_b->histogram;
    EXPECT_EQ(scaled_histogram_b_b->total_count(), 32 * 4);

    const auto vertical_slices_c_x =
        std::dynamic_pointer_cast<VerticalStatisticsSlice<int32_t>>(statistics_slice->vertical_slices[2]);
    const auto join_histogram_c_x = vertical_slices_c_x->histogram;
    EXPECT_EQ(join_histogram_c_x->bin_count(), 4u);

    const auto vertical_slices_c_y =
        std::dynamic_pointer_cast<VerticalStatisticsSlice<int32_t>>(statistics_slice->vertical_slices[3]);
    const auto scaled_histogram_c_y = vertical_slices_c_y->histogram;
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

  ASSERT_EQ(result_statistics->horizontal_slices.size(), 6u);
  ASSERT_EQ(result_statistics->row_count(), (32u * 64u) * 6u);

  for (auto& statistics_slice : result_statistics->horizontal_slices) {
    ASSERT_EQ(statistics_slice->vertical_slices.size(), 4u);

    const auto vertical_slices_b_a =
        std::dynamic_pointer_cast<VerticalStatisticsSlice<int32_t>>(statistics_slice->vertical_slices[0]);
    EXPECT_EQ(vertical_slices_b_a->histogram->total_count(), 32u * 64u);

    const auto vertical_slices_b_b =
        std::dynamic_pointer_cast<VerticalStatisticsSlice<int32_t>>(statistics_slice->vertical_slices[1]);
    EXPECT_EQ(vertical_slices_b_b->histogram->total_count(), 32u * 64u);

    const auto vertical_slices_c_x =
        std::dynamic_pointer_cast<VerticalStatisticsSlice<int32_t>>(statistics_slice->vertical_slices[2]);
    EXPECT_EQ(vertical_slices_c_x->histogram->total_count(), 32u * 64u);

    const auto vertical_slices_c_y =
        std::dynamic_pointer_cast<VerticalStatisticsSlice<int32_t>>(statistics_slice->vertical_slices[3]);
    EXPECT_EQ(vertical_slices_c_y->histogram->total_count(), 32u * 64u);
  }
}

TEST_F(CardinalityEstimatorTest, BinsInnerEquiJoin) {
  EXPECT_FLOAT_EQ(cardinality_estimation::bins_inner_equi_join(1.0f, 1.0f, 1.0f, 1.0f).first, 1.0f);
  EXPECT_FLOAT_EQ(cardinality_estimation::bins_inner_equi_join(1.0f, 1.0f, 1.0f, 1.0f).second, 1.0f);

  EXPECT_FLOAT_EQ(cardinality_estimation::bins_inner_equi_join(2.0f, 1.0f, 1.0f, 1.0f).first, 2.0f);
  EXPECT_FLOAT_EQ(cardinality_estimation::bins_inner_equi_join(2.0f, 1.0f, 1.0f, 1.0f).second, 1.0f);

  EXPECT_FLOAT_EQ(cardinality_estimation::bins_inner_equi_join(2.0f, 1.0f, 2.0f, 1.0f).first, 4.0f);
  EXPECT_FLOAT_EQ(cardinality_estimation::bins_inner_equi_join(2.0f, 1.0f, 2.0f, 1.0f).second, 1.0f);

  EXPECT_FLOAT_EQ(cardinality_estimation::bins_inner_equi_join(2.0f, 2.0f, 2.0f, 1.0f).first, 2.0f);
  EXPECT_FLOAT_EQ(cardinality_estimation::bins_inner_equi_join(2.0f, 2.0f, 1.0f, 1.0f).second, 1.0f);

  EXPECT_FLOAT_EQ(cardinality_estimation::bins_inner_equi_join(200.0f, 20.0f, 3000.0f, 2500.0f).first, 240.0f);
  EXPECT_FLOAT_EQ(cardinality_estimation::bins_inner_equi_join(200.0f, 20.0f, 3000.0f, 2500.0f).second, 20.0f);

  // Test DistinctCount > Height
  EXPECT_FLOAT_EQ(cardinality_estimation::bins_inner_equi_join(2.0f, 3.0f, 2.0f, 7.0f).first, 0.5714286f);
  EXPECT_FLOAT_EQ(cardinality_estimation::bins_inner_equi_join(2.0f, 3.0f, 1.0f, 7.0f).second, 3.0f);

  // Test Heights/Distinct counts < 1
  EXPECT_FLOAT_EQ(cardinality_estimation::bins_inner_equi_join(2.0f, 0.1f, 2.0f, 1.0f).first, 4.0f);
  EXPECT_FLOAT_EQ(cardinality_estimation::bins_inner_equi_join(2.0f, 0.1f, 2.0f, 1.0f).second, 0.1f);

  EXPECT_FLOAT_EQ(cardinality_estimation::bins_inner_equi_join(0.0f, 0.0f, 2.0f, 1.0f).first, 0.0f);
  EXPECT_FLOAT_EQ(cardinality_estimation::bins_inner_equi_join(0.0f, 0.0f, 2.0f, 1.0f).second, 0.0f);

  EXPECT_FLOAT_EQ(cardinality_estimation::bins_inner_equi_join(200.0f, 20.0f, 3000.0f, 0.1f).first, 30000.0f);
  EXPECT_FLOAT_EQ(cardinality_estimation::bins_inner_equi_join(200.0f, 20.0f, 3000.0f, 0.1f).second, 0.1f);

  EXPECT_FLOAT_EQ(cardinality_estimation::bins_inner_equi_join(200.0f, 1.0f, 0.3f, 0.3f).first, 60.0f);
  EXPECT_FLOAT_EQ(cardinality_estimation::bins_inner_equi_join(200.0f, 1.0f, 0.3f, 0.3f).second, 0.3f);
}

TEST_F(CardinalityEstimatorTest, EstimateColumnToColumnEquiScan) {
  // clang-format off
  const auto histogram_left = GenericHistogram<int32_t>(
    std::vector<int32_t>           {10, 13, 16},
    std::vector<int32_t>           {12, 14, 20},
    std::vector<HistogramCountType>{3,   9, 10},
    std::vector<HistogramCountType>{2,   3, 10});

  const auto histogram_right = GenericHistogram<int32_t>(
    std::vector<int32_t>           {0, 13, 15, 16},
    std::vector<int32_t>           {5, 14, 15, 20},
    std::vector<HistogramCountType>{7,  5, 1,  10},
    std::vector<HistogramCountType>{5,  2, 1,   2});
  // clang-format on

  const auto result_histogram = cardinality_estimation::histograms_column_vs_column_equi_scan(histogram_left, histogram_right);

  ASSERT_EQ(result_histogram->bin_count(), 2u);
  EXPECT_EQ(result_histogram->bin(BinID{0}), HistogramBin<int32_t>(13, 14, 5, 2));
  EXPECT_EQ(result_histogram->bin(BinID{1}), HistogramBin<int32_t>(16, 20, 10, 2));
}

TEST_F(CardinalityEstimatorTest, HistogramsInnerEquiJoin) {
  const auto histogram_left = GenericHistogram<int32_t>(
      std::vector<int32_t>{0, 10, 20, 30, 40, 50, 60}, std::vector<int32_t>{9, 19, 29, 39, 49, 59, 69},
      std::vector<HistogramCountType>{10, 15, 10, 20, 5, 15, 5}, std::vector<HistogramCountType>{1, 1, 3, 8, 1, 5, 1});

  const auto histogram_right =
      GenericHistogram<int32_t>(std::vector<int32_t>{20, 30, 50}, std::vector<int32_t>{29, 39, 59},
                                std::vector<HistogramCountType>{10, 5, 10}, std::vector<HistogramCountType>{7, 2, 10});

  const auto join_histogram =
      cardinality_estimation::histograms_inner_equi_join<int32_t>(histogram_left, histogram_right);

  ASSERT_EQ(join_histogram->bin_count(), 3u);

  EXPECT_EQ(join_histogram->bin_minimum(0), 20);
  EXPECT_EQ(join_histogram->bin_maximum(0), 29);
  EXPECT_FLOAT_EQ(join_histogram->bin_height(0), 10.f * 10.f * (1.f / 7.f));
  EXPECT_EQ(join_histogram->bin_distinct_count(0), 3u);

  EXPECT_EQ(join_histogram->bin_minimum(1), 30);
  EXPECT_EQ(join_histogram->bin_maximum(1), 39);
  EXPECT_FLOAT_EQ(join_histogram->bin_height(1), 20.f * 5.f * (1.f / 8.f));
  EXPECT_EQ(join_histogram->bin_distinct_count(1), 2u);

  EXPECT_EQ(join_histogram->bin_minimum(2), 50);
  EXPECT_EQ(join_histogram->bin_maximum(2), 59);
  EXPECT_FLOAT_EQ(join_histogram->bin_height(2), 15.f * 10.f * (1.f / 10.f));
  EXPECT_EQ(join_histogram->bin_distinct_count(2), 5u);
}

TEST_F(CardinalityEstimatorTest, Union) {
  // clang-format off
  const auto input_lqp =
  UnionNode::make(UnionMode::Positions,
    node_a,
    node_b);
  // clang-format on

  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp), 196.0f);
}

}  // namespace opossum
