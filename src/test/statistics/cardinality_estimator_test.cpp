#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "base_test.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/alias_node.hpp"
#include "logical_query_plan/create_prepared_plan_node.hpp"
#include "logical_query_plan/create_table_node.hpp"
#include "logical_query_plan/create_view_node.hpp"
#include "logical_query_plan/delete_node.hpp"
#include "logical_query_plan/drop_table_node.hpp"
#include "logical_query_plan/drop_view_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/insert_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/update_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"
#include "statistics/statistics_objects/generic_histogram.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/table_column_definition.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class CardinalityEstimatorTest : public BaseTest {
 public:
  void SetUp() override {
    /**
     * node_a
     */
    // clang-format on
    node_a = create_mock_node_with_statistics({{DataType::Int, "a"}, {DataType::Int, "b"}}, 100,
                                              {GenericHistogram<int32_t>::with_single_bin(1, 100, 100, 10),
                                               GenericHistogram<int32_t>::with_single_bin(10, 129, 70, 55)});
    // clang-format off

    a_a = node_a->get_column("a");
    a_b = node_a->get_column("b");

    /**
     * node_b
     */
    // clang-format off
    const auto histogram_b_a = std::make_shared<GenericHistogram<int32_t>>(
      std::vector<int32_t>{0, 5, 10}, std::vector<int32_t>{4, 9, 15},
      std::vector<HistogramCountType>{10, 10, 12}, std::vector<HistogramCountType>{5, 5, 6});

    const auto histogram_b_b = std::make_shared<GenericHistogram<int32_t>>(
      std::vector<int32_t>{0}, std::vector<int32_t>{9},
      std::vector<HistogramCountType>{32}, std::vector<HistogramCountType>{10});
    // clang-format on

    node_b = create_mock_node_with_statistics({{DataType::Int, "a"}, {DataType::Int, "b"}}, 32,
                                              {histogram_b_a, histogram_b_b});

    b_a = node_b->get_column("a");
    b_b = node_b->get_column("b");

    /**
     * node_c
     */
    // clang-format off
    const auto histogram_c_x = std::make_shared<EqualDistinctCountHistogram<int32_t>>(
      std::vector<int32_t>{0, 8}, std::vector<int32_t>{7, 15},
      std::vector<HistogramCountType>{32, 32}, 8, 0);

    const auto histogram_c_y = std::make_shared<GenericHistogram<int32_t>>(
      std::vector<int32_t>{0},
      std::vector<int32_t>{9},
      std::vector<HistogramCountType>{64},
      std::vector<HistogramCountType>{10});
    // clang-format on

    node_c = create_mock_node_with_statistics({{DataType::Int, "x"}, {DataType::Int, "y"}}, 64,
                                              {histogram_c_x, histogram_c_y});

    c_x = node_c->get_column("x");

    /**
     * node_d
     */
    node_d = create_mock_node_with_statistics(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, 100,
        {GenericHistogram<int32_t>::with_single_bin(10, 100, 100, 20),
         GenericHistogram<int32_t>::with_single_bin(50, 60, 100, 5),
         GenericHistogram<int32_t>::with_single_bin(110, 1100, 100, 2)});

    d_a = LQPColumnReference{node_d, ColumnID{0}};
    d_b = LQPColumnReference{node_d, ColumnID{1}};
    d_c = LQPColumnReference{node_d, ColumnID{2}};

    /**
     * node_e
     * Has no statistics on column "b"
     */
    // clang-format on
    node_e = create_mock_node_with_statistics({{DataType::Int, "a"}, {DataType::Int, "b"}}, 100,
                                              {GenericHistogram<int32_t>::with_single_bin(1, 100, 10, 10), nullptr});
    // clang-format off

    e_a = node_e->get_column("a");
    e_b = node_e->get_column("b");

    /**
     * node_f
     * Has columns with different data types
     */
    // clang-format on
    node_f = create_mock_node_with_statistics({{DataType::Int, "a"}, {DataType::Float, "b"}}, 200,
                                              {GenericHistogram<int32_t>::with_single_bin(1, 200, 200, 10),
                                               GenericHistogram<float>::with_single_bin(1.0f, 100.0f, 200, 150.0f)});
    // clang-format off

    f_a = node_f->get_column("a");
    f_b = node_f->get_column("b");

    /**
     * node_g
     * Has a string column
     */
    // clang-format on
    node_g = create_mock_node_with_statistics({{DataType::String, "a"}}, 100,
                                              {GenericHistogram<pmr_string>::with_single_bin("a", "z", 100, 40)});
    // clang-format off

    g_a = node_g->get_column("a");
  }

  CardinalityEstimator estimator;
  LQPColumnReference a_a, a_b, b_a, b_b, c_x, d_a, d_b, d_c, e_a, e_b, f_a, f_b, g_a;
  std::shared_ptr<MockNode> node_a, node_b, node_c, node_d, node_e, node_f, node_g;
};

TEST_F(CardinalityEstimatorTest, Aggregate) {
  // clang-format off
  const auto input_lqp =
  AggregateNode::make(expression_vector(a_b, add_(a_b, a_a)), expression_vector(sum_(a_a)),
                      node_a);
  // clang-format on

  const auto input_table_statistics = node_a->table_statistics();
  const auto result_table_statistics = estimator.estimate_statistics(input_lqp);

  EXPECT_EQ(result_table_statistics->row_count, 100u);
  ASSERT_EQ(result_table_statistics->column_statistics.size(), 3u);
  EXPECT_EQ(result_table_statistics->column_statistics.at(0), input_table_statistics->column_statistics.at(1));
  EXPECT_TRUE(result_table_statistics->column_statistics.at(1));
  EXPECT_TRUE(result_table_statistics->column_statistics.at(2));
}

TEST_F(CardinalityEstimatorTest, Alias) {
  // clang-format off
  const auto input_lqp =
  AliasNode::make(expression_vector(a_b, a_a), std::vector<std::string>{"x", "y"},
    node_a);
  // clang-format on

  const auto input_table_statistics = node_a->table_statistics();
  const auto result_table_statistics = estimator.estimate_statistics(input_lqp);

  EXPECT_EQ(result_table_statistics->row_count, 100u);
  ASSERT_EQ(result_table_statistics->column_statistics.size(), 2u);
  EXPECT_EQ(result_table_statistics->column_statistics.at(0), input_table_statistics->column_statistics.at(1));
  EXPECT_EQ(result_table_statistics->column_statistics.at(1), input_table_statistics->column_statistics.at(0));
}

TEST_F(CardinalityEstimatorTest, JoinNumericEquiInner) {
  // clang-format off
  const auto input_lqp =
  JoinNode::make(JoinMode::Inner, equals_(b_a, c_x),
     node_b,
     node_c);
  // clang-format on

  const auto result_statistics = estimator.estimate_statistics(input_lqp);

  ASSERT_EQ(result_statistics->column_statistics.size(), 4u);
  ASSERT_EQ(result_statistics->row_count, 128u);

  const auto column_statistics_b_a =
      std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(result_statistics->column_statistics.at(0));
  const auto join_histogram_b_a = column_statistics_b_a->histogram;
  EXPECT_EQ(join_histogram_b_a->bin_count(), 4u);

  const auto column_statistics_b_b =
      std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(result_statistics->column_statistics[1]);
  const auto scaled_histogram_b_b = column_statistics_b_b->histogram;
  EXPECT_EQ(scaled_histogram_b_b->total_count(), 32 * 4);

  const auto column_statistics_c_x =
      std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(result_statistics->column_statistics[2]);
  const auto join_histogram_c_x = column_statistics_c_x->histogram;
  EXPECT_EQ(join_histogram_c_x->bin_count(), 4u);

  const auto column_statistics_c_y =
      std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(result_statistics->column_statistics[3]);
  const auto scaled_histogram_c_y = column_statistics_c_y->histogram;
  EXPECT_EQ(scaled_histogram_c_y->total_count(), 64 * 2);
}

TEST_F(CardinalityEstimatorTest, JoinNumericEquiInnerMultiPredicates) {
  // For now, secondary join predicates are ignored for CardinalityEstimation

  // clang-format off
  const auto input_lqp =
  JoinNode::make(JoinMode::Inner, expression_vector(equals_(b_a, c_x), equals_(b_b, c_x)),
    node_b,
    node_c);
  // clang-format on

  const auto result_statistics = estimator.estimate_statistics(input_lqp);

  ASSERT_EQ(result_statistics->column_statistics.size(), 4u);
  ASSERT_EQ(result_statistics->row_count, 128u);
}

TEST_F(CardinalityEstimatorTest, JoinNumericNonEquiInner) {
  // Test that joins on with non-equi predicate conditions are estimated as cross joins (for now)

  // clang-format off
  const auto input_lqp_a = JoinNode::make(JoinMode::Inner, not_equals_(b_a, c_x), node_b, node_c);
  const auto input_lqp_b = JoinNode::make(JoinMode::Inner, greater_than_(b_a, c_x), node_b, node_c);
  const auto input_lqp_c = JoinNode::make(JoinMode::Inner, greater_than_equals_(b_a, c_x), node_b, node_c);
  const auto input_lqp_d = JoinNode::make(JoinMode::Inner, less_than_(b_a, c_x), node_b, node_c);
  const auto input_lqp_e = JoinNode::make(JoinMode::Inner, less_than_equals_(b_a, c_x), node_b, node_c);
  // clang-format on

  EXPECT_EQ(estimator.estimate_statistics(input_lqp_a)->row_count, 32u * 64u);
  EXPECT_EQ(estimator.estimate_statistics(input_lqp_a)->column_statistics.size(), 4u);
  EXPECT_EQ(estimator.estimate_statistics(input_lqp_b)->row_count, 32u * 64u);
  EXPECT_EQ(estimator.estimate_statistics(input_lqp_b)->column_statistics.size(), 4u);
  EXPECT_EQ(estimator.estimate_statistics(input_lqp_c)->row_count, 32u * 64u);
  EXPECT_EQ(estimator.estimate_statistics(input_lqp_c)->column_statistics.size(), 4u);
  EXPECT_EQ(estimator.estimate_statistics(input_lqp_d)->row_count, 32u * 64u);
  EXPECT_EQ(estimator.estimate_statistics(input_lqp_d)->column_statistics.size(), 4u);
  EXPECT_EQ(estimator.estimate_statistics(input_lqp_e)->row_count, 32u * 64u);
  EXPECT_EQ(estimator.estimate_statistics(input_lqp_e)->column_statistics.size(), 4u);
}

TEST_F(CardinalityEstimatorTest, JoinEquiInnerDifferentDataTypes) {
  // Test that joins on columns with non-equal data types are estimated as cross joins (for now)

  // clang-format off
  const auto input_lqp =
  JoinNode::make(JoinMode::Inner, equals_(b_a, f_b),
     node_b,
     node_f);
  // clang-format on

  const auto result_statistics = estimator.estimate_statistics(input_lqp);

  ASSERT_EQ(result_statistics->row_count, 32u * 200u);
  ASSERT_EQ(result_statistics->column_statistics.size(), 4u);
}

TEST_F(CardinalityEstimatorTest, JoinCross) {
  // clang-format off
  const auto input_lqp =
  JoinNode::make(JoinMode::Cross,
    node_b,
    node_c);
  // clang-format on

  const auto result_statistics = estimator.estimate_statistics(input_lqp);

  ASSERT_EQ(result_statistics->row_count, 32u * 64u);

  ASSERT_EQ(result_statistics->column_statistics.size(), 4u);

  const auto column_statistics_b_a =
      std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(result_statistics->column_statistics.at(0));
  EXPECT_EQ(column_statistics_b_a->histogram->total_count(), 32u * 64u);

  const auto column_statistics_b_b =
      std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(result_statistics->column_statistics.at(1));
  EXPECT_EQ(column_statistics_b_b->histogram->total_count(), 32u * 64u);

  const auto column_statistics_c_x =
      std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(result_statistics->column_statistics.at(2));
  EXPECT_EQ(column_statistics_c_x->histogram->total_count(), 32u * 64u);

  const auto column_statistics_c_y =
      std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(result_statistics->column_statistics.at(3));
  EXPECT_EQ(column_statistics_c_y->histogram->total_count(), 32u * 64u);
}

TEST_F(CardinalityEstimatorTest, JoinBinsInnerEqui) {
  EXPECT_FLOAT_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(1.0f, 1.0f, 1.0f, 1.0f).first, 1.0f);
  EXPECT_FLOAT_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(1.0f, 1.0f, 1.0f, 1.0f).second, 1.0f);

  EXPECT_FLOAT_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(2.0f, 1.0f, 1.0f, 1.0f).first, 2.0f);
  EXPECT_FLOAT_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(2.0f, 1.0f, 1.0f, 1.0f).second, 1.0f);

  EXPECT_FLOAT_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(2.0f, 1.0f, 2.0f, 1.0f).first, 4.0f);
  EXPECT_FLOAT_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(2.0f, 1.0f, 2.0f, 1.0f).second, 1.0f);

  EXPECT_FLOAT_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(2.0f, 2.0f, 2.0f, 1.0f).first, 2.0f);
  EXPECT_FLOAT_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(2.0f, 2.0f, 1.0f, 1.0f).second, 1.0f);

  EXPECT_FLOAT_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(200.0f, 20.0f, 3000.0f, 2500.0f).first,
                  240.0f);
  EXPECT_FLOAT_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(200.0f, 20.0f, 3000.0f, 2500.0f).second,
                  20.0f);

  // Test DistinctCount > Height
  EXPECT_FLOAT_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(2.0f, 3.0f, 2.0f, 7.0f).first, 0.5714286f);
  EXPECT_FLOAT_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(2.0f, 3.0f, 1.0f, 7.0f).second, 3.0f);

  // Test Heights/Distinct counts < 1
  EXPECT_FLOAT_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(2.0f, 0.1f, 2.0f, 1.0f).first, 4.0f);
  EXPECT_FLOAT_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(2.0f, 0.1f, 2.0f, 1.0f).second, 0.1f);

  EXPECT_FLOAT_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(0.0f, 0.0f, 2.0f, 1.0f).first, 0.0f);
  EXPECT_FLOAT_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(0.0f, 0.0f, 2.0f, 1.0f).second, 0.0f);

  EXPECT_FLOAT_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(200.0f, 20.0f, 3000.0f, 0.1f).first, 30000.0f);
  EXPECT_FLOAT_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(200.0f, 20.0f, 3000.0f, 0.1f).second, 0.1f);

  EXPECT_FLOAT_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(200.0f, 1.0f, 0.3f, 0.3f).first, 60.0f);
  EXPECT_FLOAT_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(200.0f, 1.0f, 0.3f, 0.3f).second, 0.3f);
}

TEST_F(CardinalityEstimatorTest, JoinInnerEquiHistograms) {
  const auto left_histogram = GenericHistogram<int32_t>(
      std::vector<int32_t>{0, 10, 20, 30, 40, 50, 60}, std::vector<int32_t>{9, 19, 29, 39, 49, 59, 69},
      std::vector<HistogramCountType>{10, 15, 10, 20, 5, 15, 5}, std::vector<HistogramCountType>{1, 1, 3, 8, 1, 5, 1});

  const auto right_histogram =
      GenericHistogram<int32_t>(std::vector<int32_t>{20, 30, 50}, std::vector<int32_t>{29, 39, 59},
                                std::vector<HistogramCountType>{10, 5, 10}, std::vector<HistogramCountType>{7, 2, 10});

  const auto join_histogram =
      CardinalityEstimator::estimate_inner_equi_join_with_histograms<int32_t>(left_histogram, right_histogram);

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

TEST_F(CardinalityEstimatorTest, JoinOuter) {
  // Test that left, right and full outer join operations are estimated the same as an inner join (for now)

  const auto inner_join_lqp = JoinNode::make(JoinMode::Inner, equals_(a_a, b_a), node_a, node_b);
  const auto inner_join_cardinality = estimator.estimate_cardinality(inner_join_lqp);

  const auto left_join_lqp = JoinNode::make(JoinMode::Left, equals_(a_a, b_a), node_a, node_b);
  EXPECT_EQ(estimator.estimate_cardinality(left_join_lqp), inner_join_cardinality);

  const auto right_join_lqp = JoinNode::make(JoinMode::Right, equals_(a_a, b_a), node_a, node_b);
  EXPECT_EQ(estimator.estimate_cardinality(right_join_lqp), inner_join_cardinality);

  const auto full_join_lqp = JoinNode::make(JoinMode::FullOuter, equals_(a_a, b_a), node_a, node_b);
  EXPECT_EQ(estimator.estimate_cardinality(left_join_lqp), inner_join_cardinality);
}

TEST_F(CardinalityEstimatorTest, JoinSemiHistograms) {
  const auto left_join_column_histogram = std::make_shared<GenericHistogram<int32_t>>(
      std::vector<int32_t>{0, 10, 20, 30, 40, 50, 60}, std::vector<int32_t>{9, 19, 29, 39, 49, 59, 69},
      std::vector<HistogramCountType>{10, 15, 10, 20, 5, 15, 5}, std::vector<HistogramCountType>{1, 1, 3, 8, 1, 6, 1});
  const auto left_join_column_statistics = std::make_shared<AttributeStatistics<int32_t>>();
  left_join_column_statistics->set_statistics_object(left_join_column_histogram);

  const auto left_non_join_column_histogram = std::make_shared<GenericHistogram<int32_t>>(
      std::vector<int32_t>{0, 5, 10}, std::vector<int32_t>{4, 9, 14}, std::vector<HistogramCountType>{20, 40, 30},
      std::vector<HistogramCountType>{1, 1, 3});
  const auto left_non_join_column_statistics = std::make_shared<AttributeStatistics<int32_t>>();
  left_non_join_column_statistics->set_statistics_object(left_non_join_column_histogram);

  const auto right_histogram = std::make_shared<GenericHistogram<int32_t>>(
      std::vector<int32_t>{20, 30, 50, 70}, std::vector<int32_t>{29, 39, 69, 79},
      std::vector<HistogramCountType>{10, 5, 10, 8}, std::vector<HistogramCountType>{7, 2, 6, 8});
  const auto right_statistics = std::make_shared<AttributeStatistics<int32_t>>();
  right_statistics->set_statistics_object(right_histogram);

  const auto left_table_statistics =
      TableStatistics{{left_join_column_statistics, left_non_join_column_statistics}, 90};
  const auto right_table_statistics = TableStatistics{{right_statistics}, 33};

  const auto join_estimation =
      CardinalityEstimator::estimate_semi_join(ColumnID{0}, ColumnID{0}, left_table_statistics, right_table_statistics);

  // Left Join Column Histogram:  |  Right Join Column Histogram:  |  Resulting Histogram
  //                              |                                |
  // Bin       Height  Distinct   |  Bin       Height  Distinct    |  Bin       Height  Distinct  Comment
  // [ 0,  9]  10      1          |                                |
  // [10, 19]  15      1          |                                |
  // [20, 29]  10      3          |  [20, 29]  (10)    7           |  [20, 29]  10      3         100% as 3 <= 7
  // [30, 39]  20      8          |  [30, 39]  (5)     2           |  [30, 39]  5       2         h: 20*(2/8)
  // [40, 49]   5      1          |                                |
  // [50, 59]  15      6          |  [50,                          |  [50, 59]  7.5     3         h: 15*((6/2)/6)
  // [60, 69]   5      1          |       69]  (10)    6           |  [69, 69]  5       1         100% as 1 <= (6/2)
  //                              |  [70, 79]  (8)     8           |
  //                              |                                |
  //                              |  (Height is ignored on right)  |      sum:  27.5

  EXPECT_EQ(join_estimation->row_count, 27.5);
  EXPECT_EQ(join_estimation->column_statistics.size(), 2);

  const auto& first_column_histogram =
      *static_cast<const AttributeStatistics<int32_t>&>(*join_estimation->column_statistics[0]).histogram;
  EXPECT_EQ(first_column_histogram.bin_count(), 4);
  EXPECT_EQ(first_column_histogram.bin(0), HistogramBin<int32_t>(20, 29, 10, 3));
  EXPECT_EQ(first_column_histogram.bin(1), HistogramBin<int32_t>(30, 39, 5, 2));
  EXPECT_EQ(first_column_histogram.bin(2), HistogramBin<int32_t>(50, 59, 7.5, 3));
  EXPECT_EQ(first_column_histogram.bin(3), HistogramBin<int32_t>(60, 69, 5, 1));

  const auto& second_column_histogram =
      *static_cast<const AttributeStatistics<int32_t>&>(*join_estimation->column_statistics[1]).histogram;
  EXPECT_EQ(second_column_histogram.bin_count(), 3);
  const auto selectivity = 27.5f / 90;
  EXPECT_EQ(second_column_histogram.bin(0), HistogramBin<int32_t>(0, 4, 20 * selectivity, 1));
  EXPECT_EQ(second_column_histogram.bin(1), HistogramBin<int32_t>(5, 9, 40 * selectivity, 1));
  EXPECT_EQ(second_column_histogram.bin(2), HistogramBin<int32_t>(10, 14, 30 * selectivity, 3));
}

TEST_F(CardinalityEstimatorTest, JoinAnti) {
  // Test that anti joins are estimated return the left input statistics (for now)

  const auto anti_null_as_false_join_lqp = JoinNode::make(JoinMode::AntiNullAsFalse, equals_(a_a, b_a), node_a, node_b);
  EXPECT_EQ(estimator.estimate_statistics(anti_null_as_false_join_lqp), node_a->table_statistics());

  const auto anti_null_as_true_join_lqp = JoinNode::make(JoinMode::AntiNullAsTrue, equals_(a_a, b_a), node_a, node_b);
  EXPECT_EQ(estimator.estimate_statistics(anti_null_as_true_join_lqp), node_a->table_statistics());
}

TEST_F(CardinalityEstimatorTest, LimitWithValueExpression) {
  const auto limit_lqp_a = LimitNode::make(value_(1), node_a);
  EXPECT_EQ(estimator.estimate_cardinality(limit_lqp_a), 1);

  const auto limit_statistics_a = estimator.estimate_statistics(limit_lqp_a);

  EXPECT_EQ(limit_statistics_a->row_count, 1);
  ASSERT_EQ(limit_statistics_a->column_statistics.size(), 2u);

  const auto column_statistics_a =
      std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(limit_statistics_a->column_statistics.at(0));
  const auto column_statistics_b =
      std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(limit_statistics_a->column_statistics.at(1));

  // Limit doesn't write out StatisticsObjects
  ASSERT_TRUE(column_statistics_a);
  EXPECT_FALSE(column_statistics_a->histogram);
  ASSERT_TRUE(column_statistics_b);
  EXPECT_FALSE(column_statistics_b->histogram);
}

TEST_F(CardinalityEstimatorTest, LimitWithValueExpressionExeedingInputRowCount) {
  const auto limit_lqp_a = LimitNode::make(value_(1000), node_a);
  EXPECT_EQ(estimator.estimate_cardinality(limit_lqp_a), 100);

  const auto limit_statistics_a = estimator.estimate_statistics(limit_lqp_a);

  EXPECT_EQ(limit_statistics_a->row_count, 100);
}

TEST_F(CardinalityEstimatorTest, LimitWithComplexExpression) {
  const auto limit_lqp_a = LimitNode::make(add_(1, 2), node_a);
  // The "complex" expression `1+2` is evaluated and LimitNode is estimated to have a selectivity of 1
  EXPECT_EQ(estimator.estimate_cardinality(limit_lqp_a), 100);
  EXPECT_EQ(estimator.estimate_statistics(limit_lqp_a), node_a->table_statistics());
}

TEST_F(CardinalityEstimatorTest, MockNode) {
  EXPECT_EQ(estimator.estimate_cardinality(node_a), 100);
  EXPECT_EQ(estimator.estimate_statistics(node_a), node_a->table_statistics());
}

TEST_F(CardinalityEstimatorTest, PredicateWithOneSimplePredicate) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(a_a, 50),
    node_a);
  // clang-format on

  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp), 50.0f);

  const auto plan_output_statistics = estimator.estimate_statistics(input_lqp);
  EXPECT_FLOAT_EQ(plan_output_statistics->row_count, 50.0f);  // Same as above
  ASSERT_EQ(plan_output_statistics->column_statistics.size(), 2u);

  const auto plan_output_statistics_a =
      std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(plan_output_statistics->column_statistics.at(0));
  const auto plan_output_statistics_b =
      std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(plan_output_statistics->column_statistics.at(1));
  ASSERT_TRUE(plan_output_statistics_a);
  ASSERT_TRUE(plan_output_statistics_b);

  ASSERT_TRUE(plan_output_statistics_a->histogram);
  ASSERT_TRUE(plan_output_statistics_b->histogram);

  ASSERT_EQ(plan_output_statistics_a->histogram->bin_count(), 1u);
  EXPECT_EQ(plan_output_statistics_a->histogram->bin_minimum(BinID{0}), 51);
  EXPECT_EQ(plan_output_statistics_a->histogram->bin_maximum(BinID{0}), 100);
  EXPECT_EQ(plan_output_statistics_a->histogram->bin_height(BinID{0}), 50);

  ASSERT_EQ(plan_output_statistics_b->histogram->bin_count(), 1u);
  EXPECT_EQ(plan_output_statistics_b->histogram->bin_minimum(BinID{0}), 10);
  EXPECT_EQ(plan_output_statistics_b->histogram->bin_maximum(BinID{0}), 129);
  EXPECT_EQ(plan_output_statistics_b->histogram->bin_height(BinID{0}), 35);
}

TEST_F(CardinalityEstimatorTest, PredicateWithOneBetweenPredicate) {
  for (const auto between_predicate_condition :
       {PredicateCondition::BetweenInclusive, PredicateCondition::BetweenLowerExclusive,
        PredicateCondition::BetweenUpperExclusive, PredicateCondition::BetweenExclusive}) {
    const auto between_predicate =
        std::make_shared<BetweenExpression>(between_predicate_condition, lqp_column_(a_a), value_(10), value_(89));

    // clang-format off
    const auto input_lqp =
      PredicateNode::make(between_predicate,
        node_a);
    // clang-format on

    EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp), 80.0f);

    const auto plan_output_statistics = estimator.estimate_statistics(input_lqp);
    EXPECT_FLOAT_EQ(plan_output_statistics->row_count, 80.0f);  // Same as above
    ASSERT_EQ(plan_output_statistics->column_statistics.size(), 2u);

    const auto plan_output_statistics_a =
        std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(plan_output_statistics->column_statistics.at(0));
    const auto plan_output_statistics_b =
        std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(plan_output_statistics->column_statistics.at(1));
    ASSERT_TRUE(plan_output_statistics_a);
    ASSERT_TRUE(plan_output_statistics_b);

    ASSERT_TRUE(plan_output_statistics_a->histogram);
    ASSERT_TRUE(plan_output_statistics_b->histogram);

    ASSERT_EQ(plan_output_statistics_a->histogram->bin_count(), 1u);
    EXPECT_EQ(plan_output_statistics_a->histogram->bin_minimum(BinID{0}), 10);
    EXPECT_EQ(plan_output_statistics_a->histogram->bin_maximum(BinID{0}), 89);
    EXPECT_EQ(plan_output_statistics_a->histogram->bin_height(BinID{0}), 80);
  }
}

TEST_F(CardinalityEstimatorTest, PredicateTwoOnTheSameColumn) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(a_a, 50),
    PredicateNode::make(less_than_equals_(a_a, 75),
      node_a));
  // clang-format on

  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp), 25);
}

TEST_F(CardinalityEstimatorTest, PredicateAnd) {
  // Same as PredicateTwoOnTheSameColumn, but the conditions are within one predicate
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(and_(greater_than_(a_a, 50), less_than_equals_(a_a, 75)),
      node_a);
  // clang-format on

  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp), 25);
}

TEST_F(CardinalityEstimatorTest, PredicateOr) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(or_(less_than_equals_(a_a, 10), greater_than_(a_a, 90)),
      node_a);
  // clang-format on

  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp), 20);
}

TEST_F(CardinalityEstimatorTest, PredicateOrDoesNotIncreaseCardinality) {
  // While we do not handle overlapping ranges yet, we at least want to make sure that tables don't become bigger.
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(or_(less_than_equals_(a_a, 60), greater_than_(a_a, 20)),
      node_a);
  // clang-format on

  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp), 100);
}

TEST_F(CardinalityEstimatorTest, PredicateTwoOnDifferentColumns) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(a_a, 50),  // s=0.5
    PredicateNode::make(less_than_equals_(a_b, 75),  // s=0.55
      node_a));
  // clang-format on

  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp), 27.5f);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp->left_input()), 55.0f);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp->left_input()->left_input()), 100.0f);
}

TEST_F(CardinalityEstimatorTest, PredicateMultiple) {
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
      PredicateNode::make(between_inclusive_(d_a, placeholder_(ParameterID{0}), placeholder_(ParameterID{1})), node_d);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(lqp_e), 25.0f);
}

TEST_F(CardinalityEstimatorTest, PredicateMultipleWithCorrelatedParameter) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(a_a, 50),  // s=0.5
    PredicateNode::make(less_than_equals_(a_b, correlated_parameter_(ParameterID{1}, a_a)),
      node_a));
  // clang-format on

  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp), 45.0f);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp->left_input()), 90.0f);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp->left_input()->left_input()), 100.0f);
}

TEST_F(CardinalityEstimatorTest, PredicateWithNull) {
  const auto lqp_a = PredicateNode::make(equals_(a_a, NullValue{}), node_a);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(lqp_a), 0.0f);

  const auto lqp_b = PredicateNode::make(like_(g_a, NullValue{}), node_g);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(lqp_b), 0.0f);

  const auto lqp_c = PredicateNode::make(between_inclusive_(a_a, 5, NullValue{}), node_a);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(lqp_c), 0.0f);
}

TEST_F(CardinalityEstimatorTest, PredicateEstimateColumnVsColumnEquiScan) {
  // clang-format off
  const auto left_histogram = GenericHistogram<int32_t>(
    std::vector<int32_t>           {10, 13, 16},
    std::vector<int32_t>           {12, 14, 20},
    std::vector<HistogramCountType>{3,   9, 10},
    std::vector<HistogramCountType>{2,   3, 10});

  const auto right_histogram = GenericHistogram<int32_t>(
    std::vector<int32_t>           {0, 13, 15, 16},
    std::vector<int32_t>           {5, 14, 15, 20},
    std::vector<HistogramCountType>{7,  5, 1,  10},
    std::vector<HistogramCountType>{5,  2, 1,   2});
  // clang-format on

  const auto result_histogram =
      CardinalityEstimator::estimate_column_vs_column_equi_scan_with_histograms(left_histogram, right_histogram);

  ASSERT_EQ(result_histogram->bin_count(), 2u);
  EXPECT_EQ(result_histogram->bin(BinID{0}), HistogramBin<int32_t>(13, 14, 5, 2));
  EXPECT_EQ(result_histogram->bin(BinID{1}), HistogramBin<int32_t>(16, 20, 10, 2));
}

TEST_F(CardinalityEstimatorTest, PredicateEstimateColumnVsColumnNonEquiScan) {
  // Test that a selectivity of 1 is used for ColumnVsColumn scans with a non-equals PredicateCondition

  EXPECT_EQ(estimator.estimate_cardinality(PredicateNode::make(not_equals_(a_a, a_b), node_a)), 100);
  EXPECT_EQ(estimator.estimate_cardinality(PredicateNode::make(greater_than_(a_a, a_b), node_a)), 100);
  EXPECT_EQ(estimator.estimate_cardinality(PredicateNode::make(greater_than_equals_(a_a, a_b), node_a)), 100);
  EXPECT_EQ(estimator.estimate_cardinality(PredicateNode::make(less_than_(a_a, a_b), node_a)), 100);
  EXPECT_EQ(estimator.estimate_cardinality(PredicateNode::make(less_than_equals_(a_a, a_b), node_a)), 100);
}

TEST_F(CardinalityEstimatorTest, PredicateColumnVsColumnDifferentDataTypes) {
  // Test that a selectivity of 1 is used for ColumnVsColumn scans on columns with differing column data types

  // clang-format off
  const auto input_lqp =
  PredicateNode::make(equals_(f_a, f_b),
    node_f);
  // clang-format on

  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp), 200.0f);

  const auto estimated_statistics = estimator.estimate_statistics(input_lqp);

  const auto estimated_column_statistics_a =
      std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(estimated_statistics->column_statistics.at(0));
  const auto estimated_column_statistics_b =
      std::dynamic_pointer_cast<AttributeStatistics<float>>(estimated_statistics->column_statistics.at(1));

  ASSERT_TRUE(estimated_column_statistics_a);
  ASSERT_TRUE(estimated_column_statistics_b);

  EXPECT_TRUE(estimated_column_statistics_a->histogram);
  EXPECT_TRUE(estimated_column_statistics_b->histogram);
}

TEST_F(CardinalityEstimatorTest, PredicateWithMissingStatistics) {
  // Test that a node, lacking statistical information on one column, produces an estimation when scanning on the column
  // WITH statistics and has a selectivity of 1 on columns WITHOUT statistics

  // clang-format off
  const auto input_lqp_a =
  PredicateNode::make(greater_than_(e_a, 50),
    node_e);

  const auto input_lqp_b =
  PredicateNode::make(greater_than_(e_b, 50),
    node_e);
  // clang-format on

  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp_a), 50.0f);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp_b), 100.0f);

  const auto estimated_statistics_a = estimator.estimate_statistics(input_lqp_a);
  const auto estimated_statistics_b = estimator.estimate_statistics(input_lqp_a);

  const auto estimated_column_statistics_a_a =
      std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(estimated_statistics_a->column_statistics.at(0));
  const auto estimated_column_statistics_a_b =
      std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(estimated_statistics_a->column_statistics.at(1));
  const auto estimated_column_statistics_b_a =
      std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(estimated_statistics_b->column_statistics.at(0));
  const auto estimated_column_statistics_b_b =
      std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(estimated_statistics_b->column_statistics.at(1));

  ASSERT_TRUE(estimated_column_statistics_a_a);
  ASSERT_TRUE(estimated_column_statistics_a_b);
  ASSERT_TRUE(estimated_column_statistics_b_a);
  ASSERT_TRUE(estimated_column_statistics_b_b);

  EXPECT_TRUE(estimated_column_statistics_a_a->histogram);
  EXPECT_FALSE(estimated_column_statistics_a_b->histogram);
  EXPECT_TRUE(estimated_column_statistics_b_a->histogram);
  EXPECT_FALSE(estimated_column_statistics_b_b->histogram);
}

TEST_F(CardinalityEstimatorTest, PredicateString) {
  const auto input_lqp_a = PredicateNode::make(equals_(g_a, "a"), node_g);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp_a), 2.5f);

  const auto input_lqp_b = PredicateNode::make(equals_(g_a, "z"), node_g);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp_b), 2.5f);

  const auto input_lqp_c = PredicateNode::make(greater_than_equals_(g_a, "a"), node_g);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp_c), 100.0f);

  const auto input_lqp_d = PredicateNode::make(greater_than_equals_(g_a, "m"), node_g);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp_d), 52.0f);

  const auto input_lqp_e = PredicateNode::make(like_(g_a, "a%"), node_g);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp_e), 10.0f);

  const auto input_lqp_f = PredicateNode::make(not_like_(g_a, "a%"), node_g);
  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp_f), 90.0f);
}

TEST_F(CardinalityEstimatorTest, Projection) {
  // clang-format off
  const auto input_lqp =
  ProjectionNode::make(expression_vector(a_b, add_(a_b, a_a), a_a),
    node_a);
  // clang-format on

  const auto input_table_statistics = node_a->table_statistics();
  const auto result_table_statistics = estimator.estimate_statistics(input_lqp);

  EXPECT_EQ(result_table_statistics->row_count, 100u);
  ASSERT_EQ(result_table_statistics->column_statistics.size(), 3u);
  EXPECT_EQ(result_table_statistics->column_statistics.at(0), input_table_statistics->column_statistics.at(1));
  EXPECT_TRUE(result_table_statistics->column_statistics.at(1));
  EXPECT_EQ(result_table_statistics->column_statistics.at(2), input_table_statistics->column_statistics.at(0));
}

TEST_F(CardinalityEstimatorTest, Sort) {
  // clang-format off
  const auto input_lqp =
  SortNode::make(expression_vector(a_b), std::vector<OrderByMode>{OrderByMode::Ascending},
                 node_a);
  // clang-format on

  const auto result_table_statistics = estimator.estimate_statistics(input_lqp);
  EXPECT_EQ(result_table_statistics, node_a->table_statistics());
}

TEST_F(CardinalityEstimatorTest, StoredTable) {
  Hyrise::get().storage_manager.add_table("t", load_table("resources/test_data/tbl/int.tbl"));
  EXPECT_EQ(estimator.estimate_cardinality(StoredTableNode::make("t")), 3);
}

TEST_F(CardinalityEstimatorTest, Validate) {
  // Test Validate doesn't break the TableStatistics. The CardinalityEstimator is not estimating anything for Validate
  // as there are no statistics available atm to base such an estimation on.

  // clang-format off
  const auto input_lqp =
  ValidateNode::make(
    node_a);
  // clang-format on

  const auto input_table_statistics = node_a->table_statistics();
  const auto result_table_statistics = estimator.estimate_statistics(input_lqp);

  EXPECT_EQ(result_table_statistics->row_count, 100u);
  ASSERT_EQ(result_table_statistics->column_statistics.size(), 2u);
}

TEST_F(CardinalityEstimatorTest, Union) {
  // Test that UnionNodes sum up the input row counts and return the left input statistics (for now)

  // clang-format off
  const auto input_lqp =
  UnionNode::make(UnionMode::Positions,
    node_a,
    node_b);
  // clang-format on

  EXPECT_FLOAT_EQ(estimator.estimate_cardinality(input_lqp), 132.0f);

  const auto result_statistics = estimator.estimate_statistics(input_lqp);

  ASSERT_EQ(result_statistics->column_statistics.size(), 2u);
  ASSERT_EQ(result_statistics->column_statistics.at(0), node_a->table_statistics()->column_statistics.at(0));
  ASSERT_EQ(result_statistics->column_statistics.at(1), node_a->table_statistics()->column_statistics.at(1));
}

TEST_F(CardinalityEstimatorTest, NonQueryNodes) {
  // Test that, basically, the CardinalityEstimator doesn't crash when processing non-query nodes. There is not much
  // more to test here

  const auto create_table_lqp = CreateTableNode::make("t", false, node_a);
  EXPECT_EQ(estimator.estimate_cardinality(create_table_lqp), 0.0f);

  const auto prepared_plan = std::make_shared<PreparedPlan>(node_a, std::vector<ParameterID>{});
  const auto create_prepared_plan_lqp = CreatePreparedPlanNode::make("t", prepared_plan);
  EXPECT_EQ(estimator.estimate_cardinality(create_prepared_plan_lqp), 0.0f);

  const auto lqp_view = std::make_shared<LQPView>(
      node_a, std::unordered_map<ColumnID, std::string>{{ColumnID{0}, "x"}, {ColumnID{1}, "y"}});
  const auto create_view_lqp = CreateViewNode::make("v", lqp_view, false);
  EXPECT_EQ(estimator.estimate_cardinality(create_view_lqp), 0.0f);

  const auto update_lqp = UpdateNode::make("t", node_a, node_b);
  EXPECT_EQ(estimator.estimate_cardinality(update_lqp), 0.0f);

  const auto insert_lqp = InsertNode::make("t", node_a);
  EXPECT_EQ(estimator.estimate_cardinality(insert_lqp), 0.0f);

  EXPECT_EQ(estimator.estimate_cardinality(DeleteNode::make(node_a)), 0.0f);
  EXPECT_EQ(estimator.estimate_cardinality(DropViewNode::make("v", false)), 0.0f);
  EXPECT_EQ(estimator.estimate_cardinality(DropTableNode::make("t", false)), 0.0f);
  EXPECT_EQ(estimator.estimate_cardinality(DummyTableNode::make()), 0.0f);
}

}  // namespace opossum
