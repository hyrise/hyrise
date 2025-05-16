#include <memory>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <vector>

#include "magic_enum.hpp"

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
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/update_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "logical_query_plan/window_node.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"
#include "statistics/statistics_objects/generic_histogram.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class CardinalityEstimatorTest : public BaseTest {
 public:
  void SetUp() override {
    // Turn off statistics pruning to see if everything works as expected.
    estimator.do_not_prune_unused_statistics();

    /**
     * node_a
     */
    node_a = create_mock_node_with_statistics({{DataType::Int, "a"}, {DataType::Int, "b"}}, 100,
                                              {GenericHistogram<int32_t>::with_single_bin(1, 100, 100, 10),
                                               GenericHistogram<int32_t>::with_single_bin(10, 129, 70, 55)});

    a_a = node_a->get_column("a");
    a_b = node_a->get_column("b");

    /**
     * node_b
     */
    const auto histogram_b_a = std::make_shared<GenericHistogram<int32_t>>(
        std::vector<int32_t>{0, 5, 10}, std::vector<int32_t>{4, 9, 15}, std::vector<HistogramCountType>{10, 10, 12},
        std::vector<HistogramCountType>{5, 5, 6});

    const auto histogram_b_b = std::make_shared<GenericHistogram<int32_t>>(
        std::vector<int32_t>{0}, std::vector<int32_t>{9}, std::vector<HistogramCountType>{32},
        std::vector<HistogramCountType>{10});

    node_b = create_mock_node_with_statistics({{DataType::Int, "a"}, {DataType::Int, "b"}}, 32,
                                              {histogram_b_a, histogram_b_b});

    b_a = node_b->get_column("a");
    b_b = node_b->get_column("b");

    /**
     * node_c
     */
    const auto histogram_c_x = std::make_shared<EqualDistinctCountHistogram<int32_t>>(
        std::vector<int32_t>{0, 8}, std::vector<int32_t>{7, 15}, std::vector<HistogramCountType>{32, 32}, 8, 0);

    const auto histogram_c_y = std::make_shared<GenericHistogram<int32_t>>(
        std::vector<int32_t>{0}, std::vector<int32_t>{9}, std::vector<HistogramCountType>{64},
        std::vector<HistogramCountType>{10});

    node_c = create_mock_node_with_statistics({{DataType::Int, "x"}, {DataType::Int, "y"}}, 64,
                                              {histogram_c_x, histogram_c_y});

    c_x = node_c->get_column("x");
    c_y = node_c->get_column("y");

    /**
     * node_d
     */
    node_d = create_mock_node_with_statistics(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, 100,
        {GenericHistogram<int32_t>::with_single_bin(10, 109, 100, 20),
         GenericHistogram<int32_t>::with_single_bin(50, 59, 100, 5),
         GenericHistogram<int32_t>::with_single_bin(100, 1099, 100, 2)});

    d_a = std::make_shared<LQPColumnExpression>(node_d, ColumnID{0});
    d_b = std::make_shared<LQPColumnExpression>(node_d, ColumnID{1});
    d_c = std::make_shared<LQPColumnExpression>(node_d, ColumnID{2});

    /**
     * node_e
     * Has no statistics on column "b"
     */
    node_e = create_mock_node_with_statistics({{DataType::Int, "a"}, {DataType::Int, "b"}}, 100,
                                              {GenericHistogram<int32_t>::with_single_bin(1, 100, 10, 10), nullptr});

    e_a = node_e->get_column("a");
    e_b = node_e->get_column("b");

    /**
     * node_f
     * Has columns with different data types
     */
    node_f = create_mock_node_with_statistics({{DataType::Int, "a"}, {DataType::Float, "b"}}, 200,
                                              {GenericHistogram<int32_t>::with_single_bin(1, 200, 200, 10),
                                               GenericHistogram<float>::with_single_bin(1.0f, 100.0f, 200, 150)});

    f_a = node_f->get_column("a");
    f_b = node_f->get_column("b");

    /**
     * node_g
     * Has a string column
     */
    node_g = create_mock_node_with_statistics({{DataType::String, "a"}}, 100,
                                              {GenericHistogram<pmr_string>::with_single_bin("a", "z", 100, 40)});

    g_a = node_g->get_column("a");
  }

  CardinalityEstimator estimator{};
  std::shared_ptr<LQPColumnExpression> a_a, a_b, b_a, b_b, c_x, c_y, d_a, d_b, d_c, e_a, e_b, f_a, f_b, g_a;
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

  EXPECT_EQ(result_table_statistics->row_count, 100);
  ASSERT_EQ(result_table_statistics->column_statistics.size(), 3);
  EXPECT_EQ(result_table_statistics->column_statistics.at(0), input_table_statistics->column_statistics.at(1));
  EXPECT_TRUE(
      dynamic_cast<const CardinalityEstimator::DummyStatistics*>(&*result_table_statistics->column_statistics.at(1)));
  EXPECT_TRUE(
      dynamic_cast<const CardinalityEstimator::DummyStatistics*>(&*result_table_statistics->column_statistics.at(2)));
}

TEST_F(CardinalityEstimatorTest, AggregateWithoutGroupBy) {
  const auto input_lqp = AggregateNode::make(expression_vector(), expression_vector(sum_(a_a)), node_a);

  const auto result_table_statistics = estimator.estimate_statistics(input_lqp);

  EXPECT_EQ(result_table_statistics->row_count, 1);
  ASSERT_EQ(result_table_statistics->column_statistics.size(), 1);
  EXPECT_TRUE(
      dynamic_cast<const CardinalityEstimator::DummyStatistics*>(&*result_table_statistics->column_statistics.at(0)));
}

TEST_F(CardinalityEstimatorTest, Alias) {
  // clang-format off
  const auto input_lqp =
  AliasNode::make(expression_vector(a_b, a_a), std::vector<std::string>{"x", "y"},
    node_a);
  // clang-format on

  const auto input_table_statistics = node_a->table_statistics();
  const auto result_table_statistics = estimator.estimate_statistics(input_lqp);

  EXPECT_EQ(result_table_statistics->row_count, 100);
  ASSERT_EQ(result_table_statistics->column_statistics.size(), 2);
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

  ASSERT_EQ(result_statistics->column_statistics.size(), 4);
  ASSERT_EQ(result_statistics->row_count, 128);

  const auto column_statistics_b_a =
      std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(result_statistics->column_statistics.at(0));
  const auto join_histogram_b_a = column_statistics_b_a->histogram;
  EXPECT_EQ(join_histogram_b_a->bin_count(), 4);

  const auto column_statistics_b_b =
      std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(result_statistics->column_statistics[1]);
  const auto scaled_histogram_b_b = column_statistics_b_b->histogram;
  EXPECT_EQ(scaled_histogram_b_b->total_count(), 32 * 4);

  const auto column_statistics_c_x =
      std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(result_statistics->column_statistics[2]);
  const auto join_histogram_c_x = column_statistics_c_x->histogram;
  EXPECT_EQ(join_histogram_c_x->bin_count(), 4);

  const auto column_statistics_c_y =
      std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(result_statistics->column_statistics[3]);
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

  ASSERT_EQ(result_statistics->column_statistics.size(), 4);
  ASSERT_EQ(result_statistics->row_count, 128);
}

TEST_F(CardinalityEstimatorTest, JoinNumericNonEquiInner) {
  // Test that joins on with non-equi predicate conditions are estimated as cross joins (for now)

  const auto input_lqp_a = JoinNode::make(JoinMode::Inner, not_equals_(b_a, c_x), node_b, node_c);
  const auto input_lqp_b = JoinNode::make(JoinMode::Inner, greater_than_(b_a, c_x), node_b, node_c);
  const auto input_lqp_c = JoinNode::make(JoinMode::Inner, greater_than_equals_(b_a, c_x), node_b, node_c);
  const auto input_lqp_d = JoinNode::make(JoinMode::Inner, less_than_(b_a, c_x), node_b, node_c);
  const auto input_lqp_e = JoinNode::make(JoinMode::Inner, less_than_equals_(b_a, c_x), node_b, node_c);

  EXPECT_EQ(estimator.estimate_statistics(input_lqp_a)->row_count, 32 * 64);
  EXPECT_EQ(estimator.estimate_statistics(input_lqp_a)->column_statistics.size(), 4);
  EXPECT_EQ(estimator.estimate_statistics(input_lqp_b)->row_count, 32 * 64);
  EXPECT_EQ(estimator.estimate_statistics(input_lqp_b)->column_statistics.size(), 4);
  EXPECT_EQ(estimator.estimate_statistics(input_lqp_c)->row_count, 32 * 64);
  EXPECT_EQ(estimator.estimate_statistics(input_lqp_c)->column_statistics.size(), 4);
  EXPECT_EQ(estimator.estimate_statistics(input_lqp_d)->row_count, 32 * 64);
  EXPECT_EQ(estimator.estimate_statistics(input_lqp_d)->column_statistics.size(), 4);
  EXPECT_EQ(estimator.estimate_statistics(input_lqp_e)->row_count, 32 * 64);
  EXPECT_EQ(estimator.estimate_statistics(input_lqp_e)->column_statistics.size(), 4);
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

  ASSERT_EQ(result_statistics->row_count, 32 * 200);
  ASSERT_EQ(result_statistics->column_statistics.size(), 4);
}

TEST_F(CardinalityEstimatorTest, JoinCross) {
  // clang-format off
  const auto input_lqp =
  JoinNode::make(JoinMode::Cross,
    node_b,
    node_c);
  // clang-format on

  const auto result_statistics = estimator.estimate_statistics(input_lqp);

  ASSERT_EQ(result_statistics->row_count, 32 * 64);

  ASSERT_EQ(result_statistics->column_statistics.size(), 4);

  const auto column_statistics_b_a =
      std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(result_statistics->column_statistics.at(0));
  EXPECT_EQ(column_statistics_b_a->histogram->total_count(), 32 * 64);

  const auto column_statistics_b_b =
      std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(result_statistics->column_statistics.at(1));
  EXPECT_EQ(column_statistics_b_b->histogram->total_count(), 32 * 64);

  const auto column_statistics_c_x =
      std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(result_statistics->column_statistics.at(2));
  EXPECT_EQ(column_statistics_c_x->histogram->total_count(), 32 * 64);

  const auto column_statistics_c_y =
      std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(result_statistics->column_statistics.at(3));
  EXPECT_EQ(column_statistics_c_y->histogram->total_count(), 32 * 64);
}

TEST_F(CardinalityEstimatorTest, JoinBinsInnerEqui) {
  EXPECT_DOUBLE_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(1.0, 1.0, 1.0, 1.0).first, 1.0);
  EXPECT_DOUBLE_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(1.0, 1.0, 1.0, 1.0).second, 1.0);

  EXPECT_DOUBLE_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(2.0, 1.0, 1.0, 1.0).first, 2.0);
  EXPECT_DOUBLE_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(2.0, 1.0, 1.0, 1.0).second, 1.0);

  EXPECT_DOUBLE_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(2.0, 1.0, 2.0, 1.0).first, 4.0);
  EXPECT_DOUBLE_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(2.0, 1.0, 2.0, 1.0).second, 1.0);

  EXPECT_DOUBLE_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(2.0, 2.0, 2.0, 1.0).first, 2.0);
  EXPECT_DOUBLE_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(2.0, 2.0, 1.0, 1.0).second, 1.0);

  EXPECT_DOUBLE_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(200.0, 20.0, 3000.0, 2500.0).first, 240.0);
  EXPECT_DOUBLE_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(200.0, 20.0, 3000.0, 2500.0).second, 20.0);

  // Test DistinctCount > Height
  EXPECT_DOUBLE_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(2.0, 3.0, 2.0, 7.0).first, 4.0 / 7.0);
  EXPECT_DOUBLE_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(2.0, 3.0, 1.0, 7.0).second, 3.0);

  // Test Heights/DistinctCounts < 1
  EXPECT_DOUBLE_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(2.0, 0.1, 2.0, 1.0).first, 4.0);
  EXPECT_DOUBLE_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(2.0, 0.1, 2.0, 1.0).second, 0.1);

  EXPECT_DOUBLE_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(0.0, 0.0, 2.0, 1.0).first, 0.0);
  EXPECT_DOUBLE_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(0.0, 0.0, 2.0, 1.0).second, 0.0);

  EXPECT_DOUBLE_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(200.0, 20.0, 3000.0, 0.1).first, 30000.0);
  EXPECT_DOUBLE_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(200.0, 20.0, 3000.0, 0.1).second, 0.1);

  EXPECT_DOUBLE_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(200.0, 1.0, 0.3, 0.3).first, 60.0);
  EXPECT_DOUBLE_EQ(CardinalityEstimator::estimate_inner_equi_join_of_bins(200.0, 1.0, 0.3, 0.3).second, 0.3);
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

  ASSERT_EQ(join_histogram->bin_count(), 3);

  EXPECT_EQ(join_histogram->bin_minimum(0), 20);
  EXPECT_EQ(join_histogram->bin_maximum(0), 29);
  EXPECT_DOUBLE_EQ(join_histogram->bin_height(0), 10.0 * 10.0 * (1.0 / 7.0));
  EXPECT_EQ(join_histogram->bin_distinct_count(0), 3);

  EXPECT_EQ(join_histogram->bin_minimum(1), 30);
  EXPECT_EQ(join_histogram->bin_maximum(1), 39);
  EXPECT_DOUBLE_EQ(join_histogram->bin_height(1), 20.0 * 5.0 * (1.0 / 8.0));
  EXPECT_EQ(join_histogram->bin_distinct_count(1), 2);

  EXPECT_EQ(join_histogram->bin_minimum(2), 50);
  EXPECT_EQ(join_histogram->bin_maximum(2), 59);
  EXPECT_DOUBLE_EQ(join_histogram->bin_height(2), 15.0 * 10.0 * (1.0 / 10.0));
  EXPECT_EQ(join_histogram->bin_distinct_count(2), 5);
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
  const auto selectivity = 27.5 / 90;
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

TEST_F(CardinalityEstimatorTest, JoinWithDummyStatistics) {
  // Joins on projections, aggregates, etc. cannot use histograms. For now, we expect those joins to preserve all tuples
  // in the worst case.
  for (const auto swap_inputs : {true, false}) {
    for (const auto join_mode : magic_enum::enum_values<JoinMode>()) {
      // Semi-/anti-joins must have the input to be reduced on the left side.
      if (is_semi_or_anti_join(join_mode) && swap_inputs) {
        continue;
      }

      auto left_input = static_pointer_cast<AbstractLQPNode>(node_a);
      // clang-format off
      auto right_input = static_pointer_cast<AbstractLQPNode>(
      ProjectionNode::make(expression_vector(value_(123)),
        DummyTableNode::make()));
      // clang-format on
      if (swap_inputs) {
        std::swap(left_input, right_input);
      }
      const auto join_node = join_mode == JoinMode::Cross ? JoinNode::make(join_mode)
                                                          : JoinNode::make(join_mode, equals_(a_a, value_(123)));
      join_node->set_left_input(left_input);
      join_node->set_right_input(right_input);

      EXPECT_EQ(estimator.estimate_cardinality(join_node), 100) << " for " << join_mode << " join";
    }
  }
}

TEST_F(CardinalityEstimatorTest, LimitWithValueExpression) {
  const auto limit_lqp = LimitNode::make(value_(1), node_a);
  EXPECT_EQ(estimator.estimate_cardinality(limit_lqp), 1);

  const auto limit_statistics = estimator.estimate_statistics(limit_lqp);

  EXPECT_EQ(limit_statistics->row_count, 1);
  ASSERT_EQ(limit_statistics->column_statistics.size(), 2);

  // Limit does not write out StatisticsObjects.
  EXPECT_TRUE(dynamic_cast<const CardinalityEstimator::DummyStatistics*>(&*limit_statistics->column_statistics.at(0)));
  EXPECT_TRUE(dynamic_cast<const CardinalityEstimator::DummyStatistics*>(&*limit_statistics->column_statistics.at(1)));
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

  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp), 50.0);

  const auto plan_output_statistics = estimator.estimate_statistics(input_lqp);
  EXPECT_DOUBLE_EQ(plan_output_statistics->row_count, 50.0);  // Same as above
  ASSERT_EQ(plan_output_statistics->column_statistics.size(), 2);

  const auto plan_output_statistics_a =
      std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(plan_output_statistics->column_statistics.at(0));
  const auto plan_output_statistics_b =
      std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(plan_output_statistics->column_statistics.at(1));
  ASSERT_TRUE(plan_output_statistics_a);
  ASSERT_TRUE(plan_output_statistics_b);

  ASSERT_TRUE(plan_output_statistics_a->histogram);
  ASSERT_TRUE(plan_output_statistics_b->histogram);

  ASSERT_EQ(plan_output_statistics_a->histogram->bin_count(), 1);
  EXPECT_EQ(plan_output_statistics_a->histogram->bin_minimum(BinID{0}), 51);
  EXPECT_EQ(plan_output_statistics_a->histogram->bin_maximum(BinID{0}), 100);
  EXPECT_EQ(plan_output_statistics_a->histogram->bin_height(BinID{0}), 50);

  ASSERT_EQ(plan_output_statistics_b->histogram->bin_count(), 1);
  EXPECT_EQ(plan_output_statistics_b->histogram->bin_minimum(BinID{0}), 10);
  EXPECT_EQ(plan_output_statistics_b->histogram->bin_maximum(BinID{0}), 129);
  EXPECT_EQ(plan_output_statistics_b->histogram->bin_height(BinID{0}), 35);
}

TEST_F(CardinalityEstimatorTest, PredicateWithOneBetweenPredicate) {
  for (const auto between_predicate_condition :
       {PredicateCondition::BetweenInclusive, PredicateCondition::BetweenLowerExclusive,
        PredicateCondition::BetweenUpperExclusive, PredicateCondition::BetweenExclusive}) {
    const auto between_predicate =
        std::make_shared<BetweenExpression>(between_predicate_condition, a_a, value_(10), value_(89));

    // clang-format off
    const auto input_lqp =
      PredicateNode::make(between_predicate,
        node_a);
    // clang-format on

    EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp), 80.0);

    const auto plan_output_statistics = estimator.estimate_statistics(input_lqp);
    EXPECT_DOUBLE_EQ(plan_output_statistics->row_count, 80.0);  // Same as above
    ASSERT_EQ(plan_output_statistics->column_statistics.size(), 2);

    const auto plan_output_statistics_a =
        std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(plan_output_statistics->column_statistics.at(0));
    const auto plan_output_statistics_b =
        std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(plan_output_statistics->column_statistics.at(1));
    ASSERT_TRUE(plan_output_statistics_a);
    ASSERT_TRUE(plan_output_statistics_b);

    ASSERT_TRUE(plan_output_statistics_a->histogram);
    ASSERT_TRUE(plan_output_statistics_b->histogram);

    ASSERT_EQ(plan_output_statistics_a->histogram->bin_count(), 1);
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

  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp), 25);
}

TEST_F(CardinalityEstimatorTest, PredicateAnd) {
  // Same as PredicateTwoOnTheSameColumn, but the conditions are within one predicate
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(and_(greater_than_(a_a, 50), less_than_equals_(a_a, 75)),
      node_a);
  // clang-format on

  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp), 25);
}

TEST_F(CardinalityEstimatorTest, PredicateOr) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(or_(less_than_equals_(a_a, 10), greater_than_(a_a, 90)),
      node_a);
  // clang-format on

  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp), 20);
}

TEST_F(CardinalityEstimatorTest, PredicateOrDoesNotIncreaseCardinality) {
  // While we do not handle overlapping ranges yet, we at least want to make sure that tables don't become bigger.
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(or_(less_than_equals_(a_a, 60), greater_than_(a_a, 20)),
      node_a);
  // clang-format on

  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp), 100);
}

TEST_F(CardinalityEstimatorTest, PredicateIn) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(in_(a_a, list_(10, 11, 12, 13)),
      node_a);
  // clang-format on

  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp), 40);
}

TEST_F(CardinalityEstimatorTest, PredicateTwoOnDifferentColumns) {
  // clang-format off
  const auto input_lqp =
  // 50% of rows in histogram, which itself covers 100% of rows in the table, leads to a selectivity of 0.5 * 1 = 0.5
  PredicateNode::make(greater_than_(a_a, 50),
    // 55% of rows in histogram, which itself covers 70% of rows in the table,
    // leads to a selectivity of 0.55*0.7 = 0.385
    PredicateNode::make(less_than_equals_(a_b, 75),
      node_a));
  // clang-format on

  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp), 19.25);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp->left_input()), 38.5);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp->left_input()->left_input()), 100.0);
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

  // Cardinality = (values below predicate) / bin width * rows = (500 - 100) / (1099 - 100 + 1) * 100 = 40.
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp->left_input()->left_input()->left_input()->left_input()),
                   40.0);

  // Cardinality = rows - (values below predicate) / bin width * rows = 40 - (90 - 10) / (109 - 10 + 1) * 40 = 8.
  // New distinct count is 20 - (90 - 10) / (109 - 10 + 1) * 20 = 4.
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp->left_input()->left_input()->left_input()), 8.0);

  // Column b's lowest value is  50, predicate does not filter anything.
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp->left_input()->left_input()), 8.0);

  // Cardinality = rows - (values below/equals predicate) / bin width * rows = 8 - (56 - 50) / (59 - 50 + 1) * 8 = 3.2.
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp->left_input()), 3.2);

  // Cardinality = rows / distinct count = 3.2 / 4 < 1.
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp), 1.0);
}

TEST_F(CardinalityEstimatorTest, PredicateWithValuePlaceholder) {
  // 20 distinct values in column d_a and 100 values total. So == is assumed to have a selectivity of 5%, != of 95%, and
  // everything else is assumed to hit 50%

  const auto lqp_a = PredicateNode::make(equals_(d_a, placeholder_(ParameterID{0})), node_d);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp_a), 5.0);

  const auto lqp_b = PredicateNode::make(not_equals_(d_a, placeholder_(ParameterID{0})), node_d);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp_b), 95.0);

  const auto lqp_c = PredicateNode::make(less_than_(d_a, placeholder_(ParameterID{0})), node_d);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp_c), 50.0);

  const auto lqp_d = PredicateNode::make(greater_than_(d_a, placeholder_(ParameterID{0})), node_d);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp_d), 50.0);

  // BETWEEN is split up into (a >= ? AND a <= ?), so it ends up with a selecitivity of 25%
  const auto lqp_e =
      PredicateNode::make(between_inclusive_(d_a, placeholder_(ParameterID{0}), placeholder_(ParameterID{1})), node_d);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp_e), 25.0);
}

TEST_F(CardinalityEstimatorTest, PredicateMultipleWithCorrelatedParameter) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(a_a, 50),  // s=0.5
    PredicateNode::make(less_than_equals_(a_b, correlated_parameter_(ParameterID{1}, a_a)),
      node_a));
  // clang-format on

  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp), 45.0);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp->left_input()), 90.0);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp->left_input()->left_input()), 100.0);
}

TEST_F(CardinalityEstimatorTest, PredicateWithNull) {
  const auto lqp_a = PredicateNode::make(equals_(a_a, NullValue{}), node_a);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp_a), 0.0);

  const auto lqp_b = PredicateNode::make(like_(g_a, NullValue{}), node_g);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp_b), 0.0);

  const auto lqp_c = PredicateNode::make(between_inclusive_(a_a, 5, NullValue{}), node_a);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp_c), 0.0);
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

  ASSERT_EQ(result_histogram->bin_count(), 2);
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

  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp), 200.0);

  const auto estimated_statistics = estimator.estimate_statistics(input_lqp);

  const auto estimated_column_statistics_a =
      std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(estimated_statistics->column_statistics.at(0));
  const auto estimated_column_statistics_b =
      std::dynamic_pointer_cast<const AttributeStatistics<float>>(estimated_statistics->column_statistics.at(1));

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

  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp_a), 5.0);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp_b), 100.0);

  const auto estimated_statistics_a = estimator.estimate_statistics(input_lqp_a);
  const auto estimated_statistics_b = estimator.estimate_statistics(input_lqp_a);

  const auto estimated_column_statistics_a_a =
      std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(estimated_statistics_a->column_statistics.at(0));
  const auto estimated_column_statistics_a_b =
      std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(estimated_statistics_a->column_statistics.at(1));
  const auto estimated_column_statistics_b_a =
      std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(estimated_statistics_b->column_statistics.at(0));
  const auto estimated_column_statistics_b_b =
      std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(estimated_statistics_b->column_statistics.at(1));

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
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp_a), 2.5);

  const auto input_lqp_b = PredicateNode::make(equals_(g_a, "z"), node_g);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp_b), 2.5);

  const auto input_lqp_c = PredicateNode::make(greater_than_equals_(g_a, "a"), node_g);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp_c), 100.0);

  const auto input_lqp_d = PredicateNode::make(greater_than_equals_(g_a, "m"), node_g);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp_d), 52.0);

  const auto input_lqp_e = PredicateNode::make(like_(g_a, "a%"), node_g);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp_e), 10.0);

  const auto input_lqp_f = PredicateNode::make(not_like_(g_a, "a%"), node_g);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp_f), 90.0);
}

TEST_F(CardinalityEstimatorTest, Projection) {
  // clang-format off
  const auto input_lqp =
  ProjectionNode::make(expression_vector(a_b, add_(a_b, a_a), a_a),
    node_a);
  // clang-format on

  const auto input_table_statistics = node_a->table_statistics();
  const auto result_table_statistics = estimator.estimate_statistics(input_lqp);

  EXPECT_EQ(result_table_statistics->row_count, 100);
  ASSERT_EQ(result_table_statistics->column_statistics.size(), 3);
  EXPECT_EQ(result_table_statistics->column_statistics.at(0), input_table_statistics->column_statistics.at(1));
  EXPECT_TRUE(
      dynamic_cast<const CardinalityEstimator::DummyStatistics*>(&*result_table_statistics->column_statistics.at(1)));
  EXPECT_EQ(result_table_statistics->column_statistics.at(2), input_table_statistics->column_statistics.at(0));
}

TEST_F(CardinalityEstimatorTest, Sort) {
  // clang-format off
  const auto input_lqp =
  SortNode::make(expression_vector(a_b), std::vector<SortMode>{SortMode::Ascending},
                 node_a);
  // clang-format on

  const auto result_table_statistics = estimator.estimate_statistics(input_lqp);
  EXPECT_EQ(result_table_statistics, node_a->table_statistics());
}

TEST_F(CardinalityEstimatorTest, StoredTable) {
  Hyrise::get().storage_manager.add_table("t", load_table("resources/test_data/tbl/int.tbl"));
  EXPECT_EQ(estimator.estimate_cardinality(StoredTableNode::make("t")), 3);
}

TEST_F(CardinalityEstimatorTest, StaticTable) {
  const auto table = load_table("resources/test_data/tbl/int.tbl");
  const auto static_table_node = StaticTableNode::make(table);

  // Case (i): No statistics available, create dummy statistics.
  const auto dummy_statistics = estimator.estimate_statistics(static_table_node);
  EXPECT_DOUBLE_EQ(dummy_statistics->row_count, 3.0);
  ASSERT_EQ(dummy_statistics->column_statistics.size(), 1);
  ASSERT_EQ(dummy_statistics->column_statistics[0]->data_type, DataType::Int);
  EXPECT_TRUE(dynamic_cast<const CardinalityEstimator::DummyStatistics*>(&*dummy_statistics->column_statistics[0]));

  // Case (ii): Statistics available, simply forward them.
  table->set_table_statistics(TableStatistics::from_table(*table));
  const auto forwarded_statistics = estimator.estimate_statistics(static_table_node);
  EXPECT_EQ(forwarded_statistics, table->table_statistics());
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

  EXPECT_EQ(result_table_statistics->row_count, 100);
  ASSERT_EQ(result_table_statistics->column_statistics.size(), 2);
}

TEST_F(CardinalityEstimatorTest, Union) {
  // Test that UnionNodes sum up the input row counts and return the left input statistics (for now)

  // clang-format off
  const auto input_lqp =
  UnionNode::make(SetOperationMode::Positions,
    node_a,
    node_b);
  // clang-format on

  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(input_lqp), 132.0);

  const auto result_statistics = estimator.estimate_statistics(input_lqp);

  ASSERT_EQ(result_statistics->column_statistics.size(), 2);
  ASSERT_EQ(result_statistics->column_statistics.at(0), node_a->table_statistics()->column_statistics.at(0));
  ASSERT_EQ(result_statistics->column_statistics.at(1), node_a->table_statistics()->column_statistics.at(1));
}

TEST_F(CardinalityEstimatorTest, NonQueryNodes) {
  // Test that, basically, the CardinalityEstimator doesn't crash when processing non-query nodes. There is not much
  // more to test here

  const auto create_table_lqp = CreateTableNode::make("t", false, node_a);
  EXPECT_EQ(estimator.estimate_cardinality(create_table_lqp), 0.0);

  const auto prepared_plan = std::make_shared<PreparedPlan>(node_a, std::vector<ParameterID>{});
  const auto create_prepared_plan_lqp = CreatePreparedPlanNode::make("t", prepared_plan);
  EXPECT_EQ(estimator.estimate_cardinality(create_prepared_plan_lqp), 0.0);

  const auto lqp_view = std::make_shared<LQPView>(
      node_a, std::unordered_map<ColumnID, std::string>{{ColumnID{0}, "x"}, {ColumnID{1}, "y"}});
  const auto create_view_lqp = CreateViewNode::make("v", lqp_view, false);
  EXPECT_EQ(estimator.estimate_cardinality(create_view_lqp), 0.0);

  const auto update_lqp = UpdateNode::make("t", node_a, node_b);
  EXPECT_EQ(estimator.estimate_cardinality(update_lqp), 0.0);

  const auto insert_lqp = InsertNode::make("t", node_a);
  EXPECT_EQ(estimator.estimate_cardinality(insert_lqp), 0.0);

  EXPECT_EQ(estimator.estimate_cardinality(DeleteNode::make(node_a)), 0.0);
  EXPECT_EQ(estimator.estimate_cardinality(DropViewNode::make("v", false)), 0.0);
  EXPECT_EQ(estimator.estimate_cardinality(DropTableNode::make("t", false)), 0.0);
  EXPECT_EQ(estimator.estimate_cardinality(DummyTableNode::make()), 0.0);
}

// Usually, we assume that we know nothing about predicates getting their values from uncorrelated subqueries and we
// simply forward the input estimations. However, special predicates with subqueries can stem from join rewrites (e.g.,
// JoinToPredicateRewriteRule). We estimate the resulting plans just like we would have done it for a semi-join. The
// following two test cases ensure the correct behavior for the possible rewrites.
TEST_F(CardinalityEstimatorTest, ValueScanWithUncorrelatedSubquery) {
  // Case (i): Predicate column = <subquery>
  // Example query:
  //     SELECT n_name FROM nation WHERE n_regionkey = (SELECT r_regionkey FROM region WHERE r_name = 'ASIA');
  // In this case, SELECT r_regionkey ... is a selection on the primary key. We know it will result in a single value,
  // so using an uncorrelated subquery to get this value is feasible.
  // clang-format off
  const auto subquery =
  ProjectionNode::make(expression_vector(c_y),
    PredicateNode::make(equals_(c_y, value_(2)),
      node_c));
  // clang-format on

  auto lqp = PredicateNode::make(equals_(a_a, lqp_subquery_(subquery)), node_a);

  const auto semi_join_lqp = JoinNode::make(JoinMode::Semi, equals_(a_a, c_y), node_a, subquery);

  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp), estimator.estimate_cardinality(semi_join_lqp));
  EXPECT_LT(estimator.estimate_cardinality(lqp), estimator.estimate_cardinality(node_a));

  // Ensure correlated subqueries are not considered and we forward the input estimates.
  lqp = PredicateNode::make(equals_(a_a, lqp_subquery_(subquery, std::make_pair(ParameterID{0}, a_b))), node_a);

  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp), estimator.estimate_cardinality(node_a));

  // Ensure that any predicate condition other than equals and between still lead to forwarding the input estimates.
  for (const auto predicate_condition :
       {PredicateCondition::LessThan, PredicateCondition::LessThanEquals, PredicateCondition::NotEquals,
        PredicateCondition::GreaterThan, PredicateCondition::GreaterThanEquals}) {
    const auto predicate =
        std::make_shared<BinaryPredicateExpression>(predicate_condition, a_a, lqp_subquery_(subquery));
    lqp = PredicateNode::make(predicate, node_a);

    EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp), estimator.estimate_cardinality(node_a));
  }
}

TEST_F(CardinalityEstimatorTest, BetweenScanWithUncorrelatedSubquery) {
  // Case (ii): Predicate column BETWEEN min(<subquery>) and max(<subquery>)
  // Example query:
  //     SELECT SUM(ws_ext_sales_price) FROM web_sales
  //      WHERE ws_sold_date_sk BETWEEN (SELECT MIN(d_date_sk) FROM date_dim WHERE d_year = 2000)
  //                                AND (SELECT MAX(d_date_sk) FROM date_dim WHERE d_year = 2000);
  const auto subquery = PredicateNode::make(between_inclusive_(c_y, value_(0), value_(2)), node_c);

  const auto min_c_y = AggregateNode::make(expression_vector(), expression_vector(min_(c_y)), subquery);

  const auto max_c_y = AggregateNode::make(expression_vector(), expression_vector(max_(c_y)), subquery);

  auto lqp = PredicateNode::make(between_inclusive_(a_a, lqp_subquery_(min_c_y), lqp_subquery_(max_c_y)), node_a);

  const auto semi_join_lqp = JoinNode::make(JoinMode::Semi, equals_(a_a, c_y), node_a, subquery);

  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp), estimator.estimate_cardinality(semi_join_lqp));
  EXPECT_LT(estimator.estimate_cardinality(lqp), estimator.estimate_cardinality(node_a));

  // Ensure that other between predicate conditions behave the same.
  for (const auto predicate_condition :
       {PredicateCondition::BetweenLowerExclusive, PredicateCondition::BetweenUpperExclusive,
        PredicateCondition::BetweenExclusive}) {
    const auto predicate =
        std::make_shared<BetweenExpression>(predicate_condition, a_a, lqp_subquery_(min_c_y), lqp_subquery_(max_c_y));
    lqp = PredicateNode::make(predicate, node_a);
    EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp), estimator.estimate_cardinality(semi_join_lqp));
  }

  // Ensure correlated subqueries are not considered and we forward the input estimates.
  // clang-format off
  lqp =
  PredicateNode::make(between_inclusive_(a_a, lqp_subquery_(min_c_y, std::make_pair(ParameterID{0}, a_b)), lqp_subquery_(max_c_y, std::make_pair(ParameterID{0}, a_b))),  // NOLINT(whitespace/line_length)
    node_a);
  // clang-format on

  // Ensure we do not consider the wrong aggregate functions and forward the input estimates.
  lqp = PredicateNode::make(between_inclusive_(a_a, lqp_subquery_(max_c_y), lqp_subquery_(max_c_y)), node_a);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp), estimator.estimate_cardinality(node_a));

  lqp = PredicateNode::make(between_inclusive_(a_a, lqp_subquery_(min_c_y), lqp_subquery_(min_c_y)), node_a);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp), estimator.estimate_cardinality(node_a));

  // Aggregate functions must be performed on the same column.
  const auto min_c_x = AggregateNode::make(expression_vector(), expression_vector(min_(c_x)), subquery);
  lqp = PredicateNode::make(between_inclusive_(a_a, lqp_subquery_(min_c_x), lqp_subquery_(max_c_y)), node_a);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp), estimator.estimate_cardinality(node_a));

  // Aggregate functions must not group the tuples. Otherwise, we do not get the minimum and maximum value for the join
  // key for the underlying node.
  const auto min_c_y_grouped = AggregateNode::make(expression_vector(c_x), expression_vector(min_(c_y)), subquery);
  lqp = PredicateNode::make(between_inclusive_(a_a, lqp_subquery_(min_c_y_grouped), lqp_subquery_(max_c_y)), node_a);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp), estimator.estimate_cardinality(node_a));

  // There must not be another operator in between aggregate and origin node. Otherwise, further join keys might be
  // filtered and the predicate is not equivalent to a semi-join with the origin node anymore.
  // clang-format off
  const auto min_c_y_filtered =
  AggregateNode::make(expression_vector(), expression_vector(min_(c_y)),
    PredicateNode::make(not_equals_(c_x, value_(1)),
      subquery));
  // clang-format on
  lqp = PredicateNode::make(between_inclusive_(a_a, lqp_subquery_(min_c_y_filtered), lqp_subquery_(max_c_y)), node_a);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp), estimator.estimate_cardinality(node_a));
}

TEST_F(CardinalityEstimatorTest, BetweenScanWithUncorrelatedSubqueryAndProjections) {
  // Similar to BetweenScanWithUncorrelatedSubquery, but with a single AggregateNode for MIN/MAX and two projections.
  // Example query is the same as for that test case:
  //     SELECT SUM(ws_ext_sales_price) FROM web_sales
  //      WHERE ws_sold_date_sk BETWEEN (SELECT MIN(d_date_sk) FROM date_dim WHERE d_year = 2000)
  //                                AND (SELECT MAX(d_date_sk) FROM date_dim WHERE d_year = 2000);
  // clang-format off
  const auto subquery =
  AggregateNode::make(expression_vector(), expression_vector(min_(c_y), max_(c_y)),
    PredicateNode::make(between_inclusive_(c_y, value_(0), value_(2)),
      node_c));
  // clang-format on

  const auto min_c_y = ProjectionNode::make(expression_vector(min_(c_y)), subquery);
  const auto max_c_y = ProjectionNode::make(expression_vector(max_(c_y)), subquery);

  auto lqp = PredicateNode::make(between_inclusive_(a_a, lqp_subquery_(min_c_y), lqp_subquery_(max_c_y)), node_a);

  const auto semi_join_lqp = JoinNode::make(JoinMode::Semi, equals_(a_a, c_y), node_a, subquery->left_input());

  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp), estimator.estimate_cardinality(semi_join_lqp));
  EXPECT_LT(estimator.estimate_cardinality(lqp), estimator.estimate_cardinality(node_a));

  // Ensure that other between predicate conditions behave the same.
  for (const auto predicate_condition :
       {PredicateCondition::BetweenLowerExclusive, PredicateCondition::BetweenUpperExclusive,
        PredicateCondition::BetweenExclusive}) {
    const auto predicate =
        std::make_shared<BetweenExpression>(predicate_condition, a_a, lqp_subquery_(min_c_y), lqp_subquery_(max_c_y));
    lqp = PredicateNode::make(predicate, node_a);
    EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp), estimator.estimate_cardinality(semi_join_lqp));
  }

  // Input nodes must be ProjectionNodes.
  const auto min_c_y_alias = AliasNode::make(expression_vector(min_(c_y)), std::vector<std::string>{"foo"}, subquery);
  lqp = PredicateNode::make(between_inclusive_(a_a, lqp_subquery_(min_c_y_alias), lqp_subquery_(max_c_y)), node_a);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp), estimator.estimate_cardinality(node_a));

  // Common node must be AggregateNode.
  const auto union_node = UnionNode::make(SetOperationMode::All, subquery, subquery);
  min_c_y->set_left_input(union_node);
  max_c_y->set_left_input(union_node);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp), estimator.estimate_cardinality(node_a));

  // Aggregate node must not group the tuples.
  const auto aggregate_node = AggregateNode::make(expression_vector(c_x), expression_vector(min_(c_y), max_(c_y)));
  min_c_y->set_left_input(aggregate_node);
  max_c_y->set_left_input(aggregate_node);
  EXPECT_DOUBLE_EQ(estimator.estimate_cardinality(lqp), estimator.estimate_cardinality(node_a));
}

TEST_F(CardinalityEstimatorTest, WindowNode) {
  auto frame_description = FrameDescription{FrameType::Range, FrameBound{0, FrameBoundType::Preceding, true},
                                            FrameBound{0, FrameBoundType::CurrentRow, false}};
  const auto window =
      window_(expression_vector(), expression_vector(), std::vector<SortMode>{}, std::move(frame_description));
  const auto lqp = WindowNode::make(min_(a_a, window), node_a);

  const auto input_table_statistics = node_a->table_statistics();
  const auto result_table_statistics = estimator.estimate_statistics(lqp);

  EXPECT_EQ(result_table_statistics->row_count, 100);
  ASSERT_EQ(result_table_statistics->column_statistics.size(), 3);
  EXPECT_EQ(result_table_statistics->column_statistics.at(0), input_table_statistics->column_statistics.at(0));
  EXPECT_EQ(result_table_statistics->column_statistics.at(1), input_table_statistics->column_statistics.at(1));
  EXPECT_TRUE(
      dynamic_cast<const CardinalityEstimator::DummyStatistics*>(&*result_table_statistics->column_statistics.at(2)));
}

TEST_F(CardinalityEstimatorTest, StatisticsCaching) {
  const auto predicate_node_1 = PredicateNode::make(greater_than_(a_a, 50), node_a);

  // Enable statistics caching.
  estimator.guarantee_bottom_up_construction(predicate_node_1);
  const auto& statistics_cache = estimator.cardinality_estimation_cache.statistics_by_lqp;
  ASSERT_TRUE(statistics_cache);
  EXPECT_TRUE(statistics_cache->empty());

  // Estimate the cardinality of a node with statistics caching.
  estimator.estimate_cardinality(predicate_node_1);
  EXPECT_EQ(statistics_cache->size(), 2);
  EXPECT_TRUE(statistics_cache->contains(node_a));
  EXPECT_TRUE(statistics_cache->contains(predicate_node_1));

  // Estimate the cardinality of a node without statistics caching.
  const auto predicate_node_2 = PredicateNode::make(less_than_(a_a, 55), node_a);
  estimator.estimate_cardinality(predicate_node_2, false);
  EXPECT_EQ(statistics_cache->size(), 2);
  EXPECT_FALSE(statistics_cache->contains(predicate_node_2));
}

TEST_F(CardinalityEstimatorTest, StatisticsPruning) {
  // clang-format off
  const auto lqp =
  PredicateNode::make(greater_than_(a_a, 50),
    ProjectionNode::make(expression_vector(add_(a_a, 1), a_a, a_b, b_a, b_b),
      JoinNode::make(JoinMode::Inner, equals_(a_b, b_a),
        node_a,
        node_b)));
  // clang-format on

  // Pruning without caching is not permitted.
  EXPECT_THROW(estimator.prune_unused_statistics(), std::logic_error);
  EXPECT_FALSE(estimator.cardinality_estimation_cache.statistics_by_lqp);
  EXPECT_FALSE(estimator.cardinality_estimation_cache.required_column_expressions);

  // Ensure required but pruned expressions are noticed.
  if constexpr (HYRISE_DEBUG) {
    // Because the cardinality estimator collects the required columns during estimation, we sneak in empty cached
    // statistics. We trick the estimator to believe `node_a` was the entire LQP so it prunes all statistics and caches
    // these completely pruned statistics for `node_a`. Any estimation that actually requires statistics should notice
    // that statistics are missing.
    estimator.guarantee_bottom_up_construction(node_a);
    estimator.estimate_cardinality(node_a);
    estimator.estimate_cardinality(node_b);
    EXPECT_THROW(estimator.estimate_statistics(lqp), std::logic_error);
    const auto projection = lqp->left_input();
    lqp->set_left_input(node_a);
    EXPECT_THROW(estimator.estimate_statistics(lqp), std::logic_error);
    lqp->set_left_input(projection);
    estimator.cardinality_estimation_cache.statistics_by_lqp.reset();
    estimator.cardinality_estimation_cache.required_column_expressions.reset();
  }

  // Guaranteeing bottom-up construction (i.e., allowing caching) should enable statistics pruning and set the LQP in
  // the cache. The required columns are not populated, yet.
  estimator.guarantee_bottom_up_construction(lqp);
  EXPECT_TRUE(estimator.cardinality_estimation_cache.statistics_by_lqp);
  EXPECT_EQ(estimator.cardinality_estimation_cache.lqp, lqp);
  ASSERT_TRUE(estimator.cardinality_estimation_cache.required_column_expressions);
  ASSERT_TRUE(estimator.cardinality_estimation_cache.required_column_expressions->empty());

  // Estimate with caching.
  auto statistics = estimator.estimate_statistics(lqp);
  ASSERT_EQ(statistics->column_statistics.size(), 5);
  EXPECT_TRUE(dynamic_cast<const CardinalityEstimator::DummyStatistics*>(&*statistics->column_statistics[0]));
  EXPECT_TRUE(dynamic_cast<const AttributeStatistics<int32_t>*>(&*statistics->column_statistics[1]));
  EXPECT_TRUE(dynamic_cast<const AttributeStatistics<int32_t>*>(&*statistics->column_statistics[2]));
  EXPECT_TRUE(dynamic_cast<const AttributeStatistics<int32_t>*>(&*statistics->column_statistics[3]));
  EXPECT_TRUE(dynamic_cast<const CardinalityEstimator::DummyStatistics*>(&*statistics->column_statistics[4]));

  // After invocation, the required columns should be populated.
  EXPECT_EQ(estimator.cardinality_estimation_cache.required_column_expressions->size(), 3);
  EXPECT_TRUE(estimator.cardinality_estimation_cache.required_column_expressions->contains(a_a));
  EXPECT_TRUE(estimator.cardinality_estimation_cache.required_column_expressions->contains(a_b));
  EXPECT_TRUE(estimator.cardinality_estimation_cache.required_column_expressions->contains(b_a));
  EXPECT_FALSE(estimator.cardinality_estimation_cache.required_column_expressions->contains(b_b));
  // The memorized LQP should have been unset when populating the required expressions. Otherwise, we would do
  // superfluous work for plans that have multiple StoredTableNodes.
  EXPECT_FALSE(estimator.cardinality_estimation_cache.lqp);
}

TEST_F(CardinalityEstimatorTest, StatisticsPruningWithPrunedColumns) {
  const auto table_u = load_table("resources/test_data/tbl/all_data_types_sorted.tbl");
  const auto node_u = StaticTableNode::make(table_u);

  // clang-format off
  const auto lqp_u =
  PredicateNode::make(greater_than_(lqp_column_(node_u, ColumnID{2}), 0),
    PredicateNode::make(greater_than_(lqp_column_(node_u, ColumnID{6}), 0.0),
      PredicateNode::make(greater_than_(lqp_column_(node_u, ColumnID{8}), "0"),
        node_u)));
  // clang-format on

  // Round one: node_u does not have base table statistics. All statistics are dummy statistics.
  const auto lqp_u_dummy_statistics = estimator.estimate_statistics(lqp_u);
  for (const auto& statistics : lqp_u_dummy_statistics->column_statistics) {
    EXPECT_TRUE(dynamic_cast<const CardinalityEstimator::DummyStatistics*>(&*statistics));
  }

  // Adding the table to the StorageManager creates statistics.
  Hyrise::get().storage_manager.add_table("table_u", table_u);
  EXPECT_TRUE(table_u->table_statistics());

  // Round two: The node has base table statistics. There should not be statistics for pruned and unused columns. We
  // only used predicates that select all tuples, thus, the input statistics should be forwared for used columns.
  // For node_u, no columns are pruned. However, only columns 2, 6, and 8 are used.
  estimator.guarantee_bottom_up_construction(lqp_u);
  const auto lqp_u_statistics = estimator.estimate_statistics(lqp_u);
  ASSERT_EQ(lqp_u_statistics->column_statistics.size(), 10);
  const auto& table_u_statistics = table_u->table_statistics()->column_statistics;
  EXPECT_TRUE(dynamic_cast<const CardinalityEstimator::DummyStatistics*>(&*lqp_u_statistics->column_statistics[0]));
  EXPECT_TRUE(dynamic_cast<const CardinalityEstimator::DummyStatistics*>(&*lqp_u_statistics->column_statistics[1]));
  EXPECT_EQ(lqp_u_statistics->column_statistics[2], table_u_statistics[2]);
  EXPECT_TRUE(dynamic_cast<const CardinalityEstimator::DummyStatistics*>(&*lqp_u_statistics->column_statistics[3]));
  EXPECT_TRUE(dynamic_cast<const CardinalityEstimator::DummyStatistics*>(&*lqp_u_statistics->column_statistics[4]));
  EXPECT_TRUE(dynamic_cast<const CardinalityEstimator::DummyStatistics*>(&*lqp_u_statistics->column_statistics[5]));
  EXPECT_EQ(lqp_u_statistics->column_statistics[6], table_u_statistics[6]);
  EXPECT_TRUE(dynamic_cast<const CardinalityEstimator::DummyStatistics*>(&*lqp_u_statistics->column_statistics[7]));
  EXPECT_EQ(lqp_u_statistics->column_statistics[8], table_u_statistics[8]);
  EXPECT_TRUE(dynamic_cast<const CardinalityEstimator::DummyStatistics*>(&*lqp_u_statistics->column_statistics[9]));

  // Create the same query as for the StaticTableNode, but with pruned columns.
  const auto node_v = StoredTableNode::make("table_u");
  const auto pruned_column_ids = std::vector{{ColumnID{1}, ColumnID{3}, ColumnID{4}, ColumnID{7}, ColumnID{9}}};
  node_v->set_pruned_column_ids(pruned_column_ids);

  // clang-format off
  const auto lqp_v =
  PredicateNode::make(greater_than_(lqp_column_(node_v, ColumnID{2}), 0),
    PredicateNode::make(greater_than_(lqp_column_(node_v, ColumnID{6}), 0.0),
      PredicateNode::make(greater_than_(lqp_column_(node_v, ColumnID{8}), "0"),
        node_v)));
  // clang-format on

  // For node_v, columns 1, 3, 4, 7, and 9 have been pruned. Thus, columns 0, 2, 5, 6 and 8 remain, of which 2, 6, and
  // 8 are used.
  estimator.guarantee_bottom_up_construction(lqp_v);
  const auto lqp_v_statistics = estimator.estimate_statistics(lqp_v);
  ASSERT_EQ(lqp_v_statistics->column_statistics.size(), 5);
  EXPECT_TRUE(dynamic_cast<const CardinalityEstimator::DummyStatistics*>(&*lqp_v_statistics->column_statistics[0]));
  EXPECT_EQ(lqp_v_statistics->column_statistics[1], table_u_statistics[2]);
  EXPECT_TRUE(dynamic_cast<const CardinalityEstimator::DummyStatistics*>(&*lqp_v_statistics->column_statistics[2]));
  EXPECT_EQ(lqp_v_statistics->column_statistics[3], table_u_statistics[6]);
  EXPECT_EQ(lqp_v_statistics->column_statistics[4], table_u_statistics[8]);

  // clang-format off
  const auto lqp_d =
  PredicateNode::make(greater_than_(d_c, 0),
    node_d);
  // clang-format on

  // For node_d, column 0 is pruned and column 2 is used.
  node_d->set_pruned_column_ids({ColumnID{0}});
  estimator.guarantee_bottom_up_construction(lqp_d);
  const auto lqp_d_statistics = estimator.estimate_statistics(lqp_d);
  const auto node_d_statistics = node_d->table_statistics();
  ASSERT_EQ(lqp_d_statistics->column_statistics.size(), 2);
  EXPECT_TRUE(dynamic_cast<const CardinalityEstimator::DummyStatistics*>(&*lqp_d_statistics->column_statistics[0]));
  EXPECT_EQ(lqp_d_statistics->column_statistics[1], node_d_statistics->column_statistics[2]);
}

TEST_F(CardinalityEstimatorTest, AssertRequiredStatistics) {
  // Fail if required statistics are not present. Exceptions are if the statistics are not for a column (but, e.g.,
  // for an aggregation) or no statistics are provided at all (e.g., for a StaticTableNode).

  const auto table_u = load_table("resources/test_data/tbl/int_float_double_string.tbl");
  Hyrise::get().storage_manager.add_table("table_u", table_u);
  // After adding to the StorageManager, the table should have statistics.
  const auto table_u_statistics = table_u->table_statistics();
  ASSERT_TRUE(table_u_statistics);
  const auto node_u = StoredTableNode::make("table_u");
  const auto u_a = node_u->get_column("i");
  const auto u_b = node_u->get_column("f");
  const auto u_d = node_u->get_column("s");

  const auto table_v = load_table("resources/test_data/tbl/int_float.tbl");
  const auto node_v = StaticTableNode::make(table_v);
  const auto v_a = lqp_column_(node_v, ColumnID{0});

  // The LQP's output expressions look like this: e_a, e_b, u_a, u_b, min_(u_d), v_a, v_b.
  // clang-format off
  const auto lqp =
  JoinNode::make(JoinMode::Inner, equals_(e_a, u_a),
    node_e,
    JoinNode::make(JoinMode::Inner, equals_(u_b, v_a),
      AggregateNode::make(expression_vector(u_a, u_b), expression_vector(min_(u_d)),
        node_u),
      node_v));
  // clang-format off

  // Create input table statistics.
  auto column_statistics = std::vector<std::shared_ptr<const BaseAttributeStatistics>>(7);
  column_statistics[1] = node_e->table_statistics()->column_statistics[1];
  column_statistics[2] = table_u_statistics->column_statistics[0];
  column_statistics[3] = std::make_shared<CardinalityEstimator::DummyStatistics>(DataType::Float);
  column_statistics[4] = std::make_shared<CardinalityEstimator::DummyStatistics>(DataType::String);
  column_statistics[5] = std::make_shared<CardinalityEstimator::DummyStatistics>(DataType::Int);
  column_statistics[6] = std::make_shared<CardinalityEstimator::DummyStatistics>(DataType::Float);
  const auto statistics = std::make_shared<TableStatistics>(std::move(column_statistics), 123);

  // First statistics object is nullptr.
  EXPECT_THROW(estimator.assert_required_statistics(ColumnID{0}, lqp, statistics), std::logic_error);
  // Second statistics object is present.
  estimator.assert_required_statistics(ColumnID{1}, lqp, statistics);
  // Third statistics object is present.
  estimator.assert_required_statistics(ColumnID{2}, lqp, statistics);
  // Fourth statistics object is dummy, but there is a base statistics object for this column.
  EXPECT_THROW(estimator.assert_required_statistics(ColumnID{3}, lqp, statistics), std::logic_error);
  // Fifth statistics object is dummy. This is okay because the expression is not an LQPColumnExpression.
  estimator.assert_required_statistics(ColumnID{4}, lqp, statistics);
  // Sixth statistics object is dummy. This is okay because the column belongs to a table without table statistics.
  estimator.assert_required_statistics(ColumnID{5}, lqp, statistics);
  // Seventh statistics object is dummy. This is okay because the column belongs to a table without table statistics.
  estimator.assert_required_statistics(ColumnID{6}, lqp, statistics);

  // Estimation should work anyway.
  estimator.guarantee_bottom_up_construction(lqp);
  estimator.estimate_cardinality(lqp);
}


TEST_F(CardinalityEstimatorTest, AssertRequiredStatisticsOnComputedExpressions) {
  // Note if statistics for other expressions than LQPColumnExpressions are present so we do not forget to add a check
  // if we estimate them.
  auto frame_description = FrameDescription{FrameType::Range, FrameBound{0, FrameBoundType::Preceding, true},
                                            FrameBound{0, FrameBoundType::CurrentRow, false}};
  const auto window =
      window_(expression_vector(), expression_vector(), std::vector<SortMode>{}, std::move(frame_description));

  // clang-format off
  const auto lqp =
  WindowNode::make(min_(d_a, window),
    ProjectionNode::make(expression_vector(d_a, min_(d_c), add_(d_b, value_(1)), cast_(d_a, DataType::Int), case_(1, 1 , 1), abs_(d_a), equals_(d_a, 1)),  // NOLINT(whitespace/line_length)
      AggregateNode::make(expression_vector(d_a, d_b), expression_vector(min_(d_c)),
        node_d)));
  // clang-format on

  const auto output_expressions = lqp->output_expressions();
  const auto column_count = output_expressions.size();
  auto column_statistics = std::vector<std::shared_ptr<const BaseAttributeStatistics>>(column_count);
  for (auto& statistics : column_statistics) {
    statistics = std::make_shared<AttributeStatistics<int32_t>>();
  }
  const auto statistics = std::make_shared<TableStatistics>(std::move(column_statistics), 123);

  // The first column still is an LQPColumnExpression.
  estimator.assert_required_statistics(ColumnID{0}, lqp, statistics);

  // All other column are no LQPColumnExpressions. We should notice that there are unexpected statistics.
  for (auto column_id = ColumnID{1}; column_id < column_count; ++column_id) {
    EXPECT_THROW(estimator.assert_required_statistics(column_id, lqp, statistics), std::logic_error)
        << " ColumnID: " << column_id << ", " << magic_enum::enum_name(output_expressions[column_id]->type)
        << " expression " << output_expressions[column_id]->description();
  }
}

TEST_F(CardinalityEstimatorTest, EstimationsOnDummyStatistics) {
  // In some cases, there are no statistics available when we want to perform estimations, e.g., because they happen
  // to be on the result of an aggregation. Ensure that everything works out then. We do not care about the estimation
  // results here.

  // clang-format off
  const auto lqp =
  PredicateNode::make(between_inclusive_(count_(c_y), 37, 72),
    PredicateNode::make(less_than_(min_(d_b), 24),
      PredicateNode::make(equals_(min_(d_b), avg_(d_c)),
        PredicateNode::make(greater_than_(min_(d_b), a_b),
          PredicateNode::make(less_than_(a_a, avg_(d_c)),
            JoinNode::make(JoinMode::Inner, equals_(sum_(a_b), b_a),
              JoinNode::make(JoinMode::Inner, equals_(c_x, sum_(a_b)),
                JoinNode::make(JoinMode::Inner, equals_(sum_(a_b), min_(d_b)),
                  AggregateNode::make(expression_vector(a_a), expression_vector(sum_(a_b)),
                    node_a),
                  AggregateNode::make(expression_vector(d_a), expression_vector(min_(d_b), avg_(d_c)),
                    node_d)),
                AggregateNode::make(expression_vector(c_x), expression_vector(count_(c_y)),
                  node_c)),
              node_b))))));
  // clang-format on

  estimator.estimate_cardinality(lqp);
}

TEST_F(CardinalityEstimatorTest, DummyStatistics) {
  const auto dummy_statistics = std::make_shared<CardinalityEstimator::DummyStatistics>(DataType::Int);

  EXPECT_EQ(dummy_statistics->data_type, DataType::Int);

  // Dummy statistics forward themselves when scaled.
  EXPECT_EQ(dummy_statistics->scaled(0.2), dummy_statistics);

  // They cannot be used for actual estimations.
  EXPECT_THROW(dummy_statistics->sliced(PredicateCondition::Equals, 1), std::logic_error);
  EXPECT_THROW(dummy_statistics->pruned(123, PredicateCondition::Equals, 1), std::logic_error);

  auto stream = std::stringstream{};
  stream << *dummy_statistics;
  EXPECT_EQ(stream.str(), "DummyStatistics");
}

}  // namespace hyrise
