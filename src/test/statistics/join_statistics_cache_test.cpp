#include "gtest/gtest.h"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "statistics/join_statistics_cache.hpp"
#include "statistics/segment_statistics2.hpp"
#include "statistics/table_statistics2.hpp"
#include "statistics/table_statistics_slice.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class JoinStatisticsCacheTest : public ::testing::Test {
 public:
  void SetUp() override {
    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}});
    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}});
    node_c = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}});

    a_a = node_a->get_column("a");
    a_b = node_a->get_column("b");
    b_a = node_b->get_column("a");
    b_b = node_b->get_column("b");
    c_a = node_c->get_column("a");

    statistics_a_a = std::make_shared<SegmentStatistics2<int32_t>>();
    statistics_a_b = std::make_shared<SegmentStatistics2<int32_t>>();
    statistics_b_a = std::make_shared<SegmentStatistics2<int32_t>>();
    statistics_b_b = std::make_shared<SegmentStatistics2<int32_t>>();

    const auto statistics_slice_ab = std::make_shared<TableStatisticsSlice>(5);
    statistics_slice_ab->segment_statistics.emplace_back(statistics_a_a);
    statistics_slice_ab->segment_statistics.emplace_back(statistics_a_b);
    statistics_slice_ab->segment_statistics.emplace_back(statistics_b_a);
    statistics_slice_ab->segment_statistics.emplace_back(statistics_b_b);

    table_statistics_a_b = std::make_shared<TableStatistics2>(std::vector<DataType>{DataType::Int, DataType::Int});
    table_statistics_a_b->cardinality_estimation_slices.emplace_back(statistics_slice_ab);

    validate_c = ValidateNode::make(node_c);

    cache = create_cache({node_a, node_b, validate_c},
                         {equals_(a_a, b_a), equals_(b_a, c_a), greater_than_(c_a, 5), less_than_(c_a, a_a)});
  }

  static std::shared_ptr<JoinStatisticsCache> create_cache(
      const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices,
      const std::vector<std::shared_ptr<AbstractExpression>>& predicates) {
    auto vertex_indices = JoinStatisticsCache::VertexIndexMap{};
    for (auto vertex_idx = size_t{0}; vertex_idx < vertices.size(); ++vertex_idx) {
      vertex_indices.emplace(vertices[vertex_idx], vertex_idx);
    }

    auto predicate_indices = JoinStatisticsCache::PredicateIndexMap{};
    for (auto predicate_idx = size_t{0}; predicate_idx < predicates.size(); ++predicate_idx) {
      predicate_indices.emplace(predicates[predicate_idx], predicate_idx);
    }

    return std::make_shared<JoinStatisticsCache>(std::move(vertex_indices), std::move(predicate_indices));
  }

  std::shared_ptr<MockNode> node_a, node_b, node_c;
  std::shared_ptr<AbstractLQPNode> validate_c;
  LQPColumnReference a_a, a_b, b_a, b_b, c_a;
  std::shared_ptr<TableStatistics2> table_statistics_a_b;
  std::shared_ptr<BaseSegmentStatistics2> statistics_a_a, statistics_a_b, statistics_b_a, statistics_b_b;
  std::shared_ptr<JoinStatisticsCache> cache;
};

TEST_F(JoinStatisticsCacheTest, Bitmask) {
  // clang-format off
  const auto lqp_a =
  PredicateNode::make(greater_than_(c_a, 5),
    validate_c);
  // clang-format on

  const auto bitmask_a = cache->bitmask(lqp_a);
  ASSERT_TRUE(bitmask_a);
  EXPECT_EQ(*bitmask_a, JoinStatisticsCache::Bitmask(7, 0b0100100));

  // clang-format off
  const auto lqp_b_0 =
  PredicateNode::make(greater_than_(c_a, 5),
    JoinNode::make(JoinMode::Inner, equals_(b_a, c_a),
      node_a,
      validate_c));
  const auto lqp_b_1 =
  PredicateNode::make(greater_than_(c_a, 5),
    PredicateNode::make(equals_(b_a, c_a),
      JoinNode::make(JoinMode::Cross,
        node_a,
        validate_c)));
  // clang-format on

  const auto bitmask_b_0 = cache->bitmask(lqp_b_0);
  ASSERT_TRUE(bitmask_b_0);
  EXPECT_EQ(*bitmask_b_0, JoinStatisticsCache::Bitmask(7, 0b0110101));

  const auto bitmask_b_1 = cache->bitmask(lqp_b_1);
  ASSERT_TRUE(bitmask_b_1);
  EXPECT_EQ(*bitmask_b_1, JoinStatisticsCache::Bitmask(7, 0b0110101));
}

TEST_F(JoinStatisticsCacheTest, BitmaskNotFound) {
  // Test LQPs for which no bitmask should be generated

  // clang-format off
  const auto lqp_a =
  PredicateNode::make(greater_than_(c_a, 5),
    JoinNode::make(JoinMode::Inner, equals_(b_a, c_a),
      node_a,
      node_c));
  // clang-format on

  EXPECT_EQ(cache->bitmask(lqp_a), std::nullopt);

  // clang-format off
  const auto lqp_b =
  PredicateNode::make(greater_than_(c_a, 5),
    JoinNode::make(JoinMode::Left, equals_(b_a, c_a),
      node_a,
      validate_c));
  // clang-format on

  EXPECT_EQ(cache->bitmask(lqp_b), std::nullopt);

  // clang-format off
  const auto lqp_d =
  PredicateNode::make(greater_than_(c_a, 6),
    JoinNode::make(JoinMode::Inner, equals_(b_a, c_a),
      node_a,
      node_c));
  // clang-format on

  EXPECT_EQ(cache->bitmask(lqp_d), std::nullopt);
}

TEST_F(JoinStatisticsCacheTest, Caching) {
  EXPECT_EQ(cache->get(JoinStatisticsCache::Bitmask{7, 0b0001011}, expression_vector(a_a, a_b, b_a, b_b)), nullptr);

  cache->set(JoinStatisticsCache::Bitmask{7, 0b0001011}, expression_vector(a_a, a_b, b_a, b_b), table_statistics_a_b);

  const auto cached_a_b = cache->get(JoinStatisticsCache::Bitmask{7, 0b0001011}, expression_vector(a_a, a_b, b_a, b_b));
  ASSERT_NE(cached_a_b, nullptr);
  ASSERT_EQ(cached_a_b->cardinality_estimation_slices.size(), 1u);
  EXPECT_EQ(cached_a_b->cardinality_estimation_slices[0]->segment_statistics.size(), 4u);
  EXPECT_EQ(cached_a_b->cardinality_estimation_slices[0]->segment_statistics[0], statistics_a_a);
  EXPECT_EQ(cached_a_b->cardinality_estimation_slices[0]->segment_statistics[1], statistics_a_b);
  EXPECT_EQ(cached_a_b->cardinality_estimation_slices[0]->segment_statistics[2], statistics_b_a);
  EXPECT_EQ(cached_a_b->cardinality_estimation_slices[0]->segment_statistics[3], statistics_b_b);

  const auto cached_b_a = cache->get(JoinStatisticsCache::Bitmask{7, 0b0001011}, expression_vector(b_a, b_b, a_a, a_b));
  ASSERT_NE(cached_b_a, nullptr);
  ASSERT_EQ(cached_b_a->cardinality_estimation_slices.size(), 1u);
  EXPECT_EQ(cached_b_a->cardinality_estimation_slices[0]->segment_statistics.size(), 4u);
  EXPECT_EQ(cached_b_a->cardinality_estimation_slices[0]->segment_statistics[0], statistics_b_a);
  EXPECT_EQ(cached_b_a->cardinality_estimation_slices[0]->segment_statistics[1], statistics_b_b);
  EXPECT_EQ(cached_b_a->cardinality_estimation_slices[0]->segment_statistics[2], statistics_a_a);
  EXPECT_EQ(cached_b_a->cardinality_estimation_slices[0]->segment_statistics[3], statistics_a_b);
}

}  // namespace opossum
