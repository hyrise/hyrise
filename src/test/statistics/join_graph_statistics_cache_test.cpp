#include "gtest/gtest.h"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/join_graph_statistics_cache.hpp"
#include "statistics/table_statistics.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class JoinGraphStatisticsCacheTest : public ::testing::Test {
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

    statistics_a_a = std::make_shared<AttributeStatistics<int32_t>>();
    statistics_a_b = std::make_shared<AttributeStatistics<int32_t>>();
    statistics_b_a = std::make_shared<AttributeStatistics<int32_t>>();
    statistics_b_b = std::make_shared<AttributeStatistics<int32_t>>();

    auto column_statistics = std::vector<std::shared_ptr<BaseAttributeStatistics>>{statistics_a_a, statistics_a_b,
                                                                                   statistics_b_a, statistics_b_b};

    table_statistics_a_b = std::make_shared<TableStatistics>(std::move(column_statistics), 5);

    validate_c = ValidateNode::make(node_c);

    cache = create_cache({node_a, node_b, validate_c}, {equals_(a_a, b_a), equals_(b_a, c_a), greater_than_(c_a, 5),
                                                        less_than_(c_a, a_a), equals_(a_a, b_b)});
  }

  static std::shared_ptr<JoinGraphStatisticsCache> create_cache(
      const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices,
      const std::vector<std::shared_ptr<AbstractExpression>>& predicates) {
    auto vertex_indices = JoinGraphStatisticsCache::VertexIndexMap{};
    for (auto vertex_idx = size_t{0}; vertex_idx < vertices.size(); ++vertex_idx) {
      vertex_indices.emplace(vertices[vertex_idx], vertex_idx);
    }

    auto predicate_indices = JoinGraphStatisticsCache::PredicateIndexMap{};
    for (auto predicate_idx = size_t{0}; predicate_idx < predicates.size(); ++predicate_idx) {
      predicate_indices.emplace(predicates[predicate_idx], predicate_idx);
    }

    return std::make_shared<JoinGraphStatisticsCache>(std::move(vertex_indices), std::move(predicate_indices));
  }

  std::shared_ptr<MockNode> node_a, node_b, node_c;
  std::shared_ptr<AbstractLQPNode> validate_c;
  LQPColumnReference a_a, a_b, b_a, b_b, c_a;
  std::shared_ptr<TableStatistics> table_statistics_a_b;
  std::shared_ptr<BaseAttributeStatistics> statistics_a_a, statistics_a_b, statistics_b_a, statistics_b_b;
  std::shared_ptr<JoinGraphStatisticsCache> cache;
};

TEST_F(JoinGraphStatisticsCacheTest, Bitmask) {
  // clang-format off
  const auto lqp_a =
  PredicateNode::make(greater_than_(c_a, 5),
    validate_c);
  // clang-format on

  const auto bitmask_a = cache->bitmask(lqp_a);
  ASSERT_TRUE(bitmask_a);
  EXPECT_EQ(*bitmask_a, JoinGraphStatisticsCache::Bitmask(8, 0b00100100));

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
  EXPECT_EQ(*bitmask_b_0, JoinGraphStatisticsCache::Bitmask(8, 0b00110101));

  const auto bitmask_b_1 = cache->bitmask(lqp_b_1);
  ASSERT_TRUE(bitmask_b_1);
  EXPECT_EQ(*bitmask_b_1, JoinGraphStatisticsCache::Bitmask(8, 0b00110101));

  // clang-format off
  const auto lqp_c_0 =
  PredicateNode::make(equals_(a_a, b_b),
    JoinNode::make(JoinMode::Inner, equals_(a_a, b_a),
      node_a,
      node_b));
  const auto lqp_c_1 =
  JoinNode::make(JoinMode::Inner, expression_vector(equals_(a_a, b_a), equals_(a_a, b_b)),
    node_a,
    node_b);
  // clang-format on

  const auto bitmask_c_0 = cache->bitmask(lqp_c_0);
  ASSERT_TRUE(bitmask_c_0);
  EXPECT_EQ(*bitmask_c_0, JoinGraphStatisticsCache::Bitmask(8, 0b10001011));

  const auto bitmask_c_1 = cache->bitmask(lqp_c_1);
  ASSERT_TRUE(bitmask_c_1);
  EXPECT_EQ(*bitmask_c_1, JoinGraphStatisticsCache::Bitmask(8, 0b10001011));
}

TEST_F(JoinGraphStatisticsCacheTest, BitmaskNotFound) {
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
  PredicateNode::make(equals_(c_a, 5),
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

TEST_F(JoinGraphStatisticsCacheTest, Caching) {
  EXPECT_EQ(cache->get(JoinGraphStatisticsCache::Bitmask{8, 0b00001011}, expression_vector(a_a, a_b, b_a, b_b, a_a)),
            nullptr);

  cache->set(JoinGraphStatisticsCache::Bitmask{8, 0b00001011}, expression_vector(a_a, a_b, b_a, b_b, a_a),
             table_statistics_a_b);

  const auto cached_a_b =
      cache->get(JoinGraphStatisticsCache::Bitmask{8, 0b00001011}, expression_vector(a_a, a_b, b_a, b_b, a_a));
  ASSERT_NE(cached_a_b, nullptr);
  EXPECT_EQ(cached_a_b->column_statistics.size(), 5u);
  EXPECT_EQ(cached_a_b->column_statistics[0], statistics_a_a);
  EXPECT_EQ(cached_a_b->column_statistics[1], statistics_a_b);
  EXPECT_EQ(cached_a_b->column_statistics[2], statistics_b_a);
  EXPECT_EQ(cached_a_b->column_statistics[3], statistics_b_b);
  EXPECT_EQ(cached_a_b->column_statistics[4], statistics_a_a);

  const auto cached_b_a =
      cache->get(JoinGraphStatisticsCache::Bitmask{8, 0b00001011}, expression_vector(b_a, b_b, a_a, a_b, a_a));
  ASSERT_NE(cached_b_a, nullptr);
  EXPECT_EQ(cached_b_a->column_statistics.size(), 5u);
  EXPECT_EQ(cached_b_a->column_statistics[0], statistics_b_a);
  EXPECT_EQ(cached_b_a->column_statistics[1], statistics_b_b);
  EXPECT_EQ(cached_b_a->column_statistics[2], statistics_a_a);
  EXPECT_EQ(cached_b_a->column_statistics[3], statistics_a_b);
  EXPECT_EQ(cached_b_a->column_statistics[4], statistics_a_a);
}

}  // namespace opossum
