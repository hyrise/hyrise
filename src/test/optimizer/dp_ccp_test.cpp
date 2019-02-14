#include "gtest/gtest.h"

#include "cost_model/cost_model_logical.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/join_ordering/dp_ccp.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "testing_assert.hpp"
#include "utils/load_table.hpp"

/**
 * The number of tests in here might seem few, but actually all main parts of DpCcp are covered: Join order, Join
 * predicate order, local predicate order, complex predicate treatment and cross join treatment.
 *
 * Note, also, that EnumerateCcp is tested separately.
 */

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class DpCcpTest : public ::testing::Test {
 public:
  void SetUp() override {
    cost_estimator = std::make_shared<CostModelLogical>();

    const auto column_statistics_a_a = std::make_shared<ColumnStatistics<int32_t>>(0.0f, 10.0f, 1, 50);
    const auto column_statistics_b_a = std::make_shared<ColumnStatistics<int32_t>>(0.0f, 10.0f, 40, 100);
    const auto column_statistics_c_a = std::make_shared<ColumnStatistics<int32_t>>(0.0f, 10.0f, 1, 100);
    const auto column_statistics_d_a = std::make_shared<ColumnStatistics<int32_t>>(0.0f, 10.0f, 1, 100);

    const auto table_statistics_a = std::make_shared<TableStatistics>(
        TableType::Data, 20, std::vector<std::shared_ptr<const BaseColumnStatistics>>{column_statistics_a_a});
    const auto table_statistics_b = std::make_shared<TableStatistics>(
        TableType::Data, 20, std::vector<std::shared_ptr<const BaseColumnStatistics>>{column_statistics_b_a});
    const auto table_statistics_c = std::make_shared<TableStatistics>(
        TableType::Data, 20, std::vector<std::shared_ptr<const BaseColumnStatistics>>{column_statistics_c_a});
    const auto table_statistics_d = std::make_shared<TableStatistics>(
        TableType::Data, 200, std::vector<std::shared_ptr<const BaseColumnStatistics>>{column_statistics_d_a});

    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "a");
    node_a->set_statistics(table_statistics_a);
    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "b");
    node_b->set_statistics(table_statistics_b);
    node_c = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "c");
    node_c->set_statistics(table_statistics_c);
    node_d = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "d");
    node_d->set_statistics(table_statistics_d);

    a_a = node_a->get_column("a");
    b_a = node_b->get_column("a");
    c_a = node_c->get_column("a");
    d_a = node_d->get_column("a");
  }

  std::shared_ptr<MockNode> node_a, node_b, node_c, node_d;
  std::shared_ptr<AbstractCostEstimator> cost_estimator;
  LQPColumnReference a_a, b_a, c_a, d_a;
};

TEST_F(DpCcpTest, JoinOrdering) {
  /**
   * Test that three vertices with three join predicates are turned into two join operations and a scan operation that
   * are efficiently ordered.
   *
   * In this case, joining A and B first is the best option, since have the lowest overlapping range.
   * For joining C in afterwards, `c_a = a_a` is the best choice for the primary join predicate, since `c_a = b_a`
   * yields more rows and is therefore more expensive.
   */

  const auto join_edge_a_b = JoinGraphEdge{JoinGraphVertexSet{3, 0b011}, expression_vector(equals_(a_a, b_a))};
  const auto join_edge_a_c = JoinGraphEdge{JoinGraphVertexSet{3, 0b101}, expression_vector(equals_(a_a, c_a))};
  const auto join_edge_b_c = JoinGraphEdge{JoinGraphVertexSet{3, 0b110}, expression_vector(equals_(b_a, c_a))};

  const auto join_graph = JoinGraph(std::vector<std::shared_ptr<AbstractLQPNode>>({node_a, node_b, node_c}),
                                    std::vector<JoinGraphEdge>({join_edge_a_b, join_edge_a_c, join_edge_b_c}));
  DpCcp dp_ccp{cost_estimator};

  const auto actual_lqp = dp_ccp(join_graph);

  // clang-format off
  const auto expected_lqp =
  PredicateNode::make(equals_(b_a, c_a),
    JoinNode::make(JoinMode::Inner, equals_(a_a, c_a),
      JoinNode::make(JoinMode::Inner, equals_(a_a, b_a),
        node_a,
        node_b),
      node_c));
  // clang-format on

  EXPECT_LQP_EQ(expected_lqp, actual_lqp);
}

TEST_F(DpCcpTest, CrossJoin) {
  /**
   * Test that if there is a non-predicated edge, a cross join is created
   */

  const auto join_edge_a_b = JoinGraphEdge{JoinGraphVertexSet{3, 0b011}, expression_vector(equals_(a_a, b_a))};
  const auto cross_join_edge_a_c = JoinGraphEdge{JoinGraphVertexSet{3, 0b101}, {}};

  const auto join_graph = JoinGraph(std::vector<std::shared_ptr<AbstractLQPNode>>({node_a, node_b, node_c}),
                                    std::vector<JoinGraphEdge>({join_edge_a_b, cross_join_edge_a_c}));
  DpCcp dp_ccp{cost_estimator};

  const auto actual_lqp = dp_ccp(join_graph);

  // clang-format off
  const auto expected_lqp =
  JoinNode::make(JoinMode::Cross,
    JoinNode::make(JoinMode::Inner, equals_(a_a, b_a),
      node_a,
      node_b),
    node_c);
  // clang-format on

  EXPECT_LQP_EQ(expected_lqp, actual_lqp);
}

TEST_F(DpCcpTest, LocalPredicateOrdering) {
  /**
   * Test that local predicates are brought into an optimal order
   */

  const auto join_edge_a_b = JoinGraphEdge{JoinGraphVertexSet{2, 0b11}, expression_vector(equals_(a_a, b_a))};

  const auto local_predicate_a_0 = equals_(a_a, add_(5, 6));           // medium complexity
  const auto local_predicate_a_1 = equals_(a_a, 5);                    // low complexity
  const auto local_predicate_a_2 = equals_(mul_(a_a, 2), add_(5, 6));  // high complexity

  const auto local_predicate_b_0 = equals_(b_a, add_(5, 6));  // medium complexity
  const auto local_predicate_b_1 = equals_(b_a, 5);           // low complexity

  const auto self_edge_a = JoinGraphEdge{
      JoinGraphVertexSet{2, 0b01}, expression_vector(local_predicate_a_0, local_predicate_a_1, local_predicate_a_2)};
  const auto self_edge_b =
      JoinGraphEdge{JoinGraphVertexSet{2, 0b10}, expression_vector(local_predicate_b_0, local_predicate_b_1)};

  const auto join_graph = JoinGraph(std::vector<std::shared_ptr<AbstractLQPNode>>({node_a, node_b}),
                                    std::vector<JoinGraphEdge>({join_edge_a_b, self_edge_a, self_edge_b}));
  DpCcp dp_ccp{cost_estimator};

  const auto actual_lqp = dp_ccp(join_graph);

  // clang-format off
  const auto expected_lqp =
  JoinNode::make(JoinMode::Inner, equals_(a_a, b_a),
    PredicateNode::make(local_predicate_a_2,
      PredicateNode::make(local_predicate_a_0,
        PredicateNode::make(local_predicate_a_1,
          node_a))),
    PredicateNode::make(local_predicate_b_0,
      PredicateNode::make(local_predicate_b_1,
        node_b)));
  // clang-format on

  EXPECT_LQP_EQ(expected_lqp, actual_lqp);
}

TEST_F(DpCcpTest, ComplexJoinPredicate) {
  /**
   * Test that complex predicates will not be considered for the join operation (since our join operators can't execute
   * them)
   */

  const auto complex_predicate = equals_(add_(a_a, 2), b_a);
  auto join_edge_a_b = JoinGraphEdge{JoinGraphVertexSet{2, 0b11}, expression_vector(complex_predicate)};

  const auto join_graph = JoinGraph(std::vector<std::shared_ptr<AbstractLQPNode>>({node_a, node_b}),
                                    std::vector<JoinGraphEdge>({join_edge_a_b}));
  DpCcp dp_ccp{cost_estimator};

  const auto actual_lqp = dp_ccp(join_graph);

  // clang-format off
  const auto expected_lqp =
  PredicateNode::make(complex_predicate,
    JoinNode::make(JoinMode::Cross,
      node_a,
      node_b));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(DpCcpTest, HyperEdge) {
  /**
   * Test that the predicates of hyper edges (i.e., those involving more than 2 base relations) are placed as soon as
   * all its relations are available in the LQP below
   */

  // Hyper edge predicate "a + c = b"
  const auto hyper_edge_predicate = equals_(add_(a_a, c_a), b_a);
  auto join_edge_a_b_c = JoinGraphEdge{JoinGraphVertexSet{4, 0b0111}, expression_vector(hyper_edge_predicate)};

  auto join_edge_a_b = JoinGraphEdge{JoinGraphVertexSet{4, 0b0011}, expression_vector(equals_(b_a, a_a))};
  auto join_edge_b_c = JoinGraphEdge{JoinGraphVertexSet{4, 0b0110}, expression_vector(equals_(c_a, a_a))};
  auto cross_edge_b_d = JoinGraphEdge{JoinGraphVertexSet{4, 0b1010}, expression_vector()};

  const auto join_graph =
      JoinGraph(std::vector<std::shared_ptr<AbstractLQPNode>>({node_a, node_b, node_c, node_d}),
                std::vector<JoinGraphEdge>({join_edge_a_b_c, join_edge_a_b, join_edge_b_c, cross_edge_b_d}));
  DpCcp dp_ccp{cost_estimator};

  const auto actual_lqp = dp_ccp(join_graph);

  // clang-format off
  const auto expected_lqp =
  JoinNode::make(JoinMode::Cross,
    PredicateNode::make(hyper_edge_predicate,
      JoinNode::make(JoinMode::Inner, equals_(c_a, a_a),
        JoinNode::make(JoinMode::Inner, equals_(b_a, a_a),
          node_a,
          node_b),
        node_c)),
    node_d);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(DpCcpTest, UncorrelatedPredicates) {
  /**
   * Test that predicates that do not reference any of the vertices in the join graph are placed in the resulting LQP
   * Such predicates might be things like "5 > 3", "5 IN (SELECT ...)" etc.
   */

  const auto subquery_lqp = PredicateNode::make(less_than_(b_a, 5), node_b);
  const auto subquery = lqp_subquery_(subquery_lqp);

  const auto uncorrelated_predicate_a = greater_than_(5, 3);
  const auto uncorrelated_predicate_b = in_(4, subquery);

  auto join_edge_a_d = JoinGraphEdge{JoinGraphVertexSet{2, 0b11}, expression_vector(equals_(d_a, a_a))};
  auto join_edge_uncorrelated =
      JoinGraphEdge{JoinGraphVertexSet{2, 0b00}, expression_vector(uncorrelated_predicate_a, uncorrelated_predicate_b)};

  const auto join_graph = JoinGraph(std::vector<std::shared_ptr<AbstractLQPNode>>({node_a, node_d}),
                                    std::vector<JoinGraphEdge>({join_edge_a_d, join_edge_uncorrelated}));
  DpCcp dp_ccp{cost_estimator};

  const auto actual_lqp = dp_ccp(join_graph);

  // clang-format off
  const auto expected_lqp =
  JoinNode::make(JoinMode::Inner, equals_(d_a, a_a),
    node_a,
    PredicateNode::make(uncorrelated_predicate_b,
      PredicateNode::make(uncorrelated_predicate_a,
        node_d)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
