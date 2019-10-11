#include "base_test.hpp"

#include "cost_estimation/cost_estimator_logical.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "optimizer/join_ordering/greedy_operator_ordering.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "statistics/cardinality_estimator.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class GreedyOperatorOrderingTest : public BaseTest {
 public:
  void SetUp() override {
    cardinality_estimator = std::make_shared<CardinalityEstimator>();
    cost_estimator = std::make_shared<CostEstimatorLogical>(cardinality_estimator);

    // All columns have the same statistics, only Table row counts differ
    const auto single_bin_histogram_a = GenericHistogram<int32_t>::with_single_bin(0, 100, 5'000, 100);
    const auto single_bin_histogram_b = GenericHistogram<int32_t>::with_single_bin(0, 100, 1'000, 100);
    const auto single_bin_histogram_c = GenericHistogram<int32_t>::with_single_bin(0, 100, 200, 100);
    const auto single_bin_histogram_d = GenericHistogram<int32_t>::with_single_bin(0, 100, 500, 100);

    node_a =
        create_mock_node_with_statistics(MockNode::ColumnDefinitions{{DataType::Int, "a_a"}, {DataType::Int, "a_b"}},
                                         5'000, {single_bin_histogram_a, single_bin_histogram_a});
    node_b = create_mock_node_with_statistics(MockNode::ColumnDefinitions{{DataType::Int, "b_a"}}, 1'000,
                                              {single_bin_histogram_b});
    node_c = create_mock_node_with_statistics(MockNode::ColumnDefinitions{{DataType::Int, "c_a"}}, 200,
                                              {single_bin_histogram_c});
    node_d = create_mock_node_with_statistics(MockNode::ColumnDefinitions{{DataType::Int, "d_a"}}, 500,
                                              {single_bin_histogram_d});

    a_a = node_a->get_column("a_a");
    a_b = node_a->get_column("a_b");
    b_a = node_b->get_column("b_a");
    c_a = node_c->get_column("c_a");
    d_a = node_d->get_column("d_a");
  }

  std::shared_ptr<MockNode> node_a, node_b, node_c, node_d;
  LQPColumnReference a_a, a_b, b_a, c_a, d_a;
  std::shared_ptr<AbstractCostEstimator> cost_estimator;
  std::shared_ptr<AbstractCardinalityEstimator> cardinality_estimator;
};

TEST_F(GreedyOperatorOrderingTest, NoEdges) {
  // Test that the most simple conceivable JoinGraph (one vertex, no edges) can be processed by GOO

  const auto join_graph =
      JoinGraph{std::vector<std::shared_ptr<AbstractLQPNode>>{node_a}, std::vector<JoinGraphEdge>{}};

  const auto actual_lqp = GreedyOperatorOrdering{}(join_graph, cost_estimator);  // NOLINT
  const auto expected_lqp = node_a->deep_copy();

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(GreedyOperatorOrderingTest, ChainQuery) {
  // Chain query with a local predicate on one vertex

  const auto edge_a = JoinGraphEdge{JoinGraphVertexSet{4, 0b0001}, expression_vector(greater_than_(a_a, 0))};
  const auto edge_ab = JoinGraphEdge{JoinGraphVertexSet{4, 0b0011}, expression_vector(equals_(a_a, b_a))};
  const auto edge_bc = JoinGraphEdge{JoinGraphVertexSet{4, 0b0110}, expression_vector(equals_(b_a, c_a))};
  const auto edge_cd = JoinGraphEdge{JoinGraphVertexSet{4, 0b1100}, expression_vector(equals_(c_a, d_a))};

  const auto join_graph = JoinGraph{std::vector<std::shared_ptr<AbstractLQPNode>>{node_a, node_b, node_c, node_d},
                                    std::vector<JoinGraphEdge>{edge_a, edge_ab, edge_bc, edge_cd}};

  const auto actual_lqp = GreedyOperatorOrdering{}(join_graph, cost_estimator);  // NOLINT

  // clang-format off
  const auto expected_lqp =
  JoinNode::make(JoinMode::Inner, equals_(a_a, b_a),
    JoinNode::make(JoinMode::Inner, equals_(b_a, c_a),
      node_b,
      JoinNode::make(JoinMode::Inner, equals_(c_a, d_a),
        node_d,
        node_c)),
    PredicateNode::make(greater_than_(a_a, 0),
      node_a));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(GreedyOperatorOrderingTest, StarQuery) {
  const auto edge_ab = JoinGraphEdge{JoinGraphVertexSet{4, 0b0011}, expression_vector(equals_(a_a, b_a))};
  const auto edge_ac = JoinGraphEdge{JoinGraphVertexSet{4, 0b0101}, expression_vector(equals_(a_a, c_a))};
  const auto edge_ad = JoinGraphEdge{JoinGraphVertexSet{4, 0b1001}, expression_vector(equals_(a_a, d_a))};

  const auto join_graph = JoinGraph{std::vector<std::shared_ptr<AbstractLQPNode>>{node_a, node_b, node_c, node_d},
                                    std::vector<JoinGraphEdge>{edge_ab, edge_ac, edge_ad}};

  const auto actual_lqp = GreedyOperatorOrdering{}(join_graph, cost_estimator);  // NOLINT

  // clang-format off
  const auto expected_lqp =
  JoinNode::make(JoinMode::Inner, equals_(a_a, b_a),
    JoinNode::make(JoinMode::Inner, equals_(a_a, d_a),
      JoinNode::make(JoinMode::Inner, equals_(a_a, c_a),
        node_a,
        node_c),
      node_d),
    node_b);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(GreedyOperatorOrderingTest, HyperEdges) {
  // Query with two binary edges and three hyperedges

  const auto edge_bc = JoinGraphEdge{JoinGraphVertexSet{4, 0b0110}, expression_vector(equals_(b_a, c_a))};
  const auto edge_cd =
      JoinGraphEdge{JoinGraphVertexSet{4, 0b1100}, expression_vector(equals_(c_a, d_a), less_than_equals_(c_a, d_a))};
  const auto edge_acd = JoinGraphEdge{JoinGraphVertexSet{4, 0b1101}, expression_vector(equals_(c_a, add_(d_a, a_a)))};
  const auto edge_bcd = JoinGraphEdge{JoinGraphVertexSet{4, 0b1110}, expression_vector(equals_(add_(b_a, c_a), d_a))};
  const auto edge_abcd =
      JoinGraphEdge{JoinGraphVertexSet{4, 0b1111}, expression_vector(greater_than_(add_(b_a, c_a), sub_(a_a, d_a)))};

  const auto join_graph = JoinGraph{std::vector<std::shared_ptr<AbstractLQPNode>>{node_a, node_b, node_c, node_d},
                                    std::vector<JoinGraphEdge>{edge_bc, edge_cd, edge_acd, edge_bcd, edge_abcd}};

  const auto actual_lqp = GreedyOperatorOrdering{}(join_graph, cost_estimator);  // NOLINT

  // clang-format off
  const auto expected_lqp =
  PredicateNode::make(greater_than_(add_(b_a, c_a), sub_(a_a, d_a)),
    PredicateNode::make(equals_(c_a, add_(d_a, a_a)),
      JoinNode::make(JoinMode::Cross,
        PredicateNode::make(equals_(add_(b_a, c_a), d_a),
          JoinNode::make(JoinMode::Inner, equals_(b_a, c_a),
            node_b,
              PredicateNode::make(less_than_equals_(c_a, d_a),
                JoinNode::make(JoinMode::Inner, expression_vector(equals_(c_a, d_a)),
                  node_d,
                  node_c)))),
        node_a)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(GreedyOperatorOrderingTest, UncorrelatedPredicate) {
  // Test that predicates that do not reference a single vertex can be processed by GOO

  const auto edge_uncorrelated = JoinGraphEdge{JoinGraphVertexSet{2, 0b0000}, expression_vector(equals_(6, 6))};
  const auto edge_ab = JoinGraphEdge{JoinGraphVertexSet{2, 0b0011}, expression_vector(equals_(a_a, b_a))};

  const auto join_graph = JoinGraph{std::vector<std::shared_ptr<AbstractLQPNode>>{node_a, node_b},
                                    std::vector<JoinGraphEdge>{edge_uncorrelated, edge_ab}};

  const auto actual_lqp = GreedyOperatorOrdering{}(join_graph, cost_estimator);  // NOLINT

  // clang-format off
  const auto expected_lqp =
  PredicateNode::make(equals_(6, 6),
    JoinNode::make(JoinMode::Inner, equals_(a_a, b_a),
      node_a,
      node_b));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
