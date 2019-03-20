#include "base_test.hpp"

#include "cost_model/cost_model_logical.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "optimizer/join_ordering/greedy_operator_ordering.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class GreedyOperatorOrderingTest : public BaseTest {
 public:
  void SetUp() override {
    cost_estimator = std::make_shared<CostModelLogical>();

    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a_a"}, {DataType::Int, "a_b"}}, "a");
    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "b_a"}}, "b");
    node_c = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "c_a"}}, "c");
    node_d = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "d_a"}}, "d");

    // All columns have the same statistics, only Table row counts differ
    const auto column_statistics = std::static_pointer_cast<const BaseColumnStatistics>(
        std::make_shared<ColumnStatistics<int32_t>>(0.0f, 100, 0, 100));

    const auto table_statistics_a = std::make_shared<TableStatistics>(
        TableType::Data, 5'000,
        std::vector<std::shared_ptr<const BaseColumnStatistics>>{column_statistics, column_statistics});

    const auto table_statistics_b = std::make_shared<TableStatistics>(
        TableType::Data, 1'000, std::vector<std::shared_ptr<const BaseColumnStatistics>>{column_statistics});

    const auto table_statistics_c = std::make_shared<TableStatistics>(
        TableType::Data, 200, std::vector<std::shared_ptr<const BaseColumnStatistics>>{column_statistics});

    const auto table_statistics_d = std::make_shared<TableStatistics>(
        TableType::Data, 500, std::vector<std::shared_ptr<const BaseColumnStatistics>>{column_statistics});

    node_a->set_statistics(table_statistics_a);
    node_b->set_statistics(table_statistics_b);
    node_c->set_statistics(table_statistics_c);
    node_d->set_statistics(table_statistics_d);

    a_a = node_a->get_column("a_a");
    a_b = node_a->get_column("a_b");
    b_a = node_b->get_column("b_a");
    c_a = node_c->get_column("c_a");
    d_a = node_d->get_column("d_a");
  }

  std::shared_ptr<MockNode> node_a, node_b, node_c, node_d;
  LQPColumnReference a_a, a_b, b_a, c_a, d_a;
  std::shared_ptr<AbstractCostEstimator> cost_estimator;
};

TEST_F(GreedyOperatorOrderingTest, NoEdges) {
  // Test that the most simple conceivable JoinGraph (one vertex, no edges) can be processed by GOO

  const auto join_graph =
      JoinGraph{std::vector<std::shared_ptr<AbstractLQPNode>>{node_a}, std::vector<JoinGraphEdge>{}};

  const auto actual_lqp = GreedyOperatorOrdering{cost_estimator}(join_graph);  // NOLINT
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

  const auto actual_lqp = GreedyOperatorOrdering{cost_estimator}(join_graph);  // NOLINT

  // clang-format off
  const auto expected_lqp =
  JoinNode::make(JoinMode::Inner, equals_(a_a, b_a),
    PredicateNode::make(greater_than_(a_a, 0),
      node_a),
    JoinNode::make(JoinMode::Inner, equals_(b_a, c_a),
      node_b,
      JoinNode::make(JoinMode::Inner, equals_(c_a, d_a),
        node_c,
        node_d)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(GreedyOperatorOrderingTest, StarQuery) {
  const auto edge_ab = JoinGraphEdge{JoinGraphVertexSet{4, 0b0011}, expression_vector(equals_(a_a, b_a))};
  const auto edge_ac = JoinGraphEdge{JoinGraphVertexSet{4, 0b0101}, expression_vector(equals_(a_a, c_a))};
  const auto edge_ad = JoinGraphEdge{JoinGraphVertexSet{4, 0b1001}, expression_vector(equals_(a_a, d_a))};

  const auto join_graph = JoinGraph{std::vector<std::shared_ptr<AbstractLQPNode>>{node_a, node_b, node_c, node_d},
                                    std::vector<JoinGraphEdge>{edge_ab, edge_ac, edge_ad}};

  const auto actual_lqp = GreedyOperatorOrdering{cost_estimator}(join_graph);  // NOLINT

  // clang-format off
  const auto expected_lqp =
  JoinNode::make(JoinMode::Inner, equals_(a_a, b_a),
    node_b,
    JoinNode::make(JoinMode::Inner, equals_(a_a, d_a),
      JoinNode::make(JoinMode::Inner, equals_(a_a, c_a),
        node_a,
        node_c),
      node_d));
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

  const auto actual_lqp = GreedyOperatorOrdering{cost_estimator}(join_graph);  // NOLINT

  // clang-format off
  const auto expected_lqp =
  PredicateNode::make(greater_than_(add_(b_a, c_a), sub_(a_a, d_a)),
    PredicateNode::make(equals_(c_a, add_(d_a, a_a)),
      JoinNode::make(JoinMode::Cross,
        node_a,
        PredicateNode::make(equals_(add_(b_a, c_a), d_a),
          JoinNode::make(JoinMode::Inner, equals_(b_a, c_a),
            node_b,
            PredicateNode::make(less_than_equals_(c_a, d_a),
              JoinNode::make(JoinMode::Inner, equals_(c_a, d_a),
                node_c,
                node_d)))))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(GreedyOperatorOrderingTest, UncorrelatedPredicate) {
  // Test that predicates that do not reference a single vertex can be processed by GOO

  const auto edge_uncorrelated = JoinGraphEdge{JoinGraphVertexSet{2, 0b0000}, expression_vector(equals_(6, 6))};
  const auto edge_ab = JoinGraphEdge{JoinGraphVertexSet{2, 0b0011}, expression_vector(equals_(a_a, b_a))};

  const auto join_graph = JoinGraph{std::vector<std::shared_ptr<AbstractLQPNode>>{node_a, node_b},
                                    std::vector<JoinGraphEdge>{edge_uncorrelated, edge_ab}};

  const auto actual_lqp = GreedyOperatorOrdering{cost_estimator}(join_graph);  // NOLINT

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
