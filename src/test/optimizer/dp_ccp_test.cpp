#include "gtest/gtest.h"

#include "cost_model/cost_model_logical.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/dp_ccp.hpp"
#include "optimizer/join_graph.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "testing_assert.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class DpCcpTest : public ::testing::Test {
 public:
  void SetUp() override {
    cost_model = std::make_shared<CostModelLogical>();

    const auto column_statistics_a_a = std::make_shared<ColumnStatistics<int32_t>>(0.0f, 10.0f, 1, 50);
    const auto column_statistics_b_a = std::make_shared<ColumnStatistics<int32_t>>(0.0f, 10.0f, 40, 100);
    const auto column_statistics_c_a = std::make_shared<ColumnStatistics<int32_t>>(0.0f, 10.0f, 1, 100);

    const auto table_statistics_a = std::make_shared<TableStatistics>(
        TableType::Data, 20, std::vector<std::shared_ptr<const BaseColumnStatistics>>{column_statistics_a_a});
    const auto table_statistics_b = std::make_shared<TableStatistics>(
        TableType::Data, 3, std::vector<std::shared_ptr<const BaseColumnStatistics>>{column_statistics_b_a});
    const auto table_statistics_c = std::make_shared<TableStatistics>(
        TableType::Data, 5, std::vector<std::shared_ptr<const BaseColumnStatistics>>{column_statistics_c_a});

    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "a");
    node_a->set_statistics(table_statistics_a);
    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "b");
    node_b->set_statistics(table_statistics_b);
    node_c = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "c");
    node_c->set_statistics(table_statistics_c);

    a_a = node_a->get_column("a");
    b_a = node_b->get_column("a");
    c_a = node_c->get_column("a");
  }

  std::shared_ptr<MockNode> node_a, node_b, node_c;
  std::shared_ptr<AbstractCostModel> cost_model;
  LQPColumnReference a_a, b_a, c_a;
};

TEST_F(DpCcpTest, Basic) {
  /**
   * Test two vertices and a single join predicate
   */

  const auto join_edge_a_b = JoinGraphEdge{JoinGraphVertexSet{3, 0b011}, expression_vector(equals_(a_a, b_a))};
  const auto join_edge_a_c = JoinGraphEdge{JoinGraphVertexSet{3, 0b101}, expression_vector(equals_(a_a, c_a))};

  const auto join_graph = JoinGraph(std::vector<std::shared_ptr<AbstractLQPNode>>({node_a, node_b, node_c}),
                                    std::vector<JoinGraphEdge>({join_edge_a_b, join_edge_a_c}));
  DpCcp dp_ccp{cost_model};

  const auto actual_lqp = dp_ccp(join_graph);

  // clang-format off
  const auto expected_lqp =
  JoinNode::make(JoinMode::Inner, equals_(a_a, c_a),
    JoinNode::make(JoinMode::Inner, equals_(a_a, b_a),
      node_a,
      node_b),
    node_c);
  // clang-format on

  EXPECT_LQP_EQ(expected_lqp, actual_lqp);
}

TEST_F(DpCcpTest, ComplexJoinPredicate) {
  // Test that complex predicates will not be considered for the join operation (since our join operators can't execute
  // them)

  const auto complex_predicate = equals_(add_(a_a, 2), b_a);
  auto join_edge_a_b = JoinGraphEdge{JoinGraphVertexSet{2, 0b11}, expression_vector(complex_predicate)};

  const auto join_graph = JoinGraph(std::vector<std::shared_ptr<AbstractLQPNode>>({node_a, node_b}),
                                    std::vector<JoinGraphEdge>({join_edge_a_b}));
  DpCcp dp_ccp{cost_model};

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

}  // namespace opossum
