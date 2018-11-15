#include "base_test.hpp"

#include "cost_model/cost_model_logical.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "optimizer/join_ordering/greedy_operator_ordering.hpp"
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
    const auto column_statistics = std::static_pointer_cast<const BaseColumnStatistics>(std::make_shared<ColumnStatistics<int32_t>>(0.0f, 100, 0, 100));

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

TEST_F(GreedyOperatorOrderingTest, Chain) {
  const auto edge_a = JoinGraphEdge{JoinGraphVertexSet{4, 0b0001}, expression_vector(greater_than_(a_a, 0))};
  const auto edge_ab = JoinGraphEdge{JoinGraphVertexSet{4, 0b0011}, expression_vector(equals_(a_a, b_a))};
  const auto edge_bc = JoinGraphEdge{JoinGraphVertexSet{4, 0b0110}, expression_vector(equals_(b_a, c_a))};
  const auto edge_cd = JoinGraphEdge{JoinGraphVertexSet{4, 0b1100}, expression_vector(equals_(c_a, d_a))};

  const auto join_graph = JoinGraph{std::vector<std::shared_ptr<AbstractLQPNode>>{node_a, node_b, node_c, node_d},
                              std::vector<JoinGraphEdge>{edge_a, edge_ab, edge_bc, edge_cd}
  };

  auto greedy_operator_ordering = GreedyOperatorOrdering{cost_estimator};
  const auto actual_lqp = greedy_operator_ordering(join_graph);

  actual_lqp->print();
//
//  // clang-format off
//  const auto expected_lqp =
//
//
//  // clang-format on



}

}  // namespace opossum