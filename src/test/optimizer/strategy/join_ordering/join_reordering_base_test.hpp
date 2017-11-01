#pragma once

#include "optimizer/abstract_syntax_tree/mock_node.hpp"
#include "optimizer/column_statistics.hpp"
#include "optimizer/join_graph.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "optimizer/table_statistics.hpp"
#include "utils/type_utils.hpp"

namespace opossum {

/**
 * Mock a table with one column containing all integer values in a specified [min, max] range
 */
class JoinOrderingTableStatistics : public TableStatistics {
 public:
  JoinOrderingTableStatistics(int32_t min, int32_t max, float row_count) :
    TableStatistics(row_count,
      std::vector<std::shared_ptr<BaseColumnStatistics>>{
        std::make_shared<ColumnStatistics<int32_t>>(ColumnID{0}, row_count, min, max, 1.0f)}) {
    Assert(min <= max, "min value should be smaller than max value");
  }
};

class JoinReorderingBaseTest : public StrategyBaseTest {
 public:
  JoinReorderingBaseTest() {
    const auto make_mock_table = [](const std::string& name, int32_t min, int32_t max, float row_count) {
      return std::make_shared<MockNode>(std::make_shared<JoinOrderingTableStatistics>(min, max, row_count), name);
    };

    const auto make_clique_join_graph = [](std::vector<std::shared_ptr<AbstractASTNode>> vertices) {
      JoinGraph::Edges edges;

      for (auto vertex_idx_a = JoinVertexID{0}; vertex_idx_a < vertices.size(); ++vertex_idx_a) {
        for (auto vertex_idx_b = make_join_vertex_id(vertex_idx_a + 1); vertex_idx_b < vertices.size(); ++vertex_idx_b) {
          JoinEdge edge({vertex_idx_a, vertex_idx_b}, {ColumnID{0}, ColumnID{0}}, JoinMode::Inner, ScanType::OpEquals);

          edges.emplace_back(edge);
        }
      }

      return std::make_shared<JoinGraph>(std::move(vertices), std::move(edges));
    };
    const auto make_chain_join_graph = [](std::vector<std::shared_ptr<AbstractASTNode>> vertices) {
      JoinGraph::Edges edges;

      for (auto vertex_idx_a = JoinVertexID{1}; vertex_idx_a < vertices.size(); ++vertex_idx_a) {
        const auto vertex_idx_b = make_join_vertex_id(vertex_idx_a - 1);

        JoinEdge edge({vertex_idx_a, vertex_idx_b}, {ColumnID{0}, ColumnID{0}}, JoinMode::Inner, ScanType::OpEquals);

        edges.emplace_back(edge);
      }

      return std::make_shared<JoinGraph>(std::move(vertices), std::move(edges));
    };

    _table_node_a = make_mock_table("a", 10, 80, 3);
    _table_node_b = make_mock_table("b", 10, 60, 60);
    _table_node_c = make_mock_table("c", 50, 100, 15);
    _table_node_d = make_mock_table("d", 53, 57, 10);
    _table_node_e = make_mock_table("e", 40, 90, 600);

    _join_graph_cde_chain = make_chain_join_graph({_table_node_c, _table_node_d, _table_node_e});
    _join_graph_bcd_clique = make_clique_join_graph({_table_node_b, _table_node_c, _table_node_d});

    /**
     * Build the ABCDE graph, which is a chain from A to E with B also being connected to E and D
     *
     * A___B___C___D___E
     *     |_______|   |
     *     |___________|
     *
     */
    JoinGraph::Vertices vertices_abcde = {_table_node_a, _table_node_b, _table_node_c, _table_node_d, _table_node_e};
    JoinGraph::Edges edges_abcde = {_create_equi_edge(JoinV0, 1),
                                    _create_equi_edge(1, 2),
                                    _create_equi_edge(2, 3),
                                    _create_equi_edge(3, 4),
                                    _create_equi_edge(1, 4),
                                    _create_equi_edge(1, 3)};
    _join_graph_abcde = std::make_shared<JoinGraph>(std::move(vertices_abcde), std::move(edges_abcde));
  }

 protected:
  std::shared_ptr<MockNode> _table_node_a;
  std::shared_ptr<MockNode> _table_node_b;
  std::shared_ptr<MockNode> _table_node_c;
  std::shared_ptr<MockNode> _table_node_d;
  std::shared_ptr<MockNode> _table_node_e;
  std::shared_ptr<JoinGraph> _join_graph_cde_chain;
  std::shared_ptr<JoinGraph> _join_graph_bcd_clique;
  std::shared_ptr<JoinGraph> _join_graph_abcde;

  JoinEdge _create_equi_edge(JoinVertexID vertex_id_a, JoinVertexID vertex_id_b) {
    return JoinEdge({vertex_id_a, vertex_id_b}, JoinMode::Inner, {ColumnID{0}, ColumnID{0}}, ScanType::OpEquals);
  }
};
}