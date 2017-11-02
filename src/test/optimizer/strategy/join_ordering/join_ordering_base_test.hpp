#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "optimizer/abstract_syntax_tree/mock_node.hpp"
#include "optimizer/column_statistics.hpp"
#include "optimizer/join_graph.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "optimizer/table_statistics.hpp"
#include "utils/type_utils.hpp"

namespace opossum {

/**
 * Base class for tests for JoinOrdering algorithms. Provides some JoinGraphs they can work on.
 */
class JoinOrderingBaseTest : public ::testing::Test {
 public:
  JoinOrderingBaseTest() {
    _table_node_a = _make_mock_node("a", 10, 80, 3);
    _table_node_b = _make_mock_node("b", 10, 60, 60);
    _table_node_c = _make_mock_node("c", 50, 100, 15);
    _table_node_d = _make_mock_node("d", 53, 57, 10);
    _table_node_e = _make_mock_node("e", 40, 90, 600);

    /**
     * Build some simple test graphs
     */
    _join_graph_cde_chain = _make_chain_join_graph({_table_node_c, _table_node_d, _table_node_e});
    _join_graph_bcd_clique = _make_clique_join_graph({_table_node_b, _table_node_c, _table_node_d});

    /**
     * Build the ABCDE graph, which is a chain from A to E with B also being connected to E and D
     *
     * A___B___C___D___E
     *     |_______|   |
     *     |___________|
     *
     */
    JoinGraph::Vertices vertices_abcde = {_table_node_a, _table_node_b, _table_node_c, _table_node_d, _table_node_e};
    JoinGraph::Edges edges_abcde = {
        _create_equi_edge(JoinVertexID{0}, JoinVertexID{1}), _create_equi_edge(JoinVertexID{1}, JoinVertexID{2}),
        _create_equi_edge(JoinVertexID{2}, JoinVertexID{3}), _create_equi_edge(JoinVertexID{3}, JoinVertexID{4}),
        _create_equi_edge(JoinVertexID{1}, JoinVertexID{4}), _create_equi_edge(JoinVertexID{1}, JoinVertexID{3})};
    _join_graph_abcde = std::make_shared<JoinGraph>(std::move(vertices_abcde), std::move(edges_abcde));

    /**
     * Build the ABCDE-Cross graph which is a chain AEDC with B being connected to A,C and E via Cross Joins
     */
    JoinGraph::Vertices vertices_abcde_cross = {_table_node_a, _table_node_b, _table_node_c, _table_node_d,
                                                _table_node_e};
    JoinGraph::Edges edges_abcde_cross = {
        _create_cross_edge(JoinVertexID{0}, JoinVertexID{1}), _create_cross_edge(JoinVertexID{1}, JoinVertexID{2}),
        _create_cross_edge(JoinVertexID{1}, JoinVertexID{4}), _create_equi_edge(JoinVertexID{2}, JoinVertexID{3}),
        _create_equi_edge(JoinVertexID{3}, JoinVertexID{4}),  _create_equi_edge(JoinVertexID{0}, JoinVertexID{4})};
    _join_graph_abcde_cross =
        std::make_shared<JoinGraph>(std::move(vertices_abcde_cross), std::move(edges_abcde_cross));
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
  std::shared_ptr<JoinGraph> _join_graph_abcde_cross;

  static JoinEdge _create_equi_edge(JoinVertexID vertex_id_a, JoinVertexID vertex_id_b) {
    return JoinEdge({vertex_id_a, vertex_id_b}, {ColumnID{0}, ColumnID{0}}, JoinMode::Inner, ScanType::OpEquals);
  }

  static JoinEdge _create_cross_edge(JoinVertexID vertex_id_a, JoinVertexID vertex_id_b) {
    return JoinEdge({vertex_id_a, vertex_id_b}, JoinMode::Cross);
  }

  static std::shared_ptr<TableStatistics> _make_mock_table_statistics(int32_t min, int32_t max, float row_count) {
    Assert(min <= max, "min value should be smaller than max value");

    const auto column_statistics = std::vector<std::shared_ptr<BaseColumnStatistics>>(
        {std::make_shared<ColumnStatistics<int32_t>>(ColumnID{0}, row_count, min, max, 1.0f)});

    return std::make_shared<TableStatistics>(row_count, column_statistics);
  }

  static std::shared_ptr<MockNode> _make_mock_node(const std::string& name, int32_t min, int32_t max, float row_count) {
    return std::make_shared<MockNode>(_make_mock_table_statistics(min, max, row_count), name);
  }

  std::shared_ptr<JoinGraph> _make_clique_join_graph(std::vector<std::shared_ptr<AbstractASTNode>> vertices) {
    JoinGraph::Edges edges;

    for (auto vertex_idx_a = JoinVertexID{0}; vertex_idx_a < vertices.size(); ++vertex_idx_a) {
      for (auto vertex_idx_b = make_join_vertex_id(vertex_idx_a + 1); vertex_idx_b < vertices.size(); ++vertex_idx_b) {
        JoinEdge edge({vertex_idx_a, vertex_idx_b}, {ColumnID{0}, ColumnID{0}}, JoinMode::Inner, ScanType::OpEquals);

        edges.emplace_back(edge);
      }
    }

    return std::make_shared<JoinGraph>(std::move(vertices), std::move(edges));
  }

  std::shared_ptr<JoinGraph> _make_chain_join_graph(std::vector<std::shared_ptr<AbstractASTNode>> vertices) {
    JoinGraph::Edges edges;

    for (auto vertex_idx_a = JoinVertexID{1}; vertex_idx_a < vertices.size(); ++vertex_idx_a) {
      const auto vertex_idx_b = make_join_vertex_id(vertex_idx_a - 1);

      JoinEdge edge({vertex_idx_a, vertex_idx_b}, {ColumnID{0}, ColumnID{0}}, JoinMode::Inner, ScanType::OpEquals);

      edges.emplace_back(edge);
    }

    return std::make_shared<JoinGraph>(std::move(vertices), std::move(edges));
  }
};
}  // namespace opossum
