#pragma once

#include <memory>
#include <set>

#include "optimizer/join_graph.hpp"
#include "types.hpp"

namespace opossum {

class AbstractASTNode;

class JoinGraph;

class GreedyJoinOrdering {
 public:
  explicit GreedyJoinOrdering(const std::shared_ptr<const JoinGraph>& input_graph);

  std::shared_ptr<AbstractASTNode> run();

 private:
  std::shared_ptr<const JoinGraph> _input_graph;

  // Lookup for column id of the leftmost column of a vertex in the join plan. INVALID_COLUMN_ID if the vertex hasn't
  // been added to the join plan yet.
  std::vector<ColumnID> _left_column_id_of_vertex;

  std::vector<std::vector<size_t>> _edges_by_vertex_id;

  std::set<size_t> _remaining_edge_indices;

  JoinVertexID pick_cheapest_vertex(const JoinGraph::Vertices& vertices) const;

  std::vector<size_t> update_neighbourhood(std::set<size_t>& neighbourhood_edges, size_t join_edge_idx);

  float cost_join(const std::shared_ptr<AbstractASTNode>& left_node, size_t edge_idx) const;

  std::pair<ColumnID, ColumnID> get_edge_column_ids(size_t edge_idx, JoinVertexID right_vertex_id) const;

  std::set<size_t> extract_vertex_neighbourhood(JoinVertexID vertex_idx);

  std::pair<JoinVertexID, JoinVertexID> order_edge_vertices(const JoinEdge& edge) const;
};
}