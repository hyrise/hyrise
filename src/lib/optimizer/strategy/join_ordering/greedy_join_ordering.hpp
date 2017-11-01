#pragma once

#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "optimizer/join_graph.hpp"
#include "types.hpp"

namespace opossum {

class AbstractASTNode;

class JoinGraph;

/**
 * A simple JoinOrdering algorithm that generates a left-deep JoinPlan.
 * The algorithm starts with the smallest Vertex-Table and then successively joins the Vertex, that generates the
 * smallest Table as a result of the Join.
 */
class GreedyJoinOrdering {
 public:
  explicit GreedyJoinOrdering(const std::shared_ptr<const JoinGraph>& input_graph);

  /**
   * Runs the algorithm, only call this once
   */
  std::shared_ptr<AbstractASTNode> run();

 private:
  std::shared_ptr<const JoinGraph> _input_graph;

  /**
   * At index `i`, contains the ColumnID that is the leftmost column of vertex with id `i` in the output of the current
   * join plan. INVALID_COLUMN_ID if the vertex is not yet added to the JoinPlan.
   */
  std::vector<ColumnID> _left_column_id_of_vertex;

  /**
   * At index `i` contains a vector of all indices of edges connected to the vertex at with id `ì`
   */
  std::vector<std::vector<size_t>> _edges_by_vertex_id;

  /**
   * Indices of the edges that were not yet added to the JoinPlan
   */
  std::set<size_t> _remaining_edge_indices;

  /**
   * - Remove the JoinEdge `join_edge_idx` from the `neighbourhood_edges` and the `_remaining_edge_indices`
   * - Identify the new vertex and add all its edges to the `neighbourhood_edges`
   * @returns neighbourhood edges that have to be turned into Predicates
   */
  std::vector<size_t> _update_neighbourhood(std::set<size_t>& neighbourhood_edges, size_t join_edge_idx);

  /**
   * Identify the cost of joining Edge at `edge_idx` to JoinPlan left_node. Currently the cost == the number of rows
   * in the resulting table which is an okay-ish model.
   */
  float _cost_join(const std::shared_ptr<AbstractASTNode>& left_node, size_t edge_idx) const;

  /**
   * @returns the ColumnIDs required for joining the vertex `right_vertex_id` to the JoinPlan using the edge `edge_idx`
   * The .first member of the returned pair is the ColumnID in the output of the current JoinPlan,
   * the .second member the ColumnID in the newly joined vertex.
   * NOTE: When creating the JoinNode, this might require the scan type to be flipped. TODO(moritz)
   */
  std::pair<ColumnID, ColumnID> _get_edge_column_ids(size_t edge_idx, JoinVertexID right_vertex_id) const;

  /**
   * @returns the edges adjacent to `vertex_idx` that connect to vertices outside of the current JoinPlan
   */
  std::set<size_t> _extract_vertex_neighbourhood(JoinVertexID vertex_idx);

  /**
   * For `edge`
   * @returns (Index of new vertex, Index of vertex already in JoinPlan)
   */
  std::pair<JoinVertexID, JoinVertexID> _order_edge_vertices(const JoinEdge& edge) const;
};
}  // namespace opossum
