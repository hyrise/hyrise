#include "greedy_join_ordering.hpp"

#include <algorithm>
#include <numeric>
#include <set>

#include "optimizer/join_graph.hpp"
#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/table_statistics.hpp"
#include "utils/assert.hpp"
#include "utils/type_utils.hpp"

namespace opossum {

GreedyJoinOrdering::GreedyJoinOrdering(const std::shared_ptr<const JoinGraph>& input_graph)
    : _input_graph(input_graph) {}

std::shared_ptr<AbstractASTNode> GreedyJoinOrdering::run() {
  _left_column_id_of_vertex.resize(_input_graph->vertices().size(), INVALID_COLUMN_ID);

  /**
   * Initialize edge by vertex lookup
   */
  _edges_by_vertex_id.resize(_input_graph->vertices().size());
  for (size_t edge_idx = 0; edge_idx < _input_graph->edges().size(); ++edge_idx) {
    auto& edge = _input_graph->edges()[edge_idx];
    _edges_by_vertex_id[edge.vertex_indices.first].emplace_back(edge_idx);
    _edges_by_vertex_id[edge.vertex_indices.second].emplace_back(edge_idx);
  }

  // Vertices still to be added to the join plan
  const auto& input_vertices = _input_graph->vertices();

  for (size_t edge_idx = 0; edge_idx < _input_graph->edges().size(); ++edge_idx) {
    _remaining_edge_indices.emplace(edge_idx);
  }

  std::set<size_t> neighbourhood_edge_indices;

  const auto initial_vertex_idx = pick_cheapest_vertex(_input_graph->vertices());

  /**
   * Initialize plan and neighbourhood with first vertex
   */
  auto current_root = _input_graph->vertices()[initial_vertex_idx];
  const auto& initial_edges = _edges_by_vertex_id[initial_vertex_idx];
  neighbourhood_edge_indices.insert(initial_edges.begin(), initial_edges.end());
  _left_column_id_of_vertex[initial_vertex_idx] = ColumnID{0};

  /**
   * Add all remaining vertices to the join plan
   */
  for (size_t join_plan_size = 1; join_plan_size < input_vertices.size(); ++join_plan_size) {
    Assert(!neighbourhood_edge_indices.empty(),
           "No neighbourhood left, but the join plan is not done yet. "
           "This means the input graph was not connected in the first place");

    /**
     * Find the next vertex to join and store it in `next_join_vertex_idx`
     */
    auto min_join_cost = std::numeric_limits<float>::max();
    auto next_join_edge_idx = 0;  // TODO(moritz) symbolic constant

    for (auto iter = neighbourhood_edge_indices.begin(); iter != neighbourhood_edge_indices.end(); ++iter) {
      auto join_cost = cost_join(current_root, *iter);

      if (join_cost < min_join_cost) {
        min_join_cost = join_cost;
        next_join_edge_idx = *iter;
      }
    }

    const auto& join_edge = _input_graph->edges()[next_join_edge_idx];
    const auto join_vertex_ids = order_edge_vertices(join_edge);

    const auto join_column_ids = get_edge_column_ids(next_join_edge_idx, join_vertex_ids.second);

    // Update the neighbourhood of the join plan with the new vertex
    const auto predicate_edge_indices = update_neighbourhood(neighbourhood_edge_indices, next_join_edge_idx);

    /**
     * Extend the join plan with a new join node
     */
    _left_column_id_of_vertex[join_vertex_ids.second] =
        ColumnID{static_cast<ColumnID::base_type>(current_root->output_column_count())};
    auto new_root = std::make_shared<JoinNode>(JoinMode::Inner, join_column_ids, join_edge.scan_type);
    new_root->set_left_child(current_root);
    new_root->set_right_child(_input_graph->vertices()[join_vertex_ids.second]);
    current_root = new_root;

    /**
     * Append a predicate for each edge that was also added to the join plan, but was not the join_edge
     */
    for (const auto& edge_idx : predicate_edge_indices) {
      const auto& predicate_edge = _input_graph->edges()[edge_idx];

      const auto left_column_id =
          _left_column_id_of_vertex[predicate_edge.vertex_indices.first] + predicate_edge.column_ids.first;
      const auto right_column_id =
          _left_column_id_of_vertex[predicate_edge.vertex_indices.second] + predicate_edge.column_ids.second;

      auto new_root = std::make_shared<PredicateNode>(ColumnID{static_cast<ColumnID::base_type>(left_column_id)},
                                                      predicate_edge.scan_type,
                                                      ColumnID{static_cast<ColumnID::base_type>(right_column_id)});
      new_root->set_left_child(current_root);
      current_root = new_root;
    }
  }

  return current_root;
}

JoinVertexID GreedyJoinOrdering::pick_cheapest_vertex(const JoinGraph::Vertices& vertices) const {
  auto cheapest_idx = JoinVertexID{0};
  auto cheapest_costs = vertices[0]->get_statistics()->row_count();

  for (auto vertex_idx = JoinVertexID{1}; vertex_idx < vertices.size(); ++vertex_idx) {
    auto costs = vertices[vertex_idx]->get_statistics()->row_count();
    if (costs < cheapest_costs) {
      cheapest_costs = costs;
      cheapest_idx = vertex_idx;
    }
  }

  return cheapest_idx;
}

std::vector<size_t> GreedyJoinOrdering::update_neighbourhood(std::set<size_t>& neighbourhood_edges,
                                                             size_t join_edge_idx) {
  const auto& join_edge = _input_graph->edges()[join_edge_idx];

  auto vertex_ids = order_edge_vertices(join_edge);

  neighbourhood_edges.erase(join_edge_idx);
  _remaining_edge_indices.erase(join_edge_idx);

  std::vector<size_t> predicate_edge_ids;
  for (const auto& edge_idx : neighbourhood_edges) {
    const auto& edge = _input_graph->edges()[edge_idx];

    if (edge.vertex_indices.first == vertex_ids.second || edge.vertex_indices.second == vertex_ids.second) {
      predicate_edge_ids.emplace_back(edge_idx);
      neighbourhood_edges.erase(edge_idx);
    }
  }

  auto new_vertex_neighbourhood = extract_vertex_neighbourhood(vertex_ids.second);
  neighbourhood_edges.insert(new_vertex_neighbourhood.begin(), new_vertex_neighbourhood.end());

  return predicate_edge_ids;
}

float GreedyJoinOrdering::cost_join(const std::shared_ptr<AbstractASTNode>& left_node, size_t edge_idx) const {
  const auto& edge = _input_graph->edges()[edge_idx];

  const auto vertex_ids = order_edge_vertices(edge);
  const auto& new_vertex = _input_graph->vertices()[vertex_ids.second];
  const auto join_column_ids = get_edge_column_ids(edge_idx, vertex_ids.second);

  const auto join_stats = left_node->get_statistics()->generate_predicated_join_statistics(
      new_vertex->get_statistics(), JoinMode::Inner, join_column_ids, edge.scan_type);
  return join_stats->row_count();
}

std::pair<ColumnID, ColumnID> GreedyJoinOrdering::get_edge_column_ids(size_t edge_idx,
                                                                      JoinVertexID right_vertex_id) const {
  /**
   * Returns the ColumnIDs required for joining the vertex right_vertex_id to the join plan using the edge edge_idx
   * The .first member of the returned pair is the ColumnID on the left side of the join, the .second member the ColumnID
   * in the newly joined vertex.
   * NOTE: When creating the JoinNode, this might require the scan type to be flipped. TODO(moritz)
   */
  const auto& edge = _input_graph->edges()[edge_idx];
  if (edge.vertex_indices.second == right_vertex_id) {
    return std::make_pair(
        make_column_id(_left_column_id_of_vertex[edge.vertex_indices.first] + edge.column_ids.first),
        edge.column_ids.second);
  }
  return std::make_pair(
      make_column_id(_left_column_id_of_vertex[edge.vertex_indices.second] + edge.column_ids.second),
      edge.column_ids.first);
}

std::set<size_t> GreedyJoinOrdering::extract_vertex_neighbourhood(JoinVertexID vertex_idx) {
  /**
   * Identify the edges adjacent to @vertex_idx that connect to vertices outside of the current join plan
   */

  std::set<size_t> edge_indices;
  for (const auto& edge_idx : _remaining_edge_indices) {
    const auto& edge = _input_graph->edges()[edge_idx];

    if (edge.vertex_indices.first == vertex_idx || edge.vertex_indices.second == vertex_idx) {
      edge_indices.emplace(edge_idx);
    }
  }

  for (const auto& edge_idx : edge_indices) {
    _remaining_edge_indices.erase(edge_idx);
  }

  return edge_indices;
}

std::pair<JoinVertexID, JoinVertexID> GreedyJoinOrdering::order_edge_vertices(const JoinEdge& edge) const {
  auto new_vertex_idx = edge.vertex_indices.first;
  auto contained_vertex_idx = edge.vertex_indices.second;

  if (_left_column_id_of_vertex[edge.vertex_indices.first] == INVALID_COLUMN_ID) {
    Assert(_left_column_id_of_vertex[edge.vertex_indices.second] != INVALID_COLUMN_ID,
           "Neither vertex of the edge to be joined is already in the join plan. This is a bug.");
    std::swap(new_vertex_idx, contained_vertex_idx);
  }

  return std::make_pair(new_vertex_idx, contained_vertex_idx);
}
}