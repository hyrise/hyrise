#include "join_graph_builder.hpp"

#include <algorithm>
#include <numeric>
#include <queue>
#include <stack>

#include "expression/expression_functional.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "utils/assert.hpp"

#include "join_graph_edge.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

std::optional<JoinGraph> JoinGraphBuilder::operator()(const std::shared_ptr<AbstractLQPNode>& lqp) {
  // No need to create a join graph consisting of just one vertex and no predicates
  if (_lqp_node_type_is_vertex(lqp->type)) return std::nullopt;

  _traverse(lqp);

  /**
   * Turn the predicates into JoinEdges and build the JoinGraph
   */
  auto edges = _join_edges_from_predicates(_vertices, _predicates);
  auto cross_edges = _cross_edges_between_components(_vertices, edges);

  edges.insert(edges.end(), cross_edges.begin(), cross_edges.end());

  // A single vertex without predicates is not considered a JoinGraph
  if (_vertices.size() <= 1u && edges.empty()) return std::nullopt;

  return JoinGraph{_vertices, edges};
}

void JoinGraphBuilder::_traverse(const std::shared_ptr<AbstractLQPNode>& node) {
  // Makes it possible to call _traverse() on inputs without checking whether they exist first.
  if (!node) return;

  if (_lqp_node_type_is_vertex(node->type)) {
    _vertices.emplace_back(node);
    return;
  }

  switch (node->type) {
    case LQPNodeType::Join: {
      /**
       * Cross joins are simply being traversed past. Outer joins are hard to address during join ordering and until we
       * do outer joins are opaque: The outer join node is added as a vertex and traversal stops at this point.
       */

      const auto join_node = std::static_pointer_cast<JoinNode>(node);

      if (join_node->join_mode == JoinMode::Inner) {
        _predicates.emplace_back(join_node->join_predicate);
      }

      if (join_node->join_mode == JoinMode::Inner || join_node->join_mode == JoinMode::Cross) {
        _traverse(node->left_input());
        _traverse(node->right_input());
      } else {
        _vertices.emplace_back(node);
      }
    } break;

    case LQPNodeType::Predicate: {
      const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);
      _predicates.emplace_back(predicate_node->predicate);

      _traverse(node->left_input());
    } break;

    case LQPNodeType::Union: {
      /**
       * A UnionNode is the entry point to a disjunction, which is parsed starting from _parse_union(). Normal traversal
       * is commenced from the node "below" the Union.
       */

      const auto union_node = std::static_pointer_cast<UnionNode>(node);

      if (union_node->union_mode == UnionMode::Positions) {
        const auto parse_result = _parse_union(union_node);

        _traverse(parse_result.base_node);
        _predicates.emplace_back(parse_result.predicate);
      } else {
        _vertices.emplace_back(node);
      }
    } break;

    default: { Fail("Node type cannot be used for JoinGraph, should have been detected as a vertex"); }
  }
}

JoinGraphBuilder::PredicateParseResult JoinGraphBuilder::_parse_predicate(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  switch (node->type) {
    case LQPNodeType::Predicate: {
      const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);

      const auto left_predicate = predicate_node->predicate;

      const auto base_node = predicate_node->left_input();

      if (base_node->output_count() > 1) {
        return {base_node, left_predicate};
      } else {
        const auto parse_result_right = _parse_predicate(base_node);
        const auto and_predicate = and_(left_predicate, parse_result_right.predicate);

        return {parse_result_right.base_node, and_predicate};
      }
    } break;

    case LQPNodeType::Union:
      return _parse_union(std::static_pointer_cast<UnionNode>(node));

    default:
      Assert(node->left_input() && !node->right_input(), "");
      return _parse_predicate(node->left_input());
  }
}

JoinGraphBuilder::PredicateParseResult JoinGraphBuilder::_parse_union(
    const std::shared_ptr<UnionNode>& union_node) const {
  DebugAssert(union_node->left_input() && union_node->right_input(),
              "UnionNode needs both inputs set in order to be parsed");

  const auto parse_result_left = _parse_predicate(union_node->left_input());
  const auto parse_result_right = _parse_predicate(union_node->right_input());

  DebugAssert(parse_result_left.base_node == parse_result_right.base_node, "Invalid OR not having a single base node");

  const auto or_predicate = or_(parse_result_left.predicate, parse_result_right.predicate);

  return {parse_result_left.base_node, or_predicate};
}

bool JoinGraphBuilder::_lqp_node_type_is_vertex(const LQPNodeType node_type) const {
  return node_type != LQPNodeType::Join && node_type != LQPNodeType::Union && node_type != LQPNodeType::Predicate;
}

std::vector<JoinGraphEdge> JoinGraphBuilder::_join_edges_from_predicates(
    const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices,
    const std::vector<std::shared_ptr<AbstractExpression>>& predicates) {
  std::map<JoinGraphVertexSet, size_t> vertices_to_edge_idx;
  std::vector<JoinGraphEdge> edges;

  for (const auto& predicate : predicates) {
    const auto vertex_set = _get_vertex_set_accessed_by_expression(*predicate, vertices);
    auto iter = vertices_to_edge_idx.find(vertex_set);
    if (iter == vertices_to_edge_idx.end()) {
      iter = vertices_to_edge_idx.emplace(vertex_set, edges.size()).first;
      edges.emplace_back(vertex_set);
    }
    edges[iter->second].predicates.emplace_back(predicate);
  }

  return edges;
}

std::vector<JoinGraphEdge> JoinGraphBuilder::_cross_edges_between_components(
    const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices, std::vector<JoinGraphEdge> edges) {
  /**
   * Create edges from the gathered JoinPlanPredicates. We can't directly create the JoinGraph from this since we want
   * the JoinGraph to be connected and there might be edges from CrossJoins still missing.
   * To make the JoinGraph connected, we identify all Components and connect them to a Chain.
   *
   * So this
   *   B
   *  / \    D--E   F
   * A---C
   *
   * becomes
   *   B
   *  / \
   * A---C
   * |
   * D--E
   * |
   * F
   *
   * where the edges AD and DF are being created and have no predicates. There is of course the theoretical chance that
   * different edges, say CD and EF would result in a better plan. We ignore this possibility for now.
   */

  std::unordered_set<size_t> remaining_vertex_indices;
  for (auto vertex_idx = size_t{0}; vertex_idx < vertices.size(); ++vertex_idx) {
    remaining_vertex_indices.insert(vertex_idx);
  }

  std::vector<size_t> one_vertex_per_component;

  while (!remaining_vertex_indices.empty()) {
    const auto vertex_idx = *remaining_vertex_indices.begin();

    one_vertex_per_component.emplace_back(vertex_idx);

    std::stack<size_t> bfs_stack;
    bfs_stack.push(vertex_idx);

    while (!bfs_stack.empty()) {
      const auto vertex_idx2 = bfs_stack.top();
      bfs_stack.pop();

      remaining_vertex_indices.erase(vertex_idx2);

      for (auto iter = edges.begin(); iter != edges.end();) {
        const auto& edge = *iter;
        // Skip edges not connected to this vertex.
        // Also skip hyperedges, as hyperedges do not connect components; components connected only by a hyperedge
        //    need a cross join edge between them anyway. DPccp needs the JoinGraphs to be connected without relying on
        //    the hyperedges
        if (!edge.vertex_set.test(vertex_idx2) || edge.vertex_set.count() != 2) {
          ++iter;
          continue;
        }

        auto connected_vertex_idx = edge.vertex_set.find_first();
        while (connected_vertex_idx != JoinGraphVertexSet::npos) {
          if (connected_vertex_idx != vertex_idx2) bfs_stack.push(connected_vertex_idx);
          connected_vertex_idx = edge.vertex_set.find_next(connected_vertex_idx);
        }

        iter = edges.erase(iter);
      }
    }
  }

  if (one_vertex_per_component.size() < 2) return {};

  std::vector<JoinGraphEdge> inter_component_edges;
  inter_component_edges.reserve(one_vertex_per_component.size() - 1);

  for (auto component_idx = size_t{1}; component_idx < one_vertex_per_component.size(); ++component_idx) {
    JoinGraphVertexSet vertex_set{vertices.size()};
    vertex_set.set(one_vertex_per_component[component_idx - 1]);
    vertex_set.set(one_vertex_per_component[component_idx]);

    inter_component_edges.emplace_back(vertex_set);
  }

  return inter_component_edges;
}

JoinGraphVertexSet JoinGraphBuilder::_get_vertex_set_accessed_by_expression(
    const AbstractExpression& expression, const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices) {
  auto vertex_set = JoinGraphVertexSet{vertices.size()};
  for (auto vertex_idx = size_t{0}; vertex_idx < vertices.size(); ++vertex_idx) {
    if (vertices[vertex_idx]->find_column_id(expression)) {
      vertex_set.set(vertex_idx);
    }
  }

  if (!vertex_set.none()) {
    // The expression is the computed output of a vertex
    return vertex_set;
  }

  for (const auto& argument : expression.arguments) {
    vertex_set |= _get_vertex_set_accessed_by_expression(*argument, vertices);
  }

  return vertex_set;
}

}  // namespace opossum
