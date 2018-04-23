#include "join_graph_builder.hpp"

#include <numeric>
#include <stack>

#include "join_edge.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::shared_ptr<JoinGraph> JoinGraphBuilder::operator()(const std::shared_ptr<AbstractLQPNode>& lqp) {
  /**
   * Traverse the LQP until the first non-vertex type (e.g. a UnionNode) is found or a node doesn't have precisely
   * one input. This way, we traverse past Sort/Aggregate etc. nodes that later form the "outputs" of the JoinGraph
   */
  auto current_node = lqp;
  while (_lqp_node_type_is_vertex(current_node->type())) {
    if (!current_node->left_input() || current_node->right_input()) {
      break;
    }

    current_node = current_node->left_input();
  }
  const auto output_relations = current_node->output_relations();

  // Traverse the LQP, identifying JoinPlanPredicates and Vertices
  _traverse(current_node);

  auto edges = join_edges_from_predicates(_vertices, _predicates);
  auto cross_edges = cross_edges_between_components(_vertices, edges);

  edges.insert(edges.end(), cross_edges.begin(), cross_edges.end());

  return std::make_shared<JoinGraph>(std::move(_vertices), std::move(output_relations), std::move(edges));
}

void JoinGraphBuilder::_traverse(const std::shared_ptr<AbstractLQPNode>& node) {
  // Makes it possible to call _traverse() on inputs without checking whether they exist first.
  if (!node) {
    return;
  }

  if (_lqp_node_type_is_vertex(node->type())) {
    _vertices.emplace_back(node);
    return;
  }

  switch (node->type()) {
    case LQPNodeType::Join: {
      /**
       * Cross joins are simply being traversed past. Outer joins are hard to address during JoinOrdering and until we
       * do, outer join predicates are not included in the JoinGraph.
       * The outer join node is added as a vertex and traversal stops at this point.
       */

      const auto join_node = std::static_pointer_cast<JoinNode>(node);

      if (join_node->join_mode() == JoinMode::Inner) {
        _predicates.emplace_back(std::make_shared<JoinPlanAtomicPredicate>(
            join_node->join_column_references()->first, *join_node->predicate_condition(),
            join_node->join_column_references()->second));
      }

      if (join_node->join_mode() == JoinMode::Inner || join_node->join_mode() == JoinMode::Cross) {
        _traverse(node->left_input());
        _traverse(node->right_input());
      } else {
        _vertices.emplace_back(node);
      }
    } break;

    case LQPNodeType::Predicate: {
      /**
       * BETWEEN PredicateNodes are turned into two predicates, because JoinPlanPredicates do not support BETWEEN. All
       * other PredicateConditions produce exactly one JoinPlanPredicate
       */

      const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);

      if (predicate_node->value2()) {
        DebugAssert(predicate_node->predicate_condition() == PredicateCondition::Between, "Expected between");

        _predicates.emplace_back(std::make_shared<JoinPlanAtomicPredicate>(
            predicate_node->column_reference(), PredicateCondition::GreaterThanEquals, predicate_node->value()));

        _predicates.emplace_back(std::make_shared<JoinPlanAtomicPredicate>(
            predicate_node->column_reference(), PredicateCondition::LessThanEquals, *predicate_node->value2()));
      } else {
        _predicates.emplace_back(std::make_shared<JoinPlanAtomicPredicate>(
            predicate_node->column_reference(), predicate_node->predicate_condition(), predicate_node->value()));
      }

      _traverse(node->left_input());
    } break;

    case LQPNodeType::Union: {
      /**
       * A UnionNode is the entry point to disjunction, which is parsed starting from _parse_union(). Normal traversal
       * is commenced from the node "below" the Union.
       */

      const auto union_node = std::static_pointer_cast<UnionNode>(node);

      if (union_node->union_mode() == UnionMode::Positions) {
        const auto parse_result = _parse_union(union_node);

        _traverse(parse_result.base_node);
        _predicates.emplace_back(parse_result.predicate);
      } else {
        _vertices.emplace_back(node);
      }
    } break;

    default: { Fail("Node type not suited for JoinGraph"); }
  }
}

JoinGraphBuilder::PredicateParseResult JoinGraphBuilder::_parse_predicate(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  if (node->type() == LQPNodeType::Predicate) {
    const auto predicate_node = std::static_pointer_cast<const PredicateNode>(node);

    std::shared_ptr<const AbstractJoinPlanPredicate> left_predicate;

    if (predicate_node->value2()) {
      DebugAssert(predicate_node->predicate_condition() == PredicateCondition::Between, "Expected between");

      left_predicate = std::make_shared<JoinPlanLogicalPredicate>(
          std::make_shared<JoinPlanAtomicPredicate>(predicate_node->column_reference(),
                                                    PredicateCondition::GreaterThanEquals, predicate_node->value()),
          JoinPlanPredicateLogicalOperator::And,
          std::make_shared<JoinPlanAtomicPredicate>(predicate_node->column_reference(),
                                                    PredicateCondition::LessThanEquals, *predicate_node->value2()));
    } else {
      left_predicate = std::make_shared<JoinPlanAtomicPredicate>(
          predicate_node->column_reference(), predicate_node->predicate_condition(), predicate_node->value());
    }

    const auto base_node = predicate_node->left_input();

    if (base_node->output_count() > 1) {
      return {base_node, left_predicate};
    } else {
      const auto parse_result_right = _parse_predicate(base_node);

      const auto and_predicate = std::make_shared<JoinPlanLogicalPredicate>(
          left_predicate, JoinPlanPredicateLogicalOperator::And, parse_result_right.predicate);

      return {parse_result_right.base_node, and_predicate};
    }
  } else if (node->type() == LQPNodeType::Union) {
    return _parse_union(std::static_pointer_cast<UnionNode>(node));
  } else {
    Fail("Unexpected node type");
  }
}

JoinGraphBuilder::PredicateParseResult JoinGraphBuilder::_parse_union(
    const std::shared_ptr<UnionNode>& union_node) const {
  DebugAssert(union_node->left_input() && union_node->right_input(),
              "UnionNode needs both inputs set in order to be parsed");

  const auto parse_result_left = _parse_predicate(union_node->left_input());
  const auto parse_result_right = _parse_predicate(union_node->right_input());

  DebugAssert(parse_result_left.base_node == parse_result_right.base_node, "Invalid OR not having a single base node");

  const auto or_predicate = std::make_shared<JoinPlanLogicalPredicate>(
      parse_result_left.predicate, JoinPlanPredicateLogicalOperator::Or, parse_result_right.predicate);

  return {parse_result_left.base_node, or_predicate};
}

bool JoinGraphBuilder::_lqp_node_type_is_vertex(const LQPNodeType node_type) const {
  return node_type != LQPNodeType::Join && node_type != LQPNodeType::Union && node_type != LQPNodeType::Predicate;
}

std::vector<std::shared_ptr<JoinEdge>> JoinGraphBuilder::join_edges_from_predicates(
    const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices,
    const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates) {
  std::unordered_map<std::shared_ptr<AbstractLQPNode>, size_t> vertex_to_index;
  std::map<JoinVertexSet, std::shared_ptr<JoinEdge>> vertices_to_edge;
  std::vector<std::shared_ptr<JoinEdge>> edges;

  for (size_t vertex_idx = 0; vertex_idx < vertices.size(); ++vertex_idx) {
    vertex_to_index[vertices[vertex_idx]] = vertex_idx;
  }

  for (const auto& predicate : predicates) {
    const auto vertex_set = predicate->get_accessed_vertex_set(vertices);
    auto iter = vertices_to_edge.find(vertex_set);
    if (iter == vertices_to_edge.end()) {
      auto edge = std::make_shared<JoinEdge>(vertex_set);
      iter = vertices_to_edge.emplace(vertex_set, edge).first;
      edges.emplace_back(edge);
    }
    iter->second->predicates.emplace_back(predicate);
  }

  return edges;
}

std::vector<std::shared_ptr<JoinEdge>> JoinGraphBuilder::cross_edges_between_components(
    const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices,
    const std::vector<std::shared_ptr<JoinEdge>>& edges) {
  /**
   * Create edges from the gathered JoinPlanPredicates. We can't directly create the JoinGraph from this since we want
   * the JoinGraph to be connected and there might be edges from CrossJoins still missing.
   * To make the JoinGraph connected, we identify all Components and connect them to a Chain.
   *
   * So this
   *
   *   B
   *  / \    D--E   F
   * A---C
   *
   *
   * becomes
   *
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
   *
   * HOW THIS IS DONE: We identify the components in the graph using a breadth first search (BFS). Every BFS will
   * identify one component. One vertex from each component is stored and these are later connected to form a chain,
   * so the entire graph becomes connected
   */

  // We flood the entire graph. remaining_vertex_indices contains all vertices not yet reached (which, as we start, are
  // all of them.
  std::unordered_set<size_t> remaining_vertex_indices;
  for (auto vertex_idx = size_t{0}; vertex_idx < vertices.size(); ++vertex_idx) {
    remaining_vertex_indices.insert(vertex_idx);
  }

  // Filled with exactly one vertex for each component. The vertices are later chained to make the entire graph
  // connected.
  std::vector<size_t> one_vertex_per_component;

  // We don't need to to look at an Edge multiple times, so we keep a set of those that we haven't reached
  auto remaining_edges = edges;

  // Loop performing one BFS for each component, identifying all vertices of that component
  while (!remaining_vertex_indices.empty()) {
    // Pick one vertex we haven't yet reached and start a breath first search-flooding from it
    const auto bfs_origin_vertex_idx = *remaining_vertex_indices.begin();

    one_vertex_per_component.emplace_back(bfs_origin_vertex_idx);

    std::stack<size_t> bfs_stack;
    bfs_stack.push(bfs_origin_vertex_idx);

    while (!bfs_stack.empty()) {
      const auto current_bfs_vertex_idx = bfs_stack.top();
      bfs_stack.pop();

      // We might reach a vertex multiple times. This doesn't cause any problems, but there is no need
      // to look for any connected edges of this vertex as done in the for loop below, so we can stop here.
      const auto erased_vertex_count = remaining_vertex_indices.erase(current_bfs_vertex_idx);
      if (erased_vertex_count == 0) continue;

      // Find the edges that are connected to this vertex and add the other vertices of this Edge to the BFS stack.
      for (auto edge_iter = remaining_edges.begin(); edge_iter != remaining_edges.end();) {
        const auto& edge = *edge_iter;
        if (!edge->vertex_set.test(current_bfs_vertex_idx)) {
          ++edge_iter;
          continue;
        }

        auto connected_vertex_idx = edge->vertex_set.find_first();
        while (connected_vertex_idx != JoinVertexSet::npos) {
          if (connected_vertex_idx != current_bfs_vertex_idx) bfs_stack.push(connected_vertex_idx);
          connected_vertex_idx = edge->vertex_set.find_next(connected_vertex_idx);
        }

        edge_iter = remaining_edges.erase(edge_iter);
      }
    }
  }

  // One or less components? No need to chain up any vertices
  if (one_vertex_per_component.size() < 2) return {};

  /**
   * Create the Edges that connect the components
   */
  std::vector<std::shared_ptr<JoinEdge>> inter_component_edges;
  inter_component_edges.reserve(one_vertex_per_component.size() - 1);

  for (auto component_idx = size_t{1}; component_idx < one_vertex_per_component.size(); ++component_idx) {
    JoinVertexSet vertex_set{vertices.size()};
    vertex_set.set(one_vertex_per_component[component_idx - 1]);
    vertex_set.set(one_vertex_per_component[component_idx]);

    inter_component_edges.emplace_back(std::make_shared<JoinEdge>(vertex_set));
  }

  return inter_component_edges;
}

}  // namespace opossum
