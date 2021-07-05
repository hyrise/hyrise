#include "dips_pruning_graph.hpp"

namespace opossum {
// To be able to push dips through joins we first need to construct a graph on which we can execute the main algorithm.
//  We are doing this by recursively traversing over the LQP graph. In every visit of a node the following steps are
// executed:
// 1. Check that the currently visited node is a join node.
// 2. Get the join predicates
// 3. Check that the left and right operands are LQPColumnExpression.
// 4. Get each of the associated StoredTableNode of the left and right expressions.
// 5. Add both of the storage nodes to the graph (if they are not in it) and connect them with edges (if they are not
// connected).
// 6. Add the predicates to the associated edges.
void DipsPruningGraph::build_graph(const std::shared_ptr<AbstractLQPNode>& node){
  // Why do we exit in this cases ?
  if (node->type == LQPNodeType::Union || node->type == LQPNodeType::Intersect || node->type == LQPNodeType::Except) {
    return;
  }

  if (node->left_input()) build_graph(node->left_input());
  if (node->right_input()) build_graph(node->right_input());

  // This rule only supports the inner and semi join
  if (node->type == LQPNodeType::Join) {
    if (std::find(supported_join_types.begin(), supported_join_types.end(),
                  std::dynamic_pointer_cast<JoinNode>(node)->join_mode) == supported_join_types.end()) {
      return;
    }
    const auto& join_node = static_cast<JoinNode&>(*node);
    const auto& join_predicates = join_node.join_predicates();

    for (const auto& predicate : join_predicates) {
      // Why do we need to cast the predicates to binary predicate expressions?
      std::shared_ptr<BinaryPredicateExpression> binary_predicate =
          std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate);

      Assert(binary_predicate, "Expected BinaryPredicateExpression!");

      // We are only interested in equal predicate conditions (The dibs rule is only working with equal predicates)
      if (binary_predicate->predicate_condition != PredicateCondition::Equals) {
        continue;
      }

      auto left_operand = binary_predicate->left_operand();
      auto right_operand = binary_predicate->right_operand();

      auto left_lqp = std::dynamic_pointer_cast<LQPColumnExpression>(left_operand);
      auto right_lqp = std::dynamic_pointer_cast<LQPColumnExpression>(right_operand);

      // We need to check that the type is LQPColumn
      if (!left_lqp || !right_lqp) {
        continue;
      }

      auto l = std::dynamic_pointer_cast<const StoredTableNode>(left_lqp->original_node.lock());
      auto r = std::dynamic_pointer_cast<const StoredTableNode>(right_lqp->original_node.lock());

      Assert(l && r, "Expected StoredTableNode");

      std::shared_ptr<StoredTableNode> left_stored_table_node = std::const_pointer_cast<StoredTableNode>(l);
      std::shared_ptr<StoredTableNode> right_stored_table_node = std::const_pointer_cast<StoredTableNode>(r);

      // access join graph nodes (every storage table note is represented inside the join graph)
      auto left_join_graph_node = _get_vertex(left_stored_table_node);
      auto right_join_graph_node = _get_vertex(right_stored_table_node);

      auto vertex_set = _get_vertex_set(left_join_graph_node, right_join_graph_node);

      _add_edge(vertex_set, binary_predicate);
    }
  }
}

std::vector<DipsPruningGraphEdge> DipsPruningGraph::top_down_traversal() {
  std::vector<DipsPruningGraphEdge> traversal_order{};
  std::set<size_t> visited{};
  _top_down_traversal_visit(0, traversal_order, visited);
  return traversal_order;  
}

std::vector<DipsPruningGraphEdge> DipsPruningGraph::bottom_up_traversal() {
  std::vector<DipsPruningGraphEdge> traversal_order{};
  std::set<size_t> visited{};
  _bottom_up_traversal_visit(0, traversal_order, visited);
  return traversal_order;
}

bool DipsPruningGraph::is_tree() {
  std::set<size_t> visited{};
  return _is_tree_visit(0, 0, visited);
}

bool DipsPruningGraph::empty() {
  return vertices.size() == 0;
}

size_t DipsPruningGraph::_get_vertex(std::shared_ptr<StoredTableNode> table_node) {
    auto it = std::find(vertices.begin(), vertices.end(), table_node);
    if (it != vertices.end()) {
      return it - vertices.begin();
    }
    vertices.push_back(table_node);
    return vertices.size() - 1;
}

std::set<size_t> DipsPruningGraph::_get_vertex_set(size_t noda_a, size_t noda_b){
    Assert((noda_a < vertices.size() || noda_b  < vertices.size()), "Nodes should exist in graph");
    return std::set<size_t>{noda_a, noda_b};
}

void DipsPruningGraph::_add_edge(std::set<size_t> vertex_set, std::shared_ptr<BinaryPredicateExpression> predicate) {
    for (auto& edge : edges) {
        if (vertex_set == edge.vertex_set) {
        edge.append_predicate(predicate);
        return;
        }
    }
    edges.emplace_back(vertex_set, predicate);
}

bool DipsPruningGraph::_is_tree_visit(size_t current_node, size_t parrent, std::set<size_t>& visited) {
  visited.insert(current_node);

  for (auto& edge : edges) {
    if (edge.connects_vertex(current_node)) {
      auto neighbour = edge.neighbour(current_node);
      // We do not want to go back to the parent node.
      if (neighbour == parrent) continue;
      if (visited.find(neighbour) != visited.end()) return false;
      if (!_is_tree_visit(neighbour, current_node, visited)) return false;
    }
  }
  return true;
}

  void DipsPruningGraph::_top_down_traversal_visit(size_t current_node, std::vector<DipsPruningGraphEdge>& traversal_order, std::set<size_t>& visited){
    visited.insert(current_node);
    for (auto& edge : edges) {
      if (edge.connects_vertex(current_node)) {
        auto neighbour = edge.neighbour(current_node);
        // We do not want to go back to the parent node.
        if (visited.find(neighbour) != visited.end()) continue;
        traversal_order.push_back(edge);
        _top_down_traversal_visit(neighbour, traversal_order, visited);
      }
    }
  }

void DipsPruningGraph::_bottom_up_traversal_visit(size_t current_node, std::vector<DipsPruningGraphEdge>& traversal_order, std::set<size_t>& visited) {
  visited.insert(current_node);
  // TODO: Fix Hacky solution
  auto parent_edge = edges[0];
  for (auto& edge : edges) {
    if (edge.connects_vertex(current_node)) {
      auto neighbour = edge.neighbour(current_node);
      if (visited.find(neighbour) != visited.end()) {
        parent_edge = edge;
        continue;
      }
      _bottom_up_traversal_visit(neighbour, traversal_order, visited);
    }
  }
  traversal_order.push_back(parent_edge);
}

} // namespace opossum
