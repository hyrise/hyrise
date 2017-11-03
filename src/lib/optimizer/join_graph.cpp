#include "join_graph.hpp"

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "constant_mappings.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/type_utils.hpp"

namespace opossum {

JoinEdge::JoinEdge(const std::pair<JoinVertexID, JoinVertexID>& vertex_indices,
                   const std::pair<ColumnID, ColumnID>& column_ids, JoinMode join_mode, ScanType scan_type)
    : vertex_indices(vertex_indices), column_ids(column_ids), join_mode(join_mode), scan_type(scan_type) {
  DebugAssert(join_mode == JoinMode::Inner, "This constructor only supports inner join edges");
}

JoinEdge::JoinEdge(const std::pair<JoinVertexID, JoinVertexID>& vertex_indices, JoinMode join_mode)
    : vertex_indices(vertex_indices), join_mode(join_mode) {
  DebugAssert(join_mode == JoinMode::Cross, "This constructor only supports cross join edges");
}

JoinVertexPredicate::JoinVertexPredicate(ColumnID column_id, ScanType scan_type, const AllParameterVariant& value)
    : column_id(column_id), scan_type(scan_type), value(value) {}

JoinVertex::JoinVertex(const std::shared_ptr<AbstractASTNode>& node) : node(node) {}

std::shared_ptr<JoinGraph> JoinGraph::build_join_graph(const std::shared_ptr<AbstractASTNode>& root) {
  JoinGraph::Vertices vertices;
  JoinGraph::Edges edges;

  _traverse_ast_for_join_graph(root, vertices, edges);

  return std::make_shared<JoinGraph>(std::move(vertices), std::move(edges));
}

JoinGraph::JoinGraph(Vertices&& vertices, Edges&& edges) : _vertices(std::move(vertices)), _edges(std::move(edges)) {}

const JoinGraph::Vertices& JoinGraph::vertices() const { return _vertices; }

const JoinGraph::Edges& JoinGraph::edges() const { return _edges; }

void JoinGraph::print(std::ostream& out) const {
  out << "==== JoinGraph ====" << std::endl;
  out << "==== Vertices ====" << std::endl;
  for (size_t vertex_idx = 0; vertex_idx < _vertices.size(); ++vertex_idx) {
    const auto& vertex = _vertices[vertex_idx];
    std::cout << vertex_idx << ":  " << vertex.node->description() << std::endl;
  }
  out << "==== Edges ====" << std::endl;
  for (const auto& edge : _edges) {
    if (edge.join_mode == JoinMode::Inner) {
      std::cout << edge.vertex_indices.first << " <-- " << edge.column_ids->first << " "
                << scan_type_to_string.left.at(*edge.scan_type) << " " << edge.column_ids->second << " --> "
                << edge.vertex_indices.second << std::endl;
    } else {
      std::cout << edge.vertex_indices.first << " <----> " << edge.vertex_indices.second << std::endl;
    }
  }

  out << "===================" << std::endl;
}

void JoinGraph::_traverse_ast_for_join_graph(const std::shared_ptr<AbstractASTNode>& node,
                                             JoinGraph::Vertices& o_vertices, JoinGraph::Edges& o_edges) {
  /**
   * Early return to make it possible to call search_join_graph() on both children without having to check whether they
   * are nullptr.
   */
  if (!node) {
    return;
  }

  Assert(node->num_parents() <= 1, "Nodes with multiple parents not supported when building JoinGraph");

  if (node->type() == ASTNodeType::Join) {
    const auto join_node = std::static_pointer_cast<JoinNode>(node);

    if (join_node->join_mode() == JoinMode::Inner) {
      _traverse_inner_join_node(join_node, o_vertices, o_edges);
    } else if (join_node->join_mode() == JoinMode::Cross) {
      _traverse_cross_join_node(join_node, o_vertices, o_edges);
    } else {
      o_vertices.emplace_back(JoinVertex(node));
      return;
    }
  } else if (node->type() == ASTNodeType::Predicate) {
    const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);

    // A non-column-to-column Predicate becomes a vertex
    if (predicate_node->value().type() != typeid(ColumnID)) {
      _traverse_value_predicate_node(predicate_node, o_vertices, o_edges);
    } else {
      _traverse_column_predicate_node(predicate_node, o_vertices, o_edges);
    }
  } else {
    // Everything that is not a Join or a Predicate becomes a vertex
    o_vertices.emplace_back(JoinVertex(node));
    return;
  }
}

void JoinGraph::_traverse_inner_join_node(const std::shared_ptr<JoinNode>& node, JoinGraph::Vertices& o_vertices,
                                          JoinGraph::Edges& o_edges) {
  // The first vertex in o_vertices that belongs to the left subtree.
  const auto first_left_subtree_vertex_idx = make_join_vertex_id(o_vertices.size());
  _traverse_ast_for_join_graph(node->left_child(), o_vertices, o_edges);

  // The first vertex in o_vertices that belongs to the right subtree.
  const auto first_right_subtree_vertex_idx = make_join_vertex_id(o_vertices.size());
  _traverse_ast_for_join_graph(node->right_child(), o_vertices, o_edges);

  auto vertex_and_column_left = _find_vertex_and_column_id(
      o_vertices, node->join_column_ids()->first, first_left_subtree_vertex_idx, first_right_subtree_vertex_idx);
  auto vertex_and_column_right =
      _find_vertex_and_column_id(o_vertices, node->join_column_ids()->second, first_right_subtree_vertex_idx,
                                 make_join_vertex_id(o_vertices.size()));

  o_edges.emplace_back(std::make_pair(vertex_and_column_left.first, vertex_and_column_right.first),
                       std::make_pair(vertex_and_column_left.second, vertex_and_column_right.second), JoinMode::Inner,
                       *node->scan_type());
}

void JoinGraph::_traverse_cross_join_node(const std::shared_ptr<JoinNode>& node, JoinGraph::Vertices& o_vertices,
                                          JoinGraph::Edges& o_edges) {
  // The first vertex in o_vertices that belongs to the left subtree.
  const auto first_left_subtree_vertex_idx = make_join_vertex_id(o_vertices.size());
  _traverse_ast_for_join_graph(node->left_child(), o_vertices, o_edges);

  // The first vertex in o_vertices that belongs to the right subtree.
  const auto first_right_subtree_vertex_idx = make_join_vertex_id(o_vertices.size());
  _traverse_ast_for_join_graph(node->right_child(), o_vertices, o_edges);

  /**
   * Create a unconditioned edge from each vertex in the left subtree to each vertex in the right subtree
   */
  for (auto left_vertex_idx = first_left_subtree_vertex_idx; left_vertex_idx < first_right_subtree_vertex_idx;
       ++left_vertex_idx) {
    for (auto right_vertex_idx = first_right_subtree_vertex_idx; right_vertex_idx < o_vertices.size();
         ++right_vertex_idx) {
      o_edges.emplace_back(std::make_pair(left_vertex_idx, right_vertex_idx), JoinMode::Cross);
    }
  }
}

void JoinGraph::_traverse_column_predicate_node(const std::shared_ptr<PredicateNode>& node,
                                                JoinGraph::Vertices& o_vertices, JoinGraph::Edges& o_edges) {
  const auto first_subtree_vertex_idx = make_join_vertex_id(o_vertices.size());
  _traverse_ast_for_join_graph(node->left_child(), o_vertices, o_edges);

  const auto column_id_left = node->column_id();
  const auto column_id_right = boost::get<ColumnID>(node->value());

  auto vertex_and_column_left = _find_vertex_and_column_id(o_vertices, column_id_left, first_subtree_vertex_idx,
                                                           make_join_vertex_id(o_vertices.size()));
  auto vertex_and_column_right = _find_vertex_and_column_id(o_vertices, column_id_right, first_subtree_vertex_idx,
                                                            make_join_vertex_id(o_vertices.size()));

  o_edges.emplace_back(std::make_pair(vertex_and_column_left.first, vertex_and_column_right.first),
                       std::make_pair(vertex_and_column_left.second, vertex_and_column_right.second), JoinMode::Inner,
                       node->scan_type());
}

void JoinGraph::_traverse_value_predicate_node(const std::shared_ptr<PredicateNode>& node,
                                               JoinGraph::Vertices& o_vertices, JoinGraph::Edges& o_edges) {
  const auto first_subtree_vertex_idx = make_join_vertex_id(o_vertices.size());
  _traverse_ast_for_join_graph(node->left_child(), o_vertices, o_edges);

  auto vertex_and_column = _find_vertex_and_column_id(o_vertices, node->column_id(), first_subtree_vertex_idx,
                                                      make_join_vertex_id(o_vertices.size()));

  auto& vertex = o_vertices[vertex_and_column.first];
  vertex.predicates.emplace_back(vertex_and_column.second, node->scan_type(), node->value());
}

std::pair<JoinVertexID, ColumnID> JoinGraph::_find_vertex_and_column_id(const Vertices& vertices, ColumnID column_id,
                                                                        JoinVertexID vertex_range_begin,
                                                                        JoinVertexID vertex_range_end) {
  /**
   * This is where the magic happens.
   *
   * For example: we found an AST node that we want to turn into a JoinEdge. The AST node is referring to two
   * ColumnIDs, one in the left subtree and one in the right subtree. We need to figure out which vertices it is
   * actually referring to, in order to form an edge.
   *
   * Think of the following table being generated by the left subtree:
   *
   * 0   | 1   | 2   | 3   | 4   | 5
   * a.a | a.b | a.c | b.a | c.a | c.b
   *
   * Now, if the join_column_ids.left is "4" it is actually referring to vertex "c" (with JoinVertexID "2") and
   * ColumnID "0".
   */

  for (auto vertex_idx = vertex_range_begin; vertex_idx < vertex_range_end; ++vertex_idx) {
    const auto& vertex = vertices[vertex_idx];
    if (column_id < vertex.node->output_column_count()) {
      return std::make_pair(vertex_idx, column_id);
    }
    column_id -= vertex.node->output_column_count();
  }
  Fail("Couldn't find column_id in vertex range.");
  return std::make_pair(INVALID_JOIN_VERTEX_ID, INVALID_COLUMN_ID);
}
}  // namespace opossum
