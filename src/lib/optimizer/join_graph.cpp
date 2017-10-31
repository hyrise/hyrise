#include "join_graph.hpp"

#include <utility>

#include "constant_mappings.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/type_utils.hpp"

namespace opossum {

JoinEdge::JoinEdge(const std::pair<JoinVertexID, JoinVertexID>& vertex_indices,
         const std::pair<ColumnID, ColumnID>& column_ids,
         JoinMode join_mode,
         ScanType scan_type):
  vertex_indices(vertex_indices),
  column_ids(column_ids), join_mode(join_mode), scan_type(scan_type)
{
  DebugAssert(join_mode == JoinMode::Inner, "Only inner join edges supported atm.");
}

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
    std::cout << vertex_idx << ":  " << vertex->description() << std::endl;
  }
  out << "==== Edges ====" << std::endl;
  for (const auto& edge : _edges) {
    std::cout << edge.vertex_indices.first << " <-- " << edge.column_ids.first << " "
              << scan_type_to_string.left.at(edge.scan_type) << " " << edge.column_ids.second
              << " --> " << edge.vertex_indices.second << std::endl;
  }

  out << "===================" << std::endl;
}

void JoinGraph::_traverse_ast_for_join_graph(const std::shared_ptr<AbstractASTNode>& node,
                                             JoinGraph::Vertices& o_vertices,
                                             JoinGraph::Edges& o_edges,
                                             ColumnID column_id_offset) {
  // To make it possible to call search_join_graph() on both children without having to check whether they are nullptr.
  if (!node) return;

  // Everything that is not a Join becomes a vertex
  if (node->type() != ASTNodeType::Join) {
    o_vertices.emplace_back(node);
    return;
  }

  const auto join_node = std::static_pointer_cast<JoinNode>(node);

  // Every non-inner join becomes a vertex for now
  if (join_node->join_mode() != JoinMode::Inner) {
    o_vertices.emplace_back(node);
    return;
  }

  Assert(join_node->scan_type(), "Need scan type for now, since only inner joins are supported");
  Assert(join_node->join_column_ids(), "Need join columns for now, since only inner joins are supported");

  /**
   * Process children on the left side
   */
  const auto left_vertex_offset = make_join_vertex_id(o_vertices.size());
  _traverse_ast_for_join_graph(node->left_child(), o_vertices, o_edges, column_id_offset);

  /**
   * Process children on the right side
   */
  const auto right_column_offset =
      ColumnID{static_cast<ColumnID::base_type>(column_id_offset + node->left_child()->output_column_count())};
  const auto right_vertex_offset = make_join_vertex_id(o_vertices.size());
  _traverse_ast_for_join_graph(node->right_child(), o_vertices, o_edges, right_column_offset);

  /**
   * Build the JoinEdge corresponding to this node
   */
  auto left_column_id = join_node->join_column_ids()->first;
  auto right_column_id = join_node->join_column_ids()->second;

  // Find vertex indices
  const auto find_column = [&o_vertices](auto column_id, const auto vertex_range_begin, const auto vertex_range_end) {
    for (auto vertex_idx = vertex_range_begin; vertex_idx < vertex_range_end; ++vertex_idx) {
      const auto& vertex = o_vertices[vertex_idx];
      if (column_id < vertex->output_column_count()) {
        return std::make_pair(vertex_idx, column_id);
      }
      column_id -= vertex->output_column_count();
    }
    Fail("Couldn't find column_id in vertex range.");
    return std::make_pair(INVALID_JOIN_VERTEX_ID, INVALID_COLUMN_ID);
  };

  const auto find_left_result = find_column(left_column_id, left_vertex_offset, right_vertex_offset);
  const auto find_right_result = find_column(right_column_id, right_vertex_offset, make_join_vertex_id(o_vertices.size()));

  const auto vertex_ids = std::make_pair(find_left_result.first, find_right_result.first);
  const auto column_ids = std::make_pair(find_left_result.second, find_right_result.second);

  JoinEdge edge(vertex_ids, column_ids, join_node->join_mode(), *join_node->scan_type());

  o_edges.emplace_back(edge);
}
}