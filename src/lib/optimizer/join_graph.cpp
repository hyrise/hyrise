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

JoinEdge::JoinEdge(const std::pair<JoinVertexID, JoinVertexID>& vertex_ids,
                   const std::pair<ColumnID, ColumnID>& column_ids, JoinMode join_mode, ScanType scan_type)
    : vertex_ids(vertex_ids), column_ids(column_ids), join_mode(join_mode), scan_type(scan_type) {
  DebugAssert(join_mode == JoinMode::Inner, "This constructor only supports inner join edges");
}

JoinEdge::JoinEdge(const std::pair<JoinVertexID, JoinVertexID>& vertex_ids, JoinMode join_mode)
    : vertex_ids(vertex_ids), join_mode(join_mode) {
  DebugAssert(join_mode == JoinMode::Cross, "This constructor only supports cross join edges");
}

JoinVertexPredicate::JoinVertexPredicate(ColumnID column_id, ScanType scan_type, const AllParameterVariant& value)
    : column_id(column_id), scan_type(scan_type), value(value) {}

JoinVertex::JoinVertex(const std::shared_ptr<AbstractASTNode>& node) : node(node) {}

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
      std::cout << edge.vertex_ids.first << " <-- " << edge.column_ids->first << " "
                << scan_type_to_string.left.at(*edge.scan_type) << " " << edge.column_ids->second << " --> "
                << edge.vertex_ids.second << std::endl;
    } else {
      std::cout << edge.vertex_ids.first << " <----> " << edge.vertex_ids.second << std::endl;
    }
  }

  out << "===================" << std::endl;
}
}  // namespace opossum
